"""The family's tasks (see scrape.json): `python survey_task.py fetch|render`.

Each queue item is one host from igod.gov.in ({"domain", "url", "ip"}). For each:
  fetch   robots.txt, home page (+ redirect chain), port 80, favicon, sitemap.xml, /wp-json/, one 404 (core_fetch.py)
  render  the home page in headless Chromium: screenshot, rendered HTML, requests, runtime libraries (core_render.py)
The result is one tar.gz per host, uploaded as <project>/<domain>.tgz through the hub (signed links, no keys here),
plus a small record per host. The analysis happens on the hub owner's box (indian-status, `survey pull`).

Politeness: every item is a different host, so a process only ever has one request open per host; items sharing a
server IP are spread out by the queue order and never run at the same time within a process; an IP that answers
429/503 is left alone for 60 s. The standard counters (n_http, t_http_s, http_error, http_<code>) are reported.
Try without the hub: python survey_task.py fetch --local https://agra.nic.in
"""
import asyncio
import io
import json
import os
import signal
import sys
import tarfile
import time
from collections import defaultdict
from pathlib import Path

import core_crawl
import core_fetch
import core_scrape
import core_render

HERE = Path(__file__).resolve().parent
PROJECT = os.environ.get("SCRAPE_POOL_PROJECT", "")
DEADLINE = time.time() + float(os.environ.get("SCRAPE_MAX_HOURS", "1")) * 3600 - 180


def bundle(summary, files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=6) as t:
        for name, data in [("summary.json", json.dumps(summary).encode())] + sorted(files.items()):
            ti = tarfile.TarInfo(name)
            ti.size, ti.mtime = len(data), int(time.time())
            t.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def slim(task, s):
    """The record kept on the hub: enough to see progress and outcomes without the bundle."""
    keep = ("code", "err", "final_url", "len", "ms", "redirects", "title", "load_ms", "n_requests", "total_ms", "main_url",
            "mode", "pages", "templates", "stopped", "docs", "seconds", "records", "types", "documents", "method", "family",
            "discovered", "listings", "listing_pages", "listed_items", "detail_items", "pages_estimate", "pages_estimate_with_mirrors")
    return {"task": task, "egress": EGRESS, **{k: s.get(k) for k in keep if k in s}}


EGRESS = ""
# politeness per server: sites that share an IP with many others (68 Mizoram sites on one Google IP, 154 Delhi sites on
# one Airtel IP) get one request at a time and a gap that grows with the group; others get 3 at a time
try:
    _hosts = json.loads((HERE / "hosts.json").read_text())
    IP_GROUP = __import__("collections").Counter(h.get("ip") for h in _hosts if h.get("ip"))
except (OSError, ValueError):
    IP_GROUP = {}


# NIC's networks: if this runner's IP gets blocked there (NIC refused dev-vps-2 on 27 Oct 2026), the runner retires so the
# pool starts a fresh VM with a new IP; the blocked items go back to the queue for another runner
NIC_NETS = ("164.100.", "103.195.", "45.127.")
BLOCK_ERRS = {"connect-timeout", "connect-error", "reset", "unreachable", "disconnected"}
BLOCK_WINDOW, BLOCK_FAILS = 10, 6


class VantageBlocked(Exception):
    pass


def pace(ip):
    n = IP_GROUP.get(ip, 1) if ip else 1
    if n <= 3:
        return {"conc": 3, "gap": 0.3}
    return {"conc": 1, "gap": min(6.0, 0.5 * n / 4)}


class Worker:
    def __init__(self, task, hub):
        self.task, self.hub = task, hub
        self.ip_locks = defaultdict(asyncio.Lock)
        self.ip_pause = defaultdict(float)
        self.browser = None
        self.pages = 0
        self.js_paths = json.loads((HERE / "js_paths.json").read_text()) if task == "render" else []
        self.ua = None
        self.nic = []          # outcomes of the last NIC-hosted sites: True = answered
        self.retiring = False

    async def start(self):
        if self.task in ("fetch", "crawl", "scrape"):
            self.session = core_fetch.make_session()
        else:
            from playwright.async_api import async_playwright
            self.pw = await async_playwright().start()
            await self.new_browser()

    async def new_browser(self):
        if self.browser:
            await self.browser.close()
        self.browser = await core_render.new_browser(self.pw)
        p = await self.browser.new_page()
        self.ua = await p.evaluate("navigator.userAgent") + core_render.UA_SUFFIX
        await p.close()
        self.pages = 0

    async def stop(self):
        if self.task in ("fetch", "crawl", "scrape"):
            await self.session.close()
        else:
            await self.browser.close()
            await self.pw.stop()

    def count(self, summary, n_req, secs):
        hub = self.hub
        hub.metric("n_http", n_req)
        hub.metric("t_http_s", round(secs, 3))
        code = summary.get("code")
        if code is None:
            hub.metric("http_error")
        elif code != 200:
            hub.metric(f"http_{code}")

    async def one(self, item):
        p = item["payload"]
        ip = p.get("ip") or p["domain"]
        async with self.ip_locks[ip]:
            wait = self.ip_pause[ip] - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            t0 = time.monotonic()
            if self.task == "fetch":
                summary, files = await asyncio.wait_for(core_fetch.fetch_host(self.session, p["url"]), 300)
                n_req = len(summary.get("reqs") or {}) or 1
            elif self.task == "scrape":
                summary, files = await asyncio.wait_for(core_scrape.scrape_site(self.session, p["url"], family=p.get("family", "s3waas")), 5 * 3600)
                n_req = summary.get("pages") or 1
            elif self.task == "crawl" and p.get("mode") == "count":
                summary, files = await asyncio.wait_for(core_crawl.count_site(self.session, p["url"], **pace(p.get("ip"))), 2.5 * 3600)
            elif self.task == "crawl":
                mode = p.get("mode", "deep")
                limit = 5 * 3600 if mode == "full" else 3600
                summary, files = await asyncio.wait_for(core_crawl.crawl_site(self.session, p["url"], mode=mode, **pace(p.get("ip"))), limit)
                n_req = summary.get("pages") or 1
            else:
                self.pages += 1
                try:
                    summary, files = await asyncio.wait_for(
                        core_render.render_host(self.browser, p["url"], self.js_paths, self.ua), 100)
                except asyncio.TimeoutError:
                    summary, files = {"url": p["url"], "err": "render-timeout"}, {}
                n_req = 1
            if summary.get("code") in (429, 503):
                self.ip_pause[ip] = time.monotonic() + 60
            if (p.get("ip") or "").startswith(NIC_NETS):
                failed = home_failed(self.task, summary, files)
                self.nic = (self.nic + [not failed])[-BLOCK_WINDOW:]
                if failed and self.nic.count(False) >= BLOCK_FAILS and True not in self.nic[-BLOCK_FAILS:]:
                    self.retiring = True
                if failed and self.retiring:
                    raise VantageBlocked(f"NIC refuses this runner ({EGRESS or 'unknown IP'})")
            self.count(summary, n_req, time.monotonic() - t0)
        summary["egress"] = EGRESS
        summary["domain"] = p["domain"]
        blob = bundle(summary, files)
        name = f"{p['mode']}/{p['domain']}.tgz" if self.task == "crawl" else f"{p['domain']}.tgz"
        await asyncio.to_thread(self.hub.upload_many, [(name, blob, "application/gzip")])
        self.hub.record(p["domain"], slim(self.task, summary))
        self.hub.complete([item["key"]], "done", fetched=1)

    async def run(self):
        await self.start()
        try:
            if self.task == "render":
                await self.run_batches()
            else:
                await self.run_slots()
        finally:
            await self.stop()

    async def guarded(self, item):
        try:
            await self.one(item)
        except VantageBlocked as r:   # back to the queue for a runner NIC still lets in
            self.hub.metric("vantage_blocked")
            self.hub.complete([item["key"]], "failed", error=str(r)[:150])
        except BaseException as r:  # noqa: BLE001: one site never stops the worker
            self.hub.error()
            self.hub.complete([item["key"]], "failed", error=f"{type(r).__name__}: {str(r)[:150]}")

    async def run_slots(self):
        """Keep `threads` sites in flight and claim the next one as soon as a slot frees (a batch would wait for its
        slowest site). `threads` is re-read from the hub every time, so the hub can widen or narrow this process."""
        running, exhausted = set(), False
        while (running or not exhausted) and time.time() < DEADLINE:
            st = self.hub.settings
            want = 0 if st.get("paused") or self.retiring else max(1, int(st.get("threads", 4)))
            while not exhausted and len(running) < want:
                items = await asyncio.to_thread(self.hub.claim, 1, True, True)
                if not items:
                    exhausted = True
                    break
                running.add(asyncio.create_task(self.guarded(items[0])))
            if running:
                _, running = await asyncio.wait(running, timeout=30, return_when=asyncio.FIRST_COMPLETED)
            elif self.retiring:
                break
            elif not exhausted:
                await asyncio.sleep(30)   # paused

    async def run_batches(self):
        while time.time() < DEADLINE:
            st = self.hub.settings
            if st.get("paused"):
                await asyncio.sleep(30)
                continue
            n = max(1, int(st.get("threads", 4)))
            items = await asyncio.to_thread(self.hub.claim, n, True, True)
            if not items:
                break
            res = await asyncio.gather(*(self.one(i) for i in items), return_exceptions=True)
            if self.task == "render" and (self.pages >= 150 or any(isinstance(r, BaseException) for r in res)):
                await self.new_browser()   # between batches only: no page of this process is open now
            for item, r in zip(items, res):
                if isinstance(r, BaseException):
                    self.hub.error()
                    self.hub.complete([item["key"]], "failed", error=f"{type(r).__name__}: {str(r)[:150]}")
            if st.get("delay_ms"):
                await asyncio.sleep(st["delay_ms"] / 1000)


def home_failed(task, summary, files):
    """Did the site's first page fail to connect at all (the sign of a blocked vantage)?"""
    try:
        if task == "scrape":
            first = json.loads((files.get("pages.jsonl") or b"").split(b"\n", 1)[0] or b"{}")
            return not first.get("code") and summary.get("pages", 0) <= 2
        if task == "fetch":
            return not summary.get("code") and summary.get("err") in BLOCK_ERRS
        idx = json.loads(files.get("index.json") or b"[]")
        first = idx[0] if idx else {}
        return not first.get("code") and (first.get("err") in BLOCK_ERRS or summary.get("pages", 0) <= 1)
    except (ValueError, IndexError, AttributeError):
        return False


class LocalHub:
    """Stand-in for HubClient so `--local URL` runs without the hub."""
    settings = {"threads": 1, "delay_ms": 0, "paused": False}

    def __init__(self, url):
        from urllib.parse import urlsplit
        mode = sys.argv[sys.argv.index("--mode") + 1] if "--mode" in sys.argv else "deep"
        self.items = [{"key": "local", "payload": {"domain": urlsplit(url).hostname, "url": url, "mode": mode}}]

    def claim(self, *a):
        out, self.items = self.items, []
        return out

    def metric(self, *a):
        print("metric", *a, file=sys.stderr)

    def error(self, *a):
        pass

    def record(self, rid, data):
        print(json.dumps({"record": rid, **data}))

    def upload_many(self, files):
        for path, data, _ in files:
            out = Path(".scrape-check") / path
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(data)
            print("saved", path, len(data), "bytes", file=sys.stderr)

    def complete(self, keys, status="done", error="", fetched=None):
        if status != "done":
            print("failed:", error, file=sys.stderr)

    def close(self, *a):
        pass


def main():
    global EGRESS
    task = sys.argv[1] if len(sys.argv) > 1 else ""
    if task not in ("fetch", "render", "crawl", "scrape"):
        sys.exit("usage: survey_task.py fetch|render|crawl|scrape [--local URL]")
    if "--local" in sys.argv:
        hub = LocalHub(sys.argv[sys.argv.index("--local") + 1])
    else:
        from scrape_hub_client import HubClient
        hub = HubClient(PROJECT, job_index=int(os.environ.get("SCRAPE_JOB_INDEX", 0)),
                        total_jobs=int(os.environ.get("SCRAPE_TOTAL_JOBS", 1)), note=f"indian-survey {task}")
        try:
            EGRESS = HubClient._egress_ip() or ""
        except Exception:  # noqa: BLE001
            EGRESS = ""
    signal.signal(signal.SIGTERM, lambda *a: (_ for _ in ()).throw(KeyboardInterrupt()))
    worker = Worker(task, hub)
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        hub.close("failed", "stopped: time is up")
        return
    if worker.retiring:
        hub.close("failed", f"NIC blocks this runner ({EGRESS or 'unknown IP'}): retiring it so the pool starts a fresh VM")
        if "--local" not in sys.argv:
            os.kill(os.getppid(), signal.SIGTERM)   # the pool worker stops; this VM's job ends, the dispatcher starts a new one
        return
    hub.close()


if __name__ == "__main__":
    main()
