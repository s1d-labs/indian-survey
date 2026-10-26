"""Phase 6 core: open the home page in headless Chromium and record what a visitor's browser does.

Standalone (playwright + stdlib), like core_fetch.py: runs here and in the pool family.
Per host: one page, 1280x800, media requests aborted, `load` within 30 s (else whatever has
rendered), then 1.5 s for late scripts. Kept: a JPEG screenshot, the rendered HTML, every request
(host, type, status), console errors, cookies set, the webappanalyzer `js` globals that exist on
the page (with their values, e.g. jQuery.fn.jquery = "3.6.0"), and the size of the DOM.
"""
import asyncio
import json
import time
from collections import Counter
from urllib.parse import urlsplit

UA_SUFFIX = " indian-status-survey/1.0 (+https://india.s1d.me)"
VIEW = {"width": 1280, "height": 800}
ABORT = {"media"}

JS_PROBE = """(paths) => {
  const out = {};
  for (const p of paths) {
    try {
      let v = window;
      for (const k of p.split('.')) { if (v == null) { v = undefined; break; } v = v[k]; }
      if (v !== undefined && v !== null) {
        const t = typeof v;
        out[p] = (t === 'string' || t === 'number' || t === 'boolean') ? String(v).slice(0, 80) : t;
      }
    } catch (e) {}
  }
  const meta = {};
  meta.dom_nodes = document.getElementsByTagName('*').length;
  meta.text_len = (document.body && document.body.innerText || '').length;
  meta.scripts = document.scripts.length;
  meta.iframes = document.getElementsByTagName('iframe').length;
  meta.lang = document.documentElement.lang || '';
  meta.ng_version = (document.querySelector('[ng-version]') || {getAttribute: () => null}).getAttribute('ng-version');
  meta.react_root = !!document.querySelector('[data-reactroot], #__next, #root > div');
  meta.vue = !!document.querySelector('[data-v-app], [data-server-rendered]');
  return {js: out, meta};
}"""


async def new_browser(pw, executable_path=None):
    return await pw.chromium.launch(headless=True, executable_path=executable_path,
                                    args=["--disable-dev-shm-usage", "--no-first-run", "--mute-audio"])


async def render_host(browser, url, js_paths, user_agent=None, budget_s=30):
    """(summary, files). files: shot.jpg, rendered.html."""
    ctx = await browser.new_context(viewport=VIEW, ignore_https_errors=True, user_agent=user_agent,
                                    locale="en-IN", java_script_enabled=True, service_workers="block")
    page = await ctx.new_page()
    reqs, fails, console = [], Counter(), []
    main = {}

    async def route(r):
        if r.request.resource_type in ABORT:
            await r.abort()
        else:
            await r.continue_()
    await page.route("**/*", route)
    page.on("console", lambda m: console.append(m.text[:200]) if m.type == "error" and len(console) < 20 else None)
    page.on("pageerror", lambda e: console.append(("pageerror: " + str(e))[:200]) if len(console) < 20 else None)
    page.on("requestfailed", lambda r: fails.update([r.failure or "failed"]))

    def on_response(resp):
        req = resp.request
        reqs.append((urlsplit(req.url).hostname or "", req.resource_type, resp.status))
        if req.is_navigation_request() and req.frame == page.main_frame:
            main["code"] = resp.status
            main["url"] = resp.url
    page.on("response", on_response)

    summary = {"url": url, "err": None}
    files = {}
    t0 = time.monotonic()
    try:
        try:
            await page.goto(url, wait_until="load", timeout=budget_s * 1000)
        except Exception as e:  # noqa: BLE001: a slow page still gets a screenshot of what loaded
            name = type(e).__name__
            summary["err"] = "load-timeout" if "Timeout" in name else ("goto:" + str(e).split("\n")[0][:120])
            if summary["err"] != "load-timeout":
                raise
        summary["load_ms"] = int((time.monotonic() - t0) * 1000)
        await page.wait_for_timeout(1500)
        probe = await asyncio.wait_for(page.evaluate(JS_PROBE, js_paths), 10)
        summary["js"] = probe["js"]
        summary["meta"] = probe["meta"]
        summary["title"] = (await page.title())[:200]
        summary["final_url"] = page.url
        files["shot.jpg"] = await page.screenshot(type="jpeg", quality=70, timeout=15000)
        files["rendered.html"] = (await page.content()).encode("utf-8", "replace")
        cookies = await ctx.cookies()
        summary["cookies"] = [{"name": c["name"], "domain": c["domain"], "httpOnly": c["httpOnly"], "secure": c["secure"]}
                              for c in cookies][:60]
    except Exception as e:  # noqa: BLE001
        summary["err"] = summary.get("err") or ("error:" + str(e).split("\n")[0][:160])
    finally:
        summary["code"] = main.get("code")
        summary["main_url"] = main.get("url")
        hosts = Counter(h for h, _, _ in reqs)
        summary["n_requests"] = len(reqs)
        summary["hosts"] = dict(hosts.most_common(60))
        summary["types"] = dict(Counter(t for _, t, _ in reqs))
        summary["status"] = dict(Counter(str(s) for _, _, s in reqs))
        summary["failed"] = dict(fails)
        summary["console"] = console
        summary["total_ms"] = int((time.monotonic() - t0) * 1000)
        try:
            await ctx.close()
        except Exception:  # noqa: BLE001
            pass
    return summary, files


def js_paths_from_webappanalyzer(tech_dir):
    """Every `js` property path named in the webappanalyzer technologies (simple dotted paths only)."""
    import pathlib
    import re
    paths = set()
    for f in pathlib.Path(tech_dir).glob("*.json"):
        for tech in json.loads(f.read_text()).values():
            for p in (tech.get("js") or {}):
                if re.fullmatch(r"[A-Za-z_$][\w$]*(\.[A-Za-z_$][\w$]*)*", p):
                    paths.add(p)
    return sorted(paths)
