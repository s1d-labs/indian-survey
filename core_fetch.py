"""Phase 2 core: fetch what a browser would ask a host for, and keep every byte.

Standalone on purpose (aiohttp + stdlib only, no package imports): the same file runs here, in the
s1d-labs/indian-survey pool family on GitHub Actions, and in the India agent.

Per host, one request at a time: robots.txt, the home page (redirect chain, up to 2 MB), plain
http:// on port 80 (does it redirect to https?), the favicon, sitemap.xml, /wp-json/, and one
made-up path (the 404 page shows the stack). Extra paths are skipped when robots.txt disallows them
and when the home page could not be reached at all.
"""
import asyncio
import re
import secrets
import socket
import ssl
import time
import urllib.robotparser
from urllib.parse import urljoin, urlsplit

import aiohttp

UA = ("Mozilla/5.0 (compatible; indian-status/1.0; public availability monitor of Indian government websites; "
      "one-time architecture survey; https://india.s1d.me)")
UA_TOKEN = "indian-status"
CAPS = {"robots": 262144, "home": 2 * 1048576, "port80": 32768, "favicon": 262144, "sitemap": 262144,
        "wpjson": 65536, "notfound": 65536, "home_http": 2 * 1048576}
GAP_S = 0.3   # pause between two requests to the same host
ICON = re.compile(rb"<link\b[^>]*\brel=[\"']?[^\"'>]*\bicon\b[^>]*>", re.I)
HREF = re.compile(rb"\bhref=[\"']?([^\"' >]+)", re.I)
DEAD = ("dns-", "connect-timeout", "refused", "unreachable", "connect-error")

_INSECURE = ssl.create_default_context()
_INSECURE.check_hostname = False
_INSECURE.verify_mode = ssl.CERT_NONE
try:
    _INSECURE.set_ciphers("DEFAULT:@SECLEVEL=0")   # old government servers still offer weak ciphers
except ssl.SSLError:
    pass
_INSECURE.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)   # old servers without RFC 5746 renegotiation


def make_session(timeout=25):
    conn = aiohttp.TCPConnector(limit=0, force_close=True, ttl_dns_cache=600, family=socket.AF_INET,
                                ssl=_INSECURE, enable_cleanup_closed=True)
    return aiohttp.ClientSession(
        connector=conn, timeout=aiohttp.ClientTimeout(total=timeout, sock_connect=10),
        headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                 "Accept-Language": "en-IN,en;q=0.8,hi;q=0.6"},
        cookie_jar=aiohttp.DummyCookieJar(), auto_decompress=True, max_line_size=65536, max_field_size=65536)


def _err(e, t_conn_done):
    if isinstance(e, aiohttp.ClientConnectorDNSError):
        msg = str(getattr(e.os_error, "strerror", "") or e).lower()
        return "dns-nxdomain" if "not found" in msg or "no data" in msg or "name or service" in msg else "dns-fail"
    if isinstance(e, aiohttp.ClientConnectorSSLError):
        return "tls-error"
    if isinstance(e, aiohttp.ClientConnectorError):
        oe = e.os_error
        if isinstance(oe, ConnectionRefusedError):
            return "refused"
        if isinstance(oe, (TimeoutError, asyncio.TimeoutError)):
            return "connect-timeout"
        if isinstance(oe, OSError) and oe.errno in (101, 113):
            return "unreachable"
        return "connect-error"
    if isinstance(e, aiohttp.TooManyRedirects):
        return "redirect-loop"
    if isinstance(e, (aiohttp.ServerTimeoutError, asyncio.TimeoutError, TimeoutError)):
        return "read-timeout" if t_conn_done else "connect-timeout"
    if isinstance(e, aiohttp.ServerDisconnectedError):
        return "disconnected"
    if isinstance(e, aiohttp.ClientOSError):
        return "reset" if "reset" in str(e).lower() else "connect-error"
    if isinstance(e, aiohttp.ClientResponseError):
        return "bad-response"
    if isinstance(e, (aiohttp.InvalidURL, ValueError, UnicodeError)):
        return "bad-url"
    return "error:" + type(e).__name__


async def _read(stream, cap):
    """read(n) may return early with whatever is buffered: keep reading to EOF or cap."""
    parts, n = [], 0
    while n < cap:
        chunk = await stream.read(min(65536, cap - n))
        if not chunk:
            break
        parts.append(chunk)
        n += len(chunk)
    return b"".join(parts)


async def get(session, url, cap, follow=True):
    """(meta, body). meta: url, final_url, code, headers [[k, v]...], history, ms, err, len, truncated."""
    meta = {"url": url, "final_url": None, "code": None, "headers": [], "history": [], "ms": None,
            "err": None, "len": 0, "truncated": False}
    body = b""
    t0 = time.monotonic()
    got_headers = False
    try:
        async with session.get(url, allow_redirects=follow, max_redirects=10) as r:
            got_headers = True
            meta["code"] = r.status
            meta["final_url"] = str(r.url)
            meta["headers"] = [[a.decode("latin-1"), b.decode("latin-1")] for a, b in r.raw_headers]
            meta["history"] = [{"url": str(h.url), "code": h.status, "location": h.headers.get("Location"),
                                "server": h.headers.get("Server"), "set_cookie": h.headers.getall("Set-Cookie", [])[:20]}
                               for h in r.history]
            try:
                body = await asyncio.wait_for(_read(r.content, cap + 1), 20)
            except (asyncio.TimeoutError, aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                meta["err"] = "body:" + type(e).__name__
            if len(body) > cap:
                body, meta["truncated"] = body[:cap], True
    except Exception as e:  # noqa: BLE001: every failure becomes a label, never a crash
        meta["err"] = _err(e, got_headers)
    meta["ms"] = int((time.monotonic() - t0) * 1000)
    meta["len"] = len(body)
    return meta, body


def origin(u):
    p = urlsplit(u)
    return f"{p.scheme}://{p.netloc}"


def icon_url(base, html):
    for tag in ICON.findall(html[:200000]):
        m = HREF.search(tag)
        if m:
            href = m.group(1).decode("utf-8", "replace").strip()
            if href and not href.startswith("data:"):
                return urljoin(base, href)
    return None


async def fetch_host(session, url, gap=GAP_S):
    """Everything for one host. Returns (summary, files) where files maps a name to bytes:
    <req>.meta (JSON) and <req>.body for each request made."""
    import json
    files, reqs = {}, {}

    async def do(name, u, follow=True):
        if reqs:
            await asyncio.sleep(gap)
        meta, body = await get(session, u, CAPS[name], follow)
        reqs[name] = meta
        files[name + ".meta"] = json.dumps(meta, separators=(",", ":")).encode()
        if body:
            files[name + ".body"] = body
        return meta, body

    host = urlsplit(url).hostname
    home, hbody = await do("home", url)
    if home["code"] is None and url.startswith("https://") and not (home["err"] or "").startswith("dns-"):
        alt, abody = await do("home_http", "http://" + url[8:])
        if alt["code"] is not None:
            home, hbody = alt, abody
    summary = {"url": url, "code": home["code"], "err": home["err"], "final_url": home["final_url"],
               "len": home["len"], "ms": home["ms"], "redirects": len(home["history"])}
    if home["code"] is None and any((home["err"] or "").startswith(d) for d in DEAD):
        summary["reqs"] = {k: {"code": v["code"], "err": v["err"]} for k, v in reqs.items()}
        return summary, files

    base = origin(home["final_url"] or url)
    rob_meta, rob = await do("robots", base + "/robots.txt")
    rp = None
    if rob_meta["code"] == 200 and rob and b"<html" not in rob[:500].lower():
        rp = urllib.robotparser.RobotFileParser()
        try:
            rp.parse(rob.decode("utf-8", "replace").splitlines())
        except Exception:  # noqa: BLE001
            rp = None

    def allowed(u):
        return rp is None or rp.can_fetch(UA_TOKEN, u)

    if url.startswith("https://"):
        await do("port80", f"http://{host}/", follow=False)
    ic = icon_url(home["final_url"] or url, hbody) or base + "/favicon.ico"
    for name, u in (("favicon", ic), ("sitemap", base + "/sitemap.xml"), ("wpjson", base + "/wp-json/"),
                    ("notfound", f"{base}/survey-{secrets.token_hex(4)}.html")):
        if allowed(u):
            await do(name, u)
        else:
            reqs[name] = {"skipped": "robots"}
    summary["reqs"] = {k: {"code": v.get("code"), "err": v.get("err"), "skipped": v.get("skipped")} for k, v in reqs.items()}
    summary["robots_disallow_all"] = bool(rp and not rp.can_fetch(UA_TOKEN, base + "/"))
    return summary, files
