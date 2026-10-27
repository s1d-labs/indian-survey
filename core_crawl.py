"""Page-type discovery crawl: find every kind of page a site has, without fetching every page.

Standalone (aiohttp + core_fetch + stdlib), like core_fetch.py: runs here, in the pool family and on a vantage
in India.

A URL is reduced to a template: language prefix dropped (/hi/, /gu/, ...), the first path segment kept (the
section), later segments collapsed ({n} for numbers, {slug} for the rest, page/{n} for pagination), query keys
kept without values. The crawl walks the site breadth-first, one request at a time, and fetches at most
`per_template` examples of each template, so a 2,000-page site is mapped in a few hundred requests. It stops at
`max_pages`, or when `stop_after` pages in a row brought no new template.

  deep  the crawl above, from the home page
  map   the home page and the site's own HTML sitemap page(s): which sections a site has, in 2-3 requests
  full  every page reachable by links (no per-template cap), to count how many pages a site really has; HTML is
        kept for the first `per_template` pages of each template only

Returns (summary, files): files["index.json"] lists every page fetched (url, final url, code, type, template,
depth, title, size); files["pNNNNN.html"] holds each HTML body.
"""
import asyncio
import json
import re
import time
import urllib.robotparser
from collections import Counter
from urllib.parse import parse_qsl, unquote, urldefrag, urljoin, urlsplit

try:
    from .core_fetch import UA_TOKEN, get
except ImportError:   # standalone (pool family, India vantage)
    from core_fetch import UA_TOKEN, get

LANGS = {"hi", "en", "gu", "te", "pa", "mr", "ta", "kn", "ml", "bn", "or", "od", "as", "ur", "ne", "ks", "sd", "kok", "mni", "sa"}
DOC_EXT = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|odt|ods|csv|zip|rar|7z|jpe?g|png|gif|svg|webp|bmp|ico|mp4|mp3|avi|mov|wmv|apk|exe|xml|json|txt|rtf)$", re.I)
SKIP_PATH = re.compile(r"/(wp-admin|wp-login|wp-json|xmlrpc|feed|cdn-cgi|wp-content|wp-includes|login|logout|signin|signup|register|"
                       r"cart|checkout|print|share|mailto|javascript)\b|^/?(tel|mailto|javascript):", re.I)
PAGE_KEYS = {"page", "paged", "p", "pg", "start", "offset", "pagenum", "pageno", "pageindex"}
HREF = re.compile(rb"""<a\b[^>]*?\bhref\s*=\s*["']?([^"' >]+)[^>]*>(.*?)</a>""", re.I | re.S)
TITLE = re.compile(rb"<title[^>]*>(.*?)</title", re.I | re.S)
SITEMAP_HINT = re.compile(r"site-?map|sitemap|साइटमैप|साइट-मैप|ಸೈಟ್|సైట్|சைட்|সাইট|સાઇટ|ਸਾਈਟ", re.I)


def same_site(host, other):
    a, b = host.lower().removeprefix("www."), (other or "").lower().removeprefix("www.")
    return a == b


def template(url):
    """'/hi/past-notices/%e0%a4%a8.../page/7/?q=1' -> 'past-notices/{slug}/page/{n}?q'."""
    p = urlsplit(url)
    segs = [unquote(s) for s in p.path.split("/") if s]
    if segs and segs[0].lower() in LANGS:
        segs = segs[1:]
    out = []
    for i, s in enumerate(segs):
        low = s.lower()
        if i == 0:
            out.append(re.sub(r"\d+", "{n}", low) if re.search(r"\d", low) and not re.search(r"[a-z]{3}", low) else low[:60])
        elif low == "page" or (out and out[-1] == "page" and s.isdigit()):
            out.append("page" if low == "page" else "{n}")
        elif s.isdigit():
            out.append("{n}")
        else:
            out.append("{slug}")
    keys = sorted({k.lower() for k, _ in parse_qsl(p.query, keep_blank_values=True)})
    t = "/".join(out) or "(home)"
    return t + ("?" + "&".join(keys) if keys else "")


def normalise(base, href):
    href = href.strip()
    if not href or href.startswith(("#", "javascript:", "mailto:", "tel:", "data:")):
        return None
    u, _ = urldefrag(urljoin(base, href))
    p = urlsplit(u)
    if p.scheme not in ("http", "https"):
        return None
    q = "&".join(f"{k}={v}" for k, v in sorted(parse_qsl(p.query, keep_blank_values=True))
                 if k.lower() in PAGE_KEYS or len(p.query) < 60)
    path = p.path or "/"
    return f"{p.scheme}://{p.netloc.lower()}{path}" + (f"?{q}" if q else "")


def links(base, html):
    """(page links, document links) from one page."""
    pages, docs = [], []
    for href, text in HREF.findall(html[:3_000_000]):
        h = href.decode("utf-8", "replace")
        u = normalise(base, h)
        if not u:
            continue
        label = " ".join(re.sub(rb"<[^>]+>", b" ", text).decode("utf-8", "replace").split())[:120]
        if DOC_EXT.search(urlsplit(u).path):
            docs.append((u, label))
        else:
            pages.append((u, label))
    return pages, docs


async def crawl_site(session, start_url, mode="deep", max_pages=1500, per_template=3, stop_after=150, gap=0.6, budget_s=3000):
    if mode == "full":
        max_pages, stop_after, budget_s = max(max_pages, 30000), 10 ** 9, max(budget_s, 4 * 3600)
    keep = per_template
    cap = 10 ** 9 if mode == "full" else per_template
    kept_t = Counter()
    t0 = time.monotonic()
    host = urlsplit(start_url).hostname
    index, files = [], {}
    seen_t = Counter()          # template -> pages queued
    visited, queued = set(), set()
    frontier = [(start_url, 0, "")]
    queued.add(start_url)
    docs_seen, doc_hosts = set(), Counter()
    rp, last_new, n = None, 0, 0
    visited_t = set()

    rmeta, rbody = await get(session, f"{urlsplit(start_url).scheme}://{host}/robots.txt", 262144)
    if rmeta["code"] == 200 and rbody and b"<html" not in rbody[:500].lower():
        rp = urllib.robotparser.RobotFileParser()
        try:
            rp.parse(rbody.decode("utf-8", "replace").splitlines())
        except Exception:  # noqa: BLE001
            rp = None

    def allowed(u):
        return rp is None or rp.can_fetch(UA_TOKEN, u)

    def enqueue(u, depth, via):
        if u in queued or not same_site(host, urlsplit(u).hostname) or SKIP_PATH.search(urlsplit(u).path):
            return
        t = template(u)
        if seen_t[t] >= cap or not allowed(u):
            return
        seen_t[t] += 1
        queued.add(u)
        frontier.append((u, depth, via))

    while frontier and n < max_pages and time.monotonic() - t0 < budget_s:
        if mode in ("deep", "full") and n - last_new > stop_after:
            break
        url, depth, via = frontier.pop(0)
        if url in visited:
            continue
        visited.add(url)
        if n:
            await asyncio.sleep(gap)
        meta, body = await get(session, url, 2_000_000)
        ctype = dict((k.lower(), v) for k, v in meta["headers"]).get("content-type", "")
        is_html = bool(body) and ("html" in ctype or body[:200].lstrip()[:1] == b"<")
        t = template(meta["final_url"] or url)
        rec = {"i": n, "url": url, "final": meta["final_url"], "code": meta["code"], "err": meta["err"], "ctype": ctype[:60],
               "template": template(url), "final_template": t, "depth": depth, "via": via[:80], "len": meta["len"], "ms": meta["ms"]}
        if is_html:
            m = TITLE.search(body)
            rec["title"] = " ".join(m.group(1).decode("utf-8", "replace").split())[:160] if m else ""
            if kept_t[rec["template"]] < keep:
                kept_t[rec["template"]] += 1
                files[f"p{n:05d}.html"] = body
            base = meta["final_url"] or url
            pages, docs = links(base, body)
            rec["n_links"], rec["n_docs"] = len(pages), len(docs)
            for du, label in docs:
                if du not in docs_seen:
                    docs_seen.add(du)
                    doc_hosts[urlsplit(du).hostname or ""] += 1
            if mode in ("deep", "full"):
                for u, label in pages:
                    enqueue(u, depth + 1, label)
            elif depth == 0:   # map: follow only the site's own sitemap page(s)
                cands = [u for u, label in pages if same_site(host, urlsplit(u).hostname) and u not in queued
                         and (SITEMAP_HINT.search(unquote(urlsplit(u).path)) or SITEMAP_HINT.search(label)) and allowed(u)]
                for u in list(dict.fromkeys(cands))[:2]:
                    queued.add(u)
                    frontier.append((u, 1, "sitemap"))
                rec["home_links"] = sorted({u for u, _ in pages if same_site(host, urlsplit(u).hostname)})[:3000]
            else:              # map: keep the sitemap's links (unfetched) for the section map
                rec["sitemap_links"] = sorted({u for u, _ in pages if same_site(host, urlsplit(u).hostname)})[:5000]
        index.append(rec)
        if rec["template"] not in visited_t:
            visited_t.add(rec["template"])
            last_new = n
        n += 1

    templates = Counter(r["template"] for r in index)
    summary = {"url": start_url, "mode": mode, "pages": n, "templates": len(templates), "frontier_left": len(frontier),
               "queued_templates": len(seen_t), "stopped": "max_pages" if n >= max_pages else "saturated" if frontier else "exhausted",
               "docs": len(docs_seen), "doc_hosts": dict(doc_hosts.most_common(10)), "seconds": int(time.monotonic() - t0),
               "robots": rp is not None}
    files["index.json"] = json.dumps(index, ensure_ascii=False).encode()
    files["docs.json"] = json.dumps(sorted(docs_seen)[:20000], ensure_ascii=False).encode()
    return summary, files
