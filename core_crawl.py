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
  count every page the home page and the sitemap page link to (up to 300); for each paginated listing also its last
        page, so items = rows per page x pages; whether listed items have their own detail pages. No HTML kept.

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


async def crawl_site(session, start_url, mode="deep", max_pages=1500, per_template=3, stop_after=150, gap=0.6, budget_s=3000, conc=1):
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
    discovered = {start_url}   # every same-site page URL seen linked, fetched or not
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

    inflight = 0

    async def fetch_one(url, depth, via):
        nonlocal n, last_new
        await asyncio.sleep(gap)
        meta, body = await get(session, url, 2_000_000)
        i = n
        n += 1
        ctype = dict((k.lower(), v) for k, v in meta["headers"]).get("content-type", "")
        is_html = bool(body) and ("html" in ctype or body[:200].lstrip()[:1] == b"<")
        t = template(meta["final_url"] or url)
        rec = {"i": i, "url": url, "final": meta["final_url"], "code": meta["code"], "err": meta["err"], "ctype": ctype[:60],
               "template": template(url), "final_template": t, "depth": depth, "via": via[:80], "len": meta["len"], "ms": meta["ms"]}
        if is_html:
            m = TITLE.search(body)
            rec["title"] = " ".join(m.group(1).decode("utf-8", "replace").split())[:160] if m else ""
            if kept_t[rec["template"]] < keep:
                kept_t[rec["template"]] += 1
                files[f"p{i:05d}.html"] = body
            base = meta["final_url"] or url
            pages, docs = links(base, body)
            rec["n_links"], rec["n_docs"] = len(pages), len(docs)
            for du, label in docs:
                if du not in docs_seen:
                    docs_seen.add(du)
                    doc_hosts[urlsplit(du).hostname or ""] += 1
            for u, _ in pages:
                if same_site(host, urlsplit(u).hostname) and not SKIP_PATH.search(urlsplit(u).path):
                    discovered.add(u)
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
            last_new = i


    async def worker():
        nonlocal inflight
        while n < max_pages and time.monotonic() - t0 < budget_s:
            if mode in ("deep", "full") and n - last_new > stop_after:
                return
            if not frontier:
                if inflight == 0:
                    return
                await asyncio.sleep(0.05)
                continue
            url, depth, via = frontier.pop(0)
            if url in visited:
                continue
            visited.add(url)
            inflight += 1
            try:
                await fetch_one(url, depth, via)
            finally:
                inflight -= 1

    # `conc` requests at a time to this site (1 = strictly sequential); the first page alone, so the frontier fills first
    await fetch_one(*frontier.pop(0)) if frontier else None
    visited.add(start_url)
    await asyncio.gather(*(worker() for _ in range(max(1, conc))))

    templates = Counter(r["template"] for r in index)
    summary = {"url": start_url, "mode": mode, "pages": n, "templates": len(templates), "frontier_left": len(frontier),
               "queued_templates": len(seen_t), "stopped": "max_pages" if n >= max_pages else "saturated" if frontier else "exhausted",
               "docs": len(docs_seen), "doc_hosts": dict(doc_hosts.most_common(10)), "seconds": int(time.monotonic() - t0),
               "robots": rp is not None, "discovered": len(discovered)}
    files["index.json"] = json.dumps(index, ensure_ascii=False).encode()
    files["docs.json"] = json.dumps(sorted(docs_seen)[:20000], ensure_ascii=False).encode()
    files["discovered.json"] = json.dumps(sorted(discovered)[:50000], ensure_ascii=False).encode()
    return summary, files


PAGER = re.compile(r"""href\s*=\s*["']([^"']*?(?:/page/(\d{1,5})/?|[?&](?:page|paged)=(\d{1,5}))(?:[&#][^"']*)?)["']""", re.I)


def listing_stats(url, html, host):
    """rows / cards / post links on a page, the last page number among its pagination links, and how many listed items
    link to an internal detail page (not a document, not a pager)."""
    from selectolax.parser import HTMLParser
    t = HTMLParser(html)
    for bad in t.css("header, footer, nav, script, style"):
        bad.decompose()
    main = t.css_first("#maincontent") or t.css_first("main") or t.css_first("#content") or t.body
    if main is None:
        return {"rows": 0, "last_page": 1, "detail_links": 0, "docs": 0}
    rows = max((len([tr for tr in tb.css("tr") if tr.css("td")]) for tb in main.css("table")), default=0)
    cards = len(main.css("li.public-utlty-catg, .views-row, article, .card, .post-item, .news-item"))
    posts = len({a.attributes.get("href") for a in main.css("a[href]") if re.search(r"/(post|node|notice|document|news|event)s?/[^/]+/?$", a.attributes.get("href") or "")})
    items = max(rows, cards, posts)
    base_path = urlsplit(url).path.rstrip("/")
    last = 1
    own = re.sub(r"/page/\d+$", "", base_path)
    for m in PAGER.finditer(html[:3_000_000]):
        h = urljoin(url, m.group(1).replace("&amp;", "&"))
        hp = re.sub(r"/page/\d+/?$", "", urlsplit(h).path.rstrip("/"))
        if hp == own and same_site(host, urlsplit(h).hostname):   # pagination of this very listing, nothing else
            last = max(last, int(m.group(2) or m.group(3)))
    detail = 0
    for a in main.css("tr a[href], li a[href], article a[href], .views-row a[href]"):
        h = urljoin(url, a.attributes.get("href") or "")
        p = urlsplit(h)
        if same_site(host, p.hostname) and not DOC_EXT.search(p.path) and not PAGER.search(f'href="{h}"') and p.path.rstrip("/") != base_path:
            detail += 1
    docs = sum(1 for a in main.css("a[href]") if DOC_EXT.search(urlsplit(a.attributes.get("href") or "").path))
    return {"rows": items, "last_page": min(last, 20000), "detail_links": detail, "docs": docs}


async def count_site(session, start_url, gap=0.5, budget_s=2 * 3600, conc=1):
    """Page census of one site: a discovery crawl (2 pages per template, so archives and every free-form page are
    reached), then for each paginated listing its last page: items = rows per page x pages. Reports linked pages,
    extra listing pages, listed items, detail pages (items whose rows link to their own page) and language mirrors."""
    t0 = time.monotonic()
    host = urlsplit(start_url).hostname
    summary, files = await crawl_site(session, start_url, mode="deep", per_template=2, stop_after=200, gap=gap, budget_s=budget_s, conc=conc)
    idx = json.loads(files["index.json"])
    pages, seen_lists = [], set()
    for r in idx:
        rec = {"url": r["url"], "template": r["template"], "code": r["code"]}
        body = files.get(f"p{r['i']:05d}.html")
        if r["code"] == 200 and body:
            st = listing_stats(r.get("final") or r["url"], body.decode("utf-8", "replace"), host)
            rec.update(st)
            base = re.sub(r"/page/\d+/?$", "", (r.get("final") or r["url"]).rstrip("/"))
            if st["last_page"] > 1 and base not in seen_lists and time.monotonic() - t0 < budget_s:
                seen_lists.add(base)
                await asyncio.sleep(gap)
                last_url = base + f"/page/{st['last_page']}/"
                m2, b2 = await get(session, last_url, 2_000_000)
                if m2["code"] == 200 and b2:
                    rec["last_rows"] = listing_stats(last_url, b2.decode("utf-8", "replace"), host)["rows"]
                rows, last = st["rows"], st["last_page"]
                rec["items"] = rows * (last - 1) + rec.get("last_rows", rows)
                rec["listing"] = base
        pages.append(rec)
    lists = {}
    for p in pages:   # one entry per listing (page 1 and page N of a listing are the same listing)
        if p.get("listing"):
            lists[p["listing"]] = p
    ok = [p for p in pages if p.get("code") == 200]
    mirror = sum(1 for p in pages if [x for x in urlsplit(p["url"]).path.split("/") if x][:1] and
                 [x for x in urlsplit(p["url"]).path.split("/") if x][0].lower() in LANGS)
    tpl_pages = Counter(p["template"] for p in ok)
    est = {
        "fetched_ok": len(ok), "templates": len(tpl_pages),
        "listings": len(lists),
        "listing_pages": sum(p["last_page"] for p in lists.values()),
        "listed_items": sum(p.get("items", 0) for p in lists.values()),
        "detail_items": sum(p.get("items", 0) for p in lists.values() if p.get("detail_links", 0) >= max(1, p.get("rows", 0) // 2)),
        "mirror_pages_seen": mirror,
    }
    disc = json.loads(files.get("discovered.json", b"[]"))
    seen_pages = Counter(re.sub(r"/page/\d+/?$", "", u.rstrip("/")) for u in disc if re.search(r"/page/\d+/?$", u))
    unseen_listing_pages = sum(max(0, p["last_page"] - 1 - seen_pages.get(base, 0)) for base, p in lists.items())
    mirror_disc = sum(1 for u in disc if [x for x in urlsplit(u).path.split("/") if x][:1] and [x for x in urlsplit(u).path.split("/") if x][0].lower() in LANGS)
    est["discovered"] = len(disc)
    est["discovered_mirror"] = mirror_disc
    est["unseen_listing_pages"] = unseen_listing_pages
    est["pages_estimate"] = len(disc) - mirror_disc + unseen_listing_pages + est["detail_items"]
    est["pages_estimate_with_mirrors"] = len(disc) + unseen_listing_pages * (2 if mirror_disc else 1) + est["detail_items"] * (2 if mirror_disc else 1)
    out = {"url": start_url, "mode": "count", "pages": summary["pages"] + len(seen_lists), "stopped": summary["stopped"],
           "seconds": int(time.monotonic() - t0), "docs": summary.get("docs"), **est}
    return out, {"index.json": json.dumps(pages, ensure_ascii=False).encode()}
