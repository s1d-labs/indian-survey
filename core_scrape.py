"""The scraper: walk a site, classify every page, turn it into records.

Standalone (aiohttp, selectolax, core_fetch, core_crawl) so the same file runs here and in the pool family.

  generic extractors   tables (bilingual header -> field), label: value pairs, S3WaaS-style cards, document links,
                       page text; work on any site
  S3WaaS               page kind from the WordPress body classes (single-<post type>, tax-<taxonomy>,
                       page-template-<name>, page-template-default), mapped to a record type:
                         notice (tenders / recruitment / announcements ...), document, facility (public utility),
                         official (who's who), judge, service, place (tourist place), event, page (free-form)
  WordPress API        /wp-json/wp/v2/{posts,pages}: records straight from the API (other WordPress sites)

scrape_site() walks every page reachable by links (pagination included), optionally skipping language mirrors,
and returns (summary, files): files["records.jsonl"] (one JSON record per line) and files["pages.jsonl"] (every
page: url, kind, record count). Documents are listed, not downloaded.
"""
import asyncio
import json
import re
import time
from collections import Counter
from urllib.parse import unquote, urljoin, urlsplit

try:
    from .core_crawl import DOC_EXT, LANGS, SKIP_PATH, normalise, same_site, template
    from .core_fetch import UA_TOKEN, get
except ImportError:   # standalone
    from core_crawl import DOC_EXT, LANGS, SKIP_PATH, normalise, same_site, template
    from core_fetch import UA_TOKEN, get

from selectolax.parser import HTMLParser

# header text (any language the sites use) -> field name
HEADERS = [
    (r"^(title|subject|शीर्षक|विषय|शिर्षक|தலைப்பு|ಶೀರ್ಷಿಕೆ|శీర్షిక|শিরোনাম|શીર્ષક|ਸਿਰਲੇਖ|ശീർഷകം|തലക്കെട്ട്|ଶୀର୍ଷକ|শিৰোনাম|शीर्षक)", "title"),
    (r"^(description|details|विवरण|वर्णन|விவரம்|ವಿವರಣೆ|వివరణ|বিবরণ|વર્ણન|ਵੇਰਵਾ|വിവരണം)", "description"),
    (r"^(start date|प्रारंभ तिथि|प्रारम्भ तिथि|आरंभ दिनांक|सुरुवात|தொடக்க|ಪ್ರಾರಂಭ|ప్రారంభ|শুরুর|શરૂઆત|ਸ਼ੁਰੂ)", "start_date"),
    (r"^(end date|last date|अंतिम तिथि|अन्तिम तिथि|शेवटची|இறுதி|ಅಂತಿಮ|ముగింపు|শেষ|અંતિમ|ਆਖਰੀ)", "end_date"),
    (r"^(date|dated|publish|दिनांक|तारीख|तिथि|தேதி|ದಿನಾಂಕ|తేదీ|তারিখ|તારીખ|ਮਿਤੀ|തീയതി|ତାରିଖ|তাৰিখ|प्रकाशन दिनांक)", "date"),
    (r"^(file|view|download|attachment|फ़ाइल|फाइल|देखें|डाउनलोड|கோப்பு|ಕಡತ|ఫైల్|ফাইল|ફાઇલ|ਫਾਈਲ|ഫയൽ|കാണുക|ଫାଇଲ|చూడండి|హక్కు)", "file"),
    (r"^(name|नाम|பெயர்|ಹೆಸರು|పేరు|নাম|નામ|ਨਾਮ)", "name"),
    (r"^(designation|post|पद|पदनाम|பதவி|ಹುದ್ದೆ|హోదా|পদবি|હોદ્દો|ਅਹੁਦਾ)", "designation"),
    (r"^(phone|mobile|contact|telephone|landline|फ़ोन|फोन|मोबाइल|संपर्क|தொலைபேசி|ದೂರವಾಣಿ|ఫోన్|ফোন|ફોન|ਫ਼ੋਨ)", "phone"),
    (r"^(e-?mail|ईमेल|ई-मेल|மின்னஞ்சல்|ಇಮೇಲ್|ఇమెయిల్|ইমেল|ઈમેલ|ਈਮੇਲ)", "email"),
    (r"^(address|location|पता|स्थान|முகவரி|ವಿಳಾಸ|చిరునామా|ঠিকানা|સરનામું|ਪਤਾ)", "address"),
    (r"^(s\.? ?no|sl\.? ?no|sr\.? ?no|क्र|क्रम|#)", "serial"),
]
HEADERS = [(re.compile(p, re.I), f) for p, f in HEADERS]
SIZE = re.compile(r"\(?\s*([\d.]+)\s*(KB|MB|GB|bytes)\s*\)?", re.I)
DATE = re.compile(r"\b(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})\b")
LABEL = re.compile(r"^\s*([^:：]{1,40})\s*[:：]\s*(.+)$", re.S)
# S3WaaS body classes -> record type
# fixed page slugs -> record type, per family (pages are free-form WordPress pages with a table or PDF).
# personal: rows name private people (staff pay, students) -> flagged so a pipeline can drop or redact them
SLUG_TYPES = {
    "s3waas-kvs": {
        "शिक्षक-रिक्ति": ("vacancy", False), "विद्यार्थी-रिक्ति": ("student_vacancy", False), "प्रवेश-श्रेणी-वार-नामांक": ("enrolment", False),
        "शुल्क-संरचना": ("fees", False), "परिणाम-विश्लेषण": ("results", False), "कार्मिकों-का-विवरण": ("staff", True),
        "कार्मिकों-का-मासिक-पारिश्रमिक": ("staff_pay", True), "जारी-स्थानांतरण-प्रमाणपत्र": ("transfer_certificates", True),
        "संविदा-कर्मचारी": ("contract_staff", True), "आरटीआई": ("rti", False), "प्रवेश-दिशानिर्देश": ("admission_guidelines", False),
        "प्रवेश-विवरण": ("admission", False), "विद्यालय-योजना": ("school_plan", False), "क्रियाकलापों-का-कैलेंडर": ("calendar", False),
        "विद्यार्थी-उपलब्धि": ("student_achievements", True), "शिक्षक-उपलब्धि": ("teacher_achievements", False),
    },
    "s3waas-ecourts": {
        "list-of-judges": ("judge_list", False), "judges-on-leave": ("judges_on_leave", False), "duty-magistrate": ("duty_roster", False),
        "police-station-wise-magistrate": ("magistrate_roster", False), "holiday-list": ("holidays", False), "former-judges": ("former_judges", False),
    },
    "s3waas": {
        "whos-who": ("official_list", False), "helpline": ("helplines", False), "directory": ("directory", False),
        "subdivision-blocks": ("admin_units", False), "tehsil": ("admin_units", False), "village-panchayats": ("admin_units", False),
        "demography": ("statistics", False), "administrative-setup": ("admin_units", False),
    },
}

# families without WordPress body classes: URL prefix -> record type (first match wins)
FAMILY_URLS = {
    "assam-drupal": [(r"^/(archives|all-whats-new|whats-new)", "notice"), (r"^/(event-created|all-events|events?)(/|$)", "event"),
                     (r"^/(documents?|documents-detail)(/|$)", "document"), (r"^/(tenders?-list-page|tenders?)(/|$)", "tender"),
                     (r"^/(scheme-page|schemes?)(/|$)", "scheme"), (r"^/(whos-who|organigram|hod)(/|$)", "official_list"),
                     (r"^/taxonomy/term/", "listing")],
    "mizoram-laravel": [(r"^/post/", "post"), (r"^/(category|posts)(/|$)", "listing"), (r"^/page/", "page")],
    "neva": [(r"^/member", "member"), (r"^/contactdirectory", "official_list"), (r"^/questions", "question"), (r"^/debates", "debate"),
             (r"^/bill", "bill"), (r"^/committee", "committee"), (r"^/(notice|business|today)", "notice")],
    "mea-missions": [(r"^/(section|archive|page-links|listview|list)(/|$)", "listing"), (r"^/(press-release|whats-new|tenders?)(\.php)?", "notice"),
                     (r"^/(page|pages|content)/", "page")],
    "puducherry-drupal": [(r"^/(notice-board|circular|notifications?)(/|$)", "notice"), (r"^/tenders?(/|$)", "tender"),
                          (r"^/recruitments?(/|$)", "vacancy"), (r"^/(acts-rules|programmes-schemes)(/|$)", "document"),
                          (r"^/whos-who", "official_list")],
}
FAMILY_URLS = {k: [(re.compile(p, re.I), t) for p, t in v] for k, v in FAMILY_URLS.items()}

KINDS = [
    (r"\b(tax-notice[_-]category|single-notice)\b", "notice"),
    (r"\b(tax-document-category|single-document|page-template-document)\b", "document"),
    (r"\btax-public-utility-category\b", "facility"),
    (r"\b(single-whoswho|page-template-whos-?who)\b", "official"),
    (r"\b(single-judge)\b", "judge"),
    (r"\b(single-service|tax-service-category|page-template-services)\b", "service"),
    (r"\b(single-tourist-place|page-template-tourist-place)\b", "place"),
    (r"\b(single-event|page-template-events|page-template-upcoming-events)\b", "event"),
    (r"\b(single-publication|tax-publication-type)\b", "publication"),
    (r"\b(single-scheme|page-template-scheme|tax-scheme-category)\b", "scheme"),
    (r"\b(page-template-forms|single-form)\b", "document"),
    (r"\b(tax-whats-new|single-whats-new)\b", "notice"),
    (r"\b(single-testimonial)\b", "media"),
    (r"\b(single-gallery|single-video|page-template-gallery)\b", "media"),
    (r"\b(single-post|category)\b", "post"),
    (r"\bpage-template-sitemap\b", "sitemap"),
    (r"\bhome\b", "home"),
    (r"\b(page-template-default|page)\b", "page"),
]
# a class name is a whole hyphenated token: "home" must not match "home-banner"
KINDS = [(re.compile(p.replace(r"\b(", r"(?<![\w-])(").replace(r")\b", r")(?![\w-])").replace(r"\bhome\b", r"(?<![\w-])home(?![\w-])")
                     .replace(r"\btax-public-utility-category\b", r"(?<![\w-])tax-public-utility-category(?![\w-])")
                     .replace(r"\bpage-template-sitemap\b", r"(?<![\w-])page-template-sitemap(?![\w-])")), k) for p, k in KINDS]


def clean(s):
    return " ".join((s or "").split())


def deobfuscate(s):
    return re.sub(r"\s*\[\s*dot\s*\]\s*", ".", re.sub(r"\s*\[\s*at\s*\]\s*", "@", s, flags=re.I), flags=re.I)


def field_of(header):
    h = clean(header).lower()
    for rx, f in HEADERS:
        if rx.search(h):
            return f
    return re.sub(r"[^\w\u0900-\u0DFF]+", "_", h).strip("_")[:40] or "col"


def to_iso(s):
    m = DATE.search(s or "")
    if not m:
        return None
    d, mo, y = (int(x) for x in m.groups())
    return f"{y:04d}-{mo:02d}-{d:02d}" if 1 <= mo <= 12 and 1 <= d <= 31 else None


def docs_in(node, base):
    out = []
    for a in node.css("a[href]"):
        href = a.attributes.get("href") or ""
        u = urljoin(base, href)
        if DOC_EXT.search(urlsplit(u).path) and not re.search(r"\.(jpe?g|png|gif|svg|webp|ico)$", u, re.I):
            m = SIZE.search(a.text() or "")
            out.append({"url": u, "label": clean(a.text())[:120], "size": f"{m.group(1)} {m.group(2).upper()}" if m else None})
    seen, uniq = set(), []
    for d in out:
        if d["url"] not in seen:
            seen.add(d["url"])
            uniq.append(d)
    return uniq


def tables(node, base):
    """Every table with a header row -> {"headers", "fields", "rows": [{field: value}], ...}."""
    out = []
    for tb in node.css("table"):
        head = tb.css("thead th") or (tb.css_first("tr").css("th") if tb.css_first("tr") else [])
        headers = [clean(h.text()) for h in head]
        rows = []
        for tr in tb.css("tbody tr") or tb.css("tr")[1:] if headers else tb.css("tr"):
            cells = tr.css("td")
            if not cells:
                continue
            if not headers:
                headers = [f"col{i + 1}" for i in range(len(cells))]
            fields = [field_of(h) for h in headers]
            row = {}
            for f, cell in zip(fields + [f"col{i}" for i in range(len(fields), len(cells))], cells):
                txt = deobfuscate(clean(cell.text()))
                d = docs_in(cell, base)
                if d:
                    row.setdefault("_docs", []).extend(d)
                if f == "file" and d:
                    continue
                if txt:
                    row[f] = txt[:4000]
            if row:
                rows.append(row)
        if rows:
            out.append({"caption": clean((tb.css_first("caption") or tb).text())[:200] if tb.css_first("caption") else None,
                        "headers": headers, "fields": [field_of(h) for h in headers], "rows": rows})
    return out


def pairs(node):
    """'<strong>Designation:</strong> <span>X</span>' and 'Label : value' lines -> {label: value}."""
    out = {}
    for li in node.css("li, p"):
        st = li.css_first("strong, b, label")
        if st is not None:
            label = clean(st.text()).rstrip(":： ")
            value = deobfuscate(clean(li.text()[len(st.text()):] if li.text().startswith(st.text()) else li.text().replace(st.text(), "", 1)))
            value = value.lstrip(":： ").strip()
            if 0 < len(label) <= 40 and value and len(value) < 1000 and field_of(label) not in out:
                out[field_of(label)] = value
    return out


def cards(node, base):
    """S3WaaS public-utility cards: <li class=public-utlty-catg><h2>Name</h2><ul><li>address</li><li>Email : x</li></ul>."""
    out = []
    for li in node.css("li.public-utlty-catg, .pub-ut-container > li"):
        h = li.css_first("h2, h3, .heading4")
        rec = {"name": clean(h.text()) if h else None}
        extra = []
        for item in li.css("ul li"):
            t = deobfuscate(clean(item.text()))
            m = LABEL.match(t)
            if m and len(m.group(1)) < 30:
                rec[field_of(m.group(1))] = m.group(2).strip()
            elif t:
                extra.append(t)
        if extra:
            rec.setdefault("address", ", ".join(extra))
        for a in li.css("a[href]"):
            href = a.attributes.get("href") or ""
            if href.startswith("http") and not same_site(urlsplit(base).hostname, urlsplit(href).hostname):
                rec.setdefault("website", href)
        if rec.get("name"):
            out.append(rec)
    return out


def main_node(tree):
    for sel in ("#maincontent", "main", "#main-content", ".main-content", "#content", "article", ".content-area"):
        n = tree.css_first(sel)
        if n is not None:
            return n
    return tree.body


def page_kind(body_classes, url):
    path = unquote(urlsplit(url).path).lower()
    if "/past-notices/" in path or path.startswith("/past-notices"):
        return "notice"
    for rx, k in KINDS:
        if rx.search(body_classes):
            return k
    return "page"


def lang_of(url):
    segs = [s for s in urlsplit(url).path.split("/") if s]
    return segs[0].lower() if segs and segs[0].lower() in LANGS else "default"


def extract(url, html, family="s3waas"):
    """(kind, records, page_summary) for one HTML page."""
    tree = HTMLParser(html)
    body = tree.css_first("body")
    classes = (body.attributes.get("class") or "") if body else ""
    kind = page_kind(classes, url)
    for bad in tree.css("script, style, noscript, svg, header, footer, nav, .printShare, .breadcrumb-outer, .skip-content"):
        bad.decompose()
    node = main_node(tree)
    if node is None:
        return page_kind(classes, url), [], {"kind": "empty", "classes": classes[:300], "title": "", "records": 0, "tables": 0}
    h1 = node.css_first("h1") or tree.css_first("h1")
    title = clean(h1.text()) if h1 else clean((tree.css_first("title") or node).text())[:200]
    path = unquote(urlsplit(url).path)
    segs = [s for s in path.split("/") if s and s.lower() not in LANGS]
    term = segs[1] if len(segs) > 1 and segs[0] in ("notice_category", "notice-category", "past-notices", "document-category",
                                                     "public-utility-category", "service-category", "publication-type", "category") else None
    base = {"url": url, "kind": kind, "lang": lang_of(url), "category": term, "page_title": title}
    recs = []
    tabs = tables(node, url)
    if kind in ("notice", "document", "publication") and tabs:
        for t in tabs:
            for r in t["rows"]:
                rec = {**base, "type": kind, "title": r.get("title") or title, "description": r.get("description"),
                       "start_date": to_iso(r.get("start_date")), "end_date": to_iso(r.get("end_date")), "date": to_iso(r.get("date")),
                       "documents": r.get("_docs", []), "fields": {k: v for k, v in r.items() if not k.startswith("_")}}
                recs.append(rec)
    elif kind == "facility":
        for c in cards(node, url):
            recs.append({**base, "type": "facility", "facility_kind": term, **c})
    elif kind in ("official", "judge") and "single-" in classes:
        p = pairs(node)
        recs.append({**base, "type": kind, "name": title, **p})
    elif kind == "service" and "single-service" in classes:
        p = pairs(node)
        link = node.css_first("a.weburl") or next((a for a in node.css("a[href]") if (a.attributes.get("href") or "").startswith("http")), None)
        recs.append({**base, "type": "service", "name": title, "link": link.attributes.get("href") if link else None, **p,
                     "text": clean(node.text())[:2000]})
    elif kind == "place" and "single-" in classes:
        m = re.search(r"maps/dir//([-\d.]+),([-\d.]+)", html)
        reach = {clean(h.text()): clean((h.next.text() if h.next else ""))[:300] for h in node.css(".reachContainer h3")}
        desc = node.css_first(".detail-div")
        recs.append({**base, "type": "place", "name": title, "lat": float(m.group(1)) if m else None, "lon": float(m.group(2)) if m else None,
                     "description": clean(desc.text())[:3000] if desc else None, "how_to_reach": reach})
    elif kind == "scheme" and "single-" in classes:
        recs.append({**base, "type": "scheme", "name": title, **pairs(node), "text": clean(node.text())[:5000],
                     "documents": docs_in(node, url), "tables": tabs[:5]})
    elif kind in ("event", "post") and ("single-" in classes):
        recs.append({**base, "type": kind, "title": title, "date": to_iso(node.text()), "text": clean(node.text())[:4000],
                     "documents": docs_in(node, url)})
    # free-form pages, and tables on any page not taken above: keep every table as rows
    if not recs and kind not in ("media", "sitemap", "home"):
        text = clean(node.text())
        slug = segs[-1].lower() if segs else ""
        typ, personal = next(((t, pf) for k, (t, pf) in (SLUG_TYPES.get(family) or {}).items() if slug.startswith(k[:24])), ("page", False))
        if typ == "page" and family in FAMILY_URLS:
            typ = next((t for rx, t in FAMILY_URLS[family] if rx.search(path)), "page")
        if typ == "post":   # a dated item with attachments (Laravel posts)
            recs.append({**base, "type": "post", "title": title, "date": to_iso(text), "text": text[:6000], "documents": docs_in(node, url)[:100]})
            return kind, recs, {"kind": kind, "classes": classes[:300], "title": title, "records": 1, "tables": len(tabs)}
        if tabs or len(text) > 200 or docs_in(node, url):
            recs.append({**base, "type": typ, "title": title, "slug": slug, "personal_data": personal, "text": text[:6000],
                         "tables": tabs[:20], "pairs": pairs(node), "documents": docs_in(node, url)[:300]})
    return kind, recs, {"kind": kind, "classes": classes[:300], "title": title, "records": len(recs), "tables": len(tabs)}


async def wp_api(session, origin, per_page=100, max_pages=50):
    """Records from the WordPress JSON API (posts and pages), for WordPress sites that leave it open."""
    recs = []
    for typ in ("posts", "pages"):
        for page in range(1, max_pages + 1):
            meta, body = await get(session, f"{origin}/wp-json/wp/v2/{typ}?per_page={per_page}&page={page}&_fields=id,date,modified,link,title,excerpt,content,type", 8_000_000)
            if meta["code"] != 200 or not body:
                break
            try:
                items = json.loads(body)
            except ValueError:
                break
            if not isinstance(items, list) or not items:
                break
            for it in items:
                content = HTMLParser((it.get("content") or {}).get("rendered") or "<p></p>")
                recs.append({"url": it.get("link"), "kind": f"wp-{typ}", "type": "post" if typ == "posts" else "page", "wp_id": it.get("id"),
                             "title": clean(HTMLParser((it.get("title") or {}).get("rendered") or "<p></p>").text()),
                             "date": (it.get("date") or "")[:10], "modified": (it.get("modified") or "")[:10],
                             "text": clean(content.text())[:6000], "documents": docs_in(content.root, it.get("link") or origin)[:100],
                             "tables": tables(content.root, it.get("link") or origin)[:10]})
            await asyncio.sleep(0.6)
            if len(items) < per_page:
                break
    return recs


async def scrape_site(session, start_url, family="s3waas", mirrors=False, max_pages=20000, gap=0.6, budget_s=4 * 3600):
    t0 = time.monotonic()
    host = urlsplit(start_url).hostname
    origin = f"{urlsplit(start_url).scheme}://{urlsplit(start_url).netloc}"
    records, pages = [], []
    if family in ("wordpress-other", "kerala-wordpress", "ux4g"):
        api = await wp_api(session, origin)
        if api:
            summary = {"url": start_url, "family": family, "method": "wordpress-api", "records": len(api), "pages": 0,
                       "types": dict(Counter(r["type"] for r in api)), "seconds": int(time.monotonic() - t0)}
            return summary, {"records.jsonl": "\n".join(json.dumps(r, ensure_ascii=False) for r in api).encode(), "pages.jsonl": b""}
    import urllib.robotparser
    rp = None
    m, b = await get(session, origin + "/robots.txt", 262144)
    if m["code"] == 200 and b and b"<html" not in b[:500].lower():
        rp = urllib.robotparser.RobotFileParser()
        rp.parse(b.decode("utf-8", "replace").splitlines())
    frontier, queued, n = [start_url], {start_url}, 0
    kinds, types = Counter(), Counter()
    docs = set()
    while frontier and n < max_pages and time.monotonic() - t0 < budget_s:
        url = frontier.pop(0)
        if n:
            await asyncio.sleep(gap)
        meta, body = await get(session, url, 3_000_000)
        n += 1
        ctype = dict((k.lower(), v) for k, v in meta["headers"]).get("content-type", "")
        if not body or "html" not in ctype:
            pages.append({"url": url, "code": meta["code"], "kind": None, "records": 0})
            continue
        final = meta["final_url"] or url
        if not same_site(host, urlsplit(final).hostname):
            pages.append({"url": url, "code": meta["code"], "kind": "offsite", "records": 0})
            continue
        html = body.decode("utf-8", "replace")
        try:
            kind, recs, ps = extract(final, html, family)
        except Exception as e:  # noqa: BLE001: one odd page never stops a site
            kind, recs, ps = "error", [], {"kind": "error", "error": f"{type(e).__name__}: {e}"[:200]}
        kinds[kind] += 1
        for r in recs:
            types[r["type"]] += 1
            for d in r.get("documents") or []:
                docs.add(d["url"])
        records.extend(recs)
        pages.append({"url": url, "final": final, "code": meta["code"], "template": template(final), **ps})
        for href in re.findall(r"""<a\b[^>]*?\bhref\s*=\s*["']?([^"' >]+)""", html[:3_000_000], re.I):
            u = normalise(final, href)
            if not u or u in queued or not same_site(host, urlsplit(u).hostname):
                continue
            p = urlsplit(u).path
            if DOC_EXT.search(p) or SKIP_PATH.search(p):
                continue
            if not mirrors and lang_of(u) != "default":
                continue
            if rp is not None and not rp.can_fetch(UA_TOKEN, u):
                continue
            queued.add(u)
            frontier.append(u)
    summary = {"url": start_url, "family": family, "method": "crawl", "pages": n, "records": len(records), "kinds": dict(kinds),
               "types": dict(types), "documents": len(docs), "frontier_left": len(frontier), "mirrors": mirrors,
               "stopped": "budget" if frontier else "exhausted", "seconds": int(time.monotonic() - t0)}
    files = {"records.jsonl": "\n".join(json.dumps(r, ensure_ascii=False) for r in records).encode(),
             "pages.jsonl": "\n".join(json.dumps(p, ensure_ascii=False) for p in pages).encode()}
    return summary, files
