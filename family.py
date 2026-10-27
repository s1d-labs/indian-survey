"""What this family does: one pass over every host listed on igod.gov.in (hosts.json, made by indian-status on the hub
owner's box: domain, start URL, resolved IP). Two projects, one queue item per host each:

  indian-survey-fetch   what the host serves (core_fetch.py)
  indian-survey-render  how its home page renders in Chromium (core_render.py)

Every item is a different host, so the per-target worker cap does not apply in the usual sense: `target` is a label and
politeness is per server IP inside the task (see survey_task.py). The queue order interleaves hosts that share an IP.
"""
import json
from pathlib import Path

FAMILY = json.loads((Path(__file__).parent / "scrape.json").read_text())["family"]
# bump with every change to how a task runs: items carry it, and a worker on older code refuses newer items
CODE_VERSION = 3
HOSTS = json.loads((Path(__file__).parent / "hosts.json").read_text())
PLAN = json.loads((Path(__file__).parent / "crawlplan.json").read_text())   # from indian-status `survey crawl plan`
IPS = {h["domain"]: h.get("ip") for h in HOSTS}
SCRAPE = json.loads((Path(__file__).parent / "scrapeplan.json").read_text())   # from indian-status `survey scrape plan`


def items():
    return [{"key": f"{i:05d}-{h['domain']}", "payload": {**h, "expected": 1}} for i, h in enumerate(HOSTS)]


def scrape_items():
    return [{"key": f"scrape-{i:04d}-{d}", "payload": {"domain": d, "url": v["url"], "ip": IPS.get(d), "family": v["family"], "expected": 1}}
            for i, (d, v) in enumerate(SCRAPE.items())]


def crawl_items(modes):
    out = []
    for mode in modes:
        run_as = {"census": "count", "census2": "count"}.get(mode, mode)   # census = the page census (count) re-run on fixed code
        for i, (d, v) in enumerate(PLAN[mode].items()):
            out.append({"key": f"{mode}-{i:05d}-{d}", "payload": {"domain": d, "url": v["url"], "ip": IPS.get(d), "mode": run_as,
                                                                   "family": v["family"], "v": CODE_VERSION, "expected": 1}})
    return out


def scrape10_items():
    return [{"key": f"scrape10-{i:04d}-{d}", "payload": {"domain": d, "url": v["url"], "ip": IPS.get(d), "family": v["family"],
                                                           "v": CODE_VERSION, "expected": 1}}
            for i, (d, v) in enumerate(PLAN["scrape10"].items())]


PROJECTS = [
    {"name": f"{FAMILY}-fetch", "title": "igod hosts: home page, robots, favicon, sitemap, 404", "task": "fetch",
     "target": "igod-hosts", "env": {}, "probe": None, "unit_label": "host", "record_label": "host", "max_workers": 40},
    {"name": f"{FAMILY}-render", "title": "igod hosts: home page rendered in Chromium", "task": "render",
     "target": "igod-hosts-render", "env": {}, "probe": None, "unit_label": "host", "record_label": "host", "max_workers": 60},
    {"name": f"{FAMILY}-crawl", "title": "page-type discovery (deep) and page counts (full) on a sample of sites", "task": "crawl",
     "target": "igod-crawl", "env": {}, "probe": None, "unit_label": "site", "record_label": "site", "max_workers": 40,
     "modes": ["full", "deep"]},
    {"name": f"{FAMILY}-map", "title": "section map of every readable host (home + its sitemap page)", "task": "crawl",
     "target": "igod-map", "env": {}, "probe": None, "unit_label": "site", "record_label": "site", "max_workers": 30,
     "modes": ["map"]},
    {"name": f"{FAMILY}-count", "title": "page census of every readable host: discovered pages, listings, items", "task": "crawl",
     "target": "igod-count", "env": {}, "probe": None, "unit_label": "site", "record_label": "site", "max_workers": 80,
     "modes": ["count"]},
    {"name": f"{FAMILY}-census", "title": "page census of every readable host (re-run on the fixed worker)", "task": "crawl",
     "target": "igod-census", "env": {}, "probe": None, "unit_label": "site", "record_label": "site", "max_workers": 60,
     "modes": ["census"]},
    {"name": f"{FAMILY}-census2", "title": "page census of every readable host (third run: the census results are kept now)", "task": "crawl",
     "target": "igod-census2", "env": {}, "probe": None, "unit_label": "site", "record_label": "site", "max_workers": 80,
     "modes": ["census2"]},
    {"name": f"{FAMILY}-scrape10", "title": "the scraper on the 10% stratified sample (every page, as records)", "task": "scrape",
     "target": "igod-scrape10", "env": {}, "probe": None, "unit_label": "site", "record_label": "site", "max_workers": 60,
     "scrape10": True},
    {"name": f"{FAMILY}-scrape", "title": "scraper smoke test: every page of a sample of sites per family, as records", "task": "scrape",
     "target": "igod-scrape", "env": {}, "probe": None, "unit_label": "site", "record_label": "site", "max_workers": 30, "scrape": True},
]

PROBES = []   # thousands of different hosts: no single health check means anything
