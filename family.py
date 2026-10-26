"""What this family scrapes. Change this file first.

FAMILY must equal the repo name (s1d-labs/<FAMILY>) and the family the hub owner added on scrape.s1d.me/runners.
Each project is one queue of work on one site; `target` is the host it hits (the hub caps workers per target).
"""
import json
from pathlib import Path

FAMILY = json.loads((Path(__file__).parent / "scrape.json").read_text())["family"]

PROJECTS = [
    {
        "name": f"{FAMILY}-books",              # project names: 2-64 chars of a-z 0-9 - _, start them with the family name
        "title": "Books to Scrape catalogue",
        "task": "crawl",                         # a task from scrape.json
        "target": "books.toscrape.com",
        "env": {"SITE": "https://books.toscrape.com"},
        "probe": f"{FAMILY}:books.toscrape.com",
        "unit_label": "page", "record_label": "book",
        # the queue: one item per catalogue page; `expected` (records per item) gives progress and ETAs on the dashboard
        "items": [{"key": f"page-{n}", "payload": {"url": f"https://books.toscrape.com/catalogue/page-{n}.html", "expected": 20}}
                  for n in range(1, 51)],
    },
]

# health checks the hub runs every minute; work for a project holds while its probe fails
PROBES = [{"name": f"{FAMILY}:books.toscrape.com", "url": "https://books.toscrape.com/", "expect": "All products"}]
