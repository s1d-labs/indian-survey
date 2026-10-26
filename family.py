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
HOSTS = json.loads((Path(__file__).parent / "hosts.json").read_text())


def items():
    return [{"key": f"{i:05d}-{h['domain']}", "payload": {**h, "expected": 1}} for i, h in enumerate(HOSTS)]


PROJECTS = [
    {"name": f"{FAMILY}-fetch", "title": "igod hosts: home page, robots, favicon, sitemap, 404", "task": "fetch",
     "target": "igod-hosts", "env": {}, "probe": None, "unit_label": "host", "record_label": "host", "max_workers": 40},
    {"name": f"{FAMILY}-render", "title": "igod hosts: home page rendered in Chromium", "task": "render",
     "target": "igod-hosts-render", "env": {}, "probe": None, "unit_label": "host", "record_label": "host", "max_workers": 60},
]

PROBES = []   # thousands of different hosts: no single health check means anything
