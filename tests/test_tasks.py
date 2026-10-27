"""Every task / mode runs through Worker.one end to end (fetches stubbed): catches crashes after the work is done,
like the census results that were thrown away on 27 Oct 2026."""
import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import core_crawl  # noqa: E402
import core_fetch  # noqa: E402
import core_scrape  # noqa: E402
import survey_task  # noqa: E402


class Hub:
    settings = {"threads": 1, "paused": False}

    def __init__(self):
        self.done, self.failed, self.uploads, self.records = [], [], [], []

    def metric(self, *a): pass
    def error(self, *a): pass
    def record(self, rid, data): self.records.append((rid, data))
    def upload_many(self, files): self.uploads.extend(files)

    def complete(self, keys, status="done", error="", fetched=None):
        (self.done if status == "done" else self.failed).extend(keys)


async def fake_site(*a, **k):
    return {"pages": 3, "code": 200, "url": "https://x.gov.in/"}, {"index.json": json.dumps([{"code": 200}]).encode()}


@pytest.mark.parametrize("task,mode", [("fetch", None), ("crawl", "deep"), ("crawl", "map"), ("crawl", "full"),
                                       ("crawl", "count"), ("scrape", None)])
def test_task_completes(monkeypatch, task, mode):
    for mod, name in ((core_crawl, "crawl_site"), (core_crawl, "count_site"), (core_scrape, "scrape_site"), (core_fetch, "fetch_host")):
        monkeypatch.setattr(mod, name, fake_site)
    hub = Hub()
    w = survey_task.Worker(task, hub)

    async def go():
        w.session = None
        item = {"key": "k1", "payload": {"domain": "x.gov.in", "url": "https://x.gov.in/", "ip": "1.2.3.4", "mode": mode, "family": "s3waas"}}
        await w.guarded(item)
    asyncio.run(go())
    assert hub.done == ["k1"] and not hub.failed, hub.failed
    assert hub.uploads and hub.records


def test_stale_worker_refuses_newer_items(monkeypatch):
    monkeypatch.setattr(core_crawl, "count_site", fake_site)
    hub = Hub()
    w = survey_task.Worker("crawl", hub)
    item = {"key": "k2", "payload": {"domain": "x.gov.in", "url": "https://x.gov.in/", "mode": "count", "v": survey_task.CODE_VERSION + 1}}
    asyncio.run(w.guarded(item))
    assert hub.failed == ["k2"] and not hub.uploads and w.retiring
