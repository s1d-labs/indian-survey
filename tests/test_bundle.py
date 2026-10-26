import io
import json
import sys
import tarfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import core_fetch  # noqa: E402
from survey_task import bundle, slim  # noqa: E402


def test_bundle_roundtrip():
    blob = bundle({"code": 200, "url": "https://x.gov.in"}, {"home.meta": b"{}", "home.body": b"<html></html>"})
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as t:
        names = sorted(m.name for m in t.getmembers())
        assert names == ["home.body", "home.meta", "summary.json"]
        assert json.loads(t.extractfile("summary.json").read())["code"] == 200


def test_icon_url():
    html = b'<head><link rel="shortcut icon" href="/wp-content/themes/sdo-theme/images/favicon.ico"></head>'
    assert core_fetch.icon_url("https://agra.nic.in/", html) == "https://agra.nic.in/wp-content/themes/sdo-theme/images/favicon.ico"
    assert core_fetch.icon_url("https://a.gov.in/", b"<p>none</p>") is None


def test_slim_keeps_outcome_only():
    s = slim("fetch", {"code": 403, "err": None, "reqs": {"home": {}}, "len": 10})
    assert s["code"] == 403 and "reqs" not in s
