# CLAUDE.md: how this repo works (s1d-labs scrape pool family)

This repo is one **family** of the s1d-labs scrape pool: scrapers that run on members' GitHub Actions minutes, coordinated by
scrape-hub (https://scrape.s1d.me). The hub is generic: it hands out queue items, sets speed per site, signs file uploads, and tracks
requests. Everything about this family's data lives here. Below is the technical reference the runner and the hub work with; the same
text is on home.s1d.me → Docs → Scraper guide (Part 2), and on team boxes at /usr/local/share/team-docs/13-scraper-guide.md.

## Working here

- `family.py` holds the projects, targets, env, queue items and health checks; `scrape.json` the setup steps and tasks; the task
  scripts do the scraping. After changing `family.py`, the **setup** workflow (Actions → setup) applies it to the hub.
- `parse()` is pure and tested on saved pages (`tests/`); `python crawl.py --local <url>` runs one page without the hub;
  `scrape-check` runs setup and a task against a local stand-in hub and reports what works (also run by the **check** workflow).
- The repo is public; uploads go through `hub.upload_many` with signed links, so no storage keys are needed here.
- The hub's speed control runs on the request metrics (`n_http`, `t_http_s`, `http_<code>`, `http_error`) and is applied through
  `hub.settings`; a task exits when the queue is empty and the runner asks for the next one.


### Repository layout (`s1d-labs/<family>`, public)

| File | Required | What |
|---|---|---|
| `scrape.json` | yes | `{"family": "<family>", "setup": ["<shell command>", ...], "tasks": {"<task>": ["<cmd>", "<arg>", ...]}}` |
| `family.py` | template convention | `FAMILY`, `PROJECTS` (list of dicts), `PROBES` (list of dicts); read by `setup_family.py` |
| `setup_family.py` | template convention | registers projects, pool entries, health checks, seeds queues (run by `.github/workflows/setup.yml`) |
| task scripts | yes | e.g. `crawl.py`; one per task named in `scrape.json` |
| `requirements.txt` | usually | installed by a `setup` step |

- `family` is the repo name, and a family that exists on the hub (the hub owner adds it).
- `tasks`: names match `^[a-z0-9][a-z0-9_-]{0,40}$`; commands are argv lists. `"python"` is replaced by the runner's interpreter.
- `setup` commands run once per runner job, in the repo root, before any task (e.g. `pip install -r requirements.txt`).

### How a task is run

The pool runner (`s1d-labs/runner`, GitHub Actions, ubuntu-latest, Python 3.12) checks out the family repo at `main` into `family/`,
installs `scrape-hub-client`, runs `setup`, then for each assignment runs `cwd=family/`, `argv = tasks[task] + assignment.args`, with:

| Env var | Meaning |
|---|---|
| `SCRAPE_POOL_PROJECT` | project to work on (string) |
| `SCRAPE_POOL_TARGET` | the project's target host |
| `SCRAPE_JOB_INDEX`, `SCRAPE_TOTAL_JOBS` | this worker's slice (ints as strings) |
| `SCRAPE_MAX_HOURS` | time budget in hours (float as string); finish and exit before it ends |
| `SCRAPE_HUB_URL` | the hub, `https://scrape.s1d.me` |
| project `env` | whatever the project's pool entry set (names `^[A-Z][A-Z0-9_]{0,63}$`, not starting with `SCRAPE_`, `GITHUB_`, `RUNNER_`, `ACTIONS_`, `PATH`, `PYTHON`, `LD_`, `HOME`) |

- Exit code 0 = finished normally (including "queue empty"). Non-zero = the worker waits a bit and asks for new work.
- The process gets SIGINT when its time is up (then SIGTERM, then SIGKILL): catch it, call `hub.close("failed", ...)`, exit.
- Authentication is automatic: the client mints a GitHub OIDC token (the workflow grants `id-token: write`). No secrets.

### Projects, pool entries, health checks (`setup_family.py`, from GitHub Actions in the family repo only)

```python
from scrape_hub_client import register_project, seed_queue, set_pool, set_probes
set_probes(FAMILY, [{"name": f"{FAMILY}:example.gov.in", "url": "https://example.gov.in/", "expect": "text on the page"}])
register_project(name, title="...", unit_label="page", record_label="company", expected_units=N, expected_records=M)
set_pool(name, FAMILY, task="crawl", target="example.gov.in", env={"SITE": "https://example.gov.in"}, probe=f"{FAMILY}:example.gov.in")
seed_queue(name, [{"key": "page-1", "payload": {"url": "...", "expected": 20}}, ...])
```

- Project names: `^[a-z0-9][a-z0-9_-]{1,63}$`; start them with the family name. A project belongs to one family.
- `set_probes` **replaces** the family's whole list. Up = HTTP 200 within 20 s and the body contains `expect` (case-insensitive).
  Checked every minute. While a project's probe fails, its work is held (`hub.settings["paused"]` is true, claims return nothing).
- `seed_queue` keeps existing keys (safe to re-run). Keys ≤ 200 chars, unique per project. `payload` is any JSON your task needs;
  `payload.expected` (int) = records expected from that item (progress and ETAs).
- `set_pool(..., max_workers=n)` caps workers on that project; the family's default per target is 8.

### The client in a task (`scrape_hub_client.HubClient`)

```python
hub = HubClient(project, job_index=int(os.environ["SCRAPE_JOB_INDEX"]), total_jobs=int(os.environ["SCRAPE_TOTAL_JOBS"]))
items = hub.claim(limit=1, steal=True, retry_failed=True)   # [{"key", "payload", "attempts"}], [] when nothing is available
hub.metric("n_http"); hub.metric("t_http_s", seconds)       # per request
hub.record(record_id, data)                                 # buffered, sent in batches of 25
hub.upload_many([(path, bytes, content_type), ...])         # -> [{"path", "key", "bytes", "md5", "sha256"}]
hub.failure(kind, key, reason, **detail)                    # something that could not be fetched (listed on the dashboard)
hub.complete([key], "done", fetched=n)                      # or "failed", error="short reason"
hub.close()                                                 # or hub.close("failed", "why")
```

| Call | Exact behaviour |
|---|---|
| `HubClient(...)` | starts a run on the hub (one per process) and a heartbeat thread; `hub.run_id` |
| `hub.settings` | refreshed on every call: `threads` (int), `delay_ms` (int), `paused` (bool), `max_claimed` (int, items in flight for the whole project), `throttled` (bool), `reason` (str) |
| `claim(limit, steal, retry_failed)` | up to `limit` (1-100) items of your slice (`partition_id % total_jobs == job_index`); `steal=True` also takes other slices' pending items; `retry_failed=True` also takes items that failed ≥ 3 minutes ago (≤ 40 attempts). Returns `[]` while paused or when the project's `max_claimed` is reached. After it, `hub.queue = {"pending", "claimed", "retry"}` for the project. An item claimed by a run that goes silent for 5 minutes is handed to someone else |
| `complete(keys, status, error, fetched)` | flushes buffered records first. `status`: `"done"` or `"failed"`; `fetched`: records this item produced |
| `record(id, data)` | `id`: str, unique per project (use the site's own id). `data`: JSON. Re-sending an id **replaces** its data (upsert) |
| `metric(name, value=1)` | adds to a counter (names `a-z 0-9 - _`); sent with each batch |
| `upload_many(files)` | path relative to the project (`[A-Za-z0-9._-]` segments joined by `/`, no `..`), stored at `<project>/<path>` in the family's bucket; ≤ 300 files per call; verifies the checksum; raises after 8 failed tries. HTTP 507 = bucket full |
| `failure(kind, key, reason, **detail)` | `kind` `^[a-z0-9][a-z0-9_-]*$`; same (kind, key) again updates it; storing a record whose id equals `key` clears it; `hub.clear_failure(kind, key)` clears explicitly |
| `extra(name, body)` | calls your family app's own endpoint, if your family has one (ask the hub owner) |

### Metrics the hub reads (send these)

| Name | Value |
|---|---|
| `n_http` | 1 per request to the site |
| `t_http_s` | seconds the request took |
| `http_error` | 1 per connection error or timeout |
| `http_<code>` | 1 per non-200 answer (`http_429`, `http_503`, ...) |
| `uploaded_files`, `uploaded_bytes` | sent by `upload_many` for you |

Anything else you count (`captcha_ok`, `items_skipped`, ...) is shown but not interpreted. `hub.block()` counts a request the site
refused (403/429/503); `hub.error()` counts any other error. Both feed the automatic back-off.

### What the hub does with them

- **Back-off** (on unless the project turns `auto_backoff` off): in the last 3 minutes, > 25% of requests blocked (with ≥ 20
  blocked + stored), ≥ 20 refusals (429/403/503), or ≥ 40 dropped connections and more of them than records stored → `threads` halves
  and `delay_ms` becomes ≥ 200 until it calms down.
- **Automatic throttle** (if the family owner turns on `settings.adaptive`): every minute, aims for 3 s per request; moves `threads`
  and `max_claimed`; halves on > 2x the target, > 5% dropped or > 10% items failing; lowers the cap if the site falls over.
- **Workers per target**: at most 8 processes on one target host (family owner can change it).

### Task skeleton (follow this shape)

```python
import os, time, requests
from scrape_hub_client import HubClient

PROJECT = os.environ["SCRAPE_POOL_PROJECT"]
DEADLINE = time.time() + float(os.environ.get("SCRAPE_MAX_HOURS", "1")) * 3600 - 120

def fetch(hub, session, url):
    t = time.time()
    try:
        r = session.get(url, timeout=30)
    except requests.RequestException:
        hub.metric("http_error"); hub.error(); return None
    hub.metric("n_http"); hub.metric("t_http_s", time.time() - t)
    if r.status_code != 200:
        hub.metric(f"http_{r.status_code}")
        if r.status_code in (403, 429, 503): hub.block()
        return None
    if not r.encoding or r.encoding.lower() == "iso-8859-1":   # no charset declared: don't let requests assume Latin-1
        r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def main():
    hub = HubClient(PROJECT, job_index=int(os.environ.get("SCRAPE_JOB_INDEX", 0)), total_jobs=int(os.environ.get("SCRAPE_TOTAL_JOBS", 1)))
    s = requests.Session()
    try:
        while time.time() < DEADLINE:
            if hub.settings.get("paused"):
                time.sleep(30); continue
            items = hub.claim(1, steal=True, retry_failed=True)
            if not items:
                break
            item = items[0]
            page = fetch(hub, s, item["payload"]["url"])
            if page is None:
                hub.complete([item["key"]], "failed", error="page did not load"); continue
            rows = parse(page)                      # pure function: test it on saved pages
            for rid, data in rows:
                hub.record(rid, data)
            hub.complete([item["key"]], "done", fetched=len(rows))
            time.sleep(max(0.5, hub.settings.get("delay_ms", 0) / 1000))
        hub.close()
    except BaseException as e:
        hub.close("failed", f"{type(e).__name__}: {e}"[:200]); raise
```

Concurrency: if you use threads, use at most `hub.settings["threads"]`, re-read it after each `claim`, and keep one `requests.Session`
per thread. `HubClient` is thread-safe for `record`, `metric`, `failure`, `upload_many`.

### Testing

- **Parsing**: keep `parse()` pure and test it with saved pages (`tests/test_parse.py`, pages in `tests/pages/`); `python -m pytest tests`.
- **One page, no hub**: `python crawl.py --local <url>` (template) fetches a page and prints the records.
- **Against the hub's API**: `scrape-check` (from `scrape-hub-client` ≥ 0.5, `pip install "scrape-hub-client @ git+https://github.com/s1d-labs/runner@main#subdirectory=sdk"`).
  It runs `setup_family.py` and one task against `scrape_hub_client.testhub.TestHub`, a local stand-in that answers the same runner API
  with the same validation, using the runner's env vars, cwd and argv. Options: `--items N` (default 3), `--project`, `--task`,
  `--pause S` (seconds the hub reports paused at the start, default 5, 0 = off), `--no-rerun`, `--timeout S`, `--json`; for repos without
  `setup_family.py`: `--project P --task T --env K=V --item '{"key": "...", "payload": {...}}'`. Exit code 0 = no FAIL.
  Checks: scrape.json (family name, tasks, scripts exist), setup runs, pool entries (family, task in scrape.json, target, probe
  registered), health checks, and for each run: exits on its own with code 0, starts a run, waits while paused (an empty claim while
  `paused` is not the end of the queue), claims all N items, completes every claimed item, ≥ 1 done, records stored, `n_http` and
  `t_http_s` reported, run closed as finished, no call rejected; then the rerun stores the same record ids. Output in `.scrape-check/`.
- **CI**: `.github/workflows/check.yml` (template) runs the parsing tests and `scrape-check --items 2` on every push.
- **First pool run**: seed 3-5 items, run setup, and watch scrape.s1d.me → Runs before seeding the rest.

### Common pitfalls

- Secrets in the repo or in `env`: family repos are public, and uploads work without keys (`upload_many`).
- Projects of other families: the hub refuses writes to them.
- An endless loop on an empty queue, or tight retries of a failing request: exiting and letting the hub retry works better.
- Fetching what is already stored, when the site's ids allow checking first.
- Renaming tasks in `scrape.json` without updating the projects' pool entries (`set_pool`).
