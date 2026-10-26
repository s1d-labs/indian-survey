# family-template

Start a new scraping family for the s1d-labs pool. A family is one kind of site (or one platform many sites share); the pool runs
its code on members' GitHub Actions minutes. This template is a complete, working family for
[books.toscrape.com](https://books.toscrape.com) (a public site made for scraping practice): copy it, then change it.

## Make your family

1. Ask the hub owner to **add the family** on [scrape.s1d.me/runners](https://scrape.s1d.me/runners) (name, what it scrapes, owner).
   The hub creates `s1d-labs/<family>` from this template, names the family in `scrape.json`, and makes the owner admin
   (with the GitHub account they added on home.s1d.me).
2. Clone it: `gh repo clone s1d-labs/<family>`.
3. In your repo:
   - `scrape.json`: set `"family"` to the family name; list your tasks (one command each) and setup steps.
   - `family.py`: your projects (one queue of work per site), their target hosts, the env your command needs, the queue items,
     and the health checks.
   - `crawl.py` (or whatever your tasks run): the scraper.
4. **Check it**: `scrape-check` in the repo (needs `pip install "scrape-hub-client @ git+https://github.com/s1d-labs/runner@main#subdirectory=sdk"`).
   It runs your setup and task against a local stand-in hub on 3 real items and says what works. The **check** workflow runs it
   (and `tests/`) on every push.
5. Run the **setup** workflow (Actions → setup → Run workflow). It registers the projects, puts them in the pool and registers
   the health checks.
6. The hub starts runs in members' accounts when the pool is on and there is work. Watch it on scrape.s1d.me.

## What your code gets

The pool worker ([s1d-labs/runner](https://github.com/s1d-labs/runner)) checks out this repo at `main`, runs the `setup` steps from
`scrape.json`, and for each assignment runs the task's command here with:

| Variable | What |
|---|---|
| `SCRAPE_POOL_PROJECT` | the project to work on |
| `SCRAPE_JOB_INDEX`, `SCRAPE_TOTAL_JOBS` | this worker's slice |
| `SCRAPE_MAX_HOURS` | how long the command may run |
| `SCRAPE_HUB_URL` | the hub |
| your project's `env` | e.g. `SITE` |

Use `scrape_hub_client` (`HubClient`) to claim queue items, store records, upload files and report counters. Exit when there is
nothing left; the worker then asks the hub for the next assignment.

## Rules (the full contract is in CLAUDE.md, written for people and for Claude Code)

- **Be polite.** Use `hub.settings["threads"]` and `delay_ms`, idle while `hub.settings["paused"]`, and report `n_http` and
  `t_http_s` for every request and `http_<code>` / `http_error` for failures: the hub throttles you from these.
- **Files go through the hub** (`hub.upload_many`): it signs each upload into the family's bucket (500 GB per member) and stops at
  the cap. Never put storage keys in this repo.
- **Public data only**; read the site's terms first. See the team docs on home.s1d.me (Scraping rules).
