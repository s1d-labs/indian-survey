# indian-survey

A one-off architecture survey of Indian government websites, run on the s1d-labs scrape pool. The host list is every site
on [igod.gov.in](https://igod.gov.in) (~10.5K that resolve) plus ~5.9K government hosts igod doesn't list (appended Oct 29), made by `indian-status` on the hub owner's box (`hosts.json`).

| Project | Task | Per host |
|---|---|---|
| `indian-survey-fetch` | `python survey_task.py fetch` | robots.txt, home page with its redirect chain, plain http on port 80, favicon, sitemap.xml, `/wp-json/`, one made-up path (the 404 page) |
| `indian-survey-render` | `python survey_task.py render` | the home page in headless Chromium: screenshot, rendered HTML, every request, runtime libraries (webappanalyzer `js` globals) |

Each host becomes one `<domain>.tgz` in the family bucket plus a small record. The analysis (platform families such as S3WaaS,
NIC's Drupal CMF, GePNIC; how sites in a family differ; shared hosting) happens in `indian-status` (`python -m indianstatus survey ...`).

- `core_fetch.py` and `core_render.py` are copied from `indian-status/indianstatus/survey/` by its `scripts/sync_family.sh`:
  change them there.
- Politeness: one request at a time per host and per server IP within a process, a 60 s pause for an IP that answers 429/503,
  extra paths skipped when robots.txt disallows them, an honest User-Agent naming the survey and https://india.s1d.me.
- Try one host without the hub: `python survey_task.py fetch --local https://agra.nic.in` (output in `.scrape-check/`).
