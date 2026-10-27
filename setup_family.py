"""Put this family's projects in the pool and seed their queues. Run by the Setup workflow (GitHub Actions: the hub trusts
this repo, s1d-labs/<family>, through its OIDC token). Safe to run again: queue items that exist are kept."""
from scrape_hub_client import register_project, seed_queue, set_pool

from family import FAMILY, PROJECTS, crawl_items, items

for p in PROJECTS:
    work = crawl_items(p["modes"]) if p.get("modes") else items()
    register_project(p["name"], title=p["title"], unit_label=p["unit_label"], record_label=p["record_label"],
                     expected_units=len(work), expected_records=len(work))
    set_pool(p["name"], FAMILY, p["task"], target=p["target"], env=p["env"], probe=p["probe"], max_workers=p["max_workers"])
    print(p["name"], "queue:", seed_queue(p["name"], work))
