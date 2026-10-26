"""Parsing tests on a saved page: fast, no network. Add a saved page for every layout your site has."""
from pathlib import Path

import crawl

PAGES = Path(__file__).parent / "pages"


def test_catalogue_page_gives_twenty_books():
    rows = crawl.parse((PAGES / "catalogue-page-1.html").read_text(encoding="utf-8"))
    assert len(rows) == 20
    rid, book = rows[0]
    assert rid == "a-light-in-the-attic_1000"                       # the site's own id: stable across runs
    assert book["title"] == "A Light in the Attic" and book["price"] == "£51.77" and book["availability"] == "In stock"
    assert book["url"].startswith("https://books.toscrape.com/catalogue/")
    assert len({r for r, _ in rows}) == 20                          # no duplicate ids on a page


def test_an_empty_page_gives_nothing():
    assert crawl.parse("<html><body>No products</body></html>") == []
