"""year_created from a lot title (decorative lots put the date in the title, not the description)."""

import pytest

from parsers.graphql_parser import parse_card
from parsers.lot_facts import facts_from_text, year_from_title
from utils.repair import offline_fill


@pytest.mark.parametrize("title, year", [
    ("A German parcel-gilt silver tankard, Daniel Stobbe, Konigsberg, circa 1680", 1680),
    ("A German parcel-gilt silver tankard, Philipp Küsel, Augsburg, 1691-1695", 1691),
    ("A German silver-gilt cup, maker's mark SP, dated 1616", 1616),
    ("A gold snuff box, Paris 1765/1766,", 1765),
    ("A silver-gilt cup and cover, Hanau, circa 1880, with import marks for Chester, 1899", 1880),
    # life dates in brackets are skipped
    ("A bronze group, Italian, late 18th / early 19th century, after Giambologna (1529 - 1608)", None),
    # only an import year: not when it was made
    ("A German silver nef, Hanau, late 19th century, with London import marks for 1911", None),
    # a century is not a year: no guessing
    ("Two German parcel-gilt silver sideboard dishes, late 19th century", None),
    # a page reference or a price is not a year
    ("A cup, attributed to Thomas Francis (see Mitchell p.522-523), London, 1613", 1613),
    ("A tankard, estimate £1,500", None),
    ("", None),
    (None, None),
])
def test_year_from_title(title, year):
    assert year_from_title(title) == year


def test_description_year_wins_over_title():
    values, flags = facts_from_text("Oil on canvas\nExecuted in 1962.", title="Untitled, circa 1960")
    assert values["year_created"] == 1962
    assert "year_created_from_title" not in flags


def test_title_year_when_description_has_none():
    values, flags = facts_from_text("chased and embossed with scrolling foliage", title="A tankard, circa 1600")
    assert values["year_created"] == 1600
    assert "year_created_from_title" in flags


def test_offline_fill_fills_a_blank_year_only():
    row = {"title": "A tankard, circa 1600", "year_created": "", "data_quality_flags": ""}
    assert "year_created" in offline_fill(row)
    assert row["year_created"] == "1600" and "year_created_from_title" in row["data_quality_flags"]

    kept = {"title": "A tankard, circa 1600", "year_created": "1598", "data_quality_flags": ""}
    offline_fill(kept)
    assert kept["year_created"] == "1598"          # never overwrites a stored value


def test_graphql_card_gets_the_title_year():
    card = {"lotId": "x1", "title": "A Victorian silver-gilt tankard, William Eley, London, 1837",
            "lotNumber": {"lotDisplayNumber": "5"}}
    lot = parse_card(card, {"auction_url": "https://example.com/a"}, "recent")
    assert lot.year_created == 1837
    assert "year_created_from_title" in lot.data_quality_flags
