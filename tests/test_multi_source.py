"""Many web sources, one schema.

A made-up second auction house ("Example House") is built here the way docs/ADDING_A_SOURCE.md
says: one small spider on top of BaseAuctionSpider plus one parser. The tests check that it runs
through the same validation and storage as Sotheby's and lands in exactly the same columns.
"""

import csv
from types import SimpleNamespace

import pytest
import scrapy
from scrapy.exceptions import DropItem
from scrapy.http import HtmlResponse, Request
from scrapy.spiderloader import SpiderLoader
from scrapy.utils.project import get_project_settings

from models.auction_lot import FIELDS, AuctionLot
from parsers.common import clean_text, to_int, utc_now_iso
from pipelines.storage import StoragePipeline, migrate_header
from pipelines.validation import ValidationPipeline
from spiders.base import BaseAuctionSpider
from spiders.historical_auctions import HistoricalAuctionsSpider
from spiders.recent_auctions import RecentAuctionsSpider

TOOLS = {"repair_details"}   # spiders that fix a csv, not crawl a source


# ---------------------------------------------------------------- a second source, as a new developer would add it
LISTING = """<html><body>
  <a class="sale" href="/sales/101" data-date="2026-10-01">Modern Art Evening</a>
  <a class="sale" href="/sales/102" data-date="2026-10-02">Design Day</a>
</body></html>"""

SALE_101 = """<html><body data-lot-count="2">
  <a class="lot" href="/sales/101/lots/1">1</a>
  <a class="lot" href="/sales/101/lots/2">2</a>
</body></html>"""

LOT_1 = """<html><body>
  <h1 class="artist">Jane Doe</h1><h2 class="title">Blue Field</h2>
  <span class="estimate" data-low="10,000" data-high="15,000" data-currency="GBP"></span>
  <span class="result" data-sold="1" data-price="18,900" data-currency="GBP"></span>
  <div class="provenance">Private collection, London</div>
</body></html>"""


def parse_example_lot(response, auction, source_version):
    """parsers/example_house.py in a real project: page in, AuctionLot out, no network."""
    estimate = response.css("span.estimate")
    result = response.css("span.result")
    sold = result.attrib.get("data-sold") == "1"
    lot_number = response.url.rstrip("/").split("/")[-1]
    return AuctionLot(
        auction_name=auction["auction_name"], auction_date=auction["auction_date"],
        auction_url=auction["auction_url"], auction_id=auction["auction_id"],
        lot_number=lot_number, lot_id=f"EX{auction['auction_id']}-lot{lot_number}", lot_url=response.url,
        artist=clean_text(response.css("h1.artist::text").get()),
        title=clean_text(response.css("h2.title::text").get()),
        estimate_low=to_int(estimate.attrib.get("data-low")),
        estimate_high=to_int(estimate.attrib.get("data-high")),
        estimate_currency=estimate.attrib.get("data-currency"),
        is_closed=True, lot_status="sold" if sold else "unsold", is_sold=sold,
        sold_price=to_int(result.attrib.get("data-price")) if sold else None,
        sold_currency=result.attrib.get("data-currency") if sold else None,
        sold_price_source="lot_page" if sold else None,
        provenance=clean_text(response.css("div.provenance::text").get()),
        scraped_at=utc_now_iso(), source_version=source_version, parse_quality="detail",
    )


class ExampleHouseSpider(BaseAuctionSpider):
    name = "example_house_recent"
    source_name = "example_house"
    allowed_domains = ["auctions.example.com"]
    source_version = "recent"
    start_urls = ["https://auctions.example.com/sales"]

    def parse(self, response):                       # listing -> auctions
        for link in response.css("a.sale"):
            auction = {"auction_url": response.urljoin(link.attrib["href"]),
                       "auction_name": clean_text(link.css("::text").get()),
                       "auction_date": link.attrib.get("data-date"),
                       "auction_id": link.attrib["href"].rstrip("/").split("/")[-1]}
            if self.should_request_auction(auction):
                yield scrapy.Request(auction["auction_url"], callback=self.parse_auction,
                                     errback=self.request_failed, cb_kwargs={"auction": auction})

    def parse_auction(self, response, auction):     # auction -> lots
        self.record_auction_total(auction, to_int(response.css("body::attr(data-lot-count)").get()))
        for href in response.css("a.lot::attr(href)").getall():
            lot = AuctionLot(lot_url=response.urljoin(href))
            if self.should_request_lot(lot):
                yield scrapy.Request(lot.lot_url, callback=self.parse_lot, errback=self.request_failed,
                                     cb_kwargs={"auction": auction})

    def parse_lot(self, response, auction):         # lot page -> AuctionLot
        lot = parse_example_lot(response, auction, self.source_version)
        self.keep_earlier_tries(response, lot)
        yield from self.finish_lot(response, lot)


def page(url, body, **cb_kwargs):
    return HtmlResponse(url=url, body=body.encode(), encoding="utf-8",
                        request=Request(url, cb_kwargs=cb_kwargs))


def run_pipelines(items, spider, tmp_path):
    validation = ValidationPipeline()
    storage = StoragePipeline(tmp_path)
    storage.open_spider(spider)
    for item in items:
        storage.process_item(validation.process_item(item, spider), spider)
    storage.close_spider(spider)
    return storage.path


# ---------------------------------------------------------------- the tests
def test_new_source_crawls_with_the_shared_base(tmp_path):
    spider = ExampleHouseSpider(raw_dir=tmp_path)
    sales = list(spider.parse(page("https://auctions.example.com/sales", LISTING)))
    assert [r.url for r in sales] == ["https://auctions.example.com/sales/101",
                                      "https://auctions.example.com/sales/102"]
    assert spider.counts["auctions_found"] == 2

    auction = sales[0].cb_kwargs["auction"]
    lots = list(spider.parse_auction(page(sales[0].url, SALE_101), auction))
    assert len(lots) == 2 and spider.counts["lots_found"] == 2
    assert list(spider.parse_auction(page(sales[0].url, SALE_101), auction)) == []   # duplicates skipped
    assert spider.counts["lots_duplicate"] == 2

    [lot] = list(spider.parse_lot(page(lots[0].url, LOT_1), auction))
    assert isinstance(lot, AuctionLot)
    assert (lot.artist, lot.sold_price, lot.lot_id) == ("Jane Doe", 18900, "EX101-lot1")


def test_every_source_lands_in_the_same_columns(tmp_path):
    example = ExampleHouseSpider(raw_dir=tmp_path)
    auction = {"auction_url": "https://auctions.example.com/sales/101", "auction_name": "Modern Art Evening",
               "auction_date": "2026-10-01", "auction_id": "101"}
    example_lot = parse_example_lot(page("https://auctions.example.com/sales/101/lots/1", LOT_1),
                                    auction, "recent")
    sothebys = RecentAuctionsSpider(raw_dir=tmp_path)
    sothebys_lot = AuctionLot(lot_id="N09375-lot11", lot_status="hidden", source_version="recent")

    files = [run_pipelines([example_lot], example, tmp_path),
             run_pipelines([sothebys_lot], sothebys, tmp_path)]
    rows = []
    for path in files:
        with open(path, newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            assert reader.fieldnames == FIELDS          # identical header for every source
            rows.extend(reader)
    assert [row["source"] for row in rows] == ["example_house", "sothebys"]   # stamped by the pipeline


def test_validation_rejects_rows_outside_the_schema():
    spider = SimpleNamespace(source_name="example_house")
    with pytest.raises(DropItem, match="not an AuctionLot"):
        ValidationPipeline().process_item({"lot_id": "x"}, spider)
    with pytest.raises(DropItem, match="source"):
        ValidationPipeline().process_item(AuctionLot(lot_id="x", source="sothebys"), spider)


def test_a_source_spider_must_name_its_source():
    with pytest.raises(TypeError, match="source_name"):
        class NoSource(BaseAuctionSpider):   # noqa: F841
            name = "no_source"


def test_every_project_crawl_spider_uses_the_base():
    loader = SpiderLoader.from_settings(get_project_settings())
    crawlers = [name for name in loader.list() if name not in TOOLS]
    assert crawlers, "no spiders found"
    for name in crawlers:
        cls = loader.load(name)
        assert issubclass(cls, BaseAuctionSpider), f"{name} must inherit BaseAuctionSpider"
        assert cls.source_name, f"{name} has no source_name"
    assert RecentAuctionsSpider.source_name == HistoricalAuctionsSpider.source_name == "sothebys"


def test_old_csv_gets_the_source_column_filled(tmp_path):
    path = tmp_path / "recent_auctions_recent.csv"
    old_fields = [name for name in FIELDS if name != "source"]
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, old_fields)
        writer.writeheader()
        writer.writerow({"lot_id": "a", "lot_status": "sold"})
    assert migrate_header(path, {"source": "sothebys"}) is True
    with open(path, newline="", encoding="utf-8-sig") as handle:
        [row] = list(csv.DictReader(handle))
    assert row["source"] == "sothebys" and row["lot_id"] == "a"
