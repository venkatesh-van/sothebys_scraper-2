# Adding a new web source

Every auction house is one spider on top of `spiders/base.py` plus one parser. All of them
yield the same row, `models.auction_lot.AuctionLot`, so every csv has the same columns and the
ML team can stack them into one dataset.

## What you get for free from `BaseAuctionSpider`

| Shared (written once, in `spiders/base.py`) | Method / attribute |
|---|---|
| Date window and the usual `-a` arguments (`days`, `start_date`, `end_date`, `details`, `max_auctions`, `recrawl_stored`, `field_retries`, ...) | `__init__` |
| Resume: skip auctions whose lots are all in the csv, and lots already stored | `should_request_auction`, `should_request_lot` |
| Remember the site's lot total per auction (lot-count check, finish partly stored auctions) | `record_auction_total` |
| `max_auctions` limit | `limit_reached` |
| Retry a page that loaded but had no data, then log why nothing was stored | `soft_failure` |
| Re-fetch a lot page while description / provenance / literature / ... are empty | `finish_lot`, `keep_earlier_tries` |
| Never lose a lot when its page fails | `detail_failed` |
| Stop the crawl on 403 / 429 | `request_failed` |
| Run summary and completeness table at the end | `closed` |

Validation, price rules, CSV storage, the coverage report and the not-stored log are pipelines
and extensions, so they already run for every spider.

## What each source writes

1. **A parser**: `parsers/<source>.py`. Pure functions, no network: response in, `AuctionLot` out.
   Fill every column you can; leave the rest `None` (never guess). Set `parse_quality` and add
   `data_quality_flags` where a value is doubtful.
2. **A spider**: `spiders/<source>.py`

```python
import scrapy

from models.auction_lot import AuctionLot
from parsers.christies import parse_auction_list, parse_lot_links, parse_lot   # your parser
from spiders.base import BaseAuctionSpider


class ChristiesRecentSpider(BaseAuctionSpider):
    name = "christies_recent"            # unique; csv = data/processed/christies_recent_recent.csv
    source_name = "christies"            # written to every row's `source` column (required)
    source_version = "recent"
    allowed_domains = ["www.christies.com"]

    def start_requests(self):
        yield scrapy.Request(self.results_url(self.start_date, self.end_date),
                             callback=self.parse, errback=self.request_failed)

    def parse(self, response):                               # listing page -> auctions
        for auction in parse_auction_list(response):         # dicts with at least auction_url
            if self.should_request_auction(auction):
                yield scrapy.Request(auction["auction_url"], callback=self.parse_auction,
                                     errback=self.request_failed, cb_kwargs={"auction": auction})

    def parse_auction(self, response, auction):              # auction page -> lots
        links, total = parse_lot_links(response)
        self.record_auction_total(auction, total)            # the site's own lot count
        for url in links:
            if self.should_request_lot(AuctionLot(lot_url=url)):
                yield scrapy.Request(url, callback=self.parse_lot, errback=self.request_failed,
                                     cb_kwargs={"auction": auction})

    def parse_lot(self, response, auction):                  # lot page -> AuctionLot
        lot = parse_lot(response, auction, self.source_version)
        self.keep_earlier_tries(response, lot)
        yield from self.finish_lot(response, lot)            # retries empty sections, then yields
```

3. **Fixtures and tests**: save a few real pages under `tests/fixtures/<source>/` and test the
   parser on them. `tests/test_multi_source.py` shows a complete example source ("Example House")
   and checks that its rows land in the same columns as Sotheby's.
4. **Run it**: `scrapy crawl christies_recent -a days=30 -a max_auctions=1`, then
   `python gap_report.py` and `python validate_prices.py` on its csv.

## Rules that keep the schema the same

- Yield `AuctionLot` only. The `ValidationPipeline` drops anything else (`dropped_wrong_schema`).
- Do not set `source` in the parser: the pipeline stamps it from the spider's `source_name`.
  A row that claims a different source is dropped.
- A spider with a `name` but no `source_name` fails at import time.
- `tests/test_multi_source.py::test_every_project_crawl_spider_uses_the_base` fails if a new
  crawl spider does not inherit `BaseAuctionSpider`.
- A new column goes into `AuctionLot` (for every source). Old csvs get it added, empty, on the
  next run (`pipelines/storage.migrate_header`).
- Within one source `lot_id` is unique. Across sources, use (`source`, `lot_id`) as the key.
