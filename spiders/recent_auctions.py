"""Recent auctions: results page -> auction page -> GraphQL lots (-> optional lot page).

    scrapy crawl recent_auctions -a days=30
    scrapy crawl recent_auctions -a days=30 -a details=1 -a max_auctions=3
    scrapy crawl recent_auctions -a days=90 -a f2=none     # no f2 filter
    scrapy crawl recent_auctions -a days=90 -a f2=auto     # discover and crawl every f2 value
    scrapy crawl recent_auctions -a days=30 -a recrawl_stored=1   # also auctions already in the csv
    scrapy crawl recent_auctions -a days=30 -a details=1 -a field_retries=4   # retry pages with empty sections

Empty sections: when a lot page leaves description / provenance / literature / exhibited /
catalogue_note empty, the page is fetched again up to `field_retries` times (default 4,
0 = off; choose the columns with -a retry_fields=a,b,c). Values found on any try are kept.
At the end a DATA COMPLETENESS table is printed from the csv (after validation).

Already stored = in the csv this run writes to (data/processed/<spider>_<source_version>.csv):
  * an auction whose lots are ALL in the csv is skipped before any request, and does not count
    toward max_auctions (so max_auctions=1 means "the first auction I do not have yet")
  * in a partly stored auction (a crash mid-way), only the missing lots are requested
  * "all" is checked against the lot total the site gave when the auction was first crawled, kept
    in <csv name>_auction_totals.json. An auction in an older csv with no recorded total counts as
    complete; use -a recrawl_stored=1 if you know one is not.
  * -s OVERWRITE=1 starts a fresh csv, so nothing counts as stored.
"""

import json

import scrapy

from config.settings import DEFAULT_F2, GRAPHQL_URL, LOT_QUERY, PAGE_SIZE
from parsers.graphql_parser import find_auction_id, find_lot_cards, parse_card, parse_connection
from parsers.html_parser import find_filter_values, parse_auction_cards, results_next_page
from parsers.lot_detail_parser import analyse_detail, merge_detail
from pipelines.storage import save_raw_response
from spiders.base import BaseAuctionSpider
from utils.logs import log_not_stored
from utils.results_url import build_results_url
from utils.retries import retry_request


class RecentAuctionsSpider(BaseAuctionSpider):
    """Sotheby's: results page -> auction page -> GraphQL lot list (-> lot page)."""

    name = "recent_auctions"
    source_name = "sothebys"
    allowed_domains = ["www.sothebys.com", "clientapi.prod.sothelabs.com"]
    source_version = "recent"

    def __init__(self, f2=DEFAULT_F2, results_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)     # dates, details, max_auctions, resume, retries: spiders/base.py
        f2_text = str(f2 or "").strip().lower()
        self.auto_f2 = f2_text == "auto"       # -a f2=auto -> discover the values from the page
        self.f2_discovered = False
        if self.auto_f2 or f2_text in ("", "none", "all"):
            f2 = None                          # first request has no f2 filter
        self.start_urls = [results_url or build_results_url(self.start_date, self.end_date, f2)]

    # ---- step 1: results page lists the auctions (and may have more pages)
    def parse(self, response):
        save_raw_response(self.raw_dir, response, "html", label="results")
        self.counts["results_pages"] += 1
        auctions = parse_auction_cards(response)
        self.logger.info("Results page: %d auctions (%s)", len(auctions), response.url)
        if not auctions:
            self.logger.warning("No auction cards found. Open the saved raw results_*.html: "
                                "if the auctions are not in it, the list is loaded by JavaScript.")
            log_not_stored(self, "results_page", "no_auction_cards", url=response.url,
                           detail="no auctions in this page: wrong dates/filter, or list loaded by JavaScript")

        # queue this page's auctions (stop at max_auctions)
        for auction in auctions:
            if self.limit_reached():
                break
            if not self.should_request_auction(auction):   # already seen / already stored
                continue
            yield scrapy.Request(auction["auction_url"], callback=self.route_auction(auction["auction_url"]),
                                 errback=self.request_failed, cb_kwargs={"auction": auction})

        # f2=auto: read every f2 value on the first page, then crawl each one
        if self.auto_f2 and not self.f2_discovered:
            self.f2_discovered = True
            if self.limit_reached():
                self.logger.info("max_auctions reached: not crawling the f2 filter pages")
            else:
                filters = find_filter_values(response)
                self.logger.info("Found %d f2 filter values", len(filters))
                for value in filters:
                    yield scrapy.Request(build_results_url(self.start_date, self.end_date, value),
                                         callback=self.parse, errback=self.request_failed)

        if not self.limit_reached():
            next_page = results_next_page(response)
            if next_page:
                self.logger.info("Results next page: %s", next_page)
                yield scrapy.Request(next_page, callback=self.parse, errback=self.request_failed)

    def route_auction(self, url):
        """Which callback handles this auction page (the historical spider adds a second kind)."""
        return self.parse_auction

    # ---- step 2: auction page -> auction id
    def parse_auction(self, response, auction):
        save_raw_response(self.raw_dir, response, "html", label="auction")
        name = auction.get("auction_name")
        raw = response.css("script#__NEXT_DATA__::text").get()
        if not raw:
            self.logger.warning("No __NEXT_DATA__ on %s", response.url)
            yield from self.soft_failure(response, "auction_page", "no_next_data",
                                         detail=f"auction '{name}': page has no __NEXT_DATA__ (blocked or layout changed)")
            return
        data = json.loads(raw)
        first = next(find_lot_cards(data), None)
        auction_id = ((first or {}).get("auction") or {}).get("auctionId")
        if not auction_id:
            # no lot cards in the page: look for the auction id anywhere in the data
            auction_id = find_auction_id(data)
            if auction_id:
                self.logger.info("No lot cards in the page, but found auction id %s: asking GraphQL anyway", auction_id)
        if not auction_id:
            page_props = (data.get("props") or {}).get("pageProps") or {}
            self.logger.warning("No lot cards / auction id on %s", response.url)
            self.counts["auctions_without_lots"] += 1
            log_not_stored(self, "auction_page", "no_lot_cards", url=response.url,
                           detail=f"auction '{name}': no lots and no auction id. pageProps keys: {list(page_props)[:12]}")
            return
        self.counts["auctions_with_lots"] += 1
        yield self.graphql_request(auction_id, 0, auction)

    # ---- step 3: all lots through GraphQL, PAGE_SIZE at a time
    def graphql_request(self, auction_id, offset, auction):
        payload = {
            "operationName": "LotCardsFilterByPaginated",
            "query": LOT_QUERY,
            "variables": {"id": auction_id, "filter": "ALL", "language": "ENGLISH",
                          "limit": PAGE_SIZE, "offset": offset},
        }
        return scrapy.Request(
            GRAPHQL_URL, method="POST", body=json.dumps(payload), dont_filter=True,
            headers={"Content-Type": "application/json", "Accept": "application/json",
                     "Origin": "https://www.sothebys.com", "Referer": "https://www.sothebys.com/"},
            callback=self.parse_graphql, errback=self.request_failed,
            cb_kwargs={"auction_id": auction_id, "offset": offset, "auction": auction},
        )

    def parse_graphql(self, response, auction_id, offset, auction):
        save_raw_response(self.raw_dir, response, "json", label=f"{auction_id}_offset{offset}")
        where = f"auction={auction_id} offset={offset}"
        try:
            page = parse_connection(response.json())
        except ValueError:
            self.logger.error("Invalid GraphQL JSON: auction=%s offset=%d", auction_id, offset)
            yield from self.soft_failure(response, "graphql", "invalid_json", detail=where)
            return
        if page["errors"]:
            self.logger.error("GraphQL errors: %s", page["errors"])
            if not page["cards"]:
                yield from self.soft_failure(response, "graphql", "graphql_errors",
                                             detail=f"{where} errors={page['errors']}")
                return
        cards, total = page["cards"], page["total"]
        self.logger.info("auction=%s offset=%d lots=%d total=%s", auction_id, offset, len(cards), total)
        if offset == 0:
            self.record_auction_total(auction, total)

        if offset == 0 and not cards and not page["errors"]:
            self.counts["auctions_graphql_empty"] += 1
            log_not_stored(self, "graphql", "auction_has_no_lots", url=auction.get("auction_url"),
                           detail=f"{where} totalCount={total}: the site itself returns no lots for this auction")
            return

        for card in cards:
            lot = parse_card(card, auction, self.source_version)
            if not self.should_request_lot(lot):   # duplicate, or already in the csv: no request
                continue
            if self.details and lot.lot_url:
                yield scrapy.Request(lot.lot_url, callback=self.parse_detail,
                                     errback=self.detail_failed, cb_kwargs={"lot": lot})
            else:
                yield lot

        has_next = page["has_next"] if page["has_next"] is not None else len(cards) >= PAGE_SIZE
        if has_next and cards:
            yield self.graphql_request(auction_id, offset + PAGE_SIZE, auction)
        elif total is not None and offset + len(cards) < total:
            self.logger.warning("Pagination ended at %d of %d lots (auction=%s)",
                                offset + len(cards), total, auction_id)
            log_not_stored(self, "pagination", "ended_early", url=response.url,
                           detail=f"auction={auction_id}: got {offset + len(cards)} of {total} lots")

    # ---- step 4 (optional): the lot's own page
    def parse_detail(self, response, lot):
        """Read the lot page. A page that looks incomplete is fetched again (SOFT_RETRY_TIMES).

        Retry only when the PAGE looks broken (no description markers at all), not whenever a
        field is blank: a lot with no literature is normal, and re-fetching it changes nothing.
        Withdrawn lots are not retried (their pages hold little).
        """
        save_raw_response(self.raw_dir, response, "html", label="lot")
        report = analyse_detail(response)
        tries = response.meta.get("retry_times", 0)

        if not report.complete and lot.lot_status != "withdrawn":
            retry = retry_request(self, response.request, "lot_page_incomplete")
            if retry is not None:
                self.counts["detail_retries"] += 1
                self.logger.warning("Lot page looks incomplete, trying again: %s", response.url)
                yield retry
                return
            lot.add_flag("detail_page_incomplete")
            self.counts["detail_incomplete_after_retry"] += 1
            log_not_stored(self, "lot_page", "page_incomplete", url=response.url, lot_id=lot.lot_id,
                           detail=f"lot saved with what the page gave after {tries} retries")
        elif report.complete and tries:
            self.counts["detail_recovered_by_retry"] += 1

        merge_detail(lot, response, report)
        self.keep_earlier_tries(response, lot)
        if not response.meta.get("field_tries"):
            for column, kind in report.gaps.items():
                self.counts[f"gap_{column}_{kind}"] += 1       # shown in the run summary
        for flag in report.flags:
            self.counts[f"fixed_or_flagged_{flag}"] += 1
        yield from self.finish_lot(response, lot)
