"""The shared base for every auction-house spider.

One web source = one spider class that inherits from BaseAuctionSpider. The base holds
everything that is the same for every site, so a new source only writes what is different:

    what the base gives every source              what each source writes itself
    ---------------------------------------       -------------------------------------
    date window, -a arguments                     source_name, name, allowed_domains
    resume: skip auctions/lots already in csv     start URLs (listing / results pages)
    max_auctions limit                            how to find auctions on a listing page
    soft retries + "why not stored" log           how to find lots in an auction
    re-fetch lot pages with empty sections        a parser: page -> AuctionLot
    stop the crawl when blocked (403 / 429)
    run summary + completeness table

Every source yields the same row: models.auction_lot.AuctionLot. The ValidationPipeline
stamps `source` from the spider and drops anything that is not an AuctionLot, so all
sources end up with the same columns and can be combined into one dataset.

See docs/ADDING_A_SOURCE.md for a step-by-step template.
"""

from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.spidermiddlewares.httperror import HttpError

from config.settings import RAW_DIR
from utils.completeness import print_csv_completeness
from utils.field_retry import (DEFAULT_FIELD_RETRIES, RETRY_FIELDS, blank_fields, field_retry_request,
                               restore, snapshot)
from utils.logs import log_not_stored
from utils.retries import retry_request
from utils.stored_data import StoredData

BLOCKED_STATUSES = (403, 429)


def _truthy(value):
    return str(value).lower() in ("1", "true", "yes")


class BaseAuctionSpider(scrapy.Spider):
    """Inherit from this for every auction house. Not crawlable on its own (it has no name)."""

    # --- each source MUST set these
    source_name = None          # short id written to every row's `source` column, e.g. "sothebys"
    # name = "..."              # Scrapy's spider name (unique), e.g. "christies_recent"
    # allowed_domains = [...]
    source_version = "run"      # second half of the csv name: data/processed/<name>_<source_version>.csv

    def __init_subclass__(cls, **kwargs):
        """A crawlable spider (one with a name) must say which source it is."""
        super().__init_subclass__(**kwargs)
        if getattr(cls, "name", None) and not getattr(cls, "source_name", None):
            raise TypeError(f"{cls.__name__} must set source_name (e.g. source_name = 'christies')")

    def __init__(self, days=30, start_date=None, end_date=None, details="0", max_auctions=None,
                 source_version=None, raw_dir=None, recrawl_stored="0", processed_dir=None,
                 field_retries=None, retry_fields=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.end_date = date.fromisoformat(end_date) if end_date else date.today()
        self.start_date = (date.fromisoformat(start_date) if start_date
                           else self.end_date - timedelta(days=int(days)))
        if self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        if source_version:
            self.source_version = source_version
        self.details = _truthy(details)
        self.max_auctions = int(max_auctions) if max_auctions else None
        self.raw_dir = Path(raw_dir) if raw_dir else RAW_DIR / self.name
        self.recrawl_stored = _truthy(recrawl_stored)
        self.processed_dir = Path(processed_dir) if processed_dir else None
        self._stored = None          # loaded on first use (see stored())
        self.field_retries = (int(field_retries) if field_retries not in (None, "")
                              else DEFAULT_FIELD_RETRIES)
        self.retry_fields = (tuple(name.strip() for name in retry_fields.split(",") if name.strip())
                             if retry_fields else RETRY_FIELDS)

        self.seen = set()            # lot ids / lot urls already requested
        self.auctions_seen = set()   # auction urls already requested
        self.counts = Counter()      # run summary, logged when the spider closes

    # ------------------------------------------------ what the csv already has
    def csv_path(self):
        """The csv this run writes to: data/processed/<spider>_<source_version>.csv"""
        folder = self.processed_dir
        if folder is None:
            crawler = getattr(self, "crawler", None)
            if crawler is None:
                return None              # unit tests without a crawler: nothing counts as stored
            folder = Path(crawler.settings.get("PROCESSED_DIR"))
        return folder / f"{self.name}_{self.source_version}.csv"

    def stored(self):
        """StoredData for this run's csv, read once."""
        if self._stored is None:
            crawler = getattr(self, "crawler", None)
            fresh = bool(crawler and crawler.settings.getbool("OVERWRITE"))
            self._stored = StoredData(self.csv_path(), enabled=not (fresh or self.recrawl_stored))
        return self._stored

    def limit_reached(self):
        """True once max_auctions auctions have been requested (the limit is for the whole run)."""
        return bool(self.max_auctions) and len(self.auctions_seen) >= self.max_auctions

    # ------------------------------------------------ deciding what to request
    def should_request_auction(self, auction):
        """True if this auction (a dict with at least `auction_url`) should be crawled now.

        Skips auctions already requested in this run and auctions whose lots are all in the
        csv already (these do not count toward max_auctions). Marks the auction as seen.
        """
        url = auction["auction_url"]
        if self.limit_reached() or url in self.auctions_seen:
            return False
        if not self.recrawl_stored and self.stored().auction_complete(url):
            self.counts["auctions_skipped_already_stored"] += 1
            self.logger.info("Already stored, skipped: %s", auction.get("auction_name") or url)
            return False
        self.auctions_seen.add(url)
        self.counts["auctions_found"] += 1
        return True

    def record_auction_total(self, auction, total):
        """Remember how many lots the SITE says an auction has, so a partly stored auction
        is finished on the next run and the lot count can be checked."""
        self.stored().record_total(auction.get("auction_url"), total)

    def should_request_lot(self, lot):
        """True if this lot is new: not seen in this run and not in the csv. Marks it as seen."""
        key = lot.lot_id or lot.lot_url
        if key in self.seen:
            self.counts["lots_duplicate"] += 1
            return False
        self.seen.add(key)
        if not self.recrawl_stored and self.stored().has_lot(lot.lot_id):
            self.counts["lots_skipped_already_stored"] += 1
            return False
        self.counts["lots_found"] += 1
        return True

    # ------------------------------------------------ page loaded but had no usable data
    def soft_failure(self, response, stage, reason, detail=None):
        """Retry the request; when out of tries, record WHY nothing was stored.

        Use as:  yield from self.soft_failure(...)
        """
        retry = retry_request(self, response.request, reason)
        if retry is not None:
            self.counts["soft_retries"] += 1
            self.logger.warning("Retrying (%s): %s", reason, response.url)
            return [retry]
        self.counts[f"lost_{reason}"] += 1
        log_not_stored(self, stage, reason, url=response.url, detail=detail)
        return []

    # ------------------------------------------------ lot page: retry while sections are empty
    def finish_lot(self, response, lot):
        """Yield a retry of this page if sections are still empty, else the lot.

        Use as:  yield from self.finish_lot(response, lot)
        """
        missing = blank_fields(lot, self.retry_fields)
        tries = response.meta.get("field_tries", 0)
        first_missing = response.meta.get("field_first_missing", missing)
        page_broken = "detail_page_incomplete" in (lot.data_quality_flags or "").split(";")
        can_retry = getattr(self, "crawler", None) is not None   # unit tests: never retry
        if (missing and can_retry and not page_broken and tries < self.field_retries
                and lot.lot_status != "withdrawn"):
            self.counts["field_retries"] += 1
            self.logger.info("Lot %s: empty %s, fetching the page again (%d/%d)",
                             lot.lot_id or lot.lot_url, ",".join(missing), tries + 1, self.field_retries)
            yield field_retry_request(response.request, tries, snapshot(lot, self.retry_fields), first_missing)
            return
        for name in first_missing:
            if name not in missing:
                self.counts[f"filled_by_retry_{name}"] += 1     # empty at first, found on a retry
        for name in missing:
            self.counts[f"absent_{name}"] += 1                  # still empty after every try
        yield lot

    def keep_earlier_tries(self, response, lot):
        """Put back values that an earlier try of this page found (see finish_lot)."""
        restore(lot, response.meta.get("field_saved"))

    # ------------------------------------------------ failures
    def detail_failed(self, failure):
        """Never lose a lot because its page failed: save it with what we already have."""
        lot = failure.request.cb_kwargs["lot"]
        lot.add_flag("detail_request_failed")
        self.logger.warning("Lot page failed: %s", failure.request.url)
        log_not_stored(self, "lot_page", "detail_request_failed", url=failure.request.url, lot_id=lot.lot_id,
                       detail=f"lot saved WITHOUT its page data: {failure.value!r}")
        yield lot

    def request_failed(self, failure):
        """Record why a request is lost. Stop the whole crawl if the site blocks or rate-limits us."""
        url = failure.request.url
        if failure.check(HttpError):
            status = failure.value.response.status
            log_not_stored(self, "request_failed", f"http_{status}", url=url,
                           detail=f"after {failure.request.meta.get('retry_times', 0)} retries")
            if status in BLOCKED_STATUSES:
                raise CloseSpider(f"access_denied_{status}")
        else:
            log_not_stored(self, "request_failed", type(failure.value).__name__, url=url,
                           detail=repr(failure.value))
        self.logger.error("Request failed: %s (%r)", url, failure.value)

    # ------------------------------------------------ end of run
    def closed(self, reason):
        """Scrapy calls this at the end: run summary + completeness of the csv (after validation)."""
        self.logger.info("Run summary (%s, source=%s): %s", reason, self.source_name, dict(self.counts))
        absent = {key[len("absent_"):]: value for key, value in self.counts.items() if key.startswith("absent_")}
        try:
            print_csv_completeness(self.csv_path(), absent)
        except Exception as error:          # the table must never break the end of a crawl
            self.logger.warning("Could not print the completeness table: %r", error)
