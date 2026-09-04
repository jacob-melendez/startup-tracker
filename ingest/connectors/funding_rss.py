"""Funding-news RSS connector (SPEC §4 Tier 2 #5).

TechCrunch, Business Wire and friends publish funding headlines as ordinary RSS. The spec is
explicit about how far to trust them: **"Use as a signal to *enrich* an existing record, never
as a primary source."** So every record this connector emits is ``enrich_only`` — if entity
resolution cannot find the company, the pipeline writes nothing at all rather than inventing a
company out of a headline (see :mod:`ingest.pipeline` and design decision 4 in
``ingest/normalize.py``, which is what lets a headline with no address resolve by name at all).

The feed list is configuration, not code (``config/connectors.yaml``: ``options.feeds``), and
so is the vocabulary: whether a headline is a funding story and which ``RoundType`` it names
are decided by ``config/classifiers.yaml``'s ``funding`` block (CLAUDE.md: "no keywords in
code"). Only the *amount* is parsed here, because a number is not a keyword.

What one item yields: the company name lifted from the headline, one
:class:`~ingest.base.FundingRoundRecord` with the round type, the amount and the item as
``raw_payload``, and nothing else — no locations, no jobs, no contacts. ``announced_date`` is
the item's publication date.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import ClassVar

import httpx
from pydantic import BaseModel, ConfigDict, Field

from db import enums
from ingest.base import CompanyRecord, Connector, FetchContext, FundingRoundRecord
from ingest.classify import classify_round_type, is_funding_announcement
from ingest.config import ConnectorConfig, RegionsConfig
from ingest.htmlutil import html_to_text
from ingest.http import RobotsDisallowed
from logging_config import get_logger

log = get_logger(__name__)

#: Multipliers for the magnitude words a headline uses.
_MAGNITUDES: dict[str, int] = {
    "k": 1_000,
    "m": 1_000_000,
    "mm": 1_000_000,
    "b": 1_000_000_000,
    "bn": 1_000_000_000,
    "t": 1_000_000_000_000,
    "thousand": 1_000,
    "million": 1_000_000,
    "billion": 1_000_000_000,
    "trillion": 1_000_000_000_000,
}
_AMOUNT = re.compile(
    r"\$\s*(?P<value>\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)\s*"
    r"(?P<magnitude>k|mm|m|bn|b|t|thousand|million|billion|trillion)?\b",
    re.IGNORECASE,
)
#: The headline shapes these feeds use: "Acme raises $12M…", "Acme, an AI startup, lands…".
#: The company is whatever precedes the announcement verb, minus any parenthetical or clause.
_HEADLINE_SPLIT = re.compile(
    r"\b(raises|raised|raising|secures|secured|lands|landed|closes|closed|nabs|banks|bags|"
    r"picks up|snags|scores|announces|announced|has raised|is raising)\b",
    re.IGNORECASE,
)
_TRAILING_CLAUSE = re.compile(r"\s*[,(—–-]\s*.*$")
_LEADING_NOISE = re.compile(r"^(?:exclusive|breaking|report|scoop)\s*[:—–-]\s*", re.IGNORECASE)
_WHITESPACE = re.compile(r"\s+")
#: An RSS/Atom document nests items under different names; these are the ones that matter.
_ITEM_TAGS = ("item", "{http://www.w3.org/2005/Atom}entry")
_ATOM = "{http://www.w3.org/2005/Atom}"
#: A company name lifted from a headline is a few words at most; anything longer is a clause.
MAX_NAME_WORDS = 6


class FundingRssOptions(BaseModel):
    """``options`` for ``funding_rss`` in ``config/connectors.yaml``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: The feeds to read. Add any RSS or Atom funding feed here — this is the only place a feed
    #: URL is written (SPEC §4 Tier 2 names TechCrunch, Axios Pro Rata and Business Wire; Axios
    #: answers 403 to non-browser clients, and Business Wire's ids are per-subject rather than
    #: per-topic, so the shipped defaults are the two TechCrunch feeds that work).
    feeds: tuple[str, ...] = (
        "https://techcrunch.com/category/venture/feed/",
        "https://techcrunch.com/category/startups/feed/",
    )
    #: Items older than this are ignored: the point of a daily feed is what is new, and a
    #: reappearing item would keep re-dating the same round.
    max_age_days: int = Field(default=30, ge=1)
    #: Items read per feed, newest first.
    max_items_per_feed: int = Field(default=100, ge=1)


# --------------------------------------------------------------------------------- raw item


@dataclass(frozen=True, slots=True)
class FeedItem:
    """One RSS/Atom entry, already flattened."""

    feed_url: str
    title: str
    link: str | None
    summary: str | None
    published_at: datetime | None
    guid: str | None

    @property
    def text(self) -> str:
        return f"{self.title}\n{self.summary or ''}"


# --------------------------------------------------------------------------------- parsing


def parse_amount(text: str) -> int | None:
    """The first dollar amount in ``text`` as whole dollars, or ``None``.

    ``"$3B"`` → 3 000 000 000, ``"$12.5 million"`` → 12 500 000, ``"$750,000"`` → 750 000. A
    bare ``"$30"`` with no magnitude word is taken literally, which is right for the rare
    "raises $30" typo and harmless otherwise. Valuations are *not* excluded here — a headline
    naming both ("raises $3B at a $30B valuation") is read left to right, and the raise is
    written first in every feed these connectors read.
    """
    match = _AMOUNT.search(text)
    if match is None:
        return None
    try:
        value = float(match.group("value").replace(",", ""))
    except ValueError:  # pragma: no cover - the pattern only matches numerals
        return None
    magnitude = (match.group("magnitude") or "").casefold()
    amount = value * _MAGNITUDES.get(magnitude, 1)
    return int(amount) if amount >= 0 else None


def parse_company_name(title: str) -> str | None:
    """The company a funding headline is about: whatever precedes its announcement verb.

    "Crusoe reportedly raises $3B at a $30B valuation" → "Crusoe". Adverbs the outlets insert
    ("reportedly", "quietly") are dropped, as is any clause after a comma or dash, so
    "Acme, an AI startup, raises…" → "Acme". Returns ``None`` when the headline has no
    announcement verb — the caller has already checked that it is a funding story, so this only
    happens for a headline phrased in some other way, and guessing would be worse.
    """
    cleaned = _LEADING_NOISE.sub("", _WHITESPACE.sub(" ", title)).strip()
    parts = _HEADLINE_SPLIT.split(cleaned, maxsplit=1)
    if len(parts) < 2:
        return None
    head = _TRAILING_CLAUSE.sub("", parts[0]).strip()
    # "Crusoe reportedly" / "Acme officially" — trim a trailing adverb, never a real word.
    head = re.sub(r"\s+\w+ly$", "", head).strip(" .,:;")
    if not head or len(head.split()) > MAX_NAME_WORDS:
        # A whole clause, not a name: "Fiat Ventures combines venture and advisory divisions
        # into new brand as it raises $35M". Such a name would resolve to nothing anyway
        # (every record here is ``enrich_only``); returning None says so honestly.
        return None
    return head


def _text_of(node: ET.Element | None) -> str | None:
    if node is None:
        return None
    value = "".join(node.itertext()).strip()
    return value or None


def _published(node: ET.Element) -> datetime | None:
    """``pubDate`` (RFC 2822, RSS) or ``updated``/``published`` (ISO 8601, Atom), as UTC."""
    raw = _text_of(node.find("pubDate")) or _text_of(node.find("{*}pubDate"))
    if raw:
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            parsed = None
        if parsed is not None:
            return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    for tag in (f"{_ATOM}published", f"{_ATOM}updated", "published", "updated", "date"):
        raw = _text_of(node.find(tag))
        if not raw:
            continue
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def _link(node: ET.Element) -> str | None:
    direct = _text_of(node.find("link"))
    if direct:
        return direct
    for candidate in node.findall(f"{_ATOM}link"):
        href = candidate.get("href")
        if href and candidate.get("rel", "alternate") == "alternate":
            return href
    return None


def parse_feed(xml: str, feed_url: str) -> list[FeedItem]:
    """Every entry of an RSS 2.0 or Atom document, in document order.

    ``xml.etree`` rather than a feed library: the four fields this connector needs are in the
    same two shapes in every feed, and SPEC §3's dependency list has no feed parser in it.
    A document that will not parse raises ``ET.ParseError`` for the caller to record.
    """
    root = ET.fromstring(xml)
    nodes: list[ET.Element] = []
    for tag in _ITEM_TAGS:
        nodes.extend(root.iter(tag))
    items: list[FeedItem] = []
    for node in nodes:
        title = _text_of(node.find("title")) or _text_of(node.find(f"{_ATOM}title"))
        if not title:
            continue
        summary = (
            _text_of(node.find("description"))
            or _text_of(node.find(f"{_ATOM}summary"))
            or _text_of(node.find(f"{_ATOM}content"))
        )
        items.append(
            FeedItem(
                feed_url=feed_url,
                title=_WHITESPACE.sub(" ", title).strip(),
                link=_link(node),
                summary=html_to_text(summary, limit=2000),
                published_at=_published(node),
                guid=_text_of(node.find("guid")) or _text_of(node.find(f"{_ATOM}id")),
            )
        )
    return items


# ------------------------------------------------------------------------------- connector


class FundingRssConnector(Connector[FeedItem]):
    """Funding-news RSS (SPEC §4 Tier 2 #5, §12 Phase 3). Enriches; never creates."""

    name: ClassVar[str] = "funding_rss"

    def __init__(self, config: ConnectorConfig, regions: RegionsConfig) -> None:
        super().__init__(config, regions)
        self.options = FundingRssOptions.model_validate(config.options)
        self.skipped: dict[str, int] = {}

    # ------------------------------------------------------------------ fetch

    async def fetch(self, ctx: FetchContext) -> AsyncIterator[FeedItem]:
        """Read every configured feed and yield its recent entries.

        A feed that fails is recorded in ``ctx.problems`` and the run continues with the next
        one: one outlet's outage must not cost the others their day.
        """
        self.skipped = {}
        cutoff = self._cutoff(ctx)
        ctx.log.info("funding_rss.start", feeds=len(self.options.feeds), cutoff=cutoff.isoformat())
        total = 0
        for feed_url in self.options.feeds:
            try:
                xml = await ctx.http.get_text(feed_url)
            except (httpx.HTTPStatusError, httpx.TransportError, RobotsDisallowed) as exc:
                ctx.problems.append(f"{feed_url} could not be fetched: {exc}")
                ctx.log.warning("funding_rss.feed_failed", url=feed_url, error=str(exc))
                continue
            try:
                items = parse_feed(xml, feed_url)
            except ET.ParseError as exc:
                ctx.problems.append(f"{feed_url} is not valid XML: {exc}")
                ctx.log.warning("funding_rss.feed_unparsable", url=feed_url, error=str(exc))
                continue
            kept = 0
            for item in items[: self.options.max_items_per_feed]:
                if item.published_at is not None and item.published_at < cutoff:
                    self.skipped["too_old"] = self.skipped.get("too_old", 0) + 1
                    continue
                kept += 1
                total += 1
                yield item
            ctx.log.debug("funding_rss.feed", url=feed_url, items=len(items), kept=kept)
        ctx.log.info(
            "funding_rss.summary",
            feeds=len(self.options.feeds),
            yielded=total,
            skipped=dict(self.skipped),
            requests=ctx.http.stats.requests,
        )

    def _cutoff(self, ctx: FetchContext) -> datetime:
        """``--since`` when given, else ``max_age_days`` before now (SPEC §14.3)."""
        if ctx.since is not None:
            return ctx.since
        return ctx.now - timedelta(days=self.options.max_age_days)

    # ------------------------------------------------------------------ to_records

    def to_records(self, raw: FeedItem) -> Iterable[CompanyRecord]:
        """One ``enrich_only`` record per funding headline, or nothing.

        Dropped with a counter: an item the ``funding.announcement`` vocabulary does not read
        as a funding story, and one whose company name cannot be lifted from the headline.
        """
        keyword = is_funding_announcement(raw.title)
        if keyword is None:
            return self._skip("not_a_funding_story", raw)
        name = parse_company_name(raw.title)
        if not name or len(name) > 120:
            return self._skip("no_company_name", raw)

        round_type, round_keyword = classify_round_type(raw.text)
        amount = parse_amount(raw.title) or parse_amount(raw.summary or "")
        announced = (raw.published_at or datetime.now(UTC)).date()
        log.info(
            "funding_rss.item",
            company=name,
            round_type=round_type.value,
            amount_usd=amount,
            matched=keyword,
            round_keyword=round_keyword,
        )
        return [
            CompanyRecord(
                name=name,
                source_url=raw.link,
                funding_rounds=(self._round(raw, round_type, amount, announced),),
                # SPEC §4 Tier 2 #5: a signal to enrich, never a primary source.
                enrich_only=True,
            )
        ]

    def _round(
        self,
        raw: FeedItem,
        round_type: enums.RoundType,
        amount: int | None,
        announced: date,
    ) -> FundingRoundRecord:
        return FundingRoundRecord(
            # The feed's own item id, so a headline re-syndicated tomorrow updates the round
            # instead of adding a second one (``FundingRoundRecord``).
            external_id=raw.guid or raw.link,
            round_type=round_type,
            amount_usd=amount,
            announced_date=announced,
            raw_payload={
                "feed_url": raw.feed_url,
                "title": raw.title,
                "link": raw.link,
                "summary": raw.summary,
                "guid": raw.guid,
                "published_at": raw.published_at.isoformat() if raw.published_at else None,
            },
            notes=f"Press mention: {raw.title}",
            source_url=raw.link,
        )

    def _skip(self, reason: str, raw: FeedItem, **details: object) -> tuple[CompanyRecord, ...]:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1
        log.debug("funding_rss.skip", reason=reason, title=raw.title, **details)
        return ()
