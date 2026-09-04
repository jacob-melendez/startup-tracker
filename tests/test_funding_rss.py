"""``ingest.connectors.funding_rss`` on recorded feeds (SPEC §4 Tier 2 #5, §12 Phase 3).

The rule this connector exists under: a funding headline is "a signal to enrich an existing
record, never as a primary source", so every record it emits is ``enrich_only`` and the
pipeline writes nothing at all when the company is unknown.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from db import enums
from ingest.base import CompanyRecord
from ingest.config import RegionsConfig, load_regions_config
from ingest.connectors.funding_rss import (
    FeedItem,
    FundingRssConnector,
    parse_amount,
    parse_company_name,
    parse_feed,
)
from ingest.http import HttpClient
from tests.support import (
    NOW,
    FakeClock,
    collect,
    connector_config,
    fixture_text,
    make_client,
    make_ctx,
)

VENTURE_FEED = "https://techcrunch.com/category/venture/feed/"
STARTUPS_FEED = "https://techcrunch.com/category/startups/feed/"


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


@pytest.fixture(scope="module")
def regions() -> RegionsConfig:
    return load_regions_config()


@pytest.fixture
def connector(regions: RegionsConfig) -> FundingRssConnector:
    return FundingRssConnector(connector_config("funding_rss"), regions)


@pytest.fixture
async def client(clock: FakeClock) -> AsyncIterator[HttpClient]:
    async with make_client(connector_config("funding_rss"), clock) as http:
        yield http


def mock_feeds(router: respx.MockRouter) -> None:
    router.get("https://techcrunch.com/robots.txt").mock(
        httpx.Response(200, text=fixture_text("funding_rss", "robots.txt"))
    )
    router.get(VENTURE_FEED).mock(
        httpx.Response(200, text=fixture_text("funding_rss", "techcrunch_venture.xml"))
    )
    router.get(STARTUPS_FEED).mock(
        httpx.Response(200, text=fixture_text("funding_rss", "techcrunch_startups.xml"))
    )


def item(title: str, **kwargs: object) -> FeedItem:
    return FeedItem(
        feed_url=VENTURE_FEED,
        title=title,
        link=str(kwargs.get("link") or "https://example.com/story"),
        summary=kwargs.get("summary"),  # type: ignore[arg-type]
        published_at=kwargs.get("published_at", NOW),  # type: ignore[arg-type]
        guid=str(kwargs.get("guid") or "guid-1"),
    )


def records(connector: FundingRssConnector, feed_item: FeedItem) -> list[CompanyRecord]:
    return list(connector.to_records(feed_item))


# ------------------------------------------------------------------ feed parsing


def test_a_real_rss_feed_parses() -> None:
    items = parse_feed(fixture_text("funding_rss", "techcrunch_venture.xml"), VENTURE_FEED)
    assert len(items) == 10
    first = items[0]
    # The feed's own double space is collapsed by the parser.
    assert first.title == "Crusoe reportedly raises $3B at a $30B valuation"
    assert first.link.startswith("https://techcrunch.com/") if first.link else False
    assert first.published_at == datetime(2026, 9, 4, 0, 48, 42, tzinfo=UTC)
    assert first.guid
    assert first.summary and "<" not in first.summary  # CDATA HTML became text


def test_an_atom_feed_parses() -> None:
    atom = """<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <entry>
        <title>Acme raises $12M Series A</title>
        <link rel="alternate" href="https://example.com/acme"/>
        <id>tag:example.com,2026:1</id>
        <published>2026-09-01T10:00:00Z</published>
        <summary>Acme, a Bay Area startup, raised a Series A.</summary>
      </entry>
    </feed>"""
    items = parse_feed(atom, "https://example.com/feed")
    assert len(items) == 1
    assert items[0].link == "https://example.com/acme"
    assert items[0].published_at == datetime(2026, 9, 1, 10, tzinfo=UTC)


def test_a_feed_with_no_items_parses_to_nothing() -> None:
    assert parse_feed("<rss><channel><title>Empty</title></channel></rss>", "u") == []


# ------------------------------------------------------------------ amounts and names


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("raises $3B", 3_000_000_000),
        ("raises $12.5 million", 12_500_000),
        ("lands $750,000", 750_000),
        ("secures $9.5M", 9_500_000),
        ("raises $70k", 70_000),
        ("raises $1.1bn", 1_100_000_000),
        ("no money here", None),
    ],
)
def test_parse_amount(text: str, expected: int | None) -> None:
    assert parse_amount(text) == expected


@pytest.mark.parametrize(
    ("headline", "expected"),
    [
        ("Crusoe reportedly raises $3B at a $30B valuation", "Crusoe"),
        ("Acme, an AI startup, raises $12M", "Acme"),
        ("Exclusive: Foo Bar secures $4M seed", "Foo Bar"),
        ("Acme quietly closes a Series B", "Acme"),
        ("How Sweden built a startup ecosystem", None),
        # A whole clause is not a name — seen live on the TechCrunch venture feed.
        (
            "Fiat Ventures combines venture and advisory divisions into a new brand "
            "as it raises $35M",
            None,
        ),
    ],
)
def test_parse_company_name(headline: str, expected: str | None) -> None:
    assert parse_company_name(headline) == expected


# ------------------------------------------------------------------ fetch


async def test_every_configured_feed_is_read(
    router: respx.MockRouter, connector: FundingRssConnector, client: HttpClient
) -> None:
    mock_feeds(router)
    items = await collect(connector, make_ctx(client, now=datetime(2026, 9, 4, 12, tzinfo=UTC)))
    assert len(items) == 20
    assert {item.feed_url for item in items} == {VENTURE_FEED, STARTUPS_FEED}


async def test_a_broken_feed_does_not_cost_the_others_their_day(
    router: respx.MockRouter, connector: FundingRssConnector, client: HttpClient
) -> None:
    router.get("https://techcrunch.com/robots.txt").mock(
        httpx.Response(200, text=fixture_text("funding_rss", "robots.txt"))
    )
    router.get(VENTURE_FEED).mock(httpx.Response(503))
    router.get(STARTUPS_FEED).mock(
        httpx.Response(200, text=fixture_text("funding_rss", "techcrunch_startups.xml"))
    )
    ctx = make_ctx(client)
    items = await collect(connector, ctx)
    assert len(items) == 10
    assert any("could not be fetched" in problem for problem in ctx.problems)


async def test_a_feed_that_is_not_xml_is_reported(
    router: respx.MockRouter, connector: FundingRssConnector, client: HttpClient
) -> None:
    router.get("https://techcrunch.com/robots.txt").mock(
        httpx.Response(200, text=fixture_text("funding_rss", "robots.txt"))
    )
    router.get(VENTURE_FEED).mock(httpx.Response(200, text="<not xml"))
    router.get(STARTUPS_FEED).mock(httpx.Response(200, text="<rss><channel/></rss>"))
    ctx = make_ctx(client)
    assert await collect(connector, ctx) == []
    assert any("is not valid XML" in problem for problem in ctx.problems)


async def test_items_older_than_the_window_are_dropped(
    router: respx.MockRouter, connector: FundingRssConnector, client: HttpClient
) -> None:
    mock_feeds(router)
    ctx = make_ctx(client, now=NOW + timedelta(days=400))
    assert await collect(connector, ctx) == []
    assert connector.skipped["too_old"] == 20


async def test_since_overrides_the_window(
    router: respx.MockRouter, connector: FundingRssConnector, client: HttpClient
) -> None:
    """SPEC §14.3: an explicit ``--since`` beats the connector's own incremental logic."""
    mock_feeds(router)
    ctx = make_ctx(client, now=NOW, since=datetime(2026, 9, 3, tzinfo=UTC))
    items = await collect(connector, ctx)
    assert items and all(
        item.published_at is None or (ctx.since is not None and item.published_at >= ctx.since)
        for item in items
    )
    assert len(items) < 20


# ------------------------------------------------------------------ to_records


def test_a_funding_headline_becomes_an_enrich_only_record(
    connector: FundingRssConnector,
) -> None:
    feed_item = item(
        "Crusoe reportedly raises $3B at a $30B valuation",
        published_at=datetime(2026, 9, 4, tzinfo=UTC),
    )
    record = records(connector, feed_item)[0]
    assert record.name == "Crusoe"
    # SPEC §4 Tier 2 #5: a headline may enrich a company, never create one.
    assert record.enrich_only is True
    assert record.locations == () and record.jobs == ()
    assert len(record.funding_rounds) == 1
    round_record = record.funding_rounds[0]
    assert round_record.amount_usd == 3_000_000_000
    assert round_record.announced_date == datetime(2026, 9, 4, tzinfo=UTC).date()
    assert round_record.external_id == "guid-1"
    assert round_record.raw_payload["title"].startswith("Crusoe")


def test_the_round_type_comes_from_the_headline(connector: FundingRssConnector) -> None:
    record = records(connector, item("Acme lands $12M Series B led by Accel"))[0]
    assert record.funding_rounds[0].round_type is enums.RoundType.SERIES_B


def test_a_headline_that_is_not_about_funding_is_skipped(
    connector: FundingRssConnector,
) -> None:
    assert records(connector, item("How Sweden built a startup ecosystem")) == []
    assert connector.skipped == {"not_a_funding_story": 1}


def test_a_funding_headline_with_no_extractable_name_is_skipped(
    connector: FundingRssConnector,
) -> None:
    assert records(connector, item("Startup funding round sizes are growing")) == []
    assert connector.skipped == {"no_company_name": 1}


def test_the_recorded_feeds_yield_only_funding_stories(
    connector: FundingRssConnector,
) -> None:
    """Twenty real headlines in, two funding rounds out — and every skip is counted."""
    items = parse_feed(
        fixture_text("funding_rss", "techcrunch_venture.xml"), VENTURE_FEED
    ) + parse_feed(fixture_text("funding_rss", "techcrunch_startups.xml"), STARTUPS_FEED)
    produced = [record for feed_item in items for record in connector.to_records(feed_item)]
    assert len(produced) + sum(connector.skipped.values()) == len(items)
    assert [record.name for record in produced] == ["Crusoe", "Fashion startup Atorie"]
    assert all(record.enrich_only for record in produced)


def test_an_item_with_no_publication_date_still_dates_its_round(
    connector: FundingRssConnector,
) -> None:
    record = records(connector, item("Acme raises $5M seed", published_at=None))[0]
    assert record.funding_rounds[0].announced_date is not None
