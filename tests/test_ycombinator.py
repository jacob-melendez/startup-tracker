"""``ingest.connectors.ycombinator`` on recorded fixtures (SPEC §4 Tier 1 #2, §12 Phase 3).

Nothing here touches the network: ``respx`` answers every request from
``tests/fixtures/ycombinator`` and an un-mocked request fails the test.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from typing import Any
from urllib.parse import unquote

import httpx
import pytest
import respx

from db import enums
from ingest.config import RegionsConfig, load_regions_config
from ingest.connectors.ycombinator import (
    DIRECTORY_URL,
    PAGINATION_LIMIT,
    YCCompany,
    YCombinatorConnector,
    parse_credentials,
    split_locations,
)
from ingest.http import HttpClient
from tests.support import (
    FakeClock,
    collect,
    connector_config,
    fixture_json,
    fixture_text,
    make_client,
    make_ctx,
)

ALGOLIA = "https://45BWZJ1SGC-dsn.algolia.net/1/indexes/*/queries"
ALGOLIA_ROBOTS = "https://45bwzj1sgc-dsn.algolia.net/robots.txt"
YC_ROBOTS = "https://www.ycombinator.com/robots.txt"


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
def connector(regions: RegionsConfig) -> YCombinatorConnector:
    return YCombinatorConnector(connector_config("ycombinator"), regions)


@pytest.fixture
async def client(clock: FakeClock) -> AsyncIterator[HttpClient]:
    async with make_client(connector_config("ycombinator"), clock) as http:
        yield http


def mock_directory(router: respx.MockRouter) -> None:
    router.get(YC_ROBOTS).mock(httpx.Response(200, text=fixture_text("ycombinator", "robots.txt")))
    router.get(DIRECTORY_URL).mock(
        httpx.Response(200, html=fixture_text("ycombinator", "companies_page.html"))
    )
    mock_algolia_robots(router)


def mock_algolia_robots(router: respx.MockRouter) -> None:
    """As recorded on 2026-09-04: the DSN host redirects ``/robots.txt`` into the REST API,
    which answers 404 — "unavailable" under RFC 9309 §2.3.1.3, i.e. unrestricted."""
    router.get(ALGOLIA_ROBOTS).mock(
        httpx.Response(301, headers={"Location": "https://algolia.net/1/404"})
    )
    router.get("https://algolia.net/1/404").mock(
        httpx.Response(404, text=fixture_text("ycombinator", "algolia_robots_404.json"))
    )


def mock_algolia(router: respx.MockRouter) -> respx.Route:
    """Answer the facet query and the two batch queries from their recorded responses."""
    facets = fixture_json("ycombinator", "facets_batch.json")
    batches = {
        "Winter 2024": fixture_json("ycombinator", "batch_winter_2024.json"),
        "Summer 2012": fixture_json("ycombinator", "batch_summer_2012.json"),
    }
    empty = {"results": [{"hits": [], "nbHits": 0, "page": 0, "nbPages": 0}]}

    def respond(request: httpx.Request) -> httpx.Response:
        params = json.loads(request.content)["requests"][0]["params"]
        if "facets=" in params:
            return httpx.Response(200, json=facets)
        decoded = unquote(params)
        for name, payload in batches.items():
            if f"batch:{name}" in decoded:
                return httpx.Response(200, json=payload)
        return httpx.Response(200, json=empty)

    return router.post(ALGOLIA).mock(side_effect=respond)


# ------------------------------------------------------------------ credentials


def test_the_public_search_key_is_read_from_the_directory_page() -> None:
    """The key rotates — the one older clients hard-coded now answers 403 — so it is read at
    run time from the page YC itself serves."""
    credentials = parse_credentials(fixture_text("ycombinator", "companies_page.html"))
    assert credentials is not None
    assert credentials.app_id == "45BWZJ1SGC"
    assert credentials.api_key.startswith("NzllNTY5")


def test_a_page_without_credentials_yields_none() -> None:
    assert parse_credentials("<html><body>nothing here</body></html>") is None


async def test_configured_credentials_are_the_fallback(
    router: respx.MockRouter, clock: FakeClock, regions: RegionsConfig
) -> None:
    router.get(YC_ROBOTS).mock(httpx.Response(404))
    router.get(DIRECTORY_URL).mock(httpx.Response(503))
    connector = YCombinatorConnector(
        connector_config("ycombinator", app_id="FALLBACK123", api_key="key"), regions
    )
    async with make_client(connector_config("ycombinator"), clock) as http:
        credentials = await connector.credentials(make_ctx(http))
    assert (credentials.app_id, credentials.api_key) == ("FALLBACK123", "key")


async def test_without_credentials_the_run_fails_loudly(
    router: respx.MockRouter, connector: YCombinatorConnector, client: HttpClient
) -> None:
    """An empty directory is indistinguishable from a broken one; failing is the honest
    outcome, and ``run_connector`` records it on the ``FetchRun`` (SPEC §7.2)."""
    router.get(YC_ROBOTS).mock(httpx.Response(404))
    router.get(DIRECTORY_URL).mock(httpx.Response(200, html="<html>no key</html>"))
    with pytest.raises(RuntimeError, match="no longer embeds an Algolia"):
        await connector.credentials(make_ctx(client))


# ------------------------------------------------------------------ fetch


async def test_the_index_is_walked_one_batch_at_a_time(
    router: respx.MockRouter, connector: YCombinatorConnector, client: HttpClient
) -> None:
    """Algolia caps page-based pagination at 1 000 hits, so the index is partitioned by the
    ``batch`` facet: one query per batch, largest batch first."""
    mock_directory(router)
    algolia = mock_algolia(router)
    hits = await collect(connector, make_ctx(client))

    # One facet query plus one query per facet value (three in the recorded facet response).
    assert algolia.call_count == 4
    queried = [
        unquote(json.loads(call.request.content)["requests"][0]["params"]) for call in algolia.calls
    ]
    assert "facets=" in queried[0]
    # Largest first: Winter 2024 (248) before Summer 2012 (83) before Winter 2007 (13).
    assert ["batch:Winter 2024" in q for q in queried[1:]] == [True, False, False]
    assert len(hits) == 16
    assert {hit.batch for hit in hits} == {"Winter 2024", "Summer 2012"}


async def test_credentials_travel_as_headers_not_in_the_url(
    router: respx.MockRouter, connector: YCombinatorConnector, client: HttpClient
) -> None:
    """The shared client caches by URL; a search key in the query string would end up in the
    on-disk cache file name (SPEC §4's 24 h cache)."""
    mock_directory(router)
    algolia = mock_algolia(router)
    await collect(connector, make_ctx(client))
    request = algolia.calls[0].request
    assert request.headers["X-Algolia-Application-Id"] == "45BWZJ1SGC"
    assert request.headers["X-Algolia-API-Key"].startswith("NzllNTY5")
    assert "Algolia" not in str(request.url)


async def test_a_batch_too_large_to_page_is_reported(
    router: respx.MockRouter, connector: YCombinatorConnector, client: HttpClient
) -> None:
    mock_directory(router)
    facets = fixture_json("ycombinator", "facets_batch.json")
    facets["results"][0]["facets"]["batch"]["Winter 2024"] = PAGINATION_LIMIT + 1
    router.post(ALGOLIA).mock(
        side_effect=lambda request: httpx.Response(
            200,
            json=facets
            if "facets=" in json.loads(request.content)["requests"][0]["params"]
            else {"results": [{"hits": []}]},
        )
    )
    ctx = make_ctx(client)
    await collect(connector, ctx)
    assert any("past Algolia's pagination limit" in problem for problem in ctx.problems)


async def test_a_failing_batch_query_does_not_stop_the_run(
    router: respx.MockRouter, connector: YCombinatorConnector, client: HttpClient
) -> None:
    mock_directory(router)
    facets = fixture_json("ycombinator", "facets_batch.json")
    winter = fixture_json("ycombinator", "batch_winter_2024.json")

    def respond(request: httpx.Request) -> httpx.Response:
        params = unquote(json.loads(request.content)["requests"][0]["params"])
        if "facets=" in params:
            return httpx.Response(200, json=facets)
        if "batch:Winter 2024" in params:
            return httpx.Response(200, json=winter)
        return httpx.Response(500)

    router.post(ALGOLIA).mock(side_effect=respond)
    ctx = make_ctx(client)
    hits = await collect(connector, ctx)
    assert len(hits) == 8  # Winter 2024 still landed
    assert len(ctx.problems) == 2  # the other two batches


async def test_an_index_with_no_facet_values_raises(
    router: respx.MockRouter, connector: YCombinatorConnector, client: HttpClient
) -> None:
    mock_directory(router)
    router.post(ALGOLIA).mock(httpx.Response(200, json={"results": [{"facets": {}}]}))
    with pytest.raises(RuntimeError, match="no 'batch' facet values"):
        await collect(connector, make_ctx(client))


# ------------------------------------------------------------------ to_records


def record_for(connector: YCombinatorConnector, name: str, batch: str, fixture: str) -> Any:
    for hit in fixture_json("ycombinator", fixture)["results"][0]["hits"]:
        if hit.get("name") == name:
            records = list(connector.to_records(YCCompany(batch=batch, hit=hit)))
            return records[0] if records else None
    raise AssertionError(f"{name!r} is not in {fixture}")


def test_a_bay_area_company_maps_every_field(connector: YCombinatorConnector) -> None:
    record = record_for(connector, "Instacart", "Summer 2012", "batch_summer_2012.json")
    assert record.name == "Instacart"
    assert record.external_id == "instacart"
    assert record.domain == "https://www.instacart.com"  # normalized by the pipeline
    assert record.one_liner
    assert record.thesis
    assert record.employee_est == 3000
    assert record.status is enums.CompanyStatus.ACTIVE
    assert record.stage is enums.Stage.PUBLIC
    assert [(loc.city, loc.metro, loc.is_hq) for loc in record.locations] == [
        ("San Francisco", "Bay Area", True)
    ]
    assert "YC S12" in record.sectors
    assert record.source_url == f"{DIRECTORY_URL}/instacart"


def test_an_acquired_company_keeps_its_status(connector: YCombinatorConnector) -> None:
    record = record_for(connector, "Lever", "Summer 2012", "batch_summer_2012.json")
    assert record.status is enums.CompanyStatus.ACQUIRED
    assert record.stage is enums.Stage.GROWTH


def test_an_early_stage_company_leaves_stage_unset(connector: YCombinatorConnector) -> None:
    """ "Early" says nothing about which round a company raised; leaving ``stage`` unset lets
    ``sec_edgar``, which outranks this connector (SPEC §8), fill it from a real Form D."""
    record = record_for(connector, "ParcelBio", "Winter 2024", "batch_winter_2024.json")
    assert record.stage is None
    assert record.status is enums.CompanyStatus.ACTIVE


@pytest.mark.parametrize(
    ("name", "fixture"),
    [
        ("Dropback", "batch_winter_2024.json"),  # all_locations: "Remote"
        ("Yarn", "batch_winter_2024.json"),  # New York City
        ("Blacksmith", "batch_winter_2024.json"),  # no location at all
        ("Plivo", "batch_summer_2012.json"),  # Austin, TX
        ("9gag", "batch_summer_2012.json"),  # Hong Kong
    ],
)
def test_companies_outside_the_configured_region_are_dropped(
    connector: YCombinatorConnector, name: str, fixture: str
) -> None:
    batch = "Winter 2024" if "winter" in fixture else "Summer 2012"
    assert record_for(connector, name, batch, fixture) is None
    assert connector.skipped["outside_region"] >= 1


def test_a_multi_office_company_takes_its_first_configured_city_as_hq(
    connector: YCombinatorConnector,
) -> None:
    hit = {
        "name": "Multi",
        "slug": "multi",
        "all_locations": "Austin, TX, USA; Palo Alto, CA, USA; Berkeley, CA, USA",
    }
    record = next(iter(connector.to_records(YCCompany(batch="Winter 2024", hit=hit))))
    assert [(loc.city, loc.is_hq) for loc in record.locations] == [
        ("Palo Alto", True),
        ("Berkeley", False),
    ]


def test_a_hit_without_a_name_is_dropped(connector: YCombinatorConnector) -> None:
    assert list(connector.to_records(YCCompany(batch="W24", hit={"slug": "x"}))) == []
    assert connector.skipped == {"no_name": 1}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("San Francisco, CA, USA", [("San Francisco", "CA")]),
        ("Los Angeles, CA, USA; Remote", [("Los Angeles", "CA")]),
        ("Remote", []),
        ("", []),
        (None, []),
        (7, []),
    ],
)
def test_split_locations(value: object, expected: list[tuple[str, str]]) -> None:
    assert split_locations(value) == expected


def test_team_size_tolerates_the_shapes_the_index_uses(
    connector: YCombinatorConnector,
) -> None:
    base = {"name": "X", "slug": "x", "all_locations": "Berkeley, CA, USA"}
    for value, expected in ((12, 12), ("12", 12), (None, None), ("many", None), (-1, None)):
        hit = {**base, "team_size": value}
        record = next(iter(connector.to_records(YCCompany(batch="W24", hit=hit))))
        assert record.employee_est == expected
