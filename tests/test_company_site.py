"""``ingest.connectors.company_site`` — the only Tier-3 connector (SPEC §4 Tier 3, §12 Phase 3).

SPEC §13 requires a test that ``robots.txt`` is checked before every Tier-3 fetch; that is
:func:`test_robots_txt_is_checked_before_every_fetch` below. The rest covers the other Tier-3
rules — one request per domain per two seconds, five pages per domain per run, depth 1 only —
and the enrichment the connector actually writes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timedelta

import httpx
import pytest
import respx

from db import enums
from ingest.base import CompanyRecord, CompanyTarget
from ingest.config import RegionsConfig, load_regions_config
from ingest.connectors.company_site import (
    MAX_PAGES_PER_COMPANY,
    CompanySiteConnector,
    SitePage,
    SiteVisit,
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
    make_target,
)

HOME = "https://www.astranis.com/"
CAREERS = "https://www.astranis.com/careers"
ROBOTS = "https://www.astranis.com/robots.txt"


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
def connector(regions: RegionsConfig) -> CompanySiteConnector:
    return CompanySiteConnector(connector_config("company_site"), regions)


@pytest.fixture
async def client(clock: FakeClock) -> AsyncIterator[HttpClient]:
    async with make_client(connector_config("company_site"), clock) as http:
        yield http


def mock_astranis(router: respx.MockRouter, *, robots: str | None = None) -> None:
    router.get(ROBOTS).mock(
        httpx.Response(
            200,
            text=robots
            if robots is not None
            else fixture_text("company_site", "astranis_robots.txt"),
        )
    )
    router.get(HOME).mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_home.html"))
    )
    router.get(CAREERS).mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_careers.html"))
    )


def astranis_target(*, last_seen_at: datetime | None = None) -> CompanyTarget:
    return make_target(
        name="Astranis", domain="astranis.com", website_url=HOME, last_seen_at=last_seen_at
    )


# ------------------------------------------------------------------ SPEC §4 Tier 3 rules


async def test_robots_txt_is_checked_before_every_fetch(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    """SPEC §13: "``robots.txt`` is checked before every Tier-3 fetch, covered by a test.\""""
    robots = router.get(ROBOTS).mock(httpx.Response(200, text="User-agent: *\nAllow: /\n"))
    home = router.get(HOME).mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_home.html"))
    )
    router.get(CAREERS).mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_careers.html"))
    )
    await collect(connector, make_ctx(client, targets=[astranis_target()]))
    assert robots.called
    # robots.txt was requested before the home page, and only once for the origin.
    urls = [str(call.request.url) for call in router.calls]
    assert urls.index(ROBOTS) < urls.index(HOME)
    assert urls.count(ROBOTS) == 1
    assert home.called


async def test_a_disallowed_site_is_never_fetched(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    router.get(ROBOTS).mock(httpx.Response(200, text="User-agent: *\nDisallow: /\n"))
    home = router.get(HOME)
    results = await collect(connector, make_ctx(client, targets=[astranis_target()]))
    assert not home.called
    assert results == []
    assert connector.stats == {"robots_disallowed": 1, "home_unreachable": 1}


async def test_a_disallowed_subpath_only_blocks_that_path(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    mock_astranis(router, robots="User-agent: *\nAllow: /\nDisallow: /careers\n")
    careers = router.get(CAREERS)
    results = await collect(connector, make_ctx(client, targets=[astranis_target()]))
    assert not careers.called
    assert len(results) == 1
    assert results[0].careers_url is None


async def test_one_request_per_domain_per_two_seconds(
    router: respx.MockRouter, connector: CompanySiteConnector, clock: FakeClock
) -> None:
    """SPEC §4 Tier 3: "max 1 request per domain per 2 seconds"."""
    config = connector_config("company_site")
    assert config.rate_limit.requests_per_second == 0.5
    assert config.rate_limit.max_requests_per_host_per_run == 5
    mock_astranis(router)
    async with make_client(config, clock) as http:
        await collect(connector, make_ctx(http, targets=[astranis_target()]))
    assert clock.sleeps and all(sleep == pytest.approx(2.0) for sleep in clock.sleeps)


async def test_never_deeper_than_depth_one(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    """SPEC §4 Tier 3: "never follow deeper than depth 1 from the homepage". The careers page
    links on to more pages; none of them is fetched."""
    mock_astranis(router)
    deeper = router.get("https://www.astranis.com/careers/engineering")
    results = await collect(connector, make_ctx(client, targets=[astranis_target()]))
    assert not deeper.called
    assert [page.url for page in results[0].pages] == [HOME, CAREERS]


def test_depth_one_links_stay_on_the_company_domain(
    connector: CompanySiteConnector,
) -> None:
    """A careers page hosted on the ATS is that ATS's site; fetching it here would spend a
    Tier-3 budget on a Tier-1 API."""
    home = SitePage(
        HOME,
        """<html><body>
        <a href="/careers">Careers</a>
        <a href="https://job-boards.greenhouse.io/astranis">Open roles</a>
        <a href="https://blog.example.com/about">About us elsewhere</a>
        <a href="mailto:jobs@astranis.com">Email</a>
        <a href="/pricing">Pricing</a>
        <a href="/careers#open">Careers again</a>
        </body></html>""",
    )
    assert connector._depth_one_urls(home) == ["https://www.astranis.com/careers"]


def test_at_most_three_pages_per_company() -> None:
    assert MAX_PAGES_PER_COMPANY == 3
    # ... and the client's own per-host budget is the SPEC §4 Tier 3 cap of five.
    assert connector_config("company_site").rate_limit.max_requests_per_host_per_run == 5


async def test_the_per_run_company_cap_is_honoured(
    router: respx.MockRouter, regions: RegionsConfig, client: HttpClient
) -> None:
    mock_astranis(router)
    router.get("https://b.example/robots.txt").mock(httpx.Response(404))
    router.get("https://b.example/").mock(httpx.Response(200, html="<html></html>"))
    connector = CompanySiteConnector(
        connector_config("company_site", max_companies_per_run=1), regions
    )
    targets = [astranis_target(), make_target(2, name="B", domain="b.example")]
    results = await collect(connector, make_ctx(client, targets=targets))
    assert len(results) == 1


async def test_a_recently_visited_company_is_skipped(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    """The targets arrive oldest-first, so the first fresh one ends the run."""
    target = astranis_target(last_seen_at=NOW - timedelta(days=1))
    assert await collect(connector, make_ctx(client, targets=[target])) == []
    assert client.stats.requests == 0
    assert connector.stats == {"visited_recently": 1}


async def test_a_company_with_no_website_is_skipped(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    target = make_target(name="No site", domain=None, website_url=None)
    assert await collect(connector, make_ctx(client, targets=[target])) == []
    assert connector.stats == {"no_website": 1}


async def test_an_unreachable_home_page_ends_that_company_quietly(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    router.get(ROBOTS).mock(httpx.Response(404))
    router.get(HOME).mock(httpx.Response(500))
    ctx = make_ctx(client, targets=[astranis_target()])
    assert await collect(connector, ctx) == []
    assert connector.stats["home_unreachable"] == 1
    # A Tier-3 site may simply decline; that is not a run-level problem (SPEC §4).
    assert ctx.problems == []


async def test_a_non_html_response_is_ignored(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    router.get(ROBOTS).mock(httpx.Response(404))
    router.get(HOME).mock(httpx.Response(200, json={"not": "html"}))
    assert await collect(connector, make_ctx(client, targets=[astranis_target()])) == []
    assert connector.stats["not_html"] == 1


# ------------------------------------------------------------------ enrichment


async def test_a_visit_enriches_the_company(
    router: respx.MockRouter, connector: CompanySiteConnector, client: HttpClient
) -> None:
    mock_astranis(router)
    results = await collect(connector, make_ctx(client, targets=[astranis_target()]))
    record = one(connector, results[0])
    assert record.name == "Astranis"
    assert record.domain == "astranis.com"
    assert record.website_url == HOME
    assert record.one_liner and record.one_liner.startswith("Astranis designs, builds")
    # SPEC §4 Tier 3: the company published its own careers URL.
    assert [(c.kind, c.value, c.confidence) for c in record.contacts] == [
        (enums.ContactKind.CAREERS_PAGE, CAREERS, enums.ContactConfidence.PUBLISHED)
    ]
    # It only ever enriches: a redirect that landed somewhere odd must not create a company.
    assert record.enrich_only is True


def one(connector: CompanySiteConnector, visit: SiteVisit) -> CompanyRecord:
    records = list(connector.to_records(visit))
    assert len(records) == 1
    return records[0]


def test_the_one_liner_prefers_og_description(connector: CompanySiteConnector) -> None:
    home = SitePage(
        "https://x.example/",
        """<html><head><title>X | Brand</title>
        <meta name="description" content="Plain description."/>
        <meta property="og:description" content="Open Graph description."/>
        </head><body></body></html>""",
    )
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert record.one_liner == "Open Graph description."


def test_a_site_with_no_description_falls_back_to_its_title(
    connector: CompanySiteConnector,
) -> None:
    home = SitePage("https://x.example/", "<html><head><title>Widgets | Acme</title></head></html>")
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert record.one_liner == "Widgets"


def test_a_bare_brand_title_is_kept_whole(connector: CompanySiteConnector) -> None:
    home = SitePage("https://x.example/", "<html><head><title>Sourcegraph</title></head></html>")
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert record.one_liner == "Sourcegraph"


def test_a_long_description_fills_thesis_and_a_shortened_one_liner(
    connector: CompanySiteConnector,
) -> None:
    long_text = "We build things. " * 40
    home = SitePage(
        "https://x.example/",
        f'<html><head><meta name="description" content="{long_text}"/></head></html>',
    )
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert record.one_liner is not None and len(record.one_liner) <= 301
    assert record.one_liner.endswith("…")
    assert record.thesis is not None and len(record.thesis) > len(record.one_liner)


def test_a_short_description_does_not_duplicate_itself_into_thesis(
    connector: CompanySiteConnector,
) -> None:
    home = SitePage(
        "https://x.example/",
        '<html><head><meta name="description" content="Short."/></head></html>',
    )
    record = one(
        connector, SiteVisit(target=make_target(), home=home, pages=(home,), careers_url=None)
    )
    assert (record.one_liner, record.thesis) == ("Short.", None)


def test_a_real_recorded_home_page_maps(connector: CompanySiteConnector) -> None:
    home = SitePage(
        "https://sourcegraph.com/", fixture_text("company_site", "sourcegraph_home.html")
    )
    record = one(
        connector,
        SiteVisit(
            target=make_target(name="Sourcegraph", domain="sourcegraph.com"),
            home=home,
            pages=(home,),
            careers_url="https://sourcegraph.com/jobs",
        ),
    )
    assert record.one_liner is not None and "complete context" in record.one_liner
    assert record.contacts[0].value == "https://sourcegraph.com/jobs"
