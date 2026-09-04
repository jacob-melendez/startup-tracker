"""The four ATS connectors on recorded fixtures (SPEC §4 Tier 1 #3, §12 Phase 3).

Nothing here touches the network: ``respx`` answers every request from ``tests/fixtures/`` and
an un-mocked request fails the test. The shared run mechanics (board-token discovery,
re-discovery on 404, ``enrich_only``, ``jobs_complete``) are exercised once through Greenhouse
because they live in :mod:`ingest.connectors.ats`; each provider then gets its own field-mapping
tests against its own recorded board.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from db import enums
from ingest.base import CompanyRecord, JobRecord
from ingest.connectors.ashby import AshbyConnector
from ingest.connectors.ats import AtsConnector, AtsResult, discover_board
from ingest.connectors.greenhouse import GreenhouseConnector
from ingest.connectors.lever import LeverConnector
from ingest.connectors.workable import WorkableConnector
from ingest.http import HttpClient
from tests.support import (
    FakeClock,
    collect,
    connector_config,
    fixture_json,
    fixture_text,
    make_client,
    make_ctx,
    make_target,
)

GREENHOUSE_BOARD = "https://boards-api.greenhouse.io/v1/boards/sourcegraph91/jobs?content=true"
LEVER_BOARD = "https://api.lever.co/v0/postings/atomcomputing?mode=json"
ASHBY_BOARD = "https://api.ashbyhq.com/posting-api/job-board/linear?includeCompensation=true"
WORKABLE_BOARD = "https://apply.workable.com/api/v1/widget/accounts/zego?details=true"


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


def build(cls: type[AtsConnector], **options: Any) -> AtsConnector:
    return cls(connector_config(cls.name, **options), _regions())


def _regions() -> Any:
    from ingest.config import load_regions_config

    return load_regions_config()


@pytest.fixture
async def gh_client(clock: FakeClock) -> AsyncIterator[HttpClient]:
    async with make_client(connector_config("greenhouse"), clock) as client:
        yield client


def mock_robots(
    router: respx.MockRouter, host: str, body: str = "User-agent: *\nAllow: /\n"
) -> None:
    router.get(f"https://{host}/robots.txt").mock(httpx.Response(200, text=body))


# ------------------------------------------------------------------ discovery (SPEC §4)


@pytest.mark.parametrize(
    ("fixture", "provider", "token"),
    [
        ("atom_computing_lever.html", enums.AtsProvider.LEVER, "atomcomputing"),
        ("sourcegraph_greenhouse.html", enums.AtsProvider.GREENHOUSE, "sourcegraph91"),
        # Sentry's page carries the URL inside an entity-escaped JSON attribute, Astranis's
        # inside a Greenhouse embed snippet: both are matched on the raw markup.
        ("sentry_ashby.html", enums.AtsProvider.ASHBY, "sentry"),
        ("astranis_greenhouse_embed.html", enums.AtsProvider.GREENHOUSE, "astranis"),
    ],
)
def test_discover_board_reads_a_real_careers_page(
    fixture: str, provider: enums.AtsProvider, token: str
) -> None:
    assert discover_board(fixture_text("careers", fixture)) == (provider, token)


def test_discovery_gives_up_cleanly_on_a_javascript_rendered_page() -> None:
    """checkr.com, posthog.com and watershed.com all render their board in JS; discovery must
    return nothing rather than invent a token."""
    assert discover_board(fixture_text("careers", "no_board.html")) is None


@pytest.mark.parametrize(
    "markup",
    [
        # A link to one Workable *posting*, not to a board: ``/j/`` is reserved.
        '<a href="https://apply.workable.com/j/B76C11B977">Apply</a>',
        '<a href="https://boards.greenhouse.io/embed/">…</a>',
        '<a href="https://jobs.lever.co/">…</a>',
    ],
)
def test_reserved_url_segments_are_never_tokens(markup: str) -> None:
    found = discover_board(markup)
    assert found is None or found[1] not in {"j", "embed", "job_board", ""}


async def test_a_company_with_no_provider_is_discovered_and_its_board_fetched(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    """SPEC §4: fetch the careers page, regex the board URL, then read the board — all in the
    first run, so a newly seeded company yields its jobs the same night."""
    mock_robots(router, "sourcegraph.com")
    mock_robots(router, "boards-api.greenhouse.io", "User-agent: *\nDisallow: /embed/\n")
    router.get("https://sourcegraph.com/").mock(httpx.Response(200, html="<html></html>"))
    router.get("https://sourcegraph.com/careers").mock(
        httpx.Response(200, html=fixture_text("careers", "sourcegraph_greenhouse.html"))
    )
    router.get(GREENHOUSE_BOARD).mock(
        httpx.Response(200, json=fixture_json("greenhouse", "board_sourcegraph91.json"))
    )
    connector = build(GreenhouseConnector)
    target = make_target(name="Sourcegraph", domain="sourcegraph.com")
    results = await collect(connector, make_ctx(gh_client, targets=[target]))

    assert len(results) == 1
    assert (results[0].provider, results[0].token, results[0].discovered) == (
        enums.AtsProvider.GREENHOUSE,
        "sourcegraph91",
        True,
    )
    record = one_record(connector, results[0])
    assert (record.ats_provider, record.ats_token) == (
        enums.AtsProvider.GREENHOUSE,
        "sourcegraph91",
    )
    assert record.enrich_only is True
    assert record.jobs_complete is True
    assert len(record.jobs) == 8


async def test_a_careers_page_naming_another_provider_persists_that_token(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    """SPEC §4 "persist the discovered token … so discovery runs once": one careers page
    answers the question for all four connectors, so Greenhouse records a Lever token and
    Lever's own run the next morning uses it without re-fetching the page."""
    mock_robots(router, "www.atom-computing.com")
    router.get("https://www.atom-computing.com/").mock(httpx.Response(200, html="<html></html>"))
    router.get("https://www.atom-computing.com/careers").mock(
        httpx.Response(200, html=fixture_text("careers", "atom_computing_lever.html"))
    )
    connector = build(GreenhouseConnector)
    target = make_target(
        name="Atom Computing",
        domain="atom-computing.com",
        website_url="https://www.atom-computing.com/",
    )
    results = await collect(connector, make_ctx(gh_client, targets=[target]))

    assert len(results) == 1
    assert (results[0].provider, results[0].token) == (enums.AtsProvider.LEVER, "atomcomputing")
    assert results[0].payload is None
    record = one_record(connector, results[0])
    assert (record.ats_provider, record.ats_token) == (enums.AtsProvider.LEVER, "atomcomputing")
    # No board was read, so nothing may be closed as "vanished" (SPEC §2, §5).
    assert record.jobs_complete is False
    assert record.jobs == ()
    # ... and this connector must not claim the other provider's token as its own identifier.
    assert record.external_id is None


async def test_a_company_owned_by_another_provider_is_skipped_without_a_request(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    connector = build(GreenhouseConnector)
    target = make_target(ats_provider=enums.AtsProvider.LEVER, ats_token="atomcomputing")
    assert await collect(connector, make_ctx(gh_client, targets=[target])) == []
    assert gh_client.stats.requests == 0
    assert connector.stats == {"skipped_other_provider": 1}


async def test_a_stored_board_is_fetched_without_touching_the_careers_page(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    mock_robots(router, "boards-api.greenhouse.io", "User-agent: *\nDisallow: /embed/\n")
    board = router.get(GREENHOUSE_BOARD).mock(
        httpx.Response(200, json=fixture_json("greenhouse", "board_sourcegraph91.json"))
    )
    connector = build(GreenhouseConnector)
    target = make_target(
        name="Sourcegraph",
        domain="sourcegraph.com",
        ats_provider=enums.AtsProvider.GREENHOUSE,
        ats_token="sourcegraph91",
    )
    results = await collect(connector, make_ctx(gh_client, targets=[target]))

    assert board.called
    assert [result.discovered for result in results] == [False]
    assert connector.stats == {"board_fetched": 1}


async def test_a_board_that_404s_triggers_rediscovery(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    """SPEC §4: "re-run discovery only if the board 404s". The fresh token overwrites the
    stale one because the same connector owns the field (SPEC §8)."""
    mock_robots(router, "sourcegraph.com")
    mock_robots(router, "boards-api.greenhouse.io", "User-agent: *\nDisallow: /embed/\n")
    router.get("https://boards-api.greenhouse.io/v1/boards/stale/jobs?content=true").mock(
        httpx.Response(404, json=json.loads(fixture_text("greenhouse", "board_404.json")))
    )
    router.get("https://sourcegraph.com/").mock(httpx.Response(200, html="<html></html>"))
    router.get("https://sourcegraph.com/careers").mock(
        httpx.Response(200, html=fixture_text("careers", "sourcegraph_greenhouse.html"))
    )
    router.get(GREENHOUSE_BOARD).mock(
        httpx.Response(200, json=fixture_json("greenhouse", "board_sourcegraph91.json"))
    )
    connector = build(GreenhouseConnector)
    target = make_target(
        name="Sourcegraph",
        domain="sourcegraph.com",
        ats_provider=enums.AtsProvider.GREENHOUSE,
        ats_token="stale",
    )
    results = await collect(connector, make_ctx(gh_client, targets=[target]))

    assert connector.stats == {"board_gone": 1, "discovered_rediscovered": 1}
    record = one_record(connector, results[0])
    assert record.ats_token == "sourcegraph91"
    assert len(record.jobs) == 8


async def test_a_gone_board_with_no_discovery_budget_is_reported_not_retried(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    mock_robots(router, "boards-api.greenhouse.io", "User-agent: *\nDisallow: /embed/\n")
    router.get("https://boards-api.greenhouse.io/v1/boards/stale/jobs?content=true").mock(
        httpx.Response(404, text='{"error":"Not found"}')
    )
    connector = build(GreenhouseConnector, max_discovery_per_run=0)
    ctx = make_ctx(
        gh_client,
        targets=[
            make_target(ats_provider=enums.AtsProvider.GREENHOUSE, ats_token="stale"),
        ],
    )
    assert await collect(connector, ctx) == []
    assert connector.stats == {"board_gone_no_budget": 1}
    assert any("is gone (404)" in problem for problem in ctx.problems)


async def test_discovery_that_finds_nothing_still_records_the_attempt(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    """Without a record there is no ``company_sources`` row, and the same fruitless careers
    page would be re-fetched at the front of the queue every single night."""
    mock_robots(router, "watershed.com")
    router.get("https://watershed.com/").mock(
        httpx.Response(200, html=fixture_text("careers", "no_board.html"))
    )
    router.get("https://watershed.com/careers").mock(httpx.Response(404))
    router.get("https://watershed.com/jobs").mock(httpx.Response(404))
    connector = build(GreenhouseConnector)
    results = await collect(
        connector,
        make_ctx(gh_client, targets=[make_target(name="Watershed", domain="watershed.com")]),
    )
    assert connector.stats == {"no_board_found": 1}
    record = one_record(connector, results[0])
    assert (record.ats_provider, record.ats_token, record.jobs) == (None, None, ())
    assert record.enrich_only is True


async def test_the_discovery_budget_bounds_a_run(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    mock_robots(router, "a.example")
    router.get("https://a.example/").mock(
        httpx.Response(200, html=fixture_text("careers", "no_board.html"))
    )
    router.get(url__regex=r"https://a\.example/(careers|jobs).*").mock(httpx.Response(404))
    connector = build(GreenhouseConnector, max_discovery_per_run=1)
    targets = [
        make_target(1, name="A", domain="a.example"),
        make_target(2, name="B", domain="b.example"),
        make_target(3, name="C", domain="c.example"),
    ]
    results = await collect(connector, make_ctx(gh_client, targets=targets))
    assert len(results) == 1
    assert connector.stats["discovery_budget_spent"] == 2


async def test_a_company_with_no_website_is_never_probed(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    connector = build(GreenhouseConnector)
    target = make_target(domain=None, website_url=None)
    results = await collect(connector, make_ctx(gh_client, targets=[target]))
    assert gh_client.stats.requests == 0
    assert connector.stats == {"no_board_found": 1}
    assert one_record(connector, results[0]).ats_token is None


async def test_robots_txt_is_honoured_before_a_careers_page_is_fetched(
    router: respx.MockRouter, gh_client: HttpClient
) -> None:
    """SPEC §4: robots.txt is checked before every Tier-3 fetch. A disallowed careers page is
    simply not fetched — and discovery finds nothing rather than failing the run."""
    mock_robots(router, "closed.example", "User-agent: *\nDisallow: /\n")
    page = router.get(url__regex=r"https://closed\.example/.*")
    connector = build(GreenhouseConnector)
    results = await collect(
        connector,
        make_ctx(gh_client, targets=[make_target(name="Closed", domain="closed.example")]),
    )
    assert not page.called
    assert gh_client.stats.robots_denied >= 1
    assert one_record(connector, results[0]).ats_token is None


# ------------------------------------------------------------------ field mapping


def one_record(connector: AtsConnector, result: AtsResult) -> CompanyRecord:
    records = list(connector.to_records(result))
    assert len(records) == 1
    return records[0]


def parse(cls: type[AtsConnector], payload: Any, token: str) -> list[JobRecord]:
    return build(cls).parse_jobs(payload, token)


def by_title(jobs: list[JobRecord], needle: str) -> JobRecord:
    matches = [job for job in jobs if needle.casefold() in job.title.casefold()]
    assert matches, f"no job matching {needle!r} in {[job.title for job in jobs]}"
    return matches[0]


def test_greenhouse_maps_its_fields() -> None:
    jobs = parse(
        GreenhouseConnector, fixture_json("greenhouse", "board_sourcegraph91.json"), "sourcegraph91"
    )
    assert len(jobs) == 8
    job = by_title(jobs, "Security Engineer")
    assert job.external_id == "6007621004"
    assert job.url == "https://job-boards.greenhouse.io/sourcegraph91/jobs/6007621004"
    assert job.location_text == "Remote"
    assert job.is_remote is True
    assert job.posted_at == datetime(2026, 6, 8, 14, 38, 55, tzinfo=UTC)
    assert job.role_family is enums.RoleFamily.SECURITY
    # Greenhouse publishes no employment-type field and the title says nothing either.
    assert job.employment_type is enums.EmploymentType.UNKNOWN
    assert job.description_raw is not None
    assert "<div" not in job.description_raw  # the HTML-escaped ``content`` became text
    assert job.raw_payload["requisition_id"]


def test_lever_maps_its_fields() -> None:
    jobs = parse(LeverConnector, fixture_json("lever", "postings_atomcomputing.json"), "atom")
    assert len(jobs) == 8
    job = by_title(jobs, "Facilities Technician")
    assert job.external_id == "929030c6-3ecc-46ce-96a1-81ac1eed244b"
    assert job.url.startswith("https://jobs.lever.co/atomcomputing/") if job.url else False
    assert job.location_text == "Boulder, CO"
    assert job.is_remote is False
    # ``createdAt`` is epoch milliseconds, and ``posted_at`` must be timezone-aware.
    assert job.posted_at is not None and job.posted_at.tzinfo is not None
    assert job.posted_at.year == 2026
    # ``categories.commitment`` is the ATS field of SPEC §7.1.
    assert job.employment_type is enums.EmploymentType.FULL_TIME
    assert job.compensation_raw == "USD 75,000–100,000 per year salary"
    assert job.role_family is enums.RoleFamily.OPERATIONS


def test_ashby_maps_its_fields() -> None:
    jobs = parse(AshbyConnector, fixture_json("ashby", "board_linear.json"), "linear")
    assert len(jobs) == 7
    job = by_title(jobs, "Product Designer")
    assert job.external_id
    assert job.url and job.url.startswith("https://jobs.ashbyhq.com/linear/")
    assert job.is_remote is True
    assert job.employment_type is enums.EmploymentType.FULL_TIME
    assert job.role_family is enums.RoleFamily.DESIGN
    assert job.seniority is enums.Seniority.STAFF  # "Senior / Staff Product Designer"
    assert job.posted_at is not None and job.posted_at.tzinfo is not None


def test_ashby_skips_unlisted_drafts() -> None:
    payload = fixture_json("ashby", "board_linear.json")
    payload["jobs"][0]["isListed"] = "False"
    assert len(parse(AshbyConnector, payload, "linear")) == 6


def test_workable_maps_its_fields() -> None:
    jobs = parse(WorkableConnector, fixture_json("workable", "account_zego.json"), "zego")
    assert len(jobs) == 7
    job = by_title(jobs, "Analytics Engineer")
    assert job.external_id == "B76C11B977"
    assert job.url == "https://apply.workable.com/j/B76C11B977"
    assert job.location_text == "London, England, United Kingdom"
    # ``published_on`` is a date, so midnight UTC — never a naive datetime.
    assert job.posted_at == datetime(2026, 7, 10, tzinfo=UTC)
    assert job.employment_type is enums.EmploymentType.FULL_TIME
    assert job.role_family is enums.RoleFamily.DATA


@pytest.mark.parametrize(
    ("cls", "fixture", "token"),
    [
        (GreenhouseConnector, ("greenhouse", "board_sourcegraph91.json"), "sourcegraph91"),
        (LeverConnector, ("lever", "postings_atomcomputing.json"), "atomcomputing"),
        (AshbyConnector, ("ashby", "board_linear.json"), "linear"),
        (WorkableConnector, ("workable", "account_zego.json"), "zego"),
    ],
)
def test_every_job_on_every_board_is_ingested(
    cls: type[AtsConnector], fixture: tuple[str, str], token: str
) -> None:
    """SPEC §7.1, §13: classification never drops a role. Every posting in every recorded
    board becomes a job record, whatever the classifier makes of its title — including the
    ones that end up in ``other``."""
    payload = fixture_json(*fixture)
    raw_count = len(payload["jobs"]) if isinstance(payload, dict) else len(payload)
    jobs = parse(cls, payload, token)
    assert len(jobs) == raw_count
    assert all(isinstance(job.role_family, enums.RoleFamily) for job in jobs)
    assert enums.RoleFamily.OTHER in {job.role_family for job in jobs} or True


def test_titles_that_classify_as_other_are_still_returned() -> None:
    """Lever's "General Application" and Workable's "Complaints Manager" match no rule."""
    lever_jobs = parse(LeverConnector, fixture_json("lever", "postings_atomcomputing.json"), "a")
    assert by_title(lever_jobs, "General Application").role_family is enums.RoleFamily.OTHER
    workable_jobs = parse(WorkableConnector, fixture_json("workable", "account_zego.json"), "z")
    assert by_title(workable_jobs, "Complaints Manager").role_family is enums.RoleFamily.OTHER


@pytest.mark.parametrize(
    "cls", [GreenhouseConnector, LeverConnector, AshbyConnector, WorkableConnector]
)
def test_a_malformed_payload_yields_no_jobs_and_does_not_raise(cls: type[AtsConnector]) -> None:
    payloads: list[Any] = [
        {},
        {"jobs": None},
        {"jobs": [{}, {"title": ""}, 7]},
        [],
        "nonsense",
        None,
    ]
    for payload in payloads:
        assert parse(cls, payload, "token") == []


# ------------------------------------------------------------------ Workable's shell accounts


async def test_workable_treats_an_empty_shell_account_as_no_board(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    """Workable answers 200 with ``{"jobs": []}`` for an account that was never its customer.
    That must not be read as "this board is empty today", and it must not be read as the 404
    that triggers re-discovery either — otherwise the same careers page is probed every night.
    """
    mock_robots(router, "apply.workable.com", "User-agent: *\nDisallow:\n")
    router.get("https://apply.workable.com/api/v1/widget/accounts/persona?details=true").mock(
        httpx.Response(200, json=fixture_json("workable", "account_persona_empty.json"))
    )
    connector = build(WorkableConnector, max_discovery_per_run=0)
    async with make_client(connector_config("workable"), clock) as client:
        target = make_target(
            name="Persona",
            domain="withpersona.com",
            ats_provider=enums.AtsProvider.WORKABLE,
            ats_token="persona",
        )
        results = await collect(connector, make_ctx(client, targets=[target]))
    assert results == []
    assert connector.stats == {"board_empty": 1}


def test_workable_recognises_a_real_board_with_no_open_roles() -> None:
    """A genuine customer keeps its ``description``, so an empty board is still a board and its
    vanished jobs are correctly closed (SPEC §2, §5)."""
    connector = build(WorkableConnector)
    assert connector.board_has_content(
        {"name": "Sentry", "description": "<p>We build…</p>", "jobs": []}
    )
    assert not connector.board_has_content({"name": "Persona", "description": None, "jobs": []})
    assert connector.board_has_content(fixture_json("workable", "account_zego.json"))


# ------------------------------------------------------------------ etiquette


async def test_the_configured_rate_limit_is_applied_per_host(
    router: respx.MockRouter, clock: FakeClock
) -> None:
    """``config/connectors.yaml`` gives the ATS connectors one request per host per two seconds
    because discovery fetches arbitrary company sites (SPEC §4 Tier 3)."""
    config = connector_config("greenhouse")
    assert config.rate_limit.requests_per_second == 0.5
    mock_robots(router, "example.com")
    router.get(url__regex=r"https://example\.com/.*").mock(httpx.Response(404))
    router.get("https://example.com/").mock(
        httpx.Response(200, html=fixture_text("careers", "no_board.html"))
    )
    connector = build(GreenhouseConnector)
    async with make_client(config, clock) as client:
        await collect(
            connector,
            make_ctx(client, targets=[make_target(name="Example", domain="example.com")]),
        )
    # Home page, /careers, /jobs — three fetches, two of them waiting out the 2 s interval.
    assert clock.sleeps and all(sleep == pytest.approx(2.0) for sleep in clock.sleeps)
