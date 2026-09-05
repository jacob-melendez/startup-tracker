"""Phase 3 end to end: connector → pipeline → Postgres (SPEC §12 Phase 3, §13).

Unit tests prove each connector maps its fixture correctly; these prove the same code actually
lands in the database, through :func:`ingest.pipeline.run_connector` and a real Postgres:

* an **unclassifiable title becomes ``role_family='other'`` and is still stored and visible**
  with no filter applied (SPEC §7.1, §13 — the guarantee the whole phase hangs on);
* an ``enrich_only`` record writes nothing when its company is unknown and enriches it when it
  is (SPEC §4 Tier 2 #5);
* the pipeline pre-loads a connector's targets so no connector opens a session;
* the seed loader's ``dead`` status and sectors land, and a re-seed is idempotent (SPEC §10);
* an ATS board closes the jobs that vanished from it rather than deleting them (SPEC §2, §5).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.models import (
    Company,
    CompanyLocation,
    CompanySource,
    Contact,
    FundingRound,
    Job,
    Location,
    Sector,
)
from db.queries import load_company_targets
from ingest.config import RegionsConfig, load_regions_config
from ingest.connectors.company_site import CompanySiteConnector
from ingest.connectors.funding_rss import FundingRssConnector
from ingest.connectors.greenhouse import GreenhouseConnector
from ingest.connectors.hn_hiring import HnHiringConnector
from ingest.contacts import people_search_url
from ingest.http import HttpClient
from ingest.pipeline import run_connector
from ingest.seed import SeedConnector, SeedFile
from tests.support import NOW, FakeClock, connector_config, fixture_json, fixture_text, make_client

GREENHOUSE_BOARD = "https://boards-api.greenhouse.io/v1/boards/sourcegraph91/jobs?content=true"
GREENHOUSE_ROBOTS = "https://boards-api.greenhouse.io/robots.txt"


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
async def gh_client(clock: FakeClock) -> AsyncIterator[HttpClient]:
    async with make_client(connector_config("greenhouse"), clock) as client:
        yield client


def seed_of(*entries: dict[str, str]) -> SeedFile:
    return SeedFile.model_validate({"companies": {"seeded": list(entries)}})


def mock_seed_site(router: respx.MockRouter, host: str, status: int = 200) -> None:
    router.get(f"https://{host}/robots.txt").mock(
        httpx.Response(200, text="User-agent: *\nAllow: /\n")
    )
    router.head(f"https://{host}/").mock(httpx.Response(status))


def mock_greenhouse(router: respx.MockRouter) -> None:
    router.get(GREENHOUSE_ROBOTS).mock(
        httpx.Response(200, text=fixture_text("greenhouse", "robots.txt"))
    )


async def seed_companies(
    session_factory: async_sessionmaker[AsyncSession],
    http: HttpClient,
    regions: RegionsConfig,
    *entries: dict[str, str],
    now: datetime = NOW,
) -> None:
    """Put companies in the database the way ``cli.py seed`` does (SPEC §10)."""
    connector = SeedConnector(connector_config("seed"), regions, seed_file=seed_of(*entries))
    await run_connector(connector, session_factory=session_factory, http=http, now=now)


async def company_by_domain(
    session_factory: async_sessionmaker[AsyncSession], domain: str
) -> Company:
    async with session_factory() as session:
        company = await session.scalar(select(Company).where(Company.domain == domain))
    assert company is not None, f"no company with domain {domain!r}"
    return company


async def count(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        total = await session.scalar(select(func.count()).select_from(model))
    return int(total or 0)


# ------------------------------------------------------------------ the seed loader


async def test_seed_writes_companies_locations_and_sectors(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
) -> None:
    mock_seed_site(router, "ok.example")
    mock_seed_site(router, "gone.example", status=404)
    async with make_client(connector_config("seed"), clock) as http:
        await seed_companies(
            session_factory,
            http,
            regions,
            {"name": "Ok Corp", "domain": "ok.example", "city": "Berkeley"},
            {"name": "Gone Inc", "domain": "gone.example", "city": "Palo Alto"},
        )
    assert await count(session_factory, Company) == 2

    ok = await company_by_domain(session_factory, "ok.example")
    assert ok.status is enums.CompanyStatus.ACTIVE  # the column default; nothing was written
    assert ok.website_url == "https://ok.example/"
    assert ok.normalized_name == "ok"  # "Corp" is a stripped legal suffix (SPEC §8 step 2)

    gone = await company_by_domain(session_factory, "gone.example")
    # SPEC §10: unreachable entries are marked dead and logged, never dropped.
    assert gone.status is enums.CompanyStatus.DEAD

    async with session_factory() as session:
        locations = (await session.scalars(select(Location))).all()
        sectors = (await session.scalars(select(Sector))).all()
    assert {location.city for location in locations} == {"Berkeley", "Palo Alto"}
    assert {location.metro for location in locations} == {"Bay Area"}
    assert [sector.name for sector in sectors] == ["Seeded"]


async def test_the_seed_run_is_recorded_and_re_seeding_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
) -> None:
    mock_seed_site(router, "ok.example")
    entry = {"name": "Ok Corp", "domain": "ok.example", "city": "Berkeley"}
    async with make_client(connector_config("seed"), clock) as http:
        connector = SeedConnector(connector_config("seed"), regions, seed_file=seed_of(entry))
        first = await run_connector(connector, session_factory=session_factory, http=http, now=NOW)
        second = await run_connector(
            connector, session_factory=session_factory, http=http, now=NOW + timedelta(days=1)
        )
    # SPEC §7.2: every run writes a FetchRun row.
    assert (first.connector, first.status) == ("seed", enums.FetchRunStatus.OK)
    assert (first.n_fetched, first.n_upserted) == (1, 1)
    assert second.status is enums.FetchRunStatus.OK
    assert await count(session_factory, Company) == 1

    async with session_factory() as session:
        source = await session.scalar(
            select(CompanySource).where(CompanySource.connector == "seed")
        )
    assert source is not None and source.external_id == "ok-corp"


async def test_a_seed_entry_never_carries_an_ats_token(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
) -> None:
    """SPEC §10: "do not hardcode ATS tokens — let the discovery step … find them"."""
    mock_seed_site(router, "ok.example")
    async with make_client(connector_config("seed"), clock) as http:
        await seed_companies(
            session_factory,
            http,
            regions,
            {"name": "Ok", "domain": "ok.example", "city": "Berkeley"},
        )
    company = await company_by_domain(session_factory, "ok.example")
    assert (company.ats_provider, company.ats_token) == (None, None)


# ------------------------------------------------------------------ SPEC §13: never excluded


async def test_an_unclassifiable_title_is_other_and_is_still_stored_and_visible(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
    gh_client: HttpClient,
) -> None:
    """SPEC §7.1 and §13: "no role is dropped at ingest for failing to classify", and with no
    filter applied every open role of every family is reachable.

    The board is Sourcegraph's real Greenhouse board plus one deliberately unclassifiable
    title, ingested end to end and then read back the way the unfiltered ``/roles`` view will.
    """
    mock_seed_site(router, "sourcegraph.com")
    async with make_client(connector_config("seed"), clock) as seed_http:
        await seed_companies(
            session_factory,
            seed_http,
            regions,
            {"name": "Sourcegraph", "domain": "sourcegraph.com", "city": "San Francisco"},
        )

    board = fixture_json("greenhouse", "board_sourcegraph91.json")
    board["jobs"].append(
        {
            "id": 99999,
            "title": "Underwater Basket Weaver",
            "absolute_url": "https://example.com/jobs/99999",
            "location": {"name": "San Francisco, CA"},
            "first_published": "2026-09-01T10:00:00-04:00",
            "content": "&lt;p&gt;Weave baskets, underwater.&lt;/p&gt;",
        }
    )
    mock_greenhouse(router)
    router.get(GREENHOUSE_BOARD).mock(httpx.Response(200, json=board))

    company = await company_by_domain(session_factory, "sourcegraph.com")
    async with session_factory() as session:
        await session.execute(
            update(Company)
            .where(Company.id == company.id)
            .values(ats_provider=enums.AtsProvider.GREENHOUSE, ats_token="sourcegraph91")
        )
        await session.commit()

    connector = GreenhouseConnector(connector_config("greenhouse"), regions)
    run = await run_connector(connector, session_factory=session_factory, http=gh_client, now=NOW)
    assert run.status is enums.FetchRunStatus.OK

    async with session_factory() as session:
        jobs = (await session.scalars(select(Job).order_by(Job.id))).all()
    # Every posting on the board is in the database — none dropped for failing to classify.
    assert len(jobs) == len(board["jobs"])
    weaver = next(job for job in jobs if job.title == "Underwater Basket Weaver")
    assert weaver.role_family is enums.RoleFamily.OTHER
    assert weaver.seniority is enums.Seniority.UNKNOWN
    assert weaver.closed_at is None
    assert weaver.description_raw == "Weave baskets, underwater."

    # ... and the unfiltered "every open role" read — the shape /roles uses — returns it.
    async with session_factory() as session:
        open_titles = set(
            (await session.scalars(select(Job.title).where(Job.closed_at.is_(None)))).all()
        )
    assert "Underwater Basket Weaver" in open_titles

    # The denormalized columns were refreshed at end of run (SPEC §5).
    refreshed = await company_by_domain(session_factory, "sourcegraph.com")
    assert refreshed.open_job_count == len(board["jobs"])
    assert refreshed.latest_job_posted_at is not None


async def test_several_role_families_land_from_one_real_board(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
    gh_client: HttpClient,
) -> None:
    mock_seed_site(router, "sourcegraph.com")
    async with make_client(connector_config("seed"), clock) as seed_http:
        await seed_companies(
            session_factory,
            seed_http,
            regions,
            {"name": "Sourcegraph", "domain": "sourcegraph.com", "city": "San Francisco"},
        )
    company = await company_by_domain(session_factory, "sourcegraph.com")
    async with session_factory() as session:
        await session.execute(
            update(Company)
            .where(Company.id == company.id)
            .values(ats_provider=enums.AtsProvider.GREENHOUSE, ats_token="sourcegraph91")
        )
        await session.commit()
    mock_greenhouse(router)
    router.get(GREENHOUSE_BOARD).mock(
        httpx.Response(200, json=fixture_json("greenhouse", "board_sourcegraph91.json"))
    )
    await run_connector(
        GreenhouseConnector(connector_config("greenhouse"), regions),
        session_factory=session_factory,
        http=gh_client,
        now=NOW,
    )
    async with session_factory() as session:
        families = set((await session.scalars(select(Job.role_family))).all())
    assert {
        enums.RoleFamily.SOFTWARE,
        enums.RoleFamily.PRODUCT,
        enums.RoleFamily.SALES,
        enums.RoleFamily.SECURITY,
        enums.RoleFamily.MARKETING,
        enums.RoleFamily.OPERATIONS,
    } <= families


# ------------------------------------------------------------------ targets (design decision 5)


async def test_the_pipeline_pre_loads_targets_for_a_connector_that_wants_them(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
    gh_client: HttpClient,
) -> None:
    """``ingest/base.py``: a connector never opens a session, so ``run_connector`` loads the
    work list and hands it over in ``ctx.targets``."""
    mock_seed_site(router, "a.example")
    mock_seed_site(router, "b.example", status=404)
    async with make_client(connector_config("seed"), clock) as seed_http:
        await seed_companies(
            session_factory,
            seed_http,
            regions,
            {"name": "A", "domain": "a.example", "city": "Berkeley"},
            {"name": "B", "domain": "b.example", "city": "Oakland"},
        )
    async with session_factory() as session:
        targets = await load_company_targets(session, "greenhouse")
    # ``b.example`` was marked dead by the seed validation, so it is not a target (SPEC §10).
    assert [target.name for target in targets] == ["A"]
    assert targets[0].domain == "a.example"
    assert targets[0].external_id is None and targets[0].last_seen_at is None


async def test_targets_are_ordered_least_recently_visited_first(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
) -> None:
    """This ordering is what makes ``company_site``'s per-run cap round-robin the database."""
    for host in ("a.example", "b.example", "c.example"):
        mock_seed_site(router, host)
    async with make_client(connector_config("seed"), clock) as seed_http:
        await seed_companies(
            session_factory,
            seed_http,
            regions,
            {"name": "A", "domain": "a.example", "city": "Berkeley"},
            {"name": "B", "domain": "b.example", "city": "Oakland"},
            {"name": "C", "domain": "c.example", "city": "Fremont"},
        )
    # Pretend company_site visited B yesterday and C a week ago.
    async with session_factory() as session:
        for domain, seen in (
            ("b.example", NOW - timedelta(days=1)),
            ("c.example", NOW - timedelta(days=7)),
        ):
            company = await session.scalar(select(Company).where(Company.domain == domain))
            assert company is not None
            session.add(
                CompanySource(company_id=company.id, connector="company_site", last_seen_at=seen)
            )
        await session.commit()

    async with session_factory() as session:
        targets = await load_company_targets(session, "company_site")
    assert [target.name for target in targets] == ["A", "C", "B"]


async def test_company_site_enriches_a_seeded_company(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
) -> None:
    mock_seed_site(router, "astranis.com")
    async with make_client(connector_config("seed"), clock) as seed_http:
        await seed_companies(
            session_factory,
            seed_http,
            regions,
            {"name": "Astranis", "domain": "astranis.com", "city": "San Francisco"},
        )
    router.get("https://astranis.com/robots.txt").mock(
        httpx.Response(200, text=fixture_text("company_site", "astranis_robots.txt"))
    )
    router.get("https://astranis.com/").mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_home.html"))
    )
    router.get("https://astranis.com/careers").mock(
        httpx.Response(200, html=fixture_text("company_site", "astranis_careers.html"))
    )
    async with make_client(connector_config("company_site"), clock) as http:
        run = await run_connector(
            CompanySiteConnector(connector_config("company_site"), regions),
            session_factory=session_factory,
            http=http,
            now=NOW,
        )
    assert run.status is enums.FetchRunStatus.OK
    assert run.n_upserted == 1

    company = await company_by_domain(session_factory, "astranis.com")
    assert company.one_liner is not None and company.one_liner.startswith("Astranis designs")
    async with session_factory() as session:
        contacts = (await session.scalars(select(Contact).order_by(Contact.kind))).all()
    # SPEC §6 end to end, through a real Postgres: everything the fixture's own footer publishes
    # is `published`, and the one link nothing published — the people search — is `constructed`.
    # The footer's `linkedin.com/company/astranis/` is exactly what the domain would construct,
    # so the constructed row the seed run wrote is promoted in place rather than duplicated.
    assert [(c.kind, c.confidence, c.value) for c in contacts] == [
        (
            enums.ContactKind.LINKEDIN_COMPANY,
            enums.ContactConfidence.PUBLISHED,
            "https://www.linkedin.com/company/astranis",
        ),
        (
            enums.ContactKind.LINKEDIN_PEOPLE,
            enums.ContactConfidence.CONSTRUCTED,
            people_search_url("Astranis"),
        ),
        (
            enums.ContactKind.CAREERS_PAGE,
            enums.ContactConfidence.PUBLISHED,
            "https://astranis.com/careers",
        ),
        (enums.ContactKind.X, enums.ContactConfidence.PUBLISHED, "https://x.com/Astranis"),
        (
            enums.ContactKind.CONTACT_FORM,
            enums.ContactConfidence.PUBLISHED,
            "https://astranis.com/contact",
        ),
    ]
    # No new company was invented by a Tier-3 visit.
    assert await count(session_factory, Company) == 1


# ------------------------------------------------------------------ enrich_only (SPEC §4)


async def test_a_funding_headline_about_an_unknown_company_writes_nothing(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
) -> None:
    """SPEC §4 Tier 2 #5: "never as a primary source"."""
    router.get("https://techcrunch.com/robots.txt").mock(
        httpx.Response(200, text=fixture_text("funding_rss", "robots.txt"))
    )
    router.get("https://techcrunch.com/category/venture/feed/").mock(
        httpx.Response(200, text=fixture_text("funding_rss", "techcrunch_venture.xml"))
    )
    router.get("https://techcrunch.com/category/startups/feed/").mock(
        httpx.Response(200, text=fixture_text("funding_rss", "techcrunch_startups.xml"))
    )
    async with make_client(connector_config("funding_rss"), clock) as http:
        run = await run_connector(
            FundingRssConnector(connector_config("funding_rss"), regions),
            session_factory=session_factory,
            http=http,
            now=datetime(2026, 9, 4, 12, tzinfo=UTC),
        )
    assert run.status is enums.FetchRunStatus.OK
    assert run.n_fetched == 20
    assert run.n_upserted == 0  # nothing resolved, so nothing was written
    assert await count(session_factory, Company) == 0
    assert await count(session_factory, FundingRound) == 0


async def test_a_funding_headline_enriches_a_company_that_exists(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
) -> None:
    """With the company already seeded, the same headline attaches a round to it — matched by
    name across the configured metros, because a headline carries no address."""
    mock_seed_site(router, "crusoe.ai")
    async with make_client(connector_config("seed"), clock) as seed_http:
        await seed_companies(
            session_factory,
            seed_http,
            regions,
            {"name": "Crusoe", "domain": "crusoe.ai", "city": "San Francisco"},
        )
    router.get("https://techcrunch.com/robots.txt").mock(
        httpx.Response(200, text=fixture_text("funding_rss", "robots.txt"))
    )
    router.get("https://techcrunch.com/category/venture/feed/").mock(
        httpx.Response(200, text=fixture_text("funding_rss", "techcrunch_venture.xml"))
    )
    router.get("https://techcrunch.com/category/startups/feed/").mock(
        httpx.Response(200, text=fixture_text("funding_rss", "techcrunch_startups.xml"))
    )
    async with make_client(connector_config("funding_rss"), clock) as http:
        await run_connector(
            FundingRssConnector(connector_config("funding_rss"), regions),
            session_factory=session_factory,
            http=http,
            now=datetime(2026, 9, 4, 12, tzinfo=UTC),
        )
    assert await count(session_factory, Company) == 1
    async with session_factory() as session:
        rounds = (await session.scalars(select(FundingRound))).all()
    assert len(rounds) == 1
    assert rounds[0].amount_usd == 3_000_000_000
    company = await company_by_domain(session_factory, "crusoe.ai")
    assert company.latest_round_id == rounds[0].id  # denormalized at end of run (SPEC §5)


async def test_two_seeded_companies_of_the_same_name_in_one_metro_are_one_company(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
) -> None:
    """SPEC §8 step 2 in practice: within a metro an exact normalized-name match *is* the same
    company, so a second seed entry under the same name enriches the first rather than
    duplicating it — and the funding headline then has exactly one company to enrich. (The
    genuinely ambiguous cross-metro case cannot arise in a single-region config; the guard
    that refuses it is covered in ``tests/test_normalize.py``.)"""
    mock_seed_site(router, "crusoe.ai")
    mock_seed_site(router, "crusoe.com")
    async with make_client(connector_config("seed"), clock) as seed_http:
        await seed_companies(
            session_factory,
            seed_http,
            regions,
            {"name": "Crusoe", "domain": "crusoe.ai", "city": "San Francisco"},
            {"name": "Crusoe", "domain": "crusoe.com", "city": "Oakland"},
        )
    assert await count(session_factory, Company) == 1
    # The domain is only ever *filled*, never changed (SPEC §8, design decision 1).
    company = await company_by_domain(session_factory, "crusoe.ai")
    assert {location.city for location in await locations_of(session_factory, company.id)} == {
        "San Francisco",
        "Oakland",
    }


async def locations_of(
    session_factory: async_sessionmaker[AsyncSession], company_id: int
) -> list[Location]:
    async with session_factory() as session:
        rows = await session.scalars(
            select(Location)
            .join(CompanyLocation, CompanyLocation.location_id == Location.id)
            .where(CompanyLocation.company_id == company_id)
        )
        return list(rows)


# ------------------------------------------------------------------ jobs are closed, not deleted


async def test_a_job_that_vanished_from_its_board_is_closed_not_deleted(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
    gh_client: HttpClient,
) -> None:
    """SPEC §2, §5: "never delete on refresh: a job missing from its source gets ``closed_at``"."""
    mock_seed_site(router, "sourcegraph.com")
    async with make_client(connector_config("seed"), clock) as seed_http:
        await seed_companies(
            session_factory,
            seed_http,
            regions,
            {"name": "Sourcegraph", "domain": "sourcegraph.com", "city": "San Francisco"},
        )
    company = await company_by_domain(session_factory, "sourcegraph.com")
    async with session_factory() as session:
        await session.execute(
            update(Company)
            .where(Company.id == company.id)
            .values(ats_provider=enums.AtsProvider.GREENHOUSE, ats_token="sourcegraph91")
        )
        await session.commit()

    board = fixture_json("greenhouse", "board_sourcegraph91.json")
    mock_greenhouse(router)
    full = router.get(GREENHOUSE_BOARD).mock(httpx.Response(200, json=board))
    connector = GreenhouseConnector(connector_config("greenhouse"), regions)
    await run_connector(connector, session_factory=session_factory, http=gh_client, now=NOW)
    assert await count(session_factory, Job) == 8

    # The next morning the board is down to three postings.
    full.mock(httpx.Response(200, json={"jobs": board["jobs"][:3], "meta": board.get("meta")}))
    later = NOW + timedelta(days=1)
    await run_connector(connector, session_factory=session_factory, http=gh_client, now=later)

    async with session_factory() as session:
        jobs = (await session.scalars(select(Job))).all()
    assert len(jobs) == 8  # nothing deleted — the database is the historical record
    assert sum(job.closed_at is None for job in jobs) == 3
    assert all(job.closed_at == later for job in jobs if job.closed_at is not None)

    refreshed = await company_by_domain(session_factory, "sourcegraph.com")
    assert refreshed.open_job_count == 3


# ------------------------------------------------------------------ hn_hiring end to end


async def test_hn_hiring_creates_a_company_with_a_job_and_a_published_contact(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    clock: FakeClock,
    regions: RegionsConfig,
) -> None:
    from ingest.connectors.hn_hiring import ITEM_URL, USER_URL

    story = fixture_json("hn_hiring", "item_49522897.json")
    discord = fixture_json("hn_hiring", "item_49525748.json")
    router.get("https://hacker-news.firebaseio.com/robots.txt").mock(
        httpx.Response(200, text=fixture_text("hn_hiring", "robots.txt"))
    )
    router.get(USER_URL).mock(httpx.Response(200, json={"submitted": [story["id"]]}))
    router.get(ITEM_URL.format(item_id=story["id"])).mock(
        httpx.Response(200, json={**story, "kids": [discord["id"]]})
    )
    router.get(ITEM_URL.format(item_id=discord["id"])).mock(httpx.Response(200, json=discord))

    async with make_client(connector_config("hn_hiring"), clock) as http:
        run = await run_connector(
            HnHiringConnector(connector_config("hn_hiring"), regions),
            session_factory=session_factory,
            http=http,
            now=NOW,
        )
    assert run.status is enums.FetchRunStatus.OK
    assert await count(session_factory, Company) == 1
    async with session_factory() as session:
        company = await session.scalar(select(Company))
        jobs = (await session.scalars(select(Job))).all()
    assert company is not None and company.name == "Discord"
    assert len(jobs) == 1
    assert jobs[0].role_family is enums.RoleFamily.SECURITY
    assert jobs[0].closed_at is None
    # A comment is a slice, so nothing was closed on the strength of it.
    assert company.open_job_count == 1


async def test_a_work_list_that_cannot_be_loaded_fails_the_run_but_still_records_it(
    session_factory: async_sessionmaker[AsyncSession],
    router: respx.MockRouter,
    regions: RegionsConfig,
    gh_client: HttpClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SPEC §7.2: "every run writes a FetchRun row regardless of outcome". The targets are
    loaded before the fetch, so a failure there must be recorded, not raised — and the fetch
    must not run at all, because a connector handed no targets looks like one with nothing
    to do."""

    async def explode(*args: object, **kwargs: object) -> list[object]:
        raise RuntimeError('relation "company_sources" does not exist')

    monkeypatch.setattr("ingest.pipeline.load_company_targets", explode)
    run = await run_connector(
        GreenhouseConnector(connector_config("greenhouse"), regions),
        session_factory=session_factory,
        http=gh_client,
        now=NOW,
    )
    assert run.status is enums.FetchRunStatus.ERROR
    assert run.error_text is not None and "targets:" in run.error_text
    assert (run.n_fetched, run.n_upserted) == (0, 0)
    assert run.finished_at is not None
    assert gh_client.stats.requests == 0
