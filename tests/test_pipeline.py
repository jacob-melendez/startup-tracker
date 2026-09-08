"""``ingest.pipeline`` — ``FetchRun`` bookkeeping (SPEC §7.2), priority-aware upserts through
``field_provenance`` (SPEC §8), never-deleted jobs (SPEC §2, §5), the constructed LinkedIn links
reconciled against what a company published (SPEC §6) and the end-of-run refresh of the
denormalized columns (SPEC §5).

Everything runs against the real migrated Postgres (``session_factory`` / ``session``
fixtures, SPEC §3). Nothing touches the network: the :class:`FakeConnector` yields prepared
dicts and the ``HttpClient`` handed to the run is never asked to fetch (asserted below).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, event, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.models import (
    Base,
    Company,
    CompanyLocation,
    CompanySector,
    CompanySource,
    Contact,
    FetchRun,
    FundingRound,
    Investor,
    Job,
    Location,
    MergeCandidate,
    Person,
    RoundInvestor,
    Sector,
    Source,
)
from db.queries import refresh_company_denormalized_columns
from ingest.base import CompanyRecord, Connector, FetchContext
from ingest.config import ConnectorConfig, RegionsConfig, load_regions_config
from ingest.contacts import people_search_url
from ingest.http import HttpClient, MemoryCache
from ingest.normalize import TRIGRAM_THRESHOLD, normalize_name
from ingest.pipeline import (
    COMPANY_MERGE_FIELDS,
    CONNECTOR_PRIORITY,
    ERROR_TEXT_LIMIT,
    ContactSyncResult,
    UpsertResult,
    connector_rank,
    join_error_messages,
    merge_company_fields,
    run_connector,
    sync_constructed_contacts,
    upsert_company_record,
)

UA = "startup-tracker/0.1 tests@example.com"
BAY_AREA = "Bay Area"
NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
DAY = timedelta(days=1)
REGIONS = load_regions_config()

SF: dict[str, Any] = {"city": "San Francisco", "state": "CA", "is_hq": True}
PALO_ALTO: dict[str, Any] = {"city": "Palo Alto", "state": "CA"}
#: A city no region in ``config/regions.yaml`` claims — the case ``_fill_metros`` drops. Which
#: city that is depends on the file, so the test asserts it instead of trusting this comment:
#: §12 Phase 7 took the config from one metro to six and the city this constant used to name
#: became a configured one, which would have left the test passing while testing nothing.
UNCONFIGURED: dict[str, Any] = {"city": "Chicago", "state": "IL"}

# ``similarity('applied intuition system', 'applied intuition systems')`` on pg_trgm 1.6
# (see tests/test_normalize.py).
NEAR_DUPLICATE_SIMILARITY = 0.8889


# ------------------------------------------------------------------------------ helpers


def make_record(**overrides: Any) -> CompanyRecord:
    """A Bay Area company record with sensible defaults; keyword overrides replace fields."""
    data: dict[str, Any] = {"name": "Acme, Inc.", "external_id": "acme-1", "locations": [SF]}
    data.update(overrides)
    return CompanyRecord.model_validate(data)


def item(**overrides: Any) -> dict[str, Any]:
    """The raw-item form of :func:`make_record` for :class:`FakeConnector`."""
    return make_record(**overrides).model_dump()


def job(external_id: str, **overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {"external_id": external_id, "title": f"Engineer {external_id}"}
    data.update(overrides)
    return data


def funding_round(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "external_id": "021-000001",
        "round_type": enums.RoundType.SAFE,
        "amount_usd": 1_000_000,
        "announced_date": date(2026, 3, 1),
        "raw_payload": {"accession": "0001-26-000001"},
    }
    data.update(overrides)
    return data


class FakeConnector(Connector[dict[str, Any]]):
    """Yields prepared dicts; ``to_records`` validates each one as a ``CompanyRecord`` — so an
    invalid dict (``{"name": ""}``) is a failing item, exactly like a corrupt filing. A dict
    that validates but that the database refuses (an ``amount_usd`` beyond ``BIGINT``) fails
    later, inside its own upsert transaction. No HTTP.

    ``fail_after=N`` makes ``fetch`` raise after yielding ``N`` items; ``problems`` are
    appended to ``ctx.problems`` (SPEC §7.2 non-fatal trouble)."""

    name = "fake"

    def __init__(
        self,
        items: Sequence[dict[str, Any]],
        *,
        fail_after: int | None = None,
        problems: Sequence[str] = (),
    ) -> None:
        super().__init__(ConnectorConfig(name=self.name), REGIONS)
        self.items = list(items)
        self.fail_after = fail_after
        self.problems = list(problems)
        self.contexts: list[FetchContext] = []

    async def fetch(self, ctx: FetchContext) -> AsyncIterator[dict[str, Any]]:
        self.contexts.append(ctx)
        ctx.problems.extend(self.problems)
        for index, raw in enumerate(self.items):
            if index == self.fail_after:
                raise RuntimeError("source exploded")
            yield raw
        if self.fail_after is not None and self.fail_after >= len(self.items):
            raise RuntimeError("source exploded")

    def to_records(self, raw: dict[str, Any]) -> Iterable[CompanyRecord]:
        return [CompanyRecord.model_validate(raw)]


@pytest.fixture
async def http() -> AsyncIterator[HttpClient]:
    async def no_sleep(seconds: float) -> None:
        return None

    async with HttpClient(user_agent=UA, cache=MemoryCache(), sleep=no_sleep) as client:
        yield client


SessionFactory = async_sessionmaker[AsyncSession]


async def run(
    session_factory: SessionFactory,
    http: HttpClient,
    items: Sequence[dict[str, Any]],
    *,
    now: datetime = NOW,
    **kwargs: Any,
) -> tuple[FetchRun, FakeConnector]:
    connector = FakeConnector(items, **kwargs)
    fetch_run = await run_connector(connector, session_factory=session_factory, http=http, now=now)
    return fetch_run, connector


async def count(session: AsyncSession, model: type[Base]) -> int:
    return await session.scalar(select(func.count()).select_from(model)) or 0


async def counts(session_factory: SessionFactory, *models: type[Base]) -> dict[str, int]:
    async with session_factory() as session:
        return {model.__tablename__: await count(session, model) for model in models}


async def only_company(session_factory: SessionFactory) -> Company:
    async with session_factory() as session:
        (company,) = (await session.execute(select(Company))).scalars().all()
        return company


async def stored_run(session_factory: SessionFactory, run_id: int) -> FetchRun:
    async with session_factory() as session:
        fetch_run = await session.get(FetchRun, run_id)
        assert fetch_run is not None
        return fetch_run


async def merge_candidates(session: AsyncSession) -> list[MergeCandidate]:
    stmt = select(MergeCandidate).execution_options(populate_existing=True)
    return list((await session.execute(stmt)).scalars())


async def stored_contacts(
    session: AsyncSession, kind: enums.ContactKind | None = None
) -> list[Contact]:
    """This company's contacts in insertion order, optionally of one kind.

    ``populate_existing`` because :mod:`ingest.pipeline` writes contacts with Core statements
    (``INSERT ... ON CONFLICT``, ``DELETE``): a row already in the session's identity map would
    otherwise be handed back with its pre-upsert ``confidence``.
    """
    stmt = select(Contact).execution_options(populate_existing=True).order_by(Contact.id)
    if kind is not None:
        stmt = stmt.where(Contact.kind == kind)
    return list((await session.execute(stmt)).scalars())


def search_keywords(url: str) -> str:
    """The decoded ``keywords`` of a people-search deep link (SPEC §6 "People search").

    Decoding is the point: it fails loudly if the link were assembled by string concatenation
    instead of ``urlencode``, and it shows the expression the user actually searches with.
    """
    (keywords,) = parse_qs(urlsplit(url).query)["keywords"]
    return keywords


# ----------------------------------------------------------- priority (SPEC §8, decision 1)


def test_connector_priority_matches_the_spec_order() -> None:
    assert CONNECTOR_PRIORITY == (
        "sec_edgar",
        "ycombinator",
        "greenhouse",
        "lever",
        "ashby",
        "workable",
        "company_site",
        "funding_rss",
        "product_hunt",
    )
    ranks = [connector_rank(name) for name in CONNECTOR_PRIORITY]
    assert ranks == sorted(ranks, reverse=True)
    assert connector_rank("product_hunt") > connector_rank("hn_hiring")
    assert connector_rank("hn_hiring") == connector_rank("seed") == connector_rank("fake") == 0


def test_merge_fields_are_the_spec_columns() -> None:
    assert COMPANY_MERGE_FIELDS == (
        "name",
        "domain",
        "website_url",
        "one_liner",
        "thesis",
        "founded_year",
        "employee_est",
        "stage",
        "status",
        "ats_provider",
        "ats_token",
    )


@pytest.mark.parametrize(
    ("existing", "provenance", "incoming", "connector", "updates", "after"),
    [
        pytest.param(
            {},
            {},
            {"name": "Acme, Inc.", "one_liner": "x", "thesis": None},
            "sec_edgar",
            {"name": "Acme, Inc.", "normalized_name": "acme", "one_liner": "x"},
            {"name": "sec_edgar", "one_liner": "sec_edgar"},
            id="new company: every non-null field, None left out",
        ),
        pytest.param(
            {"name": "Acme, Inc.", "one_liner": "x"},
            {"name": "sec_edgar", "one_liner": "sec_edgar"},
            {"name": "Acme", "thesis": "t"},
            "hn_hiring",
            {"thesis": "t"},
            {"name": "sec_edgar", "one_liner": "sec_edgar", "thesis": "hn_hiring"},
            id="lower-ranked fills gaps but does not overwrite",
        ),
        pytest.param(
            {"name": "Acme Corp"},
            {"name": "hn_hiring"},
            {"name": "Acme Robotics, Inc."},
            "sec_edgar",
            {"name": "Acme Robotics, Inc.", "normalized_name": "acme robotics"},
            {"name": "sec_edgar"},
            id="higher-ranked overwrites and normalized_name follows",
        ),
        pytest.param(
            {"one_liner": "v1"},
            {"one_liner": "hn_hiring"},
            {"one_liner": "v2"},
            "hn_hiring",
            {"one_liner": "v2"},
            {"one_liner": "hn_hiring"},
            id="same connector refreshes itself",
        ),
        pytest.param(
            {"one_liner": "x"},
            {"one_liner": "hn_hiring"},
            {"one_liner": None},
            "sec_edgar",
            {},
            {"one_liner": "hn_hiring"},
            id="None never overwrites, whoever sends it",
        ),
        pytest.param(
            {"one_liner": "from seed"},
            {"one_liner": "seed"},
            {"one_liner": "from hn"},
            "hn_hiring",
            {},
            {"one_liner": "seed"},
            id="two distinct unlisted connectors never overwrite each other",
        ),
        pytest.param(
            {"one_liner": "from hn"},
            {"one_liner": "hn_hiring"},
            {"one_liner": "from seed"},
            "seed",
            {},
            {"one_liner": "hn_hiring"},
            id="... in either direction",
        ),
        pytest.param(
            {"one_liner": "legacy"},
            {},
            {"one_liner": "new"},
            "hn_hiring",
            {"one_liner": "new"},
            {"one_liner": "hn_hiring"},
            id="a stored field without provenance is writable",
        ),
        pytest.param(
            {"thesis": None},
            {"thesis": "sec_edgar"},
            {"thesis": "t"},
            "hn_hiring",
            {"thesis": "t"},
            {"thesis": "hn_hiring"},
            id="a NULL stored value is writable even below the owner's rank",
        ),
        pytest.param(
            {"domain": "acme.com"},
            {"domain": "hn_hiring"},
            {"domain": "acme.io"},
            "sec_edgar",
            {},
            {"domain": "hn_hiring"},
            id="domain is only ever filled, never changed (decision 1)",
        ),
        pytest.param(
            {"domain": None},
            {},
            {"domain": "acme.com"},
            "product_hunt",
            {"domain": "acme.com"},
            {"domain": "product_hunt"},
            id="an empty domain is filled by anyone",
        ),
        pytest.param(
            {"name": "Acme"},
            {"name": "hn_hiring"},
            {"name": "Acme"},
            "sec_edgar",
            {},
            {"name": "sec_edgar"},
            id="an unchanged value moves provenance without an update",
        ),
        pytest.param(
            {"stage": enums.Stage.SEED},
            {"stage": "ycombinator"},
            {"stage": enums.Stage.SERIES_A, "status": enums.CompanyStatus.ACQUIRED},
            "greenhouse",
            {"status": enums.CompanyStatus.ACQUIRED},
            {"stage": "ycombinator", "status": "greenhouse"},
            id="enum fields follow the same rule",
        ),
    ],
)
def test_merge_company_fields(
    existing: dict[str, Any],
    provenance: dict[str, str],
    incoming: dict[str, Any],
    connector: str,
    updates: dict[str, Any],
    after: dict[str, str],
) -> None:
    assert merge_company_fields(existing, provenance, incoming, connector) == (updates, after)


def test_merge_company_fields_does_not_mutate_its_inputs() -> None:
    provenance = {"name": "seed"}
    merge_company_fields({"name": "A"}, provenance, {"name": "B"}, "sec_edgar")
    assert provenance == {"name": "seed"}


# ---------------------------------------------------------------- error_text (SPEC §7.2)


def test_join_error_messages() -> None:
    assert join_error_messages([]) is None
    assert join_error_messages(["a", "b"]) == "a\nb"

    many = [f"message {i:03d} " + "x" * 90 for i in range(100)]
    joined = join_error_messages(many)
    assert joined is not None
    assert len(joined) <= ERROR_TEXT_LIMIT
    lines = joined.split("\n")
    assert lines[0] == many[0]
    assert lines[-1] == f"... (+{100 - (len(lines) - 1)} more)"
    assert len(lines) - 1 < 100

    huge = join_error_messages(["y" * 10_000, "z"], limit=100)
    assert huge is not None
    assert len(huge) <= 100
    assert huge.endswith("\n... (+1 more)")


# ---------------------------------------------------------------- run_connector (SPEC §7.2)


async def test_clean_run_writes_an_ok_fetch_run(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    fetch_run, connector = await run(
        session_factory,
        http,
        [item(), item(name="Beta Labs", external_id="beta")],
    )

    assert fetch_run.status is enums.FetchRunStatus.OK
    assert fetch_run.connector == "fake"
    assert fetch_run.started_at == NOW
    assert fetch_run.finished_at is not None
    assert (fetch_run.n_fetched, fetch_run.n_upserted) == (2, 2)
    assert fetch_run.error_text is None
    stored = await stored_run(session_factory, fetch_run.id)
    assert (stored.status, stored.n_fetched, stored.n_upserted) == (
        enums.FetchRunStatus.OK,
        2,
        2,
    )
    assert stored.finished_at == fetch_run.finished_at
    assert await counts(session_factory, Company) == {"companies": 2}
    # The connector never touched the wire (SPEC §2: the pipeline only writes).
    assert http.stats.requests == 0
    (ctx,) = connector.contexts
    assert (ctx.run_id, ctx.now, ctx.since, ctx.last_success_at) == (fetch_run.id, NOW, None, None)


async def test_fetch_raising_is_recorded_as_an_error_run(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    fetch_run, _ = await run(session_factory, http, [item()], fail_after=0)

    assert fetch_run.status is enums.FetchRunStatus.ERROR
    assert fetch_run.error_text is not None
    assert "RuntimeError: source exploded" in fetch_run.error_text
    assert (fetch_run.n_fetched, fetch_run.n_upserted) == (0, 0)
    assert fetch_run.finished_at is not None
    stored = await stored_run(session_factory, fetch_run.id)
    assert stored.status is enums.FetchRunStatus.ERROR
    assert await counts(session_factory, Company, FetchRun) == {"companies": 0, "fetch_runs": 1}


async def test_one_bad_record_among_three_is_partial(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    fetch_run, _ = await run(
        session_factory,
        http,
        [item(), {"name": ""}, item(name="Gamma", external_id="gamma")],
    )

    assert fetch_run.status is enums.FetchRunStatus.PARTIAL
    assert (fetch_run.n_fetched, fetch_run.n_upserted) == (3, 2)
    assert fetch_run.error_text is not None
    assert fetch_run.error_text.startswith("item 2: to_records: ValidationError")
    assert await counts(session_factory, Company) == {"companies": 2}


async def test_problems_make_the_run_partial(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    fetch_run, _ = await run(
        session_factory,
        http,
        [item()],
        problems=["filing 0001-26-000009: HTTP 404", "window 2026-08 Palo Alto: 10001 hits"],
    )

    assert fetch_run.status is enums.FetchRunStatus.PARTIAL
    assert (fetch_run.n_fetched, fetch_run.n_upserted) == (1, 1)
    assert fetch_run.error_text == (
        "filing 0001-26-000009: HTTP 404\nwindow 2026-08 Palo Alto: 10001 hits"
    )


async def test_second_run_sees_the_first_runs_started_at(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    first, connector_1 = await run(session_factory, http, [item()], now=NOW)
    second, connector_2 = await run(session_factory, http, [item()], now=NOW + DAY)

    assert connector_1.contexts[0].last_success_at is None
    assert connector_2.contexts[0].last_success_at == first.started_at == NOW
    assert second.started_at == NOW + DAY


async def test_error_runs_never_anchor_the_window_but_partial_runs_do(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    """A fetch that dies after two good records is ``error`` with ``n_upserted=2``; the next
    run ignores it as an anchor (the scan did not complete), while a ``partial`` run — which
    scanned everything — is used (design brief step 1)."""
    failed, _ = await run(
        session_factory,
        http,
        [item(), item(name="Beta", external_id="beta")],
        fail_after=2,
        now=NOW,
    )
    assert failed.status is enums.FetchRunStatus.ERROR
    assert (failed.n_fetched, failed.n_upserted) == (2, 2)
    assert failed.error_text is not None
    assert failed.error_text.startswith("fetch: RuntimeError: source exploded")
    assert await counts(session_factory, Company) == {"companies": 2}

    partial, connector_2 = await run(
        session_factory, http, [item()], problems=["one filing 404ed"], now=NOW + DAY
    )
    assert connector_2.contexts[0].last_success_at is None
    assert partial.status is enums.FetchRunStatus.PARTIAL

    _, connector_3 = await run(session_factory, http, [item()], now=NOW + 2 * DAY)
    assert connector_3.contexts[0].last_success_at == partial.started_at == NOW + DAY


async def test_bad_items_do_not_roll_back_their_neighbours(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    """Two items that fail in ``to_records`` (Pydantic validation — before any session is
    opened) among four are skipped and reported, and the two good companies land with their
    provenance rows. The per-record transaction boundary itself — a record that fails *inside*
    the database — is covered by :func:`test_failed_upsert_is_isolated_and_the_run_continues`."""
    fetch_run, _ = await run(
        session_factory,
        http,
        [item(), {"name": ""}, item(name="Gamma", external_id="gamma"), {"name": ""}],
    )
    assert fetch_run.status is enums.FetchRunStatus.PARTIAL
    assert (fetch_run.n_fetched, fetch_run.n_upserted) == (4, 2)
    assert fetch_run.error_text is not None
    assert fetch_run.error_text.count("to_records: ValidationError") == 2
    assert await counts(session_factory, Company, CompanySource) == {
        "companies": 2,
        "company_sources": 2,
    }


async def test_failed_upsert_is_isolated_and_the_run_continues(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    """Design brief step 3, the *upsert* half: every record is written in its own
    transaction, so a record the database refuses mid-transaction is rolled back alone — the
    record before it stays committed, the record after it still lands, and the run is
    ``partial`` with the failure named in ``error_text``.

    ``amount_usd=10**19`` passes Pydantic (``int``, ``ge=0``) but does not fit ``BIGINT``, so
    the failure happens at flush time, after the company row and its ``Source`` were already
    written inside that transaction — unlike ``{"name": ""}``, which fails in ``to_records``
    before any session is opened."""
    fetch_run, _ = await run(
        session_factory,
        http,
        [
            item(name="Good One", external_id="g1"),
            item(
                name="Overflow",
                external_id="o1",
                funding_rounds=[funding_round(amount_usd=10**19)],
            ),
            item(name="Good Two", external_id="g2"),
        ],
    )

    assert fetch_run.status is enums.FetchRunStatus.PARTIAL
    assert (fetch_run.n_fetched, fetch_run.n_upserted) == (3, 2)
    assert fetch_run.error_text is not None
    assert fetch_run.error_text.startswith("item 2 ('Overflow'): upsert: ")
    assert "\n" not in fetch_run.error_text  # exactly one item failed
    stored = await stored_run(session_factory, fetch_run.id)
    assert (stored.status, stored.n_fetched, stored.n_upserted, stored.error_text) == (
        enums.FetchRunStatus.PARTIAL,
        3,
        2,
        fetch_run.error_text,
    )
    # The failed record's transaction rolled back as a whole — no company, no ``Source``, no
    # round of its own — while both neighbours are committed with their provenance rows.
    assert await counts(session_factory, Company, CompanySource, Source, FundingRound) == {
        "companies": 2,
        "company_sources": 2,
        "sources": 2,
        "funding_rounds": 0,
    }
    async with session_factory() as session:
        names = (await session.execute(select(Company.name).order_by(Company.id))).scalars()
        assert list(names) == ["Good One", "Good Two"]


# ------------------------------------------------ upsert_company_record: company row (SPEC §8)


async def test_lower_ranked_connector_fills_gaps_but_never_overwrites(
    session: AsyncSession,
) -> None:
    first = await upsert_company_record(
        session,
        make_record(name="Obsidian Security, Inc.", one_liner="Identity threat detection"),
        "sec_edgar",
        now=NOW,
    )
    assert first.created
    # No external id and no domain: resolves through normalized_name within the metro.
    second = await upsert_company_record(
        session,
        make_record(name="Obsidian Security", external_id=None, thesis="Long thesis"),
        "hn_hiring",
        now=NOW + DAY,
    )

    assert second == UpsertResult(first.company_id, created=False, merge_candidates=0)
    company = await session.get(Company, first.company_id)
    assert company is not None
    assert company.name == "Obsidian Security, Inc."
    assert company.normalized_name == "obsidian security"
    assert company.one_liner == "Identity threat detection"
    assert company.thesis == "Long thesis"
    assert company.field_provenance == {
        "name": "sec_edgar",
        "one_liner": "sec_edgar",
        "thesis": "hn_hiring",
    }
    assert (company.first_seen_at, company.last_seen_at) == (NOW, NOW + DAY)
    assert await count(session, Company) == 1


async def test_higher_ranked_connector_overwrites_and_normalized_name_follows(
    session: AsyncSession,
) -> None:
    first = await upsert_company_record(
        session,
        make_record(name="Acme Corp", external_id=None, domain="acme.com"),
        "hn_hiring",
        now=NOW,
    )
    second = await upsert_company_record(
        session,
        make_record(
            name="Acme Robotics, Inc.", external_id="cik-1", domain="https://www.acme.com/"
        ),
        "sec_edgar",
        now=NOW,
    )

    assert second.company_id == first.company_id
    company = await session.get(Company, first.company_id)
    assert company is not None
    assert (company.name, company.normalized_name) == ("Acme Robotics, Inc.", "acme robotics")
    assert company.domain == "acme.com"
    assert company.field_provenance == {"name": "sec_edgar", "domain": "hn_hiring"}


async def test_same_connector_refreshes_itself_and_none_never_overwrites(
    session: AsyncSession,
) -> None:
    first = await upsert_company_record(session, make_record(one_liner="v1"), "sec_edgar", now=NOW)
    await upsert_company_record(session, make_record(one_liner="v2"), "sec_edgar", now=NOW)
    company = await session.get(Company, first.company_id)
    assert company is not None
    assert company.one_liner == "v2"

    await upsert_company_record(session, make_record(one_liner=None), "sec_edgar", now=NOW)
    await upsert_company_record(
        session, make_record(external_id=None, one_liner=None), "ycombinator", now=NOW
    )
    assert company.one_liner == "v2"
    assert company.field_provenance == {"name": "sec_edgar", "one_liner": "sec_edgar"}
    assert await count(session, Company) == 1


async def test_same_domain_in_different_forms_is_one_company(session: AsyncSession) -> None:
    first = await upsert_company_record(
        session,
        make_record(name="Acme, Inc.", external_id="1", domain="https://www.Acme.com/about?x=1"),
        "hn_hiring",
        now=NOW,
    )
    second = await upsert_company_record(
        session,
        make_record(name="Acme Holdings", external_id="2", domain="ACME.COM/"),
        "hn_hiring",
        now=NOW,
    )

    assert (first.created, second.created) == (True, False)
    assert first.company_id == second.company_id
    company = await session.get(Company, first.company_id)
    assert company is not None
    assert company.domain == "acme.com"
    assert await count(session, Company) == 1


async def test_unparseable_domain_is_treated_as_unknown(session: AsyncSession) -> None:
    result = await upsert_company_record(
        session, make_record(domain="not a domain"), "hn_hiring", now=NOW
    )
    company = await session.get(Company, result.company_id)
    assert company is not None
    assert company.domain is None
    assert "domain" not in company.field_provenance


async def test_domainless_record_resolves_by_external_id_on_the_second_run(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    await run(session_factory, http, [item(name="Acme, Inc.", external_id="cik-1")])
    # A different (renamed) company name defeats the name match; only step 0 can resolve it.
    await run(
        session_factory,
        http,
        [item(name="Acme Robotics Holdings", external_id="cik-1")],
        now=NOW + DAY,
    )

    assert await counts(session_factory, Company, CompanySource) == {
        "companies": 1,
        "company_sources": 1,
    }
    company = await only_company(session_factory)
    assert company.name == "Acme Robotics Holdings"  # same connector refreshes itself
    async with session_factory() as session:
        (link,) = (await session.execute(select(CompanySource))).scalars().all()
    assert (link.connector, link.external_id, link.last_seen_at) == ("fake", "cik-1", NOW + DAY)


async def test_company_source_keeps_a_known_external_id(session: AsyncSession) -> None:
    result = await upsert_company_record(
        session, make_record(external_id="cik-1", domain="acme.com"), "sec_edgar", now=NOW
    )
    await upsert_company_record(
        session, make_record(external_id=None, domain="acme.com"), "sec_edgar", now=NOW + DAY
    )
    link = await session.get(CompanySource, (result.company_id, "sec_edgar"))
    assert link is not None
    await session.refresh(link)
    assert (link.external_id, link.last_seen_at) == ("cik-1", NOW + DAY)


async def test_domain_belonging_to_another_company_is_not_moved(session: AsyncSession) -> None:
    """Design decision 1: the pair becomes a ``MergeCandidate`` (similarity 1.0); nothing is
    merged (SPEC §8) and the domain stays where it was."""
    alpha = await upsert_company_record(
        session,
        make_record(name="Alpha", external_id="a", domain="alpha.com"),
        "hn_hiring",
        now=NOW,
    )
    beta = await upsert_company_record(
        session, make_record(name="Beta", external_id="b"), "hn_hiring", now=NOW
    )

    result = await upsert_company_record(
        session, make_record(name="Beta", external_id="b", domain="alpha.com"), "hn_hiring", now=NOW
    )

    assert result == UpsertResult(beta.company_id, created=False, merge_candidates=1)
    assert await count(session, Company) == 2
    alpha_row = await session.get(Company, alpha.company_id)
    beta_row = await session.get(Company, beta.company_id)
    assert alpha_row is not None and beta_row is not None
    assert (alpha_row.domain, beta_row.domain) == ("alpha.com", None)
    assert "domain" not in beta_row.field_provenance
    (candidate,) = await merge_candidates(session)
    assert (candidate.company_id_a, candidate.company_id_b) == (alpha.company_id, beta.company_id)
    assert candidate.similarity == pytest.approx(1.0)
    assert candidate.reason == "domain alpha.com reported for both companies"

    # A re-run refreshes the same row instead of adding one.
    again = await upsert_company_record(
        session, make_record(name="Beta", external_id="b", domain="alpha.com"), "hn_hiring", now=NOW
    )
    assert again.merge_candidates == 1
    assert len(await merge_candidates(session)) == 1


async def test_domain_conflict_waits_for_a_concurrent_insert_of_the_same_domain(
    session_factory: SessionFactory,
) -> None:
    """Design decision 1 under concurrency (the four ATS connectors share one cadence): while
    another run's transaction has inserted ``alpha.com`` but not yet committed, a record that
    would fill Beta's domain with ``alpha.com`` must wait for that transaction and then take
    the conflict path — domain not written, one ``MergeCandidate`` — instead of tripping
    ``ix_companies_domain`` with an ``IntegrityError`` that loses the whole record."""
    async with session_factory() as session, session.begin():
        beta = await upsert_company_record(
            session, make_record(name="Beta", external_id="b"), "hn_hiring", now=NOW
        )

    async def upsert_beta_with_alphas_domain(session: AsyncSession) -> UpsertResult:
        async with session.begin():
            return await upsert_company_record(
                session,
                make_record(name="Beta", external_id="b", domain="alpha.com"),
                "hn_hiring",
                now=NOW,
            )

    other_run = session_factory()
    this_run = session_factory()
    try:
        async with other_run.begin():
            alpha = await upsert_company_record(
                other_run,
                make_record(name="Alpha", external_id="a", domain="alpha.com"),
                "hn_hiring",
                now=NOW,
            )
            task = asyncio.create_task(upsert_beta_with_alphas_domain(this_run))
            await asyncio.sleep(0.3)
            waited = not task.done()
        # ``other_run`` has committed; the waiting update may now proceed.
        result = await asyncio.wait_for(task, timeout=10)
    finally:
        await this_run.close()
        await other_run.close()

    assert waited, "the domain write must wait for the uncommitted insert"
    assert result == UpsertResult(beta.company_id, created=False, merge_candidates=1)
    async with session_factory() as session:
        assert await count(session, Company) == 2
        alpha_row = await session.get(Company, alpha.company_id)
        beta_row = await session.get(Company, beta.company_id)
        assert alpha_row is not None and beta_row is not None
        assert (alpha_row.domain, beta_row.domain) == ("alpha.com", None)
        assert "domain" not in beta_row.field_provenance
        (candidate,) = await merge_candidates(session)
    # Beta was created first here, and the pair is stored as (lower id, higher id).
    assert (candidate.company_id_a, candidate.company_id_b) == (beta.company_id, alpha.company_id)
    assert candidate.similarity == pytest.approx(1.0)
    assert candidate.reason == "domain alpha.com reported for both companies"


async def test_concurrent_domain_less_records_resolve_to_one_company(
    session_factory: SessionFactory,
) -> None:
    """SPEC §8 step 2 under concurrency. Two runs create the same domain-less company (same
    normalized name, same metro) at once — the Phase 6 scheduler overlaps connectors, and
    EDGAR companies have no domain at all. Without a lock both pass ``resolve_company`` and
    insert, leaving two rows that step 3 will never flag either (it excludes exact-equal
    names), so the duplicate is permanent and invisible to ``merge-review``."""

    async def upsert_dup(session: AsyncSession, connector: str) -> UpsertResult:
        async with session.begin():
            return await upsert_company_record(
                session, make_record(name="Dup Labs", external_id=None), connector, now=NOW
            )

    first_run = session_factory()
    second_run = session_factory()
    try:
        async with first_run.begin():
            first = await upsert_company_record(
                first_run, make_record(name="Dup Labs", external_id=None), "funding_rss", now=NOW
            )
            task = asyncio.create_task(upsert_dup(second_run, "product_hunt"))
            await asyncio.sleep(0.3)
            waited = not task.done()
        second = await asyncio.wait_for(task, timeout=10)
    finally:
        await second_run.close()
        await first_run.close()

    assert waited, "the second insert must wait for the uncommitted one"
    assert (first.created, second.created) == (True, False)
    assert second.company_id == first.company_id
    async with session_factory() as session:
        assert await count(session, Company) == 1


async def test_naive_timestamps_are_rejected_rather_than_shifted(
    session: AsyncSession, session_factory: SessionFactory, http: HttpClient
) -> None:
    """Design decision 8: every timestamp is timezone-aware UTC. ``timestamptz`` columns take a
    naive datetime as *local* time, so a naive value would silently shift every row by the
    host's UTC offset — ``Job.posted_at`` and the ``latest_job_posted_at`` sort included."""
    naive = datetime(2026, 1, 1, 12, 0)  # deliberately naive
    with pytest.raises(ValueError, match="timezone-aware"):
        await upsert_company_record(session, make_record(), "hn_hiring", now=naive)
    with pytest.raises(ValueError, match="timezone-aware"):
        await run_connector(
            FakeConnector([]), session_factory=session_factory, http=http, now=naive
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        await run_connector(
            FakeConnector([]), session_factory=session_factory, http=http, since=naive
        )
    # A connector that builds posted_at from an epoch or a date string fails validation, so the
    # run records it as an item error instead of storing a shifted timestamp.
    with pytest.raises(ValidationError, match="timezone"):
        make_record(jobs=[job("j1", posted_at=naive)])


async def test_stored_domain_is_never_changed(session: AsyncSession) -> None:
    first = await upsert_company_record(
        session, make_record(name="Gamma", external_id=None, domain="old.com"), "hn_hiring", now=NOW
    )
    # sec_edgar outranks hn_hiring and resolves to the same company by name within the metro.
    second = await upsert_company_record(
        session,
        make_record(name="Gamma", external_id="cik-9", domain="new.com"),
        "sec_edgar",
        now=NOW,
    )

    assert second == UpsertResult(first.company_id, created=False, merge_candidates=0)
    company = await session.get(Company, first.company_id)
    assert company is not None
    assert company.domain == "old.com"
    assert company.field_provenance["domain"] == "hn_hiring"
    assert company.field_provenance["name"] == "sec_edgar"
    assert await count(session, Company) == 1


async def test_trigram_near_duplicate_is_recorded_not_merged(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    fetch_run, _ = await run(
        session_factory,
        http,
        [
            item(name="Applied Intuition System", external_id="1"),
            item(name="Applied Intuition Systems", external_id="2"),
        ],
    )

    assert fetch_run.status is enums.FetchRunStatus.OK
    assert await counts(session_factory, Company) == {"companies": 2}
    async with session_factory() as session:
        ids = sorted((await session.execute(select(Company.id))).scalars())
        (candidate,) = await merge_candidates(session)
    assert (candidate.company_id_a, candidate.company_id_b) == (ids[0], ids[1])
    assert candidate.similarity == pytest.approx(NEAR_DUPLICATE_SIMILARITY, abs=1e-4)
    assert candidate.similarity > TRIGRAM_THRESHOLD
    assert candidate.reason.startswith("trigram similarity 0.89 on normalized_name")
    assert candidate.resolved_at is None


# ----------------------------------------------- upsert_company_record: child rows (SPEC §5)


async def test_metro_less_location_is_skipped_and_run_stays_ok(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    """SPEC §1: nothing outside the configured regions is stored. The record itself still lands
    — a company is not lost because one of its offices is somewhere the config does not list —
    and the run stays ``ok``, because a national source naming out-of-region cities is the
    normal case, not trouble worth a ``partial``."""
    assert REGIONS.lookup(UNCONFIGURED["city"], UNCONFIGURED["state"]) is None, (
        "this test needs a city no configured region claims; config/regions.yaml now claims "
        f"{UNCONFIGURED['city']}"
    )
    fetch_run, _ = await run(
        session_factory,
        http,
        [
            item(locations=[UNCONFIGURED, SF]),
            item(name="Nowhere Labs", external_id="nowhere", locations=[UNCONFIGURED]),
        ],
    )

    assert fetch_run.status is enums.FetchRunStatus.OK
    assert (fetch_run.n_fetched, fetch_run.n_upserted) == (2, 2)
    assert await counts(session_factory, Company, Location, CompanyLocation) == {
        "companies": 2,
        "locations": 1,
        "company_locations": 1,
    }
    async with session_factory() as session:
        (location,) = (await session.execute(select(Location))).scalars().all()
        (link,) = (await session.execute(select(CompanyLocation))).scalars().all()
    assert (location.city, location.state, location.metro) == ("San Francisco", "CA", BAY_AREA)
    assert link.is_hq is True


async def test_configured_city_always_takes_the_configured_metro(session: AsyncSession) -> None:
    """``config/regions.yaml`` is the only source of region labels (CLAUDE.md): a connector's
    own spelling of the metro for a configured city (``"bay area"``) is replaced by the
    configured one, so the stored ``Location.metro`` and the resolution metro of SPEC §8
    steps 2/3 always agree — an off-config label would match no ``Location`` and let a
    duplicate company through."""
    first = await upsert_company_record(
        session,
        make_record(external_id="a", locations=[{**SF, "metro": "bay area"}]),
        "hn_hiring",
        now=NOW,
    )
    # No external id and another connector: only name-within-metro (step 2) can resolve this.
    second = await upsert_company_record(
        session,
        make_record(external_id=None, locations=[{**SF, "metro": "SF Bay Area"}]),
        "other",
        now=NOW,
    )

    assert second == UpsertResult(first.company_id, created=False, merge_candidates=0)
    assert await count(session, Company) == 1
    (location,) = (await session.execute(select(Location))).scalars().all()
    assert (location.city, location.state, location.metro) == ("San Francisco", "CA", BAY_AREA)


#: A city that exists nowhere but in the region file handed to the test. Invented on purpose:
#: no code path can know where ``Rivermouth`` is from geography, a fixture or a shipped config,
#: so whatever metro the row ends up with came from the configuration under test and from
#: nothing else.
RIVERMOUTH: dict[str, Any] = {"city": "Rivermouth", "state": "ZZ", "is_hq": True}


def one_city_regions(metro: str, *, country: str = "US") -> RegionsConfig:
    """A region config holding :data:`RIVERMOUTH` alone, filed under ``metro``.

    Built in memory rather than written to YAML because ``load_regions_config`` caches on the
    path: two files describing the same city under different metros would need two paths, and a
    reader would have to check that they really do differ in the one field that matters.
    """
    return RegionsConfig.model_validate(
        {
            "regions": [
                {
                    "metro": metro,
                    "state": RIVERMOUTH["state"],
                    "country": country,
                    "cities": [RIVERMOUTH["city"]],
                }
            ]
        }
    )


async def test_a_city_reassigned_in_the_config_is_relabelled_on_the_next_ingest(
    session: AsyncSession,
) -> None:
    """SPEC §11, §12 Phase 7: ``config/regions.yaml`` is the only place region logic may live,
    so where the file and a stored row disagree the file wins.

    ``_upsert_locations`` used to say ``ON CONFLICT DO NOTHING``, which made the metro a city
    was *first* ingested with the metro it kept for ever. The region logic then lived in the
    ``locations`` table — a row nobody can edit by editing YAML — and moving a city between
    metros, or renaming one, changed only what future cities were labelled. ``DO UPDATE`` is
    what makes the config authoritative over data already stored.

    ``country`` moves with it. The second config also spells the country differently — which is
    what correcting a region's ``country:`` looks like, not a city changing continents — because
    the same conflict clause writes both columns and a test watching only ``metro`` would still
    pass with ``country`` dropped from the ``SET``.

    ``populate_existing`` on the re-read because the location is written with a Core
    ``INSERT ... ON CONFLICT``: the row is already in the session's identity map with its old
    metro, and the ORM would hand that copy back rather than what Postgres now holds.
    """
    record = make_record(locations=[RIVERMOUTH])
    first = await upsert_company_record(
        session, record, "hn_hiring", now=NOW, regions=one_city_regions("Old Metro", country="USA")
    )
    (before,) = (await session.execute(select(Location))).scalars().all()
    assert (before.city, before.state, before.metro, before.country) == (
        "Rivermouth",
        "ZZ",
        "Old Metro",
        "USA",
    )

    second = await upsert_company_record(
        session, record, "hn_hiring", now=NOW + DAY, regions=one_city_regions("New Metro")
    )

    assert second.company_id == first.company_id
    stmt = select(Location).execution_options(populate_existing=True)
    (after,) = (await session.execute(stmt)).scalars().all()
    # The same row, relabelled — not a second ``locations`` row for one city, which
    # ``uq_locations_city_state`` forbids anyway, and not a stale label kept beside the config.
    assert after.id == before.id
    assert (after.metro, after.country) == ("New Metro", "US")
    assert await count(session, CompanyLocation) == 1


async def test_child_rows_are_persisted_and_idempotent(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    rich = item(
        locations=[SF, PALO_ALTO],
        sectors=["Other Technology", "Banking & Financial Services"],
        funding_rounds=[
            funding_round(investors=[{"name": "Accel", "is_lead": True}, {"name": "Sequoia"}])
        ],
        people=[
            {"full_name": "Jane Doe", "title": "CEO", "role_type": enums.RoleType.FOUNDER},
            {"full_name": "John Roe", "title": "Director"},
        ],
        contacts=[
            {
                "kind": enums.ContactKind.EMAIL,
                "value": "jobs@acme.com",
                "confidence": enums.ContactConfidence.PUBLISHED,
            }
        ],
        jobs=[job("j1"), job("j2", posted_at=NOW - 10 * DAY)],
        jobs_complete=True,
    )
    models = (
        Company,
        Location,
        CompanyLocation,
        Sector,
        CompanySector,
        FundingRound,
        Investor,
        RoundInvestor,
        Person,
        Contact,
        Job,
        CompanySource,
    )

    first, _ = await run(session_factory, http, [rich])
    assert first.status is enums.FetchRunStatus.OK
    after_first = await counts(session_factory, *models)
    assert after_first == {
        "companies": 1,
        "locations": 2,
        "company_locations": 2,
        "sectors": 2,
        "company_sectors": 2,
        "funding_rounds": 1,
        "investors": 2,
        "round_investors": 2,
        "people": 2,
        # The published e-mail, plus the constructed people-search link every company gets
        # (SPEC §6; see the ``_ensure_constructed_contacts`` section below). This record has no
        # domain, so there is no constructed ``linkedin_company`` row.
        "contacts": 2,
        "jobs": 2,
        "company_sources": 1,
    }

    second, _ = await run(session_factory, http, [rich], now=NOW + DAY)
    assert second.status is enums.FetchRunStatus.OK
    assert await counts(session_factory, *models) == after_first
    # One ``Source`` per record per run (design decision 5).
    assert await counts(session_factory, Source) == {"sources": 2}

    async with session_factory() as session:
        locations = (await session.execute(select(Location).order_by(Location.city))).scalars()
        assert [(loc.city, loc.state, loc.metro, loc.country) for loc in locations] == [
            ("Palo Alto", "CA", BAY_AREA, "US"),
            ("San Francisco", "CA", BAY_AREA, "US"),
        ]
        links = (
            await session.execute(
                select(Location.city, CompanyLocation.is_hq)
                .join(CompanyLocation, CompanyLocation.location_id == Location.id)
                .order_by(Location.city)
            )
        ).all()
        assert [tuple(row) for row in links] == [("Palo Alto", False), ("San Francisco", True)]
        slugs = set((await session.execute(select(Sector.slug))).scalars())
        assert slugs == {"other-technology", "banking-financial-services"}

        (funding,) = (await session.execute(select(FundingRound))).scalars().all()
        assert funding.round_type is enums.RoundType.SAFE
        assert funding.amount_usd == 1_000_000
        assert funding.announced_date == date(2026, 3, 1)
        assert funding.raw_payload == {
            "accession": "0001-26-000001",
            "external_id": "021-000001",
            "connector": "fake",
        }
        leads = (
            await session.execute(
                select(Investor.name, RoundInvestor.is_lead)
                .join(RoundInvestor, RoundInvestor.investor_id == Investor.id)
                .order_by(Investor.name)
            )
        ).all()
        assert [tuple(row) for row in leads] == [("Accel", True), ("Sequoia", False)]

        people = (await session.execute(select(Person).order_by(Person.full_name))).scalars()
        assert [(p.full_name, p.title, p.role_type) for p in people] == [
            ("Jane Doe", "CEO", enums.RoleType.FOUNDER),
            ("John Roe", "Director", None),
        ]
        (contact,) = await stored_contacts(session, enums.ContactKind.EMAIL)
        assert (contact.kind, contact.value, contact.confidence) == (
            enums.ContactKind.EMAIL,
            "jobs@acme.com",
            enums.ContactConfidence.PUBLISHED,
        )
        jobs = (await session.execute(select(Job).order_by(Job.external_id))).scalars().all()
        assert [(j.external_id, j.closed_at, j.first_seen_at, j.last_seen_at) for j in jobs] == [
            ("j1", None, NOW, NOW + DAY),
            ("j2", None, NOW, NOW + DAY),
        ]
        # Every child row points at a Source of this connector; the second run re-pointed
        # them at its own Source.
        newest_source = await session.scalar(select(func.max(Source.id)))
        source_ids = {funding.source_id, contact.source_id, *(row.source_id for row in jobs)}
        assert source_ids == {newest_source}
        source = await session.get(Source, newest_source)
        assert source is not None
        assert (source.connector, source.fetched_at) == ("fake", NOW + DAY)


async def test_round_amendment_updates_in_place(session: AsyncSession) -> None:
    """Round identity (design decision 3): the Form D file number is ``external_id``, shared by
    the original filing and its amendment, so the amendment updates the row."""
    result = await upsert_company_record(
        session, make_record(funding_rounds=[funding_round()]), "sec_edgar", now=NOW
    )
    amendment = funding_round(
        amount_usd=2_500_000,
        notes="Form D/A (amendment of 0001-26-000001)",
        raw_payload={"accession": "0001-26-000002", "is_amendment": True},
    )
    await upsert_company_record(
        session, make_record(funding_rounds=[amendment]), "sec_edgar", now=NOW + DAY
    )

    (funding,) = (
        (await session.execute(select(FundingRound).execution_options(populate_existing=True)))
        .scalars()
        .all()
    )
    assert funding.company_id == result.company_id
    assert funding.amount_usd == 2_500_000
    assert funding.notes == "Form D/A (amendment of 0001-26-000001)"
    assert funding.raw_payload == {
        "accession": "0001-26-000002",
        "is_amendment": True,
        "external_id": "021-000001",
        "connector": "sec_edgar",
    }

    # An amendment that does not state the amount keeps the known one (None = unknown).
    await upsert_company_record(
        session,
        make_record(funding_rounds=[funding_round(amount_usd=None)]),
        "sec_edgar",
        now=NOW + 2 * DAY,
    )
    await session.refresh(funding)
    assert funding.amount_usd == 2_500_000
    assert await count(session, FundingRound) == 1


async def test_round_without_external_id_matches_on_type_and_date(session: AsyncSession) -> None:
    seed = funding_round(
        external_id=None, round_type=enums.RoundType.SEED, announced_date=date(2026, 1, 15)
    )
    await upsert_company_record(session, make_record(funding_rounds=[seed]), "funding_rss", now=NOW)
    await upsert_company_record(session, make_record(funding_rounds=[seed]), "funding_rss", now=NOW)
    assert await count(session, FundingRound) == 1

    later = funding_round(
        external_id=None, round_type=enums.RoundType.SEED, announced_date=date(2026, 2, 15)
    )
    await upsert_company_record(
        session, make_record(funding_rounds=[later]), "funding_rss", now=NOW
    )
    assert await count(session, FundingRound) == 2


async def test_people_match_case_insensitively_and_fill_fields(session: AsyncSession) -> None:
    await upsert_company_record(
        session, make_record(people=[{"full_name": "Jane Doe"}]), "sec_edgar", now=NOW
    )
    await upsert_company_record(
        session,
        make_record(
            people=[
                {"full_name": "JANE DOE", "title": "CEO", "role_type": enums.RoleType.FOUNDER},
                {"full_name": "jane doe"},
            ]
        ),
        "sec_edgar",
        now=NOW,
    )

    (person,) = (await session.execute(select(Person))).scalars().all()
    assert (person.full_name, person.title, person.role_type) == (
        "Jane Doe",
        "CEO",
        enums.RoleType.FOUNDER,
    )


async def test_people_match_on_the_databases_lower_for_non_ascii_names(
    session: AsyncSession,
) -> None:
    """The match key ``lower(full_name)`` is evaluated by Postgres on *both* sides. Python's
    ``str.lower()`` disagrees with it for locale-sensitive letters (Turkish dotted İ, capital
    ẞ), so a key computed in Python never matches the stored row and every re-run of the same
    filing would add another ``Person``."""
    people = [
        {"full_name": "İlker Öztürk", "title": "CEO"},
        {"full_name": "STRAẞE Müller"},
        {"full_name": "İlker Öztürk"},  # a repeat inside one record is one person too
    ]
    for _ in range(2):
        await upsert_company_record(session, make_record(people=people), "sec_edgar", now=NOW)

    rows = (await session.execute(select(Person).order_by(Person.id))).scalars().all()
    assert [(person.full_name, person.title) for person in rows] == [
        ("İlker Öztürk", "CEO"),
        ("STRAẞE Müller", None),
    ]


async def test_contacts_keep_published_over_constructed(session: AsyncSession) -> None:
    def contact(confidence: enums.ContactConfidence) -> dict[str, Any]:
        return {
            "kind": enums.ContactKind.LINKEDIN_COMPANY,
            "value": "https://www.linkedin.com/company/acme",
            "confidence": confidence,
        }

    async def stored() -> enums.ContactConfidence:
        (row,) = await stored_contacts(session, enums.ContactKind.LINKEDIN_COMPANY)
        return row.confidence

    constructed = make_record(contacts=[contact(enums.ContactConfidence.CONSTRUCTED)])
    published = make_record(contacts=[contact(enums.ContactConfidence.PUBLISHED)])

    await upsert_company_record(session, constructed, "hn_hiring", now=NOW)
    assert await stored() is enums.ContactConfidence.CONSTRUCTED
    await upsert_company_record(session, published, "company_site", now=NOW)
    assert await stored() is enums.ContactConfidence.PUBLISHED
    await upsert_company_record(session, constructed, "hn_hiring", now=NOW)
    assert await stored() is enums.ContactConfidence.PUBLISHED
    # One LinkedIn company row throughout — the record has no domain, so the only other contact
    # is the constructed people search of SPEC §6 (covered below).
    assert len(await stored_contacts(session, enums.ContactKind.LINKEDIN_COMPANY)) == 1


# ------------------------------- constructed contacts and reconciliation (SPEC §6)

#: SPEC §6 "People search": the title keywords the deep link filters on, spelled as the SPEC
#: spells them. They come from ``config/classifiers.yaml`` (``contacts.people_search_terms``),
#: never from Python (CLAUDE.md), so this literal is what the YAML must keep producing.
TITLE_TERMS = 'founder OR recruiter OR "head of engineering" OR "talent"'

#: astranis.com's footer links to exactly the URL SPEC §6 constructs from its domain; the
#: sourcegraph.com footer publishes a numeric company id, which no slug can ever equal.
ASTRANIS_LINKEDIN = "https://www.linkedin.com/company/astranis"
SOURCEGRAPH_LINKEDIN = "https://www.linkedin.com/company/sourcegraph"
SOURCEGRAPH_PUBLISHED_LINKEDIN = "https://www.linkedin.com/company/4803356"


def published_linkedin(value: str) -> dict[str, Any]:
    """A ``linkedin_company`` contact as ``company_site`` reports one it read off a footer
    (SPEC §6 C1) — already canonicalised by ``ingest.contacts``, which is why the value can be
    compared with the constructed one as a string."""
    return {
        "kind": enums.ContactKind.LINKEDIN_COMPANY,
        "value": value,
        "confidence": enums.ContactConfidence.PUBLISHED,
    }


async def test_a_company_without_a_published_linkedin_gets_both_constructed_links(
    session: AsyncSession,
) -> None:
    """SPEC §6's "otherwise" branch, for a company the Tier-3 connector has never visited: the
    company URL is built from the domain and the people search from the name, both
    ``constructed``, both with no ``Source`` — nothing was fetched to produce them, so there is
    no fetch to point at (SPEC §5).

    The people-search value is asserted whole to prove the expression is ``urlencode``d rather
    than concatenated, and that nothing beyond SPEC §6's keywords is appended.
    """
    await upsert_company_record(
        session,
        make_record(name="Astranis Space Technologies", domain="astranis.com"),
        "sec_edgar",
        now=NOW,
    )

    company_link, people_link = await stored_contacts(session)  # exactly these two
    assert (
        company_link.kind,
        company_link.value,
        company_link.confidence,
        company_link.source_id,
    ) == (
        enums.ContactKind.LINKEDIN_COMPANY,
        ASTRANIS_LINKEDIN,
        enums.ContactConfidence.CONSTRUCTED,
        None,
    )
    assert (people_link.kind, people_link.confidence, people_link.source_id) == (
        enums.ContactKind.LINKEDIN_PEOPLE,
        enums.ContactConfidence.CONSTRUCTED,
        None,
    )
    assert people_link.value == (
        "https://www.linkedin.com/search/results/people/?keywords="
        "%22Astranis+Space+Technologies%22+%28founder+OR+recruiter+OR"
        "+%22head+of+engineering%22+OR+%22talent%22%29"
    )
    assert search_keywords(people_link.value) == f'"Astranis Space Technologies" ({TITLE_TERMS})'


async def test_a_published_linkedin_url_removes_the_constructed_company_link(
    session: AsyncSession,
) -> None:
    """SPEC §6 C1 beating C2 where the two values *cannot* coincide: sourcegraph.com's footer
    publishes ``/company/4803356``, a numeric id. Keeping the constructed ``/company/sourcegraph``
    alongside it would show two different LinkedIn pages for one company, and the one the company
    did not publish would be a guess presented next to a fact. The people-search row is a
    different kind and a different rule, so it survives untouched.
    """
    await upsert_company_record(
        session, make_record(name="Sourcegraph", domain="sourcegraph.com"), "sec_edgar", now=NOW
    )
    assert [
        row.value for row in await stored_contacts(session, enums.ContactKind.LINKEDIN_COMPANY)
    ] == [SOURCEGRAPH_LINKEDIN]

    await upsert_company_record(
        session,
        make_record(
            name="Sourcegraph",
            external_id=None,
            domain="sourcegraph.com",
            enrich_only=True,
            contacts=[published_linkedin(SOURCEGRAPH_PUBLISHED_LINKEDIN)],
        ),
        "company_site",
        now=NOW + DAY,
    )

    (company_link,) = await stored_contacts(session, enums.ContactKind.LINKEDIN_COMPANY)
    assert (company_link.value, company_link.confidence) == (
        SOURCEGRAPH_PUBLISHED_LINKEDIN,
        enums.ContactConfidence.PUBLISHED,
    )
    # A published row names the fetch that saw it; only a constructed row has no source.
    assert company_link.source_id is not None
    (people_link,) = await stored_contacts(session, enums.ContactKind.LINKEDIN_PEOPLE)
    assert (people_link.confidence, people_link.source_id) == (
        enums.ContactConfidence.CONSTRUCTED,
        None,
    )
    assert search_keywords(people_link.value) == f'"Sourcegraph" ({TITLE_TERMS})'


async def test_a_published_url_equal_to_the_constructed_one_upgrades_the_row_in_place(
    session: AsyncSession,
) -> None:
    """The common case: astranis.com's footer links to precisely the URL SPEC §6 would have
    constructed. Both forms are canonicalised by the same code (``ingest.contacts``), so the
    value is byte-identical, the upsert finds the existing row and promotes it — the company
    ends with **one** LinkedIn row, marked ``published``, not two rows for one page with one of
    them labelled a guess.
    """
    await upsert_company_record(
        session, make_record(name="Astranis", domain="astranis.com"), "sec_edgar", now=NOW
    )
    (before,) = await stored_contacts(session, enums.ContactKind.LINKEDIN_COMPANY)
    assert (before.value, before.confidence) == (
        ASTRANIS_LINKEDIN,
        enums.ContactConfidence.CONSTRUCTED,
    )

    await upsert_company_record(
        session,
        make_record(
            name="Astranis",
            external_id=None,
            domain="astranis.com",
            enrich_only=True,
            contacts=[published_linkedin(ASTRANIS_LINKEDIN)],
        ),
        "company_site",
        now=NOW + DAY,
    )

    (after,) = await stored_contacts(session, enums.ContactKind.LINKEDIN_COMPANY)
    assert after.id == before.id  # promoted in place, not deleted and rewritten
    assert (after.value, after.confidence) == (ASTRANIS_LINKEDIN, enums.ContactConfidence.PUBLISHED)
    assert after.source_id is not None


async def test_a_renamed_company_gets_one_rebuilt_people_search(session: AsyncSession) -> None:
    """The people search is a pure function of the stored ``Company.name``, so a rename makes the
    old row a link that searches for a company that no longer goes by that name. It is replaced,
    not kept beside the new one: a constructed row is derived, never observed, so there is no
    history in it to preserve (SPEC §2's "never delete" protects observed data).
    """
    await upsert_company_record(
        session,
        make_record(name="Acme Robotics", external_id=None, domain="acme.com"),
        "hn_hiring",
        now=NOW,
    )
    (before,) = await stored_contacts(session, enums.ContactKind.LINKEDIN_PEOPLE)
    assert search_keywords(before.value) == f'"Acme Robotics" ({TITLE_TERMS})'

    # sec_edgar outranks hn_hiring, so this record renames the company (SPEC §8).
    await upsert_company_record(
        session,
        make_record(name="Acme Robotics, Inc.", external_id="cik-1", domain="acme.com"),
        "sec_edgar",
        now=NOW + DAY,
    )

    (after,) = await stored_contacts(session, enums.ContactKind.LINKEDIN_PEOPLE)
    assert search_keywords(after.value) == f'"Acme Robotics, Inc." ({TITLE_TERMS})'
    assert after.confidence is enums.ContactConfidence.CONSTRUCTED
    assert after.source_id is None


async def test_the_people_search_is_built_from_the_stored_name_not_the_records(
    session: AsyncSession,
) -> None:
    """Construction reads the company row, not the record that triggered it.

    An ``enrich_only`` record (SPEC §4 Tier 2 #5) names the company however the page it scraped
    spells it, and SPEC §8 keeps the higher-ranked connector's name. If the link were built from
    the record, every ``company_site`` refresh would throw away a good search shortcut and store
    one made from a page title — which is also a name no LinkedIn search will match.
    """
    await upsert_company_record(
        session,
        make_record(name="Sourcegraph, Inc.", domain="sourcegraph.com"),
        "sec_edgar",
        now=NOW,
    )
    result = await upsert_company_record(
        session,
        make_record(
            name="Sourcegraph | Code Intelligence Platform",
            external_id=None,
            domain="sourcegraph.com",
            enrich_only=True,
        ),
        "company_site",
        now=NOW + DAY,
    )

    company = await session.get(Company, result.company_id)
    assert company is not None
    assert company.name == "Sourcegraph, Inc."  # company_site does not outrank sec_edgar
    (people_link,) = await stored_contacts(session, enums.ContactKind.LINKEDIN_PEOPLE)
    assert search_keywords(people_link.value) == f'"Sourcegraph, Inc." ({TITLE_TERMS})'


async def test_a_company_without_a_domain_still_gets_the_people_search(
    session: AsyncSession,
) -> None:
    """SPEC §6 constructs the company URL "from the domain", so a company with none gets no
    ``linkedin_company`` row at all — a slug guessed from nothing links to a stranger. The people
    search is built from the name, which every company has (``Company.name`` is NOT NULL), so it
    is still written: an EDGAR-only company with no website is exactly who the user needs a way
    to reach.
    """
    await upsert_company_record(
        session, make_record(name="Nowhere Labs", domain=None), "sec_edgar", now=NOW
    )

    (people_link,) = await stored_contacts(session)  # the only contact this company has
    assert (people_link.kind, people_link.confidence, people_link.source_id) == (
        enums.ContactKind.LINKEDIN_PEOPLE,
        enums.ContactConfidence.CONSTRUCTED,
        None,
    )
    assert search_keywords(people_link.value) == f'"Nowhere Labs" ({TITLE_TERMS})'


async def test_rerunning_the_same_record_leaves_every_contact_row_untouched(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    """Construction runs on every upsert, so it runs on every refresh of every company. The
    ``id`` is asserted alongside the value: an implementation that deleted and re-inserted the
    constructed rows each run would keep the same values while churning the primary keys (and
    every foreign key a future phase might hang off them), and a plain count would not see it.
    """
    record = item(
        name="Atom Computing",
        domain="atom-computing.com",
        contacts=[
            {
                "kind": enums.ContactKind.EMAIL,
                "value": "hr@atom-computing.com",
                "confidence": enums.ContactConfidence.PUBLISHED,
            }
        ],
    )

    await run(session_factory, http, [record])
    async with session_factory() as session:
        first = [
            (row.id, row.kind, row.value, row.confidence) for row in await stored_contacts(session)
        ]
    assert len(first) == 3  # the published e-mail plus both constructed links

    await run(session_factory, http, [record], now=NOW + DAY)
    async with session_factory() as session:
        rows = await stored_contacts(session)
        newest_source = await session.scalar(select(func.max(Source.id)))

    assert [(row.id, row.kind, row.value, row.confidence) for row in rows] == first
    # The published e-mail is re-pointed at the second run's ``Source``; the constructed rows
    # still have none, because the second run fetched nothing to build them from either.
    sources = {row.kind: row.source_id for row in rows}
    assert sources == {
        enums.ContactKind.EMAIL: newest_source,
        enums.ContactKind.LINKEDIN_COMPANY: None,
        enums.ContactKind.LINKEDIN_PEOPLE: None,
    }


async def test_people_are_written_once_and_a_rerun_neither_duplicates_nor_erases_them(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    """SPEC §5 ``Person`` as SPEC §6 fills it: ``linkedin_url`` is a profile link the company
    published on its own team page — nothing fetches the profile (SPEC §4 excludes LinkedIn).

    Two rules in one run: the second run must not add a second "Ben Bloom", and a later record
    that only knows the name (a filing lists officers with no title) must not blank the title,
    role type and profile URL the team page supplied.
    """
    published_person: dict[str, Any] = {
        "full_name": "Ben Bloom",
        "title": "CEO & Founder",
        "role_type": enums.RoleType.FOUNDER,
        "linkedin_url": "https://www.linkedin.com/in/benbloom",
    }
    record = item(name="Atom Computing", domain="atom-computing.com", people=[published_person])

    await run(session_factory, http, [record])
    await run(session_factory, http, [record], now=NOW + DAY)
    await run(
        session_factory,
        http,
        [
            item(
                name="Atom Computing",
                domain="atom-computing.com",
                people=[{"full_name": "Ben Bloom"}],
            )
        ],
        now=NOW + 2 * DAY,
    )

    async with session_factory() as session:
        people = (await session.execute(select(Person).order_by(Person.id))).scalars().all()
    assert [(p.full_name, p.title, p.role_type, p.linkedin_url) for p in people] == [
        (
            "Ben Bloom",
            "CEO & Founder",
            enums.RoleType.FOUNDER,
            "https://www.linkedin.com/in/benbloom",
        )
    ]


# ------------------------------------------------------ jobs (SPEC §2, §5; decision 6)


async def test_jobs_are_closed_never_deleted_and_reopen(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    async def jobs_by_id() -> dict[str, Job]:
        async with session_factory() as session:
            rows = (await session.execute(select(Job))).scalars().all()
        return {row.external_id: row for row in rows}

    await run(session_factory, http, [item(jobs=[job("j1"), job("j2")], jobs_complete=True)])
    jobs = await jobs_by_id()
    assert {k: v.closed_at for k, v in jobs.items()} == {"j1": None, "j2": None}

    await run(
        session_factory,
        http,
        [item(jobs=[job("j1")], jobs_complete=True)],
        now=NOW + DAY,
    )
    jobs = await jobs_by_id()
    assert set(jobs) == {"j1", "j2"}  # nothing deleted
    assert jobs["j1"].closed_at is None
    assert jobs["j1"].last_seen_at == NOW + DAY
    assert jobs["j2"].closed_at == NOW + DAY
    assert jobs["j2"].last_seen_at == NOW

    await run(
        session_factory,
        http,
        [item(jobs=[job("j1"), job("j2", title="Engineer j2 (reopened)")], jobs_complete=True)],
        now=NOW + 2 * DAY,
    )
    jobs = await jobs_by_id()
    assert jobs["j2"].closed_at is None
    assert jobs["j2"].title == "Engineer j2 (reopened)"
    assert (jobs["j2"].first_seen_at, jobs["j2"].last_seen_at) == (NOW, NOW + 2 * DAY)
    assert await counts(session_factory, Job) == {"jobs": 2}


async def test_jobs_from_another_connector_are_not_closed(session: AsyncSession) -> None:
    """ "This connector's jobs" are found through ``sources.connector`` (decision 5): a complete
    Greenhouse listing closes only Greenhouse jobs, never one an HN comment reported."""
    await upsert_company_record(
        session,
        make_record(external_id=None, domain="acme.com", jobs=[job("hn-1")]),
        "hn_hiring",
        now=NOW,
    )
    await upsert_company_record(
        session,
        make_record(external_id="acme", domain="acme.com", jobs=[job("gh-1")], jobs_complete=True),
        "greenhouse",
        now=NOW,
    )
    await upsert_company_record(
        session,
        make_record(external_id="acme", domain="acme.com", jobs=[], jobs_complete=True),
        "greenhouse",
        now=NOW + DAY,
    )

    rows = (await session.execute(select(Job).order_by(Job.external_id))).scalars().all()
    assert [(row.external_id, row.closed_at) for row in rows] == [
        ("gh-1", NOW + DAY),
        ("hn-1", None),
    ]


async def test_job_upsert_keeps_stored_text_when_incoming_is_none(session: AsyncSession) -> None:
    full = job("j1", url="https://acme.com/jobs/1", description_raw="Long text", posted_at=NOW)
    await upsert_company_record(session, make_record(jobs=[full]), "greenhouse", now=NOW)
    await upsert_company_record(
        session, make_record(jobs=[job("j1", title="Renamed")]), "greenhouse", now=NOW + DAY
    )

    (row,) = (await session.execute(select(Job))).scalars().all()
    assert (row.title, row.url, row.description_raw, row.posted_at) == (
        "Renamed",
        "https://acme.com/jobs/1",
        "Long text",
        NOW,
    )


# ------------------------------------------------------- denormalized columns (SPEC §5)


async def test_denormalized_columns_follow_each_run(
    session_factory: SessionFactory, http: HttpClient
) -> None:
    rounds = [
        funding_round(external_id="021-a", announced_date=date(2025, 1, 1)),
        funding_round(external_id="021-b", announced_date=date(2026, 3, 1)),
    ]
    jobs = [job("j1", posted_at=NOW - 10 * DAY), job("j2")]

    await run(session_factory, http, [item(funding_rounds=rounds, jobs=jobs, jobs_complete=True)])
    company = await only_company(session_factory)
    async with session_factory() as session:
        newest_round_id = await session.scalar(
            select(FundingRound.id).where(FundingRound.announced_date == date(2026, 3, 1))
        )
    assert company.open_job_count == 2
    assert company.latest_job_posted_at == NOW  # j2 has no posted_at: its first_seen_at counts
    assert company.latest_round_id == newest_round_id

    await run(
        session_factory,
        http,
        [item(funding_rounds=rounds, jobs=[jobs[0]], jobs_complete=True)],
        now=NOW + DAY,
    )
    company = await only_company(session_factory)
    assert company.open_job_count == 1
    assert company.latest_job_posted_at == NOW - 10 * DAY
    assert company.latest_round_id == newest_round_id

    await run(session_factory, http, [item(jobs=[], jobs_complete=True)], now=NOW + 2 * DAY)
    company = await only_company(session_factory)
    assert (company.open_job_count, company.latest_job_posted_at) == (0, None)
    assert company.latest_round_id == newest_round_id
    assert await counts(session_factory, Job, FundingRound) == {"jobs": 2, "funding_rounds": 2}


async def test_refresh_is_not_part_of_the_upsert(session: AsyncSession) -> None:
    """The denormalized columns are recomputed at end of run, not per record (SPEC §5) — a
    direct upsert leaves them at their defaults until the refresh runs."""
    result = await upsert_company_record(
        session, make_record(jobs=[job("j1")]), "greenhouse", now=NOW
    )
    assert result.company_id is not None  # only an enrich_only record can be skipped
    company = await session.get(Company, result.company_id)
    assert company is not None
    assert (company.open_job_count, company.latest_job_posted_at) == (0, None)

    assert await refresh_company_denormalized_columns(session, [result.company_id]) == 1
    await session.refresh(company)
    assert (company.open_job_count, company.latest_job_posted_at) == (1, NOW)


# ------------------------------- sync_constructed_contacts (SPEC §6, cli.py sync-contacts)


async def insert_company(session: AsyncSession, name: str, domain: str | None) -> int:
    """A company written straight to the table, bypassing ``upsert_company_record``.

    That is the state the sweep exists for and the only way to reach it: every company the
    ingest path writes already has its constructed links, so a company *without* them is one
    that predates Phase 5 — or, as here, one the pipeline never saw.
    """
    company = Company(name=name, normalized_name=normalize_name(name), domain=domain)
    session.add(company)
    await session.flush()
    return company.id


async def test_the_sweep_reaches_a_company_no_upsert_ever_touched(
    session_factory: SessionFactory,
) -> None:
    """The whole point of the command: SPEC §6's links for a row the ingest path never wrote,
    which is what a database carried over from an earlier phase is full of."""
    async with session_factory() as session, session.begin():
        company_id = await insert_company(session, "Astranis", "astranis.com")

    result = await sync_constructed_contacts(session_factory)

    assert (result.companies, result.added, result.removed) == (1, 2, 0)
    assert result.dry_run is False
    async with session_factory() as session:
        company_link, people_link = await stored_contacts(session)
        assert company_link.company_id == company_id
        assert (company_link.kind, company_link.value, company_link.confidence) == (
            enums.ContactKind.LINKEDIN_COMPANY,
            ASTRANIS_LINKEDIN,
            enums.ContactConfidence.CONSTRUCTED,
        )
        assert search_keywords(people_link.value) == f'"Astranis" ({TITLE_TERMS})'
        assert (company_link.source_id, people_link.source_id) == (None, None)


async def test_a_second_sweep_changes_nothing(session_factory: SessionFactory) -> None:
    """Idempotence is what makes the command safe to run whenever you are unsure, and the
    counters are how it says so: ``added=0 removed=0`` on a database already in the right
    state, not two rows silently rewritten."""
    async with session_factory() as session, session.begin():
        await insert_company(session, "Astranis", "astranis.com")
    first = await sync_constructed_contacts(session_factory)
    assert first.changed == 2

    second = await sync_constructed_contacts(session_factory)

    assert (second.companies, second.added, second.removed, second.changed) == (1, 0, 0, 0)
    async with session_factory() as session:
        assert len(await stored_contacts(session)) == 2


def test_changed_counts_a_sweep_that_only_dropped_rows() -> None:
    """A sweep can repair a company without writing a single row: a published company page
    means no constructed one is built, so a leftover constructed link is dropped and nothing
    replaces it — ``added=0 removed=1``.

    ``cli.py`` reads ``changed`` for its ``(already in sync)`` suffix, and that is the one run
    where the suffix would be a lie: the operator would be told nothing happened on the only
    sweep that actually fixed something. Asserted as a unit rather than through the CLI because
    the obvious end-to-end scenario — a renamed company — rebuilds as well as drops
    (``added=1 removed=1``) and so cannot tell the two definitions apart.
    """
    assert ContactSyncResult(companies=1, added=0, removed=1, dry_run=False).changed == 1


async def test_the_sweep_never_touches_a_published_contact(
    session_factory: SessionFactory,
) -> None:
    """SPEC §6's "otherwise": a published company page means no constructed one is built, and
    the published row itself — an observation (SPEC §2, §5) — is left exactly as it was, source
    and all. The sweep also leaves the published address of a company it *does* build a link
    for alone."""
    async with session_factory() as session, session.begin():
        company_id = await insert_company(session, "Sourcegraph", "sourcegraph.com")
        source = Source(connector="company_site", url="https://sourcegraph.com/", fetched_at=NOW)
        session.add(source)
        await session.flush()
        session.add_all(
            [
                Contact(
                    company_id=company_id,
                    kind=enums.ContactKind.LINKEDIN_COMPANY,
                    value=SOURCEGRAPH_PUBLISHED_LINKEDIN,
                    confidence=enums.ContactConfidence.PUBLISHED,
                    source_id=source.id,
                ),
                Contact(
                    company_id=company_id,
                    kind=enums.ContactKind.EMAIL,
                    value="jobs@sourcegraph.com",
                    confidence=enums.ContactConfidence.PUBLISHED,
                    source_id=source.id,
                ),
            ]
        )
        source_id = source.id

    result = await sync_constructed_contacts(session_factory)

    assert (result.added, result.removed) == (1, 0)  # the people search, and nothing else
    async with session_factory() as session:
        by_kind = {contact.kind: contact for contact in await stored_contacts(session)}
        assert set(by_kind) == {
            enums.ContactKind.LINKEDIN_COMPANY,
            enums.ContactKind.EMAIL,
            enums.ContactKind.LINKEDIN_PEOPLE,
        }
        published = by_kind[enums.ContactKind.LINKEDIN_COMPANY]
        assert published.value == SOURCEGRAPH_PUBLISHED_LINKEDIN
        assert (published.confidence, published.source_id) == (
            enums.ContactConfidence.PUBLISHED,
            source_id,
        )
        assert by_kind[enums.ContactKind.EMAIL].source_id == source_id
        # No constructed company link was invented next to the published one.
        assert SOURCEGRAPH_LINKEDIN not in {contact.value for contact in by_kind.values()}


async def test_the_sweep_replaces_a_link_its_inputs_have_outgrown(
    session_factory: SessionFactory,
) -> None:
    """A name or domain corrected outside the ingest path — a merge, a hand-edited row — leaves
    a constructed link pointing at the wrong company. The sweep is what repairs it, and the
    counters report both halves of the repair."""
    async with session_factory() as session, session.begin():
        company_id = await insert_company(session, "Astranis", "astranis.com")
    await sync_constructed_contacts(session_factory)

    async with session_factory() as session, session.begin():
        company = await session.get(Company, company_id)
        assert company is not None
        company.name = "Astranis Space Technologies"
        # A domain whose *registrable label* differs — `astranis.space` would not do, because
        # the slug is the label before the suffix and stays `astranis`, which is the rule
        # working rather than a link going stale.
        company.domain = "astranisspace.com"

    result = await sync_constructed_contacts(session_factory)

    assert (result.added, result.removed) == (2, 2)
    async with session_factory() as session:
        values = {contact.value for contact in await stored_contacts(session)}
        assert values == {
            "https://www.linkedin.com/company/astranisspace",
            people_search_url("Astranis Space Technologies"),
        }


async def test_a_company_with_no_domain_gets_only_the_people_search(
    session_factory: SessionFactory,
) -> None:
    """SPEC §6 builds the company URL "from the domain"; there is none, and a slug guessed from
    nothing links to a stranger. The name is still enough for the people search."""
    async with session_factory() as session, session.begin():
        await insert_company(session, "Aerdos Labs", None)

    result = await sync_constructed_contacts(session_factory)

    assert (result.added, result.removed) == (1, 0)
    async with session_factory() as session:
        (contact,) = await stored_contacts(session)
        assert contact.kind is enums.ContactKind.LINKEDIN_PEOPLE


async def test_a_dry_run_reports_what_it_would_do_and_writes_nothing(
    session_factory: SessionFactory,
) -> None:
    """The safety valve on a command that touches every company. It is not an estimate: the
    sweep runs in full and the transaction is discarded, so the counts are the counts — proved
    here by running it for real afterwards and getting the same numbers."""
    async with session_factory() as session, session.begin():
        await insert_company(session, "Astranis", "astranis.com")
        await insert_company(session, "Aerdos Labs", None)

    dry = await sync_constructed_contacts(session_factory, dry_run=True)

    assert (dry.companies, dry.added, dry.removed, dry.dry_run) == (2, 3, 0, True)
    async with session_factory() as session:
        assert await stored_contacts(session) == []

    wet = await sync_constructed_contacts(session_factory)
    assert (wet.companies, wet.added, wet.removed) == (2, 3, 0)
    async with session_factory() as session:
        assert len(await stored_contacts(session)) == 3


async def test_every_company_is_reached_when_the_batch_is_smaller_than_the_database(
    session_factory: SessionFactory,
) -> None:
    """The sweep pages by keyset over ``companies.id``. A batch that divides the table exactly,
    and one that leaves a remainder, must both cover every row: an off-by-one in the cursor
    would skip a company or loop for ever, and neither shows up at ``batch_size=500``.

    Ids are deliberately non-contiguous (every second company is deleted) because ``Identity``
    leaves gaps in a real database — a cursor advanced by ``+= batch_size`` rather than by the
    last id seen would pass this test only on a table that was never written to twice.

    The ``UPDATE`` below is load-bearing, not leftover setup: Postgres rewrites an updated row
    at the end of the heap, so after an ordinary upsert of two companies the *physical* scan
    order no longer matches id order. A page that dropped ``ORDER BY id`` would then hand back
    heap order, advance the cursor past ids it never visited, and exclude them for ever — a
    silent gap in exactly the completeness this command exists to guarantee, and one that
    ``DELETE`` alone cannot provoke, because deleting a row does not move the survivors.

    The session count is what makes ``batch_size`` mean anything. Every total here —
    ``companies``, ``added``, the row count — is identical whether the sweep ran seven pages or
    one, so without it a ``LIMIT`` that ignored the argument would pass unnoticed.
    """
    async with session_factory() as session, session.begin():
        ids = [await insert_company(session, f"Company {index}", None) for index in range(14)]
        for company_id in ids[1::2]:
            company = await session.get(Company, company_id)
            assert company is not None
            await session.delete(company)
    async with session_factory() as session, session.begin():
        await session.execute(
            text("UPDATE companies SET name = name || 'x' WHERE id IN (:first, :third)"),
            {"first": ids[0], "third": ids[2]},
        )
    remaining = len(ids[0::2])

    for batch_size in (1, 3, 7, remaining, remaining + 5):
        async with session_factory() as session, session.begin():
            await session.execute(delete(Contact))
        opened = 0

        def counting_factory() -> AsyncSession:
            nonlocal opened
            opened += 1
            return session_factory()

        result = await sync_constructed_contacts(
            cast(SessionFactory, counting_factory), batch_size=batch_size
        )

        assert result.companies == remaining, batch_size
        assert result.added == remaining, batch_size  # one people search each, no domains
        # One session per full page, plus the empty one that ends the loop.
        assert opened == -(-remaining // batch_size) + 1, batch_size
        async with session_factory() as session:
            assert len(await stored_contacts(session)) == remaining, batch_size


async def test_a_batch_size_below_one_is_rejected(session_factory: SessionFactory) -> None:
    """A zero or negative batch would page for ever over the same rows rather than fail."""
    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        await sync_constructed_contacts(session_factory, batch_size=0)


async def test_the_sweep_writes_no_fetch_run(session_factory: SessionFactory) -> None:
    """SPEC §7.2's "every run writes a FetchRun row" is about *fetching*. Nothing here fetches,
    so a row would claim a source was contacted that never was, and ``/runs`` would show a
    connector that does not exist."""
    async with session_factory() as session, session.begin():
        await insert_company(session, "Astranis", "astranis.com")

    await sync_constructed_contacts(session_factory)

    assert await counts(session_factory, FetchRun) == {"fetch_runs": 0}


async def test_the_sweep_commits_one_company_at_a_time(
    session_factory: SessionFactory,
) -> None:
    """``batch_size`` sizes the id *page*, not the transaction.

    Each company is reconciled under a row lock (see the concurrency test below), and a lock is
    held until its transaction ends. Committing a whole page at once would hold up to 500 of
    them while an ingest run waited, and could deadlock against the end-of-run ``UPDATE
    companies``, which takes its own row locks in heap order rather than id order. So the
    commit is per company, and the count of commits is the only place that is visible: every
    total the sweep reports is identical either way.
    """
    async with session_factory() as session, session.begin():
        for index in range(3):
            await insert_company(session, f"Company {index}", None)
    commits = 0

    def counting_factory() -> AsyncSession:
        session = session_factory()

        def count_commit(_: object) -> None:
            nonlocal commits
            commits += 1

        event.listen(session.sync_session, "after_commit", count_commit)
        return session

    result = await sync_constructed_contacts(cast(SessionFactory, counting_factory))

    assert (result.companies, result.added) == (3, 3)
    assert commits == 3


async def wait_until_a_backend_is_blocked(session: AsyncSession) -> None:
    """Return once some *other* backend on this database is waiting for a lock.

    A ``sleep`` would make the interleave below a race with a stopwatch; this makes it an
    observation. Postgres reports both kinds of wait this test can produce — a row lock, and an
    ``ON CONFLICT`` insert waiting on the transaction that deleted the conflicting row — as
    ``wait_event_type = 'Lock'``, so the barrier is the same whether or not the fix is in place.

    ``pg_stat_clear_snapshot`` on every pass because backend status is cached for the duration
    of the reading transaction, and this one has to stay open: it is the transaction holding
    the lock being waited for.
    """
    waiting = text(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
        "AND pid <> pg_backend_pid() AND wait_event_type = 'Lock'"
    )
    try:
        async with asyncio.timeout(15.0):
            while not await session.scalar(waiting):
                await asyncio.sleep(0.01)
                await session.execute(text("SELECT pg_stat_clear_snapshot()"))
    except TimeoutError:
        msg = "no other backend ever blocked; the interleave did not happen"
        raise AssertionError(msg) from None


async def test_a_sweep_cannot_delete_the_links_a_concurrent_upsert_just_committed(
    session_factory: SessionFactory,
) -> None:
    """The sweep reconciles companies no upsert is touching — but nothing stops a connector
    from upserting one mid-sweep, and the two write the same rows.

    The dangerous order is: the sweep reads ``(name, domain)``; a scheduled run renames the
    company, rebuilds its people-search link and commits; the sweep then writes from the name
    it read a moment ago. That is not merely a stale insert. ``_drop_superseded_constructed``
    deletes every constructed row whose value differs from the sweep's ``keep``, which is
    precisely the row the upsert just committed — so the company ends up advertising a search
    for a name it no longer has, until some connector next re-lists it. The sweep reports
    ``added=1 removed=1`` and exits 0, so nothing says anything went wrong.

    The fix is that the read takes the row lock the writes need, so the two serialize. Here the
    upsert holds that lock, waits for the sweep to block on it, and only then commits; the
    sweep must therefore work from the name it finds afterwards, and find nothing left to do.
    """
    async with session_factory() as session, session.begin():
        company_id = await insert_company(session, "Old Co", "oldco.com")
    await sync_constructed_contacts(session_factory)
    holding_the_row = asyncio.Event()

    async def rename_the_company() -> None:
        async with session_factory() as session, session.begin():
            await upsert_company_record(
                session,
                make_record(name="New Co", domain="oldco.com", external_id="new-co"),
                "sec_edgar",
                now=NOW,
            )
            holding_the_row.set()
            await wait_until_a_backend_is_blocked(session)

    async def sweep() -> ContactSyncResult:
        await holding_the_row.wait()
        return await sync_constructed_contacts(session_factory)

    _, result = await asyncio.gather(rename_the_company(), sweep())

    async with session_factory() as session:
        company = await session.get(Company, company_id, populate_existing=True)
        assert company is not None and company.name == "New Co"
        (people,) = await stored_contacts(session, enums.ContactKind.LINKEDIN_PEOPLE)
        assert people.value == people_search_url("New Co"), "the sweep overwrote a fresh link"
    # Nothing was left for the sweep to do: the upsert had already rebuilt both links.
    assert (result.added, result.removed) == (0, 0)
