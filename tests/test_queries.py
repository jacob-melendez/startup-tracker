"""``db.queries`` — the set-based statements the pipeline runs around a run: the end-of-run
recompute of the denormalized ``companies`` columns (SPEC §5, design decision 4), the
incremental-window anchor (SPEC §7.2, §14.3) and closing jobs that vanished from their source
(SPEC §2, §5). Real Postgres, rows inserted directly with the ORM.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from db import enums
from db.models import Company, FetchRun, FundingRound, Job, Source
from db.queries import (
    COMPLETED_RUN_STATUSES,
    close_missing_jobs,
    last_successful_run_started_at,
    refresh_company_denormalized_columns,
)

NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
DAY = timedelta(days=1)


# ------------------------------------------------------------------------------ helpers


async def add_company(session: AsyncSession, name: str) -> Company:
    company = Company(name=name, normalized_name=name.lower())
    session.add(company)
    await session.flush()
    return company


async def add_job(
    session: AsyncSession,
    company: Company,
    external_id: str,
    *,
    posted_at: datetime | None = None,
    first_seen_at: datetime = NOW,
    closed_at: datetime | None = None,
    source_id: int | None = None,
) -> Job:
    row = Job(
        company_id=company.id,
        external_id=external_id,
        title=f"Role {external_id}",
        posted_at=posted_at,
        first_seen_at=first_seen_at,
        closed_at=closed_at,
        source_id=source_id,
    )
    session.add(row)
    await session.flush()
    return row


async def add_round(
    session: AsyncSession, company: Company, announced_date: date | None
) -> FundingRound:
    row = FundingRound(company_id=company.id, announced_date=announced_date)
    session.add(row)
    await session.flush()
    return row


async def add_source(session: AsyncSession, connector: str) -> Source:
    row = Source(connector=connector, fetched_at=NOW)
    session.add(row)
    await session.flush()
    return row


async def add_run(
    session: AsyncSession, connector: str, started_at: datetime, status: enums.FetchRunStatus
) -> FetchRun:
    row = FetchRun(connector=connector, started_at=started_at, status=status)
    session.add(row)
    await session.flush()
    return row


def denormalized(company: Company) -> tuple[int, datetime | None, int | None]:
    """The three SPEC §5 denormalized columns, as stored."""
    return (company.open_job_count, company.latest_job_posted_at, company.latest_round_id)


# ------------------------------------------- refresh_company_denormalized_columns (SPEC §5)


async def test_refresh_with_none_covers_every_company_and_resets_closed_ones(
    session: AsyncSession,
) -> None:
    active = await add_company(session, "Active")
    await add_job(session, active, "a1", posted_at=NOW - 5 * DAY)
    await add_job(session, active, "a2", first_seen_at=NOW - DAY)  # no posted_at
    await add_job(session, active, "a3", posted_at=NOW, closed_at=NOW)  # closed: not counted
    await add_round(session, active, date(2025, 1, 1))
    newest = await add_round(session, active, date(2026, 3, 1))
    await add_round(session, active, None)

    quiet = await add_company(session, "Quiet")
    await add_job(session, quiet, "q1", posted_at=NOW, closed_at=NOW)
    # Stale denormalized values, as if left over from an earlier run.
    quiet.open_job_count = 5
    quiet.latest_job_posted_at = NOW
    quiet.latest_round_id = newest.id
    empty = await add_company(session, "Empty")
    await session.flush()

    assert await refresh_company_denormalized_columns(session, None) == 3

    for company in (active, quiet, empty):
        await session.refresh(company)
    assert denormalized(active) == (2, NOW - DAY, newest.id)
    assert denormalized(quiet) == (0, None, None)
    assert denormalized(empty) == (0, None, None)


async def test_refresh_is_limited_to_the_given_ids(session: AsyncSession) -> None:
    first = await add_company(session, "First")
    second = await add_company(session, "Second")
    await add_job(session, first, "f1", posted_at=NOW)
    await add_job(session, second, "s1", posted_at=NOW)

    assert await refresh_company_denormalized_columns(session, [first.id]) == 1

    await session.refresh(first)
    await session.refresh(second)
    assert denormalized(first) == (1, NOW, None)
    assert denormalized(second) == (0, None, None)

    assert await refresh_company_denormalized_columns(session, []) == 0
    await session.refresh(second)
    assert second.open_job_count == 0


async def test_latest_round_prefers_announced_date_then_highest_id(
    session: AsyncSession,
) -> None:
    dated = await add_company(session, "Dated")
    winner = await add_round(session, dated, date(2026, 1, 1))
    await add_round(session, dated, None)  # undated rounds sort last
    await add_round(session, dated, date(2025, 6, 1))

    undated = await add_company(session, "Undated")
    await add_round(session, undated, None)
    latest_undated = await add_round(session, undated, None)

    tied = await add_company(session, "Tied")
    await add_round(session, tied, date(2026, 5, 5))
    latest_tied = await add_round(session, tied, date(2026, 5, 5))

    await refresh_company_denormalized_columns(session, None)

    for company in (dated, undated, tied):
        await session.refresh(company)
    assert dated.latest_round_id == winner.id
    assert undated.latest_round_id == latest_undated.id
    assert tied.latest_round_id == latest_tied.id


# --------------------------------------------- last_successful_run_started_at (SPEC §7.2)


async def test_last_successful_run_started_at(session: AsyncSession) -> None:
    assert COMPLETED_RUN_STATUSES == (enums.FetchRunStatus.OK, enums.FetchRunStatus.PARTIAL)
    assert await last_successful_run_started_at(session, "sec_edgar") is None

    await add_run(session, "sec_edgar", NOW, enums.FetchRunStatus.ERROR)
    assert await last_successful_run_started_at(session, "sec_edgar") is None

    await add_run(session, "sec_edgar", NOW + DAY, enums.FetchRunStatus.OK)
    assert await last_successful_run_started_at(session, "sec_edgar") == NOW + DAY

    # A partial run scanned its whole window: it advances the anchor.
    await add_run(session, "sec_edgar", NOW + 2 * DAY, enums.FetchRunStatus.PARTIAL)
    assert await last_successful_run_started_at(session, "sec_edgar") == NOW + 2 * DAY

    # An error run did not: it is skipped, and other connectors' runs never count.
    await add_run(session, "sec_edgar", NOW + 3 * DAY, enums.FetchRunStatus.ERROR)
    await add_run(session, "ycombinator", NOW + 4 * DAY, enums.FetchRunStatus.OK)
    assert await last_successful_run_started_at(session, "sec_edgar") == NOW + 2 * DAY
    assert await last_successful_run_started_at(session, "ycombinator") == NOW + 4 * DAY


# -------------------------------------------------------- close_missing_jobs (SPEC §2, §5)


async def test_close_missing_jobs_closes_only_this_connectors_open_jobs(
    session: AsyncSession,
) -> None:
    company = await add_company(session, "Acme")
    other_company = await add_company(session, "Other")
    greenhouse = await add_source(session, "greenhouse")
    hn = await add_source(session, "hn_hiring")
    keep = await add_job(session, company, "gh-keep", source_id=greenhouse.id)
    gone = await add_job(session, company, "gh-gone", source_id=greenhouse.id)
    from_hn = await add_job(session, company, "hn-1", source_id=hn.id)
    long_closed = await add_job(
        session, company, "gh-old", source_id=greenhouse.id, closed_at=NOW - 30 * DAY
    )
    unattributed = await add_job(session, company, "orphan", source_id=None)
    elsewhere = await add_job(session, other_company, "gh-elsewhere", source_id=greenhouse.id)

    closed = await close_missing_jobs(
        session,
        company_id=company.id,
        connector="greenhouse",
        keep_external_ids=["gh-keep"],
        now=NOW,
    )

    assert closed == 1
    rows = (keep, gone, from_hn, long_closed, unattributed, elsewhere)
    for row in rows:
        await session.refresh(row)
    assert [row.closed_at for row in rows] == [None, NOW, None, NOW - 30 * DAY, None, None]

    # An empty keep list closes everything this connector still has open at the company.
    closed = await close_missing_jobs(
        session, company_id=company.id, connector="greenhouse", keep_external_ids=[], now=NOW + DAY
    )
    assert closed == 1
    await session.refresh(keep)
    assert keep.closed_at == NOW + DAY
