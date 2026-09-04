"""The few genuinely complex statements (SPEC §3 "keep those in ``db/queries.py`` with a
docstring explaining the shape").

Ordinary reads and writes use ORM ``select()`` in place; what lives here is set-based SQL that
the ingest pipeline (:mod:`ingest.pipeline`) runs at well-defined points of a run:

* :func:`refresh_company_denormalized_columns` — the end-of-run recompute of
  ``Company.latest_round_id`` / ``open_job_count`` / ``latest_job_posted_at`` (SPEC §5);
* :func:`last_successful_run_started_at` — the incremental-window anchor (SPEC §7.2, §14.3);
* :func:`close_missing_jobs` — marks jobs that vanished from their source as closed, never
  deleting them (SPEC §2, §5);
* :func:`load_company_targets` — the pre-loaded work list for the connectors that enrich
  companies already in the database instead of discovering new ones (SPEC §4 Tier 1 #3,
  Tier 3).

Phase 4 adds the company-list query (SPEC §9) here.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any, cast

from sqlalchemy import CursorResult, and_, exists, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from db import enums
from db.models import Company, CompanySource, FetchRun, FundingRound, Job, Source
from ingest.base import CompanyTarget

#: ``FetchRun`` statuses whose fetch scanned its whole window (SPEC §7.2). A ``partial`` run
#: had per-item trouble but did finish scanning, so the next incremental window may start
#: from it; an ``error`` run stopped early and must not anchor a window (design brief step 1).
COMPLETED_RUN_STATUSES = (enums.FetchRunStatus.OK, enums.FetchRunStatus.PARTIAL)


async def refresh_company_denormalized_columns(
    session: AsyncSession, company_ids: Sequence[int] | None
) -> int:
    """Recompute the three denormalized ``companies`` columns of SPEC §5 in one statement.

    Why the pipeline calls this instead of database triggers (SPEC §5 "maintained by the
    ingest pipeline at end-of-run, not by triggers"): the pipeline is the only writer of jobs
    and rounds and it knows when a run is complete, so one set-based recompute at the end of
    the run replaces thousands of per-row trigger firings, keeps the write path visible in
    Python, and lets a partial run be reasoned about (the columns describe committed state,
    never a half-applied batch). Triggers would also have to be maintained in the migrations,
    outside the model's view.

    Shape — a single ``UPDATE companies SET ... `` whose three values are correlated scalar
    subqueries over the company being updated (design decision 4):

    * ``open_job_count`` = ``count(*)`` of the company's jobs with ``closed_at IS NULL``;
    * ``latest_job_posted_at`` = ``max(coalesce(posted_at, first_seen_at))`` over those same
      open jobs — ``first_seen_at`` stands in for sources that publish no posting date, so a
      job still counts as recent from the day we first saw it (it powers the default sort,
      SPEC §9);
    * ``latest_round_id`` = the id of the round with the greatest ``announced_date``
      (``NULLS LAST``; ties and undated rounds resolved by the highest id, i.e. the most
      recently ingested).

    A company with no open jobs / no rounds gets ``0`` / ``NULL`` / ``NULL``.

    ``company_ids=None`` recomputes every company; a sequence limits the statement to those
    ids (the ones a run touched). An empty sequence is a no-op. Returns the number of
    company rows updated.
    """
    if company_ids is not None and len(company_ids) == 0:
        return 0

    open_jobs = and_(Job.company_id == Company.id, Job.closed_at.is_(None))
    open_job_count = (
        select(func.count()).select_from(Job).where(open_jobs).correlate(Company).scalar_subquery()
    )
    latest_job_posted_at = (
        select(func.max(func.coalesce(Job.posted_at, Job.first_seen_at)))
        .select_from(Job)
        .where(open_jobs)
        .correlate(Company)
        .scalar_subquery()
    )
    latest_round_id = (
        select(FundingRound.id)
        .where(FundingRound.company_id == Company.id)
        .order_by(FundingRound.announced_date.desc().nulls_last(), FundingRound.id.desc())
        .limit(1)
        .correlate(Company)
        .scalar_subquery()
    )
    stmt = update(Company).values(
        open_job_count=open_job_count,
        latest_job_posted_at=latest_job_posted_at,
        latest_round_id=latest_round_id,
    )
    if company_ids is not None:
        stmt = stmt.where(Company.id.in_(list(company_ids)))
    # ``AsyncSession.execute`` is typed as the generic ``Result``; an UPDATE always yields a
    # ``CursorResult`` whose ``rowcount`` is the number of rows matched.
    result = cast("CursorResult[Any]", await session.execute(stmt))
    updated: int = result.rowcount
    return updated


async def load_company_targets(
    session: AsyncSession, connector: str, *, statuses: Sequence[enums.CompanyStatus] | None = None
) -> list[CompanyTarget]:
    """The work list for a connector that *enriches* companies rather than discovering them
    (design decision 5): the four ATS connectors and ``company_site``.

    Shape — one ``SELECT`` over ``companies`` with an **outer join** to this connector's own
    ``company_sources`` row, so a company the connector has never visited still comes back
    (with ``external_id`` and ``last_seen_at`` NULL) instead of being filtered out by an inner
    join. Ordering is ``last_seen_at ASC NULLS FIRST, id`` — never-visited companies first,
    then the least recently visited — which is what makes a per-run cap (``company_site``
    visits at most N companies a night, SPEC §4 Tier 3) round-robin the whole database instead
    of hammering the same first N every time.

    ``statuses`` defaults to ``active`` only: there is no point fetching the careers page of a
    company the seed loader marked ``dead`` (SPEC §10). Pass an explicit sequence to widen it.

    This lives here rather than in a connector because SPEC §11 and ``ingest/base.py`` keep
    connectors free of database access: :func:`ingest.pipeline.run_connector` runs this once
    per run and hands the result over in ``FetchContext.targets``.
    """
    wanted = tuple(statuses) if statuses is not None else (enums.CompanyStatus.ACTIVE,)
    source = aliased(CompanySource)
    rows = await session.execute(
        select(
            Company.id,
            Company.name,
            Company.domain,
            Company.website_url,
            Company.status,
            Company.ats_provider,
            Company.ats_token,
            source.external_id,
            source.last_seen_at,
        )
        .outerjoin(
            source,
            and_(source.company_id == Company.id, source.connector == connector),
        )
        .where(Company.status.in_(wanted))
        .order_by(source.last_seen_at.asc().nulls_first(), Company.id)
    )
    return [
        CompanyTarget(
            company_id=row.id,
            name=row.name,
            domain=row.domain,
            website_url=row.website_url,
            status=enums.CompanyStatus(row.status),
            ats_provider=(
                enums.AtsProvider(row.ats_provider) if row.ats_provider is not None else None
            ),
            ats_token=row.ats_token,
            external_id=row.external_id,
            last_seen_at=row.last_seen_at,
        )
        for row in rows
    ]


async def last_successful_run_started_at(session: AsyncSession, connector: str) -> datetime | None:
    """``started_at`` of the newest ``FetchRun`` for ``connector`` whose fetch completed —
    ``status IN (ok, partial)`` (:data:`COMPLETED_RUN_STATUSES`) — or ``None`` when there is
    none yet.

    This is the anchor of a connector's incremental window (SPEC §14.3 "then daily
    incremental"): a ``partial`` run scanned everything and merely had per-item trouble, so
    the window may advance past it; an ``error`` run did not finish scanning and is skipped —
    otherwise one 404ed filing would force the 18-month backfill on every run.
    """
    started_at: datetime | None = await session.scalar(
        select(func.max(FetchRun.started_at)).where(
            FetchRun.connector == connector, FetchRun.status.in_(COMPLETED_RUN_STATUSES)
        )
    )
    return started_at


async def close_missing_jobs(
    session: AsyncSession,
    *,
    company_id: int,
    connector: str,
    keep_external_ids: Iterable[str],
    now: datetime,
) -> int:
    """Set ``closed_at = now`` on ``connector``'s open jobs at ``company_id`` that are not in
    ``keep_external_ids`` (SPEC §2, §5: a job missing from its source is closed, never
    deleted — the database is the historical record).

    "This connector's jobs" are found by joining ``sources.connector`` through
    ``jobs.source_id`` (design decision 5), so a Greenhouse listing never closes a job that an
    HN comment reported. Jobs already closed are left alone (their original ``closed_at``
    stands). Returns the number of jobs closed.
    """
    keep = list(keep_external_ids)
    stmt = (
        update(Job)
        .where(
            Job.company_id == company_id,
            Job.closed_at.is_(None),
            exists().where(Source.id == Job.source_id, Source.connector == connector),
        )
        .values(closed_at=now)
    )
    if keep:
        stmt = stmt.where(Job.external_id.not_in(keep))
    result = cast("CursorResult[Any]", await session.execute(stmt))
    closed: int = result.rowcount
    return closed
