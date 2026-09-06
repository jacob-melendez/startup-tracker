"""``db.queries``' Phase 6 operations layer, against real Postgres (SPEC §7.2, §8, §12 Phase 6).

Four statements are tested here, and all four hide their edge cases inside set-based SQL:

* :func:`consecutive_failure_counts` — the failure streak SPEC §7.2 wants logged at ERROR and
  surfaced on ``/runs``. What counts as "consecutive" is a stack of deliberate choices (an
  unfinished run is neither a failure nor a success, a ``partial`` run is *not* a failure, ties
  are broken by id), and each choice gets a test that fails if it is dropped.
* :func:`stats_snapshot` — every number ``cli.py stats`` prints, read as one snapshot. Each count
  is given a *different* number of rows so a mislabelled scalar sub-select shows up as a wrong
  value instead of passing by coincidence, and the 7-day window is pinned to the microsecond on
  either side of its boundary.
* :func:`merge_companies` — the one write in the system allowed to delete a company (SPEC §8,
  ``db/models.py``: "companies are only ever deleted by ``merge-review``"). Its order of
  operations is load-bearing in both directions: children have to move *before* the delete or the
  ``ON DELETE CASCADE`` destroys them, and the survivor's NULLs have to be filled *after* it or
  the unique domain index rejects the copy. Every child table, both collision paths, the bookmark
  rescue and the ordering itself are exercised below.
* :func:`merge_candidate_rows` — the queue that merge is offered from, whose ten scalar
  sub-selects per side are the reviewer's whole evidence. Each column is given a value that
  disagrees with its neighbours' so a mislabelled sub-select is a wrong value, not a coincidence.

Rows are built with the ORM factories of ``tests/support_web.py`` plus the few this file needs of
its own. Nothing here touches the network (SPEC §2, §3).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.models import (
    Company,
    CompanyLocation,
    CompanySector,
    CompanySource,
    Contact,
    FetchRun,
    FundingRound,
    Job,
    JobBookmark,
    Location,
    MergeCandidate,
    Person,
    RoundInvestor,
    Source,
    UserNote,
)
from db.queries import (
    CONSECUTIVE_FAILURE_ALERT,
    MERGEABLE_COMPANY_FIELDS,
    STATS_WINDOW_DAYS,
    CompanySummary,
    MergeResult,
    StatsSnapshot,
    consecutive_failure_counts,
    last_successful_runs,
    merge_candidate_rows,
    merge_companies,
    refresh_company_denormalized_columns,
    stats_snapshot,
)
from ingest.pipeline import COMPANY_MERGE_FIELDS
from tests.support_web import (
    DAY,
    NOW,
    make_company_source,
    make_contact,
    make_job,
    make_location,
    make_note,
    make_round,
    make_run,
    make_sector,
)

OK = enums.FetchRunStatus.OK
PARTIAL = enums.FetchRunStatus.PARTIAL
ERROR = enums.FetchRunStatus.ERROR

# ------------------------------------------------------------------------------ row factories


async def add_company(
    session: AsyncSession,
    name: str,
    *,
    domain: str | None = None,
    website_url: str | None = None,
    one_liner: str | None = None,
    thesis: str | None = None,
    founded_year: int | None = None,
    employee_est: int | None = None,
    stage: enums.Stage = enums.Stage.UNKNOWN,
    status: enums.CompanyStatus = enums.CompanyStatus.ACTIVE,
    ats_provider: enums.AtsProvider | None = None,
    ats_token: str | None = None,
    provenance: dict[str, str] | None = None,
    first_seen_at: datetime = NOW,
    last_seen_at: datetime = NOW,
) -> Company:
    """A company carrying every column a merge may read or fill.

    ``tests.support_web.make_company`` covers the browse layer's columns; a merge also reads
    ``field_provenance`` and the two ATS columns (SPEC §8), so this module has its own factory
    rather than patching rows up after the fact.
    """
    company = Company(
        name=name,
        normalized_name=name.lower().replace(" ", ""),
        domain=domain,
        website_url=website_url,
        one_liner=one_liner,
        thesis=thesis,
        founded_year=founded_year,
        employee_est=employee_est,
        stage=stage,
        status=status,
        ats_provider=ats_provider,
        ats_token=ats_token,
        field_provenance=provenance if provenance is not None else {},
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
    )
    session.add(company)
    await session.flush()
    return company


async def add_person(
    session: AsyncSession,
    company: Company,
    full_name: str,
    *,
    title: str | None = None,
    linkedin_url: str | None = None,
) -> Person:
    row = Person(company_id=company.id, full_name=full_name, title=title, linkedin_url=linkedin_url)
    session.add(row)
    await session.flush()
    return row


async def add_bookmark(session: AsyncSession, job: Job, *, note: str | None = None) -> JobBookmark:
    """A starred job. ``note`` is the user's own writing, which a merge must not lose."""
    row = JobBookmark(job_id=job.id, starred=True, note=note)
    session.add(row)
    await session.flush()
    return row


async def add_published_contact(
    session: AsyncSession,
    company: Company,
    *,
    kind: enums.ContactKind,
    value: str,
) -> tuple[Contact, int]:
    """A ``published`` contact carrying real provenance — the ``sources`` row it was seen in.

    ``tests.support_web.make_contact`` leaves ``source_id`` NULL, which cannot distinguish "this
    address was observed" from "this link was constructed"; the promotion rule moves the
    ``source_id`` along with the confidence, so a test of it needs one that is not NULL.
    """
    source = Source(connector="company_site", url=f"https://{company.normalized_name}/careers")
    session.add(source)
    await session.flush()
    row = Contact(
        company_id=company.id,
        kind=kind,
        value=value,
        confidence=enums.ContactConfidence.PUBLISHED,
        source_id=source.id,
    )
    session.add(row)
    await session.flush()
    return row, source.id


async def add_candidate(
    session: AsyncSession,
    first: Company,
    second: Company,
    *,
    similarity: float = 0.9,
    reason: str = "trigram name match",
    resolved_at: datetime | None = None,
) -> MergeCandidate:
    """A duplicate pair, normalized ``company_id_a < company_id_b`` as the pipeline writes it."""
    low, high = sorted((first.id, second.id))
    row = MergeCandidate(
        company_id_a=low,
        company_id_b=high,
        similarity=similarity,
        reason=reason,
        resolved_at=resolved_at,
    )
    session.add(row)
    await session.flush()
    return row


async def add_run(
    session: AsyncSession,
    connector: str,
    started_at: datetime,
    status: enums.FetchRunStatus,
    *,
    finished: bool = True,
) -> FetchRun:
    """One ``fetch_runs`` row (SPEC §7.2: every run writes one, whatever the outcome).

    ``finished=False`` leaves ``finished_at`` NULL, which is what an in-flight run looks like —
    and what a run whose process was killed looks like for ever, since ``status`` defaults to
    ``error``.
    """
    return await make_run(
        session,
        connector,
        started_at=started_at,
        finished_at=started_at + timedelta(minutes=5) if finished else None,
        status=status,
    )


# ----------------------------------------------------------------------------- row readers


async def company_ids(session: AsyncSession) -> list[int]:
    rows = await session.execute(select(Company.id).order_by(Company.id))
    return list(rows.scalars())


async def owners(session: AsyncSession, entity: Any) -> list[int]:
    """The ``company_id`` of every row of ``entity``, ascending — who owns the children now."""
    rows = await session.execute(select(entity.company_id).order_by(entity.company_id))
    return list(rows.scalars())


async def row_count(session: AsyncSession, entity: Any) -> int:
    total: int | None = await session.scalar(select(func.count()).select_from(entity))
    return total or 0


async def job_ids(session: AsyncSession) -> list[int]:
    rows = await session.execute(select(Job.id).order_by(Job.id))
    return list(rows.scalars())


async def person_ids(session: AsyncSession) -> list[int]:
    rows = await session.execute(select(Person.id).order_by(Person.id))
    return list(rows.scalars())


async def bookmarks(session: AsyncSession) -> dict[int, str | None]:
    """``job_id -> note`` for every star, so a rescued bookmark can be followed by its text."""
    rows = await session.execute(select(JobBookmark.job_id, JobBookmark.note))
    return {job_id: note for job_id, note in rows}


async def contacts(session: AsyncSession) -> set[tuple[int, str, str]]:
    """``(company_id, value, confidence)`` of every contact row."""
    rows = await session.execute(select(Contact.company_id, Contact.value, Contact.confidence))
    return {(company_id, value, confidence.value) for company_id, value, confidence in rows}


async def notes(
    session: AsyncSession,
) -> list[tuple[int, enums.TrackingStatus, int | None, str | None]]:
    """``(company_id, status, rating, note)`` of every ``user_notes`` row.

    ``updated_at`` is asserted separately where it matters: only the *moved* row is stamped with
    the injected clock, and a survivor's own note keeps whatever the database wrote.
    """
    rows = await session.execute(
        select(UserNote.company_id, UserNote.status, UserNote.rating, UserNote.note).order_by(
            UserNote.company_id
        )
    )
    return [(company_id, status, rating, note) for company_id, status, rating, note in rows]


async def candidate_pairs(session: AsyncSession) -> list[tuple[int, int]]:
    rows = await session.execute(
        select(MergeCandidate.company_id_a, MergeCandidate.company_id_b).order_by(
            MergeCandidate.company_id_a, MergeCandidate.company_id_b
        )
    )
    return [(a, b) for a, b in rows]


# =============================================== the copied field list (SPEC §8, brief D5 step 4)


def test_mergeable_company_fields_is_the_pipelines_own_list() -> None:
    """``db.queries.MERGEABLE_COMPANY_FIELDS`` must equal ``ingest.pipeline.COMPANY_MERGE_FIELDS``.

    The pipeline's tuple is the source of truth: those are the fields connectors write and whose
    owner is tracked in ``Company.field_provenance`` (SPEC §8), and a merge fills exactly that
    set. :mod:`db.queries` cannot import it — ``ingest.pipeline`` imports :mod:`db.queries`, which
    imports ``ingest.base``, so the import would close a genuine cycle — so the tuple is declared
    twice and *this test* is the only thing keeping the copy honest. Add a column to one and this
    fails until it is added to the other.
    """
    assert MERGEABLE_COMPANY_FIELDS == COMPANY_MERGE_FIELDS


# ================================================== consecutive_failure_counts (SPEC §7.2)


async def test_a_connector_whose_newest_finished_run_completed_is_absent(
    session: AsyncSession,
) -> None:
    """No current streak means no key at all — callers read the mapping with ``.get(name, 0)``."""
    await add_run(session, "greenhouse", NOW - 3 * DAY, ERROR)
    await add_run(session, "greenhouse", NOW - 2 * DAY, ERROR)
    await add_run(session, "greenhouse", NOW - DAY, OK)

    counts = await consecutive_failure_counts(session)

    assert counts == {}
    assert counts.get("greenhouse", 0) == 0
    # A connector that has never run at all is likewise absent: "never ran" is the staleness
    # signal's business, not this one's.
    assert counts.get("sec_edgar", 0) == 0


async def test_the_streak_is_the_run_of_errors_at_the_head(session: AsyncSession) -> None:
    """Three failures in a row is exactly SPEC §7.2's alert threshold; older ones do not count."""
    assert CONSECUTIVE_FAILURE_ALERT == 3

    await add_run(session, "greenhouse", NOW - 6 * DAY, ERROR)  # below the ok run: invisible
    await add_run(session, "greenhouse", NOW - 5 * DAY, ERROR)
    await add_run(session, "greenhouse", NOW - 4 * DAY, OK)
    for offset in (3, 2, 1):
        await add_run(session, "greenhouse", NOW - offset * DAY, ERROR)

    counts = await consecutive_failure_counts(session)

    assert counts == {"greenhouse": 3}
    assert counts["greenhouse"] >= CONSECUTIVE_FAILURE_ALERT


async def test_a_partial_run_ends_a_streak_because_it_finished_its_scan(
    session: AsyncSession,
) -> None:
    """``partial`` is deliberately *not* a failure, and the asymmetry with the staleness signal
    is deliberate too: :func:`last_successful_runs` counts ``ok`` alone, so the same connector
    reads "never succeeded" there and "1 consecutive failure" here. Both are true — it is
    limping, not broken — and ``/runs`` shows both numbers for that reason."""
    await add_run(session, "lever", NOW - 3 * DAY, ERROR)
    await add_run(session, "lever", NOW - 2 * DAY, PARTIAL)
    await add_run(session, "lever", NOW - DAY, ERROR)

    assert await consecutive_failure_counts(session) == {"lever": 1}
    assert await last_successful_runs(session) == {}


async def test_an_unfinished_run_neither_starts_nor_breaks_a_streak(
    session: AsyncSession,
) -> None:
    """``finished_at IS NULL`` is an outcome nobody knows yet, so it is excluded from both ends.

    ``FetchRun.status`` defaults to ``error`` so a run that dies mid-flight is recorded as one —
    which means a run that is merely *still going* also reads ``error``. Counting it would report
    a failure for work in progress, and a run killed by a container restart would poison the
    count for ever.
    """
    for offset in (3, 2, 1):
        await add_run(session, "ashby", NOW - offset * DAY, ERROR)
    await add_run(session, "ashby", NOW, ERROR, finished=False)  # in front of a real streak

    # ... and in front of a completed run it does not begin one.
    await add_run(session, "workable", NOW - DAY, OK)
    await add_run(session, "workable", NOW, ERROR, finished=False)

    assert await consecutive_failure_counts(session) == {"ashby": 3}


async def test_two_runs_at_the_same_instant_are_ordered_by_id(session: AsyncSession) -> None:
    """The ``id DESC`` tie-break is what makes the answer deterministic.

    ``started_at`` defaults to the transaction clock, so two runs of one connector really can
    share an instant. Without the tie-break the window frame would be ambiguous and each of the
    two connectors below could count either 1 or 0; with it, the later-inserted row is the newer
    one and the two answers are opposite.
    """
    instant = NOW - DAY
    await add_run(session, "hn_hiring", instant, OK)  # older by id
    await add_run(session, "hn_hiring", instant, ERROR)  # newer by id: the head of the streak
    await add_run(session, "funding_rss", instant, ERROR)
    await add_run(session, "funding_rss", instant, OK)  # newer by id: no streak

    assert await consecutive_failure_counts(session) == {"hn_hiring": 1}


async def test_the_connector_filter_narrows_the_statement_to_one_name(
    session: AsyncSession,
) -> None:
    """The scheduler asks about the one connector it just ran, not about the whole table."""
    await add_run(session, "greenhouse", NOW - DAY, ERROR)
    await add_run(session, "lever", NOW - DAY, ERROR)
    await add_run(session, "lever", NOW, ERROR)
    await add_run(session, "ashby", NOW, OK)

    assert await consecutive_failure_counts(session) == {"greenhouse": 1, "lever": 2}
    assert await consecutive_failure_counts(session, connector="lever") == {"lever": 2}
    assert await consecutive_failure_counts(session, connector="ashby") == {}
    assert await consecutive_failure_counts(session, connector="no_such_connector") == {}


# ============================================================= stats_snapshot (SPEC §12 Phase 6)


async def test_stats_snapshot_of_an_empty_database_is_all_zeros(session: AsyncSession) -> None:
    """``cli.py stats`` on a freshly migrated database must print zeros, not fall over."""
    assert await stats_snapshot(session, now=NOW) == StatsSnapshot(
        companies=0,
        companies_active=0,
        companies_dead=0,
        jobs_open=0,
        jobs_closed=0,
        funding_rounds=0,
        investors=0,
        people=0,
        contacts_published=0,
        contacts_constructed=0,
        locations=0,
        sectors=0,
        merge_candidates_open=0,
        merge_candidates_resolved=0,
        fetch_runs=0,
        user_notes=0,
        job_bookmarks=0,
        companies_added=0,
        jobs_added=0,
    )


async def test_stats_snapshot_counts_every_field_from_its_own_rows(session: AsyncSession) -> None:
    """Each count gets a different number of rows, so a mislabelled sub-select cannot pass.

    The splits are the point of the report and are all exercised against a non-empty other half:
    active against dead (and an ``acquired`` company that is neither), open jobs against closed
    ones, published contacts against constructed ones (SPEC §6), and unresolved merge candidates
    against a resolved one (SPEC §8).
    """
    recent = await add_company(session, "Recent", first_seen_at=NOW - DAY)
    older = await add_company(session, "Older", first_seen_at=NOW - 30 * DAY)
    dead = await add_company(
        session, "Dead", status=enums.CompanyStatus.DEAD, first_seen_at=NOW - 2 * DAY
    )
    acquired = await add_company(
        session, "Acquired", status=enums.CompanyStatus.ACQUIRED, first_seen_at=NOW - 3 * DAY
    )

    # 3 open jobs (2 of them inside the 7-day window) and 1 closed one from long ago.
    await make_job(session, recent, external_id="j1", first_seen_at=NOW - DAY)
    await make_job(session, recent, external_id="j2", first_seen_at=NOW - 20 * DAY)
    await make_job(session, older, external_id="j3", first_seen_at=NOW - 2 * DAY)
    closed = await make_job(
        session, older, external_id="j4", first_seen_at=NOW - 40 * DAY, closed_at=NOW
    )

    await make_round(
        session,
        recent,
        announced_date=date(2026, 1, 1),
        investors=(("Alpha", True), ("Beta", False)),
    )
    await make_round(session, older, announced_date=date(2025, 5, 5), investors=(("Gamma", True),))

    for name in ("Ada", "Grace", "Katherine", "Dorothy"):
        await add_person(session, recent, name)

    await make_contact(session, recent, value="https://recent.example/careers")
    await make_contact(session, older, value="https://older.example/careers")
    await make_contact(
        session,
        recent,
        kind=enums.ContactKind.LINKEDIN_PEOPLE,
        value="https://www.linkedin.com/search/results/people/?q=recent",
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )

    await make_location(session, recent, "San Francisco", is_hq=True)
    await make_location(session, older, "Oakland")
    for sector in ("Robotics", "Climate", "Fintech"):
        await make_sector(session, recent, sector)

    await add_candidate(session, recent, older)
    await add_candidate(session, recent, dead)
    await add_candidate(session, older, acquired, resolved_at=NOW - DAY)

    for offset in range(5):
        await add_run(session, "greenhouse", NOW - offset * DAY, OK)

    await make_note(session, recent, note="promising")
    await make_note(session, older)
    await add_bookmark(session, closed)

    assert await stats_snapshot(session, now=NOW) == StatsSnapshot(
        companies=4,
        companies_active=2,
        companies_dead=1,
        jobs_open=3,
        jobs_closed=1,
        funding_rounds=2,
        investors=3,
        people=4,
        contacts_published=2,
        contacts_constructed=1,
        locations=2,
        sectors=3,
        merge_candidates_open=2,
        merge_candidates_resolved=1,
        fetch_runs=5,
        user_notes=2,
        job_bookmarks=1,
        companies_added=3,
        jobs_added=2,
    )


async def test_the_seven_day_window_is_closed_at_its_boundary(session: AsyncSession) -> None:
    """``first_seen_at >= now - 7 days``: the instant itself counts, a microsecond earlier does
    not. Postgres stores ``timestamptz`` to the microsecond, so this is the tightest pair of rows
    that can straddle the edge."""
    assert STATS_WINDOW_DAYS == 7
    edge = NOW - timedelta(days=STATS_WINDOW_DAYS)
    tick = timedelta(microseconds=1)

    inside = await add_company(session, "Inside", first_seen_at=edge)
    await add_company(session, "Outside", first_seen_at=edge - tick)
    await make_job(session, inside, external_id="in", first_seen_at=edge)
    await make_job(session, inside, external_id="out", first_seen_at=edge - tick)

    snapshot = await stats_snapshot(session, now=NOW)

    assert (snapshot.companies, snapshot.companies_added) == (2, 1)
    assert (snapshot.jobs_open, snapshot.jobs_added) == (2, 1)


# ================================================================= merge_companies (SPEC §8)


async def test_a_merge_moves_every_child_table_onto_the_survivor(session: AsyncSession) -> None:
    """The happy path: nothing collides, so every child row moves and only the company dies.

    A third, unrelated company is present throughout — a merge is a two-company operation and
    must not so much as touch a bystander's rows.
    """
    keep = await add_company(session, "Acme", domain="acme.com")
    drop = await add_company(session, "Acme Inc", domain="acme.io")
    bystander = await add_company(session, "Bystander", domain="bystander.com")

    job = await make_job(session, drop, external_id="d1")
    await add_bookmark(session, job, note="apply before Friday")
    await make_round(session, drop, announced_date=date(2026, 3, 1), investors=(("Alpha", True),))
    await add_person(session, drop, "Ada Lovelace", title="CTO")
    await make_contact(session, drop, value="https://acme.io/careers")
    await make_location(session, drop, "Oakland", is_hq=True)
    await make_sector(session, drop, "Robotics")
    await make_company_source(session, drop, "greenhouse", external_id="acmeinc")
    await make_note(session, drop, note="worth a look", rating=4)
    await add_candidate(session, keep, drop)

    bystander_job = await make_job(session, bystander, external_id="b1")
    await make_contact(session, bystander, value="https://bystander.com/careers")

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert result == MergeResult(
        keep_id=keep.id,
        drop_id=drop.id,
        fields_filled=(),  # the survivor already has a domain; everything else is NULL on both
        jobs_moved=1,
        jobs_discarded=0,
        bookmarks_moved=0,  # a star on a job that simply moves needs no rescue
        funding_rounds_moved=1,
        people_moved=1,
        people_deduped=0,
        contacts_moved=1,
        contacts_discarded=0,
        locations_moved=1,
        sectors_moved=1,
        sources_moved=1,
        candidates_repointed=0,
        note_moved=True,
        note_discarded=None,
    )

    assert await company_ids(session) == [keep.id, bystander.id]
    assert await owners(session, Job) == [keep.id, bystander.id]
    assert await owners(session, Contact) == [keep.id, bystander.id]
    for entity in (FundingRound, Person, CompanyLocation, CompanySector, CompanySource, UserNote):
        assert await owners(session, entity) == [keep.id], entity.__name__
    # The round kept its investor link, and the starred job kept its own id and note.
    assert await row_count(session, RoundInvestor) == 1
    assert await bookmarks(session) == {job.id: "apply before Friday"}
    assert await job_ids(session) == sorted([job.id, bystander_job.id])
    # The reviewed pair referenced the discarded company, so the cascade took it: a merged pair
    # leaves no resolved_at row behind (a "not a duplicate" decision is what leaves one).
    assert await candidate_pairs(session) == []


async def test_a_duplicate_job_is_discarded_and_its_star_rescued_onto_the_twin(
    session: AsyncSession,
) -> None:
    """``uq_jobs_company_id_external_id`` cannot hold two rows, so one posting has to go — but
    the user's star on it must not go with it."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    twin = await make_job(session, keep, "Survivor's own copy", external_id="shared")
    duplicate = await make_job(session, drop, "Duplicate", external_id="shared")
    unique = await make_job(session, drop, "Only here", external_id="only-here")
    await add_bookmark(session, duplicate, note="ping the recruiter")
    await add_bookmark(session, unique, note="closes soon")

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.jobs_moved, result.jobs_discarded, result.bookmarks_moved) == (1, 1, 1)
    assert await job_ids(session) == sorted([twin.id, unique.id])
    assert await owners(session, Job) == [keep.id, keep.id]
    # The star moved to the survivor's twin, note and all; the star on the moving job stayed put.
    assert await bookmarks(session) == {twin.id: "ping the recruiter", unique.id: "closes soon"}


async def test_a_star_already_on_the_twin_is_not_overwritten(session: AsyncSession) -> None:
    """The rescue is guarded by the bookmark's own primary key: one row per job. When the
    survivor's copy is already starred there is nothing to rescue, and the survivor's note is the
    one that stands."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    twin = await make_job(session, keep, external_id="shared")
    duplicate = await make_job(session, drop, external_id="shared")
    await add_bookmark(session, twin, note="mine")
    await add_bookmark(session, duplicate, note="theirs")

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.jobs_moved, result.jobs_discarded, result.bookmarks_moved) == (0, 1, 0)
    assert await bookmarks(session) == {twin.id: "mine"}
    # ...but the losing note is handed back rather than dropped with the row, the same rule
    # ``user_notes`` gets: it is the user's own writing, and this is its last copy.
    assert result.bookmark_notes_discarded == ("theirs",)


async def test_a_rescued_or_moved_bookmark_note_is_not_reported_as_discarded(
    session: AsyncSession,
) -> None:
    """Only notes that are actually about to be destroyed are reported.

    A bookmark on a job that merely moves keeps its row (the job keeps its id), and a rescued
    one is now on the survivor's twin. Reporting either would train the reviewer to ignore the
    line that matters.
    """
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_job(session, keep, external_id="shared")
    rescued = await make_job(session, drop, external_id="shared")
    moving = await make_job(session, drop, external_id="only-theirs")
    await add_bookmark(session, rescued, note="rescued onto the twin")
    await add_bookmark(session, moving, note="moves with its job")

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert result.bookmarks_moved == 1
    assert result.bookmark_notes_discarded == ()


async def test_a_colliding_contact_is_discarded_whatever_its_confidence(
    session: AsyncSession,
) -> None:
    """``uq_contacts_company_id_kind_value`` ignores ``confidence``, so a constructed duplicate of
    a published address collides — and the survivor's published row is the one that stays
    (SPEC §6 keeps the two confidences apart precisely because they are not interchangeable)."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_contact(
        session, keep, kind=enums.ContactKind.EMAIL, value="jobs@acme.com"
    )  # published
    await make_contact(
        session,
        drop,
        kind=enums.ContactKind.EMAIL,
        value="jobs@acme.com",
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )
    await make_contact(session, drop, kind=enums.ContactKind.EMAIL, value="hi@acme.com")
    await make_contact(
        session,
        drop,
        kind=enums.ContactKind.LINKEDIN_COMPANY,
        value="https://www.linkedin.com/company/acme",
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.contacts_moved, result.contacts_discarded) == (2, 1)
    assert await contacts(session) == {
        (keep.id, "jobs@acme.com", "published"),
        (keep.id, "hi@acme.com", "published"),
        (keep.id, "https://www.linkedin.com/company/acme", "constructed"),
    }


async def test_a_survivors_constructed_contact_is_promoted_by_the_published_twin(
    session: AsyncSession,
) -> None:
    """The mirror of the case above, and the one that would otherwise lose information.

    ``_move_guarded`` resolves the collision by row, keeping the survivor's — so when the
    survivor's row is the *constructed* guess and the discarded company's is the address the
    company actually published, resolving by row alone would delete an observation and leave a
    guess wearing its place in the table. SPEC §6 makes that distinction load-bearing: the UI
    marks the two apart, so a downgrade is displayed to the user as fact. The survivor's row is
    promoted in place instead — same row id, same counts — and takes the ``source_id`` that
    proves where the address was seen.
    """
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    url = "https://www.linkedin.com/company/acme"
    await make_contact(
        session,
        keep,
        kind=enums.ContactKind.LINKEDIN_COMPANY,
        value=url,
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )
    mine = await session.scalar(select(Contact.id).where(Contact.company_id == keep.id))
    _, source_id = await add_published_contact(
        session, drop, kind=enums.ContactKind.LINKEDIN_COMPANY, value=url
    )

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    # The collision is still resolved by row: nothing moved, the drop's row was discarded.
    assert (result.contacts_moved, result.contacts_discarded) == (0, 1)
    rows = await session.execute(select(Contact.id, Contact.confidence, Contact.source_id))
    assert list(rows) == [(mine, enums.ContactConfidence.PUBLISHED, source_id)]


async def test_a_published_survivor_is_not_disturbed_by_a_constructed_twin(
    session: AsyncSession,
) -> None:
    """The promotion runs one way only. A survivor that already published the address keeps its
    own ``source_id``; the discarded guess brings nothing, and there is nothing to promote."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    url = "https://www.linkedin.com/company/acme"
    _, source_id = await add_published_contact(
        session, keep, kind=enums.ContactKind.LINKEDIN_COMPANY, value=url
    )
    await make_contact(
        session,
        drop,
        kind=enums.ContactKind.LINKEDIN_COMPANY,
        value=url,
        confidence=enums.ContactConfidence.CONSTRUCTED,
    )

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.contacts_moved, result.contacts_discarded) == (0, 1)
    rows = await session.execute(select(Contact.confidence, Contact.source_id))
    assert list(rows) == [(enums.ContactConfidence.PUBLISHED, source_id)]


async def test_composite_key_collisions_leave_the_survivors_own_row_alone(
    session: AsyncSession,
) -> None:
    """``company_locations``, ``company_sectors`` and ``company_sources`` are keyed on
    ``(company_id, <other>)``, so a link the survivor already has cannot move. The survivor's row
    wins every collision, which is why the discarded row's ``is_hq`` flag and ``external_id`` do
    not leak onto it."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_location(session, keep, "San Francisco", is_hq=False)
    await make_location(session, drop, "San Francisco", is_hq=True)  # collides
    await make_location(session, drop, "Oakland", is_hq=True)  # moves, flag and all
    await make_sector(session, keep, "Robotics")
    await make_sector(session, drop, "Robotics")  # collides
    await make_sector(session, drop, "Climate")  # moves
    await make_company_source(session, keep, "greenhouse", external_id="keep-token")
    await make_company_source(session, drop, "greenhouse", external_id="drop-token")  # collides
    await make_company_source(session, drop, "lever", external_id="lever-token")  # moves

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.locations_moved, result.sectors_moved, result.sources_moved) == (1, 1, 1)
    for entity in (CompanyLocation, CompanySector, CompanySource):
        assert await owners(session, entity) == [keep.id, keep.id], entity.__name__

    cities = await session.execute(
        select(Location.city, CompanyLocation.is_hq)
        .join(Location, Location.id == CompanyLocation.location_id)
        .order_by(Location.city)
    )
    # The moved link brought its own is_hq; the survivor's own link was not rewritten.
    assert list(cities) == [("Oakland", True), ("San Francisco", False)]
    sources = await session.execute(
        select(CompanySource.connector, CompanySource.external_id).order_by(CompanySource.connector)
    )
    assert list(sources) == [("greenhouse", "keep-token"), ("lever", "lever-token")]
    # The greenhouse id the survivor's row displaced is a real loss, and it is reported.
    assert result.sources_discarded == (("greenhouse", "drop-token"),)


async def test_a_survivor_with_no_external_id_adopts_the_discarded_rows(
    session: AsyncSession,
) -> None:
    """The composite key stops the *row* moving, but the column it carries is the identifier
    SPEC §8 step 0 resolves that connector's records by — and a survivor row that has none is
    strictly worse than one that has the discarded row's. Nothing is discarded here, so nothing
    is reported: the loss the reviewer needs to hear about is the one that cannot be avoided.
    """
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_company_source(session, keep, "sec_edgar")  # external_id NULL
    await make_company_source(session, drop, "sec_edgar", external_id="0009999999")

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.sources_moved, result.sources_discarded) == (0, ())
    sources = await session.execute(select(CompanySource.connector, CompanySource.external_id))
    assert list(sources) == [("sec_edgar", "0009999999")]


async def test_a_discarded_source_row_with_no_external_id_is_not_reported_as_a_loss(
    session: AsyncSession,
) -> None:
    """ "Both rows agree" and "the discarded row knew nothing" are not losses.

    Reporting either would put a line in front of the reviewer saying a connector will recreate
    the company, on a merge where it will not — which is worse than saying nothing, because the
    line's whole job is to be rare enough to act on.
    """
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_company_source(session, keep, "ashby", external_id="acme")
    await make_company_source(session, drop, "ashby")  # nothing to lose
    await make_company_source(session, keep, "lever", external_id="acme")
    await make_company_source(session, drop, "lever", external_id="acme")  # identical

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.sources_moved, result.sources_discarded) == (0, ())


async def test_people_are_deduped_within_the_survivor_only_on_an_exact_match(
    session: AsyncSession,
) -> None:
    """``people`` has no unique constraint, so the move can leave the same founder listed twice.
    Identity is exact — name plus title plus LinkedIn URL, NULLs folded to ``''`` — and the lowest
    id (the first sighting) survives. A different title is a different person until a human says
    otherwise, the same rule SPEC §8 applies to companies."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    mine = await add_person(session, keep, "Ada Lovelace", title="CEO")
    untitled = await add_person(session, keep, "Grace Hopper")
    await add_person(session, drop, "Ada Lovelace", title="CEO")  # byte-identical
    await add_person(session, drop, "Grace Hopper")  # identical with NULL titles
    other_title = await add_person(session, drop, "Ada Lovelace", title="CTO")
    other_link = await add_person(
        session, drop, "Grace Hopper", linkedin_url="https://www.linkedin.com/in/grace"
    )

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.people_moved, result.people_deduped) == (4, 2)
    assert await person_ids(session) == sorted(
        [mine.id, untitled.id, other_title.id, other_link.id]
    )


async def test_a_user_note_moves_when_the_survivor_has_none(session: AsyncSession) -> None:
    """The row moves wholesale, keeping its status, rating and text, and is stamped with the
    injected clock rather than the database's."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_note(
        session, drop, status=enums.TrackingStatus.APPLIED, rating=5, note="phone screen booked"
    )

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.note_moved, result.note_discarded) == (True, None)
    assert await notes(session) == [
        (keep.id, enums.TrackingStatus.APPLIED, 5, "phone screen booked")
    ]
    stamped = await session.scalar(select(UserNote.updated_at))
    assert stamped == NOW


async def test_two_notes_keep_the_survivors_and_hand_back_the_other_text(
    session: AsyncSession,
) -> None:
    """``user_notes`` is one row per company, so one of the two has to go — but the user's own
    writing is never destroyed silently: the discarded text comes back for the CLI to print."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_note(
        session, keep, status=enums.TrackingStatus.INTERESTED, rating=3, note="the survivor's"
    )
    await make_note(session, drop, status=enums.TrackingStatus.REJECTED, note="the other one's")

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.note_moved, result.note_discarded) == (False, "the other one's")
    assert await notes(session) == [(keep.id, enums.TrackingStatus.INTERESTED, 3, "the survivor's")]


async def test_a_blank_survivor_stub_does_not_outrank_a_filled_note(
    session: AsyncSession,
) -> None:
    """ "The survivor has a note" is a question about content, not about a row.

    ``save_note`` upserts unconditionally, so pressing Save with the form untouched — or clearing
    a note that had been written — leaves ``(status='none', rating=NULL, note=NULL)``: a row the
    UI renders exactly like no note at all. Treating that stub as the survivor's note would
    destroy a real ``applied / 5`` on the other side *and* report nothing, because
    ``note_discarded`` carries text and there is no text to hand back.
    """
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_note(session, keep, status=enums.TrackingStatus.NONE, rating=None, note=None)
    await make_note(
        session, drop, status=enums.TrackingStatus.APPLIED, rating=5, note="phone screen booked"
    )

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.note_moved, result.note_discarded) == (True, None)
    assert await notes(session) == [
        (keep.id, enums.TrackingStatus.APPLIED, 5, "phone screen booked")
    ]


@pytest.mark.parametrize(
    ("status", "rating", "note"),
    [
        (enums.TrackingStatus.INTERESTED, None, None),
        (enums.TrackingStatus.NONE, 4, None),
        (enums.TrackingStatus.NONE, None, "just a note"),
    ],
)
async def test_any_one_filled_column_makes_the_survivors_note_its_own(
    session: AsyncSession,
    status: enums.TrackingStatus,
    rating: int | None,
    note: str | None,
) -> None:
    """The stub rule is about a row with *nothing* in it. A tracking status on its own, a rating
    on its own or text on its own is the user's decision about this company, and the discarded
    note does not overwrite it — it comes back to be printed instead."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_note(session, keep, status=status, rating=rating, note=note)
    await make_note(session, drop, status=enums.TrackingStatus.APPLIED, note="the other one's")

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert (result.note_moved, result.note_discarded) == (False, "the other one's")
    assert await notes(session) == [(keep.id, status, rating, note)]


async def test_null_fields_are_filled_from_the_discarded_row_with_its_provenance(
    session: AsyncSession,
) -> None:
    """SPEC §8's rule, applied to a merge: fill what the survivor does not know, never overwrite
    what it does. The provenance entry travels with the value it explains, except where the
    survivor already claims that field — its own entries always win."""
    keep = await add_company(
        session,
        "Acme",
        domain=None,
        website_url="https://acme.com",
        employee_est=42,
        stage=enums.Stage.SEED,
        provenance={"website_url": "greenhouse", "employee_est": "greenhouse"},
        first_seen_at=NOW - 10 * DAY,
        last_seen_at=NOW - 2 * DAY,
    )
    drop = await add_company(
        session,
        "Acme Inc",
        domain="acme.com",
        website_url="https://acme.io",
        one_liner="Robots for docks",
        founded_year=2021,
        employee_est=99,
        stage=enums.Stage.SERIES_A,
        status=enums.CompanyStatus.DEAD,
        ats_provider=enums.AtsProvider.GREENHOUSE,
        ats_token="acmeinc",
        provenance={
            "domain": "sec_edgar",
            "website_url": "sec_edgar",
            "one_liner": "ycombinator",
            "founded_year": "sec_edgar",
            "employee_est": "sec_edgar",
            "ats_token": "company_site",
        },
        first_seen_at=NOW - 30 * DAY,
        last_seen_at=NOW - DAY,
    )

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    # In MERGEABLE_COMPANY_FIELDS order, and only where the survivor had nothing.
    assert result.fields_filled == (
        "domain",
        "one_liner",
        "founded_year",
        "ats_provider",
        "ats_token",
    )

    await session.refresh(keep)
    assert (keep.domain, keep.one_liner) == ("acme.com", "Robots for docks")
    assert keep.founded_year == 2021
    assert (keep.ats_provider, keep.ats_token) == (enums.AtsProvider.GREENHOUSE, "acmeinc")
    # Untouched: a value the survivor already had, and the NOT NULL columns a fill never reaches.
    # `name` in particular cannot change here, which is what keeps `normalized_name` consistent.
    assert (keep.name, keep.normalized_name) == ("Acme", "acme")
    assert (keep.website_url, keep.employee_est) == ("https://acme.com", 42)
    assert (keep.stage, keep.status) == (enums.Stage.SEED, enums.CompanyStatus.ACTIVE)
    assert keep.thesis is None  # NULL on both sides: not "filled" with a NULL
    assert keep.field_provenance == {
        "website_url": "greenhouse",  # the survivor's own entry wins over sec_edgar
        "employee_est": "greenhouse",
        "domain": "sec_edgar",
        "one_liner": "ycombinator",
        "founded_year": "sec_edgar",
        "ats_token": "company_site",
        # ats_provider: the discarded row recorded no owner, so none is invented.
    }
    # The survivor inherits the full observed history of both rows (SPEC §2).
    assert (keep.first_seen_at, keep.last_seen_at) == (NOW - 30 * DAY, NOW - DAY)


async def test_the_domain_can_only_be_filled_because_the_delete_comes_first(
    session: AsyncSession,
) -> None:
    """A regression guard on the *order* of the last two steps, not on their result.

    ``ix_companies_domain`` is unique, so copying the discarded row's domain onto the survivor
    while both rows still exist raises a unique violation. Swap step 3 (delete the company) and
    step 4 (fill the NULLs) in :func:`merge_companies` and this test stops passing — it fails with
    an ``IntegrityError`` rather than a wrong value, which is why the index is asserted to be
    real first: without it the ordering would not matter and the guard would be vacuous.
    """
    keep = await add_company(session, "Acme", domain=None)
    drop = await add_company(session, "Acme Inc", domain="acme.com")

    with pytest.raises(IntegrityError):
        async with session.begin_nested():
            session.add(Company(name="Third", normalized_name="third", domain="acme.com"))
            await session.flush()

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert result.fields_filled == ("domain",)
    await session.refresh(keep)
    assert keep.domain == "acme.com"
    assert await company_ids(session) == [keep.id]


async def test_other_candidates_are_repointed_normalized_and_deduplicated(
    session: AsyncSession,
) -> None:
    """Every *other* suspicion about the discarded company becomes a suspicion about the survivor.

    ``merge_candidates`` is unique on the ordered pair, so a repointed row has to be renormalized
    to ``company_id_a < company_id_b`` — and the pair the survivor already holds with the same
    third company is skipped rather than duplicated. The pair under review is skipped too: it
    references the discarded company, so the cascade removes it.
    """
    early = await add_company(session, "Early")
    keep = await add_company(session, "Keep")
    drop = await add_company(session, "Drop")
    third = await add_company(session, "Third")
    fourth = await add_company(session, "Fourth")
    assert early.id < keep.id < drop.id  # the ids the normalization below depends on

    await add_candidate(session, keep, drop)  # under review: dies with the cascade
    await add_candidate(session, early, drop)  # drop is in column b: (early, drop) -> (early, keep)
    await add_candidate(session, drop, third)  # drop is in column a: (drop, third) -> (keep, third)
    await add_candidate(session, keep, fourth)  # already held by the survivor
    await add_candidate(session, drop, fourth)  # would duplicate it: skipped, dies with the cascade

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert result.candidates_repointed == 2
    pairs = await candidate_pairs(session)
    assert pairs == [(early.id, keep.id), (keep.id, third.id), (keep.id, fourth.id)]
    assert all(a < b for a, b in pairs)
    assert len(set(pairs)) == len(pairs)


async def test_a_reviewed_verdict_about_the_discarded_company_is_not_carried_over(
    session: AsyncSession,
) -> None:
    """A resolved pair naming the discarded company dies with it rather than being repointed.

    A reviewer who marked ``(drop, third)`` "not a duplicate" said that about ``drop``. Moving
    the verdict onto ``keep`` would assert something nobody decided — and it would stick for
    ever, because ``ingest.pipeline``'s candidate upsert refreshes a row only ``WHERE
    resolved_at IS NULL``, so the pipeline could never raise ``(keep, third)`` again on its own
    evidence. Letting the cascade take it is the honest outcome.
    """
    keep = await add_company(session, "Keep")
    drop = await add_company(session, "Drop")
    third = await add_company(session, "Third")
    fourth = await add_company(session, "Fourth")

    await add_candidate(session, keep, drop)  # under review
    await add_candidate(session, drop, third, resolved_at=NOW - DAY)  # reviewed: not carried over
    await add_candidate(session, drop, fourth)  # still open: repointed as usual

    result = await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    assert result.candidates_repointed == 1
    assert await candidate_pairs(session) == [(keep.id, fourth.id)]


async def test_the_survivors_denormalized_columns_are_refreshed(session: AsyncSession) -> None:
    """SPEC §5's three denormalized columns are stale the moment a merge moves jobs and rounds,
    and ``/``'s default sort reads them — so the merge recomputes them for the survivor."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_job(session, keep, external_id="k1", posted_at=NOW - 5 * DAY)
    await make_job(session, drop, external_id="d1", posted_at=NOW - DAY)
    await make_job(session, drop, external_id="d2", posted_at=NOW - 2 * DAY)
    await make_job(session, drop, external_id="d3", posted_at=NOW, closed_at=NOW)  # closed
    old_round = await make_round(session, keep, announced_date=date(2025, 1, 1))
    new_round = await make_round(session, drop, announced_date=date(2026, 6, 1))

    # Leave the survivor's columns wrong on purpose, as a database between runs is.
    keep.open_job_count = 99
    keep.latest_job_posted_at = None
    keep.latest_round_id = old_round.id
    await session.flush()

    await merge_companies(session, keep_id=keep.id, drop_id=drop.id, now=NOW)

    await session.refresh(keep)
    assert keep.open_job_count == 3  # the closed job is not counted
    assert keep.latest_job_posted_at == NOW - DAY
    assert keep.latest_round_id == new_round.id


async def test_merge_rejects_equal_ids_and_a_missing_company(session: AsyncSession) -> None:
    """Both are programming errors, not review outcomes, so they raise rather than no-op."""
    keep = await add_company(session, "Acme")
    ghost = keep.id + 1_000

    with pytest.raises(ValueError, match="into itself"):
        await merge_companies(session, keep_id=keep.id, drop_id=keep.id, now=NOW)
    with pytest.raises(ValueError, match=f"no such company: {ghost}"):
        await merge_companies(session, keep_id=keep.id, drop_id=ghost, now=NOW)
    with pytest.raises(ValueError, match=f"no such company: {ghost}"):
        await merge_companies(session, keep_id=ghost, drop_id=keep.id, now=NOW)

    assert await company_ids(session) == [keep.id]


async def test_merge_companies_commits_nothing_itself(
    session: AsyncSession, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """The caller owns the transaction: ``merge-review`` commits once per pair, so a reviewer who
    quits after three merges keeps exactly those three and a failure part-way through one pair
    rolls that pair back whole."""
    keep = await add_company(session, "Acme")
    drop = await add_company(session, "Acme Inc")
    await make_job(session, drop, external_id="d1")
    # The ids are read now and kept as plain ints: the rollback below expires every ORM object,
    # and re-reading one afterwards would be a lazy load, not an assertion about the database.
    keep_id, drop_id = keep.id, drop.id
    await session.commit()

    result = await merge_companies(session, keep_id=keep_id, drop_id=drop_id, now=NOW)
    assert result.jobs_moved == 1

    # A second connection sees the pre-merge database: nothing has been committed.
    async with session_factory() as other:
        assert await company_ids(other) == [keep_id, drop_id]
        assert await owners(other, Job) == [drop_id]

    await session.rollback()
    assert await company_ids(session) == [keep_id, drop_id]
    assert await owners(session, Job) == [drop_id]


# ============================================================ merge_candidate_rows (SPEC §8, §12)


async def test_a_candidate_row_carries_the_whole_side_by_side_summary(
    session: AsyncSession,
) -> None:
    """Every column ``cli.format_candidate`` renders, read back from rows built to disagree.

    The side-by-side table is the reviewer's entire evidence for a decision that deletes a
    company, so each value is given a *different* number on the two sides — and, within one
    side, funding rounds and contacts are given different counts from each other — so a
    sub-select carrying the wrong label, or the two timestamps transposed, shows up as a wrong
    value rather than passing by coincidence. ``city`` gets the two cases its ``ORDER BY`` has:
    an HQ that does not sort first, and no HQ at all.
    """
    older = NOW - 10 * DAY
    a = await add_company(
        session,
        "Acme Robotics",
        domain="acme.com",
        stage=enums.Stage.SEED,
        status=enums.CompanyStatus.ACTIVE,
        first_seen_at=older,
        last_seen_at=NOW,
    )
    # "Oakland" sorts before "San Francisco"; the HQ flag has to win, or the table names the
    # wrong city for a company whose metro is half the reason two rows look like one.
    await make_location(session, a, "Oakland", is_hq=False)
    await make_location(session, a, "San Francisco", is_hq=True)
    await make_round(session, a, announced_date=date(2026, 1, 5))
    await make_round(session, a, announced_date=date(2026, 2, 5))
    for value in ("https://acme.com/careers", "jobs@acme.com", "hi@acme.com"):
        await make_contact(session, a, kind=enums.ContactKind.EMAIL, value=value)
    await make_job(session, a, external_id="a1")
    await make_job(session, a, external_id="a2", closed_at=NOW)
    await make_company_source(session, a, "lever")
    await make_company_source(session, a, "greenhouse")

    b = await add_company(
        session,
        "Acme Robotics Inc",
        stage=enums.Stage.UNKNOWN,
        status=enums.CompanyStatus.DEAD,
        first_seen_at=NOW,
        last_seen_at=NOW,
    )
    # No HQ anywhere, so the alphabetical tie-break is what picks the city.
    await make_location(session, b, "Berkeley", is_hq=False)
    await make_location(session, b, "Alameda", is_hq=False)
    await make_contact(session, b, kind=enums.ContactKind.EMAIL, value="hello@acme.io")
    await add_candidate(session, a, b, similarity=0.91, reason="trigram name match")
    await refresh_company_denormalized_columns(session, [a.id, b.id])

    (row,) = await merge_candidate_rows(session, limit=20)

    assert row.similarity == pytest.approx(0.91)
    assert row.reason == "trigram name match"
    assert row.a == CompanySummary(
        id=a.id,
        name="Acme Robotics",
        domain="acme.com",
        city="San Francisco",
        stage=enums.Stage.SEED,
        status=enums.CompanyStatus.ACTIVE,
        open_job_count=1,  # the closed one is not offered as a reason to keep this side
        funding_round_count=2,
        contact_count=3,
        first_seen_at=older,
        last_seen_at=NOW,
        connectors=("greenhouse", "lever"),
    )
    assert row.b == CompanySummary(
        id=b.id,
        name="Acme Robotics Inc",
        domain=None,
        city="Alameda",
        stage=enums.Stage.UNKNOWN,
        status=enums.CompanyStatus.DEAD,
        open_job_count=0,
        funding_round_count=0,
        contact_count=1,
        first_seen_at=NOW,
        last_seen_at=NOW,
        # ``array_agg`` over no rows is NULL, and the CLI joins this with ", ".
        connectors=(),
    )


async def test_the_queue_is_most_similar_first_then_oldest_and_resolved_pairs_are_gone(
    session: AsyncSession,
) -> None:
    """Ordering is ``similarity DESC, id ASC`` so the list is stable while a reviewer works
    through it, and ``resolved_at IS NULL`` because a pair called "not a duplicate" must never
    be offered again (SPEC §8). ``limit`` caps the page without hiding the rest of the queue."""
    companies = [await add_company(session, f"Acme {index}") for index in range(5)]
    # Written least-similar first, so config order and insertion order cannot both be right.
    last = await add_candidate(session, companies[0], companies[1], similarity=0.86)
    tied_first = await add_candidate(session, companies[0], companies[2], similarity=0.95)
    tied_second = await add_candidate(session, companies[1], companies[2], similarity=0.95)
    resolved = await add_candidate(
        session, companies[3], companies[4], similarity=0.99, resolved_at=NOW
    )

    offered = [row.id for row in await merge_candidate_rows(session, limit=20)]

    # The two 0.95 pairs tie, and ``id ASC`` breaks it: the earlier row stays ahead. The 0.99
    # pair outranks all three on similarity and is absent anyway, because it is resolved.
    assert offered == [tied_first.id, tied_second.id, last.id]
    assert resolved.id not in offered
    assert [row.id for row in await merge_candidate_rows(session, limit=1)] == [tied_first.id]
    with pytest.raises(ValueError, match="limit must be at least 1"):
        await merge_candidate_rows(session, limit=0)
