"""``cli.py stats`` and ``cli.py merge-review`` end to end (SPEC §12 Phase 6, §8, §7.2).

Both commands are read-mostly reports over the local database, so they are driven the way an
operator drives them — through ``CliRunner`` against the disposable Postgres, with answers piped
in — rather than by calling their helpers. What is asserted is the *contract* a reader or a
script depends on: which sections exist, that a number in the report is the number in the table,
that the database password never reaches stdout, and that every review key does what the prompt
says it does. Nothing here touches the network; neither command can (SPEC §2).

Every test is a **sync** function, as ``tests/test_cli.py``'s are, because both commands call
``asyncio.run`` and that cannot happen inside pytest-asyncio's running loop. Database setup and
read-back therefore go through :func:`db`, which opens its own engine and loop — the same
arrangement that file uses for its ``sync-contacts`` tests.

``stats`` reads ``now`` from the clock rather than taking it as an argument, so fixtures here are
placed relative to that same clock. The microsecond-exact edge of the 7-day window belongs to
``tests/test_merge.py``, which can inject a fixed one; what this file checks is that the command
carries a live clock through to the query at all.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import structlog
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from typer.testing import CliRunner, Result

import cli
from db import enums
from db.models import Base, Company, Contact, Job, JobBookmark, MergeCandidate, UserNote
from db.queries import (
    CONSECUTIVE_FAILURE_ALERT,
    STATS_WINDOW_DAYS,
    CompanySummary,
    MergeCandidateRow,
)
from db.session import dispose_engine
from ingest.config import UNIMPLEMENTED_CONNECTORS, load_connectors_config
from ingest.contacts import (
    constructed_linkedin_company_url,
    people_search_url,
)
from settings import Settings, get_settings
from tests.support_web import (
    make_bookmark,
    make_company,
    make_company_source,
    make_contact,
    make_job,
    make_location,
    make_note,
    make_round,
    make_run,
    make_sector,
)

#: A password that must never appear on stdout. Postgres is on trust auth here, so a URL
#: carrying it still connects — which is what makes the assertion a real one rather than a
#: string check against a URL nothing ever used.
SECRET = "hunter2-should-never-be-printed"


@pytest.fixture(autouse=True)
def cli_env(
    monkeypatch: pytest.MonkeyPatch, migrated_database: str, tmp_path: Path
) -> Iterator[None]:
    """``DATABASE_URL`` → the disposable database *with a password on it*, ``CONTACT_EMAIL``
    unset, the settings cache cleared either side, and the engine disposed after.

    The same shape as ``tests/test_cli.py``'s fixture, plus the password: every test in this
    module then runs against a URL that has a secret in it, so the redaction is exercised by
    the whole file rather than by the one test that remembers to ask for it.
    """
    monkeypatch.setenv("DATABASE_URL", str(make_url(migrated_database).set(password=SECRET)))
    monkeypatch.setenv("CONTACT_EMAIL", "")
    monkeypatch.setenv("HTTP_CACHE_DIR", str(tmp_path / "http-cache"))
    monkeypatch.setitem(Settings.model_config, "env_file", None)
    get_settings.cache_clear()
    try:
        yield
    finally:
        get_settings.cache_clear()
        asyncio.run(dispose_engine())


@pytest.fixture(autouse=True)
def quiet_logging(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep structlog out of stdout, as ``tests/test_cli.py`` does: unconfigured, it prints
    there, and this module asserts on exact stdout lines."""
    captured = structlog.testing.CapturingLoggerFactory()

    def fake_configure(level: str = "INFO", json_output: bool = True) -> None:
        structlog.configure(logger_factory=captured, cache_logger_on_first_use=False)

    monkeypatch.setattr(cli, "configure_logging", fake_configure)
    try:
        yield
    finally:
        structlog.reset_defaults()


@pytest.fixture(autouse=True)
def empty_database(migrated_database: str) -> str:
    """Every table truncated before the test runs, identities restarted so ids are predictable.

    Autouse because every test here counts rows or names ids: conftest's truncating ``engine``
    fixture is async and out of reach of a ``CliRunner`` test, so this is its sync twin.
    Truncation is at setup, not teardown, so a failure leaves its rows behind to look at.
    """

    async def truncate() -> None:
        engine = create_async_engine(migrated_database)
        tables = ", ".join(f'"{table.name}"' for table in Base.metadata.sorted_tables)
        try:
            async with engine.begin() as conn:
                await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
        finally:
            await engine.dispose()

    asyncio.run(truncate())
    return migrated_database


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def db[T](work: Callable[[AsyncSession], Awaitable[T]]) -> T:
    """Run one unit of database work in its own engine and event loop, committing at the end.

    Arranging fixtures and reading results back both go through this, so a test reads as
    arrange / invoke / assert with the loop boundary invisible. A fresh engine per call is what
    keeps it safe next to the CLI's own ``asyncio.run``: an asyncpg pool belongs to the loop
    that created it, and neither side may inherit the other's.
    """

    async def run() -> T:
        engine = create_async_engine(os.environ["DATABASE_URL"])
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                result = await work(session)
                await session.commit()
                return result
        finally:
            await engine.dispose()

    return asyncio.run(run())


def invoke(runner: CliRunner, *args: str, stdin: str | None = None) -> Result:
    return runner.invoke(cli.app, list(args), input=stdin, catch_exceptions=False)


def sections(output: str) -> dict[str, list[str]]:
    """The report as ``heading -> indented lines``.

    ``stats`` is designed to be read down and grepped, which makes its shape part of its
    contract: a bare word at column 0 opens a section and everything under it is indented.
    Parsing it back that way lets a test assert what is in the last-7-days section without
    pinning column widths that are computed from the data.
    """
    found: dict[str, list[str]] = {}
    heading = ""
    for line in output.splitlines():
        if not line:
            continue
        if line.startswith(cli.INDENT):
            found.setdefault(heading, []).append(line.strip())
        else:
            heading = line.split("  ")[0]
            found.setdefault(heading, [])
    return found


def entry(output: str, heading: str, label: str) -> str:
    """The one line under ``heading`` that starts with ``label``, whitespace collapsed.

    Collapsed because the alignment padding is computed from the widest value in the section,
    so pinning it would make every assertion depend on unrelated rows.
    """
    matches = [line for line in sections(output)[heading] if line.startswith(label)]
    assert len(matches) == 1, f"{label!r} under {heading!r}: {matches}"
    return " ".join(matches[0].split())


def summary_line(output: str) -> str:
    lines = [line for line in output.splitlines() if line.startswith("merge-review  ")]
    assert len(lines) == 1, output
    return lines[0]


# ============================================================================= stats


def test_stats_prints_the_three_sections_on_an_empty_database(runner: CliRunner) -> None:
    """SPEC §12 Phase 6 names three things; an empty database must still print all three.

    Zero is an answer. A report that dropped a section when its numbers were all zero would
    make "no companies yet" look exactly like "the section is broken", and a freshly migrated
    database is the first thing this command is pointed at.
    """
    result = invoke(runner, "stats")

    assert result.exit_code == 0, result.output
    window = f"last {STATS_WINDOW_DAYS} days"
    assert set(sections(result.stdout)) >= {"database", "rows", "last successful run", window}
    assert entry(result.stdout, "rows", "companies") == "companies 0 (0 active, 0 acquired, 0 dead)"
    assert entry(result.stdout, "rows", "jobs") == "jobs 0 open, 0 closed"
    assert entry(result.stdout, window, "companies added") == "companies added 0"
    assert entry(result.stdout, window, "jobs added") == "jobs added 0"


def test_stats_counts_are_the_rows_that_are_there(runner: CliRunner) -> None:
    """Every count gets a *different* number of rows, so a sub-select labelled with the wrong
    name shows up as a wrong number instead of passing by coincidence."""

    async def arrange(session: AsyncSession) -> None:
        now = datetime.now(UTC)
        alive = await make_company(session, "Alive", domain="alive.com", first_seen_at=now)
        dead = await make_company(
            session, "Dead", domain="dead.com", status=enums.CompanyStatus.DEAD, first_seen_at=now
        )
        # Carries nothing but its status, so it moves exactly one number: the middle term of the
        # companies split. ``acquired`` is printed as ``total - active - dead``, which is right
        # only if every other status is impossible; a fixture without one would let the whole
        # term be dropped, or be computed from the wrong pair of counts, and still pass.
        await make_company(
            session,
            "Bought",
            domain="bought.com",
            status=enums.CompanyStatus.ACQUIRED,
            first_seen_at=now,
        )
        job = await make_job(session, alive, "Engineer", first_seen_at=now)
        await make_job(session, alive, "Designer", first_seen_at=now)
        await make_job(session, dead, "Ghost", first_seen_at=now, closed_at=now)
        await make_round(session, alive, investors=(("Sequoia", True),))
        await make_contact(session, alive, value="https://alive.com/careers")
        await make_contact(
            session,
            alive,
            kind=enums.ContactKind.LINKEDIN_COMPANY,
            value="https://linkedin.com/company/alive",
            confidence=enums.ContactConfidence.CONSTRUCTED,
        )
        await make_contact(
            session,
            dead,
            kind=enums.ContactKind.LINKEDIN_PEOPLE,
            value="https://linkedin.com/search/dead",
            confidence=enums.ContactConfidence.CONSTRUCTED,
        )
        await make_location(session, alive, "San Francisco", is_hq=True)
        await make_sector(session, alive, "Fintech")
        await make_sector(session, dead, "Robotics")
        await make_note(session, alive, note="worth a look")
        await make_bookmark(session, job)
        await make_run(session, "greenhouse", started_at=now, finished_at=now)
        session.add(
            MergeCandidate(
                company_id_a=alive.id, company_id_b=dead.id, similarity=0.9, reason="trigram"
            )
        )

    db(arrange)

    out = invoke(runner, "stats").stdout

    assert entry(out, "rows", "companies") == "companies 3 (1 active, 1 acquired, 1 dead)"
    assert entry(out, "rows", "jobs") == "jobs 2 open, 1 closed"
    assert entry(out, "rows", "funding rounds") == "funding rounds 1"
    assert entry(out, "rows", "investors") == "investors 1"
    assert entry(out, "rows", "contacts") == "contacts 1 published, 2 constructed"
    assert entry(out, "rows", "locations") == "locations 1"
    assert entry(out, "rows", "sectors") == "sectors 2"
    assert entry(out, "rows", "merge candidates") == "merge candidates 1 unresolved, 0 resolved"
    assert entry(out, "rows", "fetch runs") == "fetch runs 1"
    assert entry(out, "rows", "notes / bookmarks") == "notes / bookmarks 1 / 1"


def test_stats_never_prints_the_database_password(runner: CliRunner) -> None:
    """``DATABASE_URL`` carries credentials. The report names the database it read, which is
    the useful half, and drops everything a screenshot could leak."""
    result = invoke(runner, "stats")

    assert SECRET not in result.output
    assert "://" not in result.output
    url = make_url(get_settings().database_url)
    assert result.stdout.splitlines()[0] == f"database  {url.database} on {url.host}:{url.port}"


def test_the_database_label_keeps_only_the_three_harmless_parts() -> None:
    """A unit check on the redaction itself, including the socket case: there is no port to
    print, and inventing 5432 would make the line untrue."""
    assert (
        cli.database_label("postgresql+asyncpg://me:secret@db.internal:6543/tracker")
        == "tracker on db.internal:6543"
    )
    assert cli.database_label("postgresql+asyncpg://me@/tracker") == "tracker on localhost"


def test_stats_counts_only_the_last_seven_days_as_added(runner: CliRunner) -> None:
    """SPEC §12 Phase 6: "companies and jobs added in the last 7 days".

    Rows are placed a minute either side of ``now - 7d``. The microsecond-exact edge of the
    half-open ``first_seen_at >= now - 7d`` window is pinned in ``tests/test_merge.py`` against
    an injected clock; what is checked here is the half a CLI test can check — that the command
    passes a live clock through, so the window moves with the day rather than being frozen at
    import time or ignored altogether.
    """

    async def arrange(session: AsyncSession) -> None:
        edge = datetime.now(UTC) - timedelta(days=STATS_WINDOW_DAYS)
        inside = await make_company(
            session, "Inside", domain="in.com", first_seen_at=edge + timedelta(minutes=1)
        )
        outside = await make_company(
            session, "Outside", domain="out.com", first_seen_at=edge - timedelta(minutes=1)
        )
        await make_job(session, inside, "Fresh", first_seen_at=edge + timedelta(minutes=1))
        await make_job(session, outside, "Ancient", first_seen_at=edge - timedelta(minutes=1))

    db(arrange)

    out = invoke(runner, "stats").stdout

    window = f"last {STATS_WINDOW_DAYS} days"
    assert entry(out, window, "companies added") == "companies added 1"
    assert entry(out, window, "jobs added") == "jobs added 1"
    # Both rows are in the database; only the window excludes one of each.
    assert entry(out, "rows", "companies").startswith("companies 2")
    assert entry(out, "rows", "jobs").startswith("jobs 2 open")


def test_stats_lists_every_configured_connector_in_config_order(runner: CliRunner) -> None:
    """Driven by ``config/connectors.yaml``, exactly as ``/runs`` is: a connector that has never
    succeeded has no row to join against, and that is the case worth seeing."""
    listed = [
        line.split()[0] for line in sections(invoke(runner, "stats").stdout)["last successful run"]
    ]

    assert listed == list(load_connectors_config().connectors)


def test_an_unimplemented_connector_is_marked_rather_than_left_reading_never(
    runner: CliRunner,
) -> None:
    """``product_hunt`` and ``opencorporates`` are configured with no code behind them yet
    (SPEC §4 Tier 2 #6, #7). A bare ``never`` beside them is true and reads as breakage, which
    is the opposite of what this section is for, so they carry the marker ``/runs`` gives them.

    An implemented connector that has never run must *not* carry it: that one is worth chasing.
    """
    out = invoke(runner, "stats").stdout

    for name in UNIMPLEMENTED_CONNECTORS:
        assert entry(out, "last successful run", name) == f"{name} never (not implemented)"
    assert entry(out, "last successful run", "greenhouse") == "greenhouse never"


def test_stats_reports_the_last_ok_and_its_age(runner: CliRunner) -> None:
    """``ok`` only — a ``partial`` run is not what SPEC §9 calls successful, so a connector
    that has only ever gone partial still reads ``never``."""

    async def arrange(session: AsyncSession) -> datetime:
        at = datetime.now(UTC) - timedelta(days=2)
        await make_run(session, "greenhouse", started_at=at, finished_at=at)
        await make_run(
            session, "lever", started_at=at, finished_at=at, status=enums.FetchRunStatus.PARTIAL
        )
        return at

    at = db(arrange)

    out = invoke(runner, "stats").stdout

    assert entry(out, "last successful run", "greenhouse") == (
        f"greenhouse {at.astimezone(UTC):%Y-%m-%d %H:%M} UTC (2d ago)"
    )
    assert entry(out, "last successful run", "lever") == "lever never"


def test_stats_marks_a_live_failure_streak_at_the_threshold(runner: CliRunner) -> None:
    """SPEC §7.2's "3 consecutive runs", through the shared constant — two failures is a bad
    night, three is what the scheduler logs at ERROR and ``/runs`` calls Failing."""

    async def arrange(session: AsyncSession) -> None:
        now = datetime.now(UTC)
        for connector, count in (
            ("greenhouse", CONSECUTIVE_FAILURE_ALERT),
            ("lever", CONSECUTIVE_FAILURE_ALERT - 1),
        ):
            for index in range(count):
                at = now - timedelta(hours=index)
                await make_run(
                    session,
                    connector,
                    started_at=at,
                    finished_at=at,
                    status=enums.FetchRunStatus.ERROR,
                )

    db(arrange)

    out = invoke(runner, "stats").stdout

    assert entry(out, "last successful run", "greenhouse") == (
        f"greenhouse never ({CONSECUTIVE_FAILURE_ALERT} consecutive failures)"
    )
    assert entry(out, "last successful run", "lever") == "lever never"


def test_stats_shows_a_stale_success_and_a_streak_together(runner: CliRunner) -> None:
    """The asymmetry SPEC §7.2 and §9 create, spelled out on one line.

    ``last_successful_runs`` counts ``ok`` alone; a failure streak is broken by ``partial`` as
    well. A connector that succeeded nine days ago and has failed every run since is both
    "9d ago" and "N consecutive failures", and printing one without the other misinforms.
    """

    async def arrange(session: AsyncSession) -> None:
        now = datetime.now(UTC)
        success = now - timedelta(days=9)
        await make_run(session, "greenhouse", started_at=success, finished_at=success)
        for index in range(CONSECUTIVE_FAILURE_ALERT):
            at = now - timedelta(hours=index)
            await make_run(
                session,
                "greenhouse",
                started_at=at,
                finished_at=at,
                status=enums.FetchRunStatus.ERROR,
            )

    db(arrange)

    line = entry(invoke(runner, "stats").stdout, "last successful run", "greenhouse")

    assert line.endswith(f"(9d ago, {CONSECUTIVE_FAILURE_ALERT} consecutive failures)")


def test_stats_exits_1_with_one_line_when_the_database_is_unreachable(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A read-only report has no partial answer worth printing, so it fails whole — one line on
    stderr in the register of the invalid-settings handler, and exit 1 for a shell script."""
    monkeypatch.setenv(
        "DATABASE_URL", str(make_url(os.environ["DATABASE_URL"]).set(host="127.0.0.1", port=1))
    )
    get_settings.cache_clear()

    result = invoke(runner, "stats")

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr.startswith("error: could not read the database")
    assert SECRET not in result.stderr


# ======================================================================= merge-review


async def add_pair(
    session: AsyncSession,
    *,
    a_name: str = "Acme Robotics",
    b_name: str = "Acme Robotics Inc",
    a_domain: str | None = "acme.com",
    b_domain: str | None = None,
    similarity: float = 0.91,
) -> tuple[Company, Company]:
    """Two companies and the unresolved candidate naming them, stored ``a.id < b.id`` as the
    pipeline stores it (SPEC §8 step 3)."""
    a = await make_company(session, a_name, domain=a_domain)
    b = await make_company(session, b_name, domain=b_domain)
    session.add(
        MergeCandidate(
            company_id_a=min(a.id, b.id),
            company_id_b=max(a.id, b.id),
            similarity=similarity,
            reason=f"trigram similarity {similarity}",
        )
    )
    await session.flush()
    return a, b


def test_merge_review_on_an_empty_queue_says_so_and_exits_0(runner: CliRunner) -> None:
    result = invoke(runner, "merge-review")

    assert result.exit_code == 0
    assert "no unresolved merge candidates" in result.stdout
    assert summary_line(result.stdout) == (
        "merge-review  merged=0  not-duplicate=0  skipped=0  remaining=0"
    )


def test_list_prints_both_sides_and_never_prompts(runner: CliRunner) -> None:
    """``--list`` is the non-interactive form: it must not read stdin at all, which is what
    makes it safe in a cron job and usable as ``merge-review --list | less``."""

    async def arrange(session: AsyncSession) -> tuple[int, int]:
        a, b = await add_pair(session)
        return a.id, b.id

    a_id, b_id = db(arrange)

    # Input is offered and must be ignored: a `--list` that consumed it would silently merge.
    result = invoke(runner, "merge-review", "--list", stdin="1\n")

    assert result.exit_code == 0
    assert "[n] not a duplicate" not in result.stdout
    for value in ("Acme Robotics", "acme.com", f"[1] #{a_id}", f"[2] #{b_id}"):
        assert value in result.stdout
    assert summary_line(result.stdout).endswith("remaining=1")
    assert db(lambda s: s.scalar(select(MergeCandidate.resolved_at))) is None


def test_key_1_folds_the_second_company_into_the_first(runner: CliRunner) -> None:
    """The point of the command, checked against the database rather than the report: the
    survivor keeps its id and gains the other's children, and the duplicate row is gone."""

    async def arrange(session: AsyncSession) -> tuple[int, int]:
        a, b = await add_pair(session)
        await make_job(session, b, "Engineer", external_id="b-1")
        await make_contact(session, b, value="https://acme.com/jobs")
        await make_company_source(session, b, "greenhouse")
        return a.id, b.id

    a_id, b_id = db(arrange)

    result = invoke(runner, "merge-review", stdin="1\n")

    assert result.exit_code == 0
    assert f"merged #{b_id} into #{a_id}" in result.stdout

    async def read(session: AsyncSession) -> tuple[object, object, object]:
        return (
            await session.get(Company, b_id),
            await session.scalar(select(Job.company_id).where(Job.external_id == "b-1")),
            await session.scalar(select(MergeCandidate.id)),
        )

    dropped, job_owner, candidate = db(read)
    assert dropped is None
    assert job_owner == a_id
    # The pair under review named the deleted company, so the cascade took it too — which is
    # why a merged pair leaves no resolved row while "not a duplicate" does.
    assert candidate is None
    assert summary_line(result.stdout) == (
        "merge-review  merged=1  not-duplicate=0  skipped=0  remaining=0"
    )


def test_key_2_folds_the_first_company_into_the_second(runner: CliRunner) -> None:
    """``[2]`` is not a mirror typo of ``[1]``: it has to pick the *other* survivor."""

    async def arrange(session: AsyncSession) -> tuple[int, int]:
        a, b = await add_pair(session)
        await make_job(session, a, "Engineer", external_id="a-1")
        return a.id, b.id

    a_id, b_id = db(arrange)

    result = invoke(runner, "merge-review", stdin="2\n")

    assert f"merged #{a_id} into #{b_id}" in result.stdout

    async def read(session: AsyncSession) -> tuple[object, object]:
        return (
            await session.get(Company, a_id),
            await session.scalar(select(Job.company_id).where(Job.external_id == "a-1")),
        )

    dropped, job_owner = db(read)
    assert dropped is None
    assert job_owner == b_id


def test_a_merge_rebuilds_the_survivors_constructed_linkedin_links(runner: CliRunner) -> None:
    """SPEC §6's two links are derived from the survivor's ``name`` and ``domain``, and a merge
    changes both of those inputs — so without a reconciliation the merge itself breaks the rule.

    The arrangement is the ordinary one: the survivor has no domain, and the duplicate carries
    it. Afterwards the survivor holds *one* people search (its own name, not the deleted
    company's) and a ``linkedin_company`` link built from the domain it has just acquired.
    Before the reconciliation existed this same input left two people searches — one of them
    for a name no longer in the database, rendered by ``/companies/{id}`` as a real link — and
    no company link at all.
    """

    async def arrange(session: AsyncSession) -> int:
        a, b = await add_pair(session, a_domain=None, b_domain="acme.com")
        await make_contact(
            session,
            a,
            kind=enums.ContactKind.LINKEDIN_PEOPLE,
            value=people_search_url("Acme Robotics"),
            confidence=enums.ContactConfidence.CONSTRUCTED,
        )
        await make_contact(
            session,
            b,
            kind=enums.ContactKind.LINKEDIN_PEOPLE,
            value=people_search_url("Acme Robotics Inc"),
            confidence=enums.ContactConfidence.CONSTRUCTED,
        )
        return a.id

    keep_id = db(arrange)

    result = invoke(runner, "merge-review", stdin="1\n")

    assert "linkedin links" in result.stdout

    async def read(session: AsyncSession) -> list[tuple[str, str]]:
        rows = await session.execute(
            select(Contact.kind, Contact.value)
            .where(Contact.company_id == keep_id)
            .order_by(Contact.kind, Contact.value)
        )
        return [(kind.value, value) for kind, value in rows]

    assert db(read) == [
        ("linkedin_company", constructed_linkedin_company_url("acme.com")),
        ("linkedin_people", people_search_url("Acme Robotics")),
    ]


def test_key_n_resolves_the_pair_without_changing_any_data(runner: CliRunner) -> None:
    """ "Not a duplicate" has to *stick* — a reviewed pair offered again next week is how a
    queue teaches its reviewer to ignore it (SPEC §8)."""

    db(add_pair)

    first = invoke(runner, "merge-review", stdin="n\n")

    assert "not a duplicate" in first.stdout

    async def read(session: AsyncSession) -> tuple[int, object]:
        companies = await session.scalar(select(func.count()).select_from(Company))
        return companies or 0, await session.scalar(select(MergeCandidate.resolved_at))

    companies, resolved = db(read)
    assert companies == 2
    assert resolved is not None
    assert summary_line(first.stdout) == (
        "merge-review  merged=0  not-duplicate=1  skipped=0  remaining=0"
    )
    # And it is never offered again.
    assert "no unresolved merge candidates" in invoke(runner, "merge-review").stdout


def test_key_s_leaves_the_pair_exactly_where_it_was(runner: CliRunner) -> None:
    db(add_pair)

    result = invoke(runner, "merge-review", stdin="s\n")

    assert db(lambda s: s.scalar(select(MergeCandidate.resolved_at))) is None
    assert summary_line(result.stdout) == (
        "merge-review  merged=0  not-duplicate=0  skipped=1  remaining=1"
    )


def test_q_stops_the_review_and_keeps_the_decisions_already_made(runner: CliRunner) -> None:
    """Each decision commits on its own, so quitting is not a rollback. Three pairs, one merge,
    then quit: the merge stands and the two untouched pairs are still queued."""

    async def arrange(session: AsyncSession) -> tuple[int, int]:
        a, b = await add_pair(session, a_domain="one.com", similarity=0.99)
        await add_pair(session, a_name="Beta", b_name="Beta Labs", a_domain="b.com", similarity=0.9)
        await add_pair(
            session, a_name="Gamma", b_name="Gamma Inc", a_domain="g.com", similarity=0.88
        )
        return a.id, b.id

    a_id, b_id = db(arrange)

    result = invoke(runner, "merge-review", stdin="1\nq\n")

    async def read(session: AsyncSession) -> tuple[object, object]:
        return await session.get(Company, a_id), await session.get(Company, b_id)

    survivor, dropped = db(read)
    assert survivor is not None
    assert dropped is None
    assert summary_line(result.stdout) == (
        "merge-review  merged=1  not-duplicate=0  skipped=0  remaining=2"
    )


def test_end_of_input_ends_the_review_cleanly(runner: CliRunner) -> None:
    """Piping answers is how this is scripted and how it runs under ``docker compose run`` with
    no TTY, so running out of input means "the reviewer is done", not a traceback."""

    async def arrange(session: AsyncSession) -> int:
        _, b = await add_pair(session)
        await add_pair(session, a_name="Beta", b_name="Beta Labs", a_domain="b.com", similarity=0.9)
        return b.id

    b_id = db(arrange)

    result = invoke(runner, "merge-review", stdin="")

    assert result.exit_code == 0
    assert summary_line(result.stdout) == (
        "merge-review  merged=0  not-duplicate=0  skipped=0  remaining=2"
    )
    assert db(lambda s: s.get(Company, b_id)) is not None


def test_an_unrecognised_answer_re_prompts_the_same_pair(runner: CliRunner) -> None:
    """A fat-fingered key must not advance past a pair, and must not decide anything."""

    async def arrange(session: AsyncSession) -> tuple[int, int]:
        a, b = await add_pair(session)
        return a.id, b.id

    a_id, b_id = db(arrange)

    result = invoke(runner, "merge-review", stdin="y\n\n1\n")

    assert result.stdout.count("unrecognised answer") == 2
    assert f"merged #{b_id} into #{a_id}" in result.stdout
    # Two rejected keys, then the accepted one: the prompt was shown three times for one pair.
    assert result.stdout.count("[n] not a duplicate") == 3


def test_the_suggestion_marks_the_side_with_a_domain(runner: CliRunner) -> None:
    """A hint, never a default: SPEC §8's "do not auto-merge" is about who decides, so the
    suggested side is printed and nothing else changes."""

    async def arrange(session: AsyncSession) -> tuple[int, int]:
        a, b = await add_pair(session, a_domain=None, b_domain="acme.com")
        return a.id, b.id

    a_id, b_id = db(arrange)

    listed = invoke(runner, "merge-review", "--list").stdout

    assert f"[2] #{b_id}  (suggested)" in listed
    assert f"[1] #{a_id}  (suggested)" not in listed


def test_the_suggestion_rule_in_isolation() -> None:
    """The three tiers, without a database: a domain beats no domain, then more open jobs, then
    the lower id — the older observation."""

    def side(company_id: int, domain: str | None, jobs: int) -> CompanySummary:
        return CompanySummary(
            id=company_id,
            name="Acme",
            domain=domain,
            city=None,
            stage=enums.Stage.UNKNOWN,
            status=enums.CompanyStatus.ACTIVE,
            open_job_count=jobs,
            funding_round_count=0,
            contact_count=0,
            first_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
            last_seen_at=datetime(2026, 1, 1, tzinfo=UTC),
            connectors=(),
        )

    def row(a: CompanySummary, b: CompanySummary) -> MergeCandidateRow:
        return MergeCandidateRow(id=1, similarity=0.9, reason="trigram", a=a, b=b)

    # A domain outranks even a much larger job count.
    assert cli.suggest_survivor(row(side(1, None, 9), side(2, "acme.com", 0))) == 2
    assert cli.suggest_survivor(row(side(1, "a.com", 2), side(2, "b.com", 7))) == 2
    assert cli.suggest_survivor(row(side(1, "a.com", 4), side(2, "b.com", 4))) == 1
    assert cli.suggest_survivor(row(side(1, None, 0), side(2, None, 0))) == 1


def test_a_discarded_note_is_printed_in_full(runner: CliRunner) -> None:
    """The user's own writing is the one thing in the database no connector can produce again.
    When both companies carry a note the survivor's stands and the other's text is printed,
    rather than going quietly with the row."""

    async def arrange(session: AsyncSession) -> int:
        a, b = await add_pair(session)
        await make_note(session, a, note="the survivor's own note")
        await make_note(session, b, note="spoke to their recruiter\nfollow up in March")
        return a.id

    a_id = db(arrange)

    out = invoke(runner, "merge-review", stdin="1\n").stdout

    assert "spoke to their recruiter" in out
    assert "follow up in March" in out
    kept = db(lambda s: s.scalar(select(UserNote.note).where(UserNote.company_id == a_id)))
    assert kept == "the survivor's own note"


def test_a_note_on_a_starred_duplicate_posting_is_printed_too(runner: CliRunner) -> None:
    """The same rule as a company note, one level down: a starred duplicate job whose twin was
    already starred cannot keep its row, so its note is printed before the cascade takes it."""

    async def arrange(session: AsyncSession) -> None:
        a, b = await add_pair(session)
        twin = await make_job(session, a, "Engineer", external_id="shared")
        duplicate = await make_job(session, b, "Engineer", external_id="shared")
        await make_bookmark(session, twin)
        session.add(JobBookmark(job_id=duplicate.id, starred=True, note="recruiter is Dana"))

    db(arrange)

    out = invoke(runner, "merge-review", stdin="1\n").stdout

    assert "recruiter is Dana" in out


def test_a_note_on_the_discarded_side_alone_is_moved_not_printed(runner: CliRunner) -> None:
    async def arrange(session: AsyncSession) -> int:
        a, b = await add_pair(session)
        await make_note(session, b, note="only copy")
        return a.id

    a_id = db(arrange)

    out = invoke(runner, "merge-review", stdin="1\n").stdout

    assert f"note           moved to #{a_id}" in out
    moved = db(lambda s: s.scalar(select(UserNote.note).where(UserNote.company_id == a_id)))
    assert moved == "only copy"


def test_limit_caps_the_pairs_offered_without_hiding_the_rest(runner: CliRunner) -> None:
    """``--limit`` is a page of the queue, not a filter on it: ``remaining`` still counts every
    unresolved pair, which is what tells the reviewer to come back."""

    async def arrange(session: AsyncSession) -> None:
        await add_pair(session, a_domain="one.com", similarity=0.99)
        await add_pair(session, a_name="Beta", b_name="Beta Ltd", a_domain="b.com", similarity=0.95)
        await add_pair(session, a_name="Gamma", b_name="Gamma Co", a_domain="g.com", similarity=0.9)

    db(arrange)

    result = invoke(runner, "merge-review", "--limit", "1", "--list")

    assert result.stdout.count("candidate #") == 1
    assert summary_line(result.stdout).endswith("remaining=3")


def test_pairs_are_offered_most_similar_first(runner: CliRunner) -> None:
    """Ordering is ``similarity DESC, id ASC`` — most confident first, then stable, so a pair
    that was skipped keeps its place while the reviewer works down the list."""

    async def arrange(session: AsyncSession) -> None:
        await add_pair(session, a_name="Low", b_name="Low Inc", a_domain="lo.com", similarity=0.86)
        await add_pair(session, a_name="High", b_name="High Inc", a_domain="hi.com", similarity=0.9)

    db(arrange)

    listed = invoke(runner, "merge-review", "--list").stdout
    names = [line.split()[1] for line in listed.splitlines() if line.startswith("  name")]

    assert names == ["High", "Low"]


def test_a_pair_taken_by_an_earlier_decision_is_skipped_not_crashed(runner: CliRunner) -> None:
    """The queue is read in one statement up front (one query, not eight per row), so a merge
    can invalidate a pair further down the same page and the review has to survive it.

    Three pairs: ``(a,b)`` at 0.99, ``(b,c)`` at 0.95, ``(a,c)`` at 0.90. Merging ``b`` into
    ``a`` tries to repoint ``(b,c)`` onto ``(a,c)``, which already exists — so it is skipped and
    then removed by the company delete's cascade. The next pair the reviewer would be offered is
    a row that no longer exists, and that has to read as one named line rather than a
    ``ValueError`` out of the middle of a sitting.

    ``(a,c)`` is then left alone for the other reason: ``a``'s row in the queue was read before
    the merge, and the merge changed it (SPEC §6's people search was built for the survivor), so
    its "contacts" column no longer describes the database. Both deferrals are announced, and
    both pairs come back — correctly rendered — on the next run.
    """

    async def arrange(session: AsyncSession) -> tuple[int, int]:
        a = await make_company(session, "Acme", domain="acme.com")
        b = await make_company(session, "Acme Inc", domain=None)
        c = await make_company(session, "Acme Robotics", domain=None)
        for left, right, similarity in ((a, b, 0.99), (b, c, 0.95), (a, c, 0.90)):
            session.add(
                MergeCandidate(
                    company_id_a=min(left.id, right.id),
                    company_id_b=max(left.id, right.id),
                    similarity=similarity,
                    reason="trigram",
                )
            )
        await session.flush()
        return a.id, c.id

    a_id, c_id = db(arrange)

    # Keep `a`, dropping `b` — which is half of the second pair too. The `s` is offered and must
    # not be consumed: neither remaining pair is put to the reviewer at all.
    result = invoke(runner, "merge-review", stdin="1\ns\n")

    assert result.exit_code == 0
    assert "is no longer in the queue" in result.stdout
    assert "is left for the next run" in result.stdout
    assert summary_line(result.stdout) == (
        "merge-review  merged=1  not-duplicate=0  skipped=0  remaining=1"
    )

    async def read(session: AsyncSession) -> MergeCandidate:
        return (await session.execute(select(MergeCandidate))).scalars().one()

    # Exactly one pair survives, and it is the (a, c) one the reviewer skipped.
    remaining = db(read)
    assert {remaining.company_id_a, remaining.company_id_b} == {a_id, c_id}
