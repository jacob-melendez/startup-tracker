"""Command-line entry point (SPEC §11 ``cli.py``).

Six commands. SPEC §11 names five (``migrate seed refresh stats merge-review``) and all five are
registered here; ``sync-contacts`` is the one addition — local reconciliation, nothing fetched —
and CLAUDE.md records it, the way the Makefile reconciles its own target list against the same
section.

* ``migrate`` — ``alembic upgrade head`` through the Alembic API, the same thing ``make migrate``
  runs inside Compose (SPEC §3: Alembic owns all DDL).
* ``seed`` — load ``config/seed_companies.yaml`` and validate every domain (SPEC §10). The seed
  loader is a connector like any other (:mod:`ingest.seed`) but is deliberately absent from the
  registry, so ``refresh --all`` never re-validates the bootstrap list; this command builds it.
* ``refresh`` — run one connector (``--connector NAME``) or every enabled one (``--all``) right
  now. Together with the Phase 6 scheduler this is the *only* place external data is fetched:
  request handlers never make outbound calls (SPEC §2, CLAUDE.md). Every run writes a
  ``fetch_runs`` row, success or failure (SPEC §7.2) — the one exception being a run whose row
  could not be written at all (database unreachable), which the summary line marks with
  :data:`NOT_RECORDED` so stdout never claims a row that does not exist.
* ``stats`` — the state of the database in one screen: row counts, the last successful run of
  every *configured* connector with any live failure streak (SPEC §7.2), and the companies and
  jobs added in the last :data:`~db.queries.STATS_WINDOW_DAYS` days. It reads and prints;
  alone among the six it writes nothing at all.
* ``merge-review`` — work through the ``merge_candidates`` queue. SPEC §8 forbids auto-merging
  a trigram match, so every probable duplicate the pipeline finds waits here for a human to
  decide; this command is consequently the only thing in the system that deletes a company.
* ``sync-contacts`` — rebuild SPEC §6's *constructed* LinkedIn links across the whole database.
  The odd one out: it fetches nothing and writes no ``fetch_runs`` row, because it is pure local
  reconciliation. The ingest path already does this per company as it upserts one; this reaches
  the companies no connector re-lists, which is what a database carried over from an earlier
  phase is full of.

Logging is configured once, in the Typer callback, from :class:`settings.Settings`. Log lines go
to stderr (see :mod:`logging_config`), so stdout carries only the per-connector summary lines.
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from pydantic import ValidationError
from sqlalchemy import func, select, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db import enums, queries
from db.models import FetchRun, MergeCandidate
from db.session import dispose_engine, get_session_factory
from ingest.base import Connector
from ingest.config import (
    UNIMPLEMENTED_CONNECTORS,
    ConnectorConfig,
    load_connectors_config,
    load_regions_config,
)
from ingest.connectors import all_connectors
from ingest.http import FileCache, HttpClient
from ingest.pipeline import (
    CONTACT_SYNC_BATCH,
    ContactSyncResult,
    reconcile_constructed_contacts,
    run_connector,
    sync_constructed_contacts,
)
from ingest.seed import SeedConnector
from logging_config import configure_logging, get_logger
from settings import Settings, get_settings

ROOT = Path(__file__).resolve().parent
ALEMBIC_INI = ROOT / "alembic.ini"
CONTACT_EMAIL_VAR = "CONTACT_EMAIL"
# Connectors whose source requires a contact address in the User-Agent (SPEC §4: SEC's fair-access
# policy). Each of them refuses to run without one anyway (``SecEdgarConnector.fetch`` raises), so
# this list only buys a clearer message and avoids writing a ``FetchRun`` that was doomed from the
# start. Nothing here is a schedule or a keyword: those stay in ``config/`` (CLAUDE.md).
CONTACT_REQUIRED = frozenset({"sec_edgar"})
# Token on a summary line for a run that never reached ``fetch_runs`` (``run_connector`` raised
# before writing the row — SPEC §7.2's "every run writes a row" has exactly this exception):
# whoever reconciles stdout with the table or the Phase 4 ``/runs`` page must not look for a row.
NOT_RECORDED = "fetch_run=not-recorded"
#: Two spaces under a bare-word section heading — the report layout `stats` and `merge-review`
#: share, and the same register as the one-line run summaries above.
INDENT = "  "
#: Default page of unresolved duplicates ``merge-review`` offers in one sitting.
MERGE_REVIEW_LIMIT = 20
#: What `stats` and `merge-review` print where a column has no value. A dash, not an empty
#: string, so an absent value is visible in a fixed-width column rather than looking like a
#: rendering slip.
NOTHING = "—"

_SECONDS_PER_MINUTE = 60
_SECONDS_PER_HOUR = 3600
_SECONDS_PER_DAY = 86400

log = get_logger(__name__)

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    # Plain-text help and usage errors (no rich panels): paragraphs re-wrap to the terminal and
    # the output stays greppable; tracebacks are left to Python.
    rich_markup_mode=None,
    pretty_exceptions_enable=False,
)

# A connector instance paired with the ``config/connectors.yaml`` block it was built from — the
# block also carries the rate limit and ``respect_robots`` flag the ``HttpClient`` needs.
Target = tuple[Connector[Any], ConnectorConfig]


@app.callback()
def main() -> None:
    """Bay Area startup tracker — migrations and on-demand ingestion (docs/SPEC.md §11)."""
    try:
        settings = get_settings()
    except ValidationError as exc:
        # A misconfigured environment or .env (CONTACT_EMAIL=dev@localhost, a non-asyncpg
        # DATABASE_URL) is a one-line error for every subcommand, not a pydantic traceback.
        typer.echo(
            f"error: invalid settings (environment or .env): {format_settings_error(exc)}",
            err=True,
        )
        raise typer.Exit(code=1) from None
    configure_logging(settings.log_level, settings.log_json)


def format_settings_error(exc: ValidationError) -> str:
    """Every invalid setting on one line, each named by its environment variable (``.env``
    uses the same names) unless the message already names it; pydantic's ``Value error,``
    prefix is dropped."""
    problems: list[str] = []
    for error in exc.errors():
        variable = "_".join(str(part) for part in error["loc"]).upper()
        message = error["msg"].removeprefix("Value error, ")
        problems.append(message if variable in message else f"{variable}: {message}")
    return "; ".join(problems)


# ------------------------------------------------------------------ refresh helpers


def parse_since(value: str | None) -> datetime | None:
    """``--since YYYY-MM-DD`` → an aware UTC-midnight datetime.

    SPEC §14.3: the CLI's explicit lower bound overrides a connector's incremental window and
    its first-run backfill; ``FetchContext.since`` carries it to the connector.
    """
    if value is None:
        return None
    try:
        day = date.fromisoformat(value)
    except ValueError:
        msg = f"expected a date as YYYY-MM-DD, got {value!r}"
        raise typer.BadParameter(msg, param_hint="--since") from None
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


def select_connectors(ctx: typer.Context, name: str | None, run_all: bool) -> list[str]:
    """The connector names to run: exactly one of ``--connector``/``--all`` (usage error
    otherwise; an unknown name lists the known ones).

    ``--all`` is every registered connector whose ``config/connectors.yaml`` block is
    ``enabled``, in registry order. A *named* connector runs even when disabled — naming it is
    the on-demand case of SPEC §2, and ``enabled`` is the scheduler's switch (SPEC §7.2).
    """
    registry = all_connectors()
    if (name is not None) == run_all:
        ctx.fail("exactly one of --connector NAME or --all is required")
    if name is not None:
        if name not in registry:
            known = ", ".join(sorted(registry))
            ctx.fail(f"unknown connector {name!r}; known connectors: {known}")
        return [name]
    return [connector for connector in registry if connector_config(ctx, connector).enabled]


def connector_config(ctx: typer.Context, name: str) -> ConnectorConfig:
    """The connector's ``config/connectors.yaml`` block (SPEC §7.2: the only place a cadence
    or rate limit is defined); a registered connector without a block is a usage error."""
    try:
        return load_connectors_config().get(name)
    except KeyError as exc:
        ctx.fail(str(exc.args[0]))


def require_contact_email(settings: Settings, names: Iterable[str]) -> None:
    """Fail fast (exit 1) before any run starts when a selected connector needs a contact email
    and ``CONTACT_EMAIL`` is unset (SPEC §4: SEC's fair-access policy requires one)."""
    if settings.contact_email is not None:
        return
    needing = sorted(name for name in names if name in CONTACT_REQUIRED)
    if not needing:
        return
    typer.echo(
        f"error: {', '.join(needing)} needs a contact email in the User-Agent (SEC fair-access "
        f"policy, SPEC §4): set {CONTACT_EMAIL_VAR}=you@example.com in the environment or .env",
        err=True,
    )
    raise typer.Exit(code=1)


def build_connectors(ctx: typer.Context, names: Sequence[str]) -> list[Target]:
    """Instantiate every selected connector up front, so a bad ``options`` block fails before
    any ``FetchRun`` is written. ``config/regions.yaml`` is the only Bay-Area-specific input."""
    registry = all_connectors()
    regions = load_regions_config()
    targets: list[Target] = []
    for name in names:
        config = connector_config(ctx, name)
        try:
            connector = registry[name](config, regions)
        except ValueError as exc:  # pydantic's ValidationError is a ValueError
            ctx.fail(f"invalid configuration for connector {name!r}: {exc}")
        targets.append((connector, config))
    return targets


def describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def format_run_summary(run: FetchRun, duration_seconds: float) -> str:
    """The one stdout line per run: connector, status, counts, duration, and ``error_text``
    (collapsed to one line) when the run recorded any (SPEC §7.2)."""
    status = enums.FetchRunStatus(run.status).value
    parts = [
        run.connector,
        f"status={status}",
        f"fetched={run.n_fetched}",
        f"upserted={run.n_upserted}",
        f"duration={duration_seconds:.1f}s",
    ]
    if run.error_text:
        parts.append(f"error={' | '.join(run.error_text.splitlines())!r}")
    return "  ".join(parts)


async def run_one(
    connector: Connector[Any],
    config: ConnectorConfig,
    *,
    settings: Settings,
    since: datetime | None,
) -> FetchRun:
    """One connector run behind a fresh ``HttpClient`` built from settings and the connector's
    config block: the settings User-Agent (``startup-tracker/0.1 <email>``, SPEC §4), the
    connector's rate limit and robots policy, and the 24 h ETag/Last-Modified file cache."""
    async with HttpClient(
        user_agent=settings.user_agent,
        rate_limit=config.rate_limit,
        respect_robots=config.respect_robots,
        cache=FileCache(settings.http_cache_dir),
        timeout=settings.http_timeout_seconds,
    ) as http:
        return await run_connector(
            connector, session_factory=get_session_factory(), http=http, since=since
        )


async def refresh_all(
    targets: Sequence[Target], *, settings: Settings, since: datetime | None
) -> list[enums.FetchRunStatus]:
    """Run the targets one after another, print a summary line each, return their statuses.

    ``run_connector`` never raises for connector, record or refresh failures — those end up in
    the ``FetchRun`` — so an exception here means the run row itself could not be written
    (database down); it is reported as ``error`` with :data:`NOT_RECORDED` on the line (there
    is no row to reconcile against) plus a note on stderr, and the next connector still runs.
    The engine is disposed in a ``finally``: ``asyncio.run`` gives every invocation a fresh
    event loop, and an asyncpg pool must be closed on the loop that created it.
    """
    statuses: list[enums.FetchRunStatus] = []
    try:
        for connector, config in targets:
            started = time.perf_counter()
            try:
                run = await run_one(connector, config, settings=settings, since=since)
            except Exception as exc:
                elapsed = time.perf_counter() - started
                log.exception("refresh failed", connector=connector.name)
                typer.echo(
                    f"{connector.name}  status=error  {NOT_RECORDED}  "
                    f"duration={elapsed:.1f}s  error={describe(exc)!r}"
                )
                typer.echo(
                    f"error: {connector.name}: the fetch_runs row could not be written (is "
                    "DATABASE_URL reachable?), so this failure is not recorded in fetch_runs",
                    err=True,
                )
                statuses.append(enums.FetchRunStatus.ERROR)
                continue
            elapsed = time.perf_counter() - started
            status = enums.FetchRunStatus(run.status)
            typer.echo(format_run_summary(run, elapsed))
            if status is enums.FetchRunStatus.PARTIAL:
                typer.echo(
                    f"warning: {connector.name} finished partial — some items failed; see the "
                    "error text above (also recorded in fetch_runs)",
                    err=True,
                )
            statuses.append(status)
    finally:
        await dispose_engine()
    return statuses


# ------------------------------------------------------------------ commands


@app.command()
def refresh(
    ctx: typer.Context,
    connector: Annotated[
        str | None,
        typer.Option(
            "--connector",
            "-c",
            metavar="NAME",
            help="Run this one connector (unknown names list the known ones).",
        ),
    ] = None,
    all_: Annotated[
        bool,
        typer.Option(
            "--all",
            help="Run every registered connector that is enabled in config/connectors.yaml.",
        ),
    ] = False,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            metavar="YYYY-MM-DD",
            help=(
                "Lower bound of the fetch window (UTC midnight). Overrides the connector's "
                "incremental window and its first-run backfill (SPEC §14.3)."
            ),
        ),
    ] = None,
    now: Annotated[
        bool,
        typer.Option(
            "--now",
            help=(
                "Run right now, ignoring the connector's cadence (SPEC §2). Every CLI invocation "
                "already does exactly this, so the flag changes nothing; there is no debounce — "
                "each invocation runs and writes a fetch_runs row (SPEC §7.2)."
            ),
        ),
    ] = False,
) -> None:
    """Fetch from one connector (--connector NAME) or every enabled one (--all).

    Each connector's records are upserted into the database and its run is recorded in
    fetch_runs (SPEC §7.2); a run whose row could not be written at all is marked
    fetch_run=not-recorded. Prints one summary line per connector to stdout. Exit status: 0 when
    every run ended ok or partial (partial prints a warning), 1 when any run ended error or was
    not recorded, when CONTACT_EMAIL is missing for a connector that needs it, or when the
    settings are invalid; 2 for a usage error.
    """
    settings = get_settings()
    names = select_connectors(ctx, connector, all_)
    since_at = parse_since(since)
    log.info("refresh", connectors=names, since=since_at, now=now)
    if not names:
        # Only --all can select nothing (a named connector always runs). Nothing failed, so
        # exit 0 — but say so rather than end silently with no summary line.
        typer.echo(
            "warning: --all selected nothing to run: no registered connector is enabled in "
            "config/connectors.yaml",
            err=True,
        )
        return
    require_contact_email(settings, names)
    targets = build_connectors(ctx, names)
    statuses = asyncio.run(refresh_all(targets, settings=settings, since=since_at))
    if enums.FetchRunStatus.ERROR in statuses:
        raise typer.Exit(code=1)


@app.command()
def seed(
    ctx: typer.Context,
    skip_validation: Annotated[
        bool,
        typer.Option(
            "--skip-validation",
            help=(
                "Do not resolve each domain over the network. SPEC §10 asks for the HEAD "
                "check; skip it only for an offline first run."
            ),
        ),
    ] = False,
) -> None:
    """Load config/seed_companies.yaml into the database, validating every domain (SPEC §10).

    Each entry's domain is resolved with a HEAD request that follows redirects; an entry whose
    host cannot be reached at all, or whose site answers 404/410, is stored with status='dead'
    and logged rather than failing the run. ATS board tokens are never seeded — the Greenhouse,
    Lever, Ashby and Workable connectors discover them (SPEC §4, §10).

    The load is a normal connector run, so it writes a fetch_runs row (SPEC §7.2) and prints
    the same summary line as `refresh`. Re-running is safe: entries are upserted on the
    normalized domain, and `seed` ranks below every real connector, so nothing it writes can
    overwrite a value a real source has since reported (SPEC §8). Exit status matches
    `refresh`.
    """
    settings = get_settings()
    config = connector_config(ctx, SeedConnector.name)
    if skip_validation:
        config = config.model_copy(
            update={"options": {**config.options, "validate_domains": False}}
        )
    try:
        connector = SeedConnector(config, load_regions_config())
    except (ValueError, OSError) as exc:  # a bad options block or an unreadable YAML file
        ctx.fail(f"invalid seed configuration: {exc}")
    log.info("seed", validate_domains=connector.options.validate_domains)
    statuses = asyncio.run(refresh_all([(connector, config)], settings=settings, since=None))
    if enums.FetchRunStatus.ERROR in statuses:
        raise typer.Exit(code=1)


@app.command()
def migrate() -> None:
    """Apply every Alembic migration (alembic upgrade head) to DATABASE_URL.

    The same thing `make migrate` runs inside Compose (SPEC §3: Alembic owns all DDL).
    """
    alembic_command.upgrade(AlembicConfig(str(ALEMBIC_INI)), "head")


# ------------------------------------------------------------------ stats (SPEC §12 Phase 6)


def database_label(database_url: str) -> str:
    """``"tracker on 127.0.0.1:5432"`` — the database being reported on, without its password.

    A ``DATABASE_URL`` carries credentials, so it is never printed. ``make_url`` parses it into
    parts and only the three harmless ones are used; a URL with no explicit port renders as the
    host alone rather than inventing 5432, because guessing would make the line a lie on a
    socket connection.
    """
    url = make_url(database_url)
    where = url.host or "localhost"
    if url.port is not None:
        where = f"{where}:{url.port}"
    return f"{url.database} on {where}"


def format_timestamp(moment: datetime) -> str:
    """``2026-09-04 05:12 UTC``. Every stored timestamp is ``timestamptz``; the report converts
    to UTC and says so, so two lines from two machines can be compared."""
    return f"{moment.astimezone(UTC):%Y-%m-%d %H:%M} UTC"


def format_age(moment: datetime, *, now: datetime) -> str:
    """A coarse relative age (``"5h ago"``, ``"12d ago"``) beside the absolute timestamp.

    ``now`` is injected rather than read from the clock so the report is reproducible in a test.
    Unlike the web's ``ago`` filter this never falls back to a date past 90 days: the absolute
    stamp is already in the column to its left, and "412d ago" is precisely the number an
    operator looking at a dead connector wants.
    """
    seconds = (now - moment).total_seconds()
    if seconds < _SECONDS_PER_MINUTE:
        return "just now"
    if seconds < _SECONDS_PER_HOUR:
        return f"{int(seconds // _SECONDS_PER_MINUTE)}m ago"
    if seconds < _SECONDS_PER_DAY:
        return f"{int(seconds // _SECONDS_PER_HOUR)}h ago"
    return f"{int(seconds // _SECONDS_PER_DAY)}d ago"


def _count_lines(rows: Sequence[tuple[str, int, str]]) -> list[str]:
    """``(label, count, suffix)`` triples as an aligned block: labels padded to the widest,
    counts right-aligned to the widest, so the numbers form a column that can be read down."""
    label_width = max(len(label) for label, _, _ in rows)
    count_width = max(len(str(count)) for _, count, _ in rows)
    return [
        f"{INDENT}{label.ljust(label_width)}  {str(count).rjust(count_width)}{suffix}"
        for label, count, suffix in rows
    ]


def format_stats(
    snapshot: queries.StatsSnapshot,
    *,
    database: str,
    connectors: Sequence[str],
    last_ok: Mapping[str, datetime],
    failures: Mapping[str, int],
    now: datetime,
) -> list[str]:
    """The whole report, as lines. Pure: every input is passed in, so a test can pin the clock.

    Three sections, the three things SPEC §12 Phase 6 asks for — row counts, the last successful
    run per connector, and the 7-day intake — under bare-word headings with their contents
    indented, which keeps the output greppable (``stats | grep 'jobs added'``) without any
    quoting.
    """
    lines = [f"database  {database}", "rows"]
    lines.extend(
        _count_lines(
            [
                (
                    "companies",
                    snapshot.companies,
                    # All three of ``CompanyStatus``, so the split sums to the total beside it
                    # the way every other split in this block does. ``acquired`` is the
                    # remainder rather than a counted column — ``StatsSnapshot`` documents it
                    # as exactly that — and on a real database it is a seventh of the rows.
                    f"  ({snapshot.companies_active} active, "
                    f"{snapshot.companies - snapshot.companies_active - snapshot.companies_dead}"
                    f" acquired, {snapshot.companies_dead} dead)",
                ),
                ("jobs", snapshot.jobs_open, f" open, {snapshot.jobs_closed} closed"),
                ("funding rounds", snapshot.funding_rounds, ""),
                ("investors", snapshot.investors, ""),
                ("people", snapshot.people, ""),
                (
                    "contacts",
                    snapshot.contacts_published,
                    f" published, {snapshot.contacts_constructed} constructed",
                ),
                ("locations", snapshot.locations, ""),
                ("sectors", snapshot.sectors, ""),
                (
                    "merge candidates",
                    snapshot.merge_candidates_open,
                    f" unresolved, {snapshot.merge_candidates_resolved} resolved",
                ),
                ("fetch runs", snapshot.fetch_runs, ""),
                ("notes / bookmarks", snapshot.user_notes, f" / {snapshot.job_bookmarks}"),
            ]
        )
    )
    lines.append("last successful run")
    lines.extend(_run_lines(connectors, last_ok=last_ok, failures=failures, now=now))
    lines.append(f"last {queries.STATS_WINDOW_DAYS} days")
    lines.extend(
        _count_lines(
            [
                ("companies added", snapshot.companies_added, ""),
                ("jobs added", snapshot.jobs_added, ""),
            ]
        )
    )
    return lines


def _run_lines(
    connectors: Sequence[str],
    *,
    last_ok: Mapping[str, datetime],
    failures: Mapping[str, int],
    now: datetime,
) -> list[str]:
    """One line per *configured* connector, in ``config/connectors.yaml`` order — the same list
    ``/runs`` shows, and for the same reason.

    A connector that has never succeeded has no row to join against, and that is exactly the
    case worth seeing, so the list is driven by the config rather than by ``fetch_runs``: it
    reads ``never``. The parenthesized note carries the age and, when the streak has reached
    :data:`~db.queries.CONSECUTIVE_FAILURE_ALERT`, SPEC §7.2's failing verdict. Both can appear
    at once: a connector that succeeded on Monday and has failed every run since is both "3d
    ago" and "4 consecutive failures", and neither half tells the story alone.

    A configured connector with no implementation yet
    (:data:`~ingest.config.UNIMPLEMENTED_CONNECTORS`) is marked as such, the way ``/runs`` marks
    it: a bare ``never`` beside ``product_hunt`` is true but reads as breakage, and the whole
    point of this section is that a source which has been silent for a month is worth chasing.
    This one is not.
    """
    if not connectors:
        return [f"{INDENT}{NOTHING} no connectors configured"]
    stamps = {
        name: format_timestamp(last_ok[name]) if name in last_ok else "never" for name in connectors
    }
    name_width = max(len(name) for name in connectors)
    stamp_width = max(len(stamp) for stamp in stamps.values())
    lines: list[str] = []
    for name in connectors:
        notes: list[str] = []
        moment = last_ok.get(name)
        if moment is not None:
            notes.append(format_age(moment, now=now))
        streak = failures.get(name, 0)
        if streak >= queries.CONSECUTIVE_FAILURE_ALERT:
            notes.append(f"{streak} consecutive failures")
        if name in UNIMPLEMENTED_CONNECTORS:
            notes.append("not implemented")
        note = f"  ({', '.join(notes)})" if notes else ""
        # ``rstrip``: a connector with no note would otherwise carry the stamp column's padding
        # to end of line, and trailing whitespace breaks a `grep -c 'never$'` and shows up as a
        # diff in anything that stores this output.
        line = f"{INDENT}{name.ljust(name_width)}  {stamps[name].ljust(stamp_width)}{note}"
        lines.append(line.rstrip())
    return lines


async def stats_once(*, now: datetime) -> list[str]:
    """Read every number in one session and format the report; dispose the engine on the way
    out, as :func:`refresh_all` does — ``asyncio.run`` gives each invocation its own loop and an
    asyncpg pool must be closed on the loop that opened it.

    The three reads share one session so the counts and the run history describe one database,
    and :func:`db.queries.stats_snapshot` is itself a single statement for the same reason.
    """
    connectors = list(load_connectors_config().connectors)
    try:
        async with get_session_factory()() as session:
            snapshot = await queries.stats_snapshot(session, now=now)
            last_ok = await queries.last_successful_runs(session)
            failures = await queries.consecutive_failure_counts(session)
    finally:
        await dispose_engine()
    return format_stats(
        snapshot,
        database=database_label(get_settings().database_url),
        connectors=connectors,
        last_ok=last_ok,
        failures=failures,
        now=now,
    )


def _unreachable(exc: Exception) -> typer.Exit:
    """One stderr line for a database that cannot be read, in the style of the settings error,
    then exit 1. A read-only report has no partial answer worth printing.

    Only the *first* line of the description survives, the way :func:`format_run_summary`
    collapses ``error_text``: SQLAlchemy's ``str()`` of a ``ProgrammingError`` embeds the whole
    failing statement and its bound parameters, so the case this message names — a reachable
    database that has never had ``cli.py migrate`` run — would otherwise print thirty lines of
    SQL. The first line still carries the exception type and the cause (``relation "companies"
    does not exist``), which is the whole of what an operator needs here.
    """
    typer.echo(
        f"error: could not read the database (is DATABASE_URL reachable, and has "
        f"`cli.py migrate` run?): {describe(exc).splitlines()[0]}",
        err=True,
    )
    return typer.Exit(code=1)


@app.command()
def stats() -> None:
    """Print the state of the local database: row counts, connector health, the last 7 days.

    Reads and prints; it is the one command that writes nothing at all, fetches nothing, and
    holds no transaction open. The connector list comes from config/connectors.yaml in config
    order — the same list /runs shows, including connectors that have never run, since a source
    that has been silent for a month is the thing worth seeing (SPEC §9).

    "last ok" counts only runs that ended `ok`, while a failure streak is broken by a `partial`
    run as well, so a connector can show a stale success and no failures at once: it is
    limping, not broken (SPEC §7.2, §9). Exit status: 0, or 1 if the database cannot be read.
    """
    now = datetime.now(UTC)
    try:
        lines = asyncio.run(stats_once(now=now))
    except (SQLAlchemyError, OSError) as exc:
        raise _unreachable(exc) from None
    for line in lines:
        typer.echo(line)


# ------------------------------------------------------------ merge-review (SPEC §8, §12)

#: The answers the review prompt accepts. Not a default among them: SPEC §8 forbids
#: auto-merging, and a keystroke that merges by accident would be exactly that.
MERGE_REVIEW_KEYS = ("1", "2", "n", "s", "q")


def suggest_survivor(row: queries.MergeCandidateRow) -> int:
    """Which side to *suggest* keeping — a hint printed beside the id, never a default.

    A domain is the canonical key (SPEC §8 step 1), so a row that has one carries more of the
    identity than one that does not; failing that the row with more open jobs is the one more of
    the app already points at; failing that the lower id, which is the older observation. The
    reviewer still has to press a key: SPEC §8's "do not auto-merge" is about who decides, and a
    suggestion that could be accepted by pressing Enter would decide for them.
    """
    a, b = row.a, row.b
    if (a.domain is None) != (b.domain is None):
        return a.id if a.domain is not None else b.id
    if a.open_job_count != b.open_job_count:
        return a.id if a.open_job_count > b.open_job_count else b.id
    return min(a.id, b.id)


def format_candidate(row: queries.MergeCandidateRow, *, suggested_id: int) -> list[str]:
    """One pair as a side-by-side table: the reviewer's whole evidence in one screen.

    Side by side rather than one company after the other, because the decision is a comparison —
    two names that differ by "Inc" and two cities that differ not at all is the shape of a
    duplicate, and it is only visible when the values sit on the same line. ``[1]``/``[2]`` head
    the columns so the prompt's keys and the columns cannot be confused, and they follow the
    stored ``company_id_a < company_id_b`` orientation, so a pair renders identically every time.
    """
    a, b = row.a, row.b
    fields: list[tuple[str, str, str]] = [
        ("name", a.name, b.name),
        ("domain", a.domain or NOTHING, b.domain or NOTHING),
        ("city", a.city or NOTHING, b.city or NOTHING),
        ("stage", a.stage.value, b.stage.value),
        ("status", a.status.value, b.status.value),
        ("open jobs", str(a.open_job_count), str(b.open_job_count)),
        ("funding rounds", str(a.funding_round_count), str(b.funding_round_count)),
        ("contacts", str(a.contact_count), str(b.contact_count)),
        ("first seen", format_timestamp(a.first_seen_at), format_timestamp(b.first_seen_at)),
        ("last seen", format_timestamp(a.last_seen_at), format_timestamp(b.last_seen_at)),
        ("seen by", ", ".join(a.connectors) or NOTHING, ", ".join(b.connectors) or NOTHING),
    ]
    heads = (
        f"[1] #{a.id}{'  (suggested)' if suggested_id == a.id else ''}",
        f"[2] #{b.id}{'  (suggested)' if suggested_id == b.id else ''}",
    )
    label_width = max(len(label) for label, _, _ in fields)
    left_width = max(len(heads[0]), *(len(left) for _, left, _ in fields))
    lines = [
        f"candidate #{row.id}  similarity {row.similarity:.2f}  {row.reason}",
        f"{INDENT}{' ' * label_width}  {heads[0].ljust(left_width)}  {heads[1]}",
    ]
    lines.extend(
        f"{INDENT}{label.ljust(label_width)}  {left.ljust(left_width)}  {right}"
        for label, left, right in fields
    )
    return lines


def format_merge_result(result: queries.MergeResult) -> list[str]:
    """What the merge actually did, in numbers the reviewer can go and check.

    The discarded note is printed **in full**: :func:`db.queries.merge_companies` hands it back
    rather than deleting it silently precisely so this line exists, and it is the user's own
    writing — the one thing in the database no source can produce again.

    A discarded ``company_sources.external_id`` is named for a different reason: it is the only
    loss here that a source can *undo*, and will. ``company_sources`` holds one row per
    ``(company, connector)``, so when both companies were known to the same connector under
    different ids the survivor's is the one that stays, and SPEC §8 step 0 resolves that
    connector's records by ``external_id`` — the id that could not be kept will therefore create
    the company again on that connector's next run. Printing the pair is what lets the reviewer
    notice in time and merge the other way round instead.
    """
    filled = ", ".join(result.fields_filled) or "none"
    lines = [
        f"merged #{result.drop_id} into #{result.keep_id}",
        f"{INDENT}fields filled  {filled}",
        f"{INDENT}moved          jobs={result.jobs_moved} bookmarks={result.bookmarks_moved} "
        f"funding_rounds={result.funding_rounds_moved} people={result.people_moved} "
        f"contacts={result.contacts_moved} locations={result.locations_moved} "
        f"sectors={result.sectors_moved} sources={result.sources_moved} "
        f"candidates_repointed={result.candidates_repointed}",
        f"{INDENT}discarded      jobs={result.jobs_discarded} contacts={result.contacts_discarded} "
        f"people_deduped={result.people_deduped}  (duplicates of rows the survivor already had)",
    ]
    if result.note_moved:
        lines.append(f"{INDENT}note           moved to #{result.keep_id}")
    if result.note_discarded is not None:
        lines.append(
            f"{INDENT}note           #{result.keep_id} already had one, so #{result.drop_id}'s is "
            "printed here in full — this is the only copy left:"
        )
        lines.extend(f"{INDENT * 2}{line}" for line in result.note_discarded.splitlines() or [""])
    if result.bookmark_notes_discarded:
        lines.append(
            f"{INDENT}bookmark note  a starred duplicate posting carried a note and the "
            "survivor's copy was already starred; the text is printed here in full:"
        )
        for note in result.bookmark_notes_discarded:
            lines.extend(f"{INDENT * 2}{line}" for line in note.splitlines())
    if result.sources_discarded:
        pairs = ", ".join(
            f"{connector}={external_id}"
            for connector, external_id in sorted(result.sources_discarded)
        )
        lines.append(
            f"{INDENT}source id      not kept — #{result.keep_id} already identifies itself to "
            f"the same connector under a different id: {pairs}"
        )
        lines.append(
            f"{INDENT * 2}a connector resolves its own records by external_id (SPEC §8 step 0), "
            f"so its next run will create #{result.drop_id}'s company again; merging the other "
            "way round would have kept it"
        )
    return lines


def read_decision(prompt: str) -> str | None:
    """Prompt, then one line from stdin; ``None`` at EOF.

    Read straight from ``sys.stdin`` rather than through ``input()`` so that end-of-input is a
    plain empty string instead of an exception: piping answers in is how this is scripted, how
    it is tested, and what ``docker compose run`` does with no TTY, and EOF there means "the
    reviewer is done", not "crash".
    """
    typer.echo(prompt)
    line = sys.stdin.readline()
    return None if line == "" else line.strip().lower()


async def _candidate_is_open(session: AsyncSession, row: queries.MergeCandidateRow) -> bool:
    """Is this candidate still there, unresolved, and still about the same two companies?

    The pairs were read in one statement up front (one query, not eight per row), so an earlier
    merge in the same sitting may since have taken this row with it — ``merge_candidates`` FKs
    cascade, and a pair naming the company just deleted goes with it.

    A pair can also be *repointed* rather than deleted: :func:`db.queries.merge_companies` moves
    a surviving pair that named the discarded company onto the survivor. Such a row is present
    and unresolved, so existence is not the whole question — the ids are compared against the
    ones in hand as well, because ``row`` still names the company that has just been deleted.
    Rendering it would offer the reviewer a company that no longer exists, ``1``/``2`` would
    fail with "no such company", and ``n`` would stamp a permanent "not a duplicate" verdict on
    a pair nobody was shown (SPEC §8: a resolved pair is never offered again). Reported closed
    instead, it keeps ``resolved_at IS NULL`` and is offered — correctly rendered — next run.

    Asked with a ``SELECT`` on the id rather than ``Session.get`` so a row deleted by a Core
    statement is reported as absent instead of raising out of the identity map.
    """
    found = (
        await session.execute(
            select(
                MergeCandidate.resolved_at,
                MergeCandidate.company_id_a,
                MergeCandidate.company_id_b,
            ).where(MergeCandidate.id == row.id)
        )
    ).first()
    if found is None or found[0] is not None:
        return False
    return (found[1], found[2]) == (row.a.id, row.b.id)


def _survivor_summary_changed(row: queries.MergeCandidateRow, result: queries.MergeResult) -> bool:
    """Did that merge change anything :func:`format_candidate` shows for the survivor?

    Every column of the side-by-side table a merge can move, checked against what
    :class:`~db.queries.MergeResult` reports: the filled fields (``domain``, ``stage``,
    ``status``), the moved children behind "open jobs", "funding rounds", "contacts", "city"
    (a location) and "seen by" (a source), and the ``first_seen_at``/``last_seen_at`` widening,
    which is visible from the snapshot itself — the merge takes the earliest and the latest of
    the two. ``name`` is ``NOT NULL`` and never changes.

    True means every pair still to come that names this survivor is holding values read before
    the merge, which is worth deferring for; false means the queue in hand still describes the
    database, which is the ordinary case of merging a duplicate that carried nothing.
    """
    keep, drop = (row.a, row.b) if result.keep_id == row.a.id else (row.b, row.a)
    moved = (
        result.jobs_moved,
        result.funding_rounds_moved,
        result.contacts_moved,
        result.locations_moved,
        result.sources_moved,
    )
    return (
        bool(result.fields_filled)
        or any(moved)
        or drop.first_seen_at < keep.first_seen_at
        or drop.last_seen_at > keep.last_seen_at
    )


async def merge_review_once(*, limit: int, list_only: bool, now: datetime) -> None:
    """Walk the unresolved duplicate queue, committing one decision at a time.

    One transaction per pair, so a reviewer who quits after three merges keeps exactly those
    three and a failure part-way through one pair rolls back only that pair
    (:func:`db.queries.merge_companies` never commits — this is the caller that does).

    A merge is two writes in that one transaction: the fold itself, then
    :func:`~ingest.pipeline.reconcile_constructed_contacts` over the survivor. The second is
    needed because the first changes both inputs of SPEC §6's derived links — it can fill the
    survivor's ``domain``, and it moves the discarded company's people search across, leaving
    the survivor with two search links of which one is for a name no longer in the database.
    Rebuilding them here is what keeps the rule true of every state this command commits.

    The queue is read in one statement before the first decision, so a merge can invalidate a
    pair further down that same page: the row may be gone or repointed
    (:func:`_candidate_is_open`), or still be exactly the pair it was while one of its companies
    has just absorbed another (:func:`_survivor_summary_changed`). Both are left for the next
    run rather than rendered from values that are no longer true — a name cluster of three
    ("Acme", "Acme Inc", "Acme Robotics") produces exactly this queue, and the side-by-side
    table is the reviewer's whole evidence for a decision that deletes a company.
    """
    try:
        async with get_session_factory()() as session:
            rows = await queries.merge_candidate_rows(session, limit=limit)
            merged = not_duplicate = skipped = 0
            # Survivors of a merge made in this sitting whose summary in ``rows`` is now stale.
            stale: set[int] = set()
            if not rows:
                typer.echo("no unresolved merge candidates")
            for row in rows:
                if not list_only and not await _candidate_is_open(session, row):
                    typer.echo(
                        f"candidate #{row.id} is no longer in the queue as read — an earlier "
                        "decision in this sitting resolved it, merged one of its companies "
                        "away, or moved it onto another company"
                    )
                    continue
                if not list_only and (row.a.id in stale or row.b.id in stale):
                    typer.echo(
                        f"candidate #{row.id} is left for the next run — a merge in this "
                        "sitting changed one of its companies after this pair was read, so "
                        "the summary here is out of date"
                    )
                    continue
                for line in format_candidate(row, suggested_id=suggest_survivor(row)):
                    typer.echo(line)
                if list_only:
                    continue
                answer = _ask(row)
                if answer is None or answer == "q":
                    # EOF and `q` are the same intent: stop here, keep what is already committed.
                    break
                if answer == "s":
                    skipped += 1
                    continue
                if answer == "n":
                    await session.execute(
                        update(MergeCandidate)
                        .where(MergeCandidate.id == row.id)
                        .values(resolved_at=now)
                    )
                    await session.commit()
                    not_duplicate += 1
                    typer.echo(f"{INDENT}recorded as not a duplicate; it will not be offered again")
                    continue
                keep_id, drop_id = (row.a.id, row.b.id) if answer == "1" else (row.b.id, row.a.id)
                try:
                    result = await queries.merge_companies(
                        session, keep_id=keep_id, drop_id=drop_id, now=now
                    )
                    # In the same transaction, under the row lock the merge already holds: a
                    # merge moves the discarded company's people-search link onto the survivor
                    # and can fill the survivor's domain, so SPEC §6's two derived links are
                    # stale the moment the merge lands. Reconciling here rather than leaving it
                    # to the next upsert keeps the rule true of every committed state.
                    links = await reconcile_constructed_contacts(session, result.keep_id)
                except (ValueError, SQLAlchemyError) as exc:
                    await session.rollback()
                    typer.echo(f"error: candidate #{row.id} not merged: {describe(exc)}", err=True)
                    continue
                await session.commit()
                merged += 1
                # The reconciliation writes contacts too, so it moves the "contacts" row of the
                # side-by-side table just as a moved contact does, and stales a later pair for
                # the same reason.
                if _survivor_summary_changed(row, result) or links.added or links.removed:
                    stale.add(result.keep_id)
                for line in format_merge_result(result):
                    typer.echo(line)
                if links.added or links.removed:
                    typer.echo(
                        f"{INDENT}linkedin links {links.added} built, {links.removed} superseded "
                        "(SPEC §6 constructed links, rebuilt from the survivor's name and domain)"
                    )
            remaining = (
                await session.execute(
                    select(func.count())
                    .select_from(MergeCandidate)
                    .where(MergeCandidate.resolved_at.is_(None))
                )
            ).scalar_one()
    finally:
        await dispose_engine()
    typer.echo(
        f"merge-review  merged={merged}  not-duplicate={not_duplicate}  skipped={skipped}  "
        f"remaining={remaining}"
    )


def _ask(row: queries.MergeCandidateRow) -> str | None:
    """Prompt until the answer is one of :data:`MERGE_REVIEW_KEYS`; ``None`` at EOF."""
    prompt = (
        f"{INDENT}[1] keep #{row.a.id}  [2] keep #{row.b.id}  [n] not a duplicate  "
        "[s] skip  [q] quit"
    )
    while True:
        answer = read_decision(prompt)
        if answer is None or answer in MERGE_REVIEW_KEYS:
            return answer
        typer.echo(
            f"{INDENT}unrecognised answer {answer!r}; expected one of "
            f"{', '.join(MERGE_REVIEW_KEYS)}"
        )


@app.command(name="merge-review")
def merge_review(
    limit: Annotated[
        int,
        typer.Option(
            "--limit",
            metavar="N",
            min=1,
            help="How many unresolved pairs to offer, most similar first.",
        ),
    ] = MERGE_REVIEW_LIMIT,
    list_only: Annotated[
        bool,
        typer.Option(
            "--list",
            help="Print the pairs and exit without prompting for any decision.",
        ),
    ] = False,
) -> None:
    """Resolve the probable-duplicate companies the pipeline refused to merge on its own.

    SPEC §8's step 3 finds companies whose names are more than 85% similar within a metro and
    writes a merge_candidates row instead of merging them: "Acme Robotics" and "Acme Robotics
    Inc" are usually one company and occasionally two, and only a person can tell. This command
    is where that judgement is made, and consequently the only thing in the system that deletes
    a company.

    Per pair: [1]/[2] fold the other company into the one you keep — every job, funding round,
    person, contact, location, sector, source, note and bookmark moves, and only rows that would
    violate one of the survivor's unique constraints are dropped. Discarded jobs and contacts are
    counted; whatever no source can write again — a note, a starred posting's note, a source's
    external id — is printed in full. [n] records "not a duplicate", which stamps the pair
    resolved so it is never offered again and changes no data. [s] leaves the pair for next
    time; [q] and end-of-input stop the review, keeping every decision already made — each one
    is committed on its own.

    The queue is read once, before the first decision, so a merge can invalidate a pair further
    down it — by taking its row away with the deleted company, by moving that row onto another
    company, or by changing something the pair's table shows. Such a pair is named on stdout
    and left alone rather than shown from values that are no longer true; whatever is still
    unresolved afterwards is offered again, against current values, on the next run. A pair
    that merely shares a company with a merge that moved nothing still describes the database,
    and is still offered here.

    Exit status: 0, or 1 if the database cannot be read.
    """
    try:
        asyncio.run(merge_review_once(limit=limit, list_only=list_only, now=datetime.now(UTC)))
    except (SQLAlchemyError, OSError) as exc:
        raise _unreachable(exc) from None


async def sync_contacts_once(*, batch_size: int, dry_run: bool) -> ContactSyncResult:
    """One sweep behind a fresh engine, disposed on the way out like `refresh_all` does."""
    try:
        return await sync_constructed_contacts(
            get_session_factory(), batch_size=batch_size, dry_run=dry_run
        )
    finally:
        await dispose_engine()


@app.command(name="sync-contacts")
def sync_contacts(
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Do the whole sweep and discard it, reporting exactly what it would change.",
        ),
    ] = False,
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size",
            metavar="N",
            min=1,
            help="Companies read per keyset page; each one is committed on its own.",
        ),
    ] = CONTACT_SYNC_BATCH,
) -> None:
    """Rebuild SPEC §6's constructed LinkedIn links for every company in the database.

    The ingest pipeline builds them whenever it upserts a company, so a database populated
    under Phase 5 already has them. This is for the rest: a database carried over from an
    earlier phase, where a company no connector re-lists — a Form D outside the incremental
    window, a seeded row (`seed` is not in `refresh --all`), a row with neither a website_url
    nor a domain for `company_site` to visit — would otherwise never gain its links at all.
    A company with no domain gets the people search only: SPEC §6 builds the company URL from
    the domain, and a slug guessed from nothing links to a stranger.

    Reads and writes only the local database; nothing is fetched, so no fetch_runs row is
    written. It touches `confidence='constructed'` contacts and nothing else: a published
    address is never altered, and re-running changes nothing (`added=0 removed=0`).
    """
    result = asyncio.run(sync_contacts_once(batch_size=batch_size, dry_run=dry_run))
    if result.dry_run:
        suffix = "  (dry run — nothing written)"
    elif not result.changed:
        # Worth saying out loud: three zeroes on a 3 000-company database look like a command
        # that did not run, and this is the answer a second invocation is supposed to give.
        suffix = "  (already in sync)"
    else:
        suffix = ""
    typer.echo(
        f"sync-contacts  companies={result.companies}  added={result.added}  "
        f"removed={result.removed}{suffix}"
    )


if __name__ == "__main__":
    app()
