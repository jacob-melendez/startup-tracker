"""Command-line entry point (SPEC §11 ``cli.py``; SPEC §12 Phase 2 adds ``refresh``).

Phase 2 ships two commands:

* ``migrate`` — ``alembic upgrade head`` through the Alembic API, the same thing ``make migrate``
  runs inside Compose (SPEC §3: Alembic owns all DDL).
* ``refresh`` — run one connector (``--connector NAME``) or every enabled one (``--all``) right
  now. Together with the Phase 6 scheduler this is the *only* place external data is fetched:
  request handlers never make outbound calls (SPEC §2, CLAUDE.md). Every run writes a
  ``fetch_runs`` row, success or failure (SPEC §7.2) — the one exception being a run whose row
  could not be written at all (database unreachable), which the summary line marks with
  :data:`NOT_RECORDED` so stdout never claims a row that does not exist.

``seed`` (Phase 3), ``stats`` and ``merge-review`` (Phase 6) join later.

Logging is configured once, in the Typer callback, from :class:`settings.Settings`. Log lines go
to stderr (see :mod:`logging_config`), so stdout carries only the per-connector summary lines.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from pydantic import ValidationError

from db import enums
from db.models import FetchRun
from db.session import dispose_engine, get_session_factory
from ingest.base import Connector
from ingest.config import ConnectorConfig, load_connectors_config, load_regions_config
from ingest.connectors import all_connectors
from ingest.http import FileCache, HttpClient
from ingest.pipeline import run_connector
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
def migrate() -> None:
    """Apply every Alembic migration (alembic upgrade head) to DATABASE_URL.

    The same thing `make migrate` runs inside Compose (SPEC §3: Alembic owns all DDL).
    """
    alembic_command.upgrade(AlembicConfig(str(ALEMBIC_INI)), "head")


if __name__ == "__main__":
    app()
