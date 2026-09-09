"""``cli.py`` — ``refresh`` and ``migrate`` (SPEC §2 ``--now``, §4, §7.2, §11, §12 Phase 2).

``ingest.pipeline.run_connector`` is replaced by an async fake that records its arguments and
returns a prepared ``FetchRun``, so nothing here touches the network or fetches anything; the
database is only ever the disposable one from ``tests/conftest.py`` (``DATABASE_URL`` points at
it). ``configure_logging`` is replaced by a recorder so the process-wide logging that pytest
owns is left alone.

The last section checks the ``Makefile`` against ``cli.py``: a target's ``## `` note about a
subcommand that is still to come must name that subcommand and must go once it is registered.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import alembic.command
import pytest
import structlog
from alembic.config import Config as AlembicConfig
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from typer.core import TyperGroup
from typer.main import get_command
from typer.testing import CliRunner, Result

import cli
from db import enums
from db.models import Base, Company, Contact, FetchRun
from db.queries import COMPLETED_RUN_STATUSES
from db.session import dispose_engine
from ingest.base import CompanyRecord, Connector, FetchContext
from ingest.config import (
    ConnectorConfig,
    ConnectorsConfig,
    load_connectors_config,
    load_regions_config,
)
from ingest.connectors import all_connectors
from ingest.connectors.sec_edgar import SecEdgarConnector
from ingest.http import FileCache, HttpClient, MemoryCache
from ingest.normalize import normalize_name
from ingest.pipeline import CONTACT_SYNC_BATCH, ContactSyncResult
from ingest.seed import SeedConnector
from logging_config import get_logger
from settings import Settings, get_settings

CONTACT = "dev@example.com"
EXPECTED_UA = f"startup-tracker/0.1 {CONTACT}"
STARTED_AT = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)
README = Path(__file__).resolve().parents[1] / "README.md"


# ------------------------------------------------------------------ fixtures


@pytest.fixture(autouse=True)
def cli_env(
    monkeypatch: pytest.MonkeyPatch, migrated_database: str, tmp_path: Path
) -> Iterator[None]:
    """``DATABASE_URL`` → the disposable database, ``CONTACT_EMAIL`` unset, the HTTP cache in
    a temp dir, the settings cache cleared before and after, and the engine disposed after —
    ``cli.refresh`` disposes its own (each ``asyncio.run`` is a new loop); this is the net.

    A developer's ``.env`` must not leak in: the README tells them to put ``CONTACT_EMAIL``
    there, which would turn the "without a contact email" tests green for the wrong reason.
    ``CONTACT_EMAIL=""`` beats the dotenv file and validates to unset (``settings.py``), and
    ``env_file=None`` — pydantic-settings reads ``model_config`` at instantiation — keeps every
    other key (``LOG_JSON``, ``HTTP_TIMEOUT_SECONDS``, ...) out as well.
    """
    monkeypatch.setenv("DATABASE_URL", migrated_database)
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
def logging_calls(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[tuple[str, bool]]]:
    """Record what the Typer callback hands ``configure_logging`` instead of reconfiguring the
    root logger under pytest.

    The stub still points structlog at an in-memory logger: unconfigured, structlog prints to
    stdout, and ``log.exception`` renders a traceback whose source context would pollute the
    CLI's stdout (the real ``configure_logging`` sends everything to stderr).
    """
    calls: list[tuple[str, bool]] = []
    captured = structlog.testing.CapturingLoggerFactory()

    def fake_configure(level: str = "INFO", json_output: bool = True) -> None:
        calls.append((level, json_output))
        structlog.configure(logger_factory=captured, cache_logger_on_first_use=False)

    monkeypatch.setattr(cli, "configure_logging", fake_configure)
    try:
        yield calls
    finally:
        structlog.reset_defaults()


@pytest.fixture
def contact_email(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("CONTACT_EMAIL", CONTACT)
    get_settings.cache_clear()
    return CONTACT


@dataclass(frozen=True, slots=True)
class RunCall:
    connector: Connector[Any]
    session_factory: async_sessionmaker[AsyncSession]
    http: HttpClient
    user_agent: str
    since: datetime | None
    now: datetime | None


@dataclass
class FakeRunConnector:
    """Stands in for ``ingest.pipeline.run_connector``: records every call and returns a
    ``FetchRun`` with the configured outcome, or raises ``error`` (the database-down case —
    the only way the real one raises)."""

    status: enums.FetchRunStatus = enums.FetchRunStatus.OK
    error_text: str | None = None
    error: Exception | None = None
    calls: list[RunCall] = field(default_factory=list)

    async def __call__(
        self,
        connector: Connector[Any],
        *,
        session_factory: async_sessionmaker[AsyncSession],
        http: HttpClient,
        since: datetime | None = None,
        now: datetime | None = None,
    ) -> FetchRun:
        # The client is open during the call and closed by the CLI afterwards; read the UA now.
        self.calls.append(RunCall(connector, session_factory, http, http.user_agent, since, now))
        if self.error is not None:
            raise self.error
        return FetchRun(
            id=len(self.calls),
            connector=connector.name,
            started_at=STARTED_AT,
            finished_at=STARTED_AT,
            status=self.status,
            n_fetched=3,
            n_upserted=2,
            error_text=self.error_text,
        )


@pytest.fixture
def fake_run(monkeypatch: pytest.MonkeyPatch) -> FakeRunConnector:
    fake = FakeRunConnector()
    monkeypatch.setattr(cli, "run_connector", fake)
    return fake


@pytest.fixture
def dispose_calls(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Count ``dispose_engine`` calls made by the CLI (delegating to the real one)."""
    calls: list[int] = []

    async def counting_dispose() -> None:
        calls.append(1)
        await dispose_engine()

    monkeypatch.setattr(cli, "dispose_engine", counting_dispose)
    return calls


class FakeConnector(Connector[dict[str, Any]]):
    """A second registered connector for the ``--all`` tests: yields nothing, needs no contact
    email, and is only ever run through the fake ``run_connector``."""

    name = "fake"
    items: tuple[dict[str, Any], ...] = ()

    async def fetch(self, ctx: FetchContext) -> AsyncIterator[dict[str, Any]]:
        for raw in self.items:
            yield raw

    def to_records(self, raw: dict[str, Any]) -> Iterable[CompanyRecord]:
        return ()


def install_registry(
    monkeypatch: pytest.MonkeyPatch,
    classes: Mapping[str, type[Connector[Any]]],
    *,
    disabled: Iterable[str] = (),
) -> None:
    """Replace the registry and the ``config/connectors.yaml`` blocks the CLI sees: ``classes``
    keyed by connector name, each block ``enabled`` unless named in ``disabled``. A connector
    with a real block keeps it (``sec_edgar``'s ``options`` are validated on construction); the
    rest get a default one."""
    real = load_connectors_config()
    off = frozenset(disabled)
    blocks = {
        name: (
            real.get(name) if name in real.connectors else ConnectorConfig(name=name)
        ).model_copy(update={"enabled": name not in off})
        for name in classes
    }
    monkeypatch.setattr(cli, "all_connectors", lambda: dict(classes))
    monkeypatch.setattr(cli, "load_connectors_config", lambda: ConnectorsConfig(connectors=blocks))


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def invoke(runner: CliRunner, *args: str) -> Result:
    # Unexpected exceptions surface as test failures; typer.Exit / usage errors still become
    # exit codes because the app runs in standalone mode.
    return runner.invoke(cli.app, list(args), catch_exceptions=False)


def summary_lines(result: Result) -> list[str]:
    return [line for line in result.stdout.splitlines() if "status=" in line]


# ------------------------------------------------------------------ help and usage errors


def test_help_lists_the_registered_commands(runner: CliRunner) -> None:
    result = invoke(runner, "--help")
    assert result.exit_code == 0
    assert "refresh" in result.output
    assert "migrate" in result.output
    assert "seed" in result.output


def test_refresh_help_documents_now_and_since(runner: CliRunner) -> None:
    result = invoke(runner, "refresh", "--help")
    assert result.exit_code == 0
    assert "--now" in result.output
    assert "--since" in result.output
    assert "--connector" in result.output
    assert "--all" in result.output


def test_refresh_without_a_selector_is_a_usage_error(
    runner: CliRunner, fake_run: FakeRunConnector
) -> None:
    result = invoke(runner, "refresh")
    assert result.exit_code == 2
    assert "--connector" in result.output
    assert "--all" in result.output
    assert fake_run.calls == []


def test_refresh_with_both_selectors_is_a_usage_error(
    runner: CliRunner, fake_run: FakeRunConnector, contact_email: str
) -> None:
    result = invoke(runner, "refresh", "--connector", "sec_edgar", "--all")
    assert result.exit_code == 2
    assert fake_run.calls == []


def test_unknown_connector_lists_the_known_ones(
    runner: CliRunner, fake_run: FakeRunConnector, contact_email: str
) -> None:
    result = invoke(runner, "refresh", "--connector", "nope")
    assert result.exit_code == 2
    assert "nope" in result.output
    assert "sec_edgar" in result.output
    assert fake_run.calls == []


def test_invalid_since_is_a_usage_error(
    runner: CliRunner, fake_run: FakeRunConnector, contact_email: str
) -> None:
    result = invoke(runner, "refresh", "--connector", "sec_edgar", "--since", "2025-13-45")
    assert result.exit_code == 2
    assert "YYYY-MM-DD" in result.output
    assert fake_run.calls == []


# ------------------------------------------------------------------ contact email (SPEC §4)


def test_sec_edgar_without_a_contact_email_fails_fast(
    runner: CliRunner, fake_run: FakeRunConnector
) -> None:
    result = invoke(runner, "refresh", "--connector", "sec_edgar")
    assert result.exit_code == 1
    assert "CONTACT_EMAIL" in result.output
    assert "sec_edgar" in result.output
    assert fake_run.calls == [], "no FetchRun may be attempted without a contact email"


def test_all_without_a_contact_email_skips_that_connector_and_runs_the_rest(
    runner: CliRunner, fake_run: FakeRunConnector
) -> None:
    """``--all`` asks for every enabled connector; one unconfigured source is not a reason to
    refuse the other eight.

    This is the *first* command a fresh checkout runs (README step 6), so the all-or-nothing
    reading ended a first run with an empty database and no roles at all — which reads as the
    project being broken rather than as one setting being unset. SPEC §4 requires the address
    for SEC alone; nothing else here needs one. Same call the seed loader makes for an
    unreachable domain (SPEC §10, "log rather than failing the run").
    """
    result = invoke(runner, "refresh", "--all")

    assert result.exit_code == 0, "nothing failed — one connector was skipped, not run and lost"
    assert "sec_edgar" in result.output
    assert "CONTACT_EMAIL" in result.output
    ran = [call.connector.name for call in fake_run.calls]
    assert "sec_edgar" not in ran, "it cannot run without an address and must not be attempted"
    assert len(ran) > 1, "every other enabled connector still runs"
    assert ran == [name for name in ran if name not in {"sec_edgar"}]


def test_a_named_connector_still_fails_fast_without_a_contact_email(
    runner: CliRunner, fake_run: FakeRunConnector
) -> None:
    """Naming the one connector that cannot run is different from asking for all of them.

    ``--connector sec_edgar`` has no other possible meaning, so it stays an error with exit 1
    and no ``FetchRun`` — the shape the README documents at step 5. The two selections diverge
    on purpose; this test and the one above are the pair that pins the difference.
    """
    result = invoke(runner, "refresh", "--connector", "sec_edgar")
    assert result.exit_code == 1
    assert fake_run.calls == []


def test_a_dotenv_in_the_cwd_does_not_leak_into_the_tests(
    runner: CliRunner,
    fake_run: FakeRunConnector,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The README tells developers to put CONTACT_EMAIL in .env; ``make test`` from that
    checkout must still see it unset — and no other .env key — thanks to ``cli_env``."""
    (tmp_path / ".env").write_text(
        "CONTACT_EMAIL=someone@example.com\nHTTP_TIMEOUT_SECONDS=1\n", encoding="utf-8"
    )
    monkeypatch.delenv("HTTP_TIMEOUT_SECONDS", raising=False)
    monkeypatch.chdir(tmp_path)

    result = invoke(runner, "refresh", "--connector", "sec_edgar")
    assert result.exit_code == 1
    assert "CONTACT_EMAIL" in result.output
    assert fake_run.calls == []
    settings = get_settings()
    assert settings.contact_email is None
    assert settings.http_timeout_seconds == Settings.model_fields["http_timeout_seconds"].default


# ------------------------------------------------------------------ invalid settings


def test_invalid_contact_email_is_a_one_line_error_not_a_traceback(
    runner: CliRunner, fake_run: FakeRunConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The exact value .env.example and the README warn about (SEC rejects it).
    monkeypatch.setenv("CONTACT_EMAIL", "dev@localhost")
    get_settings.cache_clear()

    result = invoke(runner, "refresh", "--connector", "sec_edgar")
    assert result.exit_code == 1
    assert "invalid settings" in result.output
    assert "CONTACT_EMAIL" in result.output
    assert "dev@localhost" in result.output
    assert "Traceback" not in result.output
    assert len(result.output.strip().splitlines()) == 1
    assert fake_run.calls == []


def test_invalid_settings_fail_every_subcommand_including_migrate(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both errors on one line, each named by its environment variable (the message for a
    generic pydantic failure does not name the field itself); alembic is never reached."""
    monkeypatch.setenv("DATABASE_URL", "sqlite:///tracker.db")
    monkeypatch.setenv("HTTP_TIMEOUT_SECONDS", "soon")
    get_settings.cache_clear()
    upgrades: list[str] = []
    monkeypatch.setattr(
        alembic.command, "upgrade", lambda config, revision, **kw: upgrades.append(revision)
    )

    result = invoke(runner, "migrate")
    assert result.exit_code == 1
    assert "DATABASE_URL must start with 'postgresql+asyncpg://'" in result.output
    assert "HTTP_TIMEOUT_SECONDS: " in result.output
    assert "Traceback" not in result.output
    assert len(result.output.strip().splitlines()) == 1
    assert upgrades == []


def test_a_rejected_database_url_never_reaches_stderr_with_its_password(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The one-line settings error is printed to a terminal, a log aggregator and bug reports.

    A libpq URL that merely lacks ``+asyncpg`` is the most likely thing to be rejected here and
    is normally a live credential, so the message names the scheme and stops there.
    """
    monkeypatch.setenv("DATABASE_URL", "postgresql://tracker:hunter2@db.internal:5432/prod")
    get_settings.cache_clear()

    result = invoke(runner, "stats")

    assert result.exit_code == 1
    assert "DATABASE_URL must start with 'postgresql+asyncpg://'" in result.output
    assert "got 'postgresql://'" in result.output
    assert "hunter2" not in result.output
    assert "db.internal" not in result.output


# ------------------------------------------------------------------ a successful refresh


def test_refresh_runs_sec_edgar_with_the_settings_user_agent(
    runner: CliRunner,
    fake_run: FakeRunConnector,
    contact_email: str,
    logging_calls: list[tuple[str, bool]],
    dispose_calls: list[int],
) -> None:
    result = invoke(runner, "refresh", "--connector", "sec_edgar")
    assert result.exit_code == 0, result.output

    assert len(fake_run.calls) == 1
    call = fake_run.calls[0]
    assert isinstance(call.connector, SecEdgarConnector)
    assert call.connector.config == load_connectors_config().get("sec_edgar")
    assert isinstance(call.http, HttpClient)
    assert call.user_agent == EXPECTED_UA
    # The client carries the *configured* etiquette, not the constructor defaults: dropping
    # rate_limit would fetch at 1 rps instead of the 8 in config/connectors.yaml, and a
    # MemoryCache would silently lose the 24 h ETag cache between scheduled runs (SPEC §4).
    settings = get_settings()
    config = load_connectors_config().get("sec_edgar")
    assert call.http.rate_limit == config.rate_limit
    assert call.http.rate_limit.requests_per_second == 8
    assert call.http.respect_robots is config.respect_robots is True
    assert isinstance(call.http.cache, FileCache)
    assert call.http.cache.directory == settings.http_cache_dir
    assert isinstance(call.session_factory, async_sessionmaker)
    assert call.since is None
    assert call.now is None

    lines = summary_lines(result)
    assert len(lines) == 1
    line = lines[0]
    assert line.startswith("sec_edgar")
    assert "status=ok" in line
    assert "fetched=3" in line
    assert "upserted=2" in line
    assert "duration=" in line
    assert "error=" not in line
    assert "warning" not in result.output

    settings = get_settings()
    assert logging_calls == [(settings.log_level, settings.log_json)]
    assert dispose_calls == [1]


def test_since_is_passed_through_as_an_aware_utc_midnight(
    runner: CliRunner, fake_run: FakeRunConnector, contact_email: str
) -> None:
    result = invoke(runner, "refresh", "--connector", "sec_edgar", "--since", "2025-01-01")
    assert result.exit_code == 0, result.output
    assert len(fake_run.calls) == 1
    assert fake_run.calls[0].since == datetime(2025, 1, 1, tzinfo=UTC)
    assert fake_run.calls[0].since is not None
    assert fake_run.calls[0].since.tzinfo is UTC


def test_now_runs_exactly_like_without_it(
    runner: CliRunner, fake_run: FakeRunConnector, contact_email: str
) -> None:
    plain = invoke(runner, "refresh", "--connector", "sec_edgar")
    with_now = invoke(runner, "refresh", "--connector", "sec_edgar", "--now")
    assert plain.exit_code == with_now.exit_code == 0
    assert summary_lines(plain) and summary_lines(with_now)
    assert len(fake_run.calls) == 2, "every invocation runs and writes a FetchRun — no debounce"
    first, second = fake_run.calls
    assert type(first.connector) is type(second.connector)
    assert (first.since, first.now, first.user_agent) == (
        second.since,
        second.now,
        second.user_agent,
    )


# ------------------------------------------------------------------ run outcomes → exit status


def test_error_run_exits_1_and_prints_the_error_text(
    runner: CliRunner, fake_run: FakeRunConnector, contact_email: str, dispose_calls: list[int]
) -> None:
    fake_run.status = enums.FetchRunStatus.ERROR
    fake_run.error_text = "fetch: HTTPStatusError: 503\nitem 2: upsert: boom"
    result = invoke(runner, "refresh", "--connector", "sec_edgar")
    assert result.exit_code == 1
    (line,) = summary_lines(result)
    assert "status=error" in line
    assert "HTTPStatusError: 503" in line
    assert "boom" in line
    assert "\n" not in line
    assert "not-recorded" not in line, "this run has a fetch_runs row"
    assert dispose_calls == [1]


def test_partial_run_warns_but_exits_0(
    runner: CliRunner, fake_run: FakeRunConnector, contact_email: str
) -> None:
    fake_run.status = enums.FetchRunStatus.PARTIAL
    fake_run.error_text = "filing 0001234567-26-000001: 404"
    result = invoke(runner, "refresh", "--connector", "sec_edgar")
    assert result.exit_code == 0
    (line,) = summary_lines(result)
    assert "status=partial" in line
    assert "0001234567-26-000001" in line
    assert "warning" in result.stderr
    assert "partial" in result.stderr


def test_run_connector_raising_is_reported_and_exits_1(
    runner: CliRunner, fake_run: FakeRunConnector, contact_email: str, dispose_calls: list[int]
) -> None:
    fake_run.error = ConnectionRefusedError("database down")
    result = invoke(runner, "refresh", "--connector", "sec_edgar")
    assert result.exit_code == 1
    (line,) = summary_lines(result)
    assert line.startswith("sec_edgar")
    assert "status=error" in line
    assert "database down" in line
    # No fetch_runs row exists for this failure: the line must not look like a recorded run.
    assert "fetch_run=not-recorded" in line
    assert "fetched=" not in line
    assert "fetch_runs" in result.stderr
    assert "not recorded" in result.stderr
    assert dispose_calls == [1], "the engine is disposed even when the run raised"


# ------------------------------------------------------------------ seed (SPEC §10)


def test_seed_runs_the_seed_loader_which_is_not_in_the_refresh_registry(
    runner: CliRunner, fake_run: FakeRunConnector
) -> None:
    """``cli.py seed`` builds :class:`ingest.seed.SeedConnector` directly, because a bootstrap
    must not be re-run by ``refresh --all`` every night (SPEC §10)."""
    assert SeedConnector.name not in all_connectors()

    result = invoke(runner, "seed")
    assert result.exit_code == 0, result.output
    assert [call.connector.name for call in fake_run.calls] == ["seed"]
    connector = fake_run.calls[0].connector
    assert isinstance(connector, SeedConnector)
    assert connector.options.validate_domains is True
    assert len(summary_lines(result)) == 1


def test_seed_needs_no_contact_email(runner: CliRunner, fake_run: FakeRunConnector) -> None:
    """Only ``sec_edgar`` does (SPEC §4); the seed loader probes ordinary company sites."""
    result = invoke(runner, "seed")
    assert result.exit_code == 0, result.output + result.stderr


def test_seed_skip_validation_turns_the_network_probe_off(
    runner: CliRunner, fake_run: FakeRunConnector
) -> None:
    result = invoke(runner, "seed", "--skip-validation")
    assert result.exit_code == 0, result.output
    connector = fake_run.calls[0].connector
    assert isinstance(connector, SeedConnector)
    assert connector.options.validate_domains is False


def test_seed_reports_a_failed_run_like_refresh(
    runner: CliRunner, fake_run: FakeRunConnector
) -> None:
    fake_run.status = enums.FetchRunStatus.ERROR
    fake_run.error_text = "gone.example: marked dead"
    result = invoke(runner, "seed")
    assert result.exit_code == 1
    assert "status=error" in result.output
    assert "gone.example" in result.output


def test_seed_help_explains_the_domain_validation(runner: CliRunner) -> None:
    result = invoke(runner, "seed", "--help")
    assert result.exit_code == 0
    assert "--skip-validation" in result.output
    assert "HEAD" in result.output
    assert "dead" in result.output


# ------------------------------------------------------------------ --all


def test_all_runs_every_enabled_registered_connector(
    runner: CliRunner, fake_run: FakeRunConnector, contact_email: str
) -> None:
    connectors_config = load_connectors_config()
    expected = [name for name in all_connectors() if connectors_config.get(name).enabled]
    assert "sec_edgar" in expected

    result = invoke(runner, "refresh", "--all")
    assert result.exit_code == 0, result.output
    assert [call.connector.name for call in fake_run.calls] == expected
    assert all(call.user_agent == EXPECTED_UA for call in fake_run.calls)
    assert len(summary_lines(result)) == len(expected)


def test_all_skips_connectors_disabled_in_config(
    runner: CliRunner,
    fake_run: FakeRunConnector,
    contact_email: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real registry has one connector and every yaml block is enabled, so the test above
    cannot tell "every enabled" from "every registered"; this one can."""
    install_registry(
        monkeypatch, {"sec_edgar": SecEdgarConnector, "fake": FakeConnector}, disabled={"fake"}
    )
    result = invoke(runner, "refresh", "--all")
    assert result.exit_code == 0, result.output
    assert [call.connector.name for call in fake_run.calls] == ["sec_edgar"]
    assert len(summary_lines(result)) == 1


def test_all_with_nothing_enabled_runs_nothing_and_says_so(
    runner: CliRunner, fake_run: FakeRunConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_registry(monkeypatch, {"fake": FakeConnector}, disabled={"fake"})
    result = invoke(runner, "refresh", "--all")
    assert result.exit_code == 0, result.output
    assert fake_run.calls == []
    assert summary_lines(result) == []
    assert "enabled" in result.stderr
    assert "config/connectors.yaml" in result.stderr


def test_a_named_connector_runs_even_when_disabled(
    runner: CliRunner, fake_run: FakeRunConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``enabled`` is the scheduler's switch (SPEC §7.2); naming a connector is the on-demand
    case of SPEC §2 and always runs."""
    install_registry(monkeypatch, {"fake": FakeConnector}, disabled={"fake"})
    result = invoke(runner, "refresh", "--connector", "fake")
    assert result.exit_code == 0, result.output
    assert [call.connector.name for call in fake_run.calls] == ["fake"]
    assert isinstance(fake_run.calls[0].connector, FakeConnector)
    assert len(summary_lines(result)) == 1


# ------------------------------------------------------------------ migrate


def test_migrate_calls_alembic_upgrade_head(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[AlembicConfig, str]] = []

    def fake_upgrade(
        config: AlembicConfig, revision: str, sql: bool = False, tag: str | None = None
    ) -> None:
        calls.append((config, revision))

    monkeypatch.setattr(alembic.command, "upgrade", fake_upgrade)
    result = invoke(runner, "migrate")
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    config, revision = calls[0]
    assert revision == "head"
    assert config.config_file_name is not None
    assert Path(config.config_file_name) == cli.ALEMBIC_INI


# ------------------------------------------------------------------ README (SPEC §14.3)

#: The README's sentence on ``sec_edgar``'s fetch window, matched with whitespace collapsed.
#: Every number and status it names is checked against the code below, so the two cannot
#: drift apart again; reword the README and this pattern together.
INCREMENTAL_WINDOW_CLAUSE = re.compile(
    r"looks back (?P<months>\d+) months on the first run, then from (?P<days>\d+) days "
    r"\(`overlap_days`\) before the start of its newest run that finished "
    r"(?P<anchors>`\w+`(?: or `\w+`)*), so late-indexed filings are picked up; "
    r"a run that ended `(?P<excluded>\w+)` does not advance the window"
)
LAST_SUCCESS_AT = datetime(2026, 9, 1, 5, tzinfo=UTC)


async def test_readme_describes_the_incremental_window_the_code_implements() -> None:
    """The README once said ``sec_edgar`` then looks "only since its last successful run".
    The connector restarts ``overlap_days`` before the anchor (``SecEdgarConnector.window``)
    and the anchor is the newest ``ok`` *or* ``partial`` run (``COMPLETED_RUN_STATUSES``), so
    a reader saw already-seen filings re-fetched (``fetched > 0``, nothing new) and did not
    expect a ``partial`` run to advance the window. The wording is pinned to the code."""
    text = " ".join(README.read_text(encoding="utf-8").split())
    assert "only since its last successful run" not in text
    match = INCREMENTAL_WINDOW_CLAUSE.search(text)
    assert match is not None, "README no longer describes sec_edgar's incremental window"

    anchors = {enums.FetchRunStatus(name.strip("`")) for name in match["anchors"].split(" or ")}
    assert anchors == set(COMPLETED_RUN_STATUSES)
    assert enums.FetchRunStatus(match["excluded"]) not in COMPLETED_RUN_STATUSES

    # The numbers are what the connector does on the real config, not just its defaults.
    connector = SecEdgarConnector(load_connectors_config().get("sec_edgar"), load_regions_config())
    assert int(match["months"]) == connector.options.backfill_months
    async with HttpClient(user_agent=EXPECTED_UA, cache=MemoryCache()) as http:
        ctx = FetchContext(
            http=http,
            now=STARTED_AT,
            since=None,
            last_success_at=LAST_SUCCESS_AT,
            run_id=1,
            log=get_logger("tests.cli"),
        )
        start, end = connector.window(ctx)
    assert end == STARTED_AT.date()
    assert (LAST_SUCCESS_AT.date() - start).days == int(match["days"])


# ------------------------------------------------------------------ Makefile notes vs cli.py

MAKEFILE = cli.ROOT / "Makefile"
# The ``target: ## help`` convention ``make help`` greps for — the same regex as its recipe.
MAKEFILE_TARGET = re.compile(r"^(?P<target>[a-zA-Z_-]+):.*?## (?P<help>.*)$")
CLI_INVOCATION = re.compile(r"\bpython cli\.py (?P<command>[a-z][a-z_-]*)")


@dataclass(frozen=True, slots=True)
class MakeTarget:
    name: str
    help_text: str
    recipe: tuple[str, ...]


def makefile_targets() -> dict[str, MakeTarget]:
    """Every documented target: the help text ``make help`` prints and the tab-indented recipe
    lines that follow it."""
    lines = MAKEFILE.read_text(encoding="utf-8").splitlines()
    targets: dict[str, MakeTarget] = {}
    for index, line in enumerate(lines):
        match = MAKEFILE_TARGET.match(line)
        if match is None:
            continue
        recipe: list[str] = []
        for following in lines[index + 1 :]:
            if not following.startswith("\t"):
                break
            recipe.append(following.strip())
        targets[match["target"]] = MakeTarget(match["target"], match["help"], tuple(recipe))
    return targets


def cli_commands() -> frozenset[str]:
    """The subcommands ``cli.py`` registers — what ``python cli.py --help`` lists."""
    # typer 0.27 vendors its CLI layer as typer._click; TyperGroup is the public type.
    group = get_command(cli.app)
    assert isinstance(group, TyperGroup)
    return frozenset(group.commands)


def test_makefile_help_never_claims_cli_py_is_still_to_come() -> None:
    """``cli.py`` shipped with Phase 2 (``import cli`` above proves it): a note saying it
    "arrives" in a later phase is stale — the ``seed`` target carried "cli.py arrives in
    Phase 3" next to a working ``refresh`` target."""
    assert (cli.ROOT / "cli.py").is_file()
    for target in makefile_targets().values():
        assert "cli.py arrives" not in target.help_text, f"{target.name}: {target.help_text!r}"


def test_makefile_notes_track_the_commands_cli_py_registers() -> None:
    """A target that runs ``python cli.py <command>`` describes the command's state truthfully:
    a registered command gets no "arrives later" note, an unregistered one (``seed``, Phase 3)
    gets a note naming *the command* — ``cli.py`` itself is already here. Registering the
    command flips the branch, so the note must go the moment it is stale."""
    registered = cli_commands()
    assert {"refresh", "migrate"} <= registered
    targets = makefile_targets()
    invoking = {
        target.name: match["command"]
        for target in targets.values()
        if (match := CLI_INVOCATION.search(" ".join(target.recipe))) is not None
    }
    assert {"refresh", "seed"} <= invoking.keys()
    for name, command in invoking.items():
        help_text = targets[name].help_text
        if command in registered:
            assert "arrives" not in help_text, (
                f"{name}: {help_text!r} defers {command!r}, but cli.py registers it"
            )
        else:
            assert f"the {command} command arrives in Phase" in help_text, (
                f"{name}: {help_text!r} must say which phase the {command!r} command arrives in"
            )


# ------------------------------------------------------------------ sync-contacts (SPEC §6)


@pytest.fixture
def empty_database(migrated_database: str) -> str:
    """Every table truncated before the test runs.

    The CLI tests share one session-scoped database and most of them never write to it, so
    conftest's truncating ``engine`` fixture is async and out of reach from a ``CliRunner``
    test. These four write and then assert on counts, so they need the same guarantee: emptied
    at setup, left alone at teardown, so a failure can be inspected.
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


def seed_company(name: str, domain: str | None) -> int:
    """One company written straight into the disposable database, the way a phase before this
    one left them: a row with no constructed SPEC §6 links, which no upsert will ever revisit."""

    async def write() -> int:
        engine = create_async_engine(_configured_url())
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                async with session.begin():
                    company = Company(
                        name=name, normalized_name=normalize_name(name), domain=domain
                    )
                    session.add(company)
                    await session.flush()
                    company_id = company.id
                return company_id
        finally:
            await engine.dispose()

    return asyncio.run(write())


def stored_contact_values() -> set[str]:
    async def read() -> set[str]:
        engine = create_async_engine(_configured_url())
        try:
            async with async_sessionmaker(engine)() as session:
                return set((await session.scalars(select(Contact.value))).all())
        finally:
            await engine.dispose()

    return asyncio.run(read())


def _configured_url() -> str:
    """The database ``cli_env`` pointed the CLI at — the same one the command will write to."""
    return get_settings().database_url


def test_sync_contacts_builds_the_links_and_reports_what_it_wrote(
    runner: CliRunner, empty_database: str
) -> None:
    """The command exists for exactly this database: a company the ingest path never upserted,
    which therefore has none of SPEC §6's constructed links and never would."""
    seed_company("Astranis", "astranis.com")

    result = invoke(runner, "sync-contacts")

    assert result.exit_code == 0, result.output
    assert "sync-contacts  companies=1  added=2  removed=0" in result.stdout
    assert "dry run" not in result.stdout
    assert "already in sync" not in result.stdout
    values = stored_contact_values()
    assert "https://www.linkedin.com/company/astranis" in values
    assert any("/search/results/people/" in value for value in values)

    # A second invocation is the operator's "did that work?" — three zeroes need a word, or
    # they read as a command that failed to find anything to do.
    again = invoke(runner, "sync-contacts")
    assert again.exit_code == 0, again.output
    assert "companies=1  added=0  removed=0  (already in sync)" in again.stdout
    assert stored_contact_values() == values


def test_sync_contacts_dry_run_writes_nothing_and_says_so(
    runner: CliRunner, empty_database: str
) -> None:
    """A command that touches every company needs a way to be asked first. The counts are the
    real ones — the sweep runs in full and the transaction is discarded."""
    seed_company("Astranis", "astranis.com")

    result = invoke(runner, "sync-contacts", "--dry-run")

    assert result.exit_code == 0, result.output
    assert "companies=1  added=2  removed=0  (dry run — nothing written)" in result.stdout
    assert stored_contact_values() == set()


def test_sync_contacts_disposes_the_engine(
    runner: CliRunner, dispose_calls: list[int], empty_database: str
) -> None:
    """Each ``asyncio.run`` gets a fresh event loop and an asyncpg pool must be closed on the
    loop that created it, so every command that opens one closes it (as ``refresh`` does)."""
    seed_company("Astranis", "astranis.com")

    assert invoke(runner, "sync-contacts").exit_code == 0

    assert dispose_calls == [1]


def test_sync_contacts_rejects_a_batch_size_below_one(runner: CliRunner) -> None:
    """Typer's ``min=1`` catches it as a usage error, before a connection is opened."""
    result = invoke(runner, "sync-contacts", "--batch-size", "0")

    assert result.exit_code != 0
    assert "batch-size" in result.output.lower()


def test_sync_contacts_forwards_the_batch_size_it_was_given(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A flag that is accepted, validated by Typer and then dropped is invisible to every gate
    this project has: with the constant hard-wired into the call instead of the argument, ruff,
    ``mypy --strict`` and the whole suite still pass, and stdout is byte-identical to an honest
    run. The sweep's own test proves ``batch_size`` is *used*; this proves it arrives.
    """
    calls: list[tuple[int, bool]] = []

    async def record(
        session_factory: async_sessionmaker[AsyncSession], *, batch_size: int, dry_run: bool
    ) -> ContactSyncResult:
        calls.append((batch_size, dry_run))
        return ContactSyncResult(companies=0, added=0, removed=0, dry_run=dry_run)

    monkeypatch.setattr(cli, "sync_constructed_contacts", record)

    assert invoke(runner, "sync-contacts", "--batch-size", "25").exit_code == 0
    assert invoke(runner, "sync-contacts", "--dry-run").exit_code == 0

    assert calls == [(25, False), (CONTACT_SYNC_BATCH, True)]


# ------------------------------------------------------- sync-regions (SPEC §11, §12 Phase 7)
#
# What the sweep *does* is tested in tests/test_cli_ops.py against real rows. What is tested here
# is the plumbing the two sync commands share and that no behavioural test can see: the option
# reaching the coroutine, Typer refusing a nonsense value before a connection is opened, and the
# engine being disposed on the way out.


def test_sync_regions_rejects_a_batch_size_below_one(runner: CliRunner) -> None:
    """Typer's ``min=1`` catches it as a usage error, before a connection is opened — the same
    guard ``sync-contacts`` has, because a zero page size is an infinite keyset loop."""
    result = invoke(runner, "sync-regions", "--batch-size", "0")

    assert result.exit_code != 0
    assert "batch-size" in result.output.lower()


def test_sync_regions_forwards_the_batch_size_it_was_given(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A flag accepted, validated by Typer and then dropped is invisible to every gate here: with
    the constant hard-wired into the call, ruff, ``mypy --strict`` and the whole suite still pass
    and stdout is byte-identical. The sweep's own tests prove ``batch_size`` is *used*; this
    proves it arrives, and that ``--dry-run`` leaves the default in place rather than replacing it.
    """
    calls: list[tuple[int, bool]] = []

    async def record(*, batch_size: int, dry_run: bool) -> cli.RegionSyncResult:
        calls.append((batch_size, dry_run))
        return cli.RegionSyncResult(locations=0, changes=(), unconfigured=(), dry_run=dry_run)

    monkeypatch.setattr(cli, "sync_regions_once", record)

    assert invoke(runner, "sync-regions", "--batch-size", "25").exit_code == 0
    assert invoke(runner, "sync-regions", "--dry-run").exit_code == 0

    assert calls == [(25, False), (cli.REGION_SYNC_BATCH, True)]


def test_sync_regions_disposes_the_engine(
    runner: CliRunner, dispose_calls: list[int], empty_database: str
) -> None:
    """Each ``asyncio.run`` gets a fresh event loop and an asyncpg pool must be closed on the loop
    that created it, so every command that opens one closes it — including the sweep that finds
    nothing to do, which is the path that returns earliest."""
    assert invoke(runner, "sync-regions").exit_code == 0

    assert dispose_calls == [1]


def test_the_readme_upgrade_section_promises_only_the_links_a_row_can_have() -> None:
    """SPEC §6 builds the LinkedIn *company* URL from the domain, so a company without one — an
    EDGAR-only Form D row, 53 of the 3,188 on the developer's own database — gets the people
    search and nothing else, sweep or no sweep.

    The upgrade section names exactly that kind of company as the reason to run ``sync-contacts``
    and then says one sweep fixes it. Without the caveat the operator opens a company page,
    finds no LinkedIn link where the README promised one, and goes looking for a bug in the
    command. The same paragraph must also name ``company_site``'s real skip condition: it works
    from ``CompanyTarget.home_url``, which falls back to ``https://{domain}/``, so a row with a
    domain and no website is visited every week like any other.
    """
    text = " ".join(README.read_text(encoding="utf-8").split())
    assert "the company link is built from the domain" in text
    assert "gets the people search alone" in text
    assert "neither a website nor a domain" in text
    assert "a company with no website for `company_site` to visit" not in text
