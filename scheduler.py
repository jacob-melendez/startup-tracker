"""The scheduler service (SPEC §3's third Compose service, SPEC §7.2, §11).

One long-lived process whose only job is to fire connector runs on the cron expressions in
``config/connectors.yaml`` — "loaded by APScheduler at startup" (SPEC §7.2). It holds no
schedule of its own: there is no cron expression, no connector list and no rate limit anywhere
in this module, because a second place that decided when something runs would quietly become a
second source of truth (CLAUDE.md).

Two properties are worth stating up front, because they are what make this file boring:

* **A scheduled run is the same code path as the CLI.** The job body calls :func:`cli.run_one`,
  the very function ``cli.py refresh --connector NAME`` calls. The User-Agent, the per-host rate
  limit, the ``robots.txt`` policy and the 24 h ETag cache therefore cannot drift between a
  nightly run and a hand-run one — they are not re-derived here at all.
* **Cron is parsed by the same call as ``/runs``.** Triggers are built with
  ``CronTrigger.from_crontab(cadence, timezone=UTC)``, exactly as
  :func:`ingest.config.cadence_period` does for the staleness highlight, so the schedule and the
  page that reports on it can never disagree about what "daily" means. Everything is UTC.

  One APScheduler 3.x wart to know before reading a cadence: its ``from_crontab`` numbers
  weekdays from **Monday = 0**, not crontab's Sunday = 0 (APScheduler documents this as a
  historical mistake, fixed only in 4.x). A numeric ``* * 6`` therefore means *Sunday* here, not
  Saturday, which is how the weekly cadences came to fire a day off SPEC §7.2's table. They are
  written with day names (``0 3 * * sat``) in ``config/connectors.yaml`` for that reason, and
  its header says so — keep it that way.

The process reads no database at startup — an unmigrated or unreachable database must not stop
the service from coming up, since the runs it schedules are precisely how the database gets
populated. The only query it makes is the failure-streak check after a failed run.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from datetime import UTC, datetime

# APScheduler 3.x ships no ``py.typed``, so both imports are untyped to mypy --strict; the same
# ignore sits on the ``CronTrigger`` import in ingest/config.py.
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]
from pydantic import ValidationError

from cli import CONTACT_EMAIL_VAR, CONTACT_REQUIRED, NOT_RECORDED, format_settings_error, run_one
from db import enums
from db.queries import CONSECUTIVE_FAILURE_ALERT, consecutive_failure_counts
from db.session import dispose_engine, get_session_factory
from ingest.config import (
    CONNECTORS_YAML,
    UNIMPLEMENTED_CONNECTORS,
    ConnectorsConfig,
    load_connectors_config,
    load_regions_config,
)
from ingest.connectors import all_connectors
from logging_config import configure_logging, get_logger
from settings import Settings, get_settings

log = get_logger(__name__)

#: How late a fire may be and still run — for a fire this process was alive to miss: the event
#: loop was blocked, the machine was suspended and resumed, the clock jumped. ``coalesce``
#: collapses one job's backlog to a single fire and this decides whether that survivor still runs.
#: APScheduler's default of 1 second drops anything not dispatched almost immediately, and
#: ``None`` means "run however late", which would have a machine woken after a weekend stampede
#: three days of missed daily fires at a useless hour. An hour is the middle ground.
#:
#: Two things it deliberately does *not* govern, because it cannot:
#:
#: * A run queued behind another connector's run (see :data:`_RUN_LOCK`) is never dropped by it,
#:   at any value. APScheduler tests a fire's lateness *before* awaiting the job body, and the
#:   body is where the lock is awaited, so the lateness it measures is always ~0.
#: * A fire missed while the process was *down* is not replayed at all, however short the outage.
#:   The job store is the default in-memory one and the schedule is rebuilt from scratch at every
#:   boot, so a freshly added job's first fire is always in the future. A restart is not a
#:   refresh — ``make refresh`` is (see :func:`serve`).
MISFIRE_GRACE_SECONDS = 3600

#: How long :func:`serve` waits at shutdown for a run that is already in flight.
#:
#: A signal must not stop a run mid-flight. ``ingest.pipeline.run_connector`` commits the
#: ``fetch_runs`` row *before* it fetches anything and finalizes it — ``finished_at``, ``status``,
#: the counts, ``error_text`` — from a ``finally`` that opens a second session (SPEC §7.2: every
#: run writes a ``FetchRun`` row regardless of outcome). Cancel the run and that ``finally`` dies
#: while re-opening its connection, so the row keeps the column default ``status='error'`` with a
#: NULL ``finished_at``: ``/runs`` shows it as *still running* with 0 fetched, 0 upserted and no
#: error text, for ever, and nothing reaps it. So shutdown waits instead — see :func:`drain_runs`.
#:
#: Bounded, because a wedged run must not hold the process open indefinitely; on timeout the row
#: is orphaned exactly as it would have been anyway, and a WARNING says so. Keep the ``scheduler``
#: service's ``stop_grace_period`` in docker-compose.yml above this, or Docker's SIGKILL cuts the
#: wait short and the row is orphaned regardless.
SHUTDOWN_DRAIN_SECONDS = 300

#: Reasons a configured connector is not scheduled, logged one INFO line each so an empty
#: schedule is always explained rather than merely observed.
#:
#: The two "not in the registry" cases are kept apart because they call for opposite responses.
#: ``product_hunt`` and ``opencorporates`` have no code behind them yet
#: (:data:`ingest.config.UNIMPLEMENTED_CONNECTORS`, the list ``/runs`` and ``cli.py stats`` mark
#: from), so nothing an operator does will make them run. ``seed`` is fully implemented
#: (:mod:`ingest.seed`) and deliberately kept out of the registry so ``refresh --all`` never
#: re-validates the bootstrap list — it runs, by hand, as ``cli.py seed``. One message covering
#: both would send an operator looking for a bug in whichever half they were not thinking of.
SKIP_NOT_IMPLEMENTED = "no implementation yet — nothing to run (SPEC §4 Tier 2)"
SKIP_NOT_REGISTERED = "not in the connector registry — run it by hand (seed: `cli.py seed`)"
SKIP_DISABLED = "enabled: false in config/connectors.yaml"
SKIP_ON_DEMAND = "cadence: null — on-demand only"

#: Serializes every connector run in this process: at most one run happens at a time.
#:
#: The four ATS connectors all fire at 06:00 (SPEC §7.2) and each of them may fetch an arbitrary
#: company's ``/careers`` page to discover a board token — a Tier-3 fetch, capped at one request
#: per domain per 2 s (SPEC §4). That limit is enforced per :class:`ingest.http.HttpClient`, and
#: :func:`cli.run_one` builds a fresh client per run, so two runs overlapping at 06:00 could hit
#: the same careers host twice within the interval and neither would know. Serializing also
#: makes the scheduler behave exactly like ``cli.py refresh --all``, which runs connectors one
#: after another — one fewer way for a scheduled run to differ from a hand-run one.
_RUN_LOCK = asyncio.Lock()


def build_scheduler(config: ConnectorsConfig | None = None) -> AsyncIOScheduler:
    """A configured but **unstarted** :class:`AsyncIOScheduler`, one job per runnable connector.

    ``config`` defaults to ``config/connectors.yaml`` (SPEC §7.2: the only place a schedule is
    defined). A connector gets a job when it has an implementation, is ``enabled`` and has a
    cadence; every other configured connector is logged once at INFO with the reason, so
    "nothing is scheduled" is never a silent state. No connector is named here: ``product_hunt``
    and ``opencorporates`` are read from :data:`~ingest.config.UNIMPLEMENTED_CONNECTORS` — the
    same list ``/runs`` and ``cli.py stats`` mark from — and ``seed`` falls out of the registry
    on its own, because SPEC §10's bootstrap is run by ``cli.py seed``, never on a cadence.

    Job options: ``max_instances=1`` and ``coalesce=True`` mean a slow run is never overlapped by
    its own next fire and a backlog of missed fires collapses to one, and
    ``misfire_grace_time=MISFIRE_GRACE_SECONDS`` decides how late that survivor may be. Building
    the scheduler touches nothing but the YAML: no database, no network, no clock.
    """
    connectors = config if config is not None else load_connectors_config()
    registry = frozenset(all_connectors())
    scheduler = AsyncIOScheduler(timezone=UTC)
    for name, block in connectors.connectors.items():
        # Reported in this order on purpose: a connector that is both unimplemented and
        # on-demand (``opencorporates``) is reported as unimplemented, the more fundamental fact.
        if name in UNIMPLEMENTED_CONNECTORS:
            log.info("connector not scheduled", connector=name, reason=SKIP_NOT_IMPLEMENTED)
        elif name not in registry:
            log.info("connector not scheduled", connector=name, reason=SKIP_NOT_REGISTERED)
        elif not block.enabled:
            log.info("connector not scheduled", connector=name, reason=SKIP_DISABLED)
        elif block.cadence is None:
            log.info("connector not scheduled", connector=name, reason=SKIP_ON_DEMAND)
        else:
            scheduler.add_job(
                run_scheduled,
                trigger=CronTrigger.from_crontab(block.cadence, timezone=UTC),
                args=[name],
                id=name,
                name=name,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=MISFIRE_GRACE_SECONDS,
                replace_existing=True,
            )
    return scheduler


def next_fire_times(
    scheduler: AsyncIOScheduler, *, now: datetime | None = None
) -> dict[str, datetime | None]:
    """Next fire time per job id, asked of the trigger rather than read off the job.

    APScheduler assigns ``Job.next_run_time`` only when a job reaches a *running* scheduler, so
    the scheduler :func:`build_scheduler` returns has none to read. Asking the trigger is the
    same computation the scheduler itself will make at ``start()``, and it lets a test (and the
    startup log) see the schedule without any wall-clock waiting.
    """
    at = now if now is not None else datetime.now(UTC)
    fires: dict[str, datetime | None] = {}
    for job in scheduler.get_jobs():
        fire: datetime | None = job.trigger.get_next_fire_time(None, at)
        fires[str(job.id)] = fire
    return fires


def log_schedule(
    scheduler: AsyncIOScheduler,
    *,
    settings: Settings,
    config: ConnectorsConfig,
    now: datetime | None = None,
) -> None:
    """Report the loaded schedule, then warn about anything scheduled that cannot work.

    One INFO line naming every scheduled connector with its cadence and its next fire time — the
    line an operator reads to answer "did it pick up my YAML edit, and when does it next run?".
    Then a WARNING per scheduled connector that needs a contact address in the User-Agent while
    ``CONTACT_EMAIL`` is unset (SPEC §4: SEC's fair-access policy). Such a connector is still
    scheduled on purpose: a nightly ``error`` row carrying that reason in ``error_text`` is
    visible on ``/runs``, and a connector silently missing from the schedule is not.
    """
    fires = next_fire_times(scheduler, now=now)
    jobs = [
        {
            "connector": str(job.id),
            "cadence": config.get(str(job.id)).cadence,
            "next_fire_time": _isoformat(fires[str(job.id)]),
        }
        for job in scheduler.get_jobs()
    ]
    log.info("schedule loaded", jobs=jobs, count=len(jobs), timezone="UTC")
    if settings.contact_email is not None:
        return
    for job in scheduler.get_jobs():
        if str(job.id) in CONTACT_REQUIRED:
            log.warning(
                "connector scheduled without a contact email",
                connector=str(job.id),
                setting=CONTACT_EMAIL_VAR,
                detail=(
                    f"{CONTACT_EMAIL_VAR} is unset, so every run of this connector will fail "
                    f"(SEC fair-access policy, SPEC §4) and show as an error on /runs; set "
                    f"{CONTACT_EMAIL_VAR}=you@example.com in the environment or .env"
                ),
            )


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


async def run_scheduled(name: str) -> None:
    """One scheduled connector run. Never raises — a job that raises kills nothing here, but a
    scheduler that logs a traceback and keeps its jobs is what an operator can act on.

    The connector is rebuilt from ``config/connectors.yaml`` and ``config/regions.yaml`` on every
    fire, so a run always reflects the configuration this process loaded. Note *loaded*: both
    loaders are ``@cache``d, so editing the YAML takes effect at
    ``docker compose restart scheduler``, not at the next fire.

    The run itself goes through :func:`cli.run_one` under :data:`_RUN_LOCK`, so a scheduled run
    and ``cli.py refresh --connector NAME`` are the same code path by construction (see the
    module docstring). ``run_connector`` writes the ``fetch_runs`` row itself and swallows
    connector, record and refresh failures into it (SPEC §7.2), so an exception escaping to the
    handler below means the row could not be written at all — the database is unreachable, and
    there is nothing on ``/runs`` to reconcile this failure against.
    """
    try:
        settings = get_settings()
        config = load_connectors_config().get(name)
        connector = all_connectors()[name](config, load_regions_config())
        async with _RUN_LOCK:
            run = await run_one(connector, config, settings=settings, since=None)
    except Exception:
        # The same token `cli.py refresh` prints on such a line, so one grep finds both.
        log.exception("scheduled run failed", connector=name, note=NOT_RECORDED)
        return
    status = enums.FetchRunStatus(run.status)
    log.info(
        "scheduled run",
        connector=name,
        status=status.value,
        fetched=run.n_fetched,
        upserted=run.n_upserted,
    )
    if status is enums.FetchRunStatus.ERROR:
        # Only after an error: a run that ended ok or partial has already reset the streak to 0,
        # so there is nothing to alert on and no reason to spend a query asking.
        await alert_on_failure_streak(name)


async def alert_on_failure_streak(name: str) -> None:
    """Log SPEC §7.2's ERROR line when ``name`` has now failed :data:`CONSECUTIVE_FAILURE_ALERT`
    runs in a row — the same streak ``/runs`` marks as *Failing*.

    Any failure of the streak query is logged and dropped: this is an ops signal about a run that
    has already been recorded, and turning it into a second failure would be worse than missing
    the line.
    """
    try:
        async with get_session_factory()() as session:
            counts = await consecutive_failure_counts(session, connector=name)
    except Exception:
        log.exception("consecutive-failure check failed", connector=name)
        return
    consecutive = counts.get(name, 0)
    if consecutive >= CONSECUTIVE_FAILURE_ALERT:
        log.error(
            "connector failing",
            connector=name,
            consecutive=consecutive,
            threshold=CONSECUTIVE_FAILURE_ALERT,
        )


async def drain_runs() -> None:
    """Wait for the connector run in flight, if any, to finish — at most
    :data:`SHUTDOWN_DRAIN_SECONDS`, read at call time so a test can shorten it.

    Acquiring :data:`_RUN_LOCK` *is* the wait: every run holds it for the whole of
    :func:`cli.run_one`, so holding it means no run is between the ``fetch_runs`` row it inserted
    and the ``finally`` that finalizes that row (see :data:`SHUTDOWN_DRAIN_SECONDS` for why that
    gap must not be interrupted). ``asyncio.Lock`` hands off in arrival order, so a run already
    queued behind another one is drained too rather than cancelled while it waits.

    Never raises. A timeout is reported and returned from: the caller is on its way out, and the
    orphaned row it leaves is what would have happened without any drain at all.
    """
    if not _RUN_LOCK.locked():
        return
    budget = SHUTDOWN_DRAIN_SECONDS
    log.info("waiting for the run in flight", timeout_seconds=budget)
    try:
        await asyncio.wait_for(_RUN_LOCK.acquire(), budget)
    except TimeoutError:
        log.warning(
            "shutdown drain timed out",
            timeout_seconds=budget,
            detail=(
                "a connector run was still going when the drain budget ran out and is being "
                "cancelled, so its fetch_runs row keeps status=error with no finished_at and "
                "shows as still running on /runs"
            ),
        )
        return
    _RUN_LOCK.release()
    log.info("run in flight finished")


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """SIGINT/SIGTERM set ``stop``, which is how :func:`serve` gets to run its shutdown.

    ``docker compose stop`` sends SIGTERM; without a handler the default disposition kills the
    process outright, leaving a half-written run and an undisposed connection pool.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            # Not every platform/loop supports it (Windows, a non-main thread). Nothing to do:
            # the default disposition still terminates the process, just less politely.
            log.info("signal handler unavailable", signal=sig.name)


async def serve() -> None:
    """Start the schedule and run until a signal arrives.

    Deliberately does nothing at startup beyond reading YAML: the first fetch of any connector
    happens at its next cron fire, never at boot. A restart is not a refresh — ``make refresh``
    (or ``cli.py refresh --connector NAME``) is how a run happens now. Nor does a restart replay
    a fire missed while the process was down (see :data:`MISFIRE_GRACE_SECONDS`).

    Shutdown is not immediate on purpose: a signal stops further fires and then waits, up to
    :data:`SHUTDOWN_DRAIN_SECONDS`, for the run in flight to write its ``fetch_runs`` row.
    """
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    config = load_connectors_config()
    scheduler = build_scheduler(config)
    log_schedule(scheduler, settings=settings, config=config)
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    scheduler.start()
    log.info("scheduler started")
    try:
        await stop.wait()
    finally:
        log.info("scheduler stopping")
        # This order is load-bearing. ``pause`` stops further fires without touching the run in
        # flight; :func:`drain_runs` then waits for that run to finalize its ``fetch_runs`` row;
        # only then is the scheduler torn down. ``shutdown(wait=True)`` on its own would do none
        # of that: ``AsyncIOScheduler.shutdown`` defers its work through ``call_soon_threadsafe``
        # so it returns before anything has stopped, and ``AsyncIOExecutor.shutdown`` ignores
        # ``wait`` and cancels every running job outright (its own comment: "There is no way to
        # honor wait=True"). The ``await dispose_engine()`` below would then run concurrently
        # with that cancellation and kill the run's finalizing write as it re-opened a connection.
        scheduler.pause()
        await drain_runs()
        scheduler.shutdown(wait=False)
        # The engine (if a failure-streak check ever created one) belongs to this loop; asyncpg
        # pools must be closed on the loop that opened them.
        await dispose_engine()


def _format_config_error(exc: ValidationError) -> str:
    """Every invalid entry on one line, each named by its path in the YAML.

    Deliberately not :func:`cli.format_settings_error`: that one upper-cases the location into an
    environment variable name, which is right for ``Settings`` and wrong here — it would report
    ``CONNECTORS_GREENHOUSE_CADENCE``, naming a variable that does not exist.
    """
    problems: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        problems.append(f"{location}: {error['msg'].removeprefix('Value error, ')}")
    return "; ".join(problems)


def main() -> None:
    """Sync entry point — what the ``scheduler`` Compose service runs (``python scheduler.py``).

    Both things the process must read before it can do anything are validated here rather than
    inside :func:`serve`, for one reason: the service is ``restart: unless-stopped``, so a bad
    ``.env`` or a bad ``config/connectors.yaml`` restart-loops, and it should say which entry is
    wrong on one line rather than print a pydantic traceback every few seconds. Both loaders are
    ``@cache``d, so :func:`serve` re-reading them costs nothing.
    """
    try:
        get_settings()
    except ValidationError as exc:
        # Logging is not configured yet (it is configured *from* these settings), so this goes
        # straight to stderr — the same stream every log line would have used.
        print(
            f"error: invalid settings (environment or .env): {format_settings_error(exc)}",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    try:
        load_connectors_config()
    except ValidationError as exc:
        print(
            f"error: invalid connector configuration ({CONNECTORS_YAML}): "
            f"{_format_config_error(exc)}",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    asyncio.run(serve())


if __name__ == "__main__":
    main()
