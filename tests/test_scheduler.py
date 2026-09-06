"""The scheduler service (SPEC §7.2, §11, §3's third Compose service).

The subject is a process that mostly waits, so nothing here starts a scheduler, sleeps, or looks
at a wall clock: :func:`scheduler.build_scheduler` returns a *configured but unstarted* object and
:func:`scheduler.next_fire_times` asks the trigger rather than the job, which together let a test
assert the whole schedule — ids, cron fields, and the exact instant each connector next fires —
from a pinned ``now``.

Two properties get the most attention because they are the ones that would break silently:

* the schedule comes from ``config/connectors.yaml`` and nowhere else (CLAUDE.md), checked by
  handing ``build_scheduler`` a *different* config and watching the trigger change; and
* the cadences the file actually contains are the cadences SPEC §7.2's table names, on the right
  day of the week — APScheduler 3.x numbers weekdays Monday=0, so a numeric ``* * 6`` would fire
  on Sunday and no test that only re-read the string would notice.

Nothing here touches the network: the job body is exercised with ``cli.run_one`` monkeypatched,
which is the seam that makes a scheduled run and ``cli.py refresh --connector NAME`` the same
code path in the first place.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator, MutableMapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import structlog

import cli
import scheduler
from db import enums
from db.models import FetchRun
from db.queries import CONSECUTIVE_FAILURE_ALERT
from ingest.config import ConnectorConfig, ConnectorsConfig, load_connectors_config
from ingest.connectors import all_connectors

#: A Saturday, so a weekly cadence's next fire is unambiguous in either direction.
NOW = datetime(2026, 9, 5, 19, 0, tzinfo=UTC)

#: SPEC §7.2's cadence table, as the day and time each connector must actually next fire from
#: :data:`NOW`. Written as instants rather than as cron strings on purpose: re-stating the
#: expression would only prove the YAML was copied correctly, while an instant proves the parser
#: agrees with the table — which is exactly where the Monday=0 wart bites.
SPEC_NEXT_FIRE = {
    "greenhouse": datetime(2026, 9, 6, 6, tzinfo=UTC),  # daily 06:00
    "lever": datetime(2026, 9, 6, 6, tzinfo=UTC),
    "ashby": datetime(2026, 9, 6, 6, tzinfo=UTC),
    "workable": datetime(2026, 9, 6, 6, tzinfo=UTC),
    "hn_hiring": datetime(2026, 10, 2, 9, tzinfo=UTC),  # monthly, the 2nd at 09:00
    "sec_edgar": datetime(2026, 9, 7, 5, tzinfo=UTC),  # every 3 days 05:00 (day-of-month step)
    "ycombinator": datetime(2026, 9, 6, 4, tzinfo=UTC),  # weekly, SUNDAY 04:00
    "funding_rss": datetime(2026, 9, 6, 7, tzinfo=UTC),  # daily 07:00
    "company_site": datetime(2026, 9, 12, 3, tzinfo=UTC),  # weekly, SATURDAY 03:00
}


@pytest.fixture(autouse=True)
def logs() -> Iterator[list[MutableMapping[str, Any]]]:
    """Every structlog entry this test emits, as a dict of ``event`` plus its bound keys.

    ``capture_logs`` replaces the processor chain rather than the logger factory, so what a
    test sees is the *structured* record — the keys the scheduler bound — instead of a rendered
    console line. It also keeps structlog's default stdout rendering out of the test output,
    which matters here: the scheduler logs on every path this module exercises, and two of
    them (the startup report and SPEC §7.2's failure alert) are the assertion itself.
    """
    with structlog.testing.capture_logs() as entries:
        yield entries


@pytest.fixture(autouse=True)
def fresh_run_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """A per-test :data:`scheduler._RUN_LOCK`, because the module-level one outlives its loop.

    ``asyncio.Lock`` binds itself to an event loop on its first *contended* acquire and raises
    ``RuntimeError`` on every later contention from a different loop. The service has one loop
    for the life of the process, so this never bites in production — but each test here runs its
    own ``asyncio.run``, so the first overlap test would bind the module-level lock to a loop
    that is then closed and every later one would find its run silently swallowed by
    ``run_scheduled``'s ``except Exception``. ``run_scheduled`` looks the lock up as a module
    global on every fire, so replacing it per test is enough, and monkeypatch's restore leaves
    the real one unbound for any other module that imports ``scheduler``.
    """
    monkeypatch.setattr(scheduler, "_RUN_LOCK", asyncio.Lock())


def config_of(**blocks: ConnectorConfig) -> ConnectorsConfig:
    """A ``ConnectorsConfig`` holding exactly the blocks given, in the order given."""
    return ConnectorsConfig(connectors=dict(blocks))


def block(name: str, **overrides: Any) -> ConnectorConfig:
    return ConnectorConfig(name=name, **overrides)


def jobs(built: Any) -> dict[str, Any]:
    return {job.id: job for job in built.get_jobs()}


def cron_fields(job: Any) -> dict[str, str]:
    """The trigger's cron fields as ``{name: expression}`` — what the schedule *is*, without
    depending on APScheduler's ``str()`` formatting."""
    return {field.name: str(field) for field in job.trigger.fields}


# ------------------------------------------------------ the schedule that is actually built


def test_the_schedule_is_the_configured_connectors_that_can_run() -> None:
    """A connector gets a job when it has an implementation, is ``enabled``, and has a cadence.

    No name is hard-coded in ``scheduler.py``: ``product_hunt`` and ``opencorporates`` fall out
    of the registry because nothing implements them, and ``seed`` because SPEC §10's bootstrap
    is run by ``cli.py seed`` rather than on a cadence.
    """
    built = jobs(scheduler.build_scheduler())

    configured = load_connectors_config().connectors
    expected = {
        name
        for name, config in configured.items()
        if name in all_connectors() and config.enabled and config.cadence is not None
    }
    assert set(built) == expected
    assert expected == set(SPEC_NEXT_FIRE)
    # Every job is named for its connector, so a log line or an APScheduler event identifies it.
    assert all(job.name == name for name, job in built.items())


def test_every_cadence_fires_when_spec_7_2_says_it_does() -> None:
    """SPEC §7.2's table, asserted as instants rather than as cron strings.

    This is the test that catches the APScheduler 3.x weekday numbering: ``0 4 * * 0`` parses
    happily and re-reads as "Sunday" to a human, but fires on a *Monday*, because from_crontab
    numbers weekdays Monday=0. Only comparing the computed fire time against the day the SPEC
    names can tell the difference — hence the day names in ``config/connectors.yaml``.
    """
    fires = scheduler.next_fire_times(scheduler.build_scheduler(), now=NOW)

    assert fires == SPEC_NEXT_FIRE
    # Spelled out, because these two are the ones the weekday numbering gets wrong.
    assert SPEC_NEXT_FIRE["ycombinator"].strftime("%A") == "Sunday"
    assert SPEC_NEXT_FIRE["company_site"].strftime("%A") == "Saturday"


def test_every_trigger_is_utc() -> None:
    """SPEC §7.2: cron is evaluated in UTC. A scheduler that took the container's local zone
    would drift by an hour twice a year and nothing would say so."""
    built = scheduler.build_scheduler()

    assert str(built.timezone) == "UTC"
    assert all(str(job.trigger.timezone) == "UTC" for job in built.get_jobs())


def test_the_cadence_comes_from_the_config_object_and_nowhere_else() -> None:
    """Hand ``build_scheduler`` a different config and the trigger has to change with it.

    This is the CLAUDE.md rule ("config/connectors.yaml is the only place a schedule is
    defined") stated as a behaviour: if the module held a cadence of its own — a default, a
    fallback, a hard-coded ATS group — patching the config would leave the trigger alone.
    """
    patched = config_of(greenhouse=block("greenhouse", cadence="7 1 * * *"))
    built = jobs(scheduler.build_scheduler(patched))

    assert set(built) == {"greenhouse"}
    fields = cron_fields(built["greenhouse"])
    assert (fields["hour"], fields["minute"]) == ("1", "7")
    assert scheduler.next_fire_times(scheduler.build_scheduler(), now=NOW) == SPEC_NEXT_FIRE


@pytest.mark.parametrize(
    ("name", "config", "reason"),
    [
        ("product_hunt", {"cadence": "0 5 * * sun"}, scheduler.SKIP_NOT_IMPLEMENTED),
        ("seed", {"cadence": "0 5 * * sun"}, scheduler.SKIP_NOT_REGISTERED),
        ("greenhouse", {"cadence": "0 6 * * *", "enabled": False}, scheduler.SKIP_DISABLED),
        ("greenhouse", {"cadence": None}, scheduler.SKIP_ON_DEMAND),
    ],
)
def test_a_connector_that_gets_no_job_says_why(
    name: str, config: dict[str, Any], reason: str, logs: list[MutableMapping[str, Any]]
) -> None:
    """ "Nothing is scheduled" must never be a silent state: each of the four reasons is one
    INFO line naming the connector, so an operator reading the startup log can tell an
    unimplemented connector from one someone switched off.

    ``product_hunt`` and ``seed`` are both absent from the registry and are told apart on
    purpose: nothing will ever make ``product_hunt`` run, while ``seed`` is implemented and runs
    on demand as ``cli.py seed``. One shared message would send the reader hunting for a bug in
    whichever half they were not thinking of.
    """
    built = scheduler.build_scheduler(config_of(**{name: block(name, **config)}))

    assert built.get_jobs() == []
    skipped = [entry for entry in logs if entry["event"] == "connector not scheduled"]
    assert [(entry["connector"], entry["reason"]) for entry in skipped] == [(name, reason)]


def test_an_unimplemented_connector_is_reported_as_unimplemented_not_on_demand(
    logs: list[MutableMapping[str, Any]],
) -> None:
    """``opencorporates`` is both: no implementation *and* ``cadence: null``. The more
    fundamental fact is the one worth reporting, so the checks are ordered.

    The *reason* is the assertion: reporting it as merely on-demand would tell an operator
    grepping the startup log that an implementation exists and just needs a manual run.
    """
    built = scheduler.build_scheduler()

    assert "opencorporates" not in jobs(built)
    reasons = {
        entry["connector"]: entry["reason"]
        for entry in logs
        if entry["event"] == "connector not scheduled"
    }
    assert reasons["opencorporates"] == scheduler.SKIP_NOT_IMPLEMENTED


def test_job_options_keep_a_slow_run_from_overlapping_or_stampeding() -> None:
    """``max_instances=1`` (a slow run is never overlapped by its own next fire), ``coalesce``
    (a backlog of missed fires collapses to one) and the misfire grace time (how late that
    survivor may still be) are the three settings that decide what a restart does."""
    job = jobs(scheduler.build_scheduler())["greenhouse"]

    assert job.max_instances == 1
    assert job.coalesce is True
    assert job.misfire_grace_time == scheduler.MISFIRE_GRACE_SECONDS


def test_building_the_scheduler_touches_no_database(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unmigrated or unreachable database must not stop the service coming up — the runs it
    schedules are how the database gets populated in the first place."""

    def explode() -> None:
        raise AssertionError("build_scheduler must not open a database connection")

    monkeypatch.setattr(scheduler, "get_session_factory", explode)

    assert jobs(scheduler.build_scheduler())


# --------------------------------------------------------------------- the startup report


def test_the_startup_log_names_every_scheduled_connector_with_its_next_fire(
    logs: list[MutableMapping[str, Any]],
) -> None:
    """The line an operator reads to answer "did it pick up my edit, and when does it run?"."""
    config = load_connectors_config()
    built = scheduler.build_scheduler(config)

    settings = _settings(contact_email="dev@example.com")
    scheduler.log_schedule(built, settings=settings, config=config, now=NOW)

    loaded = [entry for entry in logs if entry["event"] == "schedule loaded"]
    assert len(loaded) == 1
    listed = {entry["connector"]: entry for entry in loaded[0]["jobs"]}
    assert set(listed) == set(SPEC_NEXT_FIRE)
    assert listed["ycombinator"]["cadence"] == config.get("ycombinator").cadence
    assert listed["company_site"]["next_fire_time"] == SPEC_NEXT_FIRE["company_site"].isoformat()
    assert loaded[0]["timezone"] == "UTC"


def test_a_connector_needing_a_contact_email_is_warned_about_but_still_scheduled(
    logs: list[MutableMapping[str, Any]],
) -> None:
    """SPEC §4: SEC's fair-access policy needs an address in the User-Agent. Unscheduling the
    connector would hide the problem; a nightly ``error`` row carrying the reason is the
    visible-breakage design, so the warning is the whole intervention."""
    config = load_connectors_config()
    built = scheduler.build_scheduler(config)

    scheduler.log_schedule(built, settings=_settings(contact_email=None), config=config, now=NOW)

    warnings = [
        entry for entry in logs if entry["event"] == "connector scheduled without a contact email"
    ]
    assert [entry["connector"] for entry in warnings] == sorted(cli.CONTACT_REQUIRED)
    assert cli.CONTACT_EMAIL_VAR in warnings[0]["detail"]
    assert "sec_edgar" in jobs(built)


def test_no_contact_warning_when_the_address_is_set(logs: list[MutableMapping[str, Any]]) -> None:
    config = load_connectors_config()

    scheduler.log_schedule(
        scheduler.build_scheduler(config),
        settings=_settings(contact_email="dev@example.com"),
        config=config,
        now=NOW,
    )

    assert not [entry for entry in logs if entry["event"].startswith("connector scheduled")]


def _settings(*, contact_email: str | None) -> Any:
    """The two attributes ``log_schedule`` reads, without building a whole ``Settings``."""

    class Stub:
        pass

    stub = Stub()
    stub.contact_email = contact_email  # type: ignore[attr-defined]
    return stub


# ------------------------------------------------------------------------ the job body


class FakeRun:
    """Enough of a ``FetchRun`` for ``run_scheduled`` to log and branch on."""

    def __init__(self, status: enums.FetchRunStatus) -> None:
        self.status = status
        self.n_fetched = 3
        self.n_upserted = 2


def patch_run_one(monkeypatch: pytest.MonkeyPatch, result: Any) -> list[dict[str, Any]]:
    """Replace ``scheduler.run_one`` and record what it was called with.

    ``run_one`` is ``cli.run_one`` — the seam that makes a scheduled run and
    ``cli.py refresh --connector NAME`` the same code path. Patching it is also what keeps this
    module off the network: everything below the seam is the CLI's, and it is tested there.
    """
    calls: list[dict[str, Any]] = []

    async def fake(connector: Any, config: Any, *, settings: Any, since: Any) -> Any:
        calls.append(
            {
                "connector": connector,
                "config": config,
                "since": since,
                "locked": scheduler._RUN_LOCK.locked(),
            }
        )
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(scheduler, "run_one", fake)
    return calls


def test_the_job_body_runs_the_named_connector_under_the_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The connector is rebuilt from the YAML on every fire, run through ``cli.run_one``, and
    the module-level lock is held while it runs.

    The lock is what keeps two 06:00 ATS runs from hitting the same careers host inside SPEC
    §4's one-request-per-domain-per-2s window, since rate limiting is per ``HttpClient`` and
    each run builds its own.
    """
    calls = patch_run_one(monkeypatch, FakeRun(enums.FetchRunStatus.OK))

    asyncio.run(scheduler.run_scheduled("greenhouse"))

    assert len(calls) == 1
    assert calls[0]["connector"].name == "greenhouse"
    assert calls[0]["config"] is load_connectors_config().get("greenhouse")
    # No incremental override: the pipeline's own window decides (SPEC §7.2).
    assert calls[0]["since"] is None
    assert calls[0]["locked"] is True
    # ...and released afterwards, or the next fire would deadlock the whole schedule.
    assert scheduler._RUN_LOCK.locked() is False


def test_two_runs_do_not_overlap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Serialization, observed rather than asserted about the lock object: the second run
    cannot start until the first has finished."""
    events: list[str] = []

    async def fake(connector: Any, config: Any, *, settings: Any, since: Any) -> Any:
        events.append(f"start {connector.name}")
        await asyncio.sleep(0)
        events.append(f"end {connector.name}")
        return FakeRun(enums.FetchRunStatus.OK)

    monkeypatch.setattr(scheduler, "run_one", fake)

    async def both() -> None:
        await asyncio.gather(
            scheduler.run_scheduled("greenhouse"), scheduler.run_scheduled("lever")
        )

    asyncio.run(both())

    # All four events, or the test would also pass on a run that never happened at all.
    assert len(events) == 4
    assert events[0].startswith("start")
    assert events[1] == events[0].replace("start", "end")
    assert events[2].startswith("start") and events[2] != events[0]
    assert events[3] == events[2].replace("start", "end")


def test_an_exception_in_a_run_never_escapes_the_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """``run_connector`` writes the ``fetch_runs`` row itself and folds connector, record and
    refresh failures into it, so an exception reaching here means the row could not be written
    at all — the database is down. A job that raised would take its next fires with it, so it
    is logged with the same ``not-recorded`` token ``cli.py refresh`` prints and swallowed."""
    patch_run_one(monkeypatch, RuntimeError("database is gone"))

    asyncio.run(scheduler.run_scheduled("greenhouse"))

    assert scheduler._RUN_LOCK.locked() is False


def test_an_unknown_connector_name_is_swallowed_too(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nothing schedules a name that is not in the registry, but a job restored from a stale
    store would; it must not kill the scheduler either."""
    asyncio.run(scheduler.run_scheduled("not_a_connector"))


# ----------------------------------------------------- SPEC §7.2's ERROR line at 3 failures


#: Fixed rather than "now", so the runs a test builds are ordered by the offsets it chooses
#: instead of by how long the fixtures took to construct.
RUN_EPOCH = datetime(2026, 9, 5, 12, tzinfo=UTC)


def make_runs(
    connector: str, count: int, *, status: enums.FetchRunStatus, minutes_ago: int = 0
) -> list[FetchRun]:
    """``count`` finished runs, oldest first, one minute apart, ending ``minutes_ago`` before
    :data:`RUN_EPOCH` — ``finished_at`` set, since an unfinished run is excluded from the
    streak entirely."""
    return [
        FetchRun(
            connector=connector,
            started_at=RUN_EPOCH - timedelta(minutes=minutes_ago + index),
            finished_at=RUN_EPOCH - timedelta(minutes=minutes_ago + index),
            status=status,
            n_fetched=0,
            n_upserted=0,
        )
        for index in reversed(range(count))
    ]


@pytest.mark.parametrize(
    ("failures", "alerts"),
    [(CONSECUTIVE_FAILURE_ALERT - 1, 0), (CONSECUTIVE_FAILURE_ALERT, 1)],
)
async def test_the_error_line_fires_at_the_threshold_and_not_below(
    failures: int,
    alerts: int,
    session_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
    logs: list[MutableMapping[str, Any]],
    session: Any,
) -> None:
    """SPEC §7.2: "A connector that fails 3 consecutive runs should log at ERROR". Two failures
    is a bad night and stays at INFO; the third is the line an operator is meant to see."""
    for run in make_runs("greenhouse", failures, status=enums.FetchRunStatus.ERROR):
        session.add(run)
    await session.commit()
    monkeypatch.setattr(scheduler, "get_session_factory", lambda: session_factory)

    await scheduler.alert_on_failure_streak("greenhouse")

    failing = [entry for entry in logs if entry["event"] == "connector failing"]
    assert len(failing) == alerts
    if alerts:
        assert failing[0]["log_level"] == "error"
        assert failing[0]["consecutive"] == CONSECUTIVE_FAILURE_ALERT
        assert failing[0]["threshold"] == CONSECUTIVE_FAILURE_ALERT


async def test_a_partial_run_ends_the_streak_so_nothing_is_alerted(
    session_factory: Any,
    monkeypatch: pytest.MonkeyPatch,
    logs: list[MutableMapping[str, Any]],
    session: Any,
) -> None:
    """A ``partial`` run completed its scan, so the source is reachable and the streak is
    broken — the same reading the incremental window uses. Alerting anyway would teach an
    operator to ignore the line."""
    # A full streak, then a partial run *on top of* it — the newest finished run of the three.
    session.add_all(
        make_runs(
            "greenhouse",
            CONSECUTIVE_FAILURE_ALERT,
            status=enums.FetchRunStatus.ERROR,
            minutes_ago=1,
        )
    )
    session.add_all(make_runs("greenhouse", 1, status=enums.FetchRunStatus.PARTIAL))
    await session.commit()
    monkeypatch.setattr(scheduler, "get_session_factory", lambda: session_factory)

    await scheduler.alert_on_failure_streak("greenhouse")

    assert not [entry for entry in logs if entry["event"] == "connector failing"]


async def test_a_failing_streak_query_is_logged_and_dropped(
    monkeypatch: pytest.MonkeyPatch, logs: list[MutableMapping[str, Any]]
) -> None:
    """An ops signal about a run that is already recorded must not become a second failure."""

    def explode() -> Any:
        raise RuntimeError("database is gone")

    monkeypatch.setattr(scheduler, "get_session_factory", explode)

    await scheduler.alert_on_failure_streak("greenhouse")

    assert [entry["event"] for entry in logs] == ["consecutive-failure check failed"]


def test_a_successful_run_never_spends_the_streak_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """The alert is only worth asking about after a run that itself recorded ``error``: an
    ``ok`` or ``partial`` run has already reset the streak to zero."""
    patch_run_one(monkeypatch, FakeRun(enums.FetchRunStatus.OK))

    async def explode(name: str) -> None:
        raise AssertionError("the streak must not be queried after a successful run")

    monkeypatch.setattr(scheduler, "alert_on_failure_streak", explode)

    asyncio.run(scheduler.run_scheduled("greenhouse"))


def test_an_error_run_does_ask_for_the_streak(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[str] = []
    patch_run_one(monkeypatch, FakeRun(enums.FetchRunStatus.ERROR))

    async def record(name: str) -> None:
        asked.append(name)

    monkeypatch.setattr(scheduler, "alert_on_failure_streak", record)

    asyncio.run(scheduler.run_scheduled("greenhouse"))

    assert asked == ["greenhouse"]
