"""``/runs``'s consecutive-failure surface (SPEC §7.2, §9).

The page carries two health signals per connector and they answer different questions, so the
tests here are mostly about keeping them apart:

* **Stale** (SPEC §9) is about *silence* — no ``ok`` run in over twice the configured cadence.
  It is cadence-derived, so a connector with ``cadence: null`` can never be stale.
* **Failing** (SPEC §7.2) is about *noise* — the last
  :data:`~db.queries.CONSECUTIVE_FAILURE_ALERT` finished runs all ended in ``error``. Nothing
  about it depends on a cadence, so an on-demand connector can be failing.

The asymmetry a reader meets on this page gets its own test: a ``partial`` run ends a failure
streak (it completed its scan) but is *not* a success (:func:`db.queries.last_successful_runs`
counts ``ok`` alone), so one run can flip "failing" to "not failing" while leaving "last ok" —
and therefore "stale" — exactly where it was.

Everything is asserted against the **rendered HTML** the way ``tests/test_web_routes.py`` does:
the row's modifier classes are styling, but the words *Stale* and *Failing* are the meaning for
a reader with styles off or a screen reader, and a test that only inspected
:func:`~web.routes.runs.connector_health` would not notice them disappearing. Rows are read with
selectolax rather than a page-wide substring search so "the word Failing is on *that* row" is
what is actually checked.

Seeded rows are **committed**: the handler reads on its own session (SPEC §2 — it touches
nothing but the local database). Nothing here goes near the network.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from selectolax.parser import HTMLParser
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.queries import CONSECUTIVE_FAILURE_ALERT
from ingest.config import load_connectors_config
from tests.support_web import DAY, app_client, make_run
from web.routes.runs import RUN_HISTORY, connector_health

#: SPEC §7.2's "fails 3 consecutive runs", read from the constant both signals share so the
#: boundary tests move with the threshold instead of hard-coding one side of it.
THRESHOLD = CONSECUTIVE_FAILURE_ALERT
#: One short of it — the value that must stay quiet.
BELOW = CONSECUTIVE_FAILURE_ALERT - 1

#: Long enough that any *daily* connector is stale (SPEC §9: over twice its cadence) and short
#: enough to stay inside the `ago` filter's relative-date range.
LONG_AGO = 9 * DAY


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[httpx.AsyncClient]:
    async with app_client(session_factory) as http:
        yield http


# ------------------------------------------------------------------------------ page readers


@dataclass(frozen=True, slots=True)
class HealthRow:
    """One rendered ``.run-row`` health line: its modifier classes and its visible words."""

    connector: str
    classes: frozenset[str]
    text: str


def health_rows(markup: str) -> dict[str, HealthRow]:
    """Every health line of ``/runs``, keyed by connector, in the order the page renders them.

    ``div.run-row`` and not ``table.runs``: the page has two blocks and they share a class name
    on purpose (the stylesheet targets both), so a selector that caught the run *table* would
    quietly assert against the wrong half of the page.
    """
    rows: dict[str, HealthRow] = {}
    for node in HTMLParser(markup).css("div.run-row"):
        name = node.css_first("span")
        assert name is not None, "a health row with no connector name"
        connector = name.text().strip()
        rows[connector] = HealthRow(
            connector=connector,
            classes=frozenset((node.attributes.get("class") or "").split()),
            text=" ".join(node.text().split()),
        )
    return rows


def banner_text(markup: str) -> str | None:
    """The SPEC §7.2 banner as one whitespace-normalized line, or ``None`` when absent.

    Normalizing is the point: the sentence is written with Jinja whitespace control so it is
    announced as a single utterance, and a test that allowed newlines through would not notice
    it coming apart.
    """
    node = HTMLParser(markup).css_first("p.alert")
    if node is None:
        return None
    assert node.attributes.get("role") == "status", "the banner must be a live region"
    return " ".join(node.text().split())


def run_table_cells(markup: str) -> list[list[str]]:
    """The cells of every row of SPEC §9's "last 100 runs" table, newest first."""
    table = HTMLParser(markup).css_first("table.runs")
    if table is None:
        return []
    return [
        [" ".join(cell.text().split()) for cell in row.css("td")] for row in table.css("tbody tr")
    ]


# ------------------------------------------------------------------------------ row factories


async def make_streak(
    session: AsyncSession,
    connector: str,
    *statuses: enums.FetchRunStatus,
    now: datetime,
    step: timedelta = timedelta(hours=1),
) -> None:
    """Write one *finished* run per status, oldest first, the last of them ``step`` before ``now``.

    ``finished_at`` is always set: an unfinished run reads ``status='error'`` from the column
    default and is deliberately excluded from the streak, so leaving it ``None`` would seed a
    history that says something other than what it looks like.
    """
    for offset, status in enumerate(reversed(statuses), start=1):
        started = now - offset * step
        await make_run(
            session,
            connector,
            started_at=started,
            finished_at=started + timedelta(seconds=30),
            status=status,
            error_text=run_marker(connector, started),
        )


def run_marker(connector: str, started: datetime) -> str:
    """A per-run ``error_text`` unique across a streak, so a test can name one row in the table.

    The date is in it deliberately: a streak longer than a day would otherwise repeat its
    ``%H:%M`` and an assertion about *which* run is on a table row could pass on a coincidence.
    """
    return f"{connector} failed at {started:%Y-%m-%d %H:%M}"


async def make_success(session: AsyncSession, connector: str, *, at: datetime) -> None:
    """One finished ``ok`` run — the thing SPEC §9's staleness signal is looking for."""
    await make_run(
        session,
        connector,
        started_at=at,
        finished_at=at + timedelta(seconds=30),
        status=enums.FetchRunStatus.OK,
    )


# ------------------------------------------------------- the threshold (SPEC §7.2), in isolation


@pytest.mark.parametrize(
    ("streak", "expected"),
    [(0, False), (BELOW, False), (THRESHOLD, True), (THRESHOLD + 1, True)],
)
def test_connector_health_flags_failing_only_at_the_threshold(streak: int, expected: bool) -> None:
    """SPEC §7.2: "fails 3 consecutive runs" — three is failing, two is not, four still is.

    Both sides of the boundary, because ``>=`` and ``>`` are one keystroke apart and only one
    of them agrees with the ERROR line the scheduler logs.
    """
    now = datetime(2026, 9, 4, 12, tzinfo=UTC)

    health = {row.connector: row for row in connector_health({}, {"greenhouse": streak}, now=now)}

    assert health["greenhouse"].consecutive_failures == streak
    assert health["greenhouse"].failing is expected


def test_a_connector_with_no_streak_is_absent_from_the_mapping_not_zero_in_it() -> None:
    """:func:`db.queries.consecutive_failure_counts` omits healthy connectors; ``.get`` defaults.

    Also pins that the rows come from ``config/connectors.yaml`` and not from either mapping: a
    name that is not configured contributes nothing, however loudly it is failing.
    """
    now = datetime(2026, 9, 4, 12, tzinfo=UTC)

    rows = connector_health({}, {"not_a_connector": 99}, now=now)

    assert [row.connector for row in rows] == list(load_connectors_config().connectors)
    assert all(row.consecutive_failures == 0 for row in rows)
    assert not any(row.failing for row in rows)


# ---------------------------------------------------------------------- the banner, end to end


async def test_the_banner_is_absent_when_nothing_is_failing(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """A page that shouts on every render has stopped being a signal.

    Two runs short of the threshold is the interesting case: the streak exists, and the page
    still says nothing about it — below :data:`CONSECUTIVE_FAILURE_ALERT` there is no banner,
    no ``Failing`` word and no count on the row.
    """
    now = datetime.now(UTC)
    await make_streak(session, "greenhouse", *[enums.FetchRunStatus.ERROR] * BELOW, now=now)
    await make_success(session, "lever", at=now - timedelta(hours=2))
    await session.commit()

    body = (await client.get("/runs")).text
    rows = health_rows(body)

    assert banner_text(body) is None
    assert 'role="status"' not in body
    assert "Failing" not in body
    assert "failing" not in rows["greenhouse"].classes
    assert "consecutive errors" not in rows["greenhouse"].text


async def test_the_banner_names_every_failing_connector_with_its_streak(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """SPEC §7.2's "surface prominently": one line at the top, naming what broke and how badly.

    The sentence is asserted whole. It is what a reader quotes into a bug report, it is read
    aloud as one announcement, and its parts (the count, the plural, the threshold, each
    connector with its streak) are each capable of drifting on their own.
    """
    now = datetime.now(UTC)
    errors = [enums.FetchRunStatus.ERROR]
    await make_streak(session, "greenhouse", *errors * (THRESHOLD + 1), now=now)
    await make_streak(session, "lever", *errors * THRESHOLD, now=now)
    await make_streak(session, "ashby", *[enums.FetchRunStatus.ERROR] * BELOW, now=now)
    await make_success(session, "workable", at=now - timedelta(hours=2))
    await session.commit()

    body = (await client.get("/runs")).text

    # Config order, not "worst first": the banner reads down the same list as the rows below it.
    assert banner_text(body) == (
        f"2 connectors have failed {THRESHOLD} or more consecutive runs: "
        f"greenhouse ({THRESHOLD + 1}), lever ({THRESHOLD})."
    )
    banner = banner_text(body) or ""
    assert "ashby" not in banner
    assert "workable" not in banner


async def test_the_word_failing_is_on_the_failing_row_and_only_there(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """The class is styling; the word is the meaning with styles off or a screen reader on.

    Asserted per row rather than page-wide, so moving the word onto the wrong line — or leaving
    it on every line — fails here.
    """
    now = datetime.now(UTC)
    await make_streak(session, "greenhouse", *[enums.FetchRunStatus.ERROR] * THRESHOLD, now=now)
    await make_success(session, "lever", at=now - timedelta(hours=2))
    await session.commit()

    rows = health_rows((await client.get("/runs")).text)

    assert "failing" in rows["greenhouse"].classes
    assert "Failing" in rows["greenhouse"].text
    assert f"{THRESHOLD} consecutive errors" in rows["greenhouse"].text
    assert "Failing" not in rows["lever"].text
    assert [name for name, row in rows.items() if "Failing" in row.text] == ["greenhouse"]


async def test_a_row_can_be_both_stale_and_failing_and_says_both_words(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """The two signals are independent, and the page must not let one hide the other.

    ``greenhouse`` has been silent for nine days *and* is failing now; ``lever`` succeeded this
    morning and broke afterwards. Both are failing; only one is stale. A page that rendered a
    single "unhealthy" state would lose the difference between "nobody has heard from this
    source in a week" and "this source broke this morning".

    ``lever``'s success is placed *below* its whole streak, not merely in the recent past: a
    success interleaved with the errors would end the streak at it, which is the streak
    definition working correctly and would make this test measure the wrong thing.
    """
    now = datetime.now(UTC)
    await make_success(session, "greenhouse", at=now - LONG_AGO)
    await make_streak(session, "greenhouse", *[enums.FetchRunStatus.ERROR] * THRESHOLD, now=now)
    await make_success(session, "lever", at=now - timedelta(hours=12))
    await make_streak(session, "lever", *[enums.FetchRunStatus.ERROR] * THRESHOLD, now=now)
    await session.commit()

    rows = health_rows((await client.get("/runs")).text)

    assert rows["greenhouse"].classes == frozenset({"run-row", "stale", "failing"})
    assert "Stale" in rows["greenhouse"].text
    assert "Failing" in rows["greenhouse"].text
    assert rows["lever"].classes == frozenset({"run-row", "failing"})
    assert "Stale" not in rows["lever"].text
    assert "Failing" in rows["lever"].text


async def test_a_partial_run_breaks_the_streak_without_counting_as_a_success(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """The asymmetry a reader meets on this page, in one comparison.

    Both connectors have the same history — a success nine days ago, then a run of errors — and
    differ by exactly one run on top: ``funding_rss`` ends in ``partial``, ``greenhouse`` in
    another ``error``. A ``partial`` scan completed, so it ends the failure streak
    (:func:`db.queries.consecutive_failure_counts` treats ``ok`` and ``partial`` alike); it is
    not a *success*, so it does not move ``last_ok`` (:func:`db.queries.last_successful_runs`
    counts ``ok`` alone) and the connector stays stale.

    That is why "last ok 9d ago" and no failure streak can sit on one row and both be true: the
    connector is limping, not broken. Collapsing the two definitions into one — in either
    direction — flips one of these four assertions.
    """
    now = datetime.now(UTC)
    errors = [enums.FetchRunStatus.ERROR] * THRESHOLD
    await make_success(session, "funding_rss", at=now - LONG_AGO)
    await make_streak(session, "funding_rss", *errors, enums.FetchRunStatus.PARTIAL, now=now)
    await make_success(session, "greenhouse", at=now - LONG_AGO)
    await make_streak(session, "greenhouse", *errors, enums.FetchRunStatus.ERROR, now=now)
    await session.commit()

    body = (await client.get("/runs")).text
    rows = health_rows(body)

    # The partial ended the streak ...
    assert "failing" not in rows["funding_rss"].classes
    assert "Failing" not in rows["funding_rss"].text
    # ... and did not count as a success, so the §9 silence signal still fires.
    assert "stale" in rows["funding_rss"].classes
    assert "Stale" in rows["funding_rss"].text
    # The control: one more error instead of the partial, and the same history is failing.
    assert rows["greenhouse"].classes == frozenset({"run-row", "stale", "failing"})
    assert f"{THRESHOLD + 1} consecutive errors" in rows["greenhouse"].text
    assert banner_text(body) == (
        f"1 connector has failed {THRESHOLD} or more consecutive runs: "
        f"greenhouse ({THRESHOLD + 1})."
    )


async def test_an_on_demand_connector_can_be_failing_though_it_can_never_be_stale(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """``cadence: null`` removes the §9 signal, not the §7.2 one.

    Nothing schedules ``seed``, so there is no schedule for it to have missed and it is never
    stale — but the runs it *did* have (``cli.py seed``, or ``refresh --connector``) failed, and
    a hand-run connector that fails every time is exactly as broken as a scheduled one. This is
    also the singular form of the banner sentence.
    """
    now = datetime.now(UTC)
    await make_streak(session, "seed", *[enums.FetchRunStatus.ERROR] * THRESHOLD, now=now)
    await session.commit()

    body = (await client.get("/runs")).text
    rows = health_rows(body)

    assert rows["seed"].classes == frozenset({"run-row", "failing"})
    assert "on demand" in rows["seed"].text
    assert "Stale" not in rows["seed"].text
    assert "Failing" in rows["seed"].text
    assert banner_text(body) == (
        f"1 connector has failed {THRESHOLD} or more consecutive runs: seed ({THRESHOLD})."
    )
    # The other on-demand connector never ran: neither signal, and still listed and marked.
    assert rows["opencorporates"].classes == frozenset({"run-row"})
    assert "not implemented" in rows["opencorporates"].text


async def test_the_page_renders_on_an_empty_database(client: httpx.AsyncClient) -> None:
    """No runs at all is the first thing a fresh checkout sees, and it must not be an error.

    Nothing can be failing without a run to fail, so the banner stays away; every configured
    connector is still listed, in config order, because a connector that has never run is the
    case SPEC §9 built this page for.
    """
    response = await client.get("/runs")

    assert response.status_code == 200
    body = response.text
    rows = health_rows(body)

    assert banner_text(body) is None
    assert "Failing" not in body
    assert list(rows) == list(load_connectors_config().connectors)
    assert not any("failing" in row.classes for row in rows.values())
    # Never succeeded plus a cadence is stale; a cadence of null still is not.
    assert "stale" in rows["greenhouse"].classes
    assert "stale" not in rows["seed"].classes
    assert "stale" not in rows["opencorporates"].classes
    assert run_table_cells(body) == []
    assert "No runs yet" in body


async def test_the_last_100_runs_table_is_unaffected_by_the_failure_signal(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """SPEC §9's table still shows the newest ``RUN_HISTORY`` rows, and the streak ignores its cap.

    The two halves of the page read the same table with different windows, and this is where
    they could quietly be conflated: the streak is computed over *every* finished run, so a
    connector that has been failing longer than the table is deep still reports the true count.
    """
    now = datetime.now(UTC)
    total = RUN_HISTORY + 2
    await make_streak(session, "greenhouse", *[enums.FetchRunStatus.ERROR] * total, now=now)
    await session.commit()

    body = (await client.get("/runs")).text
    cells = run_table_cells(body)

    assert len(cells) == RUN_HISTORY
    assert all(row[0] == "greenhouse" for row in cells)
    assert all(row[4] == "Error" for row in cells)
    # Newest first, and the two oldest runs fall off the bottom rather than the top.
    assert run_marker("greenhouse", now - timedelta(hours=1)) in cells[0][7]
    assert run_marker("greenhouse", now - timedelta(hours=RUN_HISTORY)) in cells[-1][7]
    assert run_marker("greenhouse", now - timedelta(hours=total)) not in body
    # ... while the streak counts all of them, including the runs the table could not show.
    assert f"{total} consecutive errors" in health_rows(body)["greenhouse"].text
