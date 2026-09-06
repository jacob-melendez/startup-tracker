"""``/runs`` — ingestion health (SPEC §9, §7.2).

Two things on one page: the last 100 ``fetch_runs`` rows, and a health line per *configured*
connector. The second matters more. A connector that quietly stops returning anything writes no
row at all, so a list of runs can look perfectly healthy while a source has been dead for a
month — the whole point of §9's "highlight any connector with no successful run in over twice
its cadence" is to make that absence visible.

Each health line carries two independent signals, and they answer different questions:

* **Stale** — §9's cadence signal: no ``ok`` run in over twice the configured cadence. It is
  about *silence*, so a connector that has never run at all is stale, and an on-demand
  connector (``cadence: null``) never is.
* **Failing** — §7.2's "a connector that fails 3 consecutive runs should log at ERROR and
  surface prominently on ``/runs``". It is about *noise*: runs are happening and ending in
  ``error``. The scheduler emits the ERROR line; this module is the "surface prominently" half,
  as a banner above the list plus a word on the row itself.

The two can disagree in both directions, which is why both are shown — see
:func:`connector_health`.

The cadence comes from ``config/connectors.yaml`` through :mod:`ingest.config`, which is the
only place a schedule is defined (CLAUDE.md). That import is local-file-only; nothing in this
module fetches anything (SPEC §2).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter
from sqlalchemy import select
from starlette.requests import Request
from starlette.responses import HTMLResponse, Response

from db import queries
from db.models import FetchRun
from ingest.config import UNIMPLEMENTED_CONNECTORS, cadence_period, load_connectors_config
from web.deps import SessionDep
from web.templating import templates

router = APIRouter()

#: SPEC §9: "Last 100 ``FetchRun`` rows".
RUN_HISTORY = 100
#: SPEC §9: "no successful run in over twice its cadence".
STALE_CADENCE_MULTIPLE = 2


@dataclass(frozen=True, slots=True)
class ConnectorHealth:
    """One row of the ``/runs`` health summary."""

    connector: str
    #: The cron expression from ``config/connectors.yaml``; ``None`` = on-demand only.
    cadence: str | None
    #: The nominal interval between two firings of that cadence.
    period: timedelta | None
    last_ok: datetime | None
    stale: bool
    enabled: bool
    implemented: bool
    #: How many of this connector's most recent *finished* runs ended in ``error``, in a row
    #: (:func:`db.queries.consecutive_failure_counts`). ``0`` = its newest finished run
    #: completed, including a ``partial`` one.
    consecutive_failures: int

    @property
    def failing(self) -> bool:
        """SPEC §7.2's "fails 3 consecutive runs", using the threshold the scheduler logs at.

        A property rather than a stored flag so the row cannot be built with a streak that
        disagrees with its own verdict, and so the template asks the same question the
        scheduler's ERROR line does.
        """
        return self.consecutive_failures >= queries.CONSECUTIVE_FAILURE_ALERT


@router.get("/runs", response_class=HTMLResponse)
async def runs_page(request: Request, session: SessionDep) -> Response:
    """The run log and the per-connector health summary."""
    recent = await session.scalars(
        select(FetchRun).order_by(FetchRun.started_at.desc()).limit(RUN_HISTORY)
    )
    health = connector_health(
        await queries.last_successful_runs(session),
        await queries.consecutive_failure_counts(session),
    )
    context: dict[str, Any] = {
        "runs": list(recent),
        "health": health,
        # The banner's rows, computed here rather than filtered in the template: the page has
        # to say "prominently" (SPEC §7.2) with one line at the top, and that line needs the
        # count before it can be phrased.
        "failing": [row for row in health if row.failing],
        "failure_threshold": queries.CONSECUTIVE_FAILURE_ALERT,
        "page_title": "Ingestion health",
    }
    return templates.TemplateResponse(request, "runs.html", context)


def connector_health(
    last_ok_by_connector: dict[str, datetime],
    failure_counts: dict[str, int],
    *,
    now: datetime | None = None,
) -> list[ConnectorHealth]:
    """Join ``config/connectors.yaml`` with each connector's last success and failure streak.

    Driven by the *config*, not by the runs table: a connector that has never run once has no
    rows to join against and is exactly the case this page exists to surface, so it must appear
    with ``last_ok=None`` and ``stale=True``.

    Stale means "has a cadence, and either never succeeded or last succeeded more than twice
    that cadence ago" (SPEC §9). A connector with ``cadence: null`` is on-demand
    (``opencorporates``, the seed loader) and is never stale — there is no schedule for it to
    have missed.

    Failing means "its last :data:`~db.queries.CONSECUTIVE_FAILURE_ALERT` finished runs all
    ended in ``error``" (SPEC §7.2). That signal is *not* cadence-derived, so an on-demand
    connector can be failing while it can never be stale: nothing schedules it, but the runs it
    did have — by hand or from ``cli.py refresh`` — are still failing, and that is worth saying.

    The two are independent on purpose, and both are shown because either alone misleads.
    ``last_ok_by_connector`` counts ``ok`` runs only, while a ``partial`` run ends a failure
    streak (:func:`db.queries.consecutive_failure_counts`), so "last ok nine days ago" and
    "0 consecutive failures" is a real, honest pair: the connector is limping, not broken.
    The reverse — fresh ``last_ok``, streak of three — is a source that broke this morning.

    ``failure_counts`` is passed in rather than looked up here because this module is pure: the
    route does the I/O. It is positional and required so a caller cannot quietly render the
    page with the §7.2 signal missing. Both mappings omit connectors they have nothing to say
    about; ``.get`` supplies the defaults.

    ``now`` is injectable so a test can pin the boundary.
    """
    moment = now or datetime.now(UTC)
    config = load_connectors_config()
    rows: list[ConnectorHealth] = []
    for name, connector in config.connectors.items():
        period = cadence_period(connector.cadence)
        last_ok = last_ok_by_connector.get(name)
        stale = period is not None and (
            last_ok is None or moment - _as_utc(last_ok) > STALE_CADENCE_MULTIPLE * period
        )
        rows.append(
            ConnectorHealth(
                connector=name,
                cadence=connector.cadence,
                period=period,
                last_ok=last_ok,
                stale=stale,
                enabled=connector.enabled,
                implemented=name not in UNIMPLEMENTED_CONNECTORS,
                consecutive_failures=failure_counts.get(name, 0),
            )
        )
    return rows


def _as_utc(value: datetime) -> datetime:
    """Every timestamp column is ``timestamptz``; a naive value would still not be comparable."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
