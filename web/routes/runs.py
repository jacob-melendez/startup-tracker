"""``/runs`` — ingestion health (SPEC §9, §7.2).

Two things on one page: the last 100 ``fetch_runs`` rows, and a health line per *configured*
connector. The second matters more. A connector that quietly stops returning anything writes no
row at all, so a list of runs can look perfectly healthy while a source has been dead for a
month — the whole point of §9's "highlight any connector with no successful run in over twice
its cadence" is to make that absence visible.

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
from ingest.config import cadence_period, load_connectors_config
from web.deps import SessionDep
from web.templating import templates

router = APIRouter()

#: SPEC §9: "Last 100 ``FetchRun`` rows".
RUN_HISTORY = 100
#: SPEC §9: "no successful run in over twice its cadence".
STALE_CADENCE_MULTIPLE = 2
#: Connectors with a block in ``config/connectors.yaml`` but no implementation yet (SPEC §4
#: Tier 2 #6, #7). Named here rather than read from :mod:`ingest.connectors` because nothing
#: under ``web/`` may import a connector — that package pulls in the HTTP client, and a request
#: handler must not be one import away from an outbound call (SPEC §2, CLAUDE.md). They are
#: listed anyway, and marked, so an unimplemented source is visibly unimplemented rather than
#: silently missing.
UNIMPLEMENTED_CONNECTORS = frozenset({"product_hunt", "opencorporates"})


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


@router.get("/runs", response_class=HTMLResponse)
async def runs_page(request: Request, session: SessionDep) -> Response:
    """The run log and the per-connector health summary."""
    recent = await session.scalars(
        select(FetchRun).order_by(FetchRun.started_at.desc()).limit(RUN_HISTORY)
    )
    health = connector_health(await queries.last_successful_runs(session))
    context: dict[str, Any] = {
        "runs": list(recent),
        "health": health,
        "page_title": "Ingestion health",
    }
    return templates.TemplateResponse(request, "runs.html", context)


def connector_health(
    last_ok_by_connector: dict[str, datetime], *, now: datetime | None = None
) -> list[ConnectorHealth]:
    """Join ``config/connectors.yaml`` with the last successful run of each connector.

    Driven by the *config*, not by the runs table: a connector that has never run once has no
    rows to join against and is exactly the case this page exists to surface, so it must appear
    with ``last_ok=None`` and ``stale=True``.

    Stale means "has a cadence, and either never succeeded or last succeeded more than twice
    that cadence ago" (SPEC §9). A connector with ``cadence: null`` is on-demand
    (``opencorporates``, the seed loader) and is never stale — there is no schedule for it to
    have missed. Consecutive-failure detection (§7.2) is Phase 6's, not this.

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
            )
        )
    return rows


def _as_utc(value: datetime) -> datetime:
    """Every timestamp column is ``timestamptz``; a naive value would still not be comparable."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
