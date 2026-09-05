"""Request-scoped dependencies: the database session, the HTMX check, the filter facets.

Everything a handler needs that is not a query parameter lives here. Note what is *not* here:
there is no HTTP client, no connector and no fetch. A request handler reads the local database
and nothing else (SPEC §2).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Path
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.requests import Request

from db import queries

#: htmx sets this on every request it makes.
HX_REQUEST_HEADER = "HX-Request"
#: ...and adds this one when it is *restoring a history entry*, where it needs a whole page.
HX_HISTORY_RESTORE_HEADER = "HX-History-Restore-Request"

#: The largest value a Postgres ``integer`` primary key can hold. Every id column in SPEC §5
#: is an ``Identity()`` integer, so a bigger number is not "a row that does not exist" — it is
#: a value asyncpg refuses to bind, raising ``DataError`` from inside the handler and turning a
#: hand-edited or crawled URL into a 500. Declaring the bound on the path parameter turns it
#: into the same rendered 400 that ``/company/abc`` already produces.
MAX_ROW_ID = 2_147_483_647

#: A primary-key path parameter (``/company/{company_id}``, ``/jobs/{job_id}``).
RowIdPath = Annotated[int, Path(ge=1, le=MAX_ROW_ID)]


async def db_session(request: Request) -> AsyncIterator[AsyncSession]:
    """One session per request, committed on a clean exit and rolled back on any exception.

    The factory comes from ``app.state`` rather than :func:`db.session.get_session_factory` so
    that tests can build an app against their own disposable database
    (``create_app(session_factory=...)``) without touching process-wide state.

    Committing on the way out is what makes the note and bookmark handlers work without an
    explicit ``commit()`` in each of them; a GET commits an empty transaction, which is free.
    """
    factory: async_sessionmaker[AsyncSession] | None = getattr(
        request.app.state, "session_factory", None
    )
    if factory is None:  # pragma: no cover — a wiring mistake, not a runtime state
        msg = "app.state.session_factory is not set; the app's lifespan did not run"
        raise RuntimeError(msg)
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except BaseException:
            await session.rollback()
            raise


SessionDep = Annotated[AsyncSession, Depends(db_session)]


def is_htmx(request: Request) -> bool:
    """True when this request wants a *fragment* — an htmx swap into an existing page.

    The second half of the condition is not a detail: when htmx restores a history entry it
    re-requests the URL with ``HX-Request: true`` **and**
    ``HX-History-Restore-Request: true``, and it replaces the whole ``<body>`` with what comes
    back. Answering that with a fragment would blank the application — the user would press
    Back and land on a bare list of rows with no filter form and no navigation.
    """
    return (
        request.headers.get(HX_REQUEST_HEADER) == "true"
        and HX_HISTORY_RESTORE_HEADER not in request.headers
    )


HtmxDep = Annotated[bool, Depends(is_htmx)]


@dataclass(frozen=True, slots=True)
class Facets:
    """The vocabularies the filter form offers that come from data rather than from an enum."""

    cities: list[str]
    #: ``(slug, name)`` — the slug is the wire value, the name is what the user reads.
    sectors: list[tuple[str, str]]


async def facets(session: AsyncSession) -> Facets:
    """Cities and sectors for the filter form, as the database currently holds them.

    Two small ordered lookups, and the one thing here a handler calls by hand rather than
    declaring in its signature. FastAPI resolves a signature dependency before the branch that
    picks a template, so declaring this one would run both lookups on every htmx swap of ``/``
    and ``/roles`` to build a filter form the returned fragment does not contain — only the
    full-page templates include ``_filters.html``.

    They are read from the tables rather than from ``config/regions.yaml`` so the form never
    offers a city that would return nothing; the config decides what gets *ingested* (SPEC §11).
    """
    return Facets(
        cities=await queries.facet_cities(session),
        sectors=await queries.facet_sectors(session),
    )
