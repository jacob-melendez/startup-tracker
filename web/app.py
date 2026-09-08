"""The FastAPI application (SPEC §9).

``uvicorn web.app:app`` serves four pages, and **every one of them reads the local Postgres and
nothing else**. SPEC §2 and CLAUDE.md make that architectural: ingestion is batch-only, so no
request handler ever makes an outbound network call. What the browser sees is whatever the last
connector run wrote — which is also why ``/runs`` exists, to make a source that stopped
answering visible instead of silently stale data.

Wiring:

* :func:`create_app` takes an optional session factory. In production the lifespan builds one
  from :mod:`settings` and disposes the engine on shutdown; a test passes its own factory (for
  a disposable database) and the lifespan then owns nothing.
* Errors are pages, not JSON blobs: a 404 for an unknown company and a 400 for a hand-edited
  ``cursor=`` or an invalid enum value render ``error.html`` with the status code intact. A
  client that did not ask for HTML still gets JSON.
* ``/healthz`` answers without touching the database, so the Compose healthcheck reports on the
  web process rather than on Postgres.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.staticfiles import StaticFiles
from starlette.status import HTTP_400_BAD_REQUEST

from db.queries import CursorError
from db.session import dispose_engine, get_session_factory
from logging_config import configure_logging, get_logger
from settings import get_settings
from web.routes import companies, roles, runs
from web.templating import site_name, templates

STATIC_DIR = Path(__file__).resolve().parent / "static"

log = get_logger(__name__)


def create_app(session_factory: async_sessionmaker[AsyncSession] | None = None) -> FastAPI:
    """Build the application.

    ``session_factory`` is the seam for tests: pass one bound to a disposable database and the
    app never reads ``DATABASE_URL``, never creates the process-wide engine and never disposes
    it. Passing nothing is the production path.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Read settings here rather than at import time: importing ``web.app`` must not depend
        # on a valid environment (the module-level ``app`` below is imported by tests too).
        settings = get_settings()
        configure_logging(settings.log_level, settings.log_json)
        owns_engine = session_factory is None
        app.state.session_factory = session_factory or get_session_factory()
        log.info("web.startup", owns_engine=owns_engine)
        try:
            yield
        finally:
            if owns_engine:
                await dispose_engine()

    app = FastAPI(
        # The same name the pages carry, from the same source: `config/regions.yaml` is the
        # only place a metro may be named (SPEC §12 Phase 7), and this title used to name one.
        title=site_name(),
        description="Locally-run browser over the ingested company and role database (SPEC §9).",
        lifespan=lifespan,
        # No generated API surface. SPEC §1 lists none for v1, and FastAPI's stock docs pages
        # are not inert: /docs pulls swagger-ui from cdn.jsdelivr.net and /redoc pulls redoc
        # plus a stylesheet from fonts.googleapis.com. Those are outbound calls made by the
        # *browser*, on this app's own origin, which is the same rule SPEC §2 states for the
        # server — the reason htmx is vendored at /static rather than linked from a CDN.
        # /openapi.json goes with them: it is what both pages read, and it exists only to
        # describe an API this app does not offer.
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    if session_factory is not None:
        # Also set outside the lifespan: httpx's ASGITransport does not run lifespan events, so
        # a test that drives the app directly would otherwise have no session factory at all.
        app.state.session_factory = session_factory
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    app.include_router(companies.router)
    app.include_router(roles.router)
    app.include_router(runs.router)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Liveness only — deliberately no database round trip.

        The Compose healthcheck uses this to decide whether the *web* process is up; folding a
        query into it would report the database's health under the app's name and would make
        ``make up`` wait on the wrong thing.
        """
        return {"status": "ok"}

    @app.exception_handler(StarletteHTTPException)
    async def http_exception(request: Request, exc: StarletteHTTPException) -> Response:
        """404s and the hand-raised 400s, rendered as a page with the status code preserved."""
        return _error_response(
            request, exc.status_code, str(exc.detail), headers=dict(exc.headers or {})
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception(request: Request, exc: RequestValidationError) -> Response:
        """A bad query parameter is a **400 page**, not FastAPI's default 422 JSON.

        These come from a hand-edited or stale URL (``?stage=serious``), and the person holding
        that URL is in a browser. The message names the parameter so the URL can be fixed.
        """
        return _error_response(request, HTTP_400_BAD_REQUEST, _validation_message(exc))

    @app.exception_handler(CursorError)
    async def cursor_exception(request: Request, exc: CursorError) -> Response:
        """A pagination token that does not decode, or that belongs to another ordering.

        :mod:`web.filters` already turns both into a 400 before a query runs; this handler is
        the backstop, so a token that only ``db.queries`` can reject still lands as a 400 page
        rather than a 500.
        """
        return _error_response(request, HTTP_400_BAD_REQUEST, str(exc))

    return app


def _error_response(
    request: Request, status_code: int, message: str, headers: dict[str, str] | None = None
) -> Response:
    """``error.html`` for a browser, JSON for anything else — same status code either way."""
    if _wants_html(request):
        return templates.TemplateResponse(
            request,
            "error.html",
            {"status_code": status_code, "message": message},
            status_code=status_code,
            headers=headers,
        )
    payload: dict[str, Any] = {"status_code": status_code, "message": message}
    return JSONResponse(payload, status_code=status_code, headers=headers)


def _wants_html(request: Request) -> bool:
    """Whether to render a page. ``*/*`` (curl, an API client) is not a request for HTML."""
    return "text/html" in request.headers.get("accept", "")


def _validation_message(exc: RequestValidationError) -> str:
    """FastAPI's validation errors as one sentence naming the offending parameters."""
    problems: list[str] = []
    for error in exc.errors():
        location = [str(part) for part in error.get("loc", ()) if isinstance(part, str)]
        # loc is ("query", "stage", 0) for the first value of a repeated parameter.
        name = location[-1] if location else "request"
        problems.append(f"{name}: {error.get('msg', 'invalid value')}")
    return "; ".join(problems) or "Invalid request parameters."


#: The module-level instance ``uvicorn web.app:app`` and ``docker compose`` serve.
app = create_app()
