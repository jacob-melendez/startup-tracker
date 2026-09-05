"""The server-rendered web interface (SPEC §9).

Four pages — ``/`` (company list), ``/roles`` (flat job list), ``/company/{id}`` (permalink
detail) and ``/runs`` (ingestion health) — built from FastAPI routes, Jinja2 templates and
HTMX, with no build step and no JavaScript framework (SPEC §3).

The whole package is **read-only against the local database**: SPEC §2 and CLAUDE.md forbid a
request handler from making any outbound network call, so nothing here imports ``httpx``,
``ingest.http``, ``ingest.connectors`` or ``ingest.seed``. All fetching happens in the
scheduled connector jobs and the ``cli.py`` commands. ``ingest.config`` *is* imported (by
``web/routes/runs.py``) because it only reads local YAML — it is the single source of truth for
a connector's cadence, which ``/runs`` needs to decide what "stale" means.
"""

from __future__ import annotations
