"""The four SPEC §9 pages, split by view.

* :mod:`web.routes.companies` — ``/`` (the primary company list), ``/company/{id}``, the lazy
  expanded-row panel and the note/status/rating form;
* :mod:`web.routes.roles` — ``/roles`` (the flat "what opened this week" list) and the job
  bookmark toggle;
* :mod:`web.routes.runs` — ``/runs`` (ingestion health).

Each module exposes a ``router`` that :func:`web.app.create_app` includes. Every handler here
reads the local database only (SPEC §2).
"""

from __future__ import annotations
