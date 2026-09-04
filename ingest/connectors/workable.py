"""Workable job-board connector (SPEC §4 Tier 1 #3).

``https://apply.workable.com/api/v1/widget/accounts/{account}?details=true`` — the public widget
endpoint. The run mechanics live in :mod:`ingest.connectors.ats`; this module is the endpoint
and the field mapping.

Shape (recorded in ``tests/fixtures/workable/``): ``{"name": …, "description": …, "jobs": [...]}``
where a job carries ``shortcode`` (the stable id), ``title``, ``employment_type``
(``Full-time`` … — the field of SPEC §7.1), ``telecommuting``, ``city``/``state``/``country``
plus a ``locations`` list, ``published_on`` and ``created_at`` as **dates** (no time), ``url``
and an HTML ``description``.

The one trap, verified against the live API: **a wrong account name is not a 404.** Workable
answers ``200`` with ``{"name": "Persona", "description": null, "jobs": []}`` for names that
were never its customers, and answers ``404`` with the plain text ``Not Found`` (not JSON) for
others. :meth:`WorkableConnector.board_has_content` therefore treats a board with no jobs *and*
no description as "nothing here" instead of as an empty board — while still never letting it
count as the 404 that triggers re-discovery, which would otherwise re-probe the same careers
page every single night.
"""

from __future__ import annotations

from typing import Any, ClassVar

from db import enums
from ingest.base import JobRecord
from ingest.connectors.ats import (
    AtsConnector,
    description_text,
    jobs_list,
    join_locations,
    parse_iso_date,
)

BOARD_URL = "https://apply.workable.com/api/v1/widget/accounts/{token}?details=true"


class WorkableConnector(AtsConnector):
    name: ClassVar[str] = "workable"
    provider: ClassVar[enums.AtsProvider] = enums.AtsProvider.WORKABLE
    board_url_template: ClassVar[str] = BOARD_URL

    def board_has_content(self, payload: Any) -> bool:
        """A shell account (no jobs and no description) is not a board — see the module
        docstring. A real board that simply has nothing open today keeps its description, so it
        is still recognised and its jobs are correctly closed as vanished (SPEC §2, §5)."""
        if not isinstance(payload, dict):
            return False
        if jobs_list(payload):
            return True
        description = payload.get("description")
        return isinstance(description, str) and bool(description.strip())

    def parse_jobs(self, payload: Any, token: str) -> list[JobRecord]:
        jobs: list[JobRecord] = []
        for job in jobs_list(payload):
            external_id = job.get("shortcode")
            title = job.get("title")
            if not isinstance(external_id, str) or not isinstance(title, str) or not title.strip():
                continue
            jobs.append(
                self._job(
                    external_id=external_id,
                    title=title.strip(),
                    url=_string(job.get("url")) or _string(job.get("shortlink")),
                    location_text=_locations(job),
                    is_remote=job.get("telecommuting") is True,
                    # Dates only, so midnight UTC; ``published_on`` is when the posting went
                    # live and ``created_at`` can predate it by years on a re-opened role.
                    posted_at=parse_iso_date(job.get("published_on"))
                    or parse_iso_date(job.get("created_at")),
                    description=description_text(job.get("description")),
                    compensation_raw=None,
                    employment_type_hint=_string(job.get("employment_type")),
                    payload=job,
                )
            )
        return jobs


def _string(value: object) -> str | None:
    return value.strip() or None if isinstance(value, str) else None


def _locations(job: dict[str, Any]) -> str | None:
    """``"City, Region, Country"`` per entry of ``locations``, falling back to the flat
    ``city``/``state``/``country`` fields. Entries flagged ``hidden`` are left out."""
    entries = job.get("locations")
    parts: list[str] = []
    if isinstance(entries, list):
        for entry in entries:
            if not isinstance(entry, dict) or entry.get("hidden") is True:
                continue
            pieces = [_string(entry.get(key)) for key in ("city", "region", "state", "country")]
            joined = ", ".join(dict.fromkeys(piece for piece in pieces if piece))
            if joined:
                parts.append(joined)
    if not parts:
        flat = ", ".join(
            dict.fromkeys(
                piece
                for piece in (
                    _string(job.get("city")),
                    _string(job.get("state")),
                    _string(job.get("country")),
                )
                if piece
            )
        )
        if flat:
            parts.append(flat)
    return join_locations(parts)
