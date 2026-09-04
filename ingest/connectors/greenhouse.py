"""Greenhouse job-board connector (SPEC §4 Tier 1 #3).

``https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true`` — a public, documented
endpoint that returns every open role on a board, with the full posting when ``content=true``.
Everything about *how* a run works (which companies, board-token discovery, re-discovery on a
404, ``enrich_only``, ``jobs_complete``) is in :mod:`ingest.connectors.ats`; this module is only
the endpoint and the field mapping.

Shape (recorded in ``tests/fixtures/greenhouse/``): ``{"jobs": [...], "meta": {...}}`` where a
job carries ``id``, ``title``, ``absolute_url``, ``location.name``, ``first_published``,
``updated_at``, ``departments``/``offices`` and an HTML-escaped ``content``. Greenhouse
publishes **no** employment-type field, so those jobs fall through to the keyword rules in
``config/classifiers.yaml`` (SPEC §7.1). An unknown token answers ``404`` with
``{"error": "Not found"}``, which is what triggers re-discovery.
"""

from __future__ import annotations

from typing import Any, ClassVar

from db import enums
from ingest.base import JobRecord
from ingest.connectors.ats import (
    AtsConnector,
    description_text,
    jobs_list,
    parse_iso_datetime,
)

BOARD_URL = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"


class GreenhouseConnector(AtsConnector):
    name: ClassVar[str] = "greenhouse"
    provider: ClassVar[enums.AtsProvider] = enums.AtsProvider.GREENHOUSE
    board_url_template: ClassVar[str] = BOARD_URL

    def parse_jobs(self, payload: Any, token: str) -> list[JobRecord]:
        jobs: list[JobRecord] = []
        for job in jobs_list(payload):
            external_id = job.get("id")
            title = job.get("title")
            if external_id is None or not isinstance(title, str) or not title.strip():
                continue
            location = job.get("location")
            location_text = (location.get("name") if isinstance(location, dict) else None) or None
            jobs.append(
                self._job(
                    external_id=str(external_id),
                    title=title.strip(),
                    url=_string(job.get("absolute_url")),
                    location_text=_string(location_text),
                    # Greenhouse has no remote flag; the location text carries it
                    # ("Remote - US") and the UI filters on that.
                    is_remote=_looks_remote(location_text),
                    # ``first_published`` is when the posting went live; ``updated_at`` is the
                    # fallback for boards that predate the field.
                    posted_at=parse_iso_datetime(job.get("first_published"))
                    or parse_iso_datetime(job.get("updated_at")),
                    description=description_text(job.get("content")),
                    compensation_raw=None,
                    employment_type_hint=None,
                    payload=job,
                )
            )
        return jobs


def _string(value: object) -> str | None:
    return value.strip() or None if isinstance(value, str) else None


def _looks_remote(location: object) -> bool:
    return isinstance(location, str) and "remote" in location.casefold()
