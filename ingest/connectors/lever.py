"""Lever job-board connector (SPEC §4 Tier 1 #3).

``https://api.lever.co/v0/postings/{company}?mode=json`` — a public, documented endpoint that
returns a **bare JSON list** of postings. The run mechanics live in
:mod:`ingest.connectors.ats`; this module is the endpoint and the field mapping.

Shape (recorded in ``tests/fixtures/lever/``): ``id``, ``text`` (the title), ``hostedUrl``,
``categories`` (``commitment`` — the employment-type field SPEC §7.1 asks for —, ``location``,
``allLocations``, ``team``), ``createdAt`` as epoch **milliseconds**, ``workplaceType``
(``onsite`` / ``remote`` / ``hybrid``), an optional ``salaryRange``, and both HTML and plain
text descriptions. An unknown token answers ``404``.

``api.lever.co/robots.txt`` allows everything but sets ``Crawl-delay: 1``; the shared client
honours it automatically, taking the longer of the crawl delay and the configured interval.
"""

from __future__ import annotations

from typing import Any, ClassVar

from db import enums
from ingest.base import JobRecord
from ingest.connectors.ats import (
    AtsConnector,
    description_text,
    format_salary_range,
    jobs_list,
    join_locations,
    parse_epoch_millis,
)

BOARD_URL = "https://api.lever.co/v0/postings/{token}?mode=json"


class LeverConnector(AtsConnector):
    name: ClassVar[str] = "lever"
    provider: ClassVar[enums.AtsProvider] = enums.AtsProvider.LEVER
    board_url_template: ClassVar[str] = BOARD_URL

    def parse_jobs(self, payload: Any, token: str) -> list[JobRecord]:
        jobs: list[JobRecord] = []
        for job in jobs_list(payload, key=None):  # Lever returns the list itself
            external_id = job.get("id")
            title = job.get("text")
            if not isinstance(external_id, str) or not isinstance(title, str) or not title.strip():
                continue
            categories = job.get("categories")
            categories = categories if isinstance(categories, dict) else {}
            all_locations = categories.get("allLocations")
            location_text = join_locations(
                all_locations if isinstance(all_locations, list) else [categories.get("location")]
            )
            workplace = str(job.get("workplaceType") or "").casefold()
            jobs.append(
                self._job(
                    external_id=external_id,
                    title=title.strip(),
                    url=_string(job.get("hostedUrl")) or _string(job.get("applyUrl")),
                    location_text=location_text,
                    is_remote=workplace == "remote",
                    posted_at=parse_epoch_millis(job.get("createdAt")),
                    description=description_text(
                        job.get("descriptionPlain"), job.get("description")
                    ),
                    compensation_raw=format_salary_range(job.get("salaryRange")),
                    employment_type_hint=_string(categories.get("commitment")),
                    payload=job,
                )
            )
        return jobs


def _string(value: object) -> str | None:
    return value.strip() or None if isinstance(value, str) else None
