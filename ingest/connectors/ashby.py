"""Ashby job-board connector (SPEC §4 Tier 1 #3).

``https://api.ashbyhq.com/posting-api/job-board/{name}?includeCompensation=true`` — a public,
documented endpoint. The run mechanics live in :mod:`ingest.connectors.ats`; this module is the
endpoint and the field mapping.

Shape (recorded in ``tests/fixtures/ashby/``): ``{"jobs": [...], "apiVersion": "…"}`` where a
job carries ``id``, ``title``, ``department``/``team``, ``employmentType`` (``FullTime``,
``PartTime``, ``Intern``, ``Contract``, ``Temporary`` — the employment-type field of SPEC §7.1),
``location`` plus ``secondaryLocations``, ``isRemote``, ``isListed``, ``publishedAt``,
``jobUrl``, ``descriptionPlain``/``descriptionHtml`` and a ``compensation`` object.

Two things worth knowing, both verified against the live API while recording the fixtures:
board tokens are **case-insensitive** (a careers page linking ``jobs.ashbyhq.com/Linear``
resolves through ``…/job-board/linear``), and ``api.ashbyhq.com/robots.txt`` answers ``401``,
which RFC 9309 §2.3.1.3 defines as "unavailable" — the shared client then treats the origin as
unrestricted rather than blocking it.
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
    parse_iso_datetime,
)

BOARD_URL = "https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=true"


class AshbyConnector(AtsConnector):
    name: ClassVar[str] = "ashby"
    provider: ClassVar[enums.AtsProvider] = enums.AtsProvider.ASHBY
    board_url_template: ClassVar[str] = BOARD_URL

    def parse_jobs(self, payload: Any, token: str) -> list[JobRecord]:
        jobs: list[JobRecord] = []
        for job in jobs_list(payload):
            external_id = job.get("id")
            title = job.get("title")
            if not isinstance(external_id, str) or not isinstance(title, str) or not title.strip():
                continue
            if _is_false(job.get("isListed")):
                # Ashby keeps unlisted drafts in the same payload; they are not open roles.
                continue
            secondary = job.get("secondaryLocations")
            secondary_names = (
                [_location_name(entry) for entry in secondary]
                if isinstance(secondary, list)
                else []
            )
            jobs.append(
                self._job(
                    external_id=external_id,
                    title=title.strip(),
                    url=_string(job.get("jobUrl")) or _string(job.get("applyUrl")),
                    location_text=join_locations([job.get("location"), *secondary_names]),
                    is_remote=_is_true(job.get("isRemote")),
                    posted_at=parse_iso_datetime(job.get("publishedAt")),
                    description=description_text(
                        job.get("descriptionPlain"), job.get("descriptionHtml")
                    ),
                    compensation_raw=_compensation(job.get("compensation")),
                    employment_type_hint=_string(job.get("employmentType")),
                    payload=job,
                )
            )
        return jobs


def _string(value: object) -> str | None:
    return value.strip() or None if isinstance(value, str) else None


def _location_name(entry: object) -> str | None:
    """A ``secondaryLocations`` entry, which is either a string or ``{"location": "…"}``."""
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        return _string(entry.get("location")) or _string(entry.get("name"))
    return None


def _is_true(value: object) -> bool:
    """Ashby serialises booleans as JSON ``true`` in the live API and as the strings
    ``"True"``/``"False"`` in some responses; both spellings are accepted."""
    return value is True or (isinstance(value, str) and value.strip().casefold() == "true")


def _is_false(value: object) -> bool:
    return value is False or (isinstance(value, str) and value.strip().casefold() == "false")


def _compensation(value: object) -> str | None:
    """The human-readable line out of Ashby's ``compensation`` object, when it has one."""
    if not isinstance(value, dict):
        return None
    for key in ("compensationTierSummary", "scrapeableCompensationSalarySummary"):
        summary = _string(value.get(key))
        if summary:
            return summary
    return None
