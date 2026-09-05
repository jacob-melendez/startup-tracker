"""Hacker News "Who is hiring?" connector (SPEC §4 Tier 1 #4).

Every month the ``whoishiring`` account posts an *Ask HN: Who is hiring?* thread and companies
reply with one top-level comment each. Startups post there directly, often list part-time or
contract roles, and frequently include a contact address they intend to be public — which is
exactly the material this project is for (SPEC §1, §6).

Everything comes from the official Firebase API, ``https://hacker-news.firebaseio.com/v0/``:
``user/whoishiring.json`` lists the account's submissions newest-first, ``item/{id}.json``
returns a story (with its ``kids``) or a comment. ``robots.txt`` disallows ``/`` but allows
``/*.json$``, so every URL this connector builds is permitted — the shared client enforces that.

Parsing a comment
-----------------
The thread's convention is a header line of pipe-separated fields:

``COMPANY | ROLE | LOCATION | Full-time | REMOTE | https://example.com``

Fields appear in any order and most are optional, so :func:`parse_header` classifies each field
by what it looks like rather than by position: a field that resolves to a city in
``config/regions.yaml`` is the location, one the employment-type keywords recognise is the
commitment, one that is a URL is the link, and the first field is the company. Comments with no
pipes at all (a fair few) fall back to :func:`leading_name`, which takes the leading proper
name — "SwingVision is the AI tennis app…" — and only if a configured city appears in the body.

Region filter. A comment is kept only when a configured city is named, preferring the header's
location field over a mention anywhere in the body, and the reason is logged either way.

What is written. One :class:`~ingest.base.CompanyRecord` per comment, with the comment as one
:class:`~ingest.base.JobRecord` (``external_id`` = the HN item id) plus one job per
``- Role: https://…`` bullet, which is how multi-role comments are written. ``jobs_complete`` is
**False**: a comment is a slice of a company's openings, never the whole board, so nothing here
may close a job another connector reported (SPEC §2, §5). An address the poster wrote themselves
becomes a ``published`` contact (SPEC §6), normalized by :mod:`ingest.contacts` into the same
form the company-site connector stores its ``mailto:`` links in; nothing is ever guessed.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field

from db import enums
from ingest.base import (
    CompanyRecord,
    Connector,
    ContactRecord,
    FetchContext,
    JobRecord,
    LocationRecord,
)
from ingest.classify import classify
from ingest.config import ConnectorConfig, RegionsConfig
from ingest.contacts import EMAIL_IN_TEXT, normalize_email
from ingest.htmlutil import html_to_text
from ingest.http import RobotsDisallowed
from ingest.normalize import normalize_domain
from logging_config import get_logger

log = get_logger(__name__)

API = "https://hacker-news.firebaseio.com/v0"
USER_URL = f"{API}/user/whoishiring.json"
ITEM_URL = f"{API}/item/{{item_id}}.json"
#: Where a comment is read on the web (``Job.url``, ``Source.url``).
COMMENT_URL = "https://news.ycombinator.com/item?id={item_id}"
#: How much of a comment is stored as the job description.
DESCRIPTION_LIMIT = 8000

_HIRING_TITLE = re.compile(r"^ask hn:\s*who is hiring\?", re.IGNORECASE)
_URL = re.compile(r"https?://[^\s<>\"')\]]+")
#: ``- Data Labeling Intern: https://…`` / ``* Head of Product — https://…``
_ROLE_BULLET = re.compile(
    r"^[-*•]\s*(?P<title>[^:<>]{3,120}?)\s*[:\-–—]\s*(?P<url>https?://\S+)", re.MULTILINE
)
#: A leading proper name for a comment with no pipe header: up to four capitalised-ish words
#: before the first sentence break.
_LEADING_NAME = re.compile(r"^\s*([A-Z][\w.&'’+-]*(?:\s+[A-Z0-9][\w.&'’+-]*){0,3})\b")
_REMOTE = re.compile(r"\bremote\b", re.IGNORECASE)
_WHITESPACE = re.compile(r"\s+")


class HnHiringOptions(BaseModel):
    """``options`` for ``hn_hiring`` in ``config/connectors.yaml``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: Threads to read, newest first. One is the normal case (the cadence is monthly, the 2nd
    #: at 09:00 — SPEC §7.2); a first run may want the last few.
    max_threads: int = Field(default=1, ge=1)
    #: Top-level comments read per thread. A thread runs to a few hundred; the cap bounds a
    #: single run's request count against a free API.
    max_comments_per_thread: int = Field(default=400, ge=1)
    #: Link hosts that are never a company's own domain — job boards, docs, aggregators. A
    #: comment whose only link is ``app.deel.com/job-boards/…`` has no domain, and the company
    #: is then resolved by name within the metro (SPEC §8 step 2) instead of being keyed on
    #: somebody else's site.
    ignore_link_hosts: tuple[str, ...] = (
        # Applicant tracking systems and their short-link hosts.
        "greenhouse.io",
        "grnh.se",
        "lever.co",
        "ashbyhq.com",
        "workable.com",
        "deel.com",
        "rippling.com",
        "workday.com",
        "myworkdayjobs.com",
        "bamboohr.com",
        "smartrecruiters.com",
        "breezy.hr",
        "recruitee.com",
        "teamtailor.com",
        "applytojob.com",
        "pinpointhq.com",
        "jobs.gem.com",
        "workatastartup.com",
        # Documents, forms and scheduling.
        "notion.so",
        "notion.site",
        "docs.google.com",
        "forms.gle",
        "airtable.com",
        "calendly.com",
        # Social and community.
        "linkedin.com",
        "twitter.com",
        "x.com",
        "github.com",
        "youtube.com",
        "youtu.be",
        "discord.gg",
        "t.me",
        "news.ycombinator.com",
        "ycombinator.com",
        # Press: a poster linking their funding coverage is not linking their own site.
        "techcrunch.com",
        "mercurynews.com",
        "forbes.com",
        "bloomberg.com",
        "nytimes.com",
        "wsj.com",
        "medium.com",
        "substack.com",
    )
    #: Subdomain labels stripped off a discovered link before it becomes the company's key.
    #: ``careers.snowflake.com`` and ``snowflake.com`` are one company, and the canonical key
    #: is the bare domain (SPEC §8 step 1); leaving the label on would create a second row.
    strip_subdomains: tuple[str, ...] = ("careers", "jobs", "job", "apply", "hire", "work", "go")


# --------------------------------------------------------------------------------- raw item


@dataclass(frozen=True, slots=True)
class HnComment:
    """One top-level comment plus the thread it belongs to."""

    story_id: int
    story_title: str
    item: dict[str, Any]

    @property
    def item_id(self) -> int:
        return int(self.item["id"])

    @property
    def posted_at(self) -> datetime | None:
        value = self.item.get("time")
        if isinstance(value, bool) or not isinstance(value, int | float):
            return None
        return datetime.fromtimestamp(value, tz=UTC)


@dataclass(frozen=True, slots=True)
class Header:
    """What :func:`parse_header` made of a comment's first line."""

    company: str | None
    role: str | None
    location: str | None
    commitment: str | None
    url: str | None
    fields: tuple[str, ...]


# --------------------------------------------------------------------------------- parsing


def is_hiring_thread(title: object) -> bool:
    return isinstance(title, str) and bool(_HIRING_TITLE.match(title.strip()))


def comment_text(item: dict[str, Any]) -> str | None:
    """A comment's body as plain text; ``None`` for a deleted or dead comment."""
    if item.get("deleted") or item.get("dead"):
        return None
    return html_to_text(item.get("text"))


def parse_header(text: str, regions: RegionsConfig) -> Header:
    """Classify the pipe-separated fields of a comment's first line by what they look like.

    Position is not reliable — the thread's own template is advisory and posters reorder it
    freely — so each field is tested: does ``config/regions.yaml`` know it as a city, do the
    employment-type keywords in ``config/classifiers.yaml`` recognise it, is it a URL? The
    first field is the company by convention, and the first remaining field is the role.
    """
    first_line = text.strip().splitlines()[0] if text.strip() else ""
    # Posters bold the header with markdown-ish asterisks; they are not part of any field.
    fields = [field.strip(" *_\t") for field in first_line.split("|")]
    fields = [field for field in fields if field]
    if not fields:
        return Header(None, None, None, None, None, ())

    # Only a line that actually used the pipe template names its company in the first field. A
    # comment with no pipes at all is prose ("SwingVision is the AI tennis app…") and its name
    # is found by :func:`leading_name` instead — taking the whole paragraph would be absurd.
    company = fields[0] if len(fields) > 1 else None
    location: str | None = None
    commitment: str | None = None
    url: str | None = None
    leftovers: list[str] = []
    for field in fields[1:]:
        if url is None and (match := _URL.search(field)):
            url = match.group(0)
            continue
        if location is None and _city_in(field, regions) is not None:
            location = field
            continue
        if commitment is None and _looks_like_commitment(field):
            commitment = field
            continue
        leftovers.append(field)
    role = leftovers[0] if leftovers else None
    return Header(company, role, location, commitment, url, tuple(fields))


def _looks_like_commitment(field: str) -> bool:
    """Whether the employment-type rules recognise this field on its own.

    ``classify`` is given the field as a *title*, which is exactly the "title first" path of
    SPEC §7.1, so the vocabulary stays in ``config/classifiers.yaml`` and none of it is here.
    """
    return classify(field).employment_type is not enums.EmploymentType.UNKNOWN


def _city_in(text: str, regions: RegionsConfig) -> str | None:
    """The first configured city named anywhere in ``text`` (word-boundary, case-insensitive)."""
    haystack = _WHITESPACE.sub(" ", text).casefold()
    for entry in regions.cities:
        needle = _WHITESPACE.sub(" ", entry.city).casefold()
        index = haystack.find(needle)
        while index != -1:
            before = haystack[index - 1] if index else " "
            after_index = index + len(needle)
            after = haystack[after_index] if after_index < len(haystack) else " "
            if not before.isalnum() and not after.isalnum():
                return entry.city
            index = haystack.find(needle, index + 1)
    return None


def leading_name(text: str) -> str | None:
    """The leading proper name of a comment with no pipe header ("SwingVision is the…")."""
    match = _LEADING_NAME.match(text.strip())
    if match is None:
        return None
    name = match.group(1).strip(" .,:;-–—")
    return name or None


def extract_domain(
    text: str, ignore_hosts: Iterable[str], strip_subdomains: Iterable[str] = ()
) -> str | None:
    """The first link in a comment that is plausibly the company's own site.

    Links to job boards, document hosts, social networks and press coverage are skipped
    (``options.ignore_link_hosts``): keying a company on ``app.deel.com`` or ``grnh.se`` would
    merge every company that uses Deel or Greenhouse into one row. A leading careers-ish label
    is then stripped (``options.strip_subdomains``), because ``careers.snowflake.com`` is not a
    different company from ``snowflake.com`` and the canonical key is the bare domain.
    """
    blocked = tuple(host.casefold() for host in ignore_hosts)
    labels = tuple(f"{label.casefold()}." for label in strip_subdomains)
    for match in _URL.finditer(text):
        host = (urlsplit(match.group(0)).hostname or "").casefold()
        if not host or any(host == bad or host.endswith(f".{bad}") for bad in blocked):
            continue
        domain = normalize_domain(match.group(0))
        if domain is None:
            continue
        for label in labels:
            # Only a *leading* label, and only when something is left that still has a dot.
            if domain.startswith(label) and domain.count(".") >= 2:
                domain = domain[len(label) :]
                break
        return domain
    return None


def extract_emails(text: str) -> list[str]:
    """Addresses the poster wrote in their own comment (SPEC §6: ``published`` only).

    Both the pattern and the normalisation come from :mod:`ingest.contacts`, which is also what
    the company-site connector runs its ``mailto:`` hrefs through. The two sources SPEC §6 allows
    must recognise the same addresses and store each in exactly one form:
    ``HR@atom-computing.com`` from a careers page and ``hr@atom-computing.com`` from a comment
    are the same address, and storing both would put two rows past
    ``uq_contacts_company_id_kind_value`` for one company.
    """
    found: dict[str, None] = {}
    for match in EMAIL_IN_TEXT.finditer(text):
        address = normalize_email(match.group(0))
        if address is not None:
            found.setdefault(address, None)
    return list(found)


def extract_role_bullets(text: str) -> list[tuple[str, str]]:
    """``[(title, url)]`` for the ``- Role: https://…`` lines multi-role comments use."""
    out: list[tuple[str, str]] = []
    for match in _ROLE_BULLET.finditer(text):
        title = _WHITESPACE.sub(" ", match.group("title")).strip(" .,:;-–—")
        url = match.group("url").rstrip(".,;)")
        if title and url:
            out.append((title, url))
    return out


# ------------------------------------------------------------------------------- connector


class HnHiringConnector(Connector[HnComment]):
    """Hacker News "Who is hiring?" (SPEC §4 Tier 1 #4, §12 Phase 3)."""

    name: ClassVar[str] = "hn_hiring"

    def __init__(self, config: ConnectorConfig, regions: RegionsConfig) -> None:
        super().__init__(config, regions)
        self.options = HnHiringOptions.model_validate(config.options)
        self.skipped: dict[str, int] = {}

    # ------------------------------------------------------------------ fetch

    async def fetch(self, ctx: FetchContext) -> AsyncIterator[HnComment]:
        """Find the newest hiring threads and yield their top-level comments."""
        self.skipped = {}
        submissions = await self._submissions(ctx)
        threads = 0
        comments = 0
        for story_id in submissions:
            if threads >= self.options.max_threads:
                break
            story = await self._item(ctx, story_id)
            if story is None or not is_hiring_thread(story.get("title")):
                continue
            threads += 1
            title = str(story.get("title") or "")
            kids = story.get("kids")
            kid_ids = (
                [kid for kid in kids if isinstance(kid, int)] if isinstance(kids, list) else []
            )
            ctx.log.info("hn_hiring.thread", story_id=story_id, title=title, comments=len(kid_ids))
            for kid in kid_ids[: self.options.max_comments_per_thread]:
                item = await self._item(ctx, kid)
                if item is None:
                    continue
                comments += 1
                yield HnComment(story_id=story_id, story_title=title, item=item)
        if threads == 0:
            ctx.problems.append(
                "no 'Ask HN: Who is hiring?' thread found among the newest "
                f"{len(submissions)} submissions by whoishiring"
            )
        ctx.log.info(
            "hn_hiring.summary",
            threads=threads,
            comments=comments,
            skipped=dict(self.skipped),
            requests=ctx.http.stats.requests,
        )

    async def _submissions(self, ctx: FetchContext) -> list[int]:
        payload = await ctx.http.get_json(USER_URL)
        submitted = payload.get("submitted") if isinstance(payload, dict) else None
        if not isinstance(submitted, list):
            msg = f"{USER_URL} did not list the whoishiring account's submissions"
            raise RuntimeError(msg)
        # Newest first; each month contributes a "who is hiring" and a "who wants to be hired"
        # thread, so a handful of ids covers `max_threads` months even when they interleave.
        return [item for item in submitted if isinstance(item, int)][: self.options.max_threads * 4]

    async def _item(self, ctx: FetchContext, item_id: int) -> dict[str, Any] | None:
        url = ITEM_URL.format(item_id=item_id)
        try:
            payload = await ctx.http.get_json(url)
        except (httpx.HTTPStatusError, httpx.TransportError, RobotsDisallowed) as exc:
            ctx.problems.append(f"item {item_id} could not be fetched: {exc}")
            ctx.log.warning("hn_hiring.item_failed", item_id=item_id, error=str(exc))
            return None
        except ValueError as exc:
            ctx.problems.append(f"item {item_id} did not return JSON: {exc}")
            return None
        return payload if isinstance(payload, dict) else None

    # ------------------------------------------------------------------ to_records

    def to_records(self, raw: HnComment) -> Iterable[CompanyRecord]:
        """One record per in-region comment, or nothing.

        Dropped with a counter: deleted comments, comments with no recognisable company name,
        and comments that name no city from ``config/regions.yaml``.
        """
        text = comment_text(raw.item)
        if not text:
            return self._skip("empty_or_deleted", raw)

        header = parse_header(text, self.regions)
        name = header.company or leading_name(text)
        if not name or len(name) > 120:
            return self._skip("no_company_name", raw)

        city = _city_in(header.location, self.regions) if header.location else None
        matched_in = "header"
        if city is None:
            city = _city_in(text, self.regions)
            matched_in = "body"
        if city is None:
            return self._skip("outside_region", raw, header=header.fields)
        configured = self.regions.lookup(city, _state_of(city, self.regions))
        if configured is None:  # pragma: no cover - _city_in only returns configured cities
            return self._skip("outside_region", raw, header=header.fields)
        log.debug("hn_hiring.region", name=name, city=configured.city, matched_in=matched_in)

        domain = extract_domain(text, self.options.ignore_link_hosts, self.options.strip_subdomains)
        contacts = tuple(
            ContactRecord(
                kind=enums.ContactKind.EMAIL,
                value=address,
                # SPEC §6: the poster wrote it in a public comment themselves.
                confidence=enums.ContactConfidence.PUBLISHED,
            )
            for address in extract_emails(text)
        )
        return [
            CompanyRecord(
                name=name,
                external_id=str(raw.item_id),
                source_url=COMMENT_URL.format(item_id=raw.item_id),
                domain=domain,
                website_url=header.url if domain else None,
                locations=(
                    LocationRecord(
                        city=configured.city,
                        state=configured.state,
                        country=configured.country,
                        metro=configured.metro,
                        is_hq=True,
                    ),
                ),
                contacts=contacts,
                jobs=self._jobs(raw, text, header),
                # A comment is a slice of a company's openings, never its whole board.
                jobs_complete=False,
            )
        ]

    def _jobs(self, raw: HnComment, text: str, header: Header) -> tuple[JobRecord, ...]:
        """The comment itself as one job, plus one per ``- Role: https://…`` bullet.

        The comment-level job is titled from the header's role field when the poster gave one
        and otherwise names the company, which classifies as ``role_family='other'`` — still
        ingested, still displayed (SPEC §7.1, §13).
        """
        posted_at = raw.posted_at
        comment_url = COMMENT_URL.format(item_id=raw.item_id)
        location_text = header.location
        is_remote = bool(_REMOTE.search(header.location or "")) or bool(
            _REMOTE.search(" | ".join(header.fields))
        )
        description = (
            text if len(text) <= DESCRIPTION_LIMIT else text[:DESCRIPTION_LIMIT].rstrip() + "…"
        )
        hint = header.commitment

        jobs: list[JobRecord] = []
        title = header.role or f"{header.company or 'Role'} — see the Hacker News post"
        jobs.append(
            self._job_record(
                external_id=str(raw.item_id),
                title=title,
                url=comment_url,
                location_text=location_text,
                is_remote=is_remote,
                posted_at=posted_at,
                description=description,
                hint=hint,
                payload={
                    "hn_item_id": raw.item_id,
                    "hn_story_id": raw.story_id,
                    "hn_story_title": raw.story_title,
                    "hn_by": raw.item.get("by"),
                    "header_fields": list(header.fields),
                },
            )
        )
        for index, (bullet_title, url) in enumerate(extract_role_bullets(text), start=1):
            jobs.append(
                self._job_record(
                    external_id=f"{raw.item_id}#{index}",
                    title=bullet_title,
                    url=url,
                    location_text=location_text,
                    is_remote=is_remote,
                    posted_at=posted_at,
                    description=description,
                    hint=hint,
                    payload={
                        "hn_item_id": raw.item_id,
                        "hn_story_id": raw.story_id,
                        "bullet": bullet_title,
                        "bullet_url": url,
                    },
                )
            )
        return tuple(jobs)

    def _job_record(
        self,
        *,
        external_id: str,
        title: str,
        url: str,
        location_text: str | None,
        is_remote: bool,
        posted_at: datetime | None,
        description: str,
        hint: str | None,
        payload: dict[str, Any],
    ) -> JobRecord:
        result = classify(
            title,
            description=description,
            employment_type_hint=hint,
            # One comment advertises several roles, so the body must not decide the employment
            # type of any single one of them (see ``ingest/classify.py``). It still feeds
            # ``flexible_signal``, which is where "students welcome" and "flexible hours" live.
            employment_type_from_description=False,
        )
        return JobRecord(
            external_id=external_id,
            title=title[:300],
            url=url,
            location_text=location_text,
            is_remote=is_remote,
            posted_at=posted_at,
            description_raw=description,
            raw_payload=payload,
            **result.as_job_fields(),
        )

    def _skip(self, reason: str, raw: HnComment, **details: object) -> tuple[CompanyRecord, ...]:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1
        log.info("hn_hiring.skip", reason=reason, item_id=raw.item.get("id"), **details)
        return ()


def _state_of(city: str, regions: RegionsConfig) -> str:
    """The state of a configured city (``_city_in`` only ever returns configured names)."""
    for entry in regions.cities:
        if entry.city == city:
            return entry.state
    return ""
