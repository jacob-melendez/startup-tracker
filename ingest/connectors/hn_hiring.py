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
location field over a mention anywhere in the body, and the reason is logged either way. Which
city, when several are named, is :func:`find_city`'s job. It looks for every spelling
``config/regions.yaml`` records for a city — the canonical name and each of its aliases — and
returns the entry, so an alias is what matches the poster's text while the canonical name is
what reaches the database. Posters write the short form far more often than they write a city
out, and a spelling this connector cannot see is not a near miss: the comment is dropped as out
of region, so the alias list is the difference between reading a metro's comments and reading a
fraction of them (the counts that earned each alias sit beside it in the config).

Candidates are ranked by a state-qualified mention (the spelling followed by the city's own
state, as a code or written out as ``config/regions.yaml`` spells it) first, then the longest
spelling that matched, then the earliest mention.
All three pay for themselves on a national city list. The length rule stops a configured city
whose name *contains* a shorter configured one from being read as the shorter one — they are
different cities in different metros, and taking the wrong one files the company under the
wrong region and resolves it (SPEC §8 step 2) against the wrong neighbours. The state rule lets
a location written out in prose beat a bare word elsewhere in the comment that merely happens
to be a city name: a founder's surname, a product, a street.

Ranking is not the only thing the state after a mention decides. A mention followed by a
two-letter code that is **not** the city's own state is thrown out rather than ranked, because
it names a different place of the same name — see :func:`_names_another_state` for why that
veto is drawn as narrowly as it is. What stays ambiguous is a comment naming two configured
cities, neither state-qualified and neither spelling longer: the earlier mention wins, which is
a guess. ``docs/SOURCES.md`` records the same limit.

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
from ingest.config import ConnectorConfig, RegionCity, RegionsConfig
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
#: What may follow a city mention and name its state: a comma, then either a two-letter code
#: (``", XX"``) or one or two written-out capitalised words. The written-out form must be
#: capitalised so that ordinary prose behind a comma is not read as a state; a two-letter code
#: is captured in any case, since ``_names_state`` still requires it to be the city's own before
#: it promotes anything. Case matters only on the way out: ``_names_another_state`` vetoes on an
#: all-capitals code alone, because a lowercase pair of letters behind a comma is as likely to
#: be a word as a code.
#:
#: The code alternative ends on ``(?![\w-])`` rather than ``\b``, which a hyphen satisfies: this
#: thread is written in all-capitals vernacular, so ``\b`` cut the first two letters out of
#: ``", ON-SITE"`` and ``", IN-PERSON"`` and handed them to the veto as somebody else's state
#: code, dropping the company outright (measured: one lost record in 742 live comments,
#: 2026-09-08). What is left is the same word with a space in it — ``", ON SITE"`` still reads
#: as a code — which no comment in that sample wrote and which this pattern cannot tell from a
#: real code without a list of the fifty that exist.
_STATE_AFTER_CITY = re.compile(
    r"\s*,\s*(?P<state>[A-Za-z]{2}(?![\w-])|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)"
)


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
        if location is None and find_city(field, regions) is not None:
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


def _names_state(written: str, city: RegionCity) -> bool:
    """Whether ``written`` — the text just after ``"City,"`` — names ``city``'s own state.

    Two spellings answer to it and both are read off ``config/regions.yaml``: the state's code,
    and — when the file gives one — the ``state_name`` that code is written out as. A city whose
    region configures no ``state_name`` has no written-out spelling to recognise, so a qualifier
    in words never promotes a mention of it; that costs the mention a rank and cannot cost the
    record, since only :func:`_names_another_state` ever discards one and it reads codes alone.

    Both comparisons are equality, after case-folding and whitespace normalisation. What stood
    here before was a letter test — same first letter as the code, the code's second letter
    somewhere after it — written that way to keep a table of the fifty state names out of a
    connector (SPEC §11). It kept the table out and got the answer wrong: the test holds for a
    code against its own state, and it also holds for a *neighbouring* state whose written-out
    name happens to share those two letters, and for ordinary capitalised words behind a comma.
    Rank 1 is what chooses between mentions, so a spurious promotion does not cost a rank, it
    picks the wrong mention outright — measured live on 2026-09-08, a comment listing seven
    offices was filed under the second one's metro because the place name written after it
    letter-matched the first one's code. The state name belongs in the same file the code does,
    and that is now where it is read from.
    """
    spelled = _WHITESPACE.sub(" ", written).strip().casefold()
    if spelled == city.state.strip().casefold():
        return True
    return city.state_name is not None and spelled == city.state_name.strip().casefold()


def _names_another_state(written: str, city: RegionCity) -> bool:
    """Whether ``written`` — the text just after ``"City,"`` — is a two-letter code that is
    **not** ``city``'s, which makes the mention evidence *against* this city rather than for it.

    A configured name written out with somebody else's code is a different place that happens to
    share the name. SPEC §12 Phase 7 created that surface: seven of the configured city names
    also exist in states the file does not list, so before this check a company writing one of
    them with its real state was still filed under the configured metro of the same name — the
    qualifier failed to *promote* the mention and it then counted as a bare match anyway, which
    is the wrong metro, and the wrong metro scopes what SPEC §8 will merge the company with.

    The trigger is deliberately narrow because the evidence for it is narrow: a two-letter code
    is unambiguous while prose is not. A written-out qualifier stays promote-only, because the
    file names each configured state and no other: :func:`_names_state` can tell a city's own
    written-out state from anything else, but "anything else" spans every state the config does
    not configure, every country, and every capitalised word an English sentence puts after a
    comma. Vetoing on all of those would drop the very mentions this connector exists to find.

    Both letters must be capitalised, which is most of what separates a code from a word:
    ``", CA"`` is a state, while the ``", or"`` of "…, or remote" and the ``", we"`` of
    "…, we are hiring" are English. It is not all of it — the thread's own all-capitals
    vernacular writes ``", ON SITE"``, which no rule short of a list of the fifty real codes can
    tell from one — so :data:`_STATE_AFTER_CITY` refuses to cut a code out of a hyphenated word
    and that spelling is the residue, unobserved in 742 live comments. A code that is not a US
    state at all — a country abbreviation in ``", UK"`` — vetoes too, and should: it says just
    as plainly that the poster meant somewhere else.
    """
    letters = written.strip()
    return (
        len(letters) == 2
        and letters.isalpha()
        and letters.isupper()
        and not _names_state(letters, city)
    )


def find_city(text: str, regions: RegionsConfig) -> RegionCity | None:
    """The configured city ``text`` names, or ``None`` — with its state, country and metro.

    Every spelling of every configured city named anywhere in ``text`` is a candidate (whole
    word, case-insensitive) — the canonical name and each alias, paired with the city it
    resolves to by :attr:`~ingest.config.RegionsConfig.city_needles_longest_first`. Candidates
    are ranked, in this order, by

    1. whether the mention is immediately followed by its own state (:data:`_STATE_AFTER_CITY`),
    2. the length of the *spelling that matched*, not of the city's canonical name,
    3. where the mention starts.

    One kind of mention is not a candidate at all: one immediately followed by a comma and a
    two-letter code that is not the city's own state, which :func:`_names_another_state` reads
    as the poster saying they mean a different place of the same name. It is dropped before the
    ranking sees it, so it cannot win as a bare match either.

    That drop is per *mention*, though, not per run of text, and it shows wherever one configured
    spelling begins another. The qualifier sits after the longer spelling, where the shorter one
    inside it cannot see it, so the shorter one is read as an unqualified mention of its own and
    may still win. The shipped file has exactly one such pair. Leaving it is a choice rather than
    an oversight: the shorter reading rescues a mention the veto was right to take, and equally
    rescues one it was wrong to take, since a country abbreviation is also a capitalised
    two-letter code and vetoes like a state's — including the abbreviation of the country every
    configured city is in. Neither outcome has evidence behind it, and this phase adds a spelling
    only on measured evidence, so the code does the simpler thing and this paragraph records that
    it was decided rather than missed.

    See the module docstring for what each rank buys and what is left ambiguous. Returning the
    :class:`~ingest.config.RegionCity` rather than a name is the point of the function: the
    caller needs the state, and it needs the *canonical* city, since an alias is a spelling to
    match on and never a value to store — ``uq_locations_city_state`` gives one place one row.
    Resolving a city *name* back to a state, which is what this connector used to do, silently
    picks one of the states a name is configured in, so a company posting from the one that lost
    was stored in another region's metro.

    Rank 2 measures the needle because the needle is what was found: an alias can be shorter or
    longer than the name it stands for, and ranking on canonical lengths would let a city
    recognised by a short alias outrank one whose full name the poster wrote out. Walking
    longest-needle-first settles ties between equally long spellings in file order and keeps a
    longer spelling from being shadowed by a shorter one it contains. The length still appears
    in the sort key because rank 1 crosses lengths — a shorter state-qualified spelling outranks
    a longer bare one — which no iteration order alone can express.

    A spelling's precision is its own length, and that is the standing cost of a short one. The
    match runs to word-character boundaries, so a dot, a slash, an at-sign and a hyphen each end
    a needle: a two-character spelling is a whole word inside a hostname, a path segment, an
    address or a hyphenated compound, and a comment that names no other configured city can be
    filed on one of those. A full city name is safe by accident — nothing else in a comment
    happens to be it — and a short one is not, which is why ``config/regions.yaml`` admits a
    spelling only on measured evidence and argues there against short ones. The trade is recorded
    beside the spelling that carries it, since that file is where one is added or withdrawn.
    """
    haystack = _WHITESPACE.sub(" ", text)
    found: RegionCity | None = None
    best: tuple[int, int, int] | None = None
    for spelling, entry in regions.city_needles_longest_first:
        needle = _WHITESPACE.sub(" ", spelling).strip()
        if not needle:
            continue
        # Matched on the original casing, not a casefolded copy: the state check downstream
        # reads capitalisation, and ``str.casefold`` may change a string's length (so its
        # offsets would no longer point into the text the poster wrote).
        pattern = rf"(?<!\w){re.escape(needle)}(?!\w)"
        for match in re.finditer(pattern, haystack, re.IGNORECASE):
            qualifier = _STATE_AFTER_CITY.match(haystack, match.end())
            written = qualifier.group("state") if qualifier is not None else None
            if written is not None and _names_another_state(written, entry):
                continue
            qualified = written is not None and _names_state(written, entry)
            rank = (0 if qualified else 1, -len(needle), match.start())
            if best is None or rank < best:
                found, best = entry, rank
    return found


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

        configured = find_city(header.location, self.regions) if header.location else None
        matched_in = "header"
        if configured is None:
            configured = find_city(text, self.regions)
            matched_in = "body"
        if configured is None:
            return self._skip("outside_region", raw, header=header.fields)
        log.debug(
            "hn_hiring.region",
            name=name,
            city=configured.city,
            state=configured.state,
            metro=configured.metro,
            matched_in=matched_in,
        )

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
