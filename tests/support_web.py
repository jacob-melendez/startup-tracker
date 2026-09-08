"""Shared helpers for the Phase 4 web tests (SPEC §9).

Three things live here, and all three exist so the four web test modules cannot drift apart:

* :func:`app_client` — the real :func:`web.app.create_app` driven over
  ``httpx.ASGITransport``. Deliberately **not** ``fastapi.testclient.TestClient``: that client
  spins up its own event loop in a portal thread, which fights pytest-asyncio's loop and the
  asyncpg connections bound to it. ASGITransport calls the app in *this* loop, so a request
  handler and the test share one database.
* Row factories mirroring ``tests/test_queries.py``'s style. :func:`make_job` and
  :func:`make_round` refresh the denormalized ``companies`` columns of SPEC §5 before
  returning, because the default sort (``latest_job_posted_at``), the open-role count and the
  ``open_roles`` filter all read those columns — a factory that left them stale would test a
  database state the ingest pipeline never produces. :func:`make_location` places a company in
  a *real* metro of ``config/regions.yaml`` (see :data:`CITY_REGIONS`), which is what the
  region filter of SPEC §12 Phase 7 needs to be tested against anything but itself.
* Markup helpers. The rendered "Load more" URL is HTML-escaped (``&amp;``), so following it
  needs :func:`html.unescape`; :func:`walk_pages` does that and walks a keyset list to
  exhaustion the way a user clicking the button would. :func:`form_state` reads a rendered
  form back as the submission a browser would make from it, so a test can pin what a control
  *does* instead of the attribute spelling that makes it do so, and :func:`select_options`
  reads one ``<select>`` back as the choices it offers — the grouping and the labels
  :func:`form_state` deliberately throws away.

Nothing here touches the network — the app under test never makes an outbound call (SPEC §2).
"""

from __future__ import annotations

import html
import itertools
import re
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta

import httpx
from selectolax.parser import HTMLParser, Node
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.models import (
    Company,
    CompanyLocation,
    CompanySector,
    CompanySource,
    Contact,
    FetchRun,
    FundingRound,
    Investor,
    Job,
    JobBookmark,
    Location,
    RoundInvestor,
    Sector,
    UserNote,
)
from db.queries import refresh_company_denormalized_columns
from web.app import create_app

#: A fixed "now" shared by every web test, matching ``tests/support.NOW``.
NOW = datetime(2026, 9, 4, 12, tzinfo=UTC)
DAY = timedelta(days=1)

#: ASGITransport needs a base URL to build absolute request URLs from; nothing is resolved.
BASE_URL = "http://testserver"
#: The app's error handlers answer JSON unless the request asks for HTML, and httpx sends
#: ``*/*`` by default — so every client here asks for HTML, as a browser does.
ACCEPT_HTML = "text/html,application/xhtml+xml"

#: Unique ``jobs.external_id`` values; the table's uniqueness is (company_id, external_id).
_JOB_SEQUENCE = itertools.count(1)

#: ``city -> (state, metro)`` for the cities the web tests place companies in.
#:
#: SPEC §12 Phase 7 made the metro a filter of its own, so these suites need companies in more
#: than one region — and geography invented per test would let a metro predicate pass while
#: matching on the wrong column entirely. Every entry is a real city of a real metro of
#: ``config/regions.yaml``, except the handful marked below, which are real cities that config
#: does not list: the facets read ``locations``, not the config, and SPEC §2 keeps a row whose
#: city the config never named or no longer names. ``tests/`` is exempt from
#: ``tests/test_regions.py``'s literal guard for exactly this reason — a fixture that had to
#: invent its metros could not check that the real ones group correctly.
#:
#: One name deliberately resolves to only one of its two real regions: ``Newark`` is NJ in the
#: New York metro *and* CA in the Bay Area, and a mapping from a bare city name cannot hold
#: both — the same ambiguity ``RegionsConfig.lookup_city_name`` answers with ``None`` rather
#: than a guess. The commoner one is here; a caller that means the other spells out ``state``
#: and ``metro``, which is what the same-name facet test does.
CITY_REGIONS: dict[str, tuple[str, str]] = {
    "San Francisco": ("CA", "Bay Area"),
    "South San Francisco": ("CA", "Bay Area"),
    "Oakland": ("CA", "Bay Area"),
    "Berkeley": ("CA", "Bay Area"),
    "San Jose": ("CA", "Bay Area"),
    "Palo Alto": ("CA", "Bay Area"),
    "Mountain View": ("CA", "Bay Area"),
    "Fremont": ("CA", "Bay Area"),
    "Emeryville": ("CA", "Bay Area"),
    "Alameda": ("CA", "Bay Area"),  # a Bay Area city the shipped config does not list
    "New York": ("NY", "New York"),
    "Brooklyn": ("NY", "New York"),
    "Long Island City": ("NY", "New York"),
    # The two that make the metro more than a synonym for the state: a region crosses state
    # lines, so a predicate reading `locations.state` would get these wrong and only these.
    "Jersey City": ("NJ", "New York"),
    "Newark": ("NJ", "New York"),
    "Seattle": ("WA", "Seattle"),
    "Bellevue": ("WA", "Seattle"),
    "Boston": ("MA", "Boston"),
    "Cambridge": ("MA", "Boston"),
    "Los Angeles": ("CA", "Los Angeles"),
    "Santa Monica": ("CA", "Los Angeles"),
    "Austin": ("TX", "Austin"),
}

#: Where an unlisted city goes. Every caller written before SPEC §12 Phase 7 passed a Bay Area
#: city and no region at all, so this is what those calls have always meant.
_DEFAULT_REGION = ("CA", "Bay Area")


# ------------------------------------------------------------------------------ the client


@asynccontextmanager
async def app_client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[httpx.AsyncClient]:
    """The real application, bound to the test's disposable database.

    ``create_app(session_factory=...)`` sets ``app.state.session_factory`` outside the lifespan
    precisely so this works: ASGITransport does not run lifespan events, and the app must never
    read ``DATABASE_URL`` or build the process-wide engine during a test.
    """
    app = create_app(session_factory=session_factory)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url=BASE_URL,
        headers={"accept": ACCEPT_HTML},
    ) as client:
        yield client


# ----------------------------------------------------------------------------- row factories


async def make_company(
    session: AsyncSession,
    name: str = "Acme",
    *,
    domain: str | None = None,
    website_url: str | None = None,
    one_liner: str | None = None,
    thesis: str | None = None,
    stage: enums.Stage = enums.Stage.UNKNOWN,
    status: enums.CompanyStatus = enums.CompanyStatus.ACTIVE,
    first_seen_at: datetime = NOW,
    founded_year: int | None = None,
) -> Company:
    company = Company(
        name=name,
        normalized_name=name.lower(),
        domain=domain,
        website_url=website_url,
        one_liner=one_liner,
        thesis=thesis,
        stage=stage,
        status=status,
        first_seen_at=first_seen_at,
        last_seen_at=first_seen_at,
        founded_year=founded_year,
    )
    session.add(company)
    await session.flush()
    return company


async def make_job(
    session: AsyncSession,
    company: Company,
    title: str = "Engineer",
    *,
    external_id: str | None = None,
    url: str | None = None,
    location_text: str | None = None,
    is_remote: bool = False,
    employment_type: enums.EmploymentType = enums.EmploymentType.FULL_TIME,
    role_family: enums.RoleFamily = enums.RoleFamily.SOFTWARE,
    seniority: enums.Seniority = enums.Seniority.MID,
    flexible_signal: bool = False,
    compensation_raw: str | None = None,
    posted_at: datetime | None = NOW,
    first_seen_at: datetime = NOW,
    closed_at: datetime | None = None,
    description_raw: str | None = None,
    refresh: bool = True,
) -> Job:
    """One role. Refreshes the company's denormalized columns unless ``refresh=False``."""
    job = Job(
        company_id=company.id,
        external_id=external_id or f"job-{next(_JOB_SEQUENCE)}",
        title=title,
        url=url,
        location_text=location_text,
        is_remote=is_remote,
        employment_type=employment_type,
        role_family=role_family,
        seniority=seniority,
        flexible_signal=flexible_signal,
        compensation_raw=compensation_raw,
        posted_at=posted_at,
        first_seen_at=first_seen_at,
        last_seen_at=first_seen_at,
        closed_at=closed_at,
        description_raw=description_raw,
    )
    session.add(job)
    await session.flush()
    if refresh:
        await refresh_denormalized(session, company)
    return job


async def make_round(
    session: AsyncSession,
    company: Company,
    *,
    round_type: enums.RoundType = enums.RoundType.SEED,
    amount_usd: int | None = None,
    announced_date: date | None = None,
    investors: tuple[tuple[str, bool], ...] = (),
    refresh: bool = True,
) -> FundingRound:
    """One funding round, with ``(investor name, is_lead)`` pairs attached."""
    round_ = FundingRound(
        company_id=company.id,
        round_type=round_type,
        amount_usd=amount_usd,
        announced_date=announced_date,
    )
    session.add(round_)
    await session.flush()
    for investor_name, is_lead in investors:
        investor = await _get_or_create_investor(session, investor_name)
        session.add(RoundInvestor(round_id=round_.id, investor_id=investor.id, is_lead=is_lead))
    await session.flush()
    if refresh:
        await refresh_denormalized(session, company)
    return round_


async def make_location(
    session: AsyncSession,
    company: Company,
    city: str,
    *,
    state: str | None = None,
    metro: str | None = None,
    is_hq: bool = False,
) -> Location:
    """Link ``company`` to ``city``, creating the ``locations`` row on first use.

    The state and the metro come from :data:`CITY_REGIONS`, so a test says only *where* the
    company is and the row it gets agrees with ``config/regions.yaml`` — a Brooklyn office
    really does carry the New York metro, and a Jersey City one carries it too. Pass either
    explicitly to place a city the table does not hold, or to place the second ``Newark``.

    The lookup is by city name alone, which is also why ``uq_locations_city_state`` is queried
    on the pair: two calls naming the same city in different states are two ``locations`` rows,
    and that is precisely the state of the database the City select has to disambiguate.
    """
    known_state, known_metro = CITY_REGIONS.get(city, _DEFAULT_REGION)
    state = known_state if state is None else state
    metro = known_metro if metro is None else metro
    location = await session.scalar(
        select(Location).where(Location.city == city, Location.state == state)
    )
    if location is None:
        location = Location(city=city, state=state, metro=metro)
        session.add(location)
        await session.flush()
    session.add(CompanyLocation(company_id=company.id, location_id=location.id, is_hq=is_hq))
    await session.flush()
    return location


async def make_sector(
    session: AsyncSession, company: Company, name: str, *, slug: str | None = None
) -> Sector:
    """Link ``company`` to a sector, creating the ``sectors`` row on first use."""
    wanted = slug or name.lower().replace(" ", "-")
    sector = await session.scalar(select(Sector).where(Sector.slug == wanted))
    if sector is None:
        sector = Sector(name=name, slug=wanted)
        session.add(sector)
        await session.flush()
    session.add(CompanySector(company_id=company.id, sector_id=sector.id))
    await session.flush()
    return sector


async def make_note(
    session: AsyncSession,
    company: Company,
    *,
    status: enums.TrackingStatus = enums.TrackingStatus.INTERESTED,
    rating: int | None = None,
    note: str | None = None,
) -> UserNote:
    row = UserNote(company_id=company.id, status=status, rating=rating, note=note)
    session.add(row)
    await session.flush()
    return row


async def make_bookmark(session: AsyncSession, job: Job, *, starred: bool = True) -> JobBookmark:
    row = JobBookmark(job_id=job.id, starred=starred)
    session.add(row)
    await session.flush()
    return row


async def make_contact(
    session: AsyncSession,
    company: Company,
    *,
    kind: enums.ContactKind = enums.ContactKind.CAREERS_PAGE,
    value: str = "https://example.com/careers",
    confidence: enums.ContactConfidence = enums.ContactConfidence.PUBLISHED,
) -> Contact:
    row = Contact(company_id=company.id, kind=kind, value=value, confidence=confidence)
    session.add(row)
    await session.flush()
    return row


async def make_company_source(
    session: AsyncSession,
    company: Company,
    connector: str,
    *,
    last_seen_at: datetime = NOW,
    external_id: str | None = None,
) -> CompanySource:
    row = CompanySource(
        company_id=company.id,
        connector=connector,
        external_id=external_id,
        last_seen_at=last_seen_at,
    )
    session.add(row)
    await session.flush()
    return row


async def make_run(
    session: AsyncSession,
    connector: str,
    *,
    started_at: datetime = NOW,
    finished_at: datetime | None = None,
    status: enums.FetchRunStatus = enums.FetchRunStatus.OK,
    n_fetched: int = 0,
    n_upserted: int = 0,
    error_text: str | None = None,
) -> FetchRun:
    row = FetchRun(
        connector=connector,
        started_at=started_at,
        finished_at=finished_at,
        status=status,
        n_fetched=n_fetched,
        n_upserted=n_upserted,
        error_text=error_text,
    )
    session.add(row)
    await session.flush()
    return row


async def refresh_denormalized(session: AsyncSession, *companies: Company) -> None:
    """Recompute SPEC §5's denormalized columns and re-read them into the ORM objects.

    Every factory that touches ``jobs`` or ``funding_rounds`` calls this, because the ingest
    pipeline does the same thing at end of run: ``latest_job_posted_at`` (the default sort),
    ``open_job_count`` (the ``open_roles`` filter and the count the row displays) and
    ``latest_round_id`` (two sorts and three filters) are only true after it has run.
    """
    ids = [company.id for company in companies] if companies else None
    await refresh_company_denormalized_columns(session, ids)
    for company in companies:
        await session.refresh(company)


async def _get_or_create_investor(session: AsyncSession, name: str) -> Investor:
    investor = await session.scalar(select(Investor).where(Investor.name == name))
    if investor is None:
        investor = Investor(name=name)
        session.add(investor)
        await session.flush()
    return investor


# --------------------------------------------------------------------------- markup helpers

#: The keyset "Load more" control of ``_company_results.html`` / ``_job_results.html``.
_LOAD_MORE = re.compile(r'class="load-more"[^>]*?hx-get="([^"]+)"')
#: One collapsed company row — its lazy panel URL is the only per-row id in the markup.
_COMPANY_ROW = re.compile(r'hx-get="/company/(\d+)/panel')
#: One role, wherever it is rendered: every role carries exactly one bookmark form.
_JOB_ROW = re.compile(r'action="/jobs/(\d+)/bookmark"')


def next_link(markup: str) -> str | None:
    """The "Load more" URL, unescaped, or ``None`` when this is the last page.

    ``html.unescape`` is not optional: the URL is rendered into an attribute, so its ``&``
    separators arrive as ``&amp;`` and a client that followed them verbatim would request a
    single parameter named ``amp;cursor``.
    """
    found = _LOAD_MORE.search(markup)
    return html.unescape(found.group(1)) if found else None


def company_ids(markup: str) -> list[int]:
    """The company ids of every collapsed row in a rendered page or fragment, in order."""
    return [int(value) for value in _COMPANY_ROW.findall(markup)]


def job_ids(markup: str) -> list[int]:
    """The job ids of every role rendered — a ``/roles`` row or a panel's roles table."""
    return [int(value) for value in _JOB_ROW.findall(markup)]


#: Input types that are never form data — a button submits nothing but its own click.
_BUTTON_INPUTS = frozenset({"submit", "button", "reset", "image"})


def form_state(markup: str, selector: str = "form.filters") -> dict[str, list[str]]:
    """Every named control of the given form, mapped to the values it would submit as rendered.

    Asserting on the *submission* rather than on the ``selected``/``checked``/``value``
    attributes that produce it is deliberate, in both directions. It is what the templates
    actually owe the user — a re-rendered page must carry its own state back into its
    controls, or the next click silently submits a different query than the one on screen —
    and it survives the markup around those controls being relabelled or restyled.

    Browser rules are followed wherever they change the answer: a checkbox or radio
    contributes only when it is ``checked`` (defaulting to ``"on"`` with no ``value``), a
    ``<select>`` with nothing explicitly selected still submits its **first** option unless it
    is ``multiple``, a multi-select submits each selected option, and an empty text input
    still submits its empty value. A control that submits nothing — an unchecked box, an
    unselected multi-select — maps to an empty list, so the result also says which controls
    the form *has*; a nameless control is not form data at all and is skipped.
    """
    form = HTMLParser(markup).css_first(selector)
    assert form is not None, f"no {selector} in this response"
    state: dict[str, list[str]] = {}
    for node in form.css("input, select, textarea"):
        name = node.attributes.get("name")
        if not name:
            continue
        values: list[str] = []
        if node.tag == "select":
            options = node.css("option")
            chosen = [option for option in options if "selected" in option.attributes]
            if not chosen and options and "multiple" not in node.attributes:
                chosen = options[:1]
            values = [_option_value(option) for option in chosen]
        elif node.tag == "textarea":
            values = [_textarea_value(node)]
        else:
            kind = (node.attributes.get("type") or "text").lower()
            if kind in _BUTTON_INPUTS:
                continue
            if kind in {"checkbox", "radio"}:
                if "checked" in node.attributes:
                    values = [node.attributes.get("value") or "on"]
            else:
                values = [node.attributes.get("value") or ""]
        state.setdefault(name, []).extend(values)
    return state


def _option_value(option: Node) -> str:
    """An option's submitted value: its ``value``, or its own text when it carries none."""
    value = option.attributes.get("value")
    return value if value is not None else option.text().strip()


def _textarea_value(node: Node) -> str:
    """A textarea's submitted value: its text, minus the one leading newline HTML swallows."""
    text = node.text()
    return text[1:] if text.startswith("\n") else text


def select_options(markup: str, selector: str) -> list[tuple[str | None, str, str]]:
    """One ``<select>``'s options, in document order, as ``(group, value, label)``.

    The complement of :func:`form_state`, which answers what a form *submits* and so discards
    exactly what this returns: the ``<optgroup>`` an option sits in and the text on screen.
    Both carry meaning in the City select of SPEC §12 Phase 7 — the options are grouped one
    group per metro because six regions' worth of cities in a flat list is not a control anyone
    can use, and a label may append a state that its value deliberately does not carry, so that
    two same-named cities are distinguishable while the filter still matches on the name alone.
    ``group`` is ``None`` for an option outside any group, which is every option of every other
    select in the form.
    """
    node = HTMLParser(markup).css_first(selector)
    assert node is not None, f"no {selector} in this response"
    found: list[tuple[str | None, str, str]] = []
    for option in node.css("option"):
        parent = option.parent
        group = (
            parent.attributes.get("label")
            if parent is not None and parent.tag == "optgroup"
            else None
        )
        found.append((group, _option_value(option), option.text().strip()))
    return found


async def walk_pages(
    client: httpx.AsyncClient,
    url: str,
    ids_of: Callable[[str], list[int]] = company_ids,
    *,
    max_pages: int = 500,
) -> list[list[int]]:
    """Follow "Load more" to exhaustion, returning the ids of each page in order.

    This is the user's own path through a keyset list — the button swaps itself for the next
    rows plus the next button — so walking it proves the pagination contract end to end rather
    than just the SQL underneath it.
    """
    pages: list[list[int]] = []
    next_url: str | None = url
    while next_url is not None:
        response = await client.get(next_url)
        assert response.status_code == 200, f"{next_url} -> {response.status_code}"
        pages.append(ids_of(response.text))
        next_url = next_link(response.text)
        if len(pages) > max_pages:  # pragma: no cover - a cursor that never advances
            msg = f"pagination did not terminate after {max_pages} pages of {url}"
            raise AssertionError(msg)
    return pages


def flatten(pages: list[list[int]]) -> list[int]:
    """Every id of a :func:`walk_pages` result, concatenated in page order."""
    return [row_id for page in pages for row_id in page]
