"""``/``, ``/company/{id}``, the expanded-row panel and the note form (SPEC §9).

``/`` is the primary view: one collapsed row per company, filtered and sorted server-side into
a single statement (:func:`db.queries.company_list_page`) and paginated by keyset. Expanding a
row lazy-loads ``/company/{id}/panel``, which is the same fragment ``/company/{id}`` renders
standalone.

The one rule that shapes this module: **the panel shows every role the company has open**,
whatever the page-level role filter says (SPEC §9, §7.1). The filter decides which roles are
*highlighted*, never which exist — so :func:`db.queries.company_jobs` takes no ``Filters`` at
all and the job-level parameters arrive separately as ``highlight``.

Its in-row twin ("sortable and filterable within the row", same §9 sentence) is the exception
that proves it: the ``role_*`` parameters of :class:`web.filters.PanelControls` really do sort
and narrow that table, but only the row's own controls ever set one — the collapsed row's
``hx-get`` sends none — so the panel the list opens is still every role, every time.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Annotated, Any

from fastapi import APIRouter, Form, HTTPException
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response
from starlette.status import HTTP_303_SEE_OTHER, HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND

from db import enums, queries
from db.models import (
    Company,
    CompanyLocation,
    CompanySector,
    FundingRound,
    RoundInvestor,
    UserNote,
)
from db.queries import Filters
from web.deps import HtmxDep, RowIdPath, SessionDep, facets
from web.filters import (
    CompanyListRequestDep,
    HighlightDep,
    PanelControls,
    PanelControlsDep,
    job_filter_query_string,
    panel_query_string,
    role_sort_links,
)
from web.templating import next_page_url, templates

router = APIRouter()

#: SPEC §5's rating scale.
MIN_RATING = 1
MAX_RATING = 5

#: Everything ``_company_panel.html`` reads off the ORM object, loaded in one round trip per
#: relationship instead of one per row. ``selectinload`` (not ``joinedload``) because these are
#: collections: a join would multiply the company row by rounds by locations by contacts.
_DETAIL_OPTIONS = (
    selectinload(Company.funding_rounds)
    .selectinload(FundingRound.investor_links)
    .selectinload(RoundInvestor.investor),
    selectinload(Company.location_links).selectinload(CompanyLocation.location),
    selectinload(Company.sector_links).selectinload(CompanySector.sector),
    selectinload(Company.contacts),
    selectinload(Company.people),
    selectinload(Company.source_links),
    selectinload(Company.user_note),
)


@router.get("/", response_class=HTMLResponse)
async def company_list(
    request: Request,
    session: SessionDep,
    listing: CompanyListRequestDep,
    htmx: HtmxDep,
) -> Response:
    """SPEC §9's company list: filters, sort, 50 rows, and a keyset token for the next 50.

    Four statements on a fragment render, and none of them per row: the page itself, then the
    three grouped statements :func:`db.queries.company_card_extras` runs for the ≤50 ids it
    returned. A full page adds the two small facet lookups behind :func:`web.deps.facets`,
    which is called below rather than declared above precisely so a swap does not pay for a
    filter form it does not render. An htmx request gets ``_company_results.html`` — the same
    partial the "load more" button swaps in, which is why the button can replace itself with
    the next rows *plus* the next button.
    """
    page = await queries.company_list_page(
        session, filters=listing.filters, sort=listing.sort, cursor=listing.cursor
    )
    extras = await queries.company_card_extras(session, [row.id for row in page.rows])
    context: dict[str, Any] = {
        "page": page,
        "extras": extras,
        "next_url": next_page_url(request, page.next_cursor),
        "filters": listing.filters,
        "sort": listing.sort,
        "job_filter_qs": job_filter_query_string(listing.filters),
        "form_action": "/",
        "page_title": "Companies",
    }
    if htmx:
        return templates.TemplateResponse(request, "_company_results.html", context)
    context["facets"] = await facets(session)
    return templates.TemplateResponse(request, "companies.html", context)


@router.get("/company/{company_id}", response_class=HTMLResponse)
async def company_detail(
    request: Request,
    company_id: RowIdPath,
    session: SessionDep,
    highlight: HighlightDep,
    controls: PanelControlsDep,
) -> Response:
    """The permalink detail page — the expanded row, standalone and linkable (SPEC §9).

    It answers the same parameters as the fragment below, which is what makes the row's own
    sort links work with JavaScript switched off: the header is an ordinary ``<a>`` pointing
    here, and the page it lands on is the same table in the same order.
    """
    context = await _panel_context(session, company_id, highlight, controls)
    context["standalone"] = True
    context["page_title"] = context["company"].name
    return templates.TemplateResponse(request, "company.html", context)


@router.get("/company/{company_id}/panel", response_class=HTMLResponse)
async def company_panel(
    request: Request,
    company_id: RowIdPath,
    session: SessionDep,
    highlight: HighlightDep,
    controls: PanelControlsDep,
) -> Response:
    """The expanded row, lazy-loaded by ``<details hx-trigger="toggle once">``.

    Accepts the job-level filter parameters (``family``, ``employment``, ``seniority``,
    ``flexible``) and passes them along as ``highlight``. They mark matching roles; they never
    remove one — the company row promised a role count, and the panel has to be able to show
    every one of them (SPEC §9).

    The ``role_*`` parameters are the other half of that same §9 sentence, "sortable and
    filterable within the row", and they are the opposite kind of thing: the row's own controls
    re-request this URL with them, and the page never sends one, so the panel the list opens is
    always the complete list of roles.
    """
    context = await _panel_context(session, company_id, highlight, controls)
    context["standalone"] = False
    return templates.TemplateResponse(request, "_company_panel.html", context)


@router.post("/company/{company_id}/note", response_class=HTMLResponse)
async def save_note(
    request: Request,
    company_id: RowIdPath,
    session: SessionDep,
    htmx: HtmxDep,
    status: Annotated[str, Form()] = "",
    rating: Annotated[str, Form()] = "",
    note: Annotated[str, Form()] = "",
) -> Response:
    """Upsert the user's status, rating and note for one company (SPEC §5, §9).

    One statement — ``INSERT ... ON CONFLICT (company_id) DO UPDATE`` — because ``user_notes``
    has exactly one row per company and a read-then-write would need a transaction dance to say
    the same thing. ``updated_at`` is set explicitly: the model's ORM-level ``onupdate`` fires
    for an ORM flush, not for a Core upsert, so leaving it out would freeze the timestamp at
    the first save.

    Without htmx the form is a plain ``POST``, so it answers 303 to the detail page — a reload
    then re-renders the page instead of re-posting the form.
    """
    if not await session.scalar(select(Company.id).where(Company.id == company_id)):
        raise HTTPException(HTTP_404_NOT_FOUND, f"No company with id {company_id}.")
    tracking = _parse_status(status)
    stars = _parse_rating(rating)
    # An emptied textarea clears the note rather than storing "". U+0000 is removed first
    # because a Postgres ``text`` column cannot hold it at all, and a pasted NUL would
    # otherwise raise CharacterNotInRepertoireError from inside the INSERT (see
    # ``web.filters._text``, which does the same for the search box).
    text = note.replace("\x00", "").strip() or None

    upsert = (
        pg_insert(UserNote)
        .values(company_id=company_id, status=tracking, rating=stars, note=text)
        .on_conflict_do_update(
            index_elements=[UserNote.company_id],
            set_={"status": tracking, "rating": stars, "note": text, "updated_at": func.now()},
        )
        .returning(UserNote)
    )
    saved = (await session.scalars(upsert)).one()

    if not htmx:
        return RedirectResponse(f"/company/{company_id}", status_code=HTTP_303_SEE_OTHER)
    context: dict[str, Any] = {"company_id": company_id, "note": saved, "saved": True}
    return templates.TemplateResponse(request, "_note_form.html", context)


async def _panel_context(
    session: AsyncSession, company_id: int, highlight: Filters, controls: PanelControls
) -> dict[str, Any]:
    """Everything the panel renders, for both the fragment and its standalone permalink.

    One builder for two handlers because the two must not drift: ``/company/{id}`` is what the
    row's own links point at when JavaScript is off, so a difference between them would show
    up as a table that reorders under htmx and not without it.

    The links are built here rather than in the template because they are the same three
    decisions in six places — which column, which direction, what to carry forward — and
    :func:`web.filters.role_sort_links` is the one place that knows all three.
    """
    company = await _load_company(session, company_id)
    jobs = await queries.company_jobs(
        session,
        company_id,
        include_closed=controls.include_closed,
        sort=controls.sort,
        descending=controls.descending,
        title_contains=controls.title_q,
    )
    return {
        "company": company,
        "jobs": jobs,
        "highlight": highlight,
        "controls": controls,
        "role_sort_qs": role_sort_links(highlight, controls),
        # Where "show every role" goes: the same panel with the two narrowing controls off.
        "roles_reset_qs": panel_query_string(
            highlight, replace(controls, title_q=None, include_closed=False)
        ),
        "note": company.user_note,
    }


async def _load_company(session: AsyncSession, company_id: int) -> Company:
    """The company plus every relationship the panel renders, or a 404.

    An ordinary ORM read (SPEC §3: ``db/queries.py`` is for the genuinely complex statements),
    kept in the route because it is one ``select()`` with eager-load options.
    """
    company = await session.scalar(
        select(Company).where(Company.id == company_id).options(*_DETAIL_OPTIONS)
    )
    if company is None:
        raise HTTPException(HTTP_404_NOT_FOUND, f"No company with id {company_id}.")
    # Newest round first (SPEC §9's funding history table). Sorting the loaded collection in
    # place is safe and invisible to the session: ``list.sort`` is not one of the instrumented
    # collection methods, and no ordering is persisted anyway — there is no order column.
    company.funding_rounds.sort(key=_round_order, reverse=True)
    return company


def _round_order(round_: FundingRound) -> tuple[date, int]:
    """``announced_date DESC NULLS LAST, id DESC`` once reversed: an undated round sorts last."""
    return (round_.announced_date or date.min, round_.id)


def _parse_status(value: str) -> enums.TrackingStatus:
    """The submitted tracking status; a blank field means "not tracked" (SPEC §5)."""
    if not value.strip():
        return enums.TrackingStatus.NONE
    try:
        return enums.TrackingStatus(value.strip())
    except ValueError:
        known = ", ".join(member.value for member in enums.TrackingStatus)
        raise HTTPException(
            HTTP_400_BAD_REQUEST, f"unknown status {value!r}; expected one of: {known}"
        ) from None


def _parse_rating(value: str) -> int | None:
    """``""`` clears the rating; anything outside 1 to 5 is a 400, not a database error.

    ``user_notes`` carries a ``rating BETWEEN 1 AND 5`` check constraint, so an out-of-range
    value would otherwise surface as an IntegrityError 500 halfway through the request.
    """
    if not value.strip():
        return None
    try:
        stars = int(value.strip())
    except ValueError:
        raise HTTPException(
            HTTP_400_BAD_REQUEST, f"rating must be a whole number or empty, got {value!r}"
        ) from None
    if not MIN_RATING <= stars <= MAX_RATING:
        raise HTTPException(
            HTTP_400_BAD_REQUEST, f"rating must be between {MIN_RATING} and {MAX_RATING}"
        )
    return stars
