"""``/roles`` and the job bookmark toggle (SPEC §9).

The flat "what opened this week" view: one row per job across every company, the same filter
set as ``/``, default ``posted_at DESC``. Classification never excludes here either — with no
filters applied every open role of every one of the 19 families is in this list (SPEC §7.1), and
``flexible_signal`` is a badge plus an opt-in filter, never a default one.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response
from starlette.status import HTTP_303_SEE_OTHER, HTTP_404_NOT_FOUND

from db import queries
from db.models import Job, JobBookmark
from web.deps import HtmxDep, RowIdPath, SessionDep, facets
from web.filters import JobListRequestDep
from web.templating import next_page_url, templates

router = APIRouter()


@router.get("/roles", response_class=HTMLResponse)
async def role_list(
    request: Request,
    session: SessionDep,
    listing: JobListRequestDep,
    htmx: HtmxDep,
) -> Response:
    """One page of roles. A ``closed`` flag is the only way a closed role appears (SPEC §2, §9).

    One statement on a fragment render: the page of rows. A full page adds the two facet
    lookups, which are called here rather than declared as a dependency because only the full
    page renders the filter form that reads them (see :func:`web.deps.facets`).
    """
    page = await queries.job_list_page(
        session, filters=listing.filters, sort=listing.sort, cursor=listing.cursor
    )
    context: dict[str, Any] = {
        "page": page,
        "next_url": next_page_url(request, page.next_cursor),
        "filters": listing.filters,
        "sort": listing.sort,
        "form_action": "/roles",
        "page_title": "Roles",
    }
    if htmx:
        return templates.TemplateResponse(request, "_job_results.html", context)
    context["facets"] = await facets(session)
    return templates.TemplateResponse(request, "roles.html", context)


@router.post("/jobs/{job_id}/bookmark", response_class=HTMLResponse)
async def toggle_bookmark(
    request: Request, job_id: RowIdPath, session: SessionDep, htmx: HtmxDep
) -> Response:
    """Star or un-star one role.

    Two statements: the existence check that turns an unknown id into a 404 rather than a
    foreign-key error, then the toggle itself as a single
    ``INSERT ... ON CONFLICT (job_id) DO UPDATE SET starred = NOT job_bookmarks.starred
    RETURNING starred``. The first click inserts a starred row, every later click flips the
    stored value, and the value Postgres returns is what the fragment renders — so the button
    can never disagree with the table, even if two tabs click it at once.
    """
    if not await session.scalar(select(Job.id).where(Job.id == job_id)):
        raise HTTPException(HTTP_404_NOT_FOUND, f"No job with id {job_id}.")
    toggle = (
        pg_insert(JobBookmark)
        .values(job_id=job_id, starred=True)
        .on_conflict_do_update(
            index_elements=[JobBookmark.job_id],
            # The bare column reference is the *existing* row's value inside DO UPDATE.
            set_={"starred": ~JobBookmark.__table__.c.starred},
        )
        .returning(JobBookmark.starred)
    )
    starred = await session.scalar(toggle)

    if not htmx:
        return RedirectResponse("/roles", status_code=HTTP_303_SEE_OTHER)
    context: dict[str, Any] = {"job_id": job_id, "starred": bool(starred)}
    return templates.TemplateResponse(request, "_bookmark.html", context)
