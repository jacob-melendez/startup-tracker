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

import re
from collections.abc import Sequence
from dataclasses import replace
from datetime import date
from typing import Annotated, Any, NamedTuple

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
    Contact,
    FundingRound,
    Person,
    RoundInvestor,
    UserNote,
)
from db.queries import Filters

# Two imports out of ``ingest/``, and both are inert (SPEC §2, CLAUDE.md: no request handler
# makes an outbound call). ``ingest.config`` parses local YAML and nothing else —
# ``web.routes.runs`` already reads the connector cadences through it, and
# ``tests/test_web_acceptance.py`` exempts it from FORBIDDEN_IMPORTS on exactly those grounds.
# ``ingest.contacts`` is the pure-function home of every SPEC §6 URL rule, and its own module
# docstring is emphatic that **nothing in it ever requests linkedin.com**:
# :func:`people_search_url` formats a string. Reusing it is the point — the scoped search below
# and the company-wide one already stored as a contact are then built by one function, so they
# cannot drift into two different search expressions.
from ingest.config import LINKEDIN_NOTE_LIMIT, OutreachConfig, load_outreach_config
from ingest.contacts import people_search_url
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
from web.labels import CONTACT_KIND_ORDER, label_for
from web.templating import (
    draft_name,
    mailto_url,
    next_page_url,
    safe_url,
    shared_inbox,
    templates,
)

router = APIRouter()

#: SPEC §5's rating scale.
MIN_RATING = 1
MAX_RATING = 5

#: Rank per kind for the contacts block, from :data:`web.labels.CONTACT_KIND_ORDER`. A kind with
#: no rank (a member added to the enum before the order was updated) sorts last rather than
#: raising: SPEC §6 says the panel must show what was stored, and a missing display rule is not
#: a reason to drop a contact or take the page down.
_KIND_RANK: dict[str, int] = {kind.value: rank for rank, kind in enumerate(CONTACT_KIND_ORDER)}

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
    returned. A full page adds the three small facet lookups behind :func:`web.deps.facets`,
    which is called below rather than declared above precisely so a swap does not pay for a
    filter form it does not render. An htmx request gets ``_company_results.html`` — the same
    partial the "load more" button swaps in, which is why the button can replace itself with
    the next rows *plus* the next button.

    Nothing here enumerates the filters: :class:`web.filters.CompanyListRequestDep` resolves
    the whole set and this handler passes it along whole, which is why SPEC §12 Phase 7's
    ``metro`` needed no change on this side and why ``/roles`` cannot end up with a different
    filter row (SPEC §9's "same filter set").
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
        # SPEC §6's last clause is a UI requirement, so the split is made here — once, for both
        # handlers — rather than in the template, where two `if` branches over one list would
        # have to agree about the order as well as about the partition.
        "contacts": _group_contacts(company.contacts),
        # SPEC §12 Phase 8's whole point, and free: ``_DETAIL_OPTIONS`` has already loaded the
        # people and the contacts, so picking the best door adds no query to either handler. It
        # is built here rather than in the template for the same reason the contacts split is —
        # three tiers and an entity guard are not something two templates can be trusted to agree
        # about, and the standalone page and the htmx panel must show the same door.
        "door": best_outreach_door(company, outreach=load_outreach_config()),
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


class ContactGroups(NamedTuple):
    """One company's contacts, split the way SPEC §6 requires the UI to show them.

    ``published`` is what the company itself put on its own pages — a real address. Constructed
    rows are deterministic search shortcuts we built from the name and domain; SPEC §6: "the UI
    must visually distinguish ``published`` from ``constructed`` so it's obvious which is a real
    address and which is a search shortcut". Two lists, not one list plus a flag, so the template
    cannot render them interleaved and leave the badge as the only thing telling them apart.
    """

    published: list[Contact]
    constructed: list[Contact]


def _group_contacts(contacts: Sequence[Contact]) -> ContactGroups:
    """Partition by ``confidence``, each group in :data:`web.labels.CONTACT_KIND_ORDER` order.

    Sorting here rather than in the query because ``contacts`` is an eager-loaded collection of
    at most a handful of rows per company: an ``order_by`` on the relationship would buy nothing
    and would put a display decision in the data layer.

    Anything that is neither ``published`` nor ``constructed`` cannot exist — ``contact_confidence``
    is a two-member Postgres enum — so a plain else-branch is honest here rather than lossy.
    """
    published: list[Contact] = []
    constructed: list[Contact] = []
    for contact in contacts:
        target = (
            published if contact.confidence is enums.ContactConfidence.PUBLISHED else constructed
        )
        target.append(contact)
    for group in (published, constructed):
        group.sort(key=_contact_order)
    return ContactGroups(published=published, constructed=constructed)


def _contact_order(contact: Contact) -> tuple[int, str]:
    """Kind rank, then value — so two emails on one company list alphabetically and stably."""
    return (_KIND_RANK.get(contact.kind, len(_KIND_RANK)), contact.value)


# ------------------------------------------------------------- SPEC §12 Phase 8: the best door


#: What the drafted note calls somebody nobody has identified yet. The note names a person by
#: construction (``{person}``), and the company-wide search is precisely the door where no name is
#: known, so the name is left as something to fill in once the search comes back. Bracketed
#: because that is ``config/outreach.yaml``'s own convention for a blank the sender fills in — its
#: email body signs off with ``[your name]``.
_UNNAMED_PERSON = "[name]"

#: What separates two words of a name, for the entity guard: whitespace and the punctuation a
#: legal name is written with (``L.P.``, ``Acme, Inc.``). ``\W`` rather than a spelled-out ASCII
#: class, so an accent stays part of its word: ``[^0-9a-z]`` would cut ``Gpérez`` into ``gp`` and
#: ``rez`` and hand the ``GP`` marker a person to reject.
_NOT_A_WORD = re.compile(r"\W+")


class OutreachDoor(NamedTuple):
    """The best available way to reach a human at one company, and the draft to send through it
    (SPEC §6, §12 Phase 8).

    ``kind`` is one of four, most specific first:

    ``"email"``
        The company published an address. ``url`` is a ``mailto:`` already carrying the drafted
        subject and body, so the door opens straight into a composed message. It outranks every
        LinkedIn tier when the address reaches a *person*, and is demoted below a named human
        when it is a shared inbox — see :func:`best_outreach_door`.
    ``"profile"``
        The company published somebody's profile. ``url`` is that person's page.
    ``"person_search"``
        Somebody is named but carries no profile URL. ``url`` is a people search scoped to that
        person's name **and** the company, which is a very different thing from a search for the
        company alone: it lands on one person rather than on a list to triage.
    ``"company_search"``
        Nobody is named. ``url`` is the company-wide people search SPEC §6 already stores as the
        constructed ``linkedin_people`` contact, and ``looking_for`` names the roles to ask for.

    ``person`` and ``title`` are ``None`` exactly when nobody is named, and ``looking_for`` is
    empty exactly when somebody is — guidance about which role to look for is what a nameless
    search needs and noise beside a door that already names the person, so the two are mutually
    exclusive rather than both always filled.

    ``note`` is the draft the user *pastes*, which is why it is empty for an ``"email"`` door:
    that draft travels inside the ``mailto:`` and there is nothing to copy. The template renders
    the note box only when there is a note, rather than an empty box under every email.

    A :class:`~typing.NamedTuple` for the same reason :class:`ContactGroups` is one: nothing
    parses it, it is derived from rows already in hand, and a template reads it by attribute.
    """

    kind: str
    url: str
    person: str | None
    title: str | None
    note: str
    looking_for: tuple[str, ...]


def best_outreach_door(company: Company, *, outreach: OutreachConfig) -> OutreachDoor | None:
    """The most specific door this company offers, or ``None`` when it offers none.

    **This is the main path, not a fallback.** 397 of 10,219 companies in this database publish an
    email address (measured 2026-09-09, after the ``hn_hiring`` backfill), so for the other 9,822
    the LinkedIn door is the only door, and the panel has to make it worth opening: who to write
    to, a link that lands on them, and a note already drafted. The three LinkedIn tiers of
    :class:`OutreachDoor` are that, in descending order of how much the database happens to know.

    Who to ask for is ``contact_priority`` from ``config/outreach.yaml`` — founder before
    recruiter, which inverts the usual advice on purpose and for a measured reason: these
    companies are too small to have a talent function (2 recruiters in 17,172 stored people), and
    a recruiter fills approved headcount while only a founder can invent a role that does not
    exist yet, which is the entire premise of this phase. A person whose ``role_type`` is not in
    that list — including one with no ``role_type`` at all — is still offered, after everyone who
    is: a named human to search for beats a company-wide search, and the configured order is a
    preference rather than a filter (SPEC §7.1: classification never excludes).

    **Nothing here fetches anything.** ``linkedin_url`` is only ever a URL the company published
    on its own site (SPEC §4 excludes LinkedIn from the connectors), the search URLs are
    constructed by :func:`ingest.contacts.people_search_url`, and the note is text the user pastes
    themselves. It is also a pure function over data already loaded: ``_DETAIL_OPTIONS`` eager-loads
    ``people`` and ``contacts`` for the panel, so choosing a door costs no query.
    """
    # Folded, not merely stripped: a stored name can carry a newline in the middle (SPEC §2 keeps
    # what the source said), and this one goes into the drafted note and into the `mailto:`
    # subject. `web.templating.draft_name` is the single definition of that fold, so the note the
    # panel shows and the subject the mail client composes cannot disagree about the name.
    name = draft_name(company)
    candidates = _people_worth_messaging(company.people, outreach)
    personal, shared = _published_emails(company.contacts)

    # A personal address outranks everything: no character cap, no connection request, and it
    # reaches somebody who already publishes it expecting to be written to. A *shared* one is
    # ranked below a named human instead, because a scoped personal offer sent to a ticket queue
    # converts close to nothing — which is the measurement `role_address_prefixes` exists for.
    if personal:
        return _email_door(personal[0], name)

    for person in candidates:
        profile = safe_url(person.linkedin_url)
        if profile is not None:
            return _named_door("profile", profile, person, name, outreach)
    # A search for an empty phrase matches every company there is, which is worse than offering
    # no link at all — the same reason ``ingest.pipeline`` will not construct one for a blank name.
    if candidates and name:
        scoped = people_search_url(name, [_search_name(candidates[0].full_name)])
        return _named_door("person_search", scoped, candidates[0], name, outreach)

    if shared:
        return _email_door(shared[0], name)

    company_search = _company_search_url(company.contacts)
    if company_search is None:
        return None
    return OutreachDoor(
        kind="company_search",
        url=company_search,
        person=None,
        title=None,
        note=_drafted_note(outreach, company=name, person=_UNNAMED_PERSON),
        looking_for=tuple(label_for(role) for role in outreach.contact_priority),
    )


def _published_emails(contacts: Sequence[Contact]) -> tuple[list[str], list[str]]:
    """The company's published addresses, split into the ones that reach a person and the ones
    that reach a queue — in :data:`_KIND_RANK` order so the door is stable between two renders.

    The split is the whole point rather than a nicety. Measured on this database on 2026-09-09,
    **all 9** of the addresses crawled off company sites are a shared inbox, while 305 of the 437
    found in "Who is hiring?" comments are somebody's own — so treating the two alike would rank a
    support queue above a named founder for exactly the companies where that is the wrong call.

    The company-site half of that reads 9 of 9 rather than the 8 of 9 this phase was planned
    against, and it moved without a single row changing: those 9 addresses predate the backfill
    and none of them was touched by it. What moved is the vocabulary — ``recruiting@`` and two
    ``accommodations@`` are shared inboxes that the shipped ``role_address_prefixes`` recognises
    and a shorter list does not. A reader who re-derives this split and gets a different number
    should check the prefix list before concluding the corpus drifted.

    Constructed contacts are not considered at all: there is no such thing as a constructed
    email in this system (SPEC §6 forbids guessing an address), so anything here was published.
    """
    published = sorted(
        (
            contact
            for contact in contacts
            if contact.kind is enums.ContactKind.EMAIL
            and contact.confidence is enums.ContactConfidence.PUBLISHED
        ),
        key=_contact_order,
    )
    personal: list[str] = []
    shared: list[str] = []
    for contact in published:
        target = shared if shared_inbox(contact.value) else personal
        target.append(contact.value)
    return personal, shared


def _email_door(address: str, company: str) -> OutreachDoor:
    """The door onto a published address: a ``mailto:`` with the draft already in it.

    ``note`` is deliberately empty — the draft is *in* the URL, so there is nothing for the user
    to copy, and an empty note box under every email would be furniture. ``looking_for`` is empty
    for the same reason it is on a named door: the address already names who this reaches.
    """
    return OutreachDoor(
        kind="email",
        url=mailto_url(address, company),
        person=address,
        title=None,
        note="",
        looking_for=(),
    )


def _named_door(
    kind: str, url: str, person: Person, company: str, outreach: OutreachConfig
) -> OutreachDoor:
    """One :class:`OutreachDoor` onto a person, for the two tiers that have one.

    A title is shown only when the company published one — ``Person.title`` is NULL for everybody
    who arrived from a filing rather than from a team page — and an empty one is folded to
    ``None`` so the template has a single thing to test rather than two.
    """
    full_name = person.full_name.strip()
    title = (person.title or "").strip()
    return OutreachDoor(
        kind=kind,
        url=url,
        person=full_name,
        title=title or None,
        note=_drafted_note(outreach, company=company, person=full_name),
        looking_for=(),
    )


def _people_worth_messaging(people: Sequence[Person], outreach: OutreachConfig) -> list[Person]:
    """The company's named humans, best door first.

    Two things are decided here. Whether a row is a *person* at all: ``role_type`` comes partly
    from an SEC Form D "related persons" list, which files names like
    ``". <Something> Real Estate Debt Fund GP LLC"`` as a founder because that is what the filing
    calls it, and the panel must never draft "Hi <fund>, I'm a student" (:func:`_is_legal_entity`).
    And the order: ``contact_priority``'s rank, then ``id``, so the door a company shows does not
    change between two renders of the same row — the eager-loaded collection has no ``order_by``,
    so its order is Postgres's to choose (compare :func:`_contact_order`'s "stably"). A row that
    has never been flushed has no ``id`` and sorts as ``0``, so a unit test may build people by
    hand without inventing primary keys for them.

    The list is built with :func:`sorted` rather than sorted in place: ``company.people`` is an
    instrumented ORM collection and this is a display decision, not a fact about the company.
    """
    rank = {role: index for index, role in enumerate(outreach.contact_priority)}
    unranked = len(rank)
    named = [
        person
        for person in people
        if person.full_name.strip()
        and not _is_legal_entity(person.full_name, outreach.entity_markers)
    ]
    return sorted(
        named,
        key=lambda person: (
            unranked if person.role_type is None else rank.get(person.role_type, unranked),
            person.id or 0,
        ),
    )


def _is_legal_entity(name: str, markers: Sequence[str]) -> bool:
    """True when ``name`` carries one of the configured legal-entity markers.

    Whole words on both sides, compared after case and punctuation are folded away
    (:func:`_word_key`), which is what lets ``L.P.`` match a name written ``L. P.`` and — much
    more importantly — stops ``GP`` matching inside a surname. ``config/outreach.yaml`` says the
    same thing to whoever edits the list, and the cost of a marker is a real person passed over,
    so the guard has to be exactly as narrow as that file promises.

    An empty marker list switches the guard off, which the file documents and allows; a marker
    that folds away to nothing (``"."``) is skipped rather than matching every name, since a
    needle every name contains would silently delete the two tiers the guard exists to protect.
    """
    haystack = f" {_word_key(name)} "
    for marker in markers:
        needle = _word_key(marker)
        if needle and f" {needle} " in haystack:
            return True
    return False


def _word_key(text: str) -> str:
    """``". Northwind Fund GP LLC"`` → ``"northwind fund gp llc"``: lower-case words, one space
    between them, so a marker and a name can be compared word for word."""
    return " ".join(part for part in _NOT_A_WORD.split(text.casefold()) if part)


def _search_name(full_name: str) -> str:
    """A person's name as a search keyword.

    The double quote is dropped for the reason :func:`ingest.contacts.people_search_url` drops it
    from the company name: that function quotes a term containing whitespace, so a name carrying
    a quote of its own would close the phrase early and turn the rest of the search into loose
    keywords that match half the city.
    """
    return full_name.replace('"', " ").strip()


def _company_search_url(contacts: Sequence[Contact]) -> str | None:
    """The company-wide people search SPEC §6 stores as a constructed ``linkedin_people`` contact.

    Read off the row rather than rebuilt, so the button in the outreach block and the link in the
    contacts block below it are the same URL by construction. ``None`` when there is none — the
    pipeline constructs one for every company whose name is not blank, so this is the row that
    predates it or the row that has no name, and inventing a search for either would be a link to
    somebody else's company. Ordered by ``id`` for the same reason the people are: at most one is
    ever stored, and if a stale one survives, the panel still renders the same door twice running.
    """
    searches = sorted(
        (
            contact
            for contact in contacts
            if contact.kind is enums.ContactKind.LINKEDIN_PEOPLE and contact.value.strip()
        ),
        key=lambda contact: contact.id or 0,
    )
    return searches[0].value if searches else None


def _drafted_note(outreach: OutreachConfig, *, company: str, person: str) -> str:
    """The connection-request note, filled in and guaranteed to fit LinkedIn's cap.

    ``str.format`` cannot raise here: ``ingest.config`` rejects a template using any placeholder
    but these two on load, which is the whole reason that check lives there rather than here.

    The clamp is a last-resort guard, not a design: the loader has already measured the template
    against a representative pair of names, so it only bites for a name longer than that, and the
    fix is to shorten the template rather than to lean on this. It bites where it must, though —
    LinkedIn does not truncate a note one character over :data:`~ingest.config.LINKEDIN_NOTE_LIMIT`,
    it refuses to send it, so an unclamped draft would shut the door this whole tier exists to
    open. The cut falls on a word boundary and is marked, because a note that visibly ends
    mid-sentence is one the sender will fix before pasting.
    """
    note = outreach.linkedin_note.format(company=company, person=person)
    if len(note) <= LINKEDIN_NOTE_LIMIT:
        return note
    kept = note[: LINKEDIN_NOTE_LIMIT - 1].rstrip()
    head, _, _tail = kept.rpartition(" ")
    return f"{(head or kept).rstrip()}…"


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
