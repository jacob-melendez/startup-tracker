"""Query parameters in, :mod:`db.queries` value objects out (SPEC §9).

This module is the whole contract between a URL and a statement. The parameter names are the
public surface of the app — the filter form posts them, "load more" carries them, a bookmarked
URL preserves them — so they are short and stable:

===================  ========  ==================================================
parameter            repeats?  meaning
===================  ========  ==================================================
``q``                no        search text (blank is *not* a filter)
``city``             yes       ``locations.city``, by name — see below
``metro``            yes       ``locations.metro`` — the UI's "Region" select
``sector``           yes       ``sectors.slug``
``stage``            yes       ``companies.stage``
``round``            yes       latest round type
``amount_min``       no        latest round amount, whole US dollars
``amount_max``       no        latest round amount, whole US dollars
``round_months``     no        latest round announced within N months (1 to 120)
``family``           yes       ``jobs.role_family`` (all 19 values)
``employment``       yes       ``jobs.employment_type``
``seniority``        yes       ``jobs.seniority``
``flexible``         no        ``jobs.flexible_signal`` only — opt-in, never default
``open_roles``       no        ``companies.open_job_count > 0``
``tracking``         yes       ``user_notes.status`` (``none`` includes "no row")
``closed``           no        include closed roles — ``/roles`` only
``sort``             no        a ``CompanySort`` / ``JobSort`` value
``cursor``           no        an opaque keyset token from the previous page
``role_sort``        no        one expanded row's table: the column it is sorted by
``role_dir``         no        ...that column's direction, ``asc`` or ``desc``
``role_q``           no        ...a substring of the role title, narrowing the table
``role_closed``      no        ...list the company's closed roles too
===================  ========  ==================================================

``city`` and ``metro`` are the two geography filters (SPEC §12 Phase 7), and they are separate
``AND`` predicates rather than a hierarchy — picking a region does not repopulate the city
select, and does not need to. **``city`` matches on the city name alone**: a name that occurs
in two regions matches in both of them, deliberately, and ``metro`` is how you narrow it to
one. Neither value carries a state on the wire, so every URL bookmarked while there was only
one region still means what it meant, and the City select is free to append a state to an
*ambiguous label* without changing what it submits.

Both are also the exception to the "a malformed value is a 400" rule below: their vocabularies
are strings the connectors stored, not enum members, so an unrecognised value is an empty
result page rather than an error. There is no closed list to check them against that would
still be right after the next run.

Four rules the whole module follows:

* **Unknown parameters are ignored.** A stale bookmark or a tracking parameter appended by
  something else must not 400 a page.
* **An empty value means "unset", never "invalid".** A GET form submits every field it has,
  including the empty ones, so ``?q=&amount_min=&sort=`` has to mean exactly the default view.
* **A malformed value is a 400, not a 422 JSON blob.** Every parameter is declared as a plain
  string and parsed here, raising :class:`~fastapi.HTTPException` with a message naming the
  parameter and listing what it accepts. Declaring the enum parameters as
  ``Annotated[list[Stage], Query()]`` and letting FastAPI validate them would have been
  shorter, but it makes ``?stage=`` — an emptied parameter, which is the second rule's whole
  point — indistinguishable from ``?stage=serious``. The app still renders
  :class:`RequestValidationError` as a 400 page, for the *path* parameters that remain typed.
* **The four ``role_`` parameters belong to one expanded row, never to the page.** SPEC §9
  asks for the row's roles table to be "sortable and filterable within the row" while §7.1
  forbids the page-level role filter from removing anything from it, so the two vocabularies
  are kept apart by name. ``/company/{id}`` already carries
  ``family``/``employment``/``seniority``/``flexible`` as the *highlight*, and a bare ``sort``
  there would read as a ``CompanySort`` everywhere else; the prefix is what stops either set
  leaking into the other.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Annotated
from urllib.parse import urlencode

from fastapi import Depends, HTTPException, Query
from starlette import status

from db import enums
from db.queries import (
    CompanySort,
    Cursor,
    CursorError,
    Filters,
    JobSort,
    RoleSort,
    role_sort_descending,
)

#: Values accepted for a boolean parameter. Anything else — including a missing parameter and
#: the ``off``/``0`` an unchecked box never sends — is False. Checkboxes are opt-in by design
#: (SPEC §7.1: ``flexible_signal`` "is never a default filter").
TRUE_VALUES = frozenset({"1", "true", "on", "yes"})

#: SPEC §9's "last round recency (within N months)". Ten years is not a filter any more.
MAX_ROUND_MONTHS = 120

#: ``funding_rounds.amount_usd`` is a ``BIGINT`` (SPEC §5). A larger number is not a wider
#: filter — asyncpg refuses to bind it and raises ``DataError`` from inside the statement — so
#: the bound belongs here, with the rest of the "a malformed value is a 400" rule.
MAX_AMOUNT_USD = 9_223_372_036_854_775_807


@dataclass(frozen=True, slots=True, kw_only=True)
class CompanyListRequest:
    """Everything ``GET /`` needs from the URL."""

    filters: Filters
    sort: CompanySort
    cursor: Cursor | None


@dataclass(frozen=True, slots=True, kw_only=True)
class JobListRequest:
    """Everything ``GET /roles`` needs from the URL."""

    filters: Filters
    sort: JobSort
    cursor: Cursor | None


@dataclass(frozen=True, slots=True, kw_only=True)
class PanelControls:
    """How one expanded row orders and narrows its own roles table (SPEC §9).

    Separate from :class:`Filters` because it is a different kind of thing: a ``Filters`` is
    the page's question, applied before the row exists, while this is what the user did to the
    table in front of them. Nothing here is ever set by the page — the panel URL the collapsed
    row builds carries none of these parameters — so the first render of any panel lists every
    role the company has open (§9, §7.1).
    """

    sort: RoleSort
    descending: bool
    #: A substring of the role title, or ``None`` — the row's own search box, not the page's.
    title_q: str | None
    include_closed: bool

    @property
    def is_filtered(self) -> bool:
        """True when the in-row controls change *which* roles the table lists.

        Sorting is not part of it: it reorders, it never hides. The panel uses this to say so
        out loud and offer the way back, because while it is true the count in the heading is
        no longer the company's whole open-role count.
        """
        return self.title_q is not None or self.include_closed


def _flag(value: str | None) -> bool:
    return value is not None and value.strip().lower() in TRUE_VALUES


def _text(value: str) -> str:
    """One free-text parameter, trimmed and stripped of the one character Postgres cannot hold.

    A ``text`` column (and a ``tsquery``, and pg_trgm's ``%``) rejects U+0000 outright, so
    ``?q=%00`` would otherwise reach asyncpg and raise ``CharacterNotInRepertoireError`` —
    a 500 from a URL anyone can type. It is dropped rather than refused because a NUL in a
    search box is never a value the user meant: what remains is what they typed.
    """
    return value.replace("\x00", "").strip()


def _clean(values: list[str] | None) -> tuple[str, ...]:
    """Trim, drop blanks, de-duplicate, keep order — for the free-text multi-selects."""
    cleaned = [trimmed for item in values or [] if item and (trimmed := _text(item))]
    return tuple(dict.fromkeys(cleaned))


def _enum_list[E: StrEnum](name: str, members: type[E], values: list[str] | None) -> tuple[E, ...]:
    """A repeated enum parameter: blanks dropped, duplicates collapsed, order preserved.

    Parsed by hand rather than declared as ``Annotated[list[Stage], Query()]`` for the same
    reason as :func:`_whole_number`: an empty submitted field has to mean *unset*. FastAPI
    would reject ``?stage=`` as a validation error, which contradicts this module's rule that
    only a **wrong** value is an error — and a URL with an emptied parameter is exactly what a
    hand-edit or a form round trip produces.

    A genuinely unknown value is still a 400, and the message names the parameter and lists
    what it accepts, which is more use in a browser than pydantic's default rendering.
    Repeats are harmless in SQL (``IN`` is a set) but would double a chip in the UI and make
    two identical URLs look different, so they are collapsed here.
    """
    parsed: list[E] = []
    for raw in values or []:
        text = raw.strip()
        if not text:
            continue
        try:
            parsed.append(members(text))
        except ValueError:
            known = ", ".join(member.value for member in members)
            raise _bad_request(f"unknown {name} {raw!r}; expected one of: {known}") from None
    return tuple(dict.fromkeys(parsed))


def _bad_request(message: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)


def _whole_number(
    name: str, raw: str | None, *, minimum: int, maximum: int | None = None
) -> int | None:
    """An integer parameter that tolerates an empty submitted field but not a bad value.

    Parsed by hand rather than declared as ``int | None`` because an HTML form submits
    ``amount_min=`` for an untouched number input, and FastAPI would reject that as a 422.
    """
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw.strip())
    except ValueError:
        raise _bad_request(f"{name} must be a whole number, got {raw!r}") from None
    if value < minimum or (maximum is not None and value > maximum):
        upper = "" if maximum is None else f" and at most {maximum}"
        raise _bad_request(f"{name} must be at least {minimum}{upper}, got {value}")
    return value


def _parse_sort[S: StrEnum](options: type[S], raw: str | None, default: S) -> S:
    """A sort parameter, defaulting when absent or empty and 400ing when unknown."""
    if raw is None or not raw.strip():
        return default
    try:
        return options(raw.strip())
    except ValueError:
        known = ", ".join(member.value for member in options)
        raise _bad_request(f"unknown sort {raw!r}; expected one of: {known}") from None


def _parse_direction(raw: str | None, *, default: bool) -> bool:
    """``role_dir=asc|desc``, defaulting to the sorted column's own direction.

    Absent means "whatever this column reads naturally" rather than a fixed direction, which
    is what lets a header link omit the parameter entirely until the user reverses it.
    """
    if raw is None or not raw.strip():
        return default
    text = raw.strip().lower()
    if text not in {"asc", "desc"}:
        raise _bad_request(f"unknown role_dir {raw!r}; expected one of: asc, desc")
    return text == "desc"


def _parse_cursor(raw: str | None, sort: StrEnum) -> Cursor | None:
    """Decode ``cursor=``, or 400.

    Two ways to fail, both meaning "this token cannot be trusted": it does not decode
    (:class:`~db.queries.CursorError`), or it was minted under a different sort, in which case
    its key is a value of the *old* sort column and applying it would silently return an
    arbitrary slice. ``web.templating.query_string`` drops the cursor from every link that
    changes anything, so reaching either branch means a hand-edited or stale URL.
    """
    if raw is None or not raw.strip():
        return None
    try:
        cursor = Cursor.decode(raw.strip())
    except CursorError as exc:
        raise _bad_request(f"cursor is not a valid pagination token: {exc}") from exc
    if cursor.sort != sort.value:
        raise _bad_request(
            f"cursor belongs to the {cursor.sort!r} ordering, not {sort.value!r}; "
            "start again from the first page"
        )
    return cursor


def shared_filters(
    q: Annotated[str | None, Query()] = None,
    city: Annotated[list[str] | None, Query()] = None,
    metro: Annotated[list[str] | None, Query()] = None,
    sector: Annotated[list[str] | None, Query()] = None,
    stage: Annotated[list[str] | None, Query()] = None,
    round_type: Annotated[list[str] | None, Query(alias="round")] = None,
    amount_min: Annotated[str | None, Query()] = None,
    amount_max: Annotated[str | None, Query()] = None,
    round_months: Annotated[str | None, Query()] = None,
    family: Annotated[list[str] | None, Query()] = None,
    employment: Annotated[list[str] | None, Query()] = None,
    seniority: Annotated[list[str] | None, Query()] = None,
    flexible: Annotated[str | None, Query()] = None,
    open_roles: Annotated[str | None, Query()] = None,
    tracking: Annotated[list[str] | None, Query()] = None,
) -> Filters:
    """The filter set both ``/`` and ``/roles`` accept — SPEC §9's "same filter set".

    Declared once and depended on by both page dependencies, so the two views cannot drift
    apart. ``closed`` is deliberately absent: it belongs to ``/roles`` alone
    (:func:`job_list_request` adds it).

    Being declared here is also the whole of what SPEC §12 Phase 7's region selector needed on
    this side: ``metro`` reaches ``/`` and ``/roles`` because they both depend on this function,
    not because either route was taught about it. A parameter added to one route instead would
    be the drift this function exists to prevent.
    """
    text = _text(q) if q else ""
    return Filters(
        q=text or None,
        cities=_clean(city),
        # Cleaned exactly like ``city`` — same free text from the same table, so the same rule:
        # trimmed, blanks dropped (an emptied ``?metro=`` is "unset"), duplicates collapsed, and
        # an unknown value left to return nothing rather than raising.
        metros=_clean(metro),
        sector_slugs=_clean(sector),
        stages=_enum_list("stage", enums.Stage, stage),
        round_types=_enum_list("round", enums.RoundType, round_type),
        amount_min_usd=_whole_number("amount_min", amount_min, minimum=0, maximum=MAX_AMOUNT_USD),
        amount_max_usd=_whole_number("amount_max", amount_max, minimum=0, maximum=MAX_AMOUNT_USD),
        round_within_months=_whole_number(
            "round_months", round_months, minimum=1, maximum=MAX_ROUND_MONTHS
        ),
        role_families=_enum_list("family", enums.RoleFamily, family),
        employment_types=_enum_list("employment", enums.EmploymentType, employment),
        seniorities=_enum_list("seniority", enums.Seniority, seniority),
        flexible_only=_flag(flexible),
        has_open_roles=_flag(open_roles),
        tracking_statuses=_enum_list("tracking", enums.TrackingStatus, tracking),
    )


FiltersDep = Annotated[Filters, Depends(shared_filters)]


def company_list_request(
    filters: FiltersDep,
    sort: Annotated[str | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
) -> CompanyListRequest:
    """``GET /`` — filters plus the company sort and its keyset cursor."""
    company_sort = _parse_sort(CompanySort, sort, CompanySort.RECENT_JOB)
    return CompanyListRequest(
        filters=filters, sort=company_sort, cursor=_parse_cursor(cursor, company_sort)
    )


def job_list_request(
    filters: FiltersDep,
    sort: Annotated[str | None, Query()] = None,
    cursor: Annotated[str | None, Query()] = None,
    closed: Annotated[str | None, Query()] = None,
) -> JobListRequest:
    """``GET /roles`` — the shared filters plus ``closed``, the job sort and its cursor.

    An affirmative ``closed`` (the ``closed=1`` the checkbox sends, or any other
    :data:`TRUE_VALUES` spelling) is the only way a closed role is ever listed: SPEC §2 keeps
    closed jobs forever as the historical record, and §9's list is of openings.
    """
    job_sort = _parse_sort(JobSort, sort, JobSort.POSTED)
    return JobListRequest(
        filters=replace(filters, include_closed_jobs=_flag(closed)),
        sort=job_sort,
        cursor=_parse_cursor(cursor, job_sort),
    )


def job_highlight_filters(
    family: Annotated[list[str] | None, Query()] = None,
    employment: Annotated[list[str] | None, Query()] = None,
    seniority: Annotated[list[str] | None, Query()] = None,
    flexible: Annotated[str | None, Query()] = None,
) -> Filters:
    """The job-level filters alone — what ``/company/{id}/panel`` highlights with.

    The panel lists *every* role of the company (SPEC §9), so these four never reach a WHERE
    clause; they only decide which rows get a ``.match`` class. Company-level parameters are
    not accepted here because they cannot mark anything.
    """
    return Filters(
        role_families=_enum_list("family", enums.RoleFamily, family),
        employment_types=_enum_list("employment", enums.EmploymentType, employment),
        seniorities=_enum_list("seniority", enums.Seniority, seniority),
        flexible_only=_flag(flexible),
    )


def panel_controls(
    role_sort: Annotated[str | None, Query()] = None,
    role_dir: Annotated[str | None, Query()] = None,
    role_q: Annotated[str | None, Query()] = None,
    role_closed: Annotated[str | None, Query()] = None,
) -> PanelControls:
    """The expanded row's own sort and filter — SPEC §9's "sortable and filterable within
    the row".

    All four are absent until the user operates a control *inside* a row: the collapsed row's
    ``hx-get`` builds the panel URL out of :func:`job_filter_query_string`, which emits only
    the four highlight parameters. That is not an accident of the markup but the thing that
    keeps §9's "showing *all* roles regardless of the page-level role filter" and §7.1's
    "classification never excludes" true of every panel the list opens.

    Parsed exactly like the page-level parameters — an empty value is "unset", an unknown one
    is a 400 naming what it accepts — so the row's controls behave the way the rest of the URL
    surface does.
    """
    sort = _parse_sort(RoleSort, role_sort, RoleSort.POSTED)
    return PanelControls(
        sort=sort,
        descending=_parse_direction(role_dir, default=role_sort_descending(sort)),
        title_q=_text(role_q) or None if role_q else None,
        include_closed=_flag(role_closed),
    )


CompanyListRequestDep = Annotated[CompanyListRequest, Depends(company_list_request)]
JobListRequestDep = Annotated[JobListRequest, Depends(job_list_request)]
HighlightDep = Annotated[Filters, Depends(job_highlight_filters)]
PanelControlsDep = Annotated[PanelControls, Depends(panel_controls)]


def _job_filter_pairs(filters: Filters) -> list[tuple[str, str]]:
    """The four job-level parameters as ``(name, value)`` pairs, in the table's order."""
    pairs: list[tuple[str, str]] = []
    pairs.extend(("family", member.value) for member in filters.role_families)
    pairs.extend(("employment", member.value) for member in filters.employment_types)
    pairs.extend(("seniority", member.value) for member in filters.seniorities)
    if filters.flexible_only:
        pairs.append(("flexible", "1"))
    return pairs


def job_filter_query_string(filters: Filters) -> str:
    """The job-level parameters as a query string, ``""`` when none are set.

    A company row hands this to its panel endpoint
    (``hx-get="/company/7/panel{{ job_filter_qs }}"``) so the expanded row can mark the roles
    the page-level filter selected. Empty means empty — the value is concatenated onto a path,
    so a bare ``"?"`` would be noise in every unfiltered URL.
    """
    pairs = _job_filter_pairs(filters)
    if not pairs:
        return ""
    return "?" + urlencode(pairs)


def panel_query_string(highlight: Filters, controls: PanelControls) -> str:
    """A panel URL's whole query string: the highlight parameters plus the in-row controls.

    Every link and form inside the expanded row is built from this, which is what makes
    re-sorting keep the row's filter, filtering keep its sort, and both keep the ``.match``
    highlight the page handed down. It is the return journey of
    :func:`job_filter_query_string`: that one travels page → row and carries the four
    highlight parameters, this one is the row talking to itself and carries those four back
    plus its own state.

    Minimal by construction — a parameter appears only when it differs from the default — so
    the untouched panel is still the bare path, and what is in the address bar is exactly what
    the user changed. ``""`` when there is nothing to carry, for the same reason as above.
    """
    pairs = _job_filter_pairs(highlight)
    if controls.sort is not RoleSort.POSTED:
        pairs.append(("role_sort", controls.sort.value))
    if controls.descending != role_sort_descending(controls.sort):
        pairs.append(("role_dir", "desc" if controls.descending else "asc"))
    if controls.title_q is not None:
        pairs.append(("role_q", controls.title_q))
    if controls.include_closed:
        pairs.append(("role_closed", "1"))
    if not pairs:
        return ""
    return "?" + urlencode(pairs)


def role_sort_links(highlight: Filters, controls: PanelControls) -> dict[str, str]:
    """One query string per sortable column of the in-row table, keyed by the column's value.

    The column that is already sorted links to *itself reversed*; every other column links to
    its own natural direction, so a first click on "Title" reads A→Z instead of inheriting the
    descending order "Posted" was using.

    Keyed by ``RoleSort``'s value rather than by the member because a ``StrEnum`` hashes by
    member *name*: a template asking for ``role_sort_qs['title']`` would miss a dict keyed by
    ``RoleSort.TITLE``, quietly, and render an empty href.
    """
    links: dict[str, str] = {}
    for column in RoleSort:
        descending = (
            not controls.descending if column is controls.sort else role_sort_descending(column)
        )
        links[column.value] = panel_query_string(
            highlight, replace(controls, sort=column, descending=descending)
        )
    return links
