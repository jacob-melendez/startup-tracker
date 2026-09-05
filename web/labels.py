"""Human labels for the controlled vocabularies of :mod:`db.enums`, plus the order the filter
selects render them in (SPEC §9).

These are **display strings, not classification rules**. CLAUDE.md's "no keywords in code" rule
governs ``config/classifiers.yaml``, which decides what a role *is*; nothing in this module can
change a query result — it only decides that ``ml_ai`` reads as "ML / AI" and ``qa`` as "QA".
Labels therefore live next to the templates that use them rather than in YAML, where a typo
would silently stop matching a keyword.

The ``*_ORDER`` tuples are the select-option order: the enum's declaration order (which is the
funding ladder for stages and the SPEC §7.1 table order for role families), with the ``other`` /
``unknown`` fallbacks pushed to the end where a reader expects them.
:data:`CONTACT_KIND_ORDER` is the exception — it orders a rendered list rather than a select,
so it is written out by hand rather than derived from the enum.
"""

from __future__ import annotations

from enum import StrEnum

from db import enums

#: What every filter renders for "no value" — one dash, used by the templating filters too.
EM_DASH = "—"

#: Values that are a fallback rather than a real choice; last in every ``*_ORDER`` tuple.
_FALLBACK_VALUES = frozenset({"other", "unknown"})


STAGE_LABELS: dict[enums.Stage, str] = {
    enums.Stage.PRE_SEED: "Pre-seed",
    enums.Stage.SEED: "Seed",
    enums.Stage.SERIES_A: "Series A",
    enums.Stage.SERIES_B: "Series B",
    enums.Stage.SERIES_C: "Series C",
    enums.Stage.SERIES_D_PLUS: "Series D+",
    enums.Stage.GROWTH: "Growth",
    enums.Stage.PUBLIC: "Public",
    enums.Stage.UNKNOWN: "Unknown",
}

COMPANY_STATUS_LABELS: dict[enums.CompanyStatus, str] = {
    enums.CompanyStatus.ACTIVE: "Active",
    enums.CompanyStatus.ACQUIRED: "Acquired",
    enums.CompanyStatus.DEAD: "Dead",
}

ATS_PROVIDER_LABELS: dict[enums.AtsProvider, str] = {
    enums.AtsProvider.GREENHOUSE: "Greenhouse",
    enums.AtsProvider.LEVER: "Lever",
    enums.AtsProvider.ASHBY: "Ashby",
    enums.AtsProvider.WORKABLE: "Workable",
}

ROUND_TYPE_LABELS: dict[enums.RoundType, str] = {
    enums.RoundType.PRE_SEED: "Pre-seed",
    enums.RoundType.SEED: "Seed",
    enums.RoundType.SERIES_A: "Series A",
    enums.RoundType.SERIES_B: "Series B",
    enums.RoundType.SERIES_C: "Series C",
    enums.RoundType.SERIES_D_PLUS: "Series D+",
    enums.RoundType.CONVERTIBLE_NOTE: "Convertible note",
    enums.RoundType.SAFE: "SAFE",
    enums.RoundType.DEBT: "Debt",
    enums.RoundType.GRANT: "Grant",
    enums.RoundType.OTHER: "Other",
    enums.RoundType.UNKNOWN: "Unknown",
}

ROLE_TYPE_LABELS: dict[enums.RoleType, str] = {
    enums.RoleType.FOUNDER: "Founder",
    enums.RoleType.EXEC: "Exec",
    enums.RoleType.RECRUITER: "Recruiter",
    enums.RoleType.ENG_LEAD: "Eng lead",
}

CONTACT_KIND_LABELS: dict[enums.ContactKind, str] = {
    enums.ContactKind.EMAIL: "Email",
    enums.ContactKind.LINKEDIN_COMPANY: "LinkedIn",
    # SPEC §6 names this one in the UI's own words: it is the "Find people →" button, not a
    # second LinkedIn URL, and calling it "LinkedIn people search" would bury that.
    enums.ContactKind.LINKEDIN_PEOPLE: "Find people",
    enums.ContactKind.CAREERS_PAGE: "Careers page",
    enums.ContactKind.X: "X",
    enums.ContactKind.GITHUB: "GitHub",
    enums.ContactKind.CONTACT_FORM: "Contact form",
}

#: The order the panel lists contacts in, within each confidence group (SPEC §9's contacts
#: block). Usefulness first — an address you can write to, then the page you apply on, then the
#: company's own accounts — with ``linkedin_people`` last because it is the search shortcut the
#: reader falls back to when nothing above it exists. Not derived from the enum's declaration
#: order: that order mirrors the Postgres type, which migration ``0002`` fixed for storage
#: reasons and which has nothing to say about reading.
CONTACT_KIND_ORDER: tuple[enums.ContactKind, ...] = (
    enums.ContactKind.EMAIL,
    enums.ContactKind.CAREERS_PAGE,
    enums.ContactKind.LINKEDIN_COMPANY,
    enums.ContactKind.X,
    enums.ContactKind.GITHUB,
    enums.ContactKind.CONTACT_FORM,
    enums.ContactKind.LINKEDIN_PEOPLE,
)

CONTACT_CONFIDENCE_LABELS: dict[enums.ContactConfidence, str] = {
    # SPEC §6: the UI must make it obvious which of these two a value is.
    enums.ContactConfidence.PUBLISHED: "Published",
    enums.ContactConfidence.CONSTRUCTED: "Constructed",
}

EMPLOYMENT_TYPE_LABELS: dict[enums.EmploymentType, str] = {
    enums.EmploymentType.FULL_TIME: "Full-time",
    enums.EmploymentType.PART_TIME: "Part-time",
    enums.EmploymentType.CONTRACT: "Contract",
    enums.EmploymentType.INTERNSHIP: "Internship",
    enums.EmploymentType.CO_OP: "Co-op",
    enums.EmploymentType.TEMPORARY: "Temporary",
    enums.EmploymentType.UNKNOWN: "Unknown",
}

ROLE_FAMILY_LABELS: dict[enums.RoleFamily, str] = {
    enums.RoleFamily.SOFTWARE: "Software",
    enums.RoleFamily.INFRASTRUCTURE: "Infrastructure",
    enums.RoleFamily.ML_AI: "ML / AI",
    enums.RoleFamily.DATA: "Data",
    enums.RoleFamily.HARDWARE: "Hardware",
    enums.RoleFamily.ROBOTICS: "Robotics",
    enums.RoleFamily.RESEARCH: "Research",
    enums.RoleFamily.PRODUCT: "Product",
    enums.RoleFamily.DESIGN: "Design",
    enums.RoleFamily.SECURITY: "Security",
    enums.RoleFamily.QA: "QA",
    enums.RoleFamily.SALES: "Sales",
    enums.RoleFamily.MARKETING: "Marketing",
    enums.RoleFamily.BIZOPS: "BizOps",
    enums.RoleFamily.FINANCE: "Finance",
    enums.RoleFamily.PEOPLE: "People",
    enums.RoleFamily.OPERATIONS: "Operations",
    enums.RoleFamily.LEGAL: "Legal",
    # Not a leftover bucket to be hidden: SPEC §7.1 requires an unmatched role to be listed.
    enums.RoleFamily.OTHER: "Other",
}

SENIORITY_LABELS: dict[enums.Seniority, str] = {
    enums.Seniority.INTERN: "Intern",
    enums.Seniority.NEW_GRAD: "New grad",
    enums.Seniority.JUNIOR: "Junior",
    enums.Seniority.MID: "Mid",
    enums.Seniority.SENIOR: "Senior",
    enums.Seniority.STAFF: "Staff",
    enums.Seniority.PRINCIPAL: "Principal",
    enums.Seniority.LEAD: "Lead",
    enums.Seniority.MANAGER: "Manager",
    enums.Seniority.DIRECTOR: "Director",
    enums.Seniority.EXECUTIVE: "Executive",
    enums.Seniority.UNKNOWN: "Unknown",
}

FETCH_RUN_STATUS_LABELS: dict[enums.FetchRunStatus, str] = {
    enums.FetchRunStatus.OK: "OK",
    enums.FetchRunStatus.PARTIAL: "Partial",
    enums.FetchRunStatus.ERROR: "Error",
}

TRACKING_STATUS_LABELS: dict[enums.TrackingStatus, str] = {
    enums.TrackingStatus.NONE: "None",
    enums.TrackingStatus.INTERESTED: "Interested",
    enums.TrackingStatus.APPLIED: "Applied",
    enums.TrackingStatus.IN_PROCESS: "In process",
    enums.TrackingStatus.REJECTED: "Rejected",
    enums.TrackingStatus.OFFER: "Offer",
}


def _ordered[E: StrEnum](members: type[E]) -> tuple[E, ...]:
    """Declaration order, with the ``other``/``unknown`` fallbacks moved to the end."""
    ordinary = [member for member in members if member.value not in _FALLBACK_VALUES]
    fallback = [member for member in members if member.value in _FALLBACK_VALUES]
    return (*ordinary, *fallback)


ROLE_FAMILY_ORDER: tuple[enums.RoleFamily, ...] = _ordered(enums.RoleFamily)
EMPLOYMENT_TYPE_ORDER: tuple[enums.EmploymentType, ...] = _ordered(enums.EmploymentType)
SENIORITY_ORDER: tuple[enums.Seniority, ...] = _ordered(enums.Seniority)
STAGE_ORDER: tuple[enums.Stage, ...] = _ordered(enums.Stage)
ROUND_TYPE_ORDER: tuple[enums.RoundType, ...] = _ordered(enums.RoundType)
TRACKING_STATUS_ORDER: tuple[enums.TrackingStatus, ...] = _ordered(enums.TrackingStatus)


#: Every label keyed by the *stored* value, so a raw string out of a query row labels the same
#: way an enum member does. Values shared by two enums (``seed`` and ``series_a`` in both
#: :class:`~db.enums.Stage` and :class:`~db.enums.RoundType`, ``unknown`` in four, ``other`` in
#: two) carry the same label in each, so flattening them loses nothing.
_BY_VALUE: dict[str, str] = {
    member.value: label
    for mapping in (
        STAGE_LABELS,
        COMPANY_STATUS_LABELS,
        ATS_PROVIDER_LABELS,
        ROUND_TYPE_LABELS,
        ROLE_TYPE_LABELS,
        CONTACT_KIND_LABELS,
        CONTACT_CONFIDENCE_LABELS,
        EMPLOYMENT_TYPE_LABELS,
        ROLE_FAMILY_LABELS,
        SENIORITY_LABELS,
        FETCH_RUN_STATUS_LABELS,
        TRACKING_STATUS_LABELS,
    )
    for member, label in mapping.items()
}


def label_for(value: object) -> str:
    """The display label for an enum member or its raw value; the ``label`` Jinja filter.

    ``None`` renders as a dash rather than the word "None", so a template can write
    ``{{ row.latest_round_type | label }}`` for a company that has never raised. An unknown
    value (a member added to an enum before a label was written for it, say) degrades to
    ``"Some value"`` instead of raising — a missing label must never take a page down.
    """
    if value is None:
        return EM_DASH
    # A StrEnum member *is* its value; str() gives "software", not "RoleFamily.SOFTWARE".
    raw = str(value)
    return _BY_VALUE.get(raw, raw.replace("_", " ").capitalize())
