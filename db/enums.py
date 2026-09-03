"""Controlled vocabularies (SPEC §5, §7.1).

Every class is mirrored by a native Postgres ``ENUM`` type; :data:`PG_ENUMS` maps the Postgres
type name to the Python enum and is the single source of truth for ``db.models`` and the
migration tests. Members are stored by *value* (``"part_time"``), never by Python name.

Adding a member later needs an ``ALTER TYPE ... ADD VALUE`` migration.
"""

from __future__ import annotations

from enum import StrEnum


class Stage(StrEnum):
    """Company maturity (``Company.stage``).

    SPEC §5 leaves the members undefined. They mirror the funding ladder so that "stage"
    and "latest round type" (the §9 filter) line up; ``UNKNOWN`` is the default.
    """

    PRE_SEED = "pre_seed"
    SEED = "seed"
    SERIES_A = "series_a"
    SERIES_B = "series_b"
    SERIES_C = "series_c"
    SERIES_D_PLUS = "series_d_plus"
    GROWTH = "growth"
    PUBLIC = "public"
    UNKNOWN = "unknown"


class CompanyStatus(StrEnum):
    ACTIVE = "active"
    ACQUIRED = "acquired"
    DEAD = "dead"


class AtsProvider(StrEnum):
    """Applicant tracking systems with public job-board APIs (SPEC §4, Tier 1 #3)."""

    GREENHOUSE = "greenhouse"
    LEVER = "lever"
    ASHBY = "ashby"
    WORKABLE = "workable"


class RoundType(StrEnum):
    """Funding round label (``FundingRound.round_type``).

    SPEC §5 leaves the members undefined. Form D filings carry no round label, so
    ``UNKNOWN`` is the default; ``OTHER`` is for labelled rounds that fit no bucket.
    """

    PRE_SEED = "pre_seed"
    SEED = "seed"
    SERIES_A = "series_a"
    SERIES_B = "series_b"
    SERIES_C = "series_c"
    SERIES_D_PLUS = "series_d_plus"
    CONVERTIBLE_NOTE = "convertible_note"
    SAFE = "safe"
    DEBT = "debt"
    GRANT = "grant"
    OTHER = "other"
    UNKNOWN = "unknown"


class RoleType(StrEnum):
    FOUNDER = "founder"
    EXEC = "exec"
    RECRUITER = "recruiter"
    ENG_LEAD = "eng_lead"


class ContactKind(StrEnum):
    EMAIL = "email"
    LINKEDIN_COMPANY = "linkedin_company"
    CAREERS_PAGE = "careers_page"
    X = "x"
    GITHUB = "github"
    CONTACT_FORM = "contact_form"


class ContactConfidence(StrEnum):
    """SPEC §6: ``PUBLISHED`` = the company itself published it; ``CONSTRUCTED`` = a
    deterministic URL we built (a search shortcut, never a verified address)."""

    PUBLISHED = "published"
    CONSTRUCTED = "constructed"


class EmploymentType(StrEnum):
    FULL_TIME = "full_time"
    PART_TIME = "part_time"
    CONTRACT = "contract"
    INTERNSHIP = "internship"
    CO_OP = "co_op"
    TEMPORARY = "temporary"
    UNKNOWN = "unknown"


class RoleFamily(StrEnum):
    """All 19 role families (SPEC §7.1). ``OTHER`` is the fallback — a role that matches no
    rule is still stored and shown; classification never excludes."""

    SOFTWARE = "software"
    INFRASTRUCTURE = "infrastructure"
    ML_AI = "ml_ai"
    DATA = "data"
    HARDWARE = "hardware"
    ROBOTICS = "robotics"
    RESEARCH = "research"
    PRODUCT = "product"
    DESIGN = "design"
    SECURITY = "security"
    QA = "qa"
    SALES = "sales"
    MARKETING = "marketing"
    BIZOPS = "bizops"
    FINANCE = "finance"
    PEOPLE = "people"
    OPERATIONS = "operations"
    LEGAL = "legal"
    OTHER = "other"


class Seniority(StrEnum):
    INTERN = "intern"
    NEW_GRAD = "new_grad"
    JUNIOR = "junior"
    MID = "mid"
    SENIOR = "senior"
    STAFF = "staff"
    PRINCIPAL = "principal"
    LEAD = "lead"
    MANAGER = "manager"
    DIRECTOR = "director"
    EXECUTIVE = "executive"
    UNKNOWN = "unknown"


class FetchRunStatus(StrEnum):
    OK = "ok"
    PARTIAL = "partial"
    ERROR = "error"


class TrackingStatus(StrEnum):
    """Personal pipeline status (``UserNote.status``)."""

    NONE = "none"
    INTERESTED = "interested"
    APPLIED = "applied"
    IN_PROCESS = "in_process"
    REJECTED = "rejected"
    OFFER = "offer"


#: Postgres ENUM type name -> Python enum. Keep in sync with migrations/versions/0001_*.py.
PG_ENUMS: dict[str, type[StrEnum]] = {
    "stage": Stage,
    "company_status": CompanyStatus,
    "ats_provider": AtsProvider,
    "round_type": RoundType,
    "role_type": RoleType,
    "contact_kind": ContactKind,
    "contact_confidence": ContactConfidence,
    "employment_type": EmploymentType,
    "role_family": RoleFamily,
    "seniority": Seniority,
    "fetch_run_status": FetchRunStatus,
    "tracking_status": TrackingStatus,
}
