"""Domain/name normalization and entity resolution (SPEC §8, §11 ``ingest/normalize.py``).

Companies arrive from several connectors under different spellings ("Stripe, Inc.", "Stripe",
"stripe") and with or without a website. This module supplies

* the two normalizers the pipeline keys on — :func:`normalize_domain` (SPEC §8 step 1: the
  canonical key) and :func:`normalize_name` (SPEC §8 step 2) — plus :func:`slugify` for
  ``Sector.slug``;
* :func:`resolve_company`, the ordered resolution steps of SPEC §8 with a "step 0" in front
  (same connector + same ``CompanySource.external_id`` ⇒ same company);
* :func:`record_merge_candidates`, which writes the low-confidence trigram matches of step 3
  to ``merge_candidates`` for ``cli.py merge-review``. **Nothing here ever merges** (SPEC §8
  "Do not auto-merge"; CLAUDE.md).

Everything is pure or database-only: no network, no clock.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Literal, cast
from urllib.parse import urlsplit

from sqlalchemy import CursorResult, Select, func, select, text
from sqlalchemy import cast as sql_cast
from sqlalchemy.dialects.postgresql import REAL, insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Company, CompanyLocation, CompanySource, Location, MergeCandidate
from logging_config import get_logger

log = get_logger(__name__)

#: SPEC §8 step 3: ``similarity(normalized_name, ?) > 0.85`` within the same metro — strictly
#: greater. :func:`resolve_company` compares in ``float4`` (see there), so a pair scoring
#: exactly 0.85 is *not* a candidate.
TRIGRAM_THRESHOLD = 0.85

# SPEC §8 step 2 names these suffixes verbatim: ``inc|llc|corp|co|ltd|technologies|labs|holdings``.
_SPEC_SUFFIXES = frozenset({"inc", "llc", "corp", "co", "ltd", "technologies", "labs", "holdings"})
# Their spelled-out and other legal-form equivalents, so "Stripe, Inc." and "Stripe
# Incorporated" — or "Accel Growth Fund 8 L.P." and "Accel Growth Fund 8" — normalize alike.
_LEGAL_SUFFIXES = frozenset(
    {"incorporated", "corporation", "limited", "company", "lp", "llp", "plc", "pbc"}
)
#: Tokens stripped from the *end* of a name, repeatedly, by :func:`normalize_name`.
NAME_SUFFIXES = _SPEC_SUFFIXES | _LEGAL_SUFFIXES

_URL_SCHEME = re.compile(r"^[a-z][a-z0-9+.\-]*://", re.IGNORECASE)
_DOMAIN_CHARS = re.compile(r"[a-z0-9.-]+")
# A dotted initialism such as ``l.p.``, ``l.l.c.`` or ``u.s`` — single *letters* joined by
# dots, not followed by another alphanumeric (so ``x.ai`` is left alone). Digits are
# deliberately excluded: ``2.0`` in ``Web 2.0`` is a number, and its dot is punctuation that
# becomes a space like any other (``web 2 0``), not an initialism to be joined (``web 20``).
_DOTTED_INITIALISM = re.compile(r"\b[a-z](?:\.[a-z])+\.?(?![a-z0-9])")
_NON_SLUG = re.compile(r"[^a-z0-9]+")


# ------------------------------------------------------------------------ pure normalizers


def normalize_domain(value: str | None) -> str | None:
    """The canonical company key from a bare host or a full URL (SPEC §8 step 1).

    Strips scheme, credentials, port, path, query, fragment and a trailing dot; lowercases;
    drops one leading ``www.`` (``sub.www.example.co.uk`` keeps its ``sub.www.``); IDNA-encodes
    non-ASCII hosts so the stored key is always ASCII. Returns ``None`` for anything that is
    not a domain — empty input, a single label with no dot, characters outside
    ``[a-z0-9.-]``, or a ``mailto:`` value (an e-mail address is not a domain; SPEC §6 keeps
    published addresses in ``Contact``, not on the company key).
    """
    if value is None:
        return None
    raw = value.strip()
    if not raw or raw.lower().startswith("mailto:"):
        return None
    # ``urlsplit`` only recognises the authority after ``//``; a bare host (``stripe.com:443``)
    # would otherwise be read as scheme ``stripe.com``.
    target = raw if _URL_SCHEME.match(raw) or raw.startswith("//") else f"//{raw}"
    try:
        host = urlsplit(target).hostname  # lowercased, credentials and port removed
    except ValueError:
        return None
    if not host:
        return None
    host = host.rstrip(".")
    if host.startswith("www."):
        host = host[len("www.") :]
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError:  # empty label, over-long label, unencodable character
        return None
    if "." not in host or not _DOMAIN_CHARS.fullmatch(host):
        return None
    return host


def normalize_name(value: str) -> str:
    """The comparison form of a company name (SPEC §8 step 2).

    NFKD-decompose and drop combining marks (``Café`` → ``cafe``), casefold, spell out ``&``
    as ``and``, join dotted initialisms (``L.P.`` → ``lp``), turn every other punctuation
    character into a space, collapse whitespace, then strip legal-suffix tokens
    (:data:`NAME_SUFFIXES`) from the end only, repeatedly — ``Acme Technologies, Inc.`` →
    ``acme`` — but never so far that the name becomes empty (``Labs Inc`` → ``labs``,
    ``Co`` → ``co``).
    """
    decomposed = unicodedata.normalize("NFKD", value)
    text_ = "".join(ch for ch in decomposed if not unicodedata.combining(ch)).casefold()
    text_ = text_.replace("&", " and ")
    text_ = _DOTTED_INITIALISM.sub(lambda m: m.group().replace(".", ""), text_)
    text_ = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in text_)
    tokens = text_.split()
    while len(tokens) > 1 and tokens[-1] in NAME_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def slugify(value: str) -> str:
    """URL-safe identifier for ``Sector.slug`` (SPEC §5): ASCII-folded and casefolded, runs
    of non-alphanumerics become one ``-``, leading/trailing ``-`` trimmed.
    ``"Banking & Financial Services"`` → ``"banking-financial-services"``.
    """
    ascii_text = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii").casefold()
    )
    return _NON_SLUG.sub("-", ascii_text).strip("-")


# ----------------------------------------------------------------------- entity resolution

MatchedBy = Literal["external_id", "domain", "name"]


@dataclass(frozen=True, slots=True, kw_only=True)
class ResolutionKey:
    """What the pipeline knows about an incoming record, already normalized.

    ``domain`` is :func:`normalize_domain`'s output and ``normalized_name`` is
    :func:`normalize_name`'s; ``metro`` is the record's resolution metro (its HQ location's,
    else its first location's) and ``None`` when the record has no configured location.
    """

    connector: str
    external_id: str | None
    domain: str | None
    normalized_name: str
    metro: str | None


@dataclass(frozen=True, slots=True)
class Candidate:
    """A probable duplicate found by trigram similarity (SPEC §8 step 3) — never merged."""

    company_id: int
    similarity: float


@dataclass(frozen=True, slots=True)
class Resolution:
    """Outcome of :func:`resolve_company`. ``candidates`` is non-empty only when
    ``company_id`` is ``None``: the record must be inserted as a new company and the
    candidates recorded for review."""

    company_id: int | None
    matched_by: MatchedBy | None
    candidates: tuple[Candidate, ...] = ()


def _companies_in_metro(metro: str) -> Select[tuple[int]]:
    """Ids of companies with at least one location in ``metro`` (SPEC §8 "scoped to the same
    metro"). A subquery, so a company with several offices in the metro matches once."""
    return (
        select(CompanyLocation.company_id)
        .join(Location, Location.id == CompanyLocation.location_id)
        .where(Location.metro == metro)
    )


async def resolve_company(session: AsyncSession, key: ResolutionKey) -> Resolution:
    """Find the existing company an incoming record belongs to (SPEC §8), first hit wins.

    0. Same connector and same ``CompanySource.external_id`` (an EDGAR CIK, a YC slug) — a
       connector re-seeing its own record must never create a duplicate, even when the
       record carries no domain.
    1. Exact match on the normalized domain — the canonical key (SPEC §8 step 1).
    2. Exact match on ``normalized_name`` scoped to the same metro (SPEC §8 step 2).
    3. ``pg_trgm`` similarity above :data:`TRIGRAM_THRESHOLD` scoped to the same metro
       (SPEC §8 step 3) — returned as ``candidates``, **never** as a match: the caller
       inserts the company and calls :func:`record_merge_candidates`.

    Steps 2 and 3 need a metro to scope to and are skipped when ``key.metro`` is ``None``
    (or the name normalized to nothing), so a domain-less, location-less record can only
    ever be matched through step 0.
    """
    if key.external_id is not None:
        company_id = await session.scalar(
            select(CompanySource.company_id).where(
                CompanySource.connector == key.connector,
                CompanySource.external_id == key.external_id,
            )
        )
        if company_id is not None:
            log.debug("resolved", step="external_id", company_id=company_id)
            return Resolution(company_id, "external_id")

    if key.domain is not None:
        company_id = await session.scalar(select(Company.id).where(Company.domain == key.domain))
        if company_id is not None:
            log.debug("resolved", step="domain", company_id=company_id)
            return Resolution(company_id, "domain")

    if key.metro is None or not key.normalized_name:
        return Resolution(None, None)

    in_metro = _companies_in_metro(key.metro)
    company_id = await session.scalar(
        select(Company.id)
        .where(Company.normalized_name == key.normalized_name, Company.id.in_(in_metro))
        .order_by(Company.id)
        .limit(1)
    )
    if company_id is not None:
        log.debug("resolved", step="name", company_id=company_id, metro=key.metro)
        return Resolution(company_id, "name")

    # Step 3. ``%`` is pg_trgm's indexed similarity operator (it uses
    # ``ix_companies_normalized_name_trgm``, SPEC §5) and reads its threshold from the GUC, which
    # SET LOCAL pins for this transaction. ``SET`` cannot take a bind parameter, hence the
    # f-string on a module constant. ``%`` is *inclusive* (``>=`` internally), so the explicit
    # ``similarity(...) >`` is what enforces the SPEC's strict ``> 0.85`` — and it must compare
    # in ``float4``: ``similarity()`` returns ``real``, while a plain Python float binds as
    # ``float8``, which would promote real 0.85 (0.8500000238…) and admit a pair scoring exactly
    # 0.85 (``applied intuition`` / ``applied intuitions``) as a candidate. Exact equality is
    # step 2's job and is excluded here.
    await session.execute(text(f"SET LOCAL pg_trgm.similarity_threshold = {TRIGRAM_THRESHOLD}"))
    similarity = func.similarity(Company.normalized_name, key.normalized_name)
    rows = await session.execute(
        select(Company.id, similarity.label("similarity"))
        .where(
            Company.id.in_(in_metro),
            Company.normalized_name.op("%")(key.normalized_name),
            similarity > sql_cast(TRIGRAM_THRESHOLD, REAL),
            Company.normalized_name != key.normalized_name,
        )
        .order_by(similarity.desc(), Company.id)
    )
    candidates = tuple(Candidate(row.id, float(row.similarity)) for row in rows)
    if candidates:
        log.info(
            "trigram candidates",
            normalized_name=key.normalized_name,
            metro=key.metro,
            candidates=[(c.company_id, round(c.similarity, 3)) for c in candidates],
        )
    return Resolution(None, None, candidates)


async def record_merge_candidates(
    session: AsyncSession,
    company_id: int,
    candidates: Iterable[Candidate],
    *,
    metro: str | None,
) -> int:
    """Write ``MergeCandidate`` rows for ``company_id`` and each candidate (SPEC §8 step 3).

    Pairs are stored with ``company_id_a < company_id_b`` so the unique constraint sees the
    same pair regardless of which side was ingested first. Re-running the same pair updates
    ``similarity`` in place (``ON CONFLICT ... DO UPDATE``) — together with ``reason``, whose
    text embeds the number, so the row ``merge-review`` shows never contradicts itself —
    unless a reviewer has already resolved it (``resolved_at IS NOT NULL``), in which case the
    row is left untouched.
    Returns the number of rows inserted or updated. Companies are never deleted or merged
    here — that is ``cli.py merge-review``'s decision.
    """
    pairs: dict[tuple[int, int], float] = {}
    for candidate in candidates:
        if candidate.company_id == company_id:
            continue
        pair = (min(company_id, candidate.company_id), max(company_id, candidate.company_id))
        # One VALUES row per pair: Postgres refuses to update the same row twice in one INSERT.
        pairs[pair] = max(candidate.similarity, pairs.get(pair, 0.0))
    if not pairs:
        return 0

    rows = [
        {
            "company_id_a": a,
            "company_id_b": b,
            "similarity": sim,
            "reason": f"trigram similarity {sim:.2f} on normalized_name within metro {metro!r}",
        }
        for (a, b), sim in pairs.items()
    ]
    stmt = insert(MergeCandidate).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[MergeCandidate.company_id_a, MergeCandidate.company_id_b],
        set_={"similarity": stmt.excluded.similarity, "reason": stmt.excluded.reason},
        where=MergeCandidate.resolved_at.is_(None),
    )
    # ``AsyncSession.execute`` is typed as the generic ``Result``; an INSERT always yields a
    # ``CursorResult``, whose ``rowcount`` is the inserted + updated row count.
    result = cast("CursorResult[Any]", await session.execute(stmt))
    written: int = result.rowcount
    log.info("merge candidates recorded", company_id=company_id, written=written, metro=metro)
    return written
