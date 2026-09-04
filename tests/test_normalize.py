"""``ingest/normalize.py`` — SPEC §8 normalization and entity resolution.

The normalizers are pure and table-tested. Resolution runs against the real migrated Postgres
(``session`` fixture, SPEC §3) because step 3 depends on ``pg_trgm``'s ``%`` operator and on
the ``similarity()`` values Postgres actually computes — the numbers asserted below were read
back from ``SELECT similarity(...)`` on pg_trgm 1.6 before being written down.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Company, CompanyLocation, CompanySource, Location, MergeCandidate
from ingest.normalize import (
    NAME_SUFFIXES,
    TRIGRAM_THRESHOLD,
    Candidate,
    Resolution,
    ResolutionKey,
    normalize_domain,
    normalize_name,
    record_merge_candidates,
    resolve_company,
    slugify,
)

BAY_AREA = "Bay Area"
# ``similarity('applied intuition system', 'applied intuition systems')`` on pg_trgm 1.6.
NEAR_DUPLICATE_SIMILARITY = 0.8889


# ---------------------------------------------------------------- normalize_domain (SPEC §8 step 1)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("https://www.Stripe.com/about?x=1", "stripe.com"),
        ("STRIPE.COM/", "stripe.com"),
        ("http://stripe.com:443", "stripe.com"),
        ("www.stripe.com.", "stripe.com"),
        ("  stripe.com  ", "stripe.com"),
        ("https://user:secret@stripe.com/careers#eng", "stripe.com"),
        ("//stripe.com/x", "stripe.com"),
        ("stripe", None),
        ("", None),
        ("   ", None),
        (None, None),
        ("not a domain", None),
        ("mailto:jobs@stripe.com", None),
        ("http://[::1]/", None),
        ("under_score.com", None),
        (".stripe.com", None),
        ("https://münchen.de", "xn--mnchen-3ya.de"),
        ("sub.www.example.co.uk", "sub.www.example.co.uk"),
    ],
)
def test_normalize_domain(value: str | None, expected: str | None) -> None:
    assert normalize_domain(value) == expected


# ------------------------------------------------------------------ normalize_name (SPEC §8 step 2)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Stripe, Inc.", "stripe"),
        ("Modal Labs", "modal"),
        ("Acme Technologies, Inc.", "acme"),
        ("Labs Inc", "labs"),  # strip "inc", then stop: stripping "labs" would empty the name
        ("Co", "co"),
        ("KEA Cloud, Inc.", "kea cloud"),
        ("Anysphere (Cursor)", "anysphere cursor"),
        ("Twelve Co", "twelve"),
        ("Fireworks AI", "fireworks ai"),
        ("Café Ünicode Corp.", "cafe unicode"),
        ("Accel Growth Fund 8 L.P.", "accel growth fund 8"),
        ("AT&T", "at and t"),
        ("Stripe Incorporated", "stripe"),
        ("U.S. Robotics L.L.C.", "us robotics"),
        ("x.ai", "x ai"),  # not a dotted initialism: "ai" is two letters
        ("Web 2.0 Inc", "web 2 0"),  # digits never form an initialism: the dot is punctuation
        ("3.5 Labs", "3 5"),
        ("  Holdings   Corp  ", "holdings"),
        ("stripe", "stripe"),
    ],
)
def test_normalize_name(value: str, expected: str) -> None:
    assert normalize_name(value) == expected


def test_name_suffixes_contain_the_spec_list_and_their_legal_equivalents() -> None:
    spec = {"inc", "llc", "corp", "co", "ltd", "technologies", "labs", "holdings"}
    legal = {"incorporated", "corporation", "limited", "company", "lp", "llp", "plc", "pbc"}
    assert spec | legal == NAME_SUFFIXES


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Other Technology", "other-technology"),
        ("Banking & Financial Services", "banking-financial-services"),
        ("  Pooled Investment Fund  ", "pooled-investment-fund"),
        ("Café / Bar", "cafe-bar"),
    ],
)
def test_slugify(value: str, expected: str) -> None:
    assert slugify(value) == expected


# ------------------------------------------------------------------------ database helpers


async def _location(session: AsyncSession, city: str, state: str, metro: str) -> Location:
    location = Location(city=city, state=state, metro=metro)
    session.add(location)
    await session.flush()
    return location


async def _company(
    session: AsyncSession,
    name: str,
    *,
    domain: str | None = None,
    location: Location | None = None,
    source: tuple[str, str] | None = None,
) -> Company:
    """Insert a company directly with the ORM, optionally linked to ``location`` and with a
    ``CompanySource(connector, external_id)`` row."""
    company = Company(name=name, normalized_name=normalize_name(name), domain=domain)
    session.add(company)
    await session.flush()
    if location is not None:
        session.add(CompanyLocation(company_id=company.id, location_id=location.id, is_hq=True))
    if source is not None:
        connector, external_id = source
        session.add(
            CompanySource(company_id=company.id, connector=connector, external_id=external_id)
        )
    await session.flush()
    return company


def _key(**overrides: Any) -> ResolutionKey:
    fields: dict[str, Any] = {
        "connector": "test",
        "external_id": None,
        "domain": None,
        "normalized_name": "",
        "metro": None,
        "search_metros": (),
    }
    return ResolutionKey(**{**fields, **overrides})


async def _company_count(session: AsyncSession) -> int:
    return await session.scalar(select(func.count()).select_from(Company)) or 0


async def _merge_candidates(session: AsyncSession) -> list[MergeCandidate]:
    """Current ``merge_candidates`` rows, re-read from the database: the upsert is a Core
    statement that bypasses the identity map, so already-loaded rows must be repopulated."""
    stmt = select(MergeCandidate).execution_options(populate_existing=True)
    return list((await session.execute(stmt)).scalars())


# ------------------------------------------------------------- resolve_company (SPEC §8 steps 0-2)


async def test_external_id_match_beats_a_conflicting_domain(session: AsyncSession) -> None:
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    by_cik = await _company(
        session, "Obsidian Security, Inc.", location=sf, source=("sec_edgar", "0001234567")
    )
    by_domain = await _company(session, "Other Company", domain="other.example", location=sf)

    resolution = await resolve_company(
        session,
        _key(
            connector="sec_edgar",
            external_id="0001234567",
            domain="other.example",
            normalized_name="other",
            metro=BAY_AREA,
        ),
    )

    assert resolution == Resolution(by_cik.id, "external_id")
    assert resolution.company_id != by_domain.id


async def test_external_id_is_scoped_to_the_connector(session: AsyncSession) -> None:
    """The same external id under another connector is a different namespace: fall through."""
    await _company(session, "Obsidian Security, Inc.", source=("sec_edgar", "0001234567"))
    by_domain = await _company(session, "Other Company", domain="other.example")

    resolution = await resolve_company(
        session,
        _key(connector="ycombinator", external_id="0001234567", domain="other.example"),
    )

    assert resolution == Resolution(by_domain.id, "domain")


async def test_domain_match(session: AsyncSession) -> None:
    await _company(session, "Not Stripe", domain="notstripe.example")
    stripe = await _company(session, "Stripe, Inc.", domain="stripe.com")

    resolution = await resolve_company(session, _key(domain="stripe.com", normalized_name="x"))

    assert resolution == Resolution(stripe.id, "domain")


async def test_domain_mismatch_without_a_metro_finds_nothing(session: AsyncSession) -> None:
    """No metro ⇒ steps 2 and 3 have nothing to scope to and are skipped (design decision 2),
    even though a company with the very same normalized name exists."""
    await _company(session, "Stripe, Inc.", domain="stripe.com")

    resolution = await resolve_company(
        session, _key(domain="stripe.example", normalized_name="stripe", metro=None)
    )

    assert resolution == Resolution(None, None, ())


async def test_name_match_within_the_same_metro(session: AsyncSession) -> None:
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    stripe = await _company(session, "Stripe, Inc.", location=sf)

    resolution = await resolve_company(session, _key(normalized_name="stripe", metro=BAY_AREA))

    assert resolution == Resolution(stripe.id, "name")


async def test_same_name_in_a_different_metro_is_not_a_match(session: AsyncSession) -> None:
    austin = await _location(session, "Austin", "TX", "Austin")
    await _company(session, "Stripe, Inc.", location=austin)

    resolution = await resolve_company(session, _key(normalized_name="stripe", metro=BAY_AREA))

    assert resolution == Resolution(None, None, ())


async def test_name_match_is_skipped_for_a_record_without_a_metro(session: AsyncSession) -> None:
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    await _company(session, "Stripe, Inc.", location=sf)

    resolution = await resolve_company(session, _key(normalized_name="stripe", metro=None))

    assert resolution == Resolution(None, None, ())


# ------------------------------------------------- search_metros (design decision 4, Phase 3)


async def test_search_metros_matches_a_record_that_has_no_location(session: AsyncSession) -> None:
    """An ``enrich_only`` record — an RSS funding headline — carries a company name and no
    address, so SPEC §8 step 2 has no metro to scope to. ``search_metros`` scopes it to the
    metros ``config/regions.yaml`` configures instead."""
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    company = await _company(session, "Crusoe", location=sf)

    resolution = await resolve_company(
        session, _key(normalized_name="crusoe", metro=None, search_metros=(BAY_AREA,))
    )

    assert resolution == Resolution(company.id, "name", ())


async def test_search_metros_refuses_an_ambiguous_match(session: AsyncSession) -> None:
    """Two companies of that name in two metros: a headline must not guess which it means."""
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    austin = await _location(session, "Austin", "TX", "Austin")
    await _company(session, "Crusoe", location=sf)
    await _company(session, "Crusoe", location=austin)

    resolution = await resolve_company(
        session, _key(normalized_name="crusoe", metro=None, search_metros=(BAY_AREA, "Austin"))
    )

    assert resolution == Resolution(None, None, ())


async def test_search_metros_never_reaches_the_trigram_step(session: AsyncSession) -> None:
    """A fuzzy match with no metro of its own is exactly what SPEC §8 forbids."""
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    await _company(session, "Applied Intuition", location=sf)

    resolution = await resolve_company(
        session,
        _key(normalized_name="applied intuitions", metro=None, search_metros=(BAY_AREA,)),
    )

    assert resolution == Resolution(None, None, ())


async def test_search_metros_is_ignored_when_the_record_has_its_own_metro(
    session: AsyncSession,
) -> None:
    austin = await _location(session, "Austin", "TX", "Austin")
    await _company(session, "Crusoe", location=austin)

    resolution = await resolve_company(
        session, _key(normalized_name="crusoe", metro=BAY_AREA, search_metros=("Austin",))
    )

    assert resolution == Resolution(None, None, ())


# --------------------------------------------------------------- trigram step (SPEC §8 step 3)


async def test_trigram_near_duplicate_is_a_candidate_not_a_match(session: AsyncSession) -> None:
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    near = await _company(session, "Applied Intuition System", location=sf)
    await _company(session, "Anthropic", location=sf)

    resolution = await resolve_company(
        session, _key(normalized_name="applied intuition systems", metro=BAY_AREA)
    )

    assert resolution.company_id is None
    assert resolution.matched_by is None
    (candidate,) = resolution.candidates
    assert candidate.company_id == near.id
    assert candidate.similarity == pytest.approx(NEAR_DUPLICATE_SIMILARITY, abs=1e-4)
    assert candidate.similarity > TRIGRAM_THRESHOLD


async def test_trigram_candidates_are_scoped_to_the_metro(session: AsyncSession) -> None:
    austin = await _location(session, "Austin", "TX", "Austin")
    await _company(session, "Applied Intuition System", location=austin)

    resolution = await resolve_company(
        session, _key(normalized_name="applied intuition systems", metro=BAY_AREA)
    )

    assert resolution == Resolution(None, None, ())


async def test_similarity_below_the_threshold_is_not_a_candidate(session: AsyncSession) -> None:
    """``similarity('stripe payment', 'stripe payments')`` is 0.8235 on pg_trgm 1.6 — a
    one-letter difference in a short name stays *below* the SPEC's strict ``> 0.85``."""
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    await _company(session, "Stripe Payment", location=sf)

    resolution = await resolve_company(
        session, _key(normalized_name="stripe payments", metro=BAY_AREA)
    )

    assert resolution == Resolution(None, None, ())


async def test_similarity_exactly_at_the_threshold_is_not_a_candidate(
    session: AsyncSession,
) -> None:
    """SPEC §8 says ``> 0.85``, strictly. ``similarity('applied intuition', 'applied
    intuitions')`` is exactly 0.85 on pg_trgm 1.6 (as a ``real``: 0.8500000238…), and pg_trgm's
    ``%`` prefilter admits it — only the explicit comparison, done in ``float4``, keeps it out.
    Compared in ``float8`` the promoted real reads as ``> 0.85`` and the real Bay Area company
    would be flagged against its own plural."""
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    await _company(session, "Applied Intuition", location=sf)
    # Guard the premise: if a pg_trgm upgrade moves this number the test must say so rather
    # than pass for the wrong reason.
    at_threshold = await session.scalar(
        select(func.similarity("applied intuition", "applied intuitions"))
    )
    assert at_threshold == pytest.approx(TRIGRAM_THRESHOLD, abs=1e-6)

    resolution = await resolve_company(
        session, _key(normalized_name="applied intuitions", metro=BAY_AREA)
    )

    assert resolution == Resolution(None, None, ())


async def test_dissimilar_name_yields_no_candidate(session: AsyncSession) -> None:
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    await _company(session, "Applied Intuition System", location=sf)

    resolution = await resolve_company(session, _key(normalized_name="anthropic", metro=BAY_AREA))

    assert resolution == Resolution(None, None, ())


async def test_candidates_are_ordered_by_similarity_desc(session: AsyncSession) -> None:
    """Two near-duplicates, one letter off each: 0.8889 (``system``) and 0.8929
    (``intuitions``) on pg_trgm 1.6 — both clear the threshold, highest first."""
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    farther = await _company(session, "Applied Intuition System", location=sf)
    nearer = await _company(session, "Applied Intuitions Systems", location=sf)

    resolution = await resolve_company(
        session, _key(normalized_name="applied intuition systems", metro=BAY_AREA)
    )

    assert resolution.company_id is None
    assert [c.company_id for c in resolution.candidates] == [nearer.id, farther.id]
    sims = [c.similarity for c in resolution.candidates]
    assert sims == sorted(sims, reverse=True)
    assert all(s > TRIGRAM_THRESHOLD for s in sims)


# ------------------------------------------------------- record_merge_candidates (SPEC §8 step 3)


async def test_record_merge_candidates_orders_ids_and_upserts_similarity(
    session: AsyncSession,
) -> None:
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    older = await _company(session, "Applied Intuition System", location=sf)
    newer = await _company(session, "Applied Intuition Systems", location=sf)
    assert older.id < newer.id

    written = await record_merge_candidates(
        session, newer.id, [Candidate(older.id, NEAR_DUPLICATE_SIMILARITY)], metro=BAY_AREA
    )

    assert written == 1
    (row,) = await _merge_candidates(session)
    assert (row.company_id_a, row.company_id_b) == (older.id, newer.id)
    assert row.similarity == pytest.approx(NEAR_DUPLICATE_SIMILARITY)
    assert row.reason == "trigram similarity 0.89 on normalized_name within metro 'Bay Area'"
    assert row.resolved_at is None

    # Re-run from the other side with a new similarity: the same row is updated, not doubled.
    written = await record_merge_candidates(
        session, older.id, [Candidate(newer.id, 0.91)], metro=BAY_AREA
    )

    assert written == 1
    (row,) = await _merge_candidates(session)
    assert (row.company_id_a, row.company_id_b) == (older.id, newer.id)
    assert row.similarity == pytest.approx(0.91)
    # ``reason`` embeds the number, so it is refreshed with it — the row never contradicts itself.
    assert row.reason == "trigram similarity 0.91 on normalized_name within metro 'Bay Area'"
    assert await _company_count(session) == 2


async def test_record_merge_candidates_leaves_a_resolved_pair_untouched(
    session: AsyncSession,
) -> None:
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    older = await _company(session, "Applied Intuition System", location=sf)
    newer = await _company(session, "Applied Intuition Systems", location=sf)
    await record_merge_candidates(session, newer.id, [Candidate(older.id, 0.86)], metro=BAY_AREA)
    (row,) = await _merge_candidates(session)
    row.resolved_at = datetime(2026, 9, 1, tzinfo=UTC)
    await session.flush()

    written = await record_merge_candidates(
        session, newer.id, [Candidate(older.id, 0.99)], metro=BAY_AREA
    )

    assert written == 0
    (row,) = await _merge_candidates(session)
    assert row.similarity == pytest.approx(0.86)
    assert row.reason == "trigram similarity 0.86 on normalized_name within metro 'Bay Area'"
    assert row.resolved_at == datetime(2026, 9, 1, tzinfo=UTC)
    assert await _company_count(session) == 2


async def test_record_merge_candidates_with_nothing_to_record(session: AsyncSession) -> None:
    company = await _company(session, "Solo")

    assert await record_merge_candidates(session, company.id, [], metro=BAY_AREA) == 0
    # A self-pair is meaningless and skipped rather than written.
    assert (
        await record_merge_candidates(
            session, company.id, [Candidate(company.id, 1.0)], metro=BAY_AREA
        )
        == 0
    )
    assert await _merge_candidates(session) == []


async def test_record_merge_candidates_collapses_duplicate_pairs(session: AsyncSession) -> None:
    a = await _company(session, "A")
    b = await _company(session, "B")

    written = await record_merge_candidates(
        session, a.id, [Candidate(b.id, 0.86), Candidate(b.id, 0.9)], metro=None
    )

    assert written == 1
    (row,) = await _merge_candidates(session)
    assert row.similarity == pytest.approx(0.9)
    assert row.reason == "trigram similarity 0.90 on normalized_name within metro None"


async def test_resolution_then_recording_never_merges_companies(session: AsyncSession) -> None:
    """End to end: a near-duplicate arrives, is *not* matched, gets inserted by the caller,
    and the pair is written for review — both companies survive (SPEC §8 "Do not auto-merge")."""
    sf = await _location(session, "San Francisco", "CA", BAY_AREA)
    existing = await _company(session, "Applied Intuition System", location=sf)
    key = _key(normalized_name="applied intuition systems", metro=BAY_AREA)

    resolution = await resolve_company(session, key)
    assert resolution.company_id is None
    created = await _company(session, "Applied Intuition Systems", location=sf)
    written = await record_merge_candidates(
        session, created.id, resolution.candidates, metro=key.metro
    )

    assert written == 1
    (row,) = await _merge_candidates(session)
    assert (row.company_id_a, row.company_id_b) == (existing.id, created.id)
    assert row.similarity == pytest.approx(NEAR_DUPLICATE_SIMILARITY, abs=1e-4)
    assert await _company_count(session) == 2
    # And the next arrival of the same record resolves to the created company by name.
    assert await resolve_company(session, key) == Resolution(created.id, "name")
