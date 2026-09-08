"""``ingest.connectors.sec_edgar`` on recorded fixtures (SPEC §4 Tier 1 #1, §12 Phase 2).

Nothing here touches the network: ``respx`` answers every request from
``tests/fixtures/sec_edgar`` (an un-mocked request fails the test) and a :class:`FakeClock`
stands in for the clocks and ``asyncio.sleep`` so the rate limiter never waits.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db import enums
from db.models import (
    Base,
    Company,
    CompanyLocation,
    CompanySource,
    FundingRound,
    Location,
    Person,
    Sector,
)
from ingest.base import FetchContext, LocationRecord
from ingest.config import (
    ConnectorConfig,
    RegionsConfig,
    load_connectors_config,
    load_regions_config,
)
from ingest.connectors import get_connector_class
from ingest.connectors.sec_edgar import (
    ARCHIVE_URL,
    MAX_HITS_PER_QUERY,
    SEARCH_URL,
    FormDFiling,
    FormDParseError,
    SecEdgarConnector,
    date_windows,
    infer_round_type,
    months_before,
    parse_form_d,
    split_location,
)
from ingest.http import HttpClient, MemoryCache
from logging_config import get_logger

FIXTURES = Path(__file__).parent / "fixtures" / "sec_edgar"
UA = "startup-tracker/0.1 tests@example.com"
NOW = datetime(2026, 9, 3, 12, tzinfo=UTC)
# 2026-08-05..2026-09-03 is 30 days inclusive: exactly one search window at the configured
# ``search_window_days: 30``.
SINCE = datetime(2026, 8, 5, tzinfo=UTC)

# The six hits in search_palo_alto_2026-08.json: (accession, cik, file number, fixture).
OBSIDIAN = ("0002149647-26-000001", "0002149647", "021-594643", "obsidian_security_D.xml")
KEA_CLOUD = ("0002073579-26-000001", "0002073579", "021-549965", "kea_cloud_DA.xml")
FESTIMO = ("0002151025-26-000003", "0002151025", "021-594734", "festimo_D.xml")
BAY_AREA_HITS = (OBSIDIAN, KEA_CLOUD, FESTIMO)
# ... and the three the phrase search surfaced from outside the region.
CENTAUR = ("0001688486-26-000002", "0001688486")  # Middleton, WI
QUDARA = ("0002147694-26-000002", "0002147694")  # Santa Barbara, CA
LOVABLE = ("0002055162-26-000001", "0002055162")  # Dover, DE (issuer in Boston)
OUTSIDE_HITS = (CENTAUR, QUDARA, LOVABLE)

# search_san_francisco_2026-08.json lists two file numbers twice — an original Form D and its
# D/A — in EFTS score order: (file number, original accession, amendment accession). The first
# pair comes back amendment-first (fixtures README).
SF_AMENDED_ROUNDS = (
    ("021-593951", "0002139601-26-000001", "0002139601-26-000002"),
    ("021-595718", "0002152053-26-000002", "0002152053-26-000003"),
)
ANY_PRIMARY_DOC = r"https://www\.sec\.gov/Archives/edgar/data/\d+/\d+/primary_doc\.xml"


def fixture_text(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def archive_url(cik: str, accession: str) -> str:
    return ARCHIVE_URL.format(cik=int(cik), accession=accession.replace("-", ""))


# ------------------------------------------------------------------ fixtures and helpers


class FakeClock:
    """Deterministic time (the same stand-in ``tests/test_http.py`` uses): the clocks only move
    when something sleeps, and every sleep is recorded instead of waited for."""

    def __init__(self) -> None:
        self.mono = 1_000.0
        self.wall = 1_700_000_000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.mono

    def time(self) -> float:
        return self.wall

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.mono += seconds
        self.wall += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    # assert_all_mocked=True (the default) makes any un-mocked request fail the test.
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


@pytest.fixture(scope="module")
def regions() -> RegionsConfig:
    return load_regions_config()


def sec_config(**options: Any) -> ConnectorConfig:
    """The real ``sec_edgar`` block of ``config/connectors.yaml`` with ``options`` overridden."""
    config = load_connectors_config().get("sec_edgar")
    return config.model_copy(update={"options": {**config.options, **options}})


@pytest.fixture
def connector(regions: RegionsConfig) -> SecEdgarConnector:
    return SecEdgarConnector(sec_config(), regions)


def make_client(clock: FakeClock, connector: SecEdgarConnector, user_agent: str = UA) -> HttpClient:
    """An ``HttpClient`` exactly as the CLI builds it — the connector's rate limit and
    ``respect_robots`` — but on the fake clock and a memory cache."""
    return HttpClient(
        user_agent=user_agent,
        rate_limit=connector.config.rate_limit,
        respect_robots=connector.config.respect_robots,
        cache=MemoryCache(),
        clock=clock.monotonic,
        sleep=clock.sleep,
        wall_clock=clock.time,
    )


@pytest.fixture
async def client(clock: FakeClock, connector: SecEdgarConnector) -> AsyncIterator[HttpClient]:
    async with make_client(clock, connector) as client:
        yield client


def make_ctx(
    http: HttpClient,
    *,
    now: datetime = NOW,
    since: datetime | None = SINCE,
    last_success_at: datetime | None = None,
) -> FetchContext:
    return FetchContext(
        http=http,
        now=now,
        since=since,
        last_success_at=last_success_at,
        run_id=1,
        log=get_logger("tests.sec_edgar"),
    )


def make_filing(
    xml_name: str,
    *,
    accession: str = "0002149647-26-000001",
    cik: str = "0002149647",
    file_number: str | None = "021-594643",
    form: str = "D",
    filed_date: date = date(2026, 8, 18),
) -> FormDFiling:
    """A ``FormDFiling`` around a recorded primary document (defaults: Obsidian's hit)."""
    return FormDFiling(
        accession=accession,
        cik=cik,
        file_number=file_number,
        form=form,
        filed_date=filed_date,
        url=archive_url(cik, accession),
        xml=fixture_text(xml_name),
        hit={},
    )


# ------------------------------------------------------------------ parse_form_d (pure)


def test_parse_obsidian_security() -> None:
    """A plain Palo Alto equity filing: every field the connector uses."""
    form_d = parse_form_d(fixture_text("obsidian_security_D.xml"))
    assert form_d.submission_type == "D"
    assert form_d.test_or_live == "LIVE"
    assert form_d.cik == "0002149647"
    assert form_d.entity_name == "Obsidian Security, Inc."
    assert (form_d.city, form_d.state, form_d.state_description) == (
        "PALO ALTO",
        "CA",
        "CALIFORNIA",
    )
    assert form_d.jurisdiction_of_inc == "DELAWARE"
    assert form_d.entity_type == "Corporation"
    assert form_d.year_of_inc is None  # <overFiveYears>true</overFiveYears>, no <value>
    assert form_d.industry_group == "Other Technology"
    assert form_d.revenue_range == "Decline to Disclose"
    assert form_d.federal_exemptions == ("06b",)
    assert form_d.is_amendment is False
    assert form_d.previous_accession is None
    assert form_d.date_of_first_sale == date(2026, 7, 22)
    assert form_d.security_types == ("isEquityType",)
    assert form_d.description_of_other_type is None
    assert form_d.is_business_combination is False
    assert form_d.minimum_investment == 0
    assert form_d.total_offering_amount == 109_868_667
    assert form_d.total_amount_sold == 81_463_045
    assert form_d.total_remaining == 28_405_622
    assert form_d.investors_already_invested == 22

    assert len(form_d.related_persons) == 7
    paul = form_d.related_persons[0]
    assert paul.full_name == "Paul Luongo"
    assert paul.relationships == ("Executive Officer",)
    assert paul.clarification is None  # empty element
    hasan = form_d.related_persons[1]
    assert hasan.relationships == ("Executive Officer", "Director")
    assert form_d.related_persons[2].full_name == "Glenn Lindores Chisholm"  # middle name
    assert form_d.related_persons[2].relationships == ("Director",)


def test_parse_festimo_safe_with_indefinite_amounts() -> None:
    form_d = parse_form_d(fixture_text("festimo_D.xml"))
    assert form_d.entity_name == "FESTIMO INC"
    assert (form_d.city, form_d.state) == ("SAN JOSE", "CA")
    assert form_d.year_of_inc == 2026  # <withinFiveYears>true</withinFiveYears> + <value>
    assert form_d.security_types == ("isOtherType",)
    assert form_d.description_of_other_type == "Simple Agreement for Future Equity (SAFE)"
    assert infer_round_type(form_d) is enums.RoundType.SAFE
    assert form_d.total_offering_amount is None  # "Indefinite"
    assert form_d.total_remaining is None  # "Indefinite"
    assert form_d.total_amount_sold == 50_000
    assert form_d.minimum_investment == 50_000
    assert form_d.revenue_range == "No Revenues"
    assert form_d.investors_already_invested == 1


def test_parse_kea_cloud_amendment() -> None:
    form_d = parse_form_d(fixture_text("kea_cloud_DA.xml"))
    assert form_d.submission_type == "D/A"
    assert form_d.is_amendment is True
    assert form_d.previous_accession == "0002073579-25-000001"
    assert form_d.date_of_first_sale == date(2025, 5, 30)
    assert form_d.total_amount_sold == 15_903_290
    assert len(form_d.related_persons) == 4


def test_parse_pooled_fund_without_a_first_sale() -> None:
    form_d = parse_form_d(fixture_text("accel_growth_fund_8_D.xml"))
    assert form_d.industry_group == "Pooled Investment Fund"
    assert form_d.entity_type == "Limited Partnership"
    assert form_d.date_of_first_sale is None  # <yetToOccur>true</yetToOccur>
    assert form_d.security_types == ("isPooledInvestmentFundType",)
    assert form_d.federal_exemptions == ("06b", "3C", "3C.7")
    assert form_d.total_amount_sold == 0
    assert form_d.related_persons[0].clarification == (
        "Director of the General Partner of the General Partner"
    )


@pytest.mark.parametrize(
    "xml",
    [
        "<html>not a filing</html>",
        "<edgarSubmission><primaryIssuer><cik>1</cik></primaryIssuer></edgarSubmission>",
        "<edgarSubmission><offeringData/></edgarSubmission>",
        "<edgarSubmission><primaryIssuer><cik>1</cik><entityName>X</entityName>",  # truncated
        "",
    ],
)
def test_parse_rejects_documents_that_are_not_a_form_d(xml: str) -> None:
    with pytest.raises(FormDParseError):
        parse_form_d(xml)
    assert issubclass(FormDParseError, ValueError)


# ------------------------------------------------------------------ infer_round_type


@pytest.mark.parametrize(
    ("security_types", "description", "expected"),
    [
        (("isEquityType",), None, enums.RoundType.UNKNOWN),  # Form D never says seed / Series A
        (("isDebtType",), None, enums.RoundType.DEBT),
        (("isDebtType", "isEquityType"), None, enums.RoundType.UNKNOWN),  # mixed: don't guess
        (("isOtherType",), "Simple Agreement for Future Equity (SAFE)", enums.RoundType.SAFE),
        (("isOtherType",), "SAFEs", enums.RoundType.SAFE),
        (("isOtherType",), "Simple agreement for future equity", enums.RoundType.SAFE),
        (("isOtherType",), "Convertible Promissory Notes", enums.RoundType.CONVERTIBLE_NOTE),
        (("isOtherType",), "Membership Units", enums.RoundType.UNKNOWN),
        ((), None, enums.RoundType.UNKNOWN),
    ],
)
def test_infer_round_type(
    security_types: tuple[str, ...], description: str | None, expected: enums.RoundType
) -> None:
    base = parse_form_d(fixture_text("obsidian_security_D.xml"))
    form_d = dataclasses.replace(
        base, security_types=security_types, description_of_other_type=description
    )
    assert infer_round_type(form_d) is expected


# ------------------------------------------------------------------ to_records (pure)


def test_to_records_obsidian_security(connector: SecEdgarConnector) -> None:
    filing = make_filing("obsidian_security_D.xml")
    records = list(connector.to_records(filing))
    assert len(records) == 1
    record = records[0]
    assert record.name == "Obsidian Security, Inc."  # as filed
    assert record.external_id == "0002149647"  # the 10-digit CIK
    assert record.source_url == filing.url
    assert record.domain is None  # Form D has no website
    assert record.founded_year is None
    assert record.locations == (
        LocationRecord(city="Palo Alto", state="CA", country="US", metro="Bay Area", is_hq=True),
    )
    assert record.sectors == ("Other Technology",)
    assert record.jobs == ()
    assert record.jobs_complete is False

    assert len(record.funding_rounds) == 1
    round_ = record.funding_rounds[0]
    assert round_.external_id == "021-594643"
    assert round_.round_type is enums.RoundType.UNKNOWN
    assert round_.amount_usd == 81_463_045
    assert round_.announced_date == date(2026, 7, 22)
    assert round_.notes is None
    assert round_.source_url == filing.url
    assert round_.investors == ()
    assert round_.raw_payload == {
        "accession": "0002149647-26-000001",
        "file_number": "021-594643",
        "form": "D",
        "filed_date": "2026-08-18",
        "is_amendment": False,
        "previous_accession": None,
        "industry_group": "Other Technology",
        "total_offering_amount": 109_868_667,
        "total_amount_sold": 81_463_045,
        "total_remaining": 28_405_622,
        "security_types": ["isEquityType"],
        "description_of_other_type": None,
        "federal_exemptions": ["06b"],
        "investors_already_invested": 22,
        "revenue_range": "Decline to Disclose",
        "entity_type": "Corporation",
        "jurisdiction_of_inc": "DELAWARE",
        "is_business_combination": False,
        "minimum_investment": 0,
    }
    json.dumps(round_.raw_payload)  # JSONB-safe

    assert len(record.people) == 7
    by_name = {person.full_name: person for person in record.people}
    assert by_name["Paul Luongo"].title == "Executive Officer"
    assert by_name["Paul Luongo"].role_type is enums.RoleType.EXEC
    assert by_name["Hasan Imam"].title == "Executive Officer, Director"
    assert by_name["Hasan Imam"].role_type is enums.RoleType.EXEC
    assert by_name["Glenn Lindores Chisholm"].title == "Director"
    assert by_name["Glenn Lindores Chisholm"].role_type is None
    assert all(person.linkedin_url is None for person in record.people)  # SPEC §6: never fetched
    assert connector.skipped == {}


def test_to_records_festimo(connector: SecEdgarConnector) -> None:
    filing = make_filing(
        "festimo_D.xml",
        accession="0002151025-26-000003",
        cik="0002151025",
        file_number="021-594734",
        filed_date=date(2026, 8, 19),
    )
    (record,) = connector.to_records(filing)
    assert record.external_id == "0002151025"
    assert record.founded_year == 2026
    assert record.locations[0].city == "San Jose"  # configured spelling, not "SAN JOSE"
    assert record.locations[0].metro == "Bay Area"
    (round_,) = record.funding_rounds
    assert round_.round_type is enums.RoundType.SAFE
    assert round_.amount_usd == 50_000
    assert round_.announced_date == date(2026, 8, 1)
    assert round_.raw_payload["total_offering_amount"] is None  # "Indefinite"
    assert round_.raw_payload["total_remaining"] is None
    assert round_.raw_payload["description_of_other_type"] == (
        "Simple Agreement for Future Equity (SAFE)"
    )
    (person,) = record.people
    assert person.full_name == "Sandeep Nama"
    assert person.role_type is enums.RoleType.EXEC


def test_to_records_kea_cloud_amendment(connector: SecEdgarConnector) -> None:
    filing = make_filing(
        "kea_cloud_DA.xml",
        accession="0002073579-26-000001",
        cik="0002073579",
        file_number="021-549965",
        form="D/A",
        filed_date=date(2026, 8, 3),
    )
    (record,) = connector.to_records(filing)
    (round_,) = record.funding_rounds
    assert round_.external_id == "021-549965"  # shared with the original filing
    assert round_.notes == "Form D/A (amendment of 0002073579-25-000001)"
    assert round_.raw_payload["form"] == "D/A"
    assert round_.raw_payload["is_amendment"] is True
    assert round_.raw_payload["previous_accession"] == "0002073579-25-000001"
    assert round_.amount_usd == 15_903_290
    assert round_.announced_date == date(2025, 5, 30)


def test_to_records_uses_the_filing_date_when_the_first_sale_is_missing(
    connector: SecEdgarConnector,
) -> None:
    """A pooled fund would be skipped, so re-label it to reach the round mapping."""
    xml = fixture_text("accel_growth_fund_8_D.xml").replace(
        "<industryGroupType>Pooled Investment Fund</industryGroupType>",
        "<industryGroupType>Other Technology</industryGroupType>",
    )
    filing = dataclasses.replace(make_filing("obsidian_security_D.xml"), xml=xml)
    (record,) = connector.to_records(filing)
    (round_,) = record.funding_rounds
    assert round_.announced_date == filing.filed_date  # <yetToOccur>
    assert round_.amount_usd is None  # totalAmountSold 0 → undisclosed


def test_to_records_whatnot_promoters_are_founders(connector: SecEdgarConnector) -> None:
    filing = make_filing("whatnot_D.xml", accession="0001844768-26-000001", cik="0001844768")
    (record,) = connector.to_records(filing)
    by_name = {person.full_name: person for person in record.people}
    assert by_name["Grant LaFontaine"].role_type is enums.RoleType.FOUNDER
    assert by_name["Grant LaFontaine"].title == "Executive Officer, Director, Promoter"
    assert by_name["Logan Head"].role_type is enums.RoleType.FOUNDER
    assert by_name["Connie Chan"].role_type is None
    assert by_name["Connie Chan"].title == "Director"
    assert record.funding_rounds[0].amount_usd == 547_000_000


def test_to_records_coverbase_clarification_is_the_title(connector: SecEdgarConnector) -> None:
    filing = make_filing("coverbase_D.xml", accession="0002145311-26-000001", cik="0002145311")
    (record,) = connector.to_records(filing)
    by_name = {person.full_name: person for person in record.people}
    assert by_name["Clarence Chio"].title == "President, CEO, and Director"
    assert by_name["Clarence Chio"].role_type is enums.RoleType.EXEC
    assert by_name["Zi Chong Kao"].title == "Secretary and Director"
    assert by_name["Walker Forehand"].title == "Director"
    assert by_name["Walker Forehand"].role_type is None


def test_to_records_founder_in_clarification(connector: SecEdgarConnector) -> None:
    xml = fixture_text("coverbase_D.xml").replace(
        "President, CEO, and Director", "Co-Founder and Chief Executive Officer"
    )
    filing = dataclasses.replace(make_filing("coverbase_D.xml"), xml=xml)
    (record,) = connector.to_records(filing)
    clarence = next(p for p in record.people if p.full_name == "Clarence Chio")
    assert clarence.role_type is enums.RoleType.FOUNDER
    assert clarence.title == "Co-Founder and Chief Executive Officer"


def test_to_records_industry_group_other_yields_no_sector(connector: SecEdgarConnector) -> None:
    filing = make_filing("teal_health_D.xml", accession="0001910391-26-000001", cik="0002046595")
    (record,) = connector.to_records(filing)
    assert record.name == "Teal Health, Inc."
    assert record.sectors == ()
    assert record.locations[0].city == "San Francisco"


def test_to_records_skips_pooled_investment_funds(connector: SecEdgarConnector) -> None:
    """SPEC §4 wants companies raising capital; a fund is the investor, not a startup."""
    filing = make_filing(
        "accel_growth_fund_8_D.xml", accession="0002138027-26-000001", cik="0002138027"
    )
    assert list(connector.to_records(filing)) == []
    assert connector.skipped == {"excluded_industry": 1}


def test_to_records_skips_an_issuer_outside_the_region(connector: SecEdgarConnector) -> None:
    """Lovable Labs matched "Palo Alto" through a director's address; the issuer's own is not.

    The recorded issuer address is Boston MA, which SPEC §12 Phase 7 made a configured city, so
    the *issuer's* address is moved out of every region here rather than the fixture being
    re-recorded. Only the issuer block is rewritten (its twelve-space indent is unique in the
    file): the director in Palo Alto stays exactly where the filing puts him, because he is the
    reason the phrase search surfaced this filing at all and the whole point of the test is that
    a related person's in-region address never carries the company in.
    """
    xml = fixture_text("lovable_labs_D.xml").replace(
        "            <city>Boston</city>\n            <stateOrCountry>MA</stateOrCountry>",
        "            <city>Providence</city>\n            <stateOrCountry>RI</stateOrCountry>",
    )
    filing = dataclasses.replace(
        make_filing("lovable_labs_D.xml", accession=LOVABLE[0], cik=LOVABLE[1]), xml=xml
    )
    assert "<city>Providence</city>" in xml  # the rewrite landed, so the skip below means something
    assert "Palo Alto" in xml  # ... and the in-region related person survived it
    assert list(connector.to_records(filing)) == []
    assert connector.skipped == {"issuer_outside_region": 1}


def test_to_records_skips_test_filings(connector: SecEdgarConnector) -> None:
    xml = fixture_text("obsidian_security_D.xml").replace(
        "<testOrLive>LIVE</testOrLive>", "<testOrLive>TEST</testOrLive>"
    )
    filing = dataclasses.replace(make_filing("obsidian_security_D.xml"), xml=xml)
    assert list(connector.to_records(filing)) == []
    assert connector.skipped == {"test_filing": 1}


def test_to_records_raises_on_unparseable_xml(connector: SecEdgarConnector) -> None:
    """The pipeline logs a failing item and carries on; the connector does not hide it."""
    filing = dataclasses.replace(make_filing("obsidian_security_D.xml"), xml="<html/>")
    with pytest.raises(FormDParseError):
        list(connector.to_records(filing))


def test_excluded_industry_groups_come_from_config(regions: RegionsConfig) -> None:
    connector = SecEdgarConnector(sec_config(exclude_industry_groups=[]), regions)
    filing = make_filing(
        "accel_growth_fund_8_D.xml", accession="0002138027-26-000001", cik="0002138027"
    )
    (record,) = connector.to_records(filing)
    assert record.sectors == ("Pooled Investment Fund",)


# ------------------------------------------------------------------ configuration


def test_registered_under_its_name() -> None:
    assert get_connector_class("sec_edgar") is SecEdgarConnector
    assert SecEdgarConnector.name == "sec_edgar"


def test_options_default_and_validate(regions: RegionsConfig) -> None:
    connector = SecEdgarConnector(sec_config(), regions)
    assert connector.options.backfill_months == 18
    assert connector.options.overlap_days == 3
    assert connector.options.search_window_days == 30
    assert connector.options.exclude_industry_groups == ("Pooled Investment Fund",)
    assert connector.cadence == "0 5 */3 * *"  # from config/connectors.yaml, nowhere else
    bare = ConnectorConfig(name="sec_edgar")
    assert SecEdgarConnector(bare, regions).options == connector.options
    with pytest.raises(ValidationError):  # a typo in the YAML fails fast
        SecEdgarConnector(sec_config(backfill_month=6), regions)


def test_search_query(connector: SecEdgarConnector, regions: RegionsConfig) -> None:
    assert connector.search_query("Palo Alto") == '"Palo Alto" -"Pooled Investment Fund"'
    two = SecEdgarConnector(sec_config(exclude_industry_groups=["A", "B C"]), regions)
    assert two.search_query("San Jose") == '"San Jose" -"A" -"B C"'
    none = SecEdgarConnector(sec_config(exclude_industry_groups=[]), regions)
    assert none.search_query("Berkeley") == '"Berkeley"'


# ------------------------------------------------------------------ window (SPEC §14.3)


async def test_window_explicit_since_wins(connector: SecEdgarConnector, client: HttpClient) -> None:
    ctx = make_ctx(client, since=SINCE, last_success_at=datetime(2026, 9, 1, tzinfo=UTC))
    assert connector.window(ctx) == (date(2026, 8, 5), date(2026, 9, 3))


async def test_window_incremental_restarts_overlap_days_before_the_last_success(
    connector: SecEdgarConnector, client: HttpClient
) -> None:
    ctx = make_ctx(client, since=None, last_success_at=datetime(2026, 8, 20, 5, tzinfo=UTC))
    assert connector.window(ctx) == (date(2026, 8, 17), date(2026, 9, 3))


async def test_window_first_run_backfills_eighteen_months_clamped(
    connector: SecEdgarConnector, client: HttpClient
) -> None:
    ctx = make_ctx(client, now=datetime(2026, 3, 31, 23, 59, tzinfo=UTC), since=None)
    assert connector.window(ctx) == (date(2024, 9, 30), date(2026, 3, 31))


@pytest.mark.parametrize(
    ("day", "months", "expected"),
    [
        (date(2026, 3, 31), 18, date(2024, 9, 30)),
        (date(2026, 5, 31), 3, date(2026, 2, 28)),
        (date(2024, 2, 29), 12, date(2023, 2, 28)),
        (date(2026, 1, 15), 1, date(2025, 12, 15)),
        (date(2026, 9, 3), 18, date(2025, 3, 3)),
    ],
)
def test_months_before(day: date, months: int, expected: date) -> None:
    assert months_before(day, months) == expected


def test_date_windows() -> None:
    assert list(date_windows(date(2026, 8, 5), date(2026, 9, 3), 30)) == [
        (date(2026, 8, 5), date(2026, 9, 3))
    ]
    assert list(date_windows(date(2026, 8, 1), date(2026, 9, 3), 30)) == [
        (date(2026, 8, 1), date(2026, 8, 30)),
        (date(2026, 8, 31), date(2026, 9, 3)),
    ]
    assert list(date_windows(date(2026, 9, 3), date(2026, 9, 3), 30)) == [
        (date(2026, 9, 3), date(2026, 9, 3))
    ]
    assert list(date_windows(date(2026, 9, 4), date(2026, 9, 3), 30)) == []


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("San Jose, CA", ("San Jose", "CA")),
        ("Palo Alto, CA", ("Palo Alto", "CA")),
        ("Washington, District of Columbia, DC", ("Washington, District of Columbia", "DC")),
        ("Toronto", None),
        ("Luxembourg", None),
        (", CA", None),
        ("", None),
    ],
)
def test_split_location(value: str, expected: tuple[str, str] | None) -> None:
    assert split_location(value) == expected


# ------------------------------------------------------------------ fetch() on the mocked wire


EMPTY_PAGE: dict[str, Any] = {"hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}}


def palo_alto_page(**overrides: Any) -> dict[str, Any]:
    """The recorded "Palo Alto" search page, optionally with ``hits.total`` etc. patched."""
    page: dict[str, Any] = json.loads(fixture_text("search_palo_alto_2026-08.json"))
    page["hits"].update(overrides)
    return page


def search_side_effect(request: httpx.Request) -> httpx.Response:
    """EFTS as recorded: the Palo Alto page for the "Palo Alto" phrase, nothing for others."""
    if '"Palo Alto"' in request.url.params.get("q", ""):
        return httpx.Response(200, json=palo_alto_page())
    return httpx.Response(200, json=EMPTY_PAGE)


def san_francisco_page() -> dict[str, Any]:
    """The recorded "San Francisco" search page: 65 hits in EFTS score order."""
    page: dict[str, Any] = json.loads(fixture_text("search_san_francisco_2026-08.json"))
    return page


def san_francisco_side_effect(page: dict[str, Any]) -> Any:
    """``page`` for the "San Francisco" phrase (the "South San Francisco" query does not
    contain the quoted phrase), an empty page for every other city."""

    def side_effect(request: httpx.Request) -> httpx.Response:
        if '"San Francisco"' in request.url.params.get("q", ""):
            return httpx.Response(200, json=page)
        return httpx.Response(200, json=EMPTY_PAGE)

    return side_effect


def hits_for_file_number(page: dict[str, Any], file_number: str) -> list[dict[str, Any]]:
    return [
        hit for hit in page["hits"]["hits"] if file_number in (hit["_source"].get("file_num") or ())
    ]


@dataclasses.dataclass
class SecRoutes:
    www_robots: respx.Route
    efts_robots: respx.Route
    search: respx.Route
    xml: dict[str, respx.Route]  # by accession
    outside: dict[str, respx.Route]  # never fetched


def mount_sec(router: respx.MockRouter) -> SecRoutes:
    """Route the recorded fixtures: both robots files, the search endpoint and the three
    Bay Area primary documents; the three out-of-region documents get a route only so the
    test can assert they were never requested."""
    www_robots = router.get("https://www.sec.gov/robots.txt").mock(
        return_value=httpx.Response(200, text=fixture_text("www_sec_gov_robots.txt"))
    )
    efts_robots = router.get("https://efts.sec.gov/robots.txt").mock(
        return_value=httpx.Response(
            403, content=(FIXTURES / "efts_sec_gov_robots_403.json").read_bytes()
        )
    )
    search = router.get(SEARCH_URL).mock(side_effect=search_side_effect)
    xml = {
        accession: router.get(archive_url(cik, accession)).mock(
            return_value=httpx.Response(200, text=fixture_text(name))
        )
        for accession, cik, _file_number, name in BAY_AREA_HITS
    }
    outside = {
        accession: router.get(archive_url(cik, accession)).mock(return_value=httpx.Response(500))
        for accession, cik in OUTSIDE_HITS
    }
    return SecRoutes(www_robots, efts_robots, search, xml, outside)


async def collect(connector: SecEdgarConnector, ctx: FetchContext) -> list[FormDFiling]:
    return [filing async for filing in connector.fetch(ctx)]


def palo_alto_calls(search: respx.Route) -> list[httpx.Request]:
    return [c.request for c in search.calls if '"Palo Alto"' in c.request.url.params["q"]]


async def test_fetch_end_to_end(
    router: respx.MockRouter,
    clock: FakeClock,
    connector: SecEdgarConnector,
    client: HttpClient,
    regions: RegionsConfig,
) -> None:
    routes = mount_sec(router)
    ctx = make_ctx(client)
    filings = await collect(connector, ctx)

    # exactly the three Bay Area hits, with their identifiers from the search hit
    assert {f.accession for f in filings} == {a for a, *_ in BAY_AREA_HITS}
    by_accession = {f.accession: f for f in filings}
    for accession, cik, file_number, name in BAY_AREA_HITS:
        filing = by_accession[accession]
        assert filing.cik == cik
        assert filing.file_number == file_number
        assert filing.url == archive_url(cik, accession)
        assert filing.xml == fixture_text(name)
        assert filing.hit["_source"]["adsh"] == accession
        assert routes.xml[accession].call_count == 1
    assert by_accession[KEA_CLOUD[0]].form == "D/A"
    assert by_accession[KEA_CLOUD[0]].filed_date == date(2026, 8, 3)
    assert by_accession[OBSIDIAN[0]].form == "D"
    assert by_accession[OBSIDIAN[0]].filed_date == date(2026, 8, 18)
    # Centaur (WI), QUDARA (Santa Barbara) and Lovable Labs (Dover, DE) are never fetched
    assert all(route.call_count == 0 for route in routes.outside.values())
    assert connector.skipped == {"outside_region": 3}
    assert ctx.problems == []

    # one search per configured city for the single 30-day window, with the EFTS parameters
    assert routes.search.call_count == len(regions.cities)
    queries = [c.request.url.params["q"] for c in routes.search.calls]
    assert queries == [connector.search_query(city.city) for city in regions.cities]
    for call in routes.search.calls:
        params = call.request.url.params
        assert params["forms"] == "D"
        assert params["dateRange"] == "custom"
        assert params["startdt"] == "2026-08-05"
        assert params["enddt"] == "2026-09-03"
        assert params["from"] == "0"
        assert params["page"] == "1"

    # SPEC §4 etiquette: robots once per host, the contact-bearing UA on every request
    assert routes.www_robots.call_count == 1
    assert routes.efts_robots.call_count == 1
    assert {c.request.headers["user-agent"] for c in router.calls} == {UA}
    assert client.stats.robots_denied == 0
    assert client.stats.requests == len(regions.cities) + 3 + 2
    # the rate limiter (8/s from config/connectors.yaml) spaced the calls; nothing really slept
    assert clock.sleeps
    assert max(clock.sleeps) <= connector.config.rate_limit.min_interval + 1e-9


async def test_fetch_404_on_one_filing_is_a_problem_not_a_failure(
    router: respx.MockRouter, connector: SecEdgarConnector, client: HttpClient
) -> None:
    routes = mount_sec(router)
    routes.xml[OBSIDIAN[0]].mock(return_value=httpx.Response(404))
    ctx = make_ctx(client)
    filings = await collect(connector, ctx)
    assert {f.accession for f in filings} == {KEA_CLOUD[0], FESTIMO[0]}
    assert len(ctx.problems) == 1
    assert OBSIDIAN[0] in ctx.problems[0]
    assert archive_url(OBSIDIAN[1], OBSIDIAN[0]) in ctx.problems[0]
    assert "404" in ctx.problems[0]
    assert connector.skipped == {"outside_region": 3, "fetch_failed": 1}
    assert client.stats.retries == 0  # a 404 is final


async def test_fetch_unparseable_xml_is_a_problem(
    router: respx.MockRouter, connector: SecEdgarConnector, client: HttpClient
) -> None:
    routes = mount_sec(router)
    routes.xml[FESTIMO[0]].mock(return_value=httpx.Response(200, text="<html>maintenance</html>"))
    ctx = make_ctx(client)
    filings = await collect(connector, ctx)
    assert {f.accession for f in filings} == {OBSIDIAN[0], KEA_CLOUD[0]}
    assert len(ctx.problems) == 1
    assert FESTIMO[0] in ctx.problems[0]
    assert connector.skipped == {"outside_region": 3, "parse_failed": 1}


async def test_fetch_paginates_and_dedupes_on_accession(
    router: respx.MockRouter, connector: SecEdgarConnector, client: HttpClient
) -> None:
    """A synthetic two-page result: ``total.value = 150`` → ``from=0`` then ``from=100``. Both
    pages carry the same recorded hits, so the second page is all duplicates."""
    routes = mount_sec(router)

    def two_pages(request: httpx.Request) -> httpx.Response:
        if '"Palo Alto"' in request.url.params.get("q", ""):
            return httpx.Response(200, json=palo_alto_page(total={"value": 150, "relation": "eq"}))
        return httpx.Response(200, json=EMPTY_PAGE)

    routes.search.mock(side_effect=two_pages)
    ctx = make_ctx(client)
    filings = await collect(connector, ctx)
    calls = palo_alto_calls(routes.search)
    assert [c.url.params["from"] for c in calls] == ["0", "100"]
    assert [c.url.params["page"] for c in calls] == ["1", "2"]
    assert len(filings) == 3
    assert all(route.call_count == 1 for route in routes.xml.values())
    assert connector.skipped == {"outside_region": 6, "duplicate_hit": 3}
    assert ctx.problems == []


@pytest.mark.parametrize(
    ("total", "expect_problem"),
    [
        # Elasticsearch stops counting at the cap: this is what EFTS really sends past it.
        ({"value": MAX_HITS_PER_QUERY, "relation": "gte"}, True),
        # A value above the cap never happens on the live API, but must still be reported.
        ({"value": MAX_HITS_PER_QUERY + 1, "relation": "eq"}, True),
        # Exactly 10,000 matches: every hit is reachable, so no false alarm.
        ({"value": MAX_HITS_PER_QUERY, "relation": "eq"}, False),
    ],
)
async def test_fetch_records_a_problem_at_the_hit_cap(
    router: respx.MockRouter,
    connector: SecEdgarConnector,
    client: HttpClient,
    total: dict[str, Any],
    expect_problem: bool,
) -> None:
    routes = mount_sec(router)
    pages = 0

    def capped(request: httpx.Request) -> httpx.Response:
        nonlocal pages
        if '"Palo Alto"' not in request.url.params.get("q", ""):
            return httpx.Response(200, json=EMPTY_PAGE)
        pages += 1
        if pages == 1:
            return httpx.Response(200, json=palo_alto_page(total=total))
        return httpx.Response(200, json=palo_alto_page(total=total, hits=[]))  # exhausted

    routes.search.mock(side_effect=capped)
    ctx = make_ctx(client)
    filings = await collect(connector, ctx)
    assert len(filings) == 3
    assert pages == 2  # an empty page ends the scan instead of walking to the cap
    if not expect_problem:
        assert ctx.problems == []
        return
    assert len(ctx.problems) == 1
    assert '"Palo Alto"' in ctx.problems[0]
    assert "2026-08-05..2026-09-03" in ctx.problems[0]
    assert str(total["value"]) in ctx.problems[0]
    assert ("at least" in ctx.problems[0]) is (total["relation"] == "gte")


async def test_fetch_yields_an_original_before_its_amendment(
    router: respx.MockRouter, connector: SecEdgarConnector, client: HttpClient
) -> None:
    """EFTS ranks hits by score, not date: the recorded "San Francisco" page lists the D/A for
    file number 021-593951 before its original Form D. The pipeline updates a round in place
    with the last record it sees, so the connector must yield filings in filing order —
    original first, then the amendment — or the round would keep the original's stale amount
    and payload under the amendment's notes."""
    routes = mount_sec(router)
    page = san_francisco_page()
    for file_number, original, amendment in SF_AMENDED_ROUNDS:
        recorded = [hit["_source"]["adsh"] for hit in hits_for_file_number(page, file_number)]
        assert set(recorded) == {original, amendment}
    # the trap the fixture sets: the amendment is listed first
    assert [hit["_source"]["form"] for hit in hits_for_file_number(page, "021-593951")] == [
        "D/A",
        "D",
    ]
    routes.search.mock(side_effect=san_francisco_side_effect(page))
    # every primary document answers with a parseable Form D; only the order matters here
    router.get(url__regex=ANY_PRIMARY_DOC).mock(
        return_value=httpx.Response(200, text=fixture_text("obsidian_security_D.xml"))
    )
    ctx = make_ctx(client)
    filings = await collect(connector, ctx)

    assert ctx.problems == []
    assert len(filings) + connector.skipped["outside_region"] == len(page["hits"]["hits"])
    by_file_number: dict[str, list[FormDFiling]] = {}
    for filing in filings:
        by_file_number.setdefault(filing.file_number or "", []).append(filing)
    for file_number, original, amendment in SF_AMENDED_ROUNDS:
        pair = by_file_number[file_number]
        assert [f.accession for f in pair] == [original, amendment]
        assert [f.form for f in pair] == ["D", "D/A"]
        assert pair[0].filed_date <= pair[1].filed_date  # 021-595718: both on 2026-08-28
    # and the whole window comes out in filing order
    assert [f.filed_date for f in filings] == sorted(f.filed_date for f in filings)


async def test_fetch_refuses_to_run_without_a_contact_email(
    router: respx.MockRouter, clock: FakeClock, connector: SecEdgarConnector
) -> None:
    """SPEC §4: SEC requires a contact in the User-Agent; nothing is sent without one."""
    mount_sec(router)
    async with make_client(clock, connector, user_agent="startup-tracker/0.1") as client:
        with pytest.raises(RuntimeError, match="CONTACT_EMAIL"):
            await collect(connector, make_ctx(client))
    assert router.calls.call_count == 0


async def test_fetch_search_failure_propagates(
    router: respx.MockRouter, connector: SecEdgarConnector, client: HttpClient
) -> None:
    """A failed *search* means the scan did not complete: the pipeline must record ``error``
    (and not advance the incremental window), so it is raised rather than swallowed."""
    routes = mount_sec(router)
    routes.search.mock(return_value=httpx.Response(404))
    with pytest.raises(httpx.HTTPStatusError):
        await collect(connector, make_ctx(client))
    assert all(route.call_count == 0 for route in routes.xml.values())


async def test_fetch_respects_robots_for_the_archive(
    router: respx.MockRouter, connector: SecEdgarConnector, client: HttpClient
) -> None:
    """If www.sec.gov ever disallowed the archive, the filings would be skipped as problems,
    never fetched (SPEC §4: honour robots.txt before every request)."""
    routes = mount_sec(router)
    routes.www_robots.mock(
        return_value=httpx.Response(200, text="User-agent: *\nDisallow: /Archives/\n")
    )
    ctx = make_ctx(client)
    filings = await collect(connector, ctx)
    assert filings == []
    assert all(route.call_count == 0 for route in routes.xml.values())
    assert len(ctx.problems) == 3
    assert connector.skipped == {"outside_region": 3, "fetch_failed": 3}
    assert client.stats.robots_denied == 3


async def test_fetch_ignores_hits_without_identifiers(
    router: respx.MockRouter, connector: SecEdgarConnector, client: HttpClient
) -> None:
    routes = mount_sec(router)

    def broken(request: httpx.Request) -> httpx.Response:
        if '"Palo Alto"' in request.url.params.get("q", ""):
            page = palo_alto_page()
            del page["hits"]["hits"][0]["_source"]["ciks"]  # Obsidian loses its CIK
            return httpx.Response(200, json=page)
        return httpx.Response(200, json=EMPTY_PAGE)

    routes.search.mock(side_effect=broken)
    ctx = make_ctx(client)
    filings = await collect(connector, ctx)
    assert {f.accession for f in filings} == {KEA_CLOUD[0], FESTIMO[0]}
    assert len(ctx.problems) == 1
    assert connector.skipped == {"outside_region": 3, "malformed_hit": 1}


async def test_fetch_uses_every_window(
    router: respx.MockRouter,
    connector: SecEdgarConnector,
    client: HttpClient,
    regions: RegionsConfig,
) -> None:
    """2026-08-01..2026-09-03 at 30 days per window is two windows: every city is searched
    twice, with consecutive date ranges."""
    routes = mount_sec(router)
    ctx = make_ctx(client, since=datetime(2026, 8, 1, tzinfo=UTC))
    filings = await collect(connector, ctx)
    assert len(filings) == 3  # the second window's identical hits are duplicates
    assert routes.search.call_count == 2 * len(regions.cities)
    ranges = {
        (c.request.url.params["startdt"], c.request.url.params["enddt"])
        for c in routes.search.calls
    }
    assert ranges == {("2026-08-01", "2026-08-30"), ("2026-08-31", "2026-09-03")}


# ------------------------------------------------------------------ pipeline round trip (database)


async def row_counts(session_factory: async_sessionmaker[AsyncSession]) -> dict[str, int]:
    async with session_factory() as session:
        counts: dict[str, int] = {}
        for table in Base.metadata.sorted_tables:
            count = await session.scalar(select(func.count()).select_from(table))
            counts[table.name] = int(count or 0)
        return counts


async def test_pipeline_round_trip(
    router: respx.MockRouter,
    connector: SecEdgarConnector,
    client: HttpClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """``run_connector`` on the mocked wire (SPEC §7.2, §8, §12 Phase 2): one
    ``FetchRun(ok, 3, 3)``, three companies with their EDGAR provenance, Bay Area locations,
    rounds keyed by file number, people — and a second identical run changes no row counts
    (a ``Source`` row per record per run and the ``FetchRun`` itself are the only additions)."""
    from ingest.pipeline import run_connector

    mount_sec(router)
    run = await run_connector(
        connector, session_factory=session_factory, http=client, since=SINCE, now=NOW
    )
    assert run.status is enums.FetchRunStatus.OK
    assert run.n_fetched == 3
    assert run.n_upserted == 3
    assert run.error_text is None
    assert run.started_at == NOW
    assert run.finished_at is not None

    async with session_factory() as session:
        companies = (await session.scalars(select(Company).order_by(Company.name))).all()
        assert [c.name for c in companies] == [
            "FESTIMO INC",
            "KEA Cloud, Inc.",
            "Obsidian Security, Inc.",
        ]
        festimo, kea, obsidian = companies
        assert obsidian.normalized_name == "obsidian security"
        assert kea.normalized_name == "kea cloud"
        assert all(c.domain is None for c in companies)  # Form D carries no website
        assert all(c.field_provenance["name"] == "sec_edgar" for c in companies)
        assert festimo.founded_year == 2026
        assert festimo.field_provenance["founded_year"] == "sec_edgar"
        assert obsidian.founded_year is None
        assert all(c.first_seen_at == NOW and c.last_seen_at == NOW for c in companies)

        provenance = (await session.scalars(select(CompanySource))).all()
        assert {(p.company_id, p.connector, p.external_id) for p in provenance} == {
            (obsidian.id, "sec_edgar", OBSIDIAN[1]),
            (kea.id, "sec_edgar", KEA_CLOUD[1]),
            (festimo.id, "sec_edgar", FESTIMO[1]),
        }

        locations = (await session.scalars(select(Location).order_by(Location.city))).all()
        assert [(lo.city, lo.state, lo.metro, lo.country) for lo in locations] == [
            ("Palo Alto", "CA", "Bay Area", "US"),
            ("San Jose", "CA", "Bay Area", "US"),
        ]
        links = (await session.scalars(select(CompanyLocation))).all()
        assert len(links) == 3
        assert all(link.is_hq for link in links)

        sectors = (await session.scalars(select(Sector))).all()
        assert [(s.name, s.slug) for s in sectors] == [("Other Technology", "other-technology")]

        rounds = (await session.scalars(select(FundingRound))).all()
        by_company = {r.company_id: r for r in rounds}
        assert len(rounds) == 3
        for company, hit in ((obsidian, OBSIDIAN), (kea, KEA_CLOUD), (festimo, FESTIMO)):
            round_ = by_company[company.id]
            assert round_.raw_payload is not None
            assert round_.raw_payload["external_id"] == hit[2]  # the SEC file number
            assert round_.raw_payload["connector"] == "sec_edgar"
            assert round_.raw_payload["accession"] == hit[0]
            assert round_.source_id is not None
            assert company.latest_round_id == round_.id  # denormalized at end of run (SPEC §5)
            assert company.open_job_count == 0
            assert company.latest_job_posted_at is None
        assert by_company[obsidian.id].amount_usd == 81_463_045
        assert by_company[obsidian.id].announced_date == date(2026, 7, 22)
        assert by_company[kea.id].notes == "Form D/A (amendment of 0002073579-25-000001)"
        assert by_company[festimo.id].round_type is enums.RoundType.SAFE

        people = (await session.scalars(select(Person))).all()
        assert len(people) == 7 + 4 + 1
        founders_and_execs = {p.full_name for p in people if p.role_type is enums.RoleType.EXEC}
        assert {"Paul Luongo", "Hasan Imam", "Adam Ahmad", "Sandeep Nama"} <= founders_and_execs

    first = await row_counts(session_factory)
    assert first["fetch_runs"] == 1
    assert first["merge_candidates"] == 0
    assert first["jobs"] == 0

    again = await run_connector(
        connector, session_factory=session_factory, http=client, since=SINCE, now=NOW
    )
    assert again.status is enums.FetchRunStatus.OK
    assert (again.n_fetched, again.n_upserted) == (3, 3)
    second = await row_counts(session_factory)
    assert second.pop("fetch_runs") == first.pop("fetch_runs") + 1
    assert second.pop("sources") == first.pop("sources") + 3  # one Source per record per run
    assert second == first


async def test_pipeline_amendment_updates_the_round_after_its_original(
    router: respx.MockRouter,
    connector: SecEdgarConnector,
    client: HttpClient,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The two 021-593951 hits exactly as EFTS returned them (amendment first). Both primary
    documents are Obsidian's filing re-labelled — same issuer CIK, so one company — with the
    D/A reporting more sold and pointing back at the original. The single round (one per
    file number, SPEC §5) must end as the amendment left it, not as the original did."""
    from ingest.pipeline import run_connector

    routes = mount_sec(router)
    page = san_francisco_page()
    pair = hits_for_file_number(page, "021-593951")
    assert [hit["_source"]["form"] for hit in pair] == ["D/A", "D"]  # the recorded order
    amendment_hit, original_hit = pair
    page["hits"] = {"total": {"value": 2, "relation": "eq"}, "hits": pair}
    routes.search.mock(side_effect=san_francisco_side_effect(page))

    original_xml = fixture_text("obsidian_security_D.xml")
    amendment_xml = original_xml.replace(
        "<isAmendment>false</isAmendment>",
        "<isAmendment>true</isAmendment>"
        "<previousAccessionNumber>0002139601-26-000001</previousAccessionNumber>",
    ).replace(
        "<totalAmountSold>81463045</totalAmountSold>",
        "<totalAmountSold>95000000</totalAmountSold>",
    )
    assert "<isAmendment>true</isAmendment>" in amendment_xml
    assert "<totalAmountSold>95000000</totalAmountSold>" in amendment_xml
    for hit, xml in ((original_hit, original_xml), (amendment_hit, amendment_xml)):
        source = hit["_source"]
        router.get(archive_url(source["ciks"][0], source["adsh"])).mock(
            return_value=httpx.Response(200, text=xml)
        )

    run = await run_connector(
        connector, session_factory=session_factory, http=client, since=SINCE, now=NOW
    )
    assert run.status is enums.FetchRunStatus.OK
    assert run.n_fetched == 2
    async with session_factory() as session:
        companies = (await session.scalars(select(Company))).all()
        assert len(companies) == 1
        rounds = (await session.scalars(select(FundingRound))).all()
        assert len(rounds) == 1  # one round per file number
        (round_,) = rounds
        assert round_.raw_payload is not None
        assert round_.raw_payload["external_id"] == "021-593951"
        assert round_.raw_payload["accession"] == "0002139601-26-000002"
        assert round_.raw_payload["form"] == "D/A"
        assert round_.raw_payload["is_amendment"] is True
        assert round_.raw_payload["total_amount_sold"] == 95_000_000
        assert round_.amount_usd == 95_000_000
        assert round_.notes == "Form D/A (amendment of 0002139601-26-000001)"
