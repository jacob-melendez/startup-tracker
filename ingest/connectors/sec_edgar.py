"""SEC EDGAR Form D connector (SPEC §4 Tier 1 #1, §12 Phase 2, §14.3).

Companies raising private capital file **Form D** with the SEC. It is free, official and
documented, and it yields exactly what SPEC §4 promises: the legal name, the issuer's street
address (→ city/state, powering the region filter), an industry classification, the offering
amounts and date of first sale, and the named executives and directors (→ ``Person`` rows).

How a run works
---------------
1. :meth:`SecEdgarConnector.window` picks the date range: ``--since`` when given, else the last
   successful run minus ``overlap_days``, else ``backfill_months`` (18, SPEC §14.3) back.
2. :meth:`SecEdgarConnector.fetch` splits the range into ``search_window_days`` windows and, for
   every configured city (``config/regions.yaml``), pages through EDGAR full-text search
   (:data:`SEARCH_URL`) with a phrase query — the API ignores its own location filter, so the
   city name is searched as a phrase and every hit is re-checked (see the fixtures README).
   Each hit whose business address resolves to a configured city has its ``primary_doc.xml``
   fetched from :data:`ARCHIVE_URL` and is yielded as a :class:`FormDFiling`. That is one
   full-text search per city per window, so a run's query count is the number of cities in
   ``config/regions.yaml`` times the number of windows the date range splits into: adding
   cities or regions to that file lengthens a backfill in proportion, and nothing else in this
   project scales with the city count (``docs/SOURCES.md``).
3. :meth:`SecEdgarConnector.to_records` parses the XML (:func:`parse_form_d`) and maps one
   filing to one :class:`~ingest.base.CompanyRecord` with a location, a sector, a funding round
   and the related persons — or skips it (test filing, pooled investment fund, issuer outside
   the region) with an info log and a counter.

Everything on the wire goes through :class:`ingest.http.HttpClient`, which enforces SEC's
fair-access policy (declared ``User-Agent`` with a contact email, ≤ 10 requests/s; SPEC §4) and
honours ``www.sec.gov/robots.txt``. The connector refuses to run without a contact email.
Terms, rate limits and known limitations are documented in ``docs/SOURCES.md`` (SPEC §4).
"""

from __future__ import annotations

import calendar
import xml.etree.ElementTree as ET
from collections import Counter
from collections.abc import AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, ConfigDict, Field

from db import enums
from ingest.base import (
    CompanyRecord,
    Connector,
    FetchContext,
    FundingRoundRecord,
    LocationRecord,
    PersonRecord,
)
from ingest.config import ConnectorConfig, RegionsConfig
from ingest.http import RobotsDisallowed
from logging_config import get_logger

log = get_logger(__name__)

#: EDGAR full-text search (SPEC §4: ``https://efts.sec.gov/LATEST/search-index?q=...&forms=D``).
SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
#: The filing's primary document. ``cik`` is the issuer CIK as an int (no leading zeros) and
#: ``accession`` the accession number without dashes — see the fixtures README.
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/primary_doc.xml"
#: EFTS returns 100 hits per page ...
PAGE_SIZE = 100
#: ... and never more than 10,000 per query, which is why searches run in date windows.
MAX_HITS_PER_QUERY = 10_000
#: The environment variable the CLI reads the contact email from (``Settings.contact_email``).
CONTACT_EMAIL_VAR = "CONTACT_EMAIL"

# Form D flags under <typesOfSecuritiesOffered>; the parser records the ones that are "true".
_DEBT_FLAG = "isDebtType"
_EQUITY_FLAG = "isEquityType"
# Related-person relationships as Form D spells them.
_EXECUTIVE_OFFICER = "Executive Officer"
_PROMOTER = "Promoter"


class SecEdgarOptions(BaseModel):
    """``options`` for ``sec_edgar`` in ``config/connectors.yaml`` (SPEC §7.2, §14.3).

    Unknown keys are rejected so a typo in the YAML fails at startup instead of silently
    falling back to a default.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: SPEC §14.3: the first run looks back 18 months; later runs are incremental.
    backfill_months: int = Field(default=18, ge=1)
    #: Incremental runs re-scan this many days before the last successful run's start, so a
    #: filing indexed late is not missed.
    overlap_days: int = Field(default=3, ge=0)
    #: One EFTS query covers this many days — keeps every window under the 10,000-hit cap.
    search_window_days: int = Field(default=30, ge=1)
    #: Form D industry groups that are not startups (docs/SOURCES.md).
    exclude_industry_groups: tuple[str, ...] = ("Pooled Investment Fund",)


# ------------------------------------------------------------------------------ raw item


@dataclass(frozen=True, slots=True)
class FormDFiling:
    """One Form D filing as fetched: the search hit plus its primary document.

    ``accession`` is the dashed accession number (``0002149647-26-000001``), ``cik`` the issuer
    CIK exactly as EFTS lists it (10 digits), ``file_number`` the SEC file number the original
    filing and every amendment share (``021-594643`` — the round's ``external_id``), ``form``
    ``D`` or ``D/A``, ``url`` where ``xml`` was fetched from, and ``hit`` the raw EFTS hit.
    """

    accession: str
    cik: str
    file_number: str | None
    form: str
    filed_date: date
    url: str
    xml: str
    hit: dict[str, Any]


# ------------------------------------------------------------------------------ Form D XML


class FormDParseError(ValueError):
    """The primary document is not a Form D we can read."""


@dataclass(frozen=True, slots=True)
class RelatedPerson:
    """One ``<relatedPersonInfo>``: an executive officer, director or promoter (SPEC §4)."""

    first_name: str | None
    middle_name: str | None
    last_name: str | None
    relationships: tuple[str, ...]
    clarification: str | None

    @property
    def full_name(self) -> str:
        return " ".join(
            part for part in (self.first_name, self.middle_name, self.last_name) if part
        )


@dataclass(frozen=True, slots=True)
class FormD:
    """The fields of a Form D primary document that the connector uses.

    ``None`` means the filing did not say (SPEC's "None means unknown"): ``year_of_inc`` is
    ``None`` when the issuer ticked ``overFiveYears`` instead of giving a year, the amounts are
    ``None`` when the filing says ``Indefinite``, ``date_of_first_sale`` is ``None`` when the
    sale is ``yetToOccur``. ``security_types`` lists the ``<typesOfSecuritiesOffered>`` flags
    that are ``true`` (``isEquityType``, ``isOtherType``, ...).
    """

    submission_type: str
    test_or_live: str
    cik: str
    entity_name: str
    city: str | None
    state: str | None
    state_description: str | None
    jurisdiction_of_inc: str | None
    entity_type: str | None
    year_of_inc: int | None
    related_persons: tuple[RelatedPerson, ...]
    industry_group: str | None
    revenue_range: str | None
    federal_exemptions: tuple[str, ...]
    is_amendment: bool
    previous_accession: str | None
    date_of_first_sale: date | None
    security_types: tuple[str, ...]
    description_of_other_type: str | None
    is_business_combination: bool
    minimum_investment: int | None
    total_offering_amount: int | None
    total_amount_sold: int | None
    total_remaining: int | None
    investors_already_invested: int | None


def _text(element: ET.Element | None, path: str) -> str | None:
    """Stripped text of the first ``path`` under ``element``; ``None`` when absent or empty
    (Form D writes unanswered fields as empty elements)."""
    if element is None:
        return None
    value = element.findtext(path)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _int(value: str | None) -> int | None:
    """An integer amount, or ``None`` for a missing value or a non-numeric one such as
    ``Indefinite``."""
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _bool(value: str | None) -> bool:
    return value is not None and value.strip().lower() == "true"


def _date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _related_person(info: ET.Element) -> RelatedPerson:
    return RelatedPerson(
        first_name=_text(info, "relatedPersonName/firstName"),
        middle_name=_text(info, "relatedPersonName/middleName"),
        last_name=_text(info, "relatedPersonName/lastName"),
        relationships=tuple(
            text
            for rel in info.findall("relatedPersonRelationshipList/relationship")
            if (text := (rel.text or "").strip())
        ),
        clarification=_text(info, "relationshipClarification"),
    )


def parse_form_d(xml: str) -> FormD:
    """Parse a Form D ``primary_doc.xml`` into a :class:`FormD` (pure; SPEC §4 field list).

    The input is XML published by the SEC's own filing system — well-formed, schema-validated
    on submission, and not attacker-controlled — so the standard-library parser is used as is;
    a hardened parser would guard against entity-expansion tricks that EDGAR never emits.
    Raises :class:`FormDParseError` for malformed XML or a document that is not a Form D
    (no ``<primaryIssuer>`` with a CIK and an entity name).
    """
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        msg = f"not well-formed XML: {exc}"
        raise FormDParseError(msg) from exc
    if root.tag != "edgarSubmission":
        msg = f"expected <edgarSubmission>, got <{root.tag}>"
        raise FormDParseError(msg)
    issuer = root.find("primaryIssuer")
    cik = _text(issuer, "cik")
    entity_name = _text(issuer, "entityName")
    if issuer is None or cik is None or entity_name is None:
        msg = "no <primaryIssuer> with a <cik> and an <entityName>"
        raise FormDParseError(msg)

    offering = root.find("offeringData")
    filing_type = offering.find("typeOfFiling") if offering is not None else None
    securities = offering.find("typesOfSecuritiesOffered") if offering is not None else None
    amounts = offering.find("offeringSalesAmounts") if offering is not None else None
    security_types = tuple(
        child.tag for child in (securities if securities is not None else ()) if _bool(child.text)
    )
    return FormD(
        submission_type=_text(root, "submissionType") or "",
        test_or_live=_text(root, "testOrLive") or "",
        cik=cik,
        entity_name=entity_name,
        city=_text(issuer, "issuerAddress/city"),
        state=_text(issuer, "issuerAddress/stateOrCountry"),
        state_description=_text(issuer, "issuerAddress/stateOrCountryDescription"),
        jurisdiction_of_inc=_text(issuer, "jurisdictionOfInc"),
        entity_type=_text(issuer, "entityType"),
        year_of_inc=_int(_text(issuer, "yearOfInc/value")),
        related_persons=tuple(
            _related_person(info) for info in root.findall("relatedPersonsList/relatedPersonInfo")
        ),
        industry_group=_text(offering, "industryGroup/industryGroupType"),
        revenue_range=_text(offering, "issuerSize/revenueRange"),
        federal_exemptions=tuple(
            text
            for item in (
                offering.findall("federalExemptionsExclusions/item") if offering is not None else ()
            )
            if (text := (item.text or "").strip())
        ),
        is_amendment=_bool(_text(filing_type, "newOrAmendment/isAmendment")),
        previous_accession=_text(filing_type, "newOrAmendment/previousAccessionNumber"),
        date_of_first_sale=_date(_text(filing_type, "dateOfFirstSale/value")),
        security_types=security_types,
        description_of_other_type=_text(securities, "descriptionOfOtherType"),
        is_business_combination=_bool(
            _text(offering, "businessCombinationTransaction/isBusinessCombinationTransaction")
        ),
        minimum_investment=_int(_text(offering, "minimumInvestmentAccepted")),
        total_offering_amount=_int(_text(amounts, "totalOfferingAmount")),
        total_amount_sold=_int(_text(amounts, "totalAmountSold")),
        total_remaining=_int(_text(amounts, "totalRemaining")),
        investors_already_invested=_int(_text(offering, "investors/totalNumberAlreadyInvested")),
    )


def infer_round_type(form_d: FormD) -> enums.RoundType:
    """The most a Form D can tell us about the round (SPEC §5 ``round_type``).

    Form D never labels a round "seed" or "Series A", so the connector never guesses one:
    debt-only offerings are ``DEBT``, an "other" security described as a SAFE is ``SAFE``, one
    described as convertible is ``CONVERTIBLE_NOTE``, and everything else — including plain
    equity — is ``UNKNOWN``. A later connector that knows the label (SPEC §8 priority) may
    overwrite it.
    """
    flags = set(form_d.security_types)
    if _DEBT_FLAG in flags and _EQUITY_FLAG not in flags:
        return enums.RoundType.DEBT
    description = (form_d.description_of_other_type or "").lower()
    if "safe" in description or "simple agreement for future equity" in description:
        return enums.RoundType.SAFE
    if "convertible" in description:
        return enums.RoundType.CONVERTIBLE_NOTE
    return enums.RoundType.UNKNOWN


# ------------------------------------------------------------------------------ dates


def months_before(day: date, months: int) -> date:
    """``day`` minus ``months`` calendar months, the day-of-month clamped to the target month
    (``2026-03-31`` minus 18 months → ``2024-09-30``). SPEC §14.3 counts the backfill in months;
    no ``dateutil`` dependency is needed for that."""
    total = day.year * 12 + (day.month - 1) - months
    year, month0 = divmod(total, 12)
    month = month0 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def date_windows(start: date, end: date, days: int) -> Iterator[tuple[date, date]]:
    """Consecutive inclusive windows of ``days`` days covering ``start``..``end``; the last one is
    cut at ``end``. Empty when ``start > end``."""
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=days - 1), end)
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def split_location(value: str) -> tuple[str, str] | None:
    """``"City, ST"`` → ``("City", "ST")`` on the *last* comma; ``None`` for an entry with no
    comma (``"Toronto"``, ``"Luxembourg"``)."""
    city, sep, state = value.rpartition(", ")
    if not sep:
        return None
    city, state = city.strip(), state.strip()
    if not city or not state:
        return None
    return city, state


def filing_order(hit: dict[str, Any]) -> tuple[str, bool, str]:
    """Sort key that puts EFTS hits into *filing* order: filing date, originals before
    amendments filed the same day, then accession number (a filer's accessions are sequential).

    EFTS ranks hits by relevance score, so an amendment can come back ahead of its original
    (one of the recorded EFTS pages does exactly that for file number 021-593951). The
    original Form D and every D/A share the SEC file number — one round, updated in place by
    the pipeline with whatever record it sees last — so the connector must yield the original
    first or the round would keep the stale amount and payload under the amendment's notes.
    A hit missing these fields sorts first and is then reported as malformed.
    """
    source = hit.get("_source")
    source = source if isinstance(source, dict) else {}
    form = str(source.get("form") or "D")
    return (str(source.get("file_date") or ""), form != "D", str(source.get("adsh") or ""))


# ------------------------------------------------------------------------------ connector


class SecEdgarConnector(Connector[FormDFiling]):
    """Form D filings for the configured cities (SPEC §4 Tier 1 #1, §12 Phase 2).

    ``fetch`` yields one :class:`FormDFiling` per filing whose business address is in a
    configured city; ``to_records`` maps a filing to one ``CompanyRecord``. Skips are counted in
    :attr:`skipped` by reason and logged at info (design decision 13); fetch failures on
    individual filings go to ``ctx.problems`` so the run ends ``partial``, not ``error``. A
    failing *search* request propagates: the scan did not complete, and the pipeline must not
    advance the incremental window past it.
    """

    name: ClassVar[str] = "sec_edgar"

    def __init__(self, config: ConnectorConfig, regions: RegionsConfig) -> None:
        super().__init__(config, regions)
        self.options = SecEdgarOptions.model_validate(config.options)
        #: Skip counters by reason, reset at the start of every :meth:`fetch`.
        self.skipped: Counter[str] = Counter()

    # ------------------------------------------------------------------ window and query

    def window(self, ctx: FetchContext) -> tuple[date, date]:
        """Inclusive UTC date range to search (SPEC §14.3, "--since" in SPEC §2).

        ``--since`` wins when given. Otherwise an incremental run restarts ``overlap_days``
        before the last successful run *started* (a filing indexed late is picked up on the
        next pass), and the very first run backfills ``backfill_months`` calendar months.
        """
        end = ctx.now.date()
        if ctx.since is not None:
            start = ctx.since.date()
        elif ctx.last_success_at is not None:
            start = (ctx.last_success_at - timedelta(days=self.options.overlap_days)).date()
        else:
            start = months_before(end, self.options.backfill_months)
        return start, end

    def search_query(self, city: str) -> str:
        """The EFTS ``q`` for one city: the city as a phrase, minus every excluded industry
        group as a negated phrase — ``'"<city>" -"Pooled Investment Fund"'``. EFTS turns
        ``-"..."`` into a ``must_not`` clause (fixtures README), so funds never even show up."""
        negations = "".join(f' -"{group}"' for group in self.options.exclude_industry_groups)
        return f'"{city}"{negations}'

    # ------------------------------------------------------------------ fetch

    async def fetch(self, ctx: FetchContext) -> AsyncIterator[FormDFiling]:
        """Search every window and city, fetch each in-region filing's XML, yield it.

        Refuses to run when the User-Agent carries no contact email: SEC's fair-access policy
        requires one (SPEC §4) and its WAF answers 403 without it.
        """
        if "@" not in ctx.http.user_agent:
            msg = (
                "sec_edgar needs a contact email in the User-Agent (SEC fair-access policy, "
                f"SPEC §4); set {CONTACT_EMAIL_VAR} — got {ctx.http.user_agent!r}"
            )
            raise RuntimeError(msg)
        self.skipped = Counter()
        start, end = self.window(ctx)
        seen: set[str] = set()
        n_windows = n_hits = n_yielded = 0
        ctx.log.info(
            "sec_edgar.start",
            start=start.isoformat(),
            end=end.isoformat(),
            window_days=self.options.search_window_days,
            cities=len(self.regions.cities),
        )
        for window_start, window_end in date_windows(start, end, self.options.search_window_days):
            n_windows += 1
            # Every city's hits for the window are collected first and then processed in
            # filing order (see filing_order): windows advance chronologically, so across the
            # whole run an original Form D is always yielded before its amendment.
            hits: list[dict[str, Any]] = []
            for city in self.regions.cities:
                async for hit in self._search(ctx, city.city, window_start, window_end):
                    hits.append(hit)
            n_hits += len(hits)
            hits.sort(key=filing_order)
            for hit in hits:
                filing = await self._fetch_filing(ctx, hit, seen)
                if filing is not None:
                    n_yielded += 1
                    yield filing
        ctx.log.info(
            "sec_edgar.summary",
            windows=n_windows,
            cities=len(self.regions.cities),
            hits=n_hits,
            yielded=n_yielded,
            skipped=dict(self.skipped),
            problems=len(ctx.problems),
            requests=ctx.http.stats.requests,
        )

    async def _search(
        self, ctx: FetchContext, city: str, start: date, end: date
    ) -> AsyncIterator[dict[str, Any]]:
        """Page through EFTS for one city and window: ``from``/``page`` advance by
        :data:`PAGE_SIZE` until ``min(total, MAX_HITS_PER_QUERY)`` is reached. A window that
        overflows the cap is reported in ``ctx.problems`` — the hits past 10,000 are
        unreachable, and the operator should shrink ``search_window_days``.

        Elasticsearch stops counting at the cap, so an overflowing window is reported as
        ``{"value": 10000, "relation": "gte"}`` — ``value`` never exceeds the cap on the live
        API (fixtures README). Any ``relation`` other than ``eq`` therefore means overflow; a
        ``value`` above the cap is treated the same way defensively."""
        query = self.search_query(city)
        offset, page, limit = 0, 1, MAX_HITS_PER_QUERY
        while offset < limit:
            data = await ctx.http.get_json(
                SEARCH_URL,
                params={
                    "q": query,
                    "forms": "D",
                    "dateRange": "custom",
                    "startdt": start.isoformat(),
                    "enddt": end.isoformat(),
                    "from": offset,
                    "page": page,
                },
            )
            block = data.get("hits") if isinstance(data, dict) else None
            block = block if isinstance(block, dict) else {}
            hits = [hit for hit in (block.get("hits") or ()) if isinstance(hit, dict)]
            if page == 1:
                total = block.get("total")
                total_value, relation = 0, "eq"
                if isinstance(total, dict):
                    total_value = int(total.get("value") or 0)
                    relation = str(total.get("relation") or "eq")
                overflowing = relation != "eq" or total_value > MAX_HITS_PER_QUERY
                if overflowing:
                    reported = f"at least {total_value}" if relation != "eq" else str(total_value)
                    problem = (
                        f"search for {query!r} {start.isoformat()}..{end.isoformat()} has "
                        f"{reported} hits, at or above the EFTS cap of {MAX_HITS_PER_QUERY}; "
                        "hits beyond the cap were not fetched (lower search_window_days)"
                    )
                    ctx.problems.append(problem)
                    ctx.log.warning(
                        "sec_edgar.hit_cap", city=city, total=total_value, relation=relation
                    )
                limit = MAX_HITS_PER_QUERY if overflowing else min(total_value, MAX_HITS_PER_QUERY)
                ctx.log.debug(
                    "sec_edgar.search",
                    city=city,
                    start=start.isoformat(),
                    end=end.isoformat(),
                    total=total_value,
                    relation=relation,
                )
            for hit in hits:
                yield hit
            if not hits:
                break
            offset += PAGE_SIZE
            page += 1

    def _resolve_business_location(self, entries: Iterable[Any]) -> bool:
        """Whether any ``_source.biz_locations`` entry (``"City, ST"``) is a configured city.
        Entries without a comma are ignored. The phrase search matches the whole document, so a
        hit can come back for a company headquartered elsewhere whose *director* lives in the
        searched city; ``to_records`` re-checks the issuer address from the XML."""
        for entry in entries:
            if not isinstance(entry, str):
                continue
            parts = split_location(entry)
            if parts is not None and self.regions.lookup(*parts) is not None:
                return True
        return False

    async def _fetch_filing(
        self, ctx: FetchContext, hit: dict[str, Any], seen: set[str]
    ) -> FormDFiling | None:
        """Turn one EFTS hit into a :class:`FormDFiling`, or ``None`` when it is skipped.

        Skipped: business address outside the region, a duplicate of an accession already
        yielded (one filing matches several city phrases), a hit missing its identifiers, an
        XML fetch failure (404, transport error after retries, robots refusal) or an XML that
        does not parse — the last three also land in ``ctx.problems``.
        """
        source = hit.get("_source")
        source = source if isinstance(source, dict) else {}
        try:
            adsh = str(source["adsh"])
            cik = str(source["ciks"][0])
            filed_date = date.fromisoformat(str(source["file_date"]))
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            self.skipped["malformed_hit"] += 1
            problem = f"search hit {hit.get('_id')!r} is missing its identifiers: {exc!r}"
            ctx.problems.append(problem)
            ctx.log.warning("sec_edgar.malformed_hit", hit_id=hit.get("_id"), error=repr(exc))
            return None
        biz_locations = source.get("biz_locations") or ()
        if not self._resolve_business_location(biz_locations):
            self.skipped["outside_region"] += 1
            ctx.log.info(
                "sec_edgar.skip",
                reason="outside_region",
                accession=adsh,
                biz_locations=list(biz_locations),
            )
            return None
        if adsh in seen:
            self.skipped["duplicate_hit"] += 1
            ctx.log.debug("sec_edgar.skip", reason="duplicate_hit", accession=adsh)
            return None
        seen.add(adsh)

        url = ARCHIVE_URL.format(cik=int(cik), accession=adsh.replace("-", ""))
        try:
            xml = await ctx.http.get_text(url)
        except (httpx.HTTPStatusError, httpx.TransportError, RobotsDisallowed) as exc:
            self.skipped["fetch_failed"] += 1
            ctx.problems.append(f"{adsh}: could not fetch {url}: {exc}")
            ctx.log.warning("sec_edgar.fetch_failed", accession=adsh, url=url, error=str(exc))
            return None
        try:
            parse_form_d(xml)  # validate now so the pipeline never sees an unreadable filing
        except FormDParseError as exc:
            self.skipped["parse_failed"] += 1
            ctx.problems.append(f"{adsh}: could not parse {url}: {exc}")
            ctx.log.warning("sec_edgar.parse_failed", accession=adsh, url=url, error=str(exc))
            return None
        file_numbers = source.get("file_num") or ()
        file_number = str(file_numbers[0]) if file_numbers else None
        return FormDFiling(
            accession=adsh,
            cik=cik,
            file_number=file_number,
            form=str(source.get("form") or "D"),
            filed_date=filed_date,
            url=url,
            xml=xml,
            hit=hit,
        )

    # ------------------------------------------------------------------ to_records

    def to_records(self, raw: FormDFiling) -> Iterable[CompanyRecord]:
        """One ``CompanyRecord`` per filing (SPEC §4's field list), or nothing.

        Skipped with an info log and a counter: TEST filings (``testOrLive``), issuers in an
        excluded industry group (pooled investment funds are the investors, not startups),
        and issuers whose *own* address is outside every configured city — the search phrase
        may have matched a related person's address instead.
        """
        form_d = parse_form_d(raw.xml)
        if form_d.test_or_live != "LIVE":
            return self._skip("test_filing", raw, form_d, test_or_live=form_d.test_or_live)
        if form_d.industry_group in self.options.exclude_industry_groups:
            return self._skip(
                "excluded_industry", raw, form_d, industry_group=form_d.industry_group
            )
        city = self.regions.lookup(form_d.city or "", form_d.state or "")
        if city is None:
            return self._skip(
                "issuer_outside_region", raw, form_d, city=form_d.city, state=form_d.state
            )

        location = LocationRecord(
            city=city.city,  # the configured spelling, not the filing's upper case
            state=city.state,
            country=city.country,
            metro=city.metro,
            is_hq=True,
        )
        sectors = (
            (form_d.industry_group,)
            if form_d.industry_group and form_d.industry_group != "Other"
            else ()
        )
        people = tuple(
            record
            for person in form_d.related_persons
            if (record := _person_record(person)) is not None
        )
        return [
            CompanyRecord(
                name=form_d.entity_name,
                external_id=form_d.cik,
                source_url=raw.url,
                founded_year=form_d.year_of_inc,
                locations=(location,),
                sectors=sectors,
                funding_rounds=(_round_record(raw, form_d),),
                people=people,
            )
        ]

    def _skip(
        self, reason: str, raw: FormDFiling, form_d: FormD, **details: object
    ) -> tuple[CompanyRecord, ...]:
        self.skipped[reason] += 1
        log.info(
            "sec_edgar.skip",
            reason=reason,
            accession=raw.accession,
            cik=form_d.cik,
            name=form_d.entity_name,
            **details,
        )
        return ()


def _person_record(person: RelatedPerson) -> PersonRecord | None:
    """A related person as a ``PersonRecord`` (SPEC §4 "named executives/directors").

    ``title`` is the filer's own clarification ("President, CEO, and Director") when given,
    else the relationships joined. ``role_type``: a self-described founder or a *Promoter* (the
    Form D term for the people who organised the company) is ``FOUNDER``; an Executive Officer
    is ``EXEC``; a plain director has no role type.
    """
    full_name = person.full_name
    if not full_name:
        return None
    clarification = person.clarification
    title = clarification or ", ".join(person.relationships) or None
    role: enums.RoleType | None = None
    if (clarification and "founder" in clarification.lower()) or _PROMOTER in person.relationships:
        role = enums.RoleType.FOUNDER
    elif _EXECUTIVE_OFFICER in person.relationships:
        role = enums.RoleType.EXEC
    return PersonRecord(full_name=full_name, title=title, role_type=role)


def _round_record(raw: FormDFiling, form_d: FormD) -> FundingRoundRecord:
    """The filing as a funding round (SPEC §5 ``FundingRound``).

    ``external_id`` is the SEC file number, shared by the original Form D and every D/A, so an
    amendment updates the round instead of adding one (``FundingRoundRecord`` docstring).
    ``amount_usd`` is the amount *sold* so far — ``None`` when nothing has been sold yet
    (SPEC §5: NULL = undisclosed); the total offering and remaining amounts, and everything
    else the filing says, stay in ``raw_payload``. ``announced_date`` is the date of first sale
    when the filing gives one, else the filing date.
    """
    sold = form_d.total_amount_sold
    return FundingRoundRecord(
        external_id=raw.file_number,
        round_type=infer_round_type(form_d),
        amount_usd=sold if sold is not None and sold > 0 else None,
        announced_date=form_d.date_of_first_sale or raw.filed_date,
        raw_payload={
            "accession": raw.accession,
            "file_number": raw.file_number,
            "form": raw.form,
            "filed_date": raw.filed_date.isoformat(),
            "is_amendment": form_d.is_amendment,
            "previous_accession": form_d.previous_accession,
            "industry_group": form_d.industry_group,
            "total_offering_amount": form_d.total_offering_amount,
            "total_amount_sold": form_d.total_amount_sold,
            "total_remaining": form_d.total_remaining,
            "security_types": list(form_d.security_types),
            "description_of_other_type": form_d.description_of_other_type,
            "federal_exemptions": list(form_d.federal_exemptions),
            "investors_already_invested": form_d.investors_already_invested,
            "revenue_range": form_d.revenue_range,
            "entity_type": form_d.entity_type,
            "jurisdiction_of_inc": form_d.jurisdiction_of_inc,
            "is_business_combination": form_d.is_business_combination,
            "minimum_investment": form_d.minimum_investment,
        },
        notes=(
            f"Form D/A (amendment of {form_d.previous_accession or 'an earlier filing'})"
            if form_d.is_amendment
            else None
        ),
        source_url=raw.url,
    )
