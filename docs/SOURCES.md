# Data sources

SPEC §4 requires every connector to be documented here: its endpoint(s), whether it is an
official API, the terms that apply, the rate limit we apply, and the fields we extract. One
section per connector in `config/connectors.yaml`; a heading with no body is a Phase 3
connector that is not implemented yet.

Shared rules, enforced by `ingest/http.py` for every connector (SPEC §4): a declared
`User-Agent` (`startup-tracker/0.1 <contact email>`), a per-host rate limit and optional
per-run page budget from `config/connectors.yaml`, `robots.txt` fetched once per host and
honoured before every request (matched per RFC 9309 — `*` and `$` wildcards, most specific
rule wins, fractional `Crawl-delay` — because the older `urllib` parser ignores all three and
so silently *widens* what we would fetch), tenacity retries with exponential backoff (and
`Retry-After`), and a 24-hour ETag/Last-Modified cache. A `robots.txt` that cannot be fetched
at all (5xx, transport failure) disallows its origin for the rest of the run. Nothing is ever fetched from LinkedIn, Crunchbase,
Wellfound/AngelList, and no email address is guessed or SMTP-verified (SPEC §4 "Excluded").

---

## `sec_edgar` — SEC EDGAR Form D filings

**Official API: yes.** Form D is the notice companies file with the SEC when they raise
private capital under Regulation D. EDGAR is a public, documented government system; the data
is public record and free to use.

### Endpoints

| purpose | URL |
|---|---|
| full-text search | `https://efts.sec.gov/LATEST/search-index?q="<city>" -"Pooled Investment Fund"&forms=D&dateRange=custom&startdt=…&enddt=…&from=<offset>&page=<n>` |
| primary document | `https://www.sec.gov/Archives/edgar/data/{cik as int}/{accession without dashes}/primary_doc.xml` |

The search API ignores its own `locationCode`/`locationType` parameters (verified while
recording `tests/fixtures/sec_edgar/`), so the connector runs one phrase query per city in
`config/regions.yaml` and negates the excluded industry groups in the query (`-"…"` becomes a
`must_not` clause).

### Terms and rate limit

* SEC's [fair-access policy](https://www.sec.gov/os/accessing-edgar-data) requires a
  descriptive `User-Agent` that includes a contact email and allows **at most 10 requests per
  second**. We send `startup-tracker/0.1 <CONTACT_EMAIL>` on every request and cap ourselves
  at **8 requests/second** (`rate_limit.requests_per_second: 8` in `config/connectors.yaml`).
  The WAF rejects any other User-Agent shape (parentheses, URLs, an address without a real
  domain) with `403 Undeclared Automated Tool`; the connector refuses to run at all when the
  User-Agent carries no `@`, and the CLI fails fast when `CONTACT_EMAIL` is unset.
* `robots.txt` (`respect_robots: true`): `www.sec.gov/robots.txt` explicitly allows
  `/Archives/edgar/data` (and disallows `/cgi-bin`, which we never touch);
  `efts.sec.gov/robots.txt` answers `403`, which RFC 9309 treats as "no restrictions". Both
  files are recorded under `tests/fixtures/sec_edgar/` and replayed in the tests.

### Fields extracted

From the search hit: accession number, issuer CIK, SEC file number, form type (`D` / `D/A`),
filing date, business location (city/state — the region pre-filter).

From `primary_doc.xml` (`ingest/connectors/sec_edgar.py::parse_form_d`):

| stored as | Form D field |
|---|---|
| `Company.name` | `entityName`, as filed (SPEC §8 normalization happens in the pipeline) |
| `CompanySource.external_id` | `cik` (10 digits) |
| `Company.founded_year` | `yearOfInc/value` when given (`overFiveYears` ⇒ unknown) |
| `Location` (HQ, `metro` from `config/regions.yaml`) | `issuerAddress/city`, `stateOrCountry` |
| `Sector` | `industryGroupType`, unless it is `Other` |
| `FundingRound.external_id` (in `raw_payload`) | SEC file number — shared by the original filing and every amendment, so a D/A updates the round |
| `FundingRound.amount_usd` | `totalAmountSold` when > 0, else undisclosed (`NULL`) |
| `FundingRound.announced_date` | `dateOfFirstSale/value`, else the filing date |
| `FundingRound.round_type` | `debt` for debt-only offerings, `safe` / `convertible_note` from `descriptionOfOtherType`, otherwise `unknown` — Form D never labels a round |
| `FundingRound.raw_payload` | accession, file number, form, filing date, amendment flag and previous accession, industry group, total offering / sold / remaining amounts, security-type flags, other-type description, federal exemptions, number of investors, revenue range, entity type, jurisdiction, business-combination flag, minimum investment |
| `Person` | each `relatedPersonInfo`: full name, `relationshipClarification` (else the relationships joined) as title, `founder` for Promoters and self-described founders, `exec` for Executive Officers |

### Excluded, and why

* **Pooled investment funds** (`industryGroupType` in `exclude_industry_groups`): venture
  funds file Form D too, but they are the investors, not startups.
* **Issuers outside the configured region**: the phrase search matches the whole document,
  so a hit may come from a *director's* address (Lovable Labs, Boston, matched "Palo Alto");
  the issuer address in the XML is checked again and non-region issuers are dropped.
* **TEST filings** (`testOrLive != LIVE`).
* Search hits whose `biz_locations` resolve to no configured city are not even fetched.

Every skip is counted and logged at info; a filing that cannot be fetched (404, transport
failure after retries, robots refusal) or parsed is recorded in `FetchRun.error_text` and the
run ends `partial` rather than failing (SPEC §7.2).

### Known limitations

* Form D carries **no website or domain**, so EDGAR companies are created without the
  canonical key (SPEC §8 step 1) and are resolved by CIK on later runs, or by name within the
  metro when another connector reports them; a domain arrives from YC / company-site data.
* Form D carries **no round label** (seed, Series A …); `round_type` stays `unknown` unless
  the securities are debt, a SAFE or a convertible note. The amount stored is the amount
  *sold* at filing time, not the round size.
* EFTS caps a query at **10,000 hits** (`hits.total` stops counting there and reports
  `{"value": 10000, "relation": "gte"}` when there are more), so the connector searches in
  `search_window_days` (30-day) windows per city; a window that still overflows is reported as
  a problem and hits past the cap are not fetched.
* EFTS returns hits in **relevance-score order**, not by date — the recorded "San Francisco"
  page lists a D/A ahead of its own original Form D. Because an amendment shares the original's
  file number (one round, updated in place), the connector sorts every window's hits by filing
  date (originals before same-day amendments, then accession number) before fetching, so the
  round always ends as the newest filing left it.
* The phrase search may hit related-person addresses (see above) and the API ignores its
  location filter, so every hit costs one XML fetch to confirm the issuer city.
* One filing can match several city phrases (e.g. "San Francisco" and "South San Francisco");
  hits are de-duplicated on accession number within a run.

### Incremental and backfill policy (SPEC §14.3)

* **First run**: `backfill_months` (18) calendar months back from today.
* **Later runs**: from the start of the newest run that completed its scan (`status` `ok` or
  `partial`) minus `overlap_days` (3), so a filing indexed late is still picked up. A run that
  ended `error` did not finish scanning and does not advance the window.
* `python cli.py refresh --connector sec_edgar --since YYYY-MM-DD` overrides both.
* Cadence: as set in `config/connectors.yaml` (currently every 3 days at 05:00 UTC; SPEC §14.3
  assumes daily incremental — change the cron there, nothing in code).

---

## `ycombinator` — Y Combinator company directory

_Phase 3._

## `greenhouse` — Greenhouse job-board API

_Phase 3._

## `lever` — Lever postings API

_Phase 3._

## `ashby` — Ashby posting API

_Phase 3._

## `workable` — Workable widget API

_Phase 3._

## `hn_hiring` — Hacker News "Who is hiring?" threads

_Phase 3._

## `funding_rss` — funding news feeds

_Phase 3._

## `product_hunt` — Product Hunt GraphQL API

_Phase 3._

## `company_site` — direct company-site fetch (Tier 3, permission-gated)

_Phase 3._

## `opencorporates` — OpenCorporates API (on demand)

_Phase 3._
