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
at all (5xx, transport failure) disallows its origin for the rest of the run.

**Nothing is ever fetched from LinkedIn, Crunchbase or Wellfound/AngelList, and no email address
is guessed or SMTP-verified** (SPEC §4 "Excluded"). LinkedIn appears in this system only as
*links*: a URL a company published on its own site, or one constructed from its name and domain
and stored for the user to click. No request is ever made to `linkedin.com` and no profile data
is fetched or stored — see the `company_site` section below for how SPEC §6 does it instead.

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
`must_not` clause). **This is the one connector whose cost grows with the region list** — see
"Query volume" under Known limitations below before adding a metro.

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
* **Issuers outside every enabled region**: the phrase search matches the whole document, so a
  hit may come from a *director's* address rather than the issuer's — the recorded fixture has
  Lovable Labs of Boston coming back for the phrase "Palo Alto", which was a false positive when
  the Bay Area was the only configured metro and is an in-region company now that Boston is one.
  Either way the issuer address in the XML is checked against the config again, and an issuer no
  enabled region claims is dropped. Which hits are false positives is therefore a property of
  `config/regions.yaml`, not of the connector.
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
* **Query volume scales with the configured city count — and nothing else in the system does.**
  One phrase query per city per window, so a first run's 18-month backfill is about 19 windows
  times however many cities `config/regions.yaml` enables: 342 searches for the Bay Area's 18
  cities alone, 912 for all 48 across the six shipped metros. The 8 requests/second we cap
  ourselves at (SPEC §4 allows 10) is what turns that count into wall-clock time, so enabling
  another metro lengthens this connector's runs in direct proportion while leaving every other
  connector's untouched — the rest read a national index or one company's board whatever the
  region list says. Each city also brings whatever per-hit XML fetches its matches generate
  (below), which is why the shipped city lists are deliberately tight rather than administratively
  complete.
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

**Official API: no — but a public index YC serves to its own site.** `ycombinator.com/companies`
is a public directory backed by an Algolia search index. Nothing is scraped: the connector reads
the same index, with the same public search key, that the page's own JavaScript uses.

### Endpoints

| purpose | URL |
|---|---|
| credentials | `https://www.ycombinator.com/companies` (the page embeds the Algolia app id and public search key) |
| search | `POST https://{app_id}-dsn.algolia.net/1/indexes/*/queries`, index `YCCompany_production` |

The search key **rotates** — the value older open-source clients hard-code now answers `403` —
so it is read from the directory page at the start of every run. `options.app_id`/`api_key` in
`config/connectors.yaml` are an emergency fallback; with neither the run fails loudly rather
than reporting an empty directory.

### Terms and rate limit

* `www.ycombinator.com/robots.txt` **allows** `/companies` (only `/companies?*`, the faceted
  query strings, is disallowed). `respect_robots: true`; the DSN host redirects `/robots.txt`
  into the REST API, which answers `404` — "unavailable", i.e. unrestricted, under RFC 9309
  §2.3.1.3.
* **2 requests/second**, and a whole run is ~52 requests once a week (one page fetch, one facet
  query, one query per YC batch).
* The public search key is a *search-only* credential YC publishes in its own page source; it is
  sent as a header so it never lands in the on-disk response cache.

### Fields extracted

`name`, `slug` (→ `CompanySource.external_id`), `website` (→ `Company.domain`, `website_url`),
`one_liner`, `long_description` (→ `thesis`), `team_size` (→ `employee_est`),
`industries` + `tags` + `industry`/`subindustry` + the batch as `"YC W24"` (→ `Sector`),
`all_locations` (→ `Location`, filtered to `config/regions.yaml`), `status` (→
`Company.status`: `Active`/`Public` → active, `Acquired` → acquired, `Inactive` → dead) and
`Public`/`Growth` (→ `Company.stage`).

### Known limitations and deliberate omissions

* **Work at a Startup is not ingested.** SPEC §4 says "where accessible"; its listings sit
  behind a login, and this project does not authenticate to or scrape gated pages. YC's
  `isHiring` flag is not a substitute for a job list, so the ATS connectors supply the roles.
* Algolia's `paginationLimitedTo` is **1 000 hits**, so the index cannot be walked by a broad
  query (`"San Francisco"` alone matches 3 141 companies). It is walked one `batch` facet value
  at a time instead — 50 values, the largest holding 398. A facet value that outgrows the limit
  is reported as a run problem rather than silently truncated.
* `stage: Early` is deliberately **not** mapped: it says nothing about which round a company has
  raised, and leaving `Company.stage` unset lets `sec_edgar` (which outranks this connector,
  SPEC §8) fill it from a real Form D.
* Companies whose `all_locations` names no configured city — including `Remote` — are counted
  and dropped: the index is national and this project ingests only the metros
  `config/regions.yaml` enables (SPEC §1). The query cost does not change with that list — the
  index is walked by YC batch either way — only how much of the result survives the filter.
* That city match is **exact on `(city, state)`**, and YC's spelling is not always the config's:
  the directory files companies under `"New York City, NY, USA"` far more often than under
  `"New York, NY"`. The match is exact against *every* spelling `config/regions.yaml` gives a
  city — its canonical `name` and each of its `aliases` — and resolves to the configured entry
  either way, so an alias is what matches YC's string while the canonical name is what reaches
  the database (`uq_locations_city_state`: one row per real place, named by the config). Nothing
  is loosened and nothing is guessed. A substring or fuzzy match would make every containing name
  ambiguous instead, which is the failure `hn_hiring` has to rank its way out of below.
* **The alias list is measured against this directory, not imagined.** All 6204 entries were
  counted on 2026-09-08. 713 give their location as `New York City, NY` against 195 as
  `New York, NY`, so before the alias existed this connector dropped 713 of the 908 companies
  that name that city — 79% of them — for their spelling alone; it was the largest single data
  defect in the six-metro config. 3113 give `San Francisco, CA`, which is already the configured
  spelling, so that city's alias is earned on Hacker News (below) rather than here. And the same
  count is why no two-letter form is configured anywhere: 189 entries give `Los Angeles, CA`,
  while 31 give `LA, Nigeria` — Lagos State, not a nickname for a California metro. An alias is a
  claim about how a source actually spells a place, so it is added from a count of that source
  and from nothing else, and the count that earned it sits beside it in `config/regions.yaml`.

---

## Applicant tracking systems — `greenhouse`, `lever`, `ashby`, `workable`

**Official APIs: yes.** All four are public, documented, JSON endpoints that exist so a company
can publish its own board; given a board token they return every open role. This is the
highest-value source for the actual goal of the project (SPEC §4 Tier 1 #3). The shared run
mechanics live in `ingest/connectors/ats.py`.

### Endpoints

| connector | URL |
|---|---|
| `greenhouse` | `https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true` |
| `lever` | `https://api.lever.co/v0/postings/{token}?mode=json` |
| `ashby` | `https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=true` |
| `workable` | `https://apply.workable.com/api/v1/widget/accounts/{token}?details=true` |

### Board-token discovery (SPEC §4)

A company with no `ats_provider` yet has its own site fetched — the home page, then `/careers`,
`/jobs` … (`options.discovery_paths`) — and the first ATS board URL in the markup becomes its
token. Discovery matches **every** provider's URL shape, not just the running connector's, so
one careers page answers the question for all four: whichever ATS connector runs first persists
the token and the other three simply use it. The token is stored on the company row, so
discovery runs once; it is re-run **only when the stored board answers 404** — in which case the
fresh token replaces the stale one in the same run.

Discovery is allowed to fail. Plenty of careers pages (`checkr.com`, `posthog.com`,
`watershed.com`) render their board in JavaScript, so nothing is discoverable; the connector
records the attempt and moves on rather than inventing a token.

### Terms and rate limit

* **1 request per host per 2 seconds** (`requests_per_second: 0.5`) and `respect_robots: true`
  for all four. That is stricter than these APIs need, and deliberately so: discovery fetches an
  arbitrary company's careers page, which is a Tier-3 fetch (SPEC §4), and a connector's limit is
  set by the strictest thing it touches. Discovery reads at most 3 pages per company, inside
  Tier 3's cap of five per domain per run, and `options.max_discovery_per_run` (15) bounds how
  many companies one run may probe at all. No per-host budget is configured, because one API host
  serves every company's board.
* `robots.txt`: Greenhouse disallows only `/embed/`; Lever allows everything with
  `Crawl-delay: 1` (honoured on top of our own interval); Workable's file is a bare `Disallow:`,
  i.e. allow-all; `api.ashbyhq.com/robots.txt` answers `401`, which RFC 9309 §2.3.1.3 treats as
  unrestricted.

### Fields extracted

| stored as | Greenhouse | Lever | Ashby | Workable |
|---|---|---|---|---|
| `Job.external_id` | `id` | `id` | `id` | `shortcode` |
| `Job.title` | `title` | `text` | `title` | `title` |
| `Job.url` | `absolute_url` | `hostedUrl` | `jobUrl` | `url` |
| `Job.location_text` | `location.name` | `categories.allLocations` | `location` + `secondaryLocations` | `locations[]` / `city`,`state`,`country` |
| `Job.is_remote` | "remote" in the location | `workplaceType == remote` | `isRemote` | `telecommuting` |
| `Job.posted_at` | `first_published`, else `updated_at` | `createdAt` (epoch ms) | `publishedAt` | `published_on`, else `created_at` (**dates**, stored as midnight UTC) |
| `Job.employment_type` | — (keyword rules) | `categories.commitment` | `employmentType` | `employment_type` |
| `Job.compensation_raw` | — | `salaryRange` | `compensation.compensationTierSummary` | — |
| `Job.description_raw` | `content` (doubly HTML-escaped) | `descriptionPlain` | `descriptionPlain` | `description` |
| `Job.raw_payload` | the whole posting | | | |

`role_family`, `seniority` and `flexible_signal` come from `ingest/classify.py` and
`config/classifiers.yaml` for all four (SPEC §7.1). Ashby postings flagged `isListed: false` are
drafts and are skipped.

### Known limitations

* **Workable's widget answers `200` with an empty `jobs` list for account names that were never
  its customers** (verified against `persona`, `rippling`, `writer`). A board with no jobs *and*
  no description is therefore treated as "nothing here", never as an empty board — and never as
  the 404 that triggers re-discovery, which would re-probe the same careers page every night.
* Greenhouse publishes no employment-type field, so its roles fall through to the keyword rules.
* Ashby board tokens are case-insensitive (`jobs.ashbyhq.com/Linear` → `…/job-board/linear`).
* These connectors **only enrich**: every record is `enrich_only`, so a board can never create a
  company. A company with no domain and no matching name is skipped rather than guessed at.
* The board is the company's whole open-role list, so a job that disappears from it gets
  `closed_at` — never a delete (SPEC §2, §5).

---

## `hn_hiring` — Hacker News "Who is hiring?" threads

**Official API: yes.** The [Hacker News Firebase API](https://github.com/HackerNews/API) is
public, documented and unauthenticated. Comments are public posts whose authors intend them to
be read — including the contact addresses they put in them (SPEC §6).

### Endpoints

| purpose | URL |
|---|---|
| the monthly threads | `https://hacker-news.firebaseio.com/v0/user/whoishiring.json` |
| a story or comment | `https://hacker-news.firebaseio.com/v0/item/{id}.json` |

### Terms and rate limit

* **5 requests/second**; one monthly run reads `options.max_threads` threads, newest first — 3 as
  shipped, because the run is scheduled for an hour this machine is usually asleep and a missed
  month is otherwise lost for good — and up to `options.max_comments_per_thread` (400) comments
  from each. The archive was backfilled once at 24; see the README's Contacts section.
* `robots.txt` disallows `/` but allows `/*.json$` — exactly the URLs this connector builds, and
  the shared client enforces that per RFC 9309 (a `$`-anchored `Allow` the older `urllib` parser
  would have ignored).

### Fields extracted

The convention is a pipe-separated header line, `COMPANY | ROLE | LOCATION | Full-time | REMOTE
| URL`. Fields appear in any order, so each is classified by what it looks like: a field that
resolves to a city in `config/regions.yaml` — by its configured name or by one of its `aliases` —
is the location, one the employment-type keywords recognise is the commitment, one that is a URL
is the link, and the first field is the company.
A comment with no pipes falls back to its leading proper name ("SwingVision is the AI tennis
app…"). One `Job` per comment (`external_id` = the HN item id, `posted_at` = the comment time),
plus one per `- Role: https://…` bullet, which is how multi-role comments are written. An email
the poster wrote becomes a `Contact` with `confidence='published'` — normalized (lowercased, and
nothing else) by the same `ingest/contacts.py` function `company_site` runs its `mailto:` hrefs
through, so one address cannot become two rows for the same company (SPEC §6).

### Known limitations

* Only comments naming a configured city are kept — from the header's location field where
  there is one, otherwise from anywhere in the body, and the choice is logged either way. A
  passing mention of a configured city in an out-of-region post will occasionally slip through.
* **Posters abbreviate, so every spelling the config records is looked for** — a city's canonical
  name and each of its `aliases` — and whichever matched resolves to the same configured entry,
  so the alias matches the comment while the canonical name is what is stored. It pays here more
  than anywhere else, because a comment is prose rather than a form field. Of the 273 comments in
  the September 2026 thread, 53 named a configured city and 220 were dropped for naming none; 22
  of those dropped comments write `NYC` as a whole word ("Lucia | Director of Corp Dev … | Remote
  (NYC / SEA / global overlap)") and 16 write `SF` ("Uncountable | NY, SF, London and Toronto
  (In-Person) | Full-Stack Engineering"). They were dropped for their spelling and for nothing
  else, which is the whole argument for the key — and equally the argument against inventing
  entries for it: an alias earns its line by being counted in a source, and the count sits beside
  it in `config/regions.yaml`.
* **A bare city name in free text is ambiguous twice over**, and a national city list turns both
  cases from theoretical into routine. A configured name can be a whole-word substring of a longer
  configured name (`San Francisco` inside `South San Francisco`), and a name written on its own
  carries no state, so it cannot say which metro is meant the day two regions hold the same city
  name. The connector therefore does not stop at the first city it finds: every spelling of every
  configured city occurring anywhere in the text is a candidate, ranked best-first by

  1. a mention immediately followed by its own state (`", CA"`, `", California"`) beating a bare
     one — which is what lets `Austin, TX` in the body outrank the word `Austin` in a signature;
  2. a longer *matched spelling* beating a shorter one, so `South San Francisco` is never read as
     `San Francisco`. It is the length of the string that was found, not of the city's canonical
     name: an alias may be shorter or longer than the name it stands for, and ranking on canonical
     lengths would let a city recognised by a short alias outrank one whose full name the poster
     wrote out;
  3. an earlier position beating a later one.

  A bare mention still counts, so no recall is lost. What survives all three rules is the honest
  residue: two bare, un-qualified city names in one comment, settled by length and then position.
  The winner is the *configured* city, so it brings its own state and metro with it and nothing
  is resolved from a bare name afterwards.
* **A mention carrying somebody else's state code is thrown out rather than ranked.** The mirror
  of rule 1, and the one place the state after a mention vetoes instead of promoting: a configured
  name followed by a two-letter code that is not its own state is a different place that happens
  to share the name, so it is dropped before the ranking sees it. Without that it merely failed to
  be promoted and then won as a bare match anyway — the wrong metro, which is also the wrong scope
  for SPEC §8's name matching. **The trigger is deliberately narrow, because the evidence for it
  is narrow.** Two things have to hold before a pair of letters is read as a code. Both must be
  capitalised — `", CA"` is a state, while the `", or"` of "…, or remote" and the `", we"` of
  "…, we are hiring" are English, and vetoing on those would drop the very comments this connector
  reads. And the pair must stand alone as a token: the pattern ends on `(?![\w-])` rather than
  `\b`, because a hyphen satisfies a word boundary and this thread is written in all-capitals
  vernacular, so `\b` cut `ON` out of `", ON-SITE"` and `IN` out of `", IN-PERSON"` and handed
  them to the veto as somebody else's state code — one lost record in 742 live comments,
  2026-09-08. The residual is the same words with a space in them: `", ON SITE"` still reads as a
  code, and nothing short of a list of the fifty real ones could tell it from `", ON"`. No comment
  in that sample wrote it. A code that is not a US state at all (`", UK"`) vetoes too, and should
  — it says just as plainly that the poster meant somewhere else.
* **A qualifier written out in prose can only promote, never veto**, and it is read from
  `config/regions.yaml` rather than guessed. A region (or a city overriding its state) may carry
  `state_name`, how its `state` is spelled out; `_names_state` compares the text after the comma
  against the code and that name by equality, case- and whitespace-folded. It stays promote-only
  because the file names the configured states and no others, so "not this city's state" spans
  every state the config omits, every country and every capitalised word English puts after a
  comma. What stood here before was a letter heuristic, written to keep a table of state names
  out of a connector (SPEC §11) — and it kept the table out while getting the answer wrong,
  promoting a *neighbouring* state's written-out name and filing a live comment's company under
  the wrong metro. The name belongs in the same file its code does, and that is now where it is
  read from. Omitting `state_name` costs a mention a rank and never a record, since only the
  two-letter veto above ever discards one.
* **A link is not taken as the company's domain when it points at a job board or document
  host** (`options.ignore_link_hosts`: Greenhouse, Lever, Ashby, Workable, Deel, Workday,
  Notion, Google Docs, LinkedIn, GitHub …). Keying a company on `app.deel.com` would merge every
  Deel customer into one row; such a company is resolved by name within its metro instead.
* `jobs_complete` is **false**: a comment is a slice of a company's openings, so nothing here
  ever closes a job another connector reported.
* One comment advertises several roles, so the comment body is deliberately not used to infer
  any single role's employment type — an "Intern" further down would otherwise make every role
  in the comment an internship.

---

## `funding_rss` — funding news feeds

**Official API: no; public RSS.** Ordinary syndication feeds, read as published. SPEC §4 Tier 2
is explicit that this is "a signal to *enrich* an existing record, never as a primary source",
and the connector is built that way: **every record is `enrich_only`**, so when entity resolution
finds no company the pipeline writes nothing at all rather than inventing one from a headline.

### Endpoints

Whatever is listed in `funding_rss.options.feeds` — the only place a feed URL is written.
Shipped defaults:

| feed | URL |
|---|---|
| TechCrunch Venture | `https://techcrunch.com/category/venture/feed/` |
| TechCrunch Startups | `https://techcrunch.com/category/startups/feed/` |

SPEC §4 also names Axios Pro Rata and Business Wire. **Axios** (`axios.com/pro-rata.rss`) answers
`403` to non-browser clients, and **Business Wire**'s feed ids
(`feed.businesswire.com/rss/home/?rss=<id>`) are per-*subject* rather than per-topic, so neither
is a default; either can be added to `options.feeds` without a code change.

### Terms and rate limit

**1 request/second**, `respect_robots: true` (TechCrunch's file disallows only `/wp-admin/`,
`/wp-json/`, `/search/` and query-string variants — the category feeds are allowed). Only the
public feed is read; article pages are not fetched.

### Fields extracted

The company name is whatever precedes the announcement verb in the headline ("Crusoe reportedly
raises $3B…" → "Crusoe"), the amount is the first dollar figure, the `RoundType` comes from
`config/classifiers.yaml`'s `funding.round_type` block, and `announced_date` is the item's
publication date. The whole item — title, link, summary, guid, feed URL — is kept in
`FundingRound.raw_payload`, and the feed's own item id is the round's `external_id`, so a
re-syndicated headline updates the round instead of adding a second one.

### Known limitations

* Whether a headline is a funding story at all, and which round it names, are keyword decisions
  (`config/classifiers.yaml`, `funding` block). Headlines phrased another way ("Qualcomm backs
  Ultrahuman in $70M round") are skipped and counted — missing one is far better than attaching
  a round to the wrong company.
* A headline carries no address, so SPEC §8 step 2 has no metro to scope to. Such a record is
  matched by exact normalized name across the metros in `config/regions.yaml`, and **only when
  exactly one company matches**; ambiguity resolves to nothing. Trigram similarity is never used
  here — a fuzzy cross-metro match is precisely what SPEC §8 forbids.
* The amount is read left to right, so a headline naming both a raise and a valuation
  ("raises $3B at a $30B valuation") stores the raise. Feeds that lead with the valuation would
  store that instead.
* Items older than `options.max_age_days` (30) are ignored so a reappearing item cannot re-date
  a round; `--since` overrides the window (SPEC §14.3).

---

## `product_hunt` — Product Hunt GraphQL API

_Not implemented (SPEC §4 Tier 2 #6). It has a `config/connectors.yaml` block so the scheduler
and this document have one place to look._

---

## `company_site` — direct company-site fetch (Tier 3, permission-gated)

**Official API: no.** This is the only connector that touches arbitrary third-party sites, and
it only ever visits companies **already in the database** — it cannot discover one. SPEC §4's
Tier-3 rules are all in force, all enforced by the shared client from this connector's
`config/connectors.yaml` block:

* `robots.txt` parsed and honoured **before every request** (`respect_robots: true`);
* **at most 1 request per domain per 2 seconds** (`requests_per_second: 0.5`), stretched further
  by a host's own `Crawl-delay`;
* a hard cap of **5 pages per domain per run** (`max_requests_per_host_per_run: 5`), which is
  also what the connector asks for at most (`MAX_PAGES_PER_COMPANY`): the home page plus one
  depth-1 page for each of the tier's four paths. The two numbers being equal leaves no slack
  for a same-host redirect, which the client charges to the same budget, so a redirect-heavy
  site simply loses its last page and the run records `budget_exceeded` — deliberate, because
  the alternative is exceeding the cap SPEC §4 sets;
* responses cached 24 h with `ETag`/`Last-Modified`;
* **never deeper than depth 1** — the home page, then links found *on it* whose path matches
  `options.depth_one_paths` (`careers`, `jobs`, `about`, `team`) and which stay on the same
  registrable domain. A careers page hosted on the company's ATS is that ATS's site and is not
  fetched here.

`options.max_companies_per_run` (40) bounds a whole run, and the pipeline hands targets over
least-recently-visited first, so successive Saturday runs work through the entire database
rather than re-fetching the same first forty sites.

### Fields extracted

Every value below comes off a page this connector fetched from the company's **own** domain — the
home page, plus the depth-1 `careers` / `jobs` / `about` / `team` pages it links to. That is why
every `Contact` and every `Person` written here carries `confidence='published'` (SPEC §6): the
company put the value on its own site. This connector never writes a `constructed` value; those
are built by the pipeline on *every* company upsert (see below).

| stored as | where it comes from |
|---|---|
| `Company.one_liner` | `og:description`, `<meta name="description">` or `twitter:description` on the home page, else the `<title>` with its trailing brand suffix stripped, capped at 300 characters |
| `Company.thesis` | the same description when it is longer than the one-liner cap |
| `Company.website_url` | the URL the home page's redirects settled on — usually the `www.` form of a bare seeded domain |
| `Contact` `careers_page` | the careers/jobs page the home page links to, i.e. the depth-1 page the company itself published as its jobs page |
| `Contact` `email` | every `mailto:` href on any page the visit fetched — SPEC §4 Tier 3's "any `mailto:` the company published", which is also SPEC §6's "only store addresses the company itself published". Normalized as described below and capped at `options.max_emails_per_company` |
| `Contact` `linkedin_company` | a link to `linkedin.com/company/<slug>`, canonicalised to `https://www.linkedin.com/company/<slug>` (country prefix, query, fragment, trailing slash and any `/about` tail dropped; the slug keeps its published case, because LinkedIn also serves numeric ids such as `…/company/4803356`, which is what `sourcegraph.com` links to) |
| `Contact` `x` | a link to `x.com/<handle>` or `twitter.com/<handle>`, canonicalised to `https://x.com/<handle>` — the old host redirects to the new one, so one account can never become two rows |
| `Contact` `github` | a link to `github.com/<org>`, canonicalised to `https://github.com/<org>`. `<org>/<repo>` stores the org, which is what a "we're on GitHub" link means; anything deeper is rejected, as are GitHub's own reserved and marketing paths (`/about`, `/topics`, `/features`, `/pricing` …) |
| `Contact` `contact_form` | a link whose path contains `contact` **on the company's own registrable domain**. One per company: the shallowest such path on the first page that has one, so `…/contact` wins over `…/contact/request-info` |
| `Person` | each `linkedin.com/in/<slug>` link on a team or about page: `full_name` and `title` from the text the company printed beside it, `linkedin_url` the canonicalised profile URL **as published**, `role_type` classified from that title by `config/classifiers.yaml` (`contacts.person_role_type`) and left `NULL` when no rule matches. Deduped across the visit by profile URL *and* by name, capped at `options.max_people_per_company` |

The contacts of one visit come out in a stable order — careers page, then addresses, then social
links, each in page and document order. That order is what decides which contacts survive
`options.max_emails_per_company` and the one-`contact_form`-per-company rule, so the same pages
always yield the same rows and a weekly re-run of an unchanged site adds nothing and changes no
value. It does still *write*: each contact is upserted on its `(company_id, kind, value)`, and
re-observing a published one refreshes that row's `source_id` to the run that last saw it. Only
the constructed rows below are left untouched.

An address is stored as `ingest/contacts.py` normalizes it: the `?subject=…` tail dropped, percent
escapes decoded, the first address of a comma-separated list kept, and the whole thing
**lowercased** — `mailto:HR@atom-computing.com` is real markup, and the same address seen in
lowercase elsewhere must not become a second row. A share widget's address-less `mailto:?subject=…`
yields nothing.

SPEC §4 and §6 both say *footer* social links, and the anchors are searched **anywhere on the
page** rather than inside a `<footer>` element. That is where the links are in practice, not what
identifies them: `www.astranis.com` has no `<footer>` element at all (Webflow renders the social
column as a plain `<div class="social-link footernew">`), so a footer-scoped search would find
nothing on the very page SPEC §6's "published" case comes from. The shape rules in the table above
are the first half of what keeps the wider search honest — only an account URL on one of four
hosts is accepted, in the body as in the footer.

The second half is **whose** account it is, which the shape cannot say: an "Our investors" block
links a perfectly well-formed `linkedin.com/company/<slug>` that belongs to a fund. So a footer
would have proved ownership, and without one the visit decides it, across all its pages at once
(`ingest/contacts.py:select_social_contacts`) — a home page's own link has to outweigh an
investor's on `/about`. Per kind (`linkedin_company`, `x`, `github`):

1. one candidate across the whole visit is the company's. That is the ordinary case, and it is
   what accepts `sourcegraph.com`'s numeric `…/company/4803356` and `twelve.co`'s
   `…/company/twelveco2`, neither of which its domain could have produced;
2. otherwise the first whose slug, handle or org — case and separators ignored — is the company's
   own name or domain label. That keeps `…/company/acme-robotics` and drops the
   `…/company/y-combinator` a "Backed by" block links;
3. otherwise **none** of that kind is published. Several accounts and no way to tell which is
   theirs is exactly SPEC §6's "otherwise": for `linkedin_company` the pipeline then constructs a
   link to the right company and the UI labels it a search shortcut, which beats publishing a
   confident link to the wrong one.

The limit of rule 1 is worth stating: a site whose only GitHub link cites somebody else's
repository, and which publishes no org of its own, is indistinguishable from one whose org is
named nothing like the company, so that link is stored. Only `linkedin_company` has a constructed
counterpart, so it is the one kind where getting this wrong would also cost the correct link.
Contact forms are exempt — they are already restricted to the company's own registrable domain.

Extraction reads parsed `<a href>` values only, never a regex over the raw markup: a live probe of
`twelve.co` found `github.com/wix/yoshi/issues/2689` inside a bundled script, which anchor-only
parsing correctly ignores. Two guards keep page furniture out of `Person`: a name candidate must
look like a name (2–5 tokens, no digits, two capitalised tokens, and none of `linkedin`, `visit`,
`follow`, `profile`, `team`, `leadership`), and **a name claimed by more than one profile link on
the same page is a section heading, not a person** — that is `atom-computing.com/about-us`, where
every card's anchor text is "Visit our LinkedIn" under one `Executive Leadership` heading.

### LinkedIn: links only, never a fetch

**Nothing on `linkedin.com` is ever fetched, by this connector or any other.** SPEC §4 excludes
LinkedIn scraping outright — it is prohibited by their terms and would risk the user's own
account — and SPEC §6 is the alternative: the system *stores URLs* for the user to click and
never requests one, so no profile data is fetched or stored. What this connector does is read a
LinkedIn URL a company chose to print in its own footer or on its own team page. That is a
published fact about the company, obtained from the company's server.

Two contacts are **constructed**, not observed, and they are built by `ingest/pipeline.py` from
`ingest/contacts.py` on **every company upsert** — not only for the ones this Tier-3 connector
has visited, and without any fetch at all:

* `linkedin_company` — `https://www.linkedin.com/company/{slug}`, where the slug is the
  registrable label of the company's domain (`atom-computing.com` → `atom-computing`,
  `baseten.co` → `baseten`, `foo.co.uk` → `foo`). Written only when the company has **no**
  published LinkedIn company link; a published one arriving later removes it (SPEC §6: "Otherwise
  construct …"). Both forms are canonicalised by the same function, so when the published slug
  equals the constructed one the row is promoted from `constructed` to `published` in place. A
  company with no domain — an EDGAR-only row, say — gets none: a slug guessed from nothing is a
  link to a stranger.
* `linkedin_people` — the "Find people →" deep link of SPEC §6, a people search for the company
  name plus the title keywords in `config/classifiers.yaml` (`contacts.people_search_terms`:
  `founder OR recruiter OR "head of engineering" OR "talent"`).

Both carry `confidence='constructed'` and no `source_id` — nothing was fetched, so there is no
`Source` row — and the UI labels them as search shortcuts rather than addresses (SPEC §6, §9).
Because a constructed value is derived from the company's current name and domain, a stale one
(the domain changed, so the shortcut now points at somebody else's company) is replaced rather
than kept; that is not the "never delete on refresh" rule of SPEC §2/§5, which is about *observed*
data such as a job that disappeared from its board.

Construction is part of the upsert, not something the ingest run sweeps for. Every row in
`companies` is written by that upsert, so a database built under Phase 5 carries whichever of the
two links the row can have, everywhere; a database migrated from an earlier phase gains them
company by company, as each is next upserted. The remainder is what `python cli.py sync-contacts`
is for: it runs the same reconciliation over every company in the table, writes nothing else, and
reports `added=0 removed=0` when there was nothing to do. Run it once after upgrading a database —
otherwise a company no connector re-lists never gains its links at all, and a row with neither a
`website_url` nor a `domain` (every EDGAR-only company, because Form D carries no website or
domain) is one `company_site` skips on every run for ever. A domain alone is enough to be
*visited* — `CompanyTarget.home_url` falls back to `https://{domain}/` — but not to be upserted:
a site that declines or never answers yields no record, so those rows need the sweep too.

`sync-contacts` fetches nothing, so it writes no `fetch_runs` row, and it touches only
`confidence='constructed'` contacts — a published address is never altered. `--dry-run` does the
whole sweep and discards it, so its counts are what a real run would change rather than an
estimate.

### Known limitations and deliberate omissions

* A site that declines — robots, a WAF `403`, a redirect loop — is skipped quietly. SPEC §4 says
  a Tier-3 site may simply say no, and one company's Cloudflare must not colour the run.
* Every record is `enrich_only`: a redirect that lands somewhere unexpected must never create a
  company.
* **No address is ever guessed or verified** (SPEC §4 "Excluded", SPEC §6): there is no
  `firstname@company.com` pattern, no MX lookup and no SMTP probe anywhere in the codebase. An
  address is stored only because a company linked it, here or in an HN hiring comment
  (`hn_hiring`, which normalizes addresses through the same function so the two sources cannot
  write one address twice in different cases).
* Contacts are only as complete as the five pages a visit fetches — the home page plus one page
  per SPEC §4 Tier 3 section (`/careers`, `/jobs`, `/about`, `/team`). A company whose social
  links live on a `/contact` page two hops down, or whose footer is rendered in JavaScript,
  yields none — and gets the constructed LinkedIn shortcuts like everyone else.
* A social account is published only when the visit can tell it is the company's own (below).
  A site that links no account of its own, and one third party's, publishes none of that kind.
* A person is only found where the company published a LinkedIn profile link next to a name.
  Team pages that link nothing, or that render their cards client-side, produce no `Person` rows;
  `sec_edgar` supplies officers from public filings instead.

---

## `opencorporates` — OpenCorporates API (on demand)

_Not implemented (SPEC §4 Tier 2 #7). Enrichment, not discovery: `cadence: null`._

---

## `seed` — the bootstrap loader (SPEC §10)

Not a data source but a connector, so it gets the same `FetchRun` bookkeeping and upsert path.
It reads `config/seed_companies.yaml` and, for each entry, sends **one `HEAD` request to
`https://{domain}/`, following redirects** (`requests_per_second: 0.5`, `respect_robots: true`;
a server that refuses `HEAD` with 405/501 is retried once with `GET`). It is deliberately
**absent from the connector registry**, so `refresh --all` never re-validates the bootstrap
list; `python cli.py seed` runs it.

What the validation concludes:

| outcome | company status |
|---|---|
| any answer in 2xx/3xx | unchanged — the loader writes no status at all |
| `404` / `410` | `dead` |
| host unreachable (DNS, connection, TLS, timeout after retries) | `dead`, with the reason in `FetchRun.error_text` |
| `403`, `429`, `5xx` — a server *answered* | unchanged; a bot-hostile WAF is a fact about us, not about the company (`atom-computing.com` answers 403 to this User-Agent and is very much alive) |
| `robots.txt` disallows the probe | unchanged, and the skip is reported |

A dead entry is still stored: the database is the historical record (SPEC §2). The final URL
becomes `website_url`; the canonical `domain` stays the one the file names even when a redirect
crossed to another host, so one typo can never swallow another company's row. **No entry may
carry an ATS token** — the model forbids the field, and the ATS connectors discover them
(SPEC §4, §10). `seed` is unlisted in the connector priority order and therefore ranks below
every real connector, so a bootstrap value is replaced by the first source that reports it for
real (SPEC §8).
