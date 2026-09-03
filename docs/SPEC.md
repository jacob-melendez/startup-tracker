# Bay Area Startup Tracker — Build Specification

**Purpose of this document:** a complete brief to hand to Claude Code. Drop it in the repo as `docs/SPEC.md`, then work through the phased prompts in Section 12.

**Context:** built to find part-time and flexible roles at Bay Area startups during the academic year, across all functions. Future goal: expand to other US startup hubs and serve other students.

**Priority:** a clean, modern, fully-working implementation. Use whatever standard tooling makes that fastest and most maintainable — ORM, migrations framework, task scheduler, containers. Do not hand-roll infrastructure for its own sake.

---

## 1. Project summary

A locally-run web app that maintains a daily-refreshed database of Bay Area startups and their open roles, and lets the user browse, search, filter, and track them.

**Region scope for v1:** San Francisco, Palo Alto, Mountain View, Menlo Park, Redwood City, Santa Clara, Sunnyvale, San Mateo, San Jose, Berkeley, Oakland, Cupertino, Los Altos, Foster City, South San Francisco, Burlingame, Fremont, Emeryville.

**Explicit non-goals for v1:** user accounts, multi-tenancy, public deployment, mobile app, email notifications, any paid data API.

---

## 2. Architectural decision: batch ingestion, not real-time scraping

All external data is fetched by **scheduled jobs** that write into Postgres. The web app **only ever reads from the database** — it never makes an outbound network call during a page render.

Rationale:
1. Pages render in milliseconds instead of seconds.
2. Sources rate-limit and block. Batching with backoff and caching is the only sustainable pattern.
3. It creates a historical record — when a company first appeared, when a round was announced, when a role was posted and pulled.

A `--now` flag on the CLI allows an on-demand refresh of a single connector for testing.

---

## 3. Tech stack

| Layer | Choice | Notes |
|---|---|---|
| Language | Python 3.12 | |
| Package manager | `uv` | Lockfile committed |
| Database | **PostgreSQL 16** | Run via Docker Compose. Native FTS (`tsvector` + GIN), `pg_trgm` for fuzzy matching, JSONB for raw payloads, enums for controlled vocabularies |
| ORM | **SQLAlchemy 2.0**, async, typed declarative models | `asyncpg` driver |
| Migrations | **Alembic**, autogenerate + reviewed by hand | |
| Web framework | FastAPI | |
| Templating | Jinja2 + **HTMX** | Server-rendered. HTMX gives live filtering and expandable rows with no build step and no JS framework |
| CSS | One hand-written `styles.css`, ~200 lines, system font stack | No Tailwind, no component library, no build pipeline |
| Validation / settings | Pydantic v2 + `pydantic-settings` | |
| HTTP client | `httpx` (async) with `tenacity` for retries | |
| HTML parsing | `selectolax` | |
| Scheduling | APScheduler with **per-connector cron triggers** | See §7 |
| Logging | `structlog`, JSON output | |
| Lint / types | `ruff` + `mypy --strict` on `ingest/` and `db/` | |
| Testing | `pytest`, `pytest-asyncio`, `respx` (httpx mocking), `testcontainers` or a disposable Compose DB | |
| Containers | Docker Compose: `db`, `app`, `scheduler` | `make up` should be the only setup step |

**ORM usage guidance:** use SQLAlchemy models and `select()` constructs for ordinary reads and writes. Where a query is genuinely complex — the company list query in §9, ranking, window functions for latest-round-per-company — use SQLAlchemy Core or a hand-written statement rather than contorting the ORM. Keep those in `db/queries.py` with a docstring explaining the shape.

---

## 4. Data sources

The hardest part of the project and where naive implementations fail. Ranked by reliability.

### Tier 1 — Documented public APIs (build these first)

**1. SEC EDGAR — Form D filings**
- Companies raising private capital file Form D. Free, official, documented.
- Yields: legal name, **street address (→ city/state, powering the region filter)**, industry classification, total offering amount, amount sold, date of first sale, and named executives/directors (→ people records).
- Base: `https://data.sec.gov/`. Full-text search: `https://efts.sec.gov/LATEST/search-index?q=...&forms=D`.
- **Required:** a descriptive `User-Agent` header containing a contact email, per SEC's fair-access policy. Rate limit: 10 requests/second maximum.
- The best free funding-round source that exists.

**2. Y Combinator company directory**
- YC publishes a public company index (Algolia-backed, used by their own site). Heavy Bay Area concentration; includes one-liner, batch, sector tags, website, team size.
- Also ingest **Work at a Startup** listings where accessible.

**3. Applicant tracking system job-board APIs** — the highest-value source for the actual goal
Public, documented, JSON, intended for consumption. Given a board token you get every open role at that company:
- Greenhouse: `https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true`
- Lever: `https://api.lever.co/v0/postings/{company}?mode=json`
- Ashby: `https://api.ashbyhq.com/posting-api/job-board/{name}?includeCompensation=true`
- Workable: `https://apply.workable.com/api/v1/widget/accounts/{account}?details=true`

Board tokens are discovered by fetching a company's `/careers` page and regex-matching the embedded board URL. Persist the discovered token on the company row so discovery runs once, and re-run discovery only if the board 404s.

**4. Hacker News "Who is hiring?" monthly threads**
- Official Firebase API: `https://hacker-news.firebaseio.com/v0/`. Locate the monthly thread from the `whoishiring` user's submissions, then walk top-level comments. Startups post directly, often list part-time/contract roles, and frequently include a contact email they intend to be public.

### Tier 2 — Structured feeds

**5. Funding news RSS** — TechCrunch, Axios Pro Rata, Business Wire funding feeds. Parse for company + round + amount. Use as a *signal to enrich* an existing record, never as a primary source.

**6. Product Hunt GraphQL API** — free developer tier; surfaces very early companies.

**7. OpenCorporates API** — registry data, incorporation date, jurisdiction.

### Tier 3 — Direct company site fetch (permission-gated)

For companies already in the DB, fetch their own site to enrich: `<meta name="description">` / `og:description` → one-liner fallback; `/careers`, `/jobs`, `/about`, `/team`; any `mailto:` the company published; footer social links.

**Rules for this tier:** parse and honor `robots.txt` before every fetch; max 1 request per domain per 2 seconds; hard cap of 5 pages per domain per run; cache with ETag/Last-Modified for 24h; never follow deeper than depth 1 from the homepage.

### Excluded — do not build these

- **LinkedIn scraping.** Prohibited by their terms and defended with aggressive bot detection; attempting it risks restriction of the user's personal account. See §6 for the alternative.
- **Crunchbase scraping.** Bot-protected; the data is the paid product.
- **Wellfound / AngelList scraping.** Bot-protected.
- **Email pattern-guessing** (generating `firstname@company.com` and SMTP-verifying). Grey-area, low yield, gets the sending domain flagged.
- **Any paid data API** in v1.

Every connector must be documented in `docs/SOURCES.md`: endpoint, whether it's an official API, applicable terms, rate limit applied, fields extracted.

---

## 5. Data model

SQLAlchemy 2.0 typed declarative models; Alembic owns the DDL. Postgres enums for controlled vocabularies.

### Core

**`Company`** — canonical key is normalized `domain`.
`id`, `name`, `normalized_name`, `domain` (unique), `website_url`, `one_liner`, `thesis`, `founded_year`, `employee_est`, `stage` (enum), `status` (enum: active/acquired/dead), `ats_provider` (enum, nullable), `ats_token`, `latest_round_id` (FK, denormalized), `latest_job_posted_at` (denormalized — powers the default sort), `open_job_count` (denormalized), `first_seen_at`, `last_seen_at`.

Denormalized columns are maintained by the ingest pipeline at end-of-run, not by triggers. Document that choice.

**`Location`** — `id`, `city`, `state`, `metro` (`'Bay Area'` for v1 — the expansion hook), `country`. Unique on `(city, state)`.

**`CompanyLocation`** — M:N join, with `is_hq` flag.

**`Sector`** — `id`, `name`, `slug`. **`CompanySector`** — M:N join.

**`FundingRound`** — `id`, `company_id`, `round_type` (enum), `amount_usd` (bigint, nullable = undisclosed), `announced_date`, `source_id`, `raw_payload` (JSONB), `notes`.

**`Investor`** + **`RoundInvestor`** (M:N, with `is_lead`).

**`Person`** — `id`, `company_id`, `full_name`, `title`, `role_type` (enum: founder/exec/recruiter/eng_lead), `linkedin_url`, `source_id`.

**`Contact`** — `id`, `company_id`, `kind` (enum: email / linkedin_company / careers_page / x / github / contact_form), `value`, `confidence` (enum: **published** | **constructed** — see §6), `source_id`. Unique on `(company_id, kind, value)`.

**`Job`** — `id`, `company_id`, `external_id`, `title`, `url`, `location_text`, `is_remote`, `employment_type` (enum), `role_family` (enum, see §7), `seniority` (enum, see §7), `compensation_raw`, `posted_at`, `first_seen_at`, `last_seen_at`, `closed_at` (set when it disappears from the source), `description_raw`, `raw_payload` (JSONB), `source_id`. Unique on `(company_id, external_id)`.

### Operational

**`Source`** — `id`, `connector`, `url`, `fetched_at`.
**`CompanySource`** — provenance: `company_id`, `connector`, `external_id`, `last_seen_at`. PK `(company_id, connector)`.
**`FetchRun`** — `id`, `connector`, `started_at`, `finished_at`, `status` (ok/partial/error), `n_fetched`, `n_upserted`, `error_text`.
**`MergeCandidate`** — `id`, `company_id_a`, `company_id_b`, `similarity`, `reason`, `resolved_at`. See §8.

### Personal tracking layer

**`UserNote`** — `company_id` (unique), `status` (enum: none/interested/applied/in_process/rejected/offer), `rating` 1–5, `note`, `updated_at`.
**`JobBookmark`** — `job_id` (unique), `starred`, `note`, `created_at`.
**`SavedSearch`** — `id`, `name` (unique), `query_json` (JSONB), `created_at`.

### Search indexing

A generated `tsvector` column on `Company` over `name || one_liner || thesis`, with a GIN index. A second on `Job` over `title || description_raw`. Enable `pg_trgm` and add trigram indexes on `companies.normalized_name` and `companies.name` for fuzzy entity resolution (§8) and typo-tolerant search.

**Other indexes:** `companies(domain)`, `companies(stage)`, `companies(latest_job_posted_at DESC NULLS LAST)`, `jobs(company_id)`, `jobs(role_family, closed_at)`, `jobs(employment_type, closed_at)`, `jobs(posted_at DESC)`, `funding_rounds(company_id, announced_date DESC)`, `company_locations(location_id)`.

---

## 6. Contact handling — the LinkedIn approach

Rather than scraping LinkedIn, the system **constructs deterministic LinkedIn URLs** for the user to click:

- **Company page:** if the company's own website footer links to LinkedIn, store that URL with `confidence='published'`. Otherwise construct `https://www.linkedin.com/company/{slug}` from the domain and store with `confidence='constructed'`.
- **People search:** store a people-search deep link filtered to the company name plus title keywords (`founder OR recruiter OR "head of engineering" OR "talent"`). The UI renders it as a **"Find people →"** button. No profile data is ever fetched or stored.

**Emails:** only store addresses the company itself published — a `mailto:` on their own site, a jobs alias on their careers page, an address a founder posted in an HN hiring comment. Mark `confidence='published'`. Never guess, never SMTP-verify.

The UI must visually distinguish `published` from `constructed` so it's obvious which is a real address and which is a search shortcut.

---

## 7. Classification and refresh cadence

### 7.1 Role taxonomy — all functions, none filtered out by default

**Every role is ingested and displayed.** Classification exists to power filters, never to exclude. Any role that doesn't match a rule gets `role_family='other'` and still appears in the list.

`role_family` enum:

| Value | Matches on |
|---|---|
| `software` | engineer, developer, SWE, backend, frontend, full-stack, mobile, iOS, Android |
| `infrastructure` | infrastructure, platform, DevOps, SRE, reliability, cloud |
| `ml_ai` | machine learning, ML engineer, AI engineer, applied scientist, LLM, NLP, computer vision |
| `data` | data scientist, data engineer, analytics, analyst, BI |
| `hardware` | hardware, electrical, EE, firmware, embedded, ASIC, FPGA, RF, PCB, mechanical, manufacturing, test engineer, optics |
| `robotics` | robotics, autonomy, controls, perception, motion planning |
| `research` | research scientist, research engineer, PhD, postdoc |
| `product` | product manager, product owner, APM, technical product, product analyst |
| `design` | designer, UX, UI, product design, brand, motion, industrial design |
| `security` | security, appsec, infosec, compliance engineer, trust and safety |
| `qa` | QA, test, quality, SDET |
| `sales` | sales, account executive, AE, SDR, BDR, partnerships, solutions engineer |
| `marketing` | marketing, growth, demand gen, content, SEO, community, developer relations, DevRel |
| `bizops` | business operations, strategy, chief of staff, BizOps, corporate development |
| `finance` | finance, accounting, controller, FP&A, treasury |
| `people` | recruiting, talent, HR, people ops |
| `operations` | operations, supply chain, logistics, customer success, support, program manager |
| `legal` | legal, counsel, paralegal, policy |
| `other` | fallback |

`employment_type` enum: `full_time`, `part_time`, `contract`, `internship`, `co_op`, `temporary`, `unknown`. Derived from the ATS field where present; otherwise keyword matching (`part-time`, `part time`, `contract`, `contractor`, `intern`, `co-op`, `fractional`, `hourly`, `10-20 hours`).

`seniority` enum: `intern`, `new_grad`, `junior`, `mid`, `senior`, `staff`, `principal`, `lead`, `manager`, `director`, `executive`, `unknown`.

A separate boolean **`flexible_signal`** flags roles whose text suggests student compatibility (`students welcome`, `flexible hours`, `during the school year`, `part-time OK`, `20 hrs/week`, `remote-friendly`). This is a **surfacing hint shown as a badge and available as a filter — it is never a default filter and never hides anything.**

All keyword rules live in `config/classifiers.yaml`, editable without code changes. Log the matched keyword on each classification so false positives can be traced.

### 7.2 Per-connector refresh cadence

Defined in `config/connectors.yaml` as cron expressions, one per connector, loaded by APScheduler at startup:

| Connector | Cadence | Reason |
|---|---|---|
| `greenhouse`, `lever`, `ashby`, `workable` | Daily, 06:00 | Job postings turn over fast; this is the freshness that matters |
| `hn_hiring` | Monthly, 2nd of the month, 09:00 | The thread posts on the 1st |
| `sec_edgar` | Every 3 days, 05:00 | Form D filings trickle in; no value in polling daily |
| `ycombinator` | Weekly, Sunday 04:00 | Directory changes slowly outside batch announcements |
| `funding_rss` | Daily, 07:00 | Cheap, and the signal is time-sensitive |
| `product_hunt` | Weekly, Sunday 05:00 | |
| `company_site` | Weekly, Saturday 03:00, and on-demand for newly added companies | Politeness — this is the only tier hitting arbitrary sites |
| `opencorporates` | On-demand only | Enrichment, not discovery |

Every run writes a `FetchRun` row regardless of outcome. A connector that fails 3 consecutive runs should log at ERROR and surface prominently on `/runs`.

---

## 8. Entity resolution

Companies arrive from multiple connectors under different names ("Stripe, Inc.", "Stripe", "stripe"). Resolution order:

1. **Exact match on normalized domain** — the primary key. Normalize by stripping scheme, `www.`, path, query, and lowercasing.
2. If no domain, **exact match on `normalized_name`** (lowercase; strip `inc|llc|corp|co|ltd|technologies|labs|holdings`; collapse whitespace and punctuation) **scoped to the same metro**.
3. If still no match, **trigram similarity** via `pg_trgm` (`similarity(normalized_name, ?) > 0.85`) scoped to the same metro. **Do not auto-merge.** Write a `MergeCandidate` row for review via `cli merge-review`.

All writes are **upserts** (`INSERT ... ON CONFLICT (domain) DO UPDATE`) that refresh `last_seen_at` and overwrite a field only when the incoming value is non-null and its connector outranks the connector that wrote the existing value.

**Connector priority:** `sec_edgar` > `ycombinator` > ATS APIs > `company_site` > `funding_rss` > `product_hunt`.

Store the writing connector per field group so priority can be evaluated — a small `field_provenance` JSONB column on `Company` mapping field name → connector is sufficient.

---

## 9. Web interface

Server-rendered, HTMX-enhanced, four pages. Deliberately plain styling.

### `/` — company list (primary view)

- **Search bar** querying the company `tsvector`, with trigram fallback for typos.
- **Filter sidebar or filter row** — all combinable, composed server-side into one parameterized query, submitted via HTMX so results update without a full page load:
  - City (multi-select)
  - Sector (multi-select)
  - Stage / latest round type
  - Last round amount (range)
  - Last round recency (within N months)
  - **Role family (multi-select, all 19 values)** — matches companies having ≥1 open role in those families
  - Employment type (multi-select)
  - Seniority (multi-select)
  - `flexible_signal` only (off by default)
  - Has open roles (boolean)
  - Tracking status
- **Default sort: `latest_job_posted_at DESC NULLS LAST`** — companies that posted most recently float to the top. Other sort options: last funding date, amount raised, recently added, open role count, name.
- **Results:** one collapsed row per company showing name, HQ city, sector chips, latest round + date, open role count, and a compact breakdown of which role families are open. Expanding the row (HTMX `hx-get` into a `<details>`) loads:
  - Thesis / description
  - All locations
  - Funding history table (round, amount, date, investors)
  - **Full open-roles table** — title, role family, employment type, seniority, location, posted date, link — sortable and filterable within the row, showing *all* roles regardless of the page-level role filter, with matching ones highlighted
  - Contacts block — published emails, careers page, LinkedIn company link, "Find people →", with published/constructed clearly labeled
  - Provenance line: which connectors saw this and when
  - Inline note + status + rating control (HTMX POST to `/company/{id}/note`)
- Pagination, 50 per page, keyset-based on the sort column.

### `/roles` — flat job list (secondary view)

Same filter set, but one row per job across all companies, default sorted by `posted_at DESC`. This is the "what opened this week" view. Bookmark/star toggle per row.

### `/company/{id}` — permalink detail page

Standalone, linkable version of the expanded row.

### `/runs` — ingestion health

Last 100 `FetchRun` rows: connector, timestamps, status, counts, error text. Highlight any connector with no successful run in over 2× its cadence. Makes silent breakage visible.

---

## 10. Seed list

Cold-starting from EDGAR alone is slow and the ATS connectors have nothing to discover tokens from. Bootstrap with `config/seed_companies.yaml`.

**Loader requirements:** on load, resolve each domain (HEAD request, follow redirects); mark unreachable entries `status='dead'` and log rather than failing the run; do not hardcode ATS tokens — let the discovery step in the Greenhouse/Lever/Ashby connectors find them. Some entries below may have been acquired or wound down since this list was compiled, which the validation step will catch.

```yaml
# config/seed_companies.yaml — Bay Area seed set
# fields: name, domain, city, sector_hint

ai_and_ml:
  - {name: OpenAI,            domain: openai.com,        city: San Francisco}
  - {name: Anthropic,         domain: anthropic.com,     city: San Francisco}
  - {name: Scale AI,          domain: scale.com,         city: San Francisco}
  - {name: Perplexity,        domain: perplexity.ai,     city: San Francisco}
  - {name: Sierra,            domain: sierra.ai,         city: San Francisco}
  - {name: Harvey,            domain: harvey.ai,         city: San Francisco}
  - {name: Anysphere (Cursor),domain: cursor.com,        city: San Francisco}
  - {name: Glean,             domain: glean.com,         city: Palo Alto}
  - {name: Together AI,       domain: together.ai,       city: San Francisco}
  - {name: Fireworks AI,      domain: fireworks.ai,      city: Redwood City}
  - {name: Baseten,           domain: baseten.co,        city: San Francisco}
  - {name: Modal Labs,        domain: modal.com,         city: San Francisco}
  - {name: Decagon,           domain: decagon.ai,        city: San Francisco}
  - {name: Writer,            domain: writer.com,        city: San Francisco}

dev_tools_and_infra:
  - {name: Databricks,        domain: databricks.com,    city: San Francisco}
  - {name: Vercel,            domain: vercel.com,        city: San Francisco}
  - {name: Sourcegraph,       domain: sourcegraph.com,   city: San Francisco}
  - {name: Sentry,            domain: sentry.io,         city: San Francisco}
  - {name: PostHog,           domain: posthog.com,       city: San Francisco}
  - {name: Warp,              domain: warp.dev,          city: San Francisco}
  - {name: Replit,            domain: replit.com,        city: Foster City}
  - {name: Retool,            domain: retool.com,        city: San Francisco}
  - {name: Linear,            domain: linear.app,        city: San Francisco}

fintech:
  - {name: Stripe,            domain: stripe.com,        city: South San Francisco}
  - {name: Plaid,             domain: plaid.com,         city: San Francisco}
  - {name: Brex,              domain: brex.com,          city: San Francisco}
  - {name: Mercury,           domain: mercury.com,       city: San Francisco}
  - {name: Chime,             domain: chime.com,         city: San Francisco}
  - {name: Gusto,             domain: gusto.com,         city: San Francisco}
  - {name: Pave,              domain: pave.com,          city: San Francisco}

saas_and_work_tools:
  - {name: Figma,             domain: figma.com,         city: San Francisco}
  - {name: Notion,            domain: notion.so,         city: San Francisco}
  - {name: Airtable,          domain: airtable.com,      city: San Francisco}
  - {name: Rippling,          domain: rippling.com,      city: San Francisco}
  - {name: Deel,              domain: deel.com,          city: San Francisco}
  - {name: Ironclad,          domain: ironcladapp.com,   city: San Francisco}
  - {name: Ashby,             domain: ashbyhq.com,       city: San Francisco}

security_and_compliance:
  - {name: Vanta,             domain: vanta.com,         city: San Francisco}
  - {name: Persona,           domain: withpersona.com,   city: San Francisco}
  - {name: Checkr,            domain: checkr.com,        city: San Francisco}
  - {name: Verkada,           domain: verkada.com,       city: San Mateo}

hardware_semis_robotics:
  - {name: Cerebras Systems,  domain: cerebras.ai,       city: Sunnyvale}
  - {name: Groq,              domain: groq.com,          city: Mountain View}
  - {name: SambaNova Systems, domain: sambanova.ai,      city: Palo Alto}
  - {name: Zoox,              domain: zoox.com,          city: Foster City}
  - {name: Nuro,              domain: nuro.ai,           city: Mountain View}
  - {name: Skydio,            domain: skydio.com,        city: San Mateo}
  - {name: Applied Intuition, domain: appliedintuition.com, city: Mountain View}
  - {name: Astranis,          domain: astranis.com,      city: San Francisco}

quantum_and_deep_tech:
  - {name: PsiQuantum,        domain: psiquantum.com,    city: Palo Alto}
  - {name: SandboxAQ,         domain: sandboxaq.com,     city: Palo Alto}
  - {name: Atom Computing,    domain: atom-computing.com,city: Berkeley}

health:
  - {name: Hinge Health,      domain: hingehealth.com,   city: San Francisco}
  - {name: Carbon Health,     domain: carbonhealth.com,  city: San Francisco}
  - {name: Included Health,   domain: includedhealth.com,city: San Francisco}
  - {name: Color Health,      domain: color.com,         city: San Francisco}

climate:
  - {name: Watershed,         domain: watershed.com,     city: San Francisco}
  - {name: Twelve,            domain: twelve.co,         city: Berkeley}
```

The seed list is a bootstrap, not a target. Within a few refresh cycles the EDGAR and YC connectors should be contributing far more companies than this.

---

## 11. Repository structure

```
startup-tracker/
├── CLAUDE.md
├── README.md
├── Makefile                   # up, down, migrate, seed, refresh, test, lint
├── docker-compose.yml         # db, app, scheduler
├── pyproject.toml
├── .env.example
├── alembic.ini
├── migrations/                # Alembic versions/
├── config/
│   ├── regions.yaml           # Bay Area city list — the expansion hook
│   ├── connectors.yaml        # per-connector cron cadence + rate limits
│   ├── classifiers.yaml       # role_family / employment_type / seniority rules
│   └── seed_companies.yaml
├── db/
│   ├── models.py              # SQLAlchemy 2.0 typed declarative models
│   ├── session.py             # async engine, session factory
│   ├── queries.py             # the few complex Core/raw statements
│   └── enums.py
├── ingest/
│   ├── base.py                # Connector ABC
│   ├── http.py                # shared client: retries, backoff, robots.txt, cache, UA
│   ├── normalize.py           # domain/name normalization, entity resolution
│   ├── classify.py            # role_family / employment_type / seniority / flexible_signal
│   ├── pipeline.py            # run orchestration, upserts, denorm refresh, FetchRun
│   └── connectors/
│       ├── sec_edgar.py
│       ├── ycombinator.py
│       ├── greenhouse.py
│       ├── lever.py
│       ├── ashby.py
│       ├── workable.py
│       ├── hn_hiring.py
│       ├── funding_rss.py
│       └── company_site.py
├── web/
│   ├── app.py
│   ├── routes/
│   ├── templates/             # includes HTMX partials
│   └── static/styles.css
├── cli.py                     # typer: migrate, seed, refresh, stats, merge-review
├── scheduler.py
├── tests/
│   └── fixtures/              # recorded HTTP responses
└── docs/
    ├── SPEC.md                # this file
    ├── SOURCES.md
    └── ARCHITECTURE.md
```

---

## 12. Phased build plan and prompts for Claude Code

Build in this order. Each phase ends with a working, committed, testable state. **One phase per prompt** — the failure mode on a project this size is a large unreviewable diff that half-works.

### 12.0 — First, create `CLAUDE.md`

> Read `docs/SPEC.md` in full. Then write a `CLAUDE.md` at the repo root capturing the non-negotiable conventions: SQLAlchemy 2.0 async typed models with Alembic migrations, Postgres 16 only (no SQLite fallback), batch ingestion only (no outbound network calls in request handlers), server-rendered Jinja + HTMX with no JS framework or build step, all classification rules driven by `config/classifiers.yaml`, all connector cadence driven by `config/connectors.yaml`, and the excluded data sources listed in §4. Also record the rule that classification never excludes a role from the database or the default view. Keep it under 80 lines. Write no other code yet.

### Phase 1 — Foundation

> Set up the project skeleton: `pyproject.toml` with `uv`, `docker-compose.yml` with Postgres 16 and the app service, a `Makefile` with `up/down/migrate/seed/refresh/test/lint`, `.env.example`, structlog config, and ruff/mypy config. Implement `db/models.py` with every model in §5 of the spec including enums, the generated tsvector columns, and all indexes listed. Enable the `pg_trgm` extension in the first Alembic migration. Wire up `db/session.py` with an async engine and session factory. Generate the initial Alembic migration and review it by hand. Write pytest tests that spin up a disposable Postgres, run the migration, and assert every table, enum, index, and extension exists. Build nothing else yet.

### Phase 2 — Ingestion framework + first connector

> Implement `ingest/base.py` (a `Connector` ABC with `name`, `cadence`, `fetch()`, `to_records()`), `ingest/http.py` (async httpx client with per-domain rate limiting, tenacity retries with exponential backoff, `robots.txt` parsing and enforcement, ETag/Last-Modified caching, configurable User-Agent including a contact email), `ingest/normalize.py` (domain and name normalization plus the three-step entity resolution from §8, writing low-confidence pairs to `MergeCandidate`), and `ingest/pipeline.py` (orchestration, priority-aware upserts using `field_provenance`, denormalized-column refresh at end of run, `FetchRun` bookkeeping). Then implement exactly one connector: `sec_edgar.py`, pulling Form D filings filtered to the cities in `config/regions.yaml`. Add `cli.py refresh --connector sec_edgar`. Tests use recorded fixtures via respx — never hit the live API in tests.

### Phase 3 — Seed loader and remaining connectors

> Implement the seed loader per §10, including domain validation. Then add connectors in this order: `ycombinator.py`, the ATS connectors (`greenhouse.py`, `lever.py`, `ashby.py`, `workable.py`) including careers-page board-token discovery, `hn_hiring.py`, `funding_rss.py`, `company_site.py`. Each subclasses `Connector`, respects the shared HTTP rules, and has fixture-based tests. Implement `ingest/classify.py` per §7.1 driven by `config/classifiers.yaml`, covering all 19 role families, the employment-type and seniority enums, and the `flexible_signal` boolean — and confirm in tests that an unmatched title lands in `other` and is still persisted. Add `cli.py refresh --all`. Write `docs/SOURCES.md`.

### Phase 4 — Web interface

> Build the FastAPI app and templates for `/`, `/roles`, `/company/{id}`, and `/runs` per §9. The company list's filters must compose into a single parameterized query with keyset pagination — put it in `db/queries.py` with a docstring explaining the composition strategy. Default sort is `latest_job_posted_at DESC NULLS LAST`. Use HTMX for filter submission and for lazy-loading the expanded company row; the only bespoke JavaScript permitted is small glue. Write `styles.css` from scratch, under 200 lines, system font stack, no framework. Implement the note/status/rating form and the job bookmark toggle. Verify that with no filters applied, every open role of every role family is reachable from the UI.

### Phase 5 — Contacts and LinkedIn links

> Implement §6. Add contact extraction to `company_site.py` (published `mailto:` links, careers page URL, footer social links) and the LinkedIn URL construction logic. Populate `Contact` and `Person` with correct `confidence` values. Update the company detail template to visually separate published contacts from constructed search links, with a one-line explanation of the difference.

### Phase 6 — Scheduling and operations

> Implement `scheduler.py` with APScheduler reading per-connector cron triggers from `config/connectors.yaml` per §7.2, running as its own Compose service. Add consecutive-failure detection and surface it on `/runs`. Add `cli.py stats` (row counts, last successful run per connector, companies and jobs added in the last 7 days) and `cli.py merge-review`. Write the README with setup, first-run instructions, and troubleshooting for common connector failures. Write `docs/ARCHITECTURE.md` covering the ingestion pipeline, entity resolution, and the denormalization strategy.

### Phase 7 — Multi-region expansion (later)

> Generalize `config/regions.yaml` to multiple metros, add a region selector to the UI, and verify no Bay-Area-specific logic exists outside that config file.

---

## 13. Acceptance criteria

- [ ] `make up && make migrate && make seed` produces a working app from a clean checkout.
- [ ] `cli.py refresh --all` completes with `status='ok'` for every connector and populates ≥ 300 Bay Area companies.
- [ ] All 19 role families are represented in the DB, and no role is dropped at ingest for failing to classify.
- [ ] With no filters applied, `/roles` shows every open job of every type.
- [ ] No web request handler makes an outbound HTTP call.
- [ ] `robots.txt` is checked before every Tier-3 fetch, covered by a test.
- [ ] No connector fetches from LinkedIn, Crunchbase, or Wellfound.
- [ ] The company list renders in under 200ms with 5,000 companies and 20,000 jobs.
- [ ] Default sort is most-recent-job-posting, verified by a test.
- [ ] Every filter combination produces correct results, covered by tests.
- [ ] `mypy --strict` passes on `ingest/` and `db/`; `ruff` is clean.
- [ ] `docs/SOURCES.md` documents every connector's legal basis and rate limit.

---

## 14. Settled decisions

1. **Per-connector refresh cadence** — implemented, §7.2.
2. **Seed list** — 55 Bay Area companies, §10, with runtime domain validation.
3. **EDGAR historical depth** — **18 months on first backfill**, then daily incremental. Reasoning: recency is the better signal for hiring capacity, but an 18-month window still catches a company that raised a Series A last year and is only now scaling headcount, and it gives enough history for the "last round recency" filter to be meaningful. A 6-month window would have produced a thin database on day one. Configurable via `--since` on the CLI, and the UI's funding-recency filter lets recency be applied at query time rather than baked into ingestion.
4. **Companies-first, sorted by most recent job posting** — implemented, §9, with `/roles` as a secondary flat job view.
