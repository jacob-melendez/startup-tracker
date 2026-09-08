# CLAUDE.md — Startup Tracker

Locally-run web app: scheduled connectors fill a Postgres database of startups and their open roles
across the US metros `config/regions.yaml` lists — the Bay Area and five more since §12 Phase 7 — and
a server-rendered UI browses, filters, and tracks them. `docs/SPEC.md` is the source of truth and
every rule below cites it. Build one §12 phase per prompt, each ending in a working, committed, tested state.
v1 non-goals (§1): user accounts, multi-tenancy, public deployment, mobile app, email notifications, paid APIs.

## Non-negotiables

### Database
- PostgreSQL 16 is the only database. No SQLite fallback — not for dev, not for tests (§3).
  Use Postgres-native features: `tsvector` + GIN, `pg_trgm`, JSONB, Postgres enums (§5).
- SQLAlchemy 2.0 **async** typed declarative models on the `asyncpg` driver (§3).
- Alembic owns all DDL: autogenerate, then review every migration by hand (§3, §5). Tests get their
  schema by running the migrations against a disposable Postgres (§12 Phase 1).
- Columns specified outside §5 that still belong to the models: `Company.field_provenance` JSONB (§8)
  and `Job.flexible_signal` boolean (§7.1).
- Ordinary reads and writes use ORM `select()`. Genuinely complex statements (company list, ranking,
  latest-round window functions) live in `db/queries.py` with a docstring explaining the shape (§3).
- Denormalized `latest_round_id`, `latest_job_posted_at`, `open_job_count` are refreshed by the ingest
  pipeline at end of run — never by triggers (§5).

### Ingestion is batch-only
- **No web request handler ever makes an outbound network call.** Handlers touch only the local database (§2).
- All external fetching happens in scheduled connector jobs or CLI commands: `refresh` (`--now` for a
  single connector) and the `seed` loader's domain validation (§2, §10).
- Every run writes a `FetchRun` row, success or failure (§7.2).
- Never delete on refresh: a job missing from its source gets `closed_at`; the DB is the historical record (§2, §5).
- Writes are upserts keyed on normalized domain; a field is overwritten only when the incoming value is
  non-null and its connector outranks the one recorded in `field_provenance` (§8).
- Never auto-merge fuzzy matches — write a `MergeCandidate` for `cli.py merge-review` (§8).
- Tests never hit live APIs: `respx` with recorded fixtures in `tests/fixtures/` (§3, §12 Phase 2).

### Web
- Server-rendered Jinja2 + HTMX only. No JS framework, no build step, no Tailwind, no component library.
  One hand-written `web/static/styles.css` (~200 lines, system font stack). Only small glue JS (§3, §9).
- Default company sort is `latest_job_posted_at DESC NULLS LAST`; keyset pagination, 50 per page (§9).

### Config-driven behaviour — edit YAML, not Python
- `config/classifiers.yaml` holds every `role_family`, `employment_type`, `seniority`, and `flexible_signal`
  keyword rule, editable without code changes. No keywords in code. Log the matched keyword (§7.1).
- `config/connectors.yaml` holds every connector's cron cadence and rate limits, loaded by APScheduler at
  startup. It is the only place a schedule is defined (§7.2, §11).
- `config/regions.yaml` holds every metro and its city list — each city's state, how that state is written
  out (`state_name`), the alias spellings sources use for a city, and the `enabled` flag that takes a metro
  out of ingestion without deleting a row. It is the only place region-specific logic
  may live: no metro or city name may appear as a literal anywhere else, comments and docstrings included,
  and `tests/test_regions.py` fails the build when one does. Editing it changes what is *ingested*; run
  `cli.py sync-regions` to relabel what is already stored (§11, §12 Phase 7).
- `config/seed_companies.yaml` bootstraps; never hardcode ATS tokens — connector discovery finds them (§4, §10).

### Classification never excludes
- **Every role is ingested and displayed.** Classification powers filters; it never filters by default (§7.1).
- An unmatched title gets `role_family='other'` and still lands in the DB and the default `/` and `/roles` views.
- `flexible_signal` is a badge and an opt-in filter. It is never a default filter and never hides anything (§7.1).
- With no filters applied, every open role of every family must be reachable in the UI (§12 Phase 4, §13).

### Excluded data sources — do not build (§4)
- LinkedIn scraping (ToS-prohibited; risks the user's own account). Instead (§6): store a footer-published
  company URL as `published`, else construct `linkedin.com/company/{slug}` as `constructed`; never fetch profiles.
- Crunchbase scraping.
- Wellfound / AngelList scraping.
- Email pattern-guessing or SMTP verification. Store only addresses the company itself published (§6).
- Any paid data API in v1.

### Source etiquette
- Tier-3 site fetches: honor `robots.txt` before every request; ≤ 1 request per domain per 2 s;
  ≤ 5 pages per domain per run; ETag/Last-Modified cache for 24 h; never deeper than depth 1 (§4).
- SEC EDGAR: descriptive `User-Agent` containing a contact email; ≤ 10 requests per second (§4).
- Contacts carry `confidence` = `published` or `constructed`; the UI must visually distinguish them (§6).
- Every connector is documented in `docs/SOURCES.md`: endpoint, official-API status, terms, rate limit, fields (§4).

## Toolchain (§3, §11)
- Python 3.12; `uv` with committed lockfile; FastAPI; Pydantic v2 + `pydantic-settings`; `httpx` + `tenacity`;
  `selectolax`; APScheduler; `structlog` JSON logs; `typer` CLI.
- `ruff` clean and `mypy --strict` passing on `ingest/` and `db/` before a phase is called done (§13).
- `pytest` + `pytest-asyncio`; DB tests run against a disposable Postgres (testcontainers or Compose).
- Docker Compose services `db`, `app`, `scheduler`; `make up` is the only setup step.
- Makefile targets: `up down migrate seed refresh test lint`. CLI: `migrate seed refresh stats merge-review`,
  plus `sync-contacts` (§6's constructed links across a whole database — local only, fetches nothing) and
  `sync-regions` (§11's metros across a whole database — local only, fetches nothing, never re-runs entity
  resolution, since §8 forbids deciding a merge without a person).
