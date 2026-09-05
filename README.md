# startup-tracker

A locally-run tracker of Bay Area startups and their open roles. Scheduled connectors fill a
Postgres 16 database; a server-rendered UI browses it. `docs/SPEC.md` is the source of truth;
`CLAUDE.md` lists the non-negotiables; `docs/SOURCES.md` documents every data source.

## Setup

```sh
cp .env.example .env      # then set CONTACT_EMAIL (see below)
make up                   # Postgres 16 + the web app, via Docker Compose
make migrate              # alembic upgrade head
make seed                 # load the Bay Area bootstrap list (SPEC §10)
```

`make up` waits for both services to report healthy and leaves the UI on
<http://localhost:8000>. Start it before `make migrate` if you like — the app answers its
healthcheck without touching the database — but the pages have nothing to show until you have
migrated and seeded.

## Seeding

`make seed` (or `python cli.py seed`) loads `config/seed_companies.yaml` — the Bay Area
bootstrap set of SPEC §10 — and **resolves every domain over the network**: one `HEAD` request
per entry, redirects followed. An entry whose host cannot be reached at all, or whose site
answers `404`/`410`, is stored with `status='dead'` and logged; the run never fails because of
it. A `403` from a bot-blocking WAF is *not* treated as dead — the host answered. `--skip-validation`
turns the probe off for an offline first run.

Seeding takes a few minutes: the loader is polite (one request per host per two seconds, and it
reads each host's `robots.txt` first). It is safe to re-run — entries are upserted on the
normalized domain — and it never writes an ATS board token: the Greenhouse, Lever, Ashby and
Workable connectors discover those from each company's own careers page (SPEC §4, §10).

The seed list is a bootstrap, not a target. After a `refresh --all` the `ycombinator` and
`sec_edgar` connectors contribute far more companies than these 58.

## Refreshing data

All external fetching happens in batch — through the scheduler (Phase 6) or the CLI. Web request
handlers never make outbound calls (SPEC §2).

1. Set `CONTACT_EMAIL` in `.env` (or the environment). SEC EDGAR's fair-access policy requires a
   contact address in the User-Agent (`startup-tracker/0.1 you@example.com`, SPEC §4), so
   `sec_edgar` refuses to run without one; the address must have a real domain (`dev@localhost`
   is rejected with a one-line `invalid settings` error, as is a non-asyncpg `DATABASE_URL`).
2. Run one connector, or every enabled one:

   ```sh
   make refresh CONNECTOR=sec_edgar                 # inside Compose
   make refresh                                     # --all: every enabled connector
   python cli.py refresh --connector sec_edgar      # on the host (uv sync first)
   python cli.py refresh --connector sec_edgar --since 2025-01-01
   python cli.py refresh --all
   ```

   `--since YYYY-MM-DD` sets the lower bound of the fetch window (UTC midnight) and overrides the
   connector's incremental window and its first-run backfill — `sec_edgar` otherwise looks back
   18 months on the first run, then from 3 days (`overlap_days`) before the start of its newest
   run that finished `ok` or `partial`, so late-indexed filings are picked up; a run that ended
   `error` does not advance the window (SPEC §14.3, `docs/SOURCES.md`).

   `--now` means "run right now, ignoring the connector's cadence" (SPEC §2). Every CLI
   invocation already does exactly that, so the flag changes nothing: there is no debounce, each
   invocation runs and records a run.

3. Every run — success or failure — is recorded in the `fetch_runs` table (`connector`, `status`
   `ok`/`partial`/`error`, counts, `error_text`; SPEC §7.2). The CLI prints one summary line per
   connector and exits 1 when any run ended `error` (`partial` prints a warning and exits 0).
   The `/runs` page is a view onto that same table. The one exception: when the run row
   itself cannot be written (the database is unreachable), the summary line reads
   `status=error  fetch_run=not-recorded ...`, a note goes to stderr, and nothing appears in
   `fetch_runs` — do not look for a row for that line.

Cadences and rate limits live in `config/connectors.yaml`, the city list in `config/regions.yaml`,
and every keyword that classifies a role — `role_family`, `employment_type`, `seniority`,
`flexible_signal` — in `config/classifiers.yaml`. All four are edited without touching Python.

**Classification never excludes.** A job title that matches no rule is stored with
`role_family='other'` and still appears in the default views; `flexible_signal` is a badge and an
opt-in filter, never a default one (SPEC §7.1). The list order in `config/classifiers.yaml` *is*
the precedence between overlapping keywords, so reordering two rules is how you change which one
wins.

`seed` is deliberately absent from `refresh --all`: it is a bootstrap, and re-validating 58
domains every night buys nothing. Run `make seed` when the list changes.

## Web interface

Four pages, server-rendered Jinja2 with HTMX for the interactive parts, on
<http://localhost:8000> (SPEC §9):

- **`/` — the company list.** One collapsed row per company: name, HQ city, sector chips, latest
  round with amount and date, open role count, and a breakdown of which role families are open.
  Expanding a row lazy-loads the rest over HTMX — the thesis, every location, the funding history
  with investors, the full open-roles table, contacts (what the company published on its own site,
  kept visibly separate from the LinkedIn search shortcuts the pipeline constructs — SPEC §6), a
  provenance line ("seen by greenhouse 2h ago"), and an inline note / status / rating form that
  saves without leaving the page.
- **`/roles` — every open job, newest first.** The "what opened this week" view: one row per job
  across all companies, with a star toggle to bookmark one.
- **`/company/{id}`** — the expanded row as a standalone, linkable page.
- **`/runs` — ingestion health.** Every configured connector with its cadence and last successful
  run, marked `Stale` when there has been no successful run in over twice its cadence, then the
  last 100 `fetch_runs` rows with counts and error text. This is where silent breakage shows up.

`/healthz` returns `{"status": "ok"}` without touching the database; it is what the Compose
healthcheck probes.

### The app never makes an outbound call

Every page is rendered from the local Postgres database and nothing else (SPEC §2). No request
handler fetches anything: nothing under `web/` may even import `httpx` or a connector, and a test
walks the AST of every module there to prove it. So pages render in milliseconds, and no amount
of clicking around can get you rate-limited or blocked by a source. External links — job posting
URLs, company sites, careers pages, LinkedIn pages and searches — are rendered for *you* to click,
and each one is checked for an `http`/`https` scheme first, so a scraped `javascript:` URL never
becomes an `href`. LinkedIn in particular is only ever a link: nothing in the codebase fetches
`linkedin.com`, in a request handler or a connector (SPEC §4, §6).

### Filters

Both `/` and `/roles` share one filter set. It is submitted over HTMX so results swap without a
full page load, but it is a real `GET` form: every filtered view is a bookmarkable URL, and the
form still works with JavaScript off. All filters combine into a single parameterized query.

| Parameter | Repeats | Effect |
|---|---|---|
| `q` | | Full-text search over the company `tsvector`, plus a `pg_trgm` similarity arm on the name so a typo still finds it. On `/roles` the job text gets the full-text search and the company name keeps the typo tolerance. |
| `city` | yes | HQ or office city. |
| `sector` | yes | Sector slug. |
| `stage` | yes | Company stage. |
| `round` | yes | Type of the *latest* round. |
| `amount_min` / `amount_max` | | Latest round amount, in whole US dollars. An undisclosed amount is NULL and so falls outside any range. |
| `round_months` | | Latest round announced within N months (1–120). |
| `family` | yes | Role family, all 19 values. On `/` this selects companies with at least one matching open role. |
| `employment` | yes | Employment type. |
| `seniority` | yes | Seniority. |
| `flexible` | | `flexible=1` restricts to roles carrying the `flexible_signal` badge. Off by default, always (SPEC §7.1). |
| `open_roles` | | `open_roles=1` restricts to companies with at least one open role. |
| `tracking` | yes | Your own tracking status; `none` matches companies you have never touched. |
| `closed` | | `/roles` only: `closed=1` also shows roles whose source dropped them. They are never deleted, just stamped `closed_at` (SPEC §2), so this is the historical view. |
| `sort` | | `/`: `recent_job` (the default — `latest_job_posted_at DESC NULLS LAST`), `funding_date`, `amount`, `newest`, `open_roles`, `name`. `/roles`: `posted` (default), `first_seen`, `company`. |
| `cursor` | | The keyset page token. The "Load more" button supplies it; you never write it by hand. |

The four role-level filters constrain the *same* role, not four different ones: a company matching
`family=software&employment=part_time` has a part-time software role, not a full-time software
role and a separate part-time marketing one.

Filtering never hides a role from you once you are looking at a company. The open-roles table
inside an expanded row lists **every** open role that company has, with the ones matching your
filter highlighted rather than the others removed (SPEC §9). And with no filters applied at all,
every open role of every family is reachable — a title that matched no classification rule is
stored as `role_family='other'` and shows up like any other. There is a test for exactly that
(SPEC §13).

### "Load more"

Lists page 50 rows at a time, keyset-style rather than with `OFFSET`. The "Load more" button
carries an opaque cursor holding the last row's sort key and id; the server returns the next 50
rows plus a fresh button, and htmx swaps the button out for them, so rows accumulate on the page
instead of the page reloading. Two consequences worth knowing:

- There is no result total anywhere in the UI. A keyset list deliberately runs no `COUNT(*)`; it
  fetches 51 rows and the 51st only decides whether there is a next page.
- A cursor belongs to one sort order. Changing the sort or any filter starts over at page one, and
  a hand-edited cursor from a different sort is rejected with a `400` rather than quietly handing
  back the wrong page.

### Running it

Under Compose, `make up` builds and starts it with the source tree bind-mounted. Template and CSS
edits are picked up on the next request; a change to a Python module needs
`docker compose restart app`, because the container runs uvicorn without `--reload`.

```sh
make up                          # db + app, waits for both healthchecks
docker compose logs -f app       # follow the app's logs (LOG_LEVEL / LOG_JSON in .env)
docker compose up -d --build app # after a dependency change
```

On the host, skip Compose for the app and run uvicorn directly against the Compose database — the
fastest loop, since `--reload` restarts on every Python edit:

```sh
uv sync
uv run uvicorn web.app:app --reload      # http://127.0.0.1:8000
```

That reads `DATABASE_URL` from `.env`, which `.env.example` already points at
`localhost:5432` — the port `docker compose up -d db` publishes. If you changed `POSTGRES_PORT`,
change `DATABASE_URL` to match.

## Development

```sh
make test     # pytest against a disposable Postgres (Docker, or TEST_DATABASE_URL — see .env.example)
make lint     # ruff check, ruff format --check, mypy --strict
```

`make lint` covers `web/` too: `mypy --strict` runs over `db/`, `ingest/`, `web/`, `cli.py`,
`settings.py`, `logging_config.py` and `tests/`.
