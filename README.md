# startup-tracker

A locally-run tracker of Bay Area startups and their open roles. Scheduled connectors fill a
Postgres 16 database; a server-rendered UI (Phase 4) browses it. `docs/SPEC.md` is the source of
truth; `CLAUDE.md` lists the non-negotiables; `docs/SOURCES.md` documents every data source.

## Setup

```sh
cp .env.example .env      # then set CONTACT_EMAIL (see below)
make up                   # Postgres 16 via Docker Compose
make migrate              # alembic upgrade head
make seed                 # load the Bay Area bootstrap list (SPEC §10)
```

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
   Phase 4 adds the `/runs` page on top of the same table. The one exception: when the run row
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

## Development

```sh
make test     # pytest against a disposable Postgres (Docker, or TEST_DATABASE_URL — see .env.example)
make lint     # ruff check, ruff format --check, mypy --strict
```
