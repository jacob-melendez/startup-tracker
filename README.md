# startup-tracker

A locally-run tracker of startups and their open roles across the US hubs named in
`config/regions.yaml` — Bay Area, New York, Seattle, Boston, Los Angeles and Austin as shipped,
and whatever you edit that file to say (see [Regions](#regions)). Scheduled connectors fill a
Postgres 16 database; a server-rendered UI browses it. `docs/SPEC.md` is the source of truth;
`CLAUDE.md` lists the non-negotiables; `docs/SOURCES.md` documents every data source; and
`docs/ARCHITECTURE.md` explains how the ingest path fits together.

## Setup and first run

From a clean checkout to a database worth looking at is six commands and about an hour, most of
it spent being polite to other people's servers. Each step says what "good" looks like, so you
can tell a slow step from a broken one. The two long steps are long in proportion to how many
regions `config/regions.yaml` has enabled; the times below are for the six that ship.

```sh
cp .env.example .env      # 1. then set CONTACT_EMAIL — see below
make up                   # 2. Postgres 16 + the web app + the scheduler (~1 min first time)
make migrate              # 3. alembic upgrade head (~2 s)
make seed                 # 4. the Bay Area bootstrap list (SPEC §10) (~2 min)
make refresh CONNECTOR=sec_edgar   # 5. one connector, to prove the plumbing (~10 min first run)
make refresh                       # 6. --all: every enabled connector (~30 min)
```

1. **`cp .env.example .env`, then set `CONTACT_EMAIL`.** SEC EDGAR's fair-access policy requires
   a real contact address in the User-Agent (SPEC §4); the client sends
   `startup-tracker/0.1 you@example.com`, and `sec_edgar` refuses to run without one rather than
   getting the whole project blocked. The address must have a real domain — `dev@localhost` is
   rejected on startup with a one-line `error: invalid settings (environment or .env): ...`,
   as is a `DATABASE_URL` that is not `postgresql+asyncpg://`.
2. **`make up`.** Builds the image and starts all three services — `db`, `app`, `scheduler` — and
   waits for `db` and `app` to report healthy. Good: `docker compose ps` shows three services
   running and <http://localhost:8000/healthz> answers `{"status": "ok"}` — the healthcheck
   deliberately touches no table, which is why `app` reports healthy before you have migrated.
   The pages themselves answer `500` until step 3 creates the tables; that is this step working,
   not failing.
3. **`make migrate`.** `alembic upgrade head`. Good: the last line names the newest revision and
   the command exits 0. Alembic owns every table, index and enum in this project (SPEC §3).
4. **`make seed`.** Loads `config/seed_companies.yaml` and resolves each domain over the network
   at one request per host per two seconds. Good: a summary line like
   `seed  status=ok  fetched=58  upserted=58  duration=118.4s`. A handful of `status='dead'`
   rows in the log is normal — see [Seeding](#seeding).
5. **`make refresh CONNECTOR=sec_edgar`.** One connector, so a configuration problem surfaces
   before you wait out a full sweep — and on a fresh database this is the single longest
   connector run there is: the first `sec_edgar` run backfills 18 months and searches every
   configured city separately, so its length is set by the city count ([Regions](#regions)).
   Good: `sec_edgar  status=ok  fetched=... upserted=...`
   and exit 0. An unset `CONTACT_EMAIL` prints no summary line at all — just
   `error: sec_edgar needs a contact email in the User-Agent ...` and exit 1, before any run is
   started or recorded — and a malformed one is refused earlier still, with
   `error: invalid settings (environment or .env): ...`. A `status=error` line here means the
   fetch itself failed; see [Troubleshooting](#troubleshooting-connector-failures).
6. **`make refresh`.** Every enabled connector, one after another. About half an hour on a fresh
   database once step 5 has absorbed `sec_edgar`'s backfill: `company_site` visits real
   companies' careers pages at a request every two seconds, and `ycombinator` walks the whole
   directory one batch at a time. Good: a summary line per connector, all `ok` or `partial`, and
   exit 0.

Then open <http://localhost:8000> for the company list and <http://localhost:8000/runs> for
ingestion health. On `/runs`, good is: every *implemented* connector showing a recent "last ok"
and marked neither **Stale** nor **Failing**, and no red banner across the top. One row is
expected to look bad and is not: `product_hunt` has a cadence in `config/connectors.yaml` but no
implementation yet, so nothing can ever record a successful run for it and it is permanently
**Stale**. Its cadence is the one SPEC §7.2 assigns it and stays. `opencorporates` is likewise
*not implemented* and `seed` is the on-demand bootstrap; neither has a cadence, so neither can
ever be Stale. All three are the design, not a fault.

From here on the scheduler keeps it current on its own; you never need to run `refresh` again
unless you want data *now*.

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
`sec_edgar` connectors contribute far more companies than these 58. It is also still a *Bay Area*
list, and stays one: the other five shipped regions have no seed entries and need none, because
they arrive entirely through the connectors ([Regions](#regions)).

## Refreshing data

All external fetching happens in batch — through the [scheduler](#scheduling) or the CLI. Web
request handlers never make outbound calls (SPEC §2). This section is the CLI half: the
on-demand "fetch it now" path.

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

Cadences and rate limits live in `config/connectors.yaml`, the metros and their cities in
`config/regions.yaml` ([Regions](#regions)), and every keyword that classifies a role —
`role_family`, `employment_type`, `seniority`, `flexible_signal` — in `config/classifiers.yaml`.
All four are edited without touching Python.

**Classification never excludes.** A job title that matches no rule is stored with
`role_family='other'` and still appears in the default views; `flexible_signal` is a badge and an
opt-in filter, never a default one (SPEC §7.1). The list order in `config/classifiers.yaml` *is*
the precedence between overlapping keywords, so reordering two rules is how you change which one
wins.

`seed` is deliberately absent from `refresh --all`: it is a bootstrap, and re-validating 58
domains every night buys nothing. Run `make seed` when the list changes.

### Upgrading a database that predates the contacts phase

The LinkedIn company link and the "Find people →" search link (SPEC §6) are built whenever a
company is upserted, so a database populated from scratch already has them everywhere — except
that the company link is built from the domain, so a company without one (an EDGAR-only row)
gets the people search alone. A database carried over from an earlier phase does not have even
that: a company no connector re-lists — a Form D outside the incremental window, a seeded row, a
company with neither a website nor a domain for `company_site` to visit — would never gain them.
One sweep fixes that:

```sh
python cli.py sync-contacts --dry-run   # says exactly what it would change
python cli.py sync-contacts             # ~5 seconds for 3,000 companies
```

It reads and writes only the local database — nothing is fetched, so no `fetch_runs` row is
written — and it touches only `constructed` contacts, never a published address. Running it twice
is running it once: the second run reports `added=0  removed=0`.

## Regions

`config/regions.yaml` is the only place a metro or a city is named. That is SPEC §11's expansion
hook and it is enforced, not merely intended: `tests/test_regions.py` reads the config, takes
every metro and city name in it as a needle, and fails the build if one of them appears as a
literal anywhere under `db/`, `ingest/`, `web/`, `migrations/` or the top-level scripts and build
files — comments and docstrings included, because a docstring that explains the code by naming
one region is exactly the drift the test exists to catch. Prose is exempt: this section, the
other documents and `config/` and `tests/` themselves are allowed to name the shipped regions,
and are checked by eye. Six metros ship, all enabled — Bay Area, New York, Seattle, Boston, Los
Angeles and Austin — with 48 cities between them.

```yaml
regions:
  - metro: Bay Area
    state: CA               # the default state for every city in this region
    state_name: California  # optional; how that code is written out in prose
    country: US             # optional; US is the default
    enabled: true           # optional; true is the default
    cities:
      - Palo Alto             # a bare string takes the region's state
      - name: San Francisco   # the mapping form: a state override, an alias list, or both
        aliases:
          - SF                # a spelling to match on, never a value to store
      - name: Jersey City     # a mapping overrides the state for one city
        state: NJ
        state_name: New Jersey
```

`state_name` is never stored. It exists so a text scanner can tell `City, <this city's state
written out>` — a poster naming the place the config means — from `City, <somewhere else>`, which
names a different place of the same name; `hn_hiring` reads it and nothing else does. Omitting it
costs a mention a rank and never a record: without it a written-out qualifier simply stops
promoting and the mention is ranked as the bare one it then looks like. On a per-city override
`state` and `state_name` **travel together** — a city that overrides its state takes its own name
for it or none at all, never the region's, which spells a different state — so `state_name`
without `state` on a city fails at load, naming the field.

The mapping form is there because a real metro crosses a state line: the shipped New York region
has `state: NY` and still takes in Jersey City, Hoboken and Newark NJ and Stamford CT. `country`
is *not* overridable per city — every hub in SPEC §1's "other US startup hubs" is in the US, and
the day one is not is the day to add it.

`enabled: false` takes a region out of ingestion and out of every city lookup while leaving its
list in the file, which is how a metro is switched off in one line. It changes nothing already
stored: rows keep the metro they were ingested with, because the database is the historical
record (SPEC §2), so a disabled metro can still appear in the Region filter — honest rather than
a bug. What the flag does change on sight is the app's own name: while exactly one region is
enabled every page titles itself `<that metro> Startup Tracker`, and with two or more enabled it
is plain `Startup Tracker`, because two metros have no honest shared label and inventing one would
be a claim this file does not make. A `(city, state)` pair may appear only once in the whole file,
disabled regions included: one city gets exactly one metro in the database
(`uq_locations_city_state`), so a file mapping a city into two metros describes a database that
cannot exist, and switching one of the two off does not make it exist. The loader refuses such a
file at startup, naming the value.

### Aliases

A city entry may also list the other spellings sources write that same place as:

```yaml
      - name: New York
        aliases:
          - New York City
          - NYC
```

A lookup answers an alias exactly as it answers the canonical name, and answers it with the entry,
so **the canonical `name` is what gets stored, always**. An alias is a string to match on and
never a value to write: the ingest path stores `name`, not the spelling a source handed it, so
recognising another spelling can never fragment `locations` into two rows for one place.
`uq_locations_city_state` keeps one row per real city and this file decides what that row is
called. That is also why an alias is held to the same uniqueness rule the names are: after
normalisation it may not collide with another city's name, or with another alias, in the same
state anywhere in the file — a spelling two cities answer to is a lookup with no honest answer.

Without an alias there is no near miss to fall back on. The geography filter has no "close
enough": a record whose spelling the file does not know is dropped as out of region, so a metro's
apparent size is decided partly by orthography. Measured against the live sources on 2026-09-08 —
the whole Y Combinator directory, 6204 entries, and the September 2026 Hacker News "Who is
hiring?" thread, 273 comments:

- 713 YC entries give their location as `New York City, NY` and 195 as `New York, NY`. With only
  the canonical spelling configured, the New York metro was therefore dropping 713 of the 908
  companies that name it — 79% — on spelling alone. That was the largest single data defect in
  the six-metro config, and closing it is why this key exists.
- Of the 273 HN comments, 53 named a configured city and 220 were dropped for naming none. 22 of
  those dropped comments write `NYC` as a whole word and 16 write `SF`. They were dropped for
  their spelling and for nothing else.

**Only an alias earned by evidence belongs in the file.** Three ship — `New York City` and `NYC`
for New York, and `SF` for San Francisco — and each carries the count that earned it in a comment
beside it, because an alias list is a claim about how sources actually write rather than a guess
at what somebody might type. A spelling nothing was observed to use widens every lookup and every
scan over free text for nothing. A *short* one is worse than nothing: the obvious two-letter form
of Los Angeles was measured and rejected, because of those same 6204 YC entries 31 give their
location as `LA, Nigeria` — Lagos State — while the metro's own companies write the name out, 189
of them as `Los Angeles, CA`. Two letters name a substring of the world rather than one city, and
aliasing them would file companies from another continent into a California metro under a name
they never used. So count it in the source first; if nothing writes it that way, it is not an
alias.

**After editing the file, in this order:**

```sh
docker compose restart scheduler       # first: it is holding the old file in memory
python cli.py sync-regions --dry-run   # says exactly which locations it would relabel
python cli.py sync-regions             # relabels the rows already stored
make refresh                           # ingests the cities that are new
```

The restart comes first because `load_regions_config` is cached at load: a scheduler process that
has been up since before the edit keeps ingesting under the *old* file, and its next fire rewrites
a `locations` row the sweep has just corrected — back to a metro the file no longer names — until
it restarts. Only an edit that **reassigns a city already stored** can be reverted that way, which
means a city moved between metros or a metro renamed; adding a metro, adding a city, or switching
one off with `enabled: false` cannot, because there is no stored row whose label the two vintages
of the file disagree about. Restarting first costs nothing in the cases where it is unnecessary,
so it is the step to take rather than the distinction to remember.

`python cli.py sync-regions` walks every row of `locations`, looks each up in the enabled config
and rewrites `metro` (and `country`) where they differ. Like `sync-contacts` it reads and writes
only the local database — nothing is fetched, so no `fetch_runs` row is written — and running it
twice is running it once. A city in no enabled region is reported and left exactly as it is;
nothing is ever deleted (SPEC §2). `--batch-size` bounds how many rows one `SELECT` reads per
keyset page, as it does for `sync-contacts`, and not how many a transaction carries: each changed
row is committed on its own, deliberately, so the sweep never holds write locks on several
`locations` rows at once and cannot deadlock against a concurrent ingest run upserting the same
cities in its own order — and an interrupted sweep keeps every row it had already relabelled. It
rarely matters here, because `locations` holds one row per `(city, state)` — a few dozen — however
many companies sit behind them.

It deliberately does **not** re-run entity resolution. Moving a city between metros can change
which companies SPEC §8 *would* have treated as the same one — step 2 matches names within a
metro — and silently re-deciding that is precisely what §8 forbids.

Nor does a later `refresh` re-decide it for you, which is the part worth knowing before you rely
on it: SPEC §8's candidate step runs only for a record that is *creating* a company, and
re-ingesting a company already stored resolves it by `external_id`, domain or name and stops
there, so the pair this move newly put in one metro is never re-compared. It reaches
`merge-review` only if some later record creates a third, similar row. After moving a city, look
at the receiving metro yourself: `python cli.py stats` says whether its company count moved, and
the UI's Region filter lists who is in it.

Neither half of the order of operations substitutes for the other: skip the `refresh` and the new
cities have no data at all; skip `sync-regions` and the companies already stored stay filed under
their old metro until something happens to re-ingest them, which for an EDGAR-only company with
no website may be never.

Adding an **alias** is a third case, and the sweep is no help there: a company dropped as out of
region was never stored, so there is no mislabelled row to relabel — there are no rows at all. It
takes a `refresh` of whichever connector was dropping the spelling, and for a monthly source that
is the next thread.

**What another metro costs.** One connector scales with the city count, and only one: `sec_edgar`
issues one EDGAR full-text search per configured city per 30-day window (SPEC §4,
`docs/SOURCES.md`), so its query count is simply proportional to the number of cities enabled —
one per *city*, not per spelling, so an alias costs a lookup and never a query.
Going from the Bay Area's 18 to all 48 makes the first run's 18-month backfill roughly two and a
half times longer, and every later incremental run proportionally longer too. Every other
connector reads the same national index, or the same company's board, whatever the region list
says, and filters locally — so it costs the same with one region enabled or all six. Each extra
city also widens the surface for a false-positive name match, which is why the shipped lists are
tight: a city earns its place by being somewhere startups actually file and post rather than by
being inside the metro's official boundary.

## Scheduling

The `scheduler` service (`python scheduler.py`, started by `make up`) is the third Compose
service and the reason you only run `refresh` by hand when you are impatient. It reads
`config/connectors.yaml` at startup — **the only place a cadence is defined** — and builds one
APScheduler job per connector that has an implementation, is `enabled`, and has a cadence.
There is no cron expression, connector list or rate limit anywhere in `scheduler.py`.

| Connector | Cadence | When it fires (UTC) |
|---|---|---|
| `sec_edgar` | `0 5 */3 * *` | every 3rd day of the month, 05:00 |
| `ycombinator` | `0 4 * * sun` | Sunday 04:00 |
| `greenhouse` | `0 6 * * *` | daily 06:00 |
| `lever` | `0 6 * * *` | daily 06:00 |
| `ashby` | `0 6 * * *` | daily 06:00 |
| `workable` | `0 6 * * *` | daily 06:00 |
| `hn_hiring` | `0 9 2 * *` | monthly, the 2nd at 09:00 |
| `funding_rss` | `0 7 * * *` | daily 07:00 |
| `product_hunt` | `0 5 * * sun` | *not scheduled* — no implementation yet |
| `company_site` | `0 3 * * sat` | Saturday 03:00 |
| `opencorporates` | — | on-demand only (and no implementation yet) |
| `seed` | — | on-demand only: `make seed` |

That table is checked against `config/connectors.yaml` by a test, so it cannot drift from what
the service actually runs.

Things worth knowing before you edit a cadence:

- **Everything is UTC.** `0 6 * * *` is 06:00 UTC, not 06:00 wherever the machine is.
- **Write weekdays as names** (`sun`, `sat`), not numbers. APScheduler 3.x numbers weekdays
  Monday = 0 while crontab numbers them Sunday = 0, so `0 3 * * 6` would fire on a *Sunday*.
  Names mean the same day under either convention; the header of `config/connectors.yaml` says
  so, and a test asserts each connector's next fire lands on the day SPEC §7.2 names.
- **Runs are serialized.** At most one connector run happens at a time, process-wide. The four
  ATS connectors all fire at 06:00 and each may fetch an arbitrary company's careers page, where
  SPEC §4 allows one request per domain per two seconds; rate limiting is per HTTP client and
  each run builds its own, so overlapping runs could double up on the same host.
- **Nothing runs at startup.** The first fetch of any connector is at its next cron fire. Use
  `make refresh` when you want data now — a restart is not a refresh.
- **An edit needs a restart.** The YAML is read once, at startup:
  `docker compose restart scheduler`.
- **`enabled: false` and `cadence: null` are how you switch something off**, and the startup log
  says which of the four reasons applies to every connector that got no job: no implementation
  yet, not in the registry (that is `seed`, which is run by hand as `cli.py seed`), disabled, or
  on-demand only.

```sh
docker compose logs -f scheduler          # follow it
docker compose logs scheduler | grep 'schedule loaded'   # what it picked up, with next fire times
docker compose restart scheduler          # after editing config/connectors.yaml
```

A run that fails three times in a row logs at ERROR (`connector failing  connector=... consecutive=3`)
and shows up on `/runs`; see [Operations](#operations) below.

## Operations

Two read-mostly commands for looking after a live database. Neither fetches anything. The other
two local-only commands, `sync-contacts` and `sync-regions`, are written up beside the thing they
reconcile — [contacts](#upgrading-a-database-that-predates-the-contacts-phase) and
[regions](#regions).

### `python cli.py stats`

The state of the database in one screen: row counts, how the companies fall across the metros,
the last successful run of every *configured* connector, and the last seven days' intake
(SPEC §12).

```
$ docker compose run --rm app python cli.py stats
database  tracker on db:5432
rows
  companies          3188  (2144 active, 475 acquired, 569 dead)
  jobs               1605 open, 0 closed
  funding rounds       32
  investors             0
  people              143
  contacts            229 published, 515 constructed
  locations            46
  sectors             399
  merge candidates      3 unresolved, 0 resolved
  fetch runs           12
  notes / bookmarks     1 / 1
companies per metro  (an office in two metros counts in both)
  Bay Area     2087
  New York      402
  Boston        231
  Seattle       188
  Los Angeles   141
  Austin         76
last successful run
  sec_edgar       2026-09-04 09:55 UTC  (1d ago)
  ycombinator     2026-09-04 09:55 UTC  (1d ago)
  greenhouse      never                 (4 consecutive failures)
  ...
  opencorporates  never                 (not implemented)
last 7 days
  companies added    3188
  jobs added         1605
```

- `companies` splits into all three of `active` / `acquired` / `dead`, so the three numbers sum to
  the total beside them. A `dead` or `acquired` company is never deleted — it is history (SPEC §2).
- `jobs` splits into open and closed. A job whose source stopped listing it is stamped
  `closed_at`, never removed, which is why the closed count only grows.
- `contacts` splits by SPEC §6 confidence: `published` is an address the company put on its own
  site; `constructed` is a LinkedIn link built from the domain. The UI keeps them apart and so
  does this.
- `merge candidates` is the queue `merge-review` works through (below).
- **`companies per metro` is the operational answer to "did the new region actually ingest
  anything?"**, biggest first. It counts a company once per metro it has a location in, so a
  company with offices in two of them is counted in both and the section does not sum to
  `companies`; a company with no location at all appears in no line. The metros come from the
  `locations` rows, not from `config/regions.yaml`, so a region you disabled but already ingested
  keeps its line — which is the point, since the rows are still there ([Regions](#regions)).
- **The last-successful-run list is driven by the config, not by the runs table** — the same list
  `/runs` shows — so a connector that has *never* succeeded appears, reading `never`. That is the
  case worth seeing.
- `(not implemented)` marks a connector that is configured but has no code behind it yet
  (`product_hunt`, `opencorporates` — SPEC §4 Tier 2). `/runs` marks the same two. Their `never`
  is not breakage and nothing will change it until the connector is written.
- `(N consecutive failures)` marks a connector whose last three-or-more finished runs all ended
  `error` (SPEC §7.2). It is independent of the timestamp beside it: "last ok 9d ago" with no
  failure marker means the connector is *limping* — its recent runs ended `partial`, which
  completes the scan but logs per-item trouble — whereas a fresh "last ok" plus a streak means
  something broke this morning. Both numbers are shown because either alone misleads.
- The `database` line names the database and host and never the URL: `DATABASE_URL` carries a
  password.

Exit status is 0, or 1 with a one-line stderr message if the database cannot be read.

### `python cli.py merge-review`

Entity resolution (SPEC §8) matches on `external_id`, then normalized domain, then normalized
name within a metro. Past that it falls back to trigram similarity, and a similarity above 0.85
**never merges**: it writes a `merge_candidates` row instead. "Acme Robotics" and "Acme Robotics
Inc" are usually one company and occasionally two, and only a person can tell — so the pipeline
files the question and this command is where it gets answered. It is consequently the only thing
in the system that deletes a company.

```sh
docker compose run --rm app python cli.py merge-review --list     # look, decide nothing
docker compose run --rm app python cli.py merge-review            # work through the queue
docker compose run --rm app python cli.py merge-review --limit 5
```

Each pair is printed side by side, most similar first:

```
candidate #1  similarity 0.91  trigram similarity 0.91 on normalized_name within metro 'Bay Area'
                  [1] #2  (suggested)             [2] #5
  name            Acme Robotics                   Acme Robotics Inc
  domain          acme.com                        —
  city            San Francisco                   San Francisco
  stage           seed                            unknown
  status          active                          active
  open jobs       12                              1
  funding rounds  1                               0
  contacts        3                               0
  first seen      2026-03-02 09:41 UTC            2026-08-30 06:00 UTC
  last seen       2026-09-05 06:00 UTC            2026-09-05 06:00 UTC
  seen by         greenhouse, sec_edgar, seed     ycombinator
  [1] keep #2  [2] keep #5  [n] not a duplicate  [s] skip  [q] quit
```

| Key | What it does |
|---|---|
| `1` / `2` | Fold the *other* company into this one. |
| `n` | Record "not a duplicate": the pair is stamped resolved and never offered again. No data changes. |
| `s` | Leave the pair on the queue for next time. |
| `q` | Stop. Everything already decided is kept — each decision commits on its own. |

End-of-input does what `q` does, so piping answers in (or running with no TTY) ends cleanly.
`(suggested)` marks the side with a domain, else the one with more open jobs, else the lower id —
a hint, never a default keystroke, because a default that merged would be the auto-merge SPEC §8
forbids.

**What a merge moves.** Every job, funding round, person, contact, location, sector, source,
note and bookmark moves to the survivor. Only rows that would violate one of the survivor's
unique constraints stay behind — a job whose `external_id` the survivor already has, a contact
it already has — and those are counted and reported. A star on a discarded duplicate job is
moved onto the survivor's copy rather than lost. Anything the *user* wrote that cannot be kept is
**printed in full** instead of being dropped quietly: the note on the discarded company when the
survivor already had one, and the note on a starred duplicate posting whose twin was already
starred. That text is the one thing in the database no connector can produce again. A reviewed
"not a duplicate" verdict about the discarded company is not carried over to the survivor — it
was a decision about a company that no longer exists, and the pipeline is free to raise the pair
again on its own evidence. The duplicate row is then deleted — which has to happen before the
next step, because `domain` is uniquely indexed and two rows cannot hold the same one at once —
after which fields the survivor has no value for are filled from the discarded row along with
their provenance, `first_seen_at`/`last_seen_at` widen to cover both, and the survivor's
denormalized counts are refreshed.

**Three collisions are worth more than "the survivor's row wins".**

- A contact the survivor only **constructed** while the discarded company **published** the same
  address is promoted to `published` in place, carrying the discarded row's `source_id`. SPEC §6
  makes that distinction visible in the UI, so letting a guess outrank an observation because it
  was written first would be displayed to you as fact.
- A `company_sources` row is one per `(company, connector)`. A survivor row with no `external_id`
  adopts the discarded row's. A *second* id for a connector the survivor already knows cannot be
  kept, so the pair is printed: SPEC §8 step 0 resolves that connector's records by `external_id`,
  which means its next run will create the company you have just merged away. If you see that
  line, merging the other way round would have kept it.
- The survivor counts as having a note only when its row holds a status, a rating or text. The
  detail page writes a row on every Save, so an empty submit leaves a blank stub — and a stub must
  not outrank a real `applied / 5` on the other side.

**After the fold**, SPEC §6's two constructed LinkedIn links are rebuilt for the survivor, in the
same transaction. A merge changes both of their inputs: it can fill the survivor's `domain`, and
it moves the discarded company's people-search link across. Without the rebuild the survivor would
end up with two people searches, one of them for a company name no longer in the database. The
report says `linkedin links N built, N superseded` when anything changed.

The final line is `merge-review  merged=N  not-duplicate=N  skipped=N  remaining=N`, where
`remaining` counts every unresolved pair still in the table — including ones past `--limit`.

## Troubleshooting connector failures

Read `/runs` first. It carries two independent signals, and they mean different things:

| Signal | Means | Read it as |
|---|---|---|
| **Stale** | No `ok` run in over twice the connector's cadence (SPEC §9) | *Silence.* Nothing is running, or every run is going `partial`. An on-demand connector (`cadence: null`) is never stale. |
| **Failing** | The last 3 or more *finished* runs all ended `error` (SPEC §7.2) | *Noise.* Runs are happening and failing. Also logged at ERROR by the scheduler, and shown as a banner at the top of `/runs`. |

A row can be both. A row can be neither and still be worth a look: a connector whose runs keep
ending `partial` breaks its failure streak (a partial run completed its scan) while never
refreshing its last *successful* run, so it reads "last ok 9d ago, 0 consecutive failures".

| Symptom | Cause | Fix |
|---|---|---|
| `error: sec_edgar needs a contact email in the User-Agent ...` with no summary line (CLI), or a `sec_edgar` run on `/runs` whose `error_text` starts `fetch: RuntimeError: sec_edgar needs a contact email ...` (scheduler) | `CONTACT_EMAIL` is unset. The connector refuses to run rather than earn the project a `403 Undeclared Automated Tool` from SEC's WAF, which is what a User-Agent with no contact address gets (SPEC §4). | Set `CONTACT_EMAIL=you@example.com` in `.env` and restart. The accepted shape is `startup-tracker/0.1 you@example.com`; `dev@localhost` never reaches the guard — the settings validator refuses it at startup. That validator only insists on a dotted domain, so a plausible-looking fake (`a@b.invalid`) is the one way a real `403` still reaches you. |
| `status=error  fetch_run=not-recorded` | The `fetch_runs` row itself could not be written — the database is unreachable. | Check `DATABASE_URL` and `docker compose ps db`. This is the one case where a failure leaves *no* row; do not go looking for one. |
| A run ends `partial` | The scan completed but some records failed. `error_text` on `/runs` holds the first problems (capped at 4,000 characters). | Usually harmless — one malformed posting, one unreachable careers page. Read `error_text`; the run still counts as completed and still advances the incremental window. |
| `/runs` shows a connector **Stale** but nothing is failing | Nothing is running it: `enabled: false`, no implementation (`product_hunt`), or the scheduler is down. A `cadence: null` connector is *not* a cause — with no schedule to miss, it is never Stale. | `docker compose logs scheduler \| grep 'not scheduled'` names the reason for every connector that got no job. |
| The scheduler scheduled nothing at all | Every connector was skipped, or the service is not running. | `docker compose ps scheduler`, then the `schedule loaded` log line, which lists what it did pick up. |
| `ycombinator` fails with `403` from Algolia | YC's public search key **rotates**; the connector re-reads it from the directory page on every run, so a 403 usually means the page layout changed. | Check `docs/SOURCES.md` § `ycombinator`. `options.app_id`/`api_key` in `config/connectors.yaml` are the emergency override. |
| A Workable account returns nothing | Workable's widget answers `200` with an empty `jobs` list for account names that were never real. | Not a failure. The connector treats an empty board with no description as "nothing here" and moves on. |
| `company_site` discovers no ATS token for a company | Some careers pages render their board in JavaScript, so there is no link to find. | Not a failure, and not fixable from here — this project does not run a browser (SPEC §4). |
| A fetch is skipped with "robots.txt disallows" | The host's `robots.txt` forbids that path, and `respect_robots: true`. | Honour it. This is SPEC §4's etiquette, not a bug. A `robots.txt` that cannot be read at all is also treated as a refusal. |
| Repeated `429` / slow runs | Rate limiting and backoff. Retries are exponential with jitter, and `Retry-After` is honoured on 429/503. | Lower `requests_per_second` for that connector in `config/connectors.yaml`. Do not raise it. |
| A connector refetches everything and is slow | The 24 h ETag/Last-Modified cache is cold. | It lives in `HTTP_CACHE_DIR` (`.cache/http`). Deleting it forces a full refetch — occasionally useful, usually not what you want. |
| `hn_hiring` finds no thread | The "Who is hiring?" thread posts on the 1st; the cadence is the 2nd at 09:00 UTC. | Run it after the 2nd, or not at all until then. An empty month is recorded like any other run. |
| `make test` fails to start a database | testcontainers needs Docker. | Set `TEST_DATABASE_URL` to a disposable Postgres 16 whose name ends in `_test` (see `.env.example`). Its `public` schema is dropped on every run, which is why the name is checked. |

## Web interface

Four pages, server-rendered Jinja2 with HTMX for the interactive parts, on
<http://localhost:8000> (SPEC §9):

- **`/` — the company list.** One collapsed row per company: name, HQ city, sector chips, latest
  round with amount and date, open role count, and a breakdown of which role families are open.
  Filter by Region or City and a company headquartered elsewhere can still match on a second
  office — it really does have one there — so the row adds `Seattle office` beside the HQ to say
  which one answered. The marker appears only when the row is not already naming that city, so
  an unfiltered list never shows it.
  Expanding a row lazy-loads the rest over HTMX — the thesis, every location, the funding history
  with investors, the full open-roles table, contacts (what the company published on its own site,
  kept visibly separate from the LinkedIn search shortcuts the pipeline constructs — SPEC §6), a
  provenance line ("seen by greenhouse 2h ago"), and an inline note / status / rating form that
  saves without leaving the page.
- **`/roles` — every open job, newest first.** The "what opened this week" view: one row per job
  across all companies, with a star toggle to bookmark one.
- **`/company/{id}`** — the expanded row as a standalone, linkable page.
- **`/runs` — ingestion health.** Every configured connector with its cadence and last successful
  run, marked **Stale** when there has been no successful run in over twice its cadence and
  **Failing** when its last three or more finished runs all ended `error` — with a banner across
  the top naming every failing connector and its streak (SPEC §7.2, §9). Then the last 100
  `fetch_runs` rows with counts and error text. This is where silent breakage shows up; the two
  signals are explained under [Troubleshooting](#troubleshooting-connector-failures).

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
| `metro` | yes | Metro region, labelled **Region** in the form and first in the row. A company matches when any of its locations carries that metro. Named for the column it filters (`locations.metro`); the values offered are the metros actually in the database, so a region you have disabled but already ingested is still there to filter by. |
| `city` | yes | HQ or office city. The form groups the options by region, because dozens of cities in one flat list is not a control anyone can use; the value on the wire stays the bare city name, so an old bookmark keeps working and a city name shared by two states matches both. Where the facet holds such a shared name, that option's label carries its state so you can tell the two apart on screen. |
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

`metro` and `city` are two independent geography filters, ANDed like everything else rather than
one narrowing the other: picking a region does not repopulate the city list, and
`?metro=X&city=Y` asks for companies that have a location in region X *and* a location in city Y.
Neither is a mode, a scope or a setting — there is nothing to switch back afterwards, and the URL
carries the whole state of the view.

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
make up                          # db + app + scheduler, waits for the healthchecks
docker compose logs -f app       # follow the app's logs (LOG_LEVEL / LOG_JSON in .env)
docker compose up -d --build app # after a dependency change
```

The `scheduler` service publishes no port and serves nothing; its liveness signal is `/runs`.
`stats` and `merge-review` are run against the same image on demand:
`docker compose run --rm app python cli.py stats`.

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
`scheduler.py`, `settings.py`, `logging_config.py` and `tests/`.

`docs/ARCHITECTURE.md` is the next thing to read if you are changing the ingest path: it covers
the pipeline's lifecycle, entity resolution, the denormalization strategy, and a table mapping
each rule to the config file that owns it.
