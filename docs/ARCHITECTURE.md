# Architecture

For the contributor who has read `README.md` and now has to change the ingest path. It explains
how a byte of JSON from a job board becomes a row on `/`, and — more usefully — which invariants
that path is holding up, so a change does not quietly break one.

`docs/SPEC.md` is the source of truth for *what* the system does; this document describes *how the
code as committed* does it, and says so when the two are not the same shape. `docs/SOURCES.md`
covers each individual source's endpoints, terms and rate limits, and is not repeated here.

---

## Process topology

Three Compose services (SPEC §3), and the boundary between them is the whole architecture.

```
  external sources
 ┌──────────────────┐        ┌─────────────────────────────────────────┐
 │ EDGAR full-text  │        │  scheduler          (scheduler.py)      │
 │ YC Algolia index │        │  APScheduler, cron read from            │
 │ Greenhouse API   │        │  config/connectors.yaml; runs           │
 │ Lever API        │        │  serialized, one connector at a time    │
 │ Ashby API        │        └──────────────────┬──────────────────────┘
 │ Workable API     │◀───┐                      │ calls cli.run_one
 │ HN Firebase      │    │   ┌──────────────────▼──────────────────────┐
 │ TechCrunch RSS   │    │   │  connectors      (ingest/connectors/*)  │
 │ company websites │    └───┤  fetch() → raw items                    │
 └──────────────────┘        │  to_records() → CompanyRecord  (pure)   │
        ▲                    └──────────────────┬──────────────────────┘
        │ every request                         │ records
        │ goes through       ┌──────────────────▼──────────────────────┐
   ingest/http.py            │  pipeline        (ingest/pipeline.py)   │
   UA · robots.txt           │  resolve → upsert → close → denormalize │
   rate limit · retries      │  FetchRun bookkeeping                   │
   24 h ETag cache           └──────────────────┬──────────────────────┘
                                                │ SQL (asyncpg)
 ┌──────────────────┐        ┌──────────────────▼──────────────────────┐
 │  cli.py          │───────▶│  db                 PostgreSQL 16       │
 │  migrate  seed   │        │  tsvector + GIN, pg_trgm, JSONB, enums  │
 │  refresh  stats  │        └──────────────────┬──────────────────────┘
 │  merge-review    │                           │ SELECT only
 │  sync-contacts   │        ┌──────────────────▼──────────────────────┐
 │  sync-regions    │        │  app     (uvicorn web.app:app, :8000)   │
 └──────────────────┘        │  / · /roles · /company/{id} · /runs     │
                             │  Jinja2 + HTMX; no outbound call, ever  │
                             └─────────────────────────────────────────┘
```

The `cli.py` arrow is simplified: `refresh` and `seed` run the *same* connectors → pipeline path
the scheduler does (that is the point — see below), while `migrate`, `stats`, `merge-review`,
`sync-contacts` and `sync-regions` touch nothing but `db`.

* **`db`** — `postgres:16`. The only database; there is no SQLite path anywhere, including in
  tests (CLAUDE.md). Alembic owns every piece of DDL.
* **`app`** — `uvicorn web.app:app` on port 8000. It reads the local database and does nothing
  else. It is also the image `docker compose run --rm app python cli.py …` uses for one-off
  commands, which is how `make migrate`, `make seed` and `make refresh` work.
* **`scheduler`** — `python scheduler.py`. No published port and no healthcheck: an APScheduler
  process has no endpoint to probe, and inventing a heartbeat file would be a second source of
  truth about liveness. Its liveness signal is `/runs`, which is the right one — a scheduler that
  is up but scheduling nothing is exactly as broken as one that is down.

### The one-way rule

**No web request handler ever makes an outbound network call** (SPEC §2). This is not a
convention people remember; it is enforced structurally in two places.

1. Nothing under `web/` may import a fetcher. `tests/test_web_acceptance.py::
   test_no_module_under_web_imports_a_fetcher` walks the AST of every module under `web/` and
   fails on any import of `httpx`, `ingest.http`, `ingest.connectors` or `ingest.seed`. Note what
   is *not* forbidden: `web/routes/runs.py` imports `ingest.config` for `load_connectors_config`
   and `cadence_period`, because that module only reads local YAML. The test is deliberately
   about the transitive reach of a request handler, not about package names.
2. Connectors are equally walled off from the database. `ingest/base.py` states it as a contract:
   a connector never opens a session. Connectors that need to know which companies exist —
   the four ATS connectors and `company_site`, which set `Connector.wants_targets` — are handed a
   pre-loaded work list in `FetchContext.targets` by the pipeline instead.

So the data flow is strictly one-way: sources → connectors → pipeline → Postgres → web. The web
layer's freshness is entirely a property of the last connector run, which is why `/runs` exists.

---

## The ingestion pipeline

### The connector contract

`ingest/base.py` defines `Connector[RawT]`, an ABC with two abstract methods and nothing else
of substance:

| member | what it is |
|---|---|
| `name` (ClassVar) | the key in `config/connectors.yaml`, `FetchRun.connector` and `CompanySource.connector` |
| `wants_targets` (ClassVar) | ask the pipeline to pre-load `FetchContext.targets` |
| `cadence` (property) | `self.config.cadence` — the cron string, or `None` for on-demand |
| `fetch(ctx) -> AsyncIterator[RawT]` | all network I/O, through `ctx.http` |
| `to_records(raw) -> Iterable[CompanyRecord]` | pure mapping: no I/O, no database |

Splitting fetching from mapping is what makes `to_records` testable from a recorded fixture with
no HTTP at all, which is how every connector test in `tests/` works (`respx` + `tests/fixtures/`).
`__init__(config, regions)` raises `ValueError` if it is handed another connector's config block,
so a wiring mistake fails at construction rather than mid-run.

`FetchContext` carries `http`, `now` (injected UTC, so tests control time), `since` (the CLI's
`--since`), `last_success_at`, `run_id`, a bound `log`, `problems` and `targets`.

**`ctx.problems` is the connector's channel for non-fatal trouble** — a filing that 404ed, a
search window that hit the API's result cap. Append a one-line description and carry on; the run
is recorded `partial` and the messages land in `FetchRun.error_text`. Reach for it instead of
raising whenever the scan can honestly continue.

### The exact lifecycle of `run_connector`

`ingest/pipeline.py::run_connector` is the only thing that turns connector output into rows. In
order:

1. **Validate time.** `now = now or utc_now()`; a naive `now` or `since` is a `ValueError`. Every
   timestamp column is `timestamptz` and asyncpg encodes a naive datetime as *local* time, so a
   naive value would silently shift every row the run writes by the host's UTC offset.
2. **Write the `FetchRun` row first, and commit it.** `_start_run` inserts the row in its own
   transaction *before anything is fetched*, and reads `last_successful_run_started_at` in the
   same transaction. `FetchRun.status` defaults to `error` in the schema, so a process killed
   mid-run still leaves a row that says so (SPEC §7.2: "every run writes a `FetchRun` row
   regardless of outcome").
3. **Pre-load targets** when `connector.wants_targets`, via `db.queries.load_company_targets`.
   A failure here is recorded, never raised, and fails the run *before a single request is made*
   through the internal `_TargetsUnavailable` — a connector handed no targets would otherwise
   look like a connector with nothing to do.
4. **Iterate.** For each item `connector.fetch(ctx)` yields: `n_fetched += 1`, then
   `connector.to_records(raw)` inside a try/except (a raising item is logged with `exception`,
   its message appended to `item_errors`, and skipped), then **each record is upserted in its own
   transaction** (`async with session_factory() as session, session.begin()`), so one bad filing
   never rolls back the others. `n_upserted` counts records written; the touched company ids
   accumulate in a set.
5. **Skips are not errors.** An `enrich_only` record whose company does not exist writes nothing
   (SPEC §4 Tier 2 #5) and counts as neither upserted nor touched. It does not make the run
   `partial`.
6. **Refresh the denormalized columns** of the touched companies, in one more transaction. A
   failure here is appended to `item_errors`, never propagated.
7. **Finalize, in a `finally`.** The status rule is exactly:

   | status | condition |
   |---|---|
   | `error` | `fetch_error is not None` **or** `fetch` did not run to completion |
   | `partial` | fetch completed, but there were item errors, `ctx.problems`, or a refresh failure |
   | `ok` | fetch completed with none of those |

   `error_text` is `join_error_messages([fetch_error, *item_errors, *ctx.problems])` — fetch
   exception first, then item and refresh errors, then the connector's own problems — capped at
   `ERROR_TEXT_LIMIT` (4000 characters) with a `"... (+N more)"` suffix that counts the messages
   that did not fit. Each message is collapsed to one line by `_describe`, because
   `join_error_messages` separates messages with newlines and a `DBAPIError` or a Pydantic
   `ValidationError` spans many; a multi-line message would be indistinguishable from several
   failures on `/runs`.
8. **Nothing escapes.** Connector, record and refresh failures never raise out of
   `run_connector`; only a failure to write the *initial* `FetchRun` row (i.e. the database is
   down) propagates. `cli.py` catches that one and prints `NOT_RECORDED`
   (`fetch_run=not-recorded`) so stdout never claims a row that does not exist.

### The incremental window

`FetchContext.last_success_at` is `db.queries.last_successful_run_started_at(session, connector)`:
the `started_at` of the newest earlier run whose status is in `COMPLETED_RUN_STATUSES` —
`ok` **or** `partial`. A connector applies its own incremental logic from it, falling back to a
configured backfill when there is none (`sec_edgar`'s `backfill_months: 18`, SPEC §14.3).

Why `partial` counts: it scanned its whole window and merely had per-item trouble. Why `error`
must not: it stopped early, so anchoring the next window on it would skip whatever it never
reached. Concretely, without that rule one 404ed filing would force the 18-month backfill on
every subsequent run.

`--since YYYY-MM-DD` on the CLI overrides both, arriving as `FetchContext.since`.

This is *not* the same question `/runs` asks. `db.queries.last_successful_runs` counts
`status = 'ok'` **only**, because SPEC §9's health line is about "no *successful* run in over
twice its cadence" — and a run that had to log errors is not the thing whose absence that line
exists to make loud. So a connector that has gone `partial` every night for a week still advances
its incremental window and still shows as Stale. The asymmetry is deliberate; both docstrings say
so, and so does the README.

### Jobs are closed, never deleted

`_upsert_jobs` upserts on `uq_jobs_company_id_external_id`. A re-seen job gets
`last_seen_at = now` and `closed_at = NULL` (it re-opened); `first_seen_at` is kept; nullable text
fields keep their stored value when the incoming one is `None` (`coalesce(excluded.x, jobs.x)`);
the classification columns take the incoming values outright, since the record always carries a
usable fallback member.

When a record asserts `jobs_complete=True` — "this is the company's *entire* open-job list at this
source" — `db.queries.close_missing_jobs` sets `closed_at = now` on this connector's other open
jobs at that company. Two details matter:

* "This connector's jobs" is resolved by joining `sources.connector` through `jobs.source_id`;
  `jobs` has no connector column of its own. A Greenhouse listing therefore never closes a job an
  HN comment reported.
* Jobs already closed keep their original `closed_at`. Nothing is deleted (SPEC §2, §5): the
  database is the historical record of when a role was posted and when it was pulled.

A connector that sees only a slice of a company's jobs (`hn_hiring`, a single posting) leaves
`jobs_complete=False` and closes nothing.

Nothing the pipeline ever deletes is *observed* data. It issues exactly one kind of `DELETE`:
`confidence='constructed'` contacts that a change of name or domain has superseded — and a
constructed contact is not an observation, it is a pure function of the company's current `name`
and `domain`, recomputed on every upsert, so the stale one is a dead search shortcut rather than a
historical fact (`_ensure_constructed_contacts` argues this at length). A `published` contact is
untouchable; the insert is `DO NOTHING` and every delete filters on `confidence = 'constructed'`.
The only other deletion in the system is outside the pipeline entirely: the duplicate company row
`merge-review` removes.

### The region model

`config/regions.yaml` is SPEC §11's expansion hook and, per CLAUDE.md, the only place a metro or
a city may be named. `ingest/config.py::load_regions_config` is its only reader — `@cache`d on the
path, so a test that swaps in an alternate file needs a unique path or a `cache_clear()`. Three
shapes in that file are worth knowing before you read the code that consumes it.

**`enabled`.** A region carries `enabled: bool = True`, and `RegionsConfig` exposes two views of
the same file: `cities` / `metros` / `lookup()` see only the enabled regions — the view ingest,
the CLI and the web layer use, so switching a metro off in one line stops it being ingested —
while `all_cities` sees every region including the disabled ones. Only the file's own consistency
checks use the wide view, which is why a `(city, state)` pair repeated across a *disabled* region
is still a load error: `uq_locations_city_state` gives one city exactly one metro, so the file
would describe a database that cannot exist, and flipping the flag back would be the moment it
broke. `enabled: false` is not a delete, either — rows already stored keep their metro (SPEC §2),
so a disabled metro can still appear in the UI's Region facet, which reads the `locations` table
rather than the config precisely so the form never offers a value that returns nothing. The
*count* of enabled regions also names the product: `web.templating.site_name` reads that same
enabled view and returns `<metro> Startup Tracker` while exactly one region is enabled, plain
`Startup Tracker` otherwise, for every page `<title>`, the header brand and the FastAPI app
title. Two or more metros have no honest shared label, and inventing one would be a claim the
config does not make.

**Per-city state.** A city entry is either a bare string, which takes the region's default
`state`, or a `{name, state}` mapping that overrides it for that one city — required because a
real metro crosses a state line (the New York region is `state: NY` and holds Stamford CT). The
loader normalizes both spellings to a flat `RegionCity` carrying its own `state`, so nothing
downstream knows which form the file used. `country` is deliberately not overridable per city.

Beside `state`, either level may carry an optional `state_name` — the same state written out in
prose. It is never stored and only `hn_hiring` reads it, to tell a poster naming *this* city's
state from one naming a different place of the same name; the state names live in the config for
the same reason the metros do, and the letter heuristic that stood in for them before read a
neighbouring state's name as this city's own. On a per-city override the two keys travel
together: `Region.resolved_cities` takes `state` and `state_name` from the same side of the
override, so a city in another state never inherits the region's spelling for its own, and
`state_name` without `state` on a city is a load error with a path to the field.

The consequence is that a *bare* city name no longer implies a state, so nothing may infer one by
taking the first entry with that name: `RegionsConfig.lookup_city_name` answers only when exactly
one enabled *state* configures the name and returns `None` — never a guess — when two do, whether
those two sit in different regions or, as the mapping form allows, in the same one.
`ingest/seed.py` uses it and *skips* an entry it cannot place, counting it under the
`outside_region` skip counter every other connector already uses, rather than raising: with six
metros and an `enabled` flag, a routine edit to the config must not be able to crash `make seed`.

**Aliases.** The mapping form also carries `aliases`, the other spellings sources write that same
place as. Every lookup matches the canonical `name` and each alias alike — `lookup`,
`lookup_city_name`, and the needle sequence a text scanner walks — and every one of them answers
with the `RegionCity`, whose `city` is the canonical spelling. That is the invariant to hold on
to: **an alias is a string to match on and never a value to store.** `_fill_metros` writes
`entry.city`, not the string the connector was handed, so recognising a source's own spelling
costs no second row and `uq_locations_city_state` keeps holding one row per real place, named by
this file. What a *missing* spelling costs is the whole record: the geography filter has no "close
enough", so the connector drops it as out of region and the metro's company count comes out lower
for a reason that is purely orthographic. Which spellings earn a line is therefore a measured
question, answered in `config/regions.yaml` beside each alias with the count that earned it; the
README's Regions section works the numbers through.

The loader holds an alias to the same one-spelling-one-city rule it holds a name to: after
normalization it may not collide with another city's name, or with another alias, within a state
anywhere in the file — disabled regions included, for the same reason the `(city, state)` check
includes them, that flipping `enabled: true` must not be the moment a file starts describing a
database that cannot exist. A spelling two cities in one state answer to is exactly the tie-break
`lookup_city_name` refuses to make, and the checks run in two passes so that an alias colliding
with a city configured *later* in the file is reported as the alias collision it is. Two *states*
may configure the same spelling, just as they may configure the same city name, and
`lookup_city_name` returns `None` for it rather than guessing.

Aliases are also why there are two ordered sequences and not one. `cities_longest_first` carries
one string per city, ordered by the canonical name; `city_needles_longest_first` carries a
`CityNeedle` per *spelling* — canonical names and aliases — ordered by the length of the needle.
**A scanner over free text must use the second**: the first leaves it nothing to match an alias
with, and ordering by the city's name would let a short name shadow a longer alias, which is the
very failure longest-first ordering exists to prevent, arriving through the mechanism added to fix
a related one. `hn_hiring.find_city` ranks the matched spelling's length for the same reason.

**How a metro change propagates.** Editing the file changes what is *ingested*; it does not touch
what is *stored*. Three separate mechanisms, and the third is a deliberate non-mechanism:

1. **The next upsert adopts it.** `_fill_metros` replaces a configured city's `city`, `state`,
   `country` *and* `metro` with the configured spelling before the record is written, and
   `_upsert_locations` is `ON CONFLICT (city, state) DO UPDATE SET metro = …, country = …` rather
   than `DO NOTHING` for exactly this reason: under `DO NOTHING` the metro a city was *first*
   ingested with would be the metro it kept for ever, and the config would have stopped being
   where the answer lives. It is safe to have every writer update the row because `_fill_metros`
   has already made every writer write the same value *as it loaded the config* — within one
   process the update is a byte-identical no-op, and the stored metro can only change when the
   config did. Across processes that qualifier is the whole story: `load_regions_config` is
   `@cache`d at load, so a scheduler that has not restarted since the edit still holds the old
   file and its next fire rewrites a row a freshly-loaded `cli.py sync-regions` has just
   corrected. It converges rather than corrupts — after the restart the same `DO UPDATE` writes
   the new value — which is why the statement is left alone and the README's region recipe
   restarts the scheduler *before* the sweep. Only an edit reassigning an already-stored city can
   be reverted this way; adding a metro or a city, or `enabled: false`, cannot. The other
   exception is a city no region claims: it keeps whatever label its connector invented, and
   there the last writer wins, because nothing in the config can adjudicate a city it does not
   list.

   The rows are also locked in one global `(city, state)` order rather than the record's order,
   which is the second thing the clause change changed: `DO NOTHING` takes no lock on a
   conflicting row, while `DO UPDATE` takes an exclusive one and holds it until the record's
   whole transaction commits. Record order as lock order let two overlapping runs naming the same
   two cities in opposite orders deadlock on them, so `_upsert_locations` sorts; `sync-regions`
   reaches the same rule from the other side by committing one row at a time.
2. **`cli.py sync-regions` covers the rest.** A company nothing re-ingests — an EDGAR-only row
   with no website, a seeded company whose board 404s — would otherwise keep its old metro
   indefinitely, which is the same gap `sync-contacts` closes for SPEC §6. It walks `locations`,
   looks each row up in the enabled config, rewrites `metro`/`country` where they differ, fetches
   nothing and writes no `fetch_runs` row. A city in no enabled region is reported, never changed
   and never deleted (SPEC §2).
3. **Entity resolution is not re-run, on purpose.** Step 2 below matches names *within a metro*,
   so moving a city between metros changes which companies would have been compared. Re-deciding
   that in bulk from a config edit is the auto-merge SPEC §8 forbids — nobody looked at those
   pairs. So nothing is merged or split by editing the file. Neither does a later ingest run pick
   the pair up: `record_merge_candidates` runs *only when the company was created* (see the call
   order below), and a company already stored resolves by `external_id`, domain or name and never
   reaches it again — so a newly co-located pair enters `merge_candidates` only if some third,
   similar record is created later. Nothing in the tree re-compares an existing pair on demand,
   deliberately; after a move, `cli.py stats`' companies-per-metro section and the UI's Region
   filter are what let a person look.

---

## Entity resolution

SPEC §8, in the order `ingest/normalize.py::resolve_company` actually applies it. First hit wins.

**Step 0 — same connector, same `external_id`.** Not in the SPEC; added in Phase 2 because
without it a connector re-seeing its own record would create a duplicate whenever the record
carries no domain (an EDGAR filing with no website, a YC entry mid-edit). It looks up
`company_sources` on `(connector, external_id)` with `limit(2)`: that pair is *not* unique in the
schema — the primary key is `(company_id, connector)` — and an ATS board token can legitimately be
shared by a parent and a subsidiary. Two matches mean the identifier does not identify anybody,
so the step logs and falls through rather than picking whichever row the planner returned first.

**Step 1 — exact match on the normalized domain.** The canonical key.
`normalize_domain` strips scheme, credentials, port, path, query, fragment and a trailing dot,
lowercases, drops one leading `www.`, and IDNA-encodes non-ASCII hosts so the stored key is always
ASCII. It returns `None` for anything that is not a domain — a single label with no dot, a
`mailto:` value — and the pipeline treats an unparseable domain as "unknown" rather than failing.

**Step 2 — exact match on `normalized_name`, scoped to the same metro.** `normalize_name`
NFKD-decomposes, drops combining marks, casefolds, spells `&` as `and`, joins dotted initialisms
(`L.P.` → `lp`), turns remaining punctuation into spaces, and strips legal-suffix tokens from the
end repeatedly — but never so far that the name empties (`Labs Inc` → `labs`). The metro scope is
a semi-join through `company_locations`/`locations`, so a company with several offices in the
metro matches once. The record's metro is its HQ location's, else its first location's, after
`_fill_metros` has adopted the spelling in `config/regions.yaml`.

**Step 3 — `pg_trgm` similarity > 0.85 in the same metro, which never merges.** It runs
`SET LOCAL pg_trgm.similarity_threshold = 0.85` so the indexed `%` operator uses
`ix_companies_normalized_name_trgm`, and then re-checks `similarity(...) > cast(0.85 AS REAL)`,
because `%` is inclusive and a plain Python float binds as `float8` — which would promote real
0.85 to 0.8500000238… and admit a pair scoring *exactly* 0.85. Exact equality is step 2's job and
is excluded here. The matches come back as `Resolution.candidates`, never as a match: the caller
inserts the company and calls `record_merge_candidates`, which writes `merge_candidates` rows with
`company_id_a < company_id_b` for `cli.py merge-review`. **Nothing in the ingest path ever
auto-merges** (SPEC §8, CLAUDE.md).

Steps 2 and 3 need a metro and are skipped without one, so a domain-less, location-less record can
only ever resolve through step 0 — unless it is `enrich_only`, in which case
`_resolve_across_metros` runs step 2 across every metro in `config/regions.yaml` and accepts the
result **only when exactly one company matches**. Two "Acme"s resolve to neither; the trigram step
is deliberately not attempted, because a fuzzy match with no metro to scope it is precisely what
SPEC §8 forbids.

### Concurrency: the advisory lock

Resolution reads ("does this domain belong to another company?", "is there a company with this
name in this metro?") and then writes. Under READ COMMITTED a concurrent run can insert a matching
row in between — the four ATS connectors share the 06:00 cadence, and `refresh --now` can overlap
a scheduled run. So `upsert_company_record` takes `pg_advisory_xact_lock(hashtextextended(key, 0))`
before resolving, keyed on the normalized domain when there is one and otherwise on
`"{metro}\x1f{normalized_name}"`. It is released at commit or rollback, needs no row, and is taken
before any `SELECT … FOR UPDATE` so no transaction holds a row lock while waiting for it. A hash
collision merely serializes two unrelated records.

Without it, the domain case raises an `IntegrityError` that rolls back the whole record (jobs and
rounds included), and the name case leaves two companies with the same `normalized_name` in one
metro and *no* `MergeCandidate` — step 3 excludes exact-equal names, so the duplicate would never
even be offered for review.

### The priority-aware upsert

`merge_company_fields(existing, provenance, incoming, connector)` is the pure rule of SPEC §8 and
the right place to start reading. For each column in `COMPANY_MERGE_FIELDS` (`name`, `domain`,
`website_url`, `one_liner`, `thesis`, `founded_year`, `employee_est`, `stage`, `status`,
`ats_provider`, `ats_token`):

* an incoming `None` is "unknown" and is never written;
* otherwise `_may_overwrite` allows the write when the stored value is NULL, the field has no
  provenance, the incoming connector *is* the recorded owner (a connector refreshes its own
  values), or it strictly outranks the owner;
* `domain` is only ever *filled*, never changed. A non-null stored domain is kept whoever reports
  a different one — and if the incoming domain already belongs to a different company,
  `_record_domain_conflict` writes a `MergeCandidate` with `similarity=1.0` instead of moving it;
* whenever `name` is written, `normalized_name` is written with it, so step 2 can never go stale.

Rank comes from `CONNECTOR_PRIORITY`, highest first:

```
sec_edgar > ycombinator > greenhouse > lever > ashby > workable > company_site > funding_rss > product_hunt
```

`connector_rank` returns `0` for anything unlisted — `hn_hiring`, the `seed` loader,
`opencorporates`, test fakes — so two unlisted connectors never overwrite one another, and any
listed connector outranks them all.

The provenance itself lives in `Company.field_provenance`, a JSONB map of field name → connector.
A field the connector was *allowed* to write becomes that connector's in the returned provenance
even when the value happens to be unchanged, so the map always names the most authoritative
connector that has confirmed the value; the `UPDATE` is emitted only when the value actually
differs.

### What `upsert_company_record` does, in order

Order is load-bearing; changing it changes behaviour.

```
normalize (domain, name, metros)
  → advisory lock on the resolution key
  → resolve_company
  → INSERT (ON CONFLICT (domain) DO NOTHING; lost race ⇒ re-resolve by domain and update)
      or UPDATE under SELECT … FOR UPDATE
  → CompanySource            (one row per company per connector; a NULL external_id never erases one)
  → Source                   (one row per record per run; every child row points at it)
  → locations → sectors → funding rounds + investors → people → contacts
  → _ensure_constructed_contacts   (must run AFTER contacts, so a URL published in this very
                                    record already counts as published)
  → jobs                     (and close_missing_jobs when jobs_complete)
  → record_merge_candidates  (only when the company was created)
```

A note for anyone comparing this with the SPEC: §8 describes the write as
`INSERT … ON CONFLICT (domain) DO UPDATE`. The code resolves first and then inserts or updates,
using `ON CONFLICT (domain) DO NOTHING` only as a lost-race fallback. The reason is that the merge
rule needs the *stored* `field_provenance` to decide each column, which a single `ON CONFLICT DO
UPDATE` cannot express, and that resolution has three steps of which the domain is only one. The
observable semantics — upsert keyed on normalized domain, `last_seen_at` refreshed, a field
overwritten only when non-null and outranking — are the SPEC's.

### Where `merge-review` fits

`merge_candidates` is a queue of pairs the pipeline was forbidden to act on. `cli.py merge-review`
is where a human resolves one, and `db.queries.merge_companies(session, keep_id=…, drop_id=…,
now=…)` performs it. Its order is also load-bearing:

1. move the children **first** — jobs (skipping ones that would collide on
   `(company_id, external_id)`, after rescuing any bookmark onto the surviving twin), funding
   rounds, people (then de-duplicate exact matches within the survivor), contacts, locations,
   sectors, sources, the user's note, and every other **unresolved** `merge_candidates` pair
   naming the discarded company, repointed onto the survivor and renormalized to
   `company_id_a < company_id_b`. A *resolved* pair is not carried over: a reviewer's "not a
   duplicate" was about the company being deleted, and `ingest.pipeline`'s candidate upsert
   refreshes a row only `WHERE resolved_at IS NULL`, so moving the verdict would silence that
   pair for ever on a decision nobody made about the survivor;

   Each move is guarded by `NOT EXISTS` against the survivor's own unique constraint, so the
   survivor's row wins every collision and the rest are left for the cascade. Three collisions
   carry something the winning row would otherwise lose, and each is settled *before* the delete:

   * **contacts** — the survivor's row wins, but not its *confidence*. A contact the survivor
     only `constructed` while the discarded company `published` the same `(kind, value)` is
     promoted in place to `published`, taking the discarded row's `source_id` with it. SPEC §6
     makes that distinction the difference between an address the company put on its own site and
     a link this system guessed, and letting a guess outrank an observation because it was
     written first would be a downgrade the UI then displays as fact;
   * **`company_sources`** — one row per `(company, connector)`. A survivor row with no
     `external_id` adopts the discarded row's, the same "fill what the survivor does not know"
     rule step 3 applies to the `companies` columns. What cannot be kept is a *second*
     `external_id` for a connector the survivor already identifies itself to; that pair is
     returned in `MergeResult.sources_discarded` and `merge-review` prints it, because SPEC §8
     step 0 resolves that connector's records by `external_id` and its next run will therefore
     create the discarded company again;
   * **`user_notes`** — the survivor's note wins and the discarded text is *returned*, never
     silently dropped. "The survivor has a note" means its row holds *content* — a tracking
     status, a rating or text — not merely that a row exists: `/companies/{id}` writes a row on
     every save, so an empty submit leaves a blank stub that must not outrank a filled note;
2. **delete the drop company**, whose `ON DELETE CASCADE` clears whatever could not move;
3. **then** fill the survivor's NULL columns from the values read in step 1, together with their
   `field_provenance` entries. This must come after the delete because `ix_companies_domain` is
   unique — copying the drop's domain onto the survivor while both rows exist would raise;
4. refresh the survivor's denormalized columns.

`cli.py merge-review` then calls `ingest.pipeline.reconcile_constructed_contacts` for the survivor
in that same transaction, before it commits. A merge changes both inputs of SPEC §6's derived
links — step 3 can fill the survivor's `domain`, and step 1 moved the discarded company's
people-search link onto it — so without this the survivor commits holding two people searches, one
of them built from a name that is no longer in the database, and no `linkedin_company` link for the
domain it has just acquired. It is the same reconciliation the ingest path runs on every upsert and
`cli.py sync-contacts` runs over the whole database; only `constructed` rows are ever written or
dropped by it.

`db/models.py` states the licence in one sentence: *companies are only ever deleted by
`merge-review`*. That covers the duplicate row and the child rows a unique constraint cannot let
survive, and nothing else. A "not a duplicate" decision sets `resolved_at` and changes no data, so
the pair is never offered again; a merged pair leaves no `resolved_at` row at all, because the
cascade took it with the deleted company.

---

## The denormalization strategy

Three columns on `companies` are derived (SPEC §5):

| column | meaning |
|---|---|
| `open_job_count` | `count(*)` of the company's jobs with `closed_at IS NULL` |
| `latest_job_posted_at` | `max(coalesce(posted_at, first_seen_at))` over those same open jobs |
| `latest_round_id` | id of the round with the greatest `announced_date` (NULLS LAST; ties and undated rounds broken by the highest id, i.e. most recently ingested) |

`db.queries.refresh_company_denormalized_columns(session, company_ids)` recomputes all three in a
single `UPDATE companies` whose values are correlated scalar subqueries. `company_ids=None`
recomputes every company; a sequence limits it to those ids; an empty sequence is a no-op. A
company with no open jobs or no rounds gets `0` / `NULL` / `NULL`.

**Who calls it, and when.** Exactly two callers: `run_connector`, once at the end of every run,
over `sorted(touched)` — the companies that run actually upserted — and `merge_companies`, for the
survivor of a merge. Nothing else writes these columns.

**Why not triggers** (SPEC §5 mandates the choice; `db/models.py` and the query docstring record
the reasoning): the pipeline is the only writer of jobs and rounds and it knows when a run is
complete, so one set-based recompute at the end of a run replaces thousands of per-row trigger
firings; the write path stays visible in Python; the columns describe committed state rather than
a half-applied batch; and a trigger would have to be maintained in the migrations, outside the
models' view.

**`coalesce(posted_at, first_seen_at)` is deliberate.** Several sources publish no posting date at
all. Without the fallback those jobs would sort to the NULL tail for ever and the companies
posting them would never surface on `/`; with it, a job counts as recent from the day we first saw
it. That is a small, documented lie about the past in exchange for the default sort meaning
something.

**What goes stale, and why that is acceptable.** These columns change only when a run touches the
company. So between runs:

* `open_job_count` does not decay. A role pulled from a board this morning stays counted until the
  next run of the connector that owns it re-lists the company with `jobs_complete`. That is the
  same lag the underlying `jobs` rows have — nothing is *more* stale than the data it summarizes.
* A company nothing re-lists keeps its numbers indefinitely. If its ATS board 404s, `/runs` is
  where that shows up, not the company row.
* A run that ends `error` still refreshes whatever it managed to touch; a record whose upsert
  raised was rolled back and is not in `touched`, so it is consistent either way.

**How `/` depends on them.** SPEC §9's default sort is `latest_job_posted_at DESC NULLS LAST`,
which is a plain index scan over `ix_companies_latest_job_posted_at`. The `has_open_roles` filter
is `open_job_count > 0`, not an `EXISTS` — deliberately the same number the row displays, so the
filter and the badge can never disagree. And `latest_round_id` lets `company_list_page` reach the
latest round with `LEFT JOIN funding_rounds AS latest_round ON latest_round.id =
companies.latest_round_id`, one primary-key lookup per row, which serves the displayed round, the
three round filters and two of the six sorts at once. A window function would compute the same
answer on every query; the pointer computes it once per run.

The trade-off is stated plainly in `company_list_page`'s docstring: the list is only as fresh as
the last `refresh_company_denormalized_columns`. If you add a fourth derived column, add it to
that one statement — not to a trigger, and not to a query-time subquery that would diverge from
the other three.

---

## Operations

### The scheduler

`scheduler.py::build_scheduler` returns a configured but **unstarted** `AsyncIOScheduler`, built
from `config/connectors.yaml` and nothing else — no database, no network, no clock. It walks the
config blocks and adds a job when the connector has an implementation in `all_connectors()`, is
`enabled`, and has a non-null `cadence`, building the trigger with
`CronTrigger.from_crontab(cadence, timezone=UTC)` — the same call `ingest.config.cadence_period`
makes, so the scheduler and `/runs` can never disagree about what "daily" means. Every other
configured connector is logged once at INFO with the reason (`SKIP_NOT_REGISTERED`,
`SKIP_DISABLED`, `SKIP_ON_DEMAND`), so "nothing is scheduled" is never a silent state. No
connector is named in Python, and neither is a cron expression or a rate limit.

Each job carries `max_instances=1` and `coalesce=True`, so a slow run is never overlapped by its
own next fire and a backlog of missed fires collapses to one, plus
`misfire_grace_time=MISFIRE_GRACE_SECONDS` (one hour) to decide how late that survivor may be.

Two properties worth knowing before you change it:

* **Runs are serialized** by a module-level `asyncio.Lock`, so at most one connector run happens
  at a time. The four ATS connectors all fire at 06:00 and each may fetch an arbitrary company's
  careers page; rate limiting is per-`HttpClient` and each run builds its own, so two concurrent
  runs could hit the same host at once and break SPEC §4 Tier 3's one-request-per-domain-per-2s.
  Serializing also makes the scheduler behave exactly like `refresh --all`.
* **The job body calls `cli.run_one`.** A scheduled run and `cli.py refresh --connector NAME` are
  the same code path by construction, so the User-Agent, rate limit, robots policy and 24 h cache
  can never drift apart between them. Config is re-read through the `@cache`d loaders, so a
  restart — `docker compose restart scheduler` — is what picks up a YAML edit.

Nothing is run at startup and the scheduler never queries the database there, so an unmigrated
database can still start the service — the first fetch of any connector is at its next cron fire,
and `make refresh` is how you fetch now.

After a run that ended `error`, and only then, `alert_on_failure_streak` reads that connector's
streak and logs `connector failing` at ERROR with `consecutive` and `threshold` once it has
reached `CONSECUTIVE_FAILURE_ALERT` — SPEC §7.2's requirement. A run that ended `ok` or `partial`
has already reset the streak, so there is nothing to alert on and no reason to spend the query. A
failure of the streak query itself is logged and dropped: an ops signal about an already-recorded
run must not become a second failure.

### The two health signals on `/runs`

They answer different questions and a row can carry both.

| signal | source | means |
|---|---|---|
| **Stale** | `last_successful_runs` (`status='ok'` only) vs `cadence_period × STALE_CADENCE_MULTIPLE` | no successful run in over twice this connector's cadence (SPEC §9) |
| **Failing** | `consecutive_failure_counts` ≥ `CONSECUTIVE_FAILURE_ALERT` (3) | the last N *finished* runs were all `error` (SPEC §7.2) |

`connector_health` in `web/routes/runs.py` is driven by the *config*, not by the runs table: a
connector that has never run has no rows to join against and is exactly the case the page exists
to surface, so it appears with `last_ok=None` and `stale=True`. `UNIMPLEMENTED_CONNECTORS` lives in
`ingest/config.py` — written out rather than read from `ingest.connectors`, because importing that
package would put a request handler one import away from the HTTP client, and because the registry
is the wrong question anyway (`seed` is implemented yet deliberately absent from it). `/runs` and
`cli.py stats` both import it from there and both mark the same two connectors, so the two views of
one list cannot disagree; `tests/test_docs.py` pins it against the YAML and the registry.

`consecutive_failure_counts` counts only runs with `finished_at IS NOT NULL`: an in-flight run has
the default `status='error'` and no `finished_at`, and a process hard-killed mid-run leaves such a
row for ever, so counting it would report a failure for a run whose outcome is genuinely unknown.
It walks newest-first by `(started_at DESC, id DESC)` and the streak ends at the first `ok` **or**
`partial` — a `partial` run completed its scan, so it is not a failure. That is wider than the
Stale signal's `ok`-only rule, which is why "last ok 9 days ago" can sit beside "0 consecutive
failures" and both be true.

A connector with `cadence: null` is never Stale — there is no schedule for it to have missed —
but it can still be Failing, because it can still be run by hand.

### The CLI

Seven commands, all local except where noted (SPEC §11 names five of them; `sync-contacts` and
`sync-regions` are the two additions, and CLAUDE.md records both). The pair are the same shape on
purpose: each reconciles rows already stored against a rule that has since changed, neither
fetches anything, and neither writes a `fetch_runs` row.

| command | fetches? | what it does |
|---|---|---|
| `migrate` | no | `alembic upgrade head` through the Alembic API — the same thing `make migrate` runs |
| `seed` | yes | loads `config/seed_companies.yaml`, HEAD-validating every domain (SPEC §10). A normal connector run, so it writes a `fetch_runs` row |
| `refresh` | yes | one connector (`--connector NAME`) or every enabled one (`--all`), with `--since YYYY-MM-DD`. Together with the scheduler, the only place external data is fetched |
| `stats` | no | row counts, companies per metro, last successful run per connector, and companies and jobs added in the last 7 days |
| `merge-review` | no | resolves `merge_candidates` pairs — the rows the pipeline is forbidden to auto-merge |
| `sync-contacts` | no | rebuilds SPEC §6's *constructed* LinkedIn links across the whole database; writes no `fetch_runs` row, because nothing was fetched |
| `sync-regions` | no | relabels `locations` rows whose `metro`/`country` no longer match `config/regions.yaml`, for the companies nothing is re-ingesting; reports a city no enabled region claims rather than touching it, and never re-runs entity resolution ([the region model](#the-region-model)) |

`stats` never prints `DATABASE_URL` — `cli.database_label` parses it with `make_url` and keeps
only the database, host and (when there is one) port, because the URL carries a password. Log
lines go to stderr (`logging_config`), so stdout carries only the report, which is what makes it
greppable: a bare word at column 0 opens a section and its contents are indented under it.

### A note on `company_site` "on demand for newly added companies"

SPEC §7.2 lists `company_site` as "Weekly, Saturday 03:00, **and on-demand for newly added
companies**". There is no second scheduling mechanism in the code, and there should not be: a
second place that decides when something runs would break "`config/connectors.yaml` is the only
place a schedule is defined". What exists instead is `load_company_targets`, which orders the work
list `last_seen_at ASC NULLS FIRST, id` — **never-visited companies first** — so the Saturday run
reaches a newly added company before any company it has already seen, and
`options.max_companies_per_run` (40) round-robins the rest of the database instead of hammering
the same first N. The on-demand half is `cli.py refresh --connector company_site`, which is SPEC
§2's `--now` case. If you want a new company visited immediately, that is the command.

---

## Where a rule lives

The point of this table is to learn to edit YAML rather than Python.

| Rule | Lives in | Not in |
|---|---|---|
| When a connector runs (cron cadence, UTC; weekdays as **names**, see below) | `config/connectors.yaml` → `cadence` | `scheduler.py`, which contains no cron expression |
| Whether a connector runs at all | `config/connectors.yaml` → `enabled` | anywhere else; `refresh --connector NAME` overrides it on purpose |
| Per-host rate limit, per-run page budget | `config/connectors.yaml` → `rate_limit` | `ingest/http.py`, which only *enforces* it |
| Whether `robots.txt` is honoured | `config/connectors.yaml` → `respect_robots` | — |
| Connector-specific knobs (backfill months, feed URLs, page caps) | `config/connectors.yaml` → `options` | the connector, which validates but does not hard-code them |
| `role_family` / `employment_type` / `seniority` / `flexible_signal` keywords | `config/classifiers.yaml` | `ingest/classify.py`, which compiles them; the matched keyword is logged |
| Round-type keywords, `Person.role_type` keywords, and SPEC §6's people-search terms | `config/classifiers.yaml` → `funding`, `contacts.person_role_type`, `contacts.people_search_terms` | `ingest/contacts.py`, which only assembles the URL |
| Which metros exist, which cities are in each, each city's state, the other spellings a source may write a city as (`aliases`), and whether a metro is `enabled` at all | `config/regions.yaml` | everywhere else — SPEC §11 makes it the only place region-specific logic may live, and `tests/test_regions.py` takes every metro and city name in the file as a needle and fails the build if one appears as a literal in the source tree, comments and docstrings included ([the region model](#the-region-model)) |
| The bootstrap company list | `config/seed_companies.yaml` | — ATS tokens are never seeded; the connectors discover them |
| **Connector priority** (SPEC §8) | `ingest/pipeline.py` → `CONNECTOR_PRIORITY` | `config/connectors.yaml`, deliberately: priority is a correctness property of the merge rule, not an operational knob |
| Which connectors exist and in what order `--all` runs them | `ingest/connectors/__init__.py` → `all_connectors()` | config; a name with no implementation is listed in the YAML and marked unimplemented on `/runs` |
| Trigram threshold (0.85) | `ingest/normalize.py` → `TRIGRAM_THRESHOLD` | config — SPEC §8 fixes the number |
| Failure-streak threshold (3) | `db/queries.py` → `CONSECUTIVE_FAILURE_ALERT` | config — SPEC §7.2 fixes it |
| Staleness multiple (2×) | `web/routes/runs.py` → `STALE_CADENCE_MULTIPLE` | config — SPEC §9 fixes it |
| Page size (50) | `db/queries.py` → `PAGE_SIZE` | — |
| Contact email, database URL, cache directory, log level | environment / `.env` (`settings.py`) | any config file, since they are per-deployment secrets and paths |

Two of those rows are the ones people try to move most often.

**Connector priority is not configuration** because changing it changes which source's answer is
*true* for a field, and the provenance already recorded in `Company.field_provenance` was written
under the old ordering — a reordering does not retroactively re-decide those columns.

**Write weekdays as names.** APScheduler 3.x — the parser both `scheduler.py` and `/runs` use —
numbers weekdays Monday = 0, while crontab numbers them Sunday = 0. A numeric `0 3 * * 6` fires on
a *Sunday* here, a day away from what SPEC §7.2's table says, and re-reading the string tells you
nothing. `mon`..`sun` mean the same day under either convention; the header of
`config/connectors.yaml` says so, and `tests/test_scheduler.py` asserts each connector's next fire
lands on the day the SPEC names rather than merely re-reading the expression.

**The cadence table really is the only schedule.** A connector may still pace *itself* within a
run — `company_site`'s `min_revisit_days: 6` skips a site it visited on Sunday, and
`max_companies_per_run: 40` bounds the run — but note where those numbers live: in that
connector's `options` block, not in its Python. If you find yourself adding a `sleep`, a second
trigger, or a hard-coded "only if it has been N days" to a connector module, either it belongs in
`options` or it belongs in `cadence`, and it is worth working out which before writing it.
