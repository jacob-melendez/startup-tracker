# Recorded HTTP fixtures

Every connector test replays these with `respx`; nothing in the suite touches the network (an
un-mocked request fails the test, and the whole suite passes under
`HTTPS_PROXY=http://127.0.0.1:9`). `sec_edgar/README.md` documents the Phase 2 fixtures; this
file covers the Phase 3 ones, all recorded on **2026-09-04** with the User-Agent the app sends,
`startup-tracker/0.1 startup-tracker@example.com`.

Payloads are unmodified except where noted. Boards and feeds are **trimmed** — a live Greenhouse
board is 168 KB and Databricks' is 9.4 MB — by keeping a spread of role families and truncating
long description fields with a visible `[…trimmed for the fixture…]` marker. Careers and company
pages are trimmed to the elements that matter (the ATS board URL, the description metas, the
depth-1 links), with a comment in each file naming the source URL.

## Applicant tracking systems (SPEC §4 Tier 1 #3)

| file | source | what it is for |
|---|---|---|
| `greenhouse/board_sourcegraph91.json` | `boards-api.greenhouse.io/v1/boards/sourcegraph91/jobs?content=true` | 8 of Sourcegraph's postings across six role families |
| `greenhouse/board_404.json` | the same endpoint with an unknown token | the 404 body that triggers re-discovery |
| `lever/postings_atomcomputing.json` | `api.lever.co/v0/postings/atomcomputing?mode=json` | 8 Atom Computing postings, with `salaryRange` and `categories.commitment` |
| `lever/postings_404.json` | the same with an unknown token | |
| `ashby/board_linear.json` | `api.ashbyhq.com/posting-api/job-board/linear?includeCompensation=true` | 7 Linear postings, `employmentType`, `isRemote`, `publishedAt` |
| `ashby/board_unknown.json` | the same with an unknown token | Ashby 404s on a name it has never seen |
| `workable/account_zego.json` | `apply.workable.com/api/v1/widget/accounts/zego?details=true` | 7 postings; `published_on` is a **date**, not a timestamp |
| `workable/account_persona_empty.json` | the same for `persona` | **the trap**: Workable answers `200` with `jobs: []` for a name that was never its customer |
| `workable/account_404.txt` | the same for a nonsense name | `404` with the plain text `Not Found`, not JSON |
| `*/robots.txt` | each API host | Greenhouse disallows only `/embed/`; Lever allows all with `Crawl-delay: 1`; Workable's is a bare `Disallow:` (allow-all). `api.ashbyhq.com/robots.txt` answers 401 — unrestricted under RFC 9309 §2.3.1.3 — so there is nothing to record. |

## Careers pages — board-token discovery (SPEC §4)

| file | source | why it is here |
|---|---|---|
| `careers/atom_computing_lever.html` | `atom-computing.com/careers` | a plain `<a href="https://jobs.lever.co/atomcomputing">` |
| `careers/sourcegraph_greenhouse.html` | `sourcegraph.com/careers` | `job-boards.greenhouse.io/sourcegraph91` in per-job links |
| `careers/astranis_greenhouse_embed.html` | `astranis.com/careers` | the page carries both `boards.greenhouse.io/embed/job_board?for=…` and `job-boards.greenhouse.io/astranis` |
| `careers/sentry_ashby.html` | `sentry.io/careers/` | the board URL lives **entity-escaped inside an attribute** (an Astro island's `props`), which is why discovery matches raw markup rather than parsed `href`s. Hand-trimmed to two openings; the live page is 660 KB. |
| `careers/no_board.html` | shaped after `watershed.com/careers` | the board is rendered by JavaScript, so nothing is discoverable — `checkr.com` and `posthog.com` behave the same way. Discovery must give up cleanly. |

Note `jobs.ashbyhq.com/**Linear**` (capital L) on Sentry's neighbour page: Ashby's board tokens
are case-insensitive, and `…/job-board/linear` resolves.

## Y Combinator (SPEC §4 Tier 1 #2)

| file | source |
|---|---|
| `ycombinator/companies_page.html` | `www.ycombinator.com/companies`, trimmed to the element carrying the Algolia application id and **public search key**. The key rotates — the value older clients hard-coded now answers 403 — which is why the connector reads it at run time. `robots.txt` allows `/companies` (only `/companies?*` is disallowed). |
| `ycombinator/facets_batch.json` | the `batch` facet query, trimmed to three of the 50 values |
| `ycombinator/batch_winter_2024.json`, `batch_summer_2012.json` | one query per batch, trimmed to five Bay Area companies and three outside it |
| `ycombinator/algolia_robots_404.json` | the DSN host redirects `/robots.txt` into the REST API, which answers 404 — unrestricted under RFC 9309 §2.3.1.3 |

Facts the connector relies on, verified while recording: the index holds 6 200 companies;
Algolia's `paginationLimitedTo` is **1 000**, so a `"San Francisco"` query (3 141 hits) can never
be paged through, while the largest single `batch` facet value holds 398 — hence one query per
batch. `all_locations` is `"City, ST, USA"`, sometimes several offices joined by `;`.

## Hacker News "Who is hiring?" (SPEC §4 Tier 1 #4)

`hn_hiring/user_whoishiring.json` plus `item_*.json` — the September 2026 thread
(`item_49522897.json`) and 14 of its top-level comments, taken verbatim from
`hacker-news.firebaseio.com/v0/`. They were chosen for the shapes the parser has to survive:

* `item_49525748.json` — Discord, a textbook pipe header with a Bay Area city;
* `item_49559434.json` — SwingVision, **no pipes at all** (the name is in prose), the city is in
  the body, and its only links point at Deel's job boards rather than its own domain;
* `item_49524098.json` — a deleted comment;
* `item_49523835.json` — Fastly, whose header is wrapped in markdown asterisks;
* the rest are out-of-region comments that must be skipped and counted.

`hn_hiring/robots.txt` disallows `/` but allows `/*.json$`, which is exactly what the connector
fetches.

## Funding RSS (SPEC §4 Tier 2 #5)

`funding_rss/techcrunch_venture.xml` and `techcrunch_startups.xml`, each trimmed to 10 items,
plus `techcrunch.com/robots.txt`. Twenty real headlines of which two are funding announcements —
which is the point: the connector must skip the other eighteen rather than invent companies.

Axios Pro Rata (`axios.com/pro-rata.rss`) answers **403** to non-browser clients and Business
Wire's feed ids are per-subject rather than per-topic, so neither is a shipped default; both can
be added to `funding_rss.options.feeds` in `config/connectors.yaml`.

## Company sites — Tier 3 (SPEC §4 Tier 3)

| file | source |
|---|---|
| `company_site/astranis_home.html`, `astranis_careers.html`, `astranis_robots.txt` | `astranis.com` — a home page whose only depth-1 match is `/careers`, and a careers page that links on to Greenhouse |
| `company_site/sourcegraph_home.html`, `sourcegraph_robots.txt` | `sourcegraph.com` — `og:description` and `description` both present; robots disallows only `/search?q=*` |
| `company_site/wordpress_crawl_delay_robots.txt` | `atom-computing.com/robots.txt` — a Yoast file with a `Crawl-delay: 10` line **before** any `User-agent`, which RFC 9309 says belongs to no group |

`atom-computing.com` itself answers **403** to this User-Agent (Cloudflare), which is why the
seed loader treats a 403 as "the host is alive" rather than as a dead domain (SPEC §10).
