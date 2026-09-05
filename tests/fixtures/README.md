# Recorded HTTP fixtures

Every connector test replays these with `respx`; nothing in the suite touches the network (an
un-mocked request fails the test, and the whole suite passes under
`HTTPS_PROXY=http://127.0.0.1:9`). `sec_edgar/README.md` documents the Phase 2 fixtures; this
file covers the Phase 3 and Phase 5 ones, all recorded on **2026-09-04** with the User-Agent the
app sends, `startup-tracker/0.1 startup-tracker@example.com`, `robots.txt` checked first.

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

| file | source | the trap it encodes |
|---|---|---|
| `company_site/astranis_home.html`, `astranis_careers.html`, `astranis_robots.txt` | `astranis.com` | a home page whose only depth-1 match is `/careers`, and a careers page that links on to Greenhouse |
| `company_site/sourcegraph_home.html`, `sourcegraph_robots.txt` | `sourcegraph.com` | `og:description` and `description` both present; robots disallows only `/search?q=*` |
| `company_site/wordpress_crawl_delay_robots.txt` | `atom-computing.com/robots.txt` | a Yoast file with a `Crawl-delay: 10` line **before** any `User-agent`, which RFC 9309 says belongs to no group |

### Contacts, social links and people (SPEC §6, Phase 5)

Each of these is a **second trim of the same recording** — the Phase 3 rows above kept the metas
and depth-1 links, these keep the anchors SPEC §6 reads. `astranis_home.html` and
`sourcegraph_home.html` were extended in place; their metas and depth-1 links are untouched.

Two deviations from "unmodified", both deliberate. `sourcegraph_home.html`'s two depth-1 anchors
(`/about`, `/jobs`) kept the Phase 3 trim's placeholder body `link` rather than the recorded
`About` and `Careers`; every other anchor in these files is verbatim. And where a recording
repeats one anchor across several columns or breakpoints — `twelve.co` prints `/contact` in both
its footer and its mobile menu — only one copy is kept, since dedupe is exercised across *pages*
by the three `atom_computing_*` files rather than within one.

| file | source | the trap it encodes |
|---|---|---|
| `company_site/astranis_home.html` | `www.astranis.com/` | Webflow renders **no `<footer>` element at all** — the social column is a plain `<div class="social-link footernew">`, so extraction cannot look for a footer subtree. The LinkedIn href is `www.linkedin.com/company/astranis/` **with a trailing slash**: canonicalised it equals the URL SPEC §6 constructs from `astranis.com`, which is the case where a published row must *replace* the constructed one in place rather than sit beside it. Also `/contact` (a same-domain contact form), `twitter.com/Astranis`, Instagram and YouTube — and **no `mailto:` anywhere on the site** |
| `company_site/sourcegraph_home.html` | `sourcegraph.com/` | the real `<footer>`; each social anchor's only child is an `<svg>`, so the anchor text is **empty** and the account exists only in the href. LinkedIn is the numeric company id `www.linkedin.com/company/4803356/`, which can never equal the constructed `…/company/sourcegraph` — the published/constructed reconciliation case where the two values genuinely differ. Plus `github.com/sourcegraph` and `x.com/Sourcegraph`, and the pair `docs/SOURCES.md` states the contact-form rule with: the nav's `/contact/request-info` demo CTA comes first in the document and the footer's `/contact` second, so only a **shallowest**-wins rule picks `/contact` |
| `company_site/atom_computing_home.html` | `atom-computing.com/` | **no description meta of any kind**, so the one-liner falls back to the title. Every nav href is absolute with a trailing slash, so the depth-1 links are `…/about-us/` then `…/careers/` in that order — the two pages below are exactly what a three-page visit fetches. Carries the same footer social block as every other page on the site, which is how cross-page contact dedupe gets exercised |
| `company_site/atom_computing_careers.html` | `atom-computing.com/careers` | **`mailto:HR@atom-computing.com`** — an uppercase local part, the only published address on the whole site, and the reason addresses are lowercased before storage (`hn_hiring` writes them lowercase). The Oxygen builder emits **single-quoted attributes**, so a raw-markup `href="…"` regex finds none of the social links; the Twitter href carries a `?lang=en` query string; and every social anchor's visible text is `Visit our …` from an `<svg><title>`, never a name |
| `company_site/atom_computing_about.html` | `atom-computing.com/about-us/` | the people case, trimmed from 12 profile links to the first three Executive Leadership cards. **All three anchors have the same text, "Visit our LinkedIn"**, so the name must come from the card's `<h3>`; walking one ancestor too far up hands all three the `<h2>Executive Leadership</h2>` above them, which is why a name claimed by more than one profile URL is dropped as a heading. The name carries a trailing credential (`Ben Bloom, PhD`); the title is the **next sibling `<div>`**, not the `Read Bio` anchor that follows it, and contains a zero-width space (U+200B) plus, on the second card, `&nbsp;` and a `<br>`; `Read Bio` points at the literal href `http://`, a URL with no host |
| `company_site/twelve_home.html` | `twelve.co/` | `https://github.com/wix/yoshi/issues/2689` appears in a bundled Wix stylesheet's comment and **is not an anchor** — the regression case for parsing `<a href>` values instead of raw markup. Its LinkedIn href has both a `?viewAsMember=true` query string and a trailing slash, and the published slug `twelveco2` is one the domain `twelve.co` could never produce (`twelve`) — which also covers the `.co` TLD in the slug rule. Its second footer column supplies an **absolute** same-domain `/contact`, the counterpart to astranis's relative one |

Two things the recordings do **not** contain, so a test that needs them must use hand-written
markup and say so: a `mailto:?subject=…` share widget (none of the seven pages recorded on
2026-09-04 has one — `atom-computing.com/careers` holds the set's only `mailto:` at all), and a
`linkedin.com/in/` link outside a person card.

`atom-computing.com` sits behind Cloudflare and **sometimes answers 403** to this User-Agent —
it did while Phase 3 was recorded, which is why the seed loader treats a 403 as "the host is
alive" rather than as a dead domain (SPEC §10). The Phase 5 recording got through.
