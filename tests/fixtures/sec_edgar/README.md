# Recorded SEC EDGAR fixtures

Recorded 2026-09-03 with `curl -A "startup-tracker/0.1 startup-tracker@example.com"` (the
User-Agent shape SEC accepts — see `ingest/http.py`). Public filings, unmodified except that the
two search responses are pretty-printed. Tests replay them with `respx`; nothing hits the live API.

## Full-text search (`https://efts.sec.gov/LATEST/search-index`)

| file | query |
|---|---|
| `search_palo_alto_2026-08.json` | `q="Palo Alto" -"Pooled Investment Fund"&forms=D&dateRange=custom&startdt=2026-08-01&enddt=2026-09-03` — 6 hits |
| `search_san_francisco_2026-08.json` | same for `"San Francisco"` — 65 hits, one page; two original + amendment pairs (file numbers `021-593951`, `021-595718`) |

Facts the connector relies on (verified while recording):

* `locationCode` / `locationType` are **ignored** by the API (the echoed `query` carries no
  location filter), hence one phrase query per city from `config/regions.yaml`.
* `q` supports phrase negation: `-"Pooled Investment Fund"` becomes a `must_not` clause.
* `q=*` returns nothing; omitting `q` returns every Form D nationally.
* 100 hits per page; `from=<offset>&page=<n>` paginates; `hits.total.value` is exact and capped
  at 10,000 per query, which is why the connector searches in date windows. (That is
  Elasticsearch's `track_total_hits` default: past the cap the API reports
  `{"value": 10000, "relation": "gte"}` — never a value above 10,000 — so the connector treats
  any `relation` other than `eq` as an overflowing window.)
* Hits come back in **relevance-score order, not by date**: in `search_san_francisco_2026-08.json`
  the D/A for file number `021-593951` (index 33, filed 2026-08-13) precedes its original Form D
  (index 41, filed 2026-08-12), while `021-595718` happens to come out original-first (both filed
  2026-08-28). The connector sorts each window's hits by filing date before fetching.
* `_source.biz_locations` is `["City, ST"]` for the issuer's business address;
  `_source.ciks[0]` and `_source.adsh` build the filing URL
  `https://www.sec.gov/Archives/edgar/data/{cik as int}/{adsh without dashes}/primary_doc.xml`
  (the accession prefix may be a filing agent's CIK, e.g. Teal Health — the issuer CIK still works).
* A phrase hit can come from a *related person's* address — Lovable Labs (Boston) matched
  "Palo Alto" — so the issuer city is checked again from the XML.

## Form D primary documents

| file | why it is here |
|---|---|
| `obsidian_security_D.xml` | plain Palo Alto startup: equity, `overFiveYears`, execs + directors |
| `kea_cloud_DA.xml` | amendment (`D/A`, `isAmendment=true`, `previousAccessionNumber`) sharing file number `021-549965` |
| `festimo_D.xml` | San Jose; `isOtherType` with `descriptionOfOtherType` "Simple Agreement for Future Equity (SAFE)"; `totalOfferingAmount` = `Indefinite`; `withinFiveYears` + `value` 2026 |
| `accel_growth_fund_8_D.xml` | Palo Alto **Pooled Investment Fund** — must be skipped |
| `accel_core_DA.xml` | pooled fund amendment |
| `lovable_labs_D.xml` | issuer in Boston, MA; related persons in Palo Alto / Menlo Park — must be dropped |
| `whatnot_D.xml` | `Promoter` relationships, $547M |
| `databricks_D_1.xml`, `databricks_D_2.xml` | two filings by one issuer on one day, different file numbers (two rounds); `isBusinessCombinationTransaction=true` on the first; `06c` exemption on the second |
| `teal_health_D.xml` | accession number prefixed by a filing agent's CIK; industry group `Other` |
| `coverbase_D.xml` | `relationshipClarification` values ("President, CEO, and Director") |
| `sylvan_labs_D.xml`, `aerdos_labs_D.xml`, `vitalis_ai_D.xml` | ordinary San Francisco startups (revenue range, 2025/2026 incorporation) |

## robots.txt

* `www_sec_gov_robots.txt` — the live file; `Allow: /Archives/edgar/data`, no `Crawl-delay`.
* `efts_sec_gov_robots_403.json` — `efts.sec.gov/robots.txt` answers 403, which RFC 9309 treats
  as "no restrictions".
