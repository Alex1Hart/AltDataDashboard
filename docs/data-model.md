# Data model and research semantics

## Company insight grain

The target research product is a versioned company insight:

```text
ticker x as_of_date x signal_name x evidence_vintage
```

Each insight must link to reviewed company entities and source observations, preserve point-in-time
availability, state whether evidence is observed, company-linked, or inferred, and include
confidence and counterevidence. Aggregate port/trade data cannot satisfy the company-evidence
requirement by itself. See [the company insight strategy](company-insight-strategy.md) for the
output contract.

## Company entity graph

Registry version 2 contains three dated node types:

```text
issuer -> subsidiary
issuer or subsidiary -> facility
```

Each entity has a stable internal `entity_id`, name and aliases, geography, a validity interval,
evidence references, and optional dated external identifiers such as ticker, SEC CIK, LEI, UEI,
EPA FRS, or OSHA establishment ID. Each directed relationship records its type (`owns` or
`operates`), confidence, supported validity interval, and evidence.

The registry rejects duplicate entity and relationship IDs, duplicate external identifiers,
unknown evidence or endpoints, invalid facility relationships, cycles, and nodes that cannot be
reached from a covered issuer. `valid_from` means the earliest date supported by the cited
evidence; it must not be interpreted as an incorporation, acquisition, or facility-opening date
unless the source explicitly establishes that event.

## Trade flows

Current table: `trade_flows`

```text
month × port_code × commodity_code × country_code × source
```

Measures include total import value, vessel value and weight, and containerized vessel value and
weight. `trade_flows` contains the latest known value. `trade_flow_revisions` retains every
distinct value observed by PortWatch.

## Port operations

Current table: `port_operations`

```text
period_start × port_code × metric × source
```

The first adapter captures the Port of Los Angeles monthly loaded import, loaded export, total
loaded, empty, and total TEUs. Daily dwell, vessel, rail, and blank-sailing observations belong in
this domain but require separate source adapters and frequencies.

## Publication and revision timestamps

| Field | Meaning |
|---|---|
| `period_start` / `month` | Economic period represented by the observation |
| `publication_at` | Source-provided update time when available; otherwise ingestion time |
| `available_at` | First time the PortWatch pipeline could have used this vintage |
| `valid_from` | Time a revision became the current PortWatch value |
| `valid_until` | Time a later revision superseded it; null for the current vintage |
| `revision_number` | Monotonic revision within the natural key |
| `payload_sha256` | Link to the exact archived response body |

Backtests must filter on `available_at`, never only on the economic month. A forced backfill can
discover a changed upstream value; unchanged values are idempotent and do not create revisions.

## Ingestion audit grain

`ingestion_runs` records source, slice or entity dimensions, status, timestamps, record counts, and
errors. Trade runs use month/port/commodity/country; company evidence runs use `entity_id` and
`ticker`. This supports resumability: a trade slice is skipped only when the same complete set of
dimensions has a successful run, unless `--force` is supplied.

## Deterministic trade signals

Signals are calculated at:

```text
month × port_code × commodity_code
```

| Signal | Definition |
|---|---|
| `value_yoy` | Current containerized value divided by the value 12 months earlier, minus one |
| `value_3m_momentum` | Latest trailing-three-month value divided by the prior trailing three months, minus one |
| `value_24m_zscore` | Value relative to its trailing 24-month mean and sample standard deviation; requires 12 months |
| `country_hhi` | Sum of squared origin-country value shares; higher means more concentration |
| `unit_value_usd_per_kg` | Containerized value divided by containerized weight; mix proxy, not a pure price index |

Signals are derived from current validated observations. A point-in-time backtest should rebuild
them from the revision table using the appropriate `available_at` cutoff.

## Company exposure registry

The registry is code-reviewed YAML. Each mapping includes:

- ticker and company name;
- HS exposure and weight;
- demand/input/mixed direction;
- rationale;
- evidence IDs linked to dated HTTPS disclosures;
- analyst review date and confidence;
- explicit limitations.

The dashboard's `weighted_zscore` is a weighted average of matched latest commodity z-scores,
including optional port weights. It is an economic-exposure indicator. It is not evidence that a
company owned, shipped, or received any underlying cargo.

## SEC filing events

`sec_filings` stores recent submission metadata at:

```text
accession_number
```

Each filing is linked to the reviewed registry `entity_id` resolved from the response CIK. The
table preserves filing and report dates, the SEC acceptance timestamp, form, primary-document URL,
XBRL flags, raw payload hash, ingestion run, and pipeline availability time. Accession numbers are
globally unique and filing events are inserted idempotently.

## SEC Company Facts

`sec_company_facts` stores the latest numeric XBRL observation for each deterministic context key.
`fact_id` hashes the entity, concept, unit, reporting period, accession, fiscal context, form, and
frame. A different accession creates a distinct observation; a source correction to the same
context increments `revision_number` and closes the prior value in
`sec_company_fact_revisions`. The current and revision writes share one database transaction.

Facts from accessions present in the recent submissions response use the exact SEC acceptance
timestamp. Older facts use the filing date at midnight UTC and set `acceptance_is_estimated=true`.
Point-in-time research must use `accepted_at`/`publication_at` and retain that estimation flag.
The original decimal value is stored as text to avoid precision loss; dashboard queries may derive
a floating-point value for display.

## Federal prime contracts

ContractWatch deliberately keeps two economic grains separate. `federal_contract_awards` stores
the latest USAspending award snapshot at:

```text
generated_internal_award_id × source
```

Each award records its matched registry entity, ticker, recipient identifiers, match method, PIID,
agencies, NAICS/PSC, place of performance, dates, current cumulative obligation amount, outlays,
source URL, availability timestamp, and raw response hash. `federal_contract_award_revisions`
retains every changed snapshot with `valid_from` and `valid_until` for point-in-time analysis.

`federal_contract_transactions` stores the dated action history at:

```text
USAspending transaction ID × source
```

Each action links to its parent award and reviewed company entity. It records action date, signed
`federal_action_obligation_usd`, action type, modification number, description, publication and
availability timestamps, and raw response hash. Positive obligations add funding; negative values
are deobligations. `federal_contract_transaction_revisions` preserves corrections to a stable
transaction ID. Award and transaction changes are written in one database transaction, preventing
a partially refreshed company view.

Attribution is deterministic: UEI first, then a reviewed USAspending recipient ID, then an exact
punctuation-insensitive match to a reviewed entity name or alias active on the award's base
obligation date. Unresolved fuzzy-search results remain in the raw archive and are counted as
rejected; they do not enter either normalized table.

Primary ContractWatch signals are:

| Signal | Meaning |
|---|---|
| `ttm_net_obligations_usd` | Signed obligation actions in the trailing twelve months |
| `ttm_net_obligations_yoy` | TTM net actions versus the preceding twelve months |
| `ttm_gross_obligations_usd` | Positive TTM obligation actions before deobligations |
| `ttm_deobligations_usd` | Absolute value of negative TTM obligation actions |
| `ttm_modification_count` | TTM actions carrying a nonzero modification number |
| `transaction_coverage` | Share of matched current awards with ingested action history |
| `total_current_obligations_usd` | Sum of current cumulative award obligations |
| `agency_hhi` | Concentration of positive current award obligations by awarding agency |
| `next_12m_expiring_award_value_usd` | Current obligations on awards ending in the next year |

Current award inventory and transaction flow are not interchangeable. Neither is company revenue,
funded backlog, a contract ceiling, remaining contract value, or evidence of margin. See
[ContractWatch research semantics](contractwatch.md) for pitch use and limitations.

## Company hiring demand

`job_postings` stores the current state at:

```text
career source x source job ID
```

Each row links a first-party posting to the reviewed issuer/entity, raw payload, observed and
source dates, business line, function, seniority, strategic themes, and classification version.
`job_posting_locations` is a many-to-many child table so a multi-location posting remains one job
while contributing to each advertised geography. A deterministic locality match may link a
location to a reviewed facility node; unmatched locations remain explicit rather than guessed.

`job_posting_revisions` retains every meaningful content or status vintage. `job_events` records
the source's initial `baseline`, followed by `opened`, `updated`, `closed`, and `reopened` events.
Baseline rows do not count as new-posting signals. A job is not closed after one absence: its
`missing_snapshot_count` must reach the configured threshold, and the counter advances only after
a complete source snapshot. This protects the lifecycle from pagination and transient-site errors.

Primary HiringWatch signals are active postings, 28-day openings/reopenings and closures, net
posting change, median active age, business-line/function mix, strategic-theme mix, facility and
location concentration, remote share, and classification coverage. These measure advertised labor
demand. They do not measure filled roles, employee headcount, hiring spend, or management guidance.

## Industry labor benchmarks

`labor_market_observations` stores the latest official Census QWI observation at:

```text
quarter x geography x NAICS industry x metric x source
```

Metrics include beginning/ending employment, hires, separations, firm job gains/losses, stable-job
earnings, and payroll. `labor_market_observation_revisions` preserves corrections. QWI is a lagged
macro benchmark; it contextualizes company posting changes but cannot be synchronized as if both
series represented the same event or publication date.
