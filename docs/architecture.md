# Architecture decision record: MVP

## Product boundary

PortWatch is a company-centered evidence system. Its primary analytical unit is a covered company;
ports, commodities, countries, and operating metrics provide contextual evidence. The system does
not infer importer ownership from aggregate cargo. A company conclusion requires a reviewed entity
relationship and at least one company-specific or company-linked observation.

The v0.3 foundation implements resumable port/trade ingestion, source revision history,
deterministic contextual signals, a reviewed entity graph, and registry-resolved SEC submissions
and Company Facts ingestion. The next milestone adds deterministic company signals and
evidence-gated company insight cards. LLM-generated briefs remain downstream of these validated
layers.

## Components

| Component | Responsibility | Must not do |
|---|---|---|
| Source adapter | Authentication, HTTP policy, source response parsing | Calculate research signals |
| Domain model | Normalize types, units, names, and provenance | Depend on storage or UI |
| Validator | Enforce batch-level semantic contracts | Silently repair invalid source data |
| Repository | Persist raw payloads, observations, and run metadata | Contain source-specific parsing |
| Service | Orchestrate source → validate → archive → upsert | Embed dashboard logic |
| Entity registry | Resolve issuers, subsidiaries, facilities, and source identifiers | Treat fuzzy matches as confirmed entities |
| Evidence engine | Join dated company evidence with contextual signals | Promote inferred evidence to observed evidence |
| Insight card | Present a falsifiable company claim, evidence, counterevidence, and confidence | Hide missing or stale inputs |
| Dashboard | Query and display validated observations | Call upstream sources directly |

## Storage choice

DuckDB is the MVP store because it is reproducible, analytical, local, and requires no service
setup. The repository boundary allows PostgreSQL or object-storage adapters to be added later
without changing source clients or domain models.

Raw response bodies are stored as content-addressed blobs using SHA-256. `raw_payload_links`
records every run-to-payload relationship, resource type, retrieval time, and source URL even when
multiple runs receive identical content. Normalized trade and port observations have a latest-value
table and a complete revision-history table. SEC accessions are inserted idempotently; Company
Facts source corrections create atomic revisions. Every write references an ingestion run and
preserves when the value became available to the pipeline.

## Failure policy

- Missing credentials fail before a network request.
- SEC requests require a declared contact email and are limited below ten requests per second.
- Timeouts and network errors receive bounded exponential-backoff retries.
- HTTP errors are not blindly retried; the run is recorded as failed.
- Schema drift and semantic violations fail the batch.
- A failed batch never writes normalized observations.
- Error messages are retained in the ingestion audit table.

## Provenance vocabulary

- **Observed:** obtained from a named data source and passed validation.
- **Reported:** contained in a company, port, or regulator disclosure.
- **Inferred:** calculated exposure or LLM-assisted classification.
- **Unknown:** insufficient evidence; no attribution is made.

## Planned deployment shape

The local MVP runs through Typer and Streamlit. A later production profile will package the same
components in Docker, schedule ingestion through GitHub Actions or a managed job runner, store raw
objects outside the analytical database, and expose dashboard-ready aggregates through a small
API. Those components are intentionally deferred until the data contracts have proven stable.
