# Architecture decision record: MVP

## Product boundary

The MVP observes monthly industrial imports through Los Angeles and Long Beach. It does not
attempt direct importer identification. The v0.2 architecture adds resumable backfills, source
revision history, public Port of Los Angeles monthly operating statistics, deterministic signals,
and a reviewed company exposure registry. LLM-generated briefs remain downstream of these
validated layers.

## Components

| Component | Responsibility | Must not do |
|---|---|---|
| Source adapter | Authentication, HTTP policy, source response parsing | Calculate research signals |
| Domain model | Normalize types, units, names, and provenance | Depend on storage or UI |
| Validator | Enforce batch-level semantic contracts | Silently repair invalid source data |
| Repository | Persist raw payloads, observations, and run metadata | Contain source-specific parsing |
| Service | Orchestrate source → validate → archive → upsert | Embed dashboard logic |
| Dashboard | Query and display validated observations | Call upstream sources directly |

## Storage choice

DuckDB is the MVP store because it is reproducible, analytical, local, and requires no service
setup. The repository boundary allows PostgreSQL or object-storage adapters to be added later
without changing source clients or domain models.

Raw response bodies are stored as content-addressed blobs using SHA-256. Normalized observations
have a latest-value table and a complete revision-history table. Identical re-ingestion is idempotent;
changed values close the prior vintage and increment the revision number. Every write references
an ingestion run and preserves when the value became available to the pipeline.

## Failure policy

- Missing credentials fail before a network request.
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
