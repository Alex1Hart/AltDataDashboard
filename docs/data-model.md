# Data model and research semantics

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

`ingestion_runs` records source, slice dimensions, status, timestamps, record counts, and errors.
This supports resumability: a slice is skipped only when the same source/month/port/commodity has
a successful run, unless `--force` is supplied.

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

