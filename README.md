# PortWatch

PortWatch is an alternative-data pipeline and research dashboard for monitoring U.S. port
conditions and industrial trade flows. It combines validated public data with explicit
provenance so an analyst can separate observed cargo movements from inferred public-company
exposure.

> **Project status:** v0.2 research pipeline. PortWatch ingests monthly Census imports and public
> Port of Los Angeles container statistics into a vintage-aware DuckDB store, calculates
> deterministic signals, and maps them to explicitly inferred company exposure.

## Why this exists

Traditional macro releases describe trade at a high level. PortWatch is designed to answer
more specific research questions:

- What industrial goods are moving through a port?
- Which origin countries drive the change?
- Is containerized value or weight accelerating or declining?
- Are port operating conditions consistent with the trade signal?
- Which covered companies may be exposed, and what evidence supports that inference?

PortWatch never presents an inferred company exposure as observed shipment ownership.

## Current architecture

```mermaid
flowchart LR
    A["Census International Trade API"] --> B["Source adapter"]
    B --> C["Pydantic normalization"]
    C --> D["Semantic validation"]
    D --> E["Raw payload archive"]
    D --> F["DuckDB analytical tables"]
    F --> G["Streamlit dashboard"]
    F --> H["Research and LLM layer — planned"]
```

The ingestion path includes bounded retries for transient network errors, request timeouts,
source-specific validation, resumable configuration-driven backfills, raw response hashing,
revision history, and success/failure run audits.

See [the project structure](docs/project-structure.md), [data model](docs/data-model.md), and
[architecture notes](docs/architecture.md) for component boundaries and research semantics.

## Data grain

The normalized `trade_flows` table is monthly:

```text
month × U.S. port × HS commodity × origin country × source
```

Measures currently include total import value, vessel value and weight, and containerized
vessel value and weight. The source is the official
[Census International Trade API](https://www.census.gov/data/developers/data-sets/international-trade.html).

## Quick start

Prerequisites:

- Python 3.12 or newer
- A free [Census API key](https://api.census.gov/data/key_signup.html)

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
cp .env.example .env       # Windows PowerShell: Copy-Item .env.example .env
```

Add the Census key to `.env`, then initialize the database:

```bash
portwatch init-db
```

Ingest industrial machinery for Los Angeles and Long Beach for one month:

```bash
portwatch ingest census --month 2026-05 --port 2704 --commodity 84
portwatch ingest census --month 2026-05 --port 2709 --commodity 84
```

Launch the dashboard:

```bash
portwatch dashboard
```

Run the full configured backfill and latest Port of Los Angeles release:

```bash
portwatch backfill --config config/portwatch.yml
portwatch ingest port-la
```

Schedule D port codes used in the initial scope:

| Port | Code |
|---|---:|
| Los Angeles, CA | `2704` |
| Long Beach, CA | `2709` |

## Development

```bash
ruff check .
ruff format --check .
pytest --cov=portwatch --cov-report=term-missing
mypy src/portwatch
```

Tests use source-shaped fixtures and mocked HTTP transports. They do not call live services or
require credentials.

## Roadmap

- [x] Typed Census port/HS ingestion adapter
- [x] Semantic data contracts and ingestion audit table
- [x] Idempotent DuckDB storage and dashboard shell
- [x] Resumable backfill orchestration for the Industrials HS universe
- [x] Publication-aware current values and complete revision history
- [x] Port of Los Angeles monthly public container-statistics adapter
- [x] Commodity momentum, concentration, z-score, and unit-value signals
- [x] Evidence-backed public-company exposure registry structure
- [ ] Daily dwell, vessel, rail, and blank-sailing adapters
- [ ] Long Beach public operating-report adapter
- [ ] Grounded research copilot with citations
- [ ] Containerized deployment and scheduled ingestion

## Data and research limitations

Census data identify commodity, country, port, value, weight, and mode—not the beneficial cargo
owner. Carrier schedules and aggregate port reports cannot be safely joined to a commodity flow
as if they were shipment-level bills of lading. Any later company exposure model will therefore
be labeled as inferred unless a licensed shipment-level source supports the attribution.

This project is for research and education and is not investment advice.

## License

MIT
