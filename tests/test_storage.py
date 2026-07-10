from __future__ import annotations

import json
from pathlib import Path

from portwatch.ingestion.census import parse_census_payload
from portwatch.models import SourceName
from portwatch.storage.duckdb import DuckDBRepository

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "census_porths_response.json"


def test_repository_upserts_idempotently(tmp_path: Path) -> None:
    payload = FIXTURE_PATH.read_bytes()
    flows = parse_census_payload(json.loads(payload), requested_port_code="2704")
    repository = DuckDBRepository(tmp_path / "portwatch.duckdb")
    repository.initialize()

    repository.start_run("run-1", SourceName.CENSUS_PORT_HS, flows[0].ingested_at)
    repository.store_raw_payload("run-1", SourceName.CENSUS_PORT_HS, payload)
    repository.upsert_trade_flows("run-1", flows)
    repository.upsert_trade_flows("run-1", flows)

    row_count = repository.execute_scalar("SELECT COUNT(*) FROM trade_flows")
    raw_count = repository.execute_scalar("SELECT COUNT(*) FROM raw_payloads")

    assert row_count == 2
    assert raw_count == 1
