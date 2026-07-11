from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
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
    payload_hash = repository.store_raw_payload("run-1", SourceName.CENSUS_PORT_HS, payload)
    first_write = repository.upsert_trade_flows("run-1", flows, payload_sha256=payload_hash)
    second_write = repository.upsert_trade_flows("run-1", flows, payload_sha256=payload_hash)

    row_count = repository.execute_scalar("SELECT COUNT(*) FROM trade_flows")
    raw_count = repository.execute_scalar("SELECT COUNT(*) FROM raw_payloads")

    assert row_count == 2
    assert raw_count == 1
    assert first_write == 2
    assert second_write == 0
    assert repository.execute_scalar("SELECT COUNT(*) FROM trade_flow_revisions") == 2

    revised = flows[0].model_copy(
        update={
            "containerized_value_usd": Decimal("1320000100"),
            "ingested_at": flows[0].ingested_at + timedelta(days=1),
        }
    )
    revised_count = repository.upsert_trade_flows(
        "run-1",
        [revised],
        payload_sha256="revised-payload",
    )

    assert revised_count == 1
    assert repository.execute_scalar("SELECT MAX(revision_number) FROM trade_flows") == 2
    assert repository.execute_scalar("SELECT COUNT(*) FROM trade_flow_revisions") == 3
    assert (
        repository.execute_scalar(
            "SELECT COUNT(*) FROM trade_flow_revisions WHERE valid_until IS NOT NULL"
        )
        == 1
    )
