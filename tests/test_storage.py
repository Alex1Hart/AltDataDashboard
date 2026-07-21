from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from portwatch.ingestion.census import parse_census_payload
from portwatch.models import IngestionStatus, SourceName
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


def test_successful_trade_slice_matches_country_scope_exactly(tmp_path: Path) -> None:
    payload = FIXTURE_PATH.read_bytes()
    flow = parse_census_payload(json.loads(payload), requested_port_code="2704")[0]
    repository = DuckDBRepository(tmp_path / "country-scope.duckdb")
    repository.initialize()
    repository.start_run(
        "country-run",
        SourceName.CENSUS_PORT_HS,
        flow.ingested_at,
        period_start=flow.month,
        port_code=flow.port_code,
        commodity_code=flow.commodity_code,
        country_code=flow.country_code,
    )
    repository.finish_run(
        "country-run",
        status=IngestionStatus.SUCCEEDED,
        records_received=1,
        records_written=1,
    )

    assert repository.has_successful_trade_slice(
        source=flow.source,
        month=flow.month,
        port_code=flow.port_code,
        commodity_code=flow.commodity_code,
        country_code=flow.country_code,
    )
    assert not repository.has_successful_trade_slice(
        source=flow.source,
        month=flow.month,
        port_code=flow.port_code,
        commodity_code=flow.commodity_code,
        country_code=None,
    )


def test_trade_revision_write_rolls_back_as_one_transaction(tmp_path: Path) -> None:
    payload = FIXTURE_PATH.read_bytes()
    flow = parse_census_payload(json.loads(payload), requested_port_code="2704")[0]
    database_path = tmp_path / "revision-rollback.duckdb"
    repository = DuckDBRepository(database_path)
    repository.initialize()
    repository.upsert_trade_flows("run-1", [flow], payload_sha256="original")

    # Reserve revision 2 so the history insert fails after the current row is replaced.
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            INSERT INTO trade_flow_revisions
            SELECT month, port_code, port_name, commodity_code, commodity_description,
                   country_code, country_name, general_value_usd, vessel_value_usd,
                   vessel_weight_kg, containerized_value_usd, containerized_weight_kg,
                   source, source_updated_at, ingested_at, run_id, publication_at,
                   available_at, 2, 'reserved-revision', valid_from, valid_until
            FROM trade_flow_revisions
            WHERE revision_number = 1
            """
        )

    revised = flow.model_copy(
        update={
            "containerized_value_usd": flow.containerized_value_usd + Decimal("1"),
            "ingested_at": flow.ingested_at + timedelta(days=1),
        }
    )
    with pytest.raises(duckdb.ConstraintException):
        repository.upsert_trade_flows("run-2", [revised], payload_sha256="revised")

    assert repository.execute_scalar("SELECT revision_number FROM trade_flows") == 1
    assert repository.execute_scalar("SELECT payload_sha256 FROM trade_flows") == "original"
    assert (
        repository.execute_scalar(
            "SELECT COUNT(*) FROM trade_flow_revisions "
            "WHERE revision_number = 1 AND valid_until IS NULL"
        )
        == 1
    )
