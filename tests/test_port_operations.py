from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import httpx
import pytest

from portwatch.config import Settings
from portwatch.ingestion.port_of_la import (
    PortOfLosAngelesClient,
    parse_container_statistics_html,
)
from portwatch.models import IngestionStatus, PortMetricName
from portwatch.port_service import PortOperationsIngestionService
from portwatch.storage.duckdb import DuckDBRepository
from portwatch.validation import validate_port_operations

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "port_of_la_container_statistics.html"


def test_parse_and_validate_port_of_la_metrics() -> None:
    operations = parse_container_statistics_html(FIXTURE_PATH.read_text(encoding="utf-8"))

    validate_port_operations(operations)
    values = {operation.metric: operation.value for operation in operations}
    assert len(operations) == 5
    assert values[PortMetricName.LOADED_IMPORT_TEU] == Decimal("449370.25")
    assert values[PortMetricName.TOTAL_TEU] == Decimal("840164.50")


def test_port_service_archives_and_upserts_release(tmp_path: Path) -> None:
    html = FIXTURE_PATH.read_text(encoding="utf-8")
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html, request=request))
    repository = DuckDBRepository(tmp_path / "operations.duckdb")
    service = PortOperationsIngestionService(
        client=PortOfLosAngelesClient(Settings(), transport=transport),
        repository=repository,
    )

    first = service.ingest_latest()
    second = service.ingest_latest()

    assert first.status is IngestionStatus.SUCCEEDED
    assert first.records_written == 5
    assert second.records_written == 0
    assert repository.execute_scalar("SELECT COUNT(*) FROM port_operations") == 5
    assert repository.execute_scalar("SELECT COUNT(*) FROM port_operation_revisions") == 5


def test_port_revision_write_rolls_back_as_one_transaction(tmp_path: Path) -> None:
    operation = parse_container_statistics_html(FIXTURE_PATH.read_text(encoding="utf-8"))[0]
    database_path = tmp_path / "port-revision-rollback.duckdb"
    repository = DuckDBRepository(database_path)
    repository.initialize()
    repository.upsert_port_operations("run-1", [operation], payload_sha256="original")

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            INSERT INTO port_operation_revisions
            SELECT period_start, frequency, port_code, port_name, metric, value, unit,
                   source, source_url, source_published_at, ingested_at, run_id,
                   publication_at, available_at, 2, 'reserved-revision',
                   valid_from, valid_until
            FROM port_operation_revisions
            WHERE revision_number = 1
            """
        )

    revised = operation.model_copy(
        update={
            "value": operation.value + Decimal("1"),
            "ingested_at": operation.ingested_at + timedelta(days=1),
        }
    )
    with pytest.raises(duckdb.ConstraintException):
        repository.upsert_port_operations("run-2", [revised], payload_sha256="revised")

    assert repository.execute_scalar("SELECT revision_number FROM port_operations") == 1
    assert repository.execute_scalar("SELECT payload_sha256 FROM port_operations") == "original"
    assert (
        repository.execute_scalar(
            "SELECT COUNT(*) FROM port_operation_revisions "
            "WHERE revision_number = 1 AND valid_until IS NULL"
        )
        == 1
    )
