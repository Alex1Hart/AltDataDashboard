from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import httpx

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
