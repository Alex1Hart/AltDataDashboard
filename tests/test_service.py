from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from portwatch.config import Settings
from portwatch.ingestion.census import CensusPortHSClient
from portwatch.models import IngestionStatus
from portwatch.service import IngestionService
from portwatch.storage.duckdb import DuckDBRepository

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "census_porths_response.json"


def test_service_records_successful_end_to_end_run(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE_PATH.read_bytes())
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    repository = DuckDBRepository(tmp_path / "success.duckdb")
    service = IngestionService(
        census_client=CensusPortHSClient(
            Settings(CENSUS_API_KEY="test-key"),
            transport=transport,
        ),
        repository=repository,
    )

    result = service.ingest_census_month(
        month=date(2026, 5, 1),
        port_code="2704",
        commodity_code="84",
    )

    assert result.status is IngestionStatus.SUCCEEDED
    assert result.records_received == 2
    assert result.records_written == 2
    assert repository.execute_scalar("SELECT COUNT(*) FROM trade_flows") == 2
    assert repository.execute_scalar("SELECT status FROM ingestion_runs") == "succeeded"
    assert repository.has_successful_trade_slice(
        source=result.source,
        month=date(2026, 5, 1),
        port_code="2704",
        commodity_code="84",
        country_code=None,
    )


def test_service_audits_failed_run(tmp_path: Path) -> None:
    repository = DuckDBRepository(tmp_path / "failure.duckdb")
    service = IngestionService(
        census_client=CensusPortHSClient(Settings(CENSUS_API_KEY=None)),
        repository=repository,
    )

    with pytest.raises(RuntimeError, match="ingestion run"):
        service.ingest_census_month(
            month=date(2026, 5, 1),
            port_code="2704",
            commodity_code="84",
        )

    run = repository.recent_runs(limit=1).iloc[0]
    assert run["status"] == "failed"
    assert "CENSUS_API_KEY is required" in run["error_message"]
