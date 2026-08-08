from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from portwatch.ingestion.qwi import parse_qwi_payload
from portwatch.models import LaborMetricName
from portwatch.storage.duckdb import DuckDBRepository

FIXTURE = Path(__file__).parent / "fixtures" / "qwi_response.json"


def test_qwi_parser_and_revision_storage_are_idempotent(tmp_path: Path) -> None:
    observations, rejected = parse_qwi_payload(
        FIXTURE.read_bytes(),
        requested_year=2025,
        requested_quarter=1,
        requested_industry_code="333120",
        requested_geography_level="state",
        requested_geography_code="17",
        seasonally_adjusted=False,
        ingested_at=datetime(2026, 8, 7, tzinfo=UTC),
    )
    repository = DuckDBRepository(tmp_path / "qwi.duckdb")
    repository.initialize()

    first = repository.upsert_labor_market_observations(
        "run-1", observations, payload_sha256="payload"
    )
    second = repository.upsert_labor_market_observations(
        "run-2", observations, payload_sha256="payload"
    )

    assert rejected == 0
    assert len(observations) == 8
    assert observations[0].period_start == date(2025, 1, 1)
    assert observations[0].metric is LaborMetricName.EMPLOYMENT
    assert first == 8
    assert second == 0
    assert repository.source_counts()["labor_market_observations"] == 8
