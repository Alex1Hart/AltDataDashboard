from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

from portwatch.ingestion.census import CensusPortHSClient
from portwatch.models import IngestionResult, IngestionStatus, SourceName
from portwatch.storage.duckdb import DuckDBRepository
from portwatch.validation import validate_trade_flows


class IngestionService:
    def __init__(
        self,
        *,
        census_client: CensusPortHSClient,
        repository: DuckDBRepository,
    ) -> None:
        self.census_client = census_client
        self.repository = repository

    def ingest_census_month(
        self,
        *,
        month: date,
        port_code: str,
        commodity_code: str,
        country_code: str | None = None,
    ) -> IngestionResult:
        run_id = str(uuid4())
        started_at = datetime.now(UTC)
        source = SourceName.CENSUS_PORT_HS
        self.repository.initialize()
        self.repository.start_run(run_id, source, started_at)

        received = 0
        try:
            flows, raw_payload = self.census_client.fetch_month(
                month=month,
                port_code=port_code,
                commodity_code=commodity_code,
                country_code=country_code,
            )
            received = len(flows)
            validate_trade_flows(
                flows,
                expected_month=month,
                expected_port_code=port_code,
                expected_commodity_code=commodity_code,
            )
            self.repository.store_raw_payload(run_id, source, raw_payload)
            written = self.repository.upsert_trade_flows(run_id, flows)
            completed_at = self.repository.finish_run(
                run_id,
                status=IngestionStatus.SUCCEEDED,
                records_received=received,
                records_written=written,
            )
            return IngestionResult(
                run_id=run_id,
                source=source,
                status=IngestionStatus.SUCCEEDED,
                records_received=received,
                records_written=written,
                started_at=started_at,
                completed_at=completed_at,
            )
        except Exception as exc:
            completed_at = self.repository.finish_run(
                run_id,
                status=IngestionStatus.FAILED,
                records_received=received,
                records_written=0,
                error_message=str(exc)[:2000],
            )
            raise RuntimeError(
                f"Census ingestion run {run_id} failed at {completed_at.isoformat()}"
            ) from exc
