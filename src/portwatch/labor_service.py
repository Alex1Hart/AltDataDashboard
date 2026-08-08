from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from portwatch.ingestion.qwi import CensusQWIClient
from portwatch.models import IngestionResult, IngestionStatus, SourceName
from portwatch.storage.duckdb import DuckDBRepository


class LaborMarketIngestionService:
    def __init__(self, *, client: CensusQWIClient, repository: DuckDBRepository) -> None:
        self.client = client
        self.repository = repository

    def ingest_qwi(
        self,
        *,
        year: int,
        quarter: int,
        industry_code: str,
        geography_level: str,
        geography_code: str,
        seasonally_adjusted: bool = False,
    ) -> IngestionResult:
        run_id = str(uuid4())
        source = SourceName.CENSUS_QWI
        started_at = datetime.now(UTC)
        period_start = datetime(year, 1 + ((quarter - 1) * 3), 1, tzinfo=UTC).date()
        self.repository.initialize()
        received = 0
        rejected = 0
        run_started = False
        try:
            self.repository.start_run(run_id, source, started_at, period_start=period_start)
            run_started = True
            batch = self.client.fetch(
                year=year,
                quarter=quarter,
                industry_code=industry_code,
                geography_level=geography_level,
                geography_code=geography_code,
                seasonally_adjusted=seasonally_adjusted,
            )
            rejected = batch.records_rejected
            received = len(batch.observations) + rejected
            payload_hash = self.repository.store_raw_payload(
                run_id,
                source,
                batch.raw_payload,
                resource_type="census_qwi_response",
                source_url=batch.source_url,
            )
            written = self.repository.upsert_labor_market_observations(
                run_id,
                list(batch.observations),
                payload_sha256=payload_hash,
            )
            completed_at = self.repository.finish_run(
                run_id,
                status=IngestionStatus.SUCCEEDED,
                records_received=received,
                records_written=written,
                records_rejected=rejected,
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
            if not run_started:
                self.repository.start_run(run_id, source, started_at, period_start=period_start)
            completed_at = self.repository.finish_run(
                run_id,
                status=IngestionStatus.FAILED,
                records_received=received,
                records_written=0,
                records_rejected=rejected,
                error_message=str(exc)[:2000],
            )
            raise RuntimeError(
                f"Census QWI ingestion run {run_id} failed at {completed_at.isoformat()}"
            ) from exc
