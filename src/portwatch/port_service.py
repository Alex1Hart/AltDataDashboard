from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from portwatch.ingestion.port_of_la import PortOfLosAngelesClient
from portwatch.models import IngestionResult, IngestionStatus, SourceName
from portwatch.storage.duckdb import DuckDBRepository
from portwatch.validation import validate_port_operations


class PortOperationsIngestionService:
    def __init__(
        self,
        *,
        client: PortOfLosAngelesClient,
        repository: DuckDBRepository,
    ) -> None:
        self.client = client
        self.repository = repository

    def ingest_latest(self) -> IngestionResult:
        run_id = str(uuid4())
        source = SourceName.PORT_OF_LA_CONTAINER_STATS
        started_at = datetime.now(UTC)
        self.repository.initialize()

        received = 0
        run_started = False
        try:
            operations, raw_payload = self.client.fetch_latest()
            received = len(operations)
            validate_port_operations(operations)
            self.repository.start_run(
                run_id,
                source,
                started_at,
                period_start=operations[0].period_start,
                port_code=operations[0].port_code,
            )
            run_started = True
            payload_sha256 = self.repository.store_raw_payload(run_id, source, raw_payload)
            written = self.repository.upsert_port_operations(
                run_id,
                operations,
                payload_sha256=payload_sha256,
            )
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
            if not run_started:
                self.repository.start_run(run_id, source, started_at)
            completed_at = self.repository.finish_run(
                run_id,
                status=IngestionStatus.FAILED,
                records_received=received,
                records_written=0,
                error_message=str(exc)[:2000],
            )
            raise RuntimeError(
                f"Port operations ingestion run {run_id} failed at {completed_at.isoformat()}"
            ) from exc
