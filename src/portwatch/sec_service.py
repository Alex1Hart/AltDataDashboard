from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import uuid4

from portwatch.ingestion.sec import SecEdgarClient
from portwatch.models import IngestionResult, IngestionStatus, SourceName
from portwatch.storage.duckdb import DuckDBRepository


class SecEdgarIngestionService:
    """Orchestrate one registry-resolved SEC submissions and Company Facts ingestion."""

    def __init__(
        self,
        *,
        client: SecEdgarClient,
        repository: DuckDBRepository,
        progress_fn: Callable[[str], None] | None = None,
    ) -> None:
        self.client = client
        self.repository = repository
        self.progress_fn = progress_fn or (lambda message: None)

    def ingest_company(self, ticker: str) -> IngestionResult:
        run_id = str(uuid4())
        source = SourceName.SEC_EDGAR
        started_at = datetime.now(UTC)
        normalized_ticker = ticker.upper()
        self.repository.initialize()

        received = 0
        run_started = False
        entity_id: str | None = None
        try:
            entity_id, _ = self.client.registered_identity(normalized_ticker)
            self.repository.start_run(
                run_id,
                source,
                started_at,
                entity_id=entity_id,
                ticker=normalized_ticker,
            )
            run_started = True
            batch = self.client.fetch_company(normalized_ticker)
            received = len(batch.filings) + len(batch.facts)
            self.progress_fn(
                f"SEC: persisting {len(batch.filings):,} filings and "
                f"{len(batch.facts):,} normalized facts atomically"
            )
            submissions_hash = self.repository.store_raw_payload(
                run_id,
                source,
                batch.submissions_payload,
                resource_type="sec_submissions",
                source_url=batch.submissions_url,
            )
            company_facts_hash = self.repository.store_raw_payload(
                run_id,
                source,
                batch.company_facts_payload,
                resource_type="sec_company_facts",
                source_url=batch.company_facts_url,
            )
            written = self.repository.store_sec_edgar_batch(
                run_id,
                batch.filings,
                batch.facts,
                submissions_payload_sha256=submissions_hash,
                company_facts_payload_sha256=company_facts_hash,
            )
            self.progress_fn(f"SEC: persistence complete; {written:,} rows changed")
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
        except KeyboardInterrupt:
            if not run_started:
                self.repository.start_run(
                    run_id,
                    source,
                    started_at,
                    entity_id=entity_id,
                    ticker=normalized_ticker,
                )
            self.repository.finish_run(
                run_id,
                status=IngestionStatus.FAILED,
                records_received=received,
                records_written=0,
                error_message="SEC ingestion interrupted by user",
            )
            raise
        except Exception as exc:
            if not run_started:
                self.repository.start_run(
                    run_id,
                    source,
                    started_at,
                    entity_id=entity_id,
                    ticker=normalized_ticker,
                )
            completed_at = self.repository.finish_run(
                run_id,
                status=IngestionStatus.FAILED,
                records_received=received,
                records_written=0,
                error_message=str(exc)[:2000],
            )
            raise RuntimeError(
                f"SEC EDGAR ingestion run {run_id} failed at {completed_at.isoformat()}"
            ) from exc
