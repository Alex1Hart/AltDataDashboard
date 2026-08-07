from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
from uuid import uuid4

from portwatch.ingestion.usaspending import (
    USAspendingClient,
    company_contract_search_terms,
)
from portwatch.models import IngestionResult, IngestionStatus, SourceName
from portwatch.storage.duckdb import DuckDBRepository


class FederalContractIngestionService:
    """Orchestrate audited USAspending ingestion for one reviewed company graph."""

    def __init__(
        self,
        *,
        client: USAspendingClient,
        repository: DuckDBRepository,
        progress_fn: Callable[[str], None] | None = None,
    ) -> None:
        self.client = client
        self.repository = repository
        self.progress_fn = progress_fn or (lambda message: None)

    def ingest_company(
        self,
        ticker: str,
        *,
        start_date: date,
        end_date: date,
    ) -> IngestionResult:
        run_id = str(uuid4())
        source = SourceName.USA_SPENDING_CONTRACT_AWARDS
        started_at = datetime.now(UTC)
        normalized_ticker = ticker.upper()
        self.repository.initialize()

        received = 0
        rejected = 0
        run_started = False
        entity_id: str | None = None
        try:
            entity_id, _ = company_contract_search_terms(
                self.client.registry,
                normalized_ticker,
                as_of=end_date,
            )
            self.repository.start_run(
                run_id,
                source,
                started_at,
                entity_id=entity_id,
                ticker=normalized_ticker,
                period_start=start_date,
                period_end=end_date,
            )
            run_started = True
            batch = self.client.fetch_company(
                normalized_ticker,
                start_date=start_date,
                end_date=end_date,
            )
            received = batch.records_received
            rejected = batch.records_unmatched

            payload_by_award: dict[str, str] = {}
            for page in batch.pages:
                self.repository.store_raw_payload(
                    run_id,
                    source,
                    page.request_payload,
                    resource_type="usaspending_contract_awards_request",
                    source_url=page.source_url,
                )
                response_hash = self.repository.store_raw_payload(
                    run_id,
                    source,
                    page.response_payload,
                    resource_type="usaspending_contract_awards_response",
                    source_url=page.source_url,
                )
                for award in page.awards:
                    payload_by_award.setdefault(award.award_key, response_hash)

            self.progress_fn(
                f"USAspending: {len(batch.awards):,} uniquely matched awards; "
                f"{rejected:,} candidates rejected by deterministic entity resolution"
            )
            written = self.repository.upsert_federal_contract_awards(
                run_id,
                list(batch.awards),
                payload_sha256_by_award=payload_by_award,
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
        except KeyboardInterrupt:
            if not run_started:
                self.repository.start_run(
                    run_id,
                    source,
                    started_at,
                    entity_id=entity_id,
                    ticker=normalized_ticker,
                    period_start=start_date,
                    period_end=end_date,
                )
            self.repository.finish_run(
                run_id,
                status=IngestionStatus.FAILED,
                records_received=received,
                records_written=0,
                records_rejected=rejected,
                error_message="USAspending ingestion interrupted by user",
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
                    period_start=start_date,
                    period_end=end_date,
                )
            completed_at = self.repository.finish_run(
                run_id,
                status=IngestionStatus.FAILED,
                records_received=received,
                records_written=0,
                records_rejected=rejected,
                error_message=str(exc)[:2000],
            )
            raise RuntimeError(
                f"USAspending ingestion run {run_id} failed at {completed_at.isoformat()}"
            ) from exc
