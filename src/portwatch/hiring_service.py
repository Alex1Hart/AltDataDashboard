from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from portwatch.hiring_classifier import classify_posting
from portwatch.hiring_config import HiringWatchConfig
from portwatch.ingestion.careers import CareerSourceClient
from portwatch.models import (
    CompanyExposureRegistry,
    IngestionResult,
    IngestionStatus,
    JobPosting,
    SourceName,
)
from portwatch.registry import company_entity_ids
from portwatch.storage.duckdb import DuckDBRepository


class HiringIngestionService:
    """Archive, classify, diff, and persist one complete company-career snapshot."""

    def __init__(
        self,
        *,
        client: CareerSourceClient,
        repository: DuckDBRepository,
        config: HiringWatchConfig,
        registry: CompanyExposureRegistry,
        progress_fn: Callable[[str], None] | None = None,
    ) -> None:
        self.client = client
        self.repository = repository
        self.config = config
        self.registry = registry
        self.progress_fn = progress_fn or (lambda message: None)

    def ingest(self) -> IngestionResult:
        run_id = str(uuid4())
        source = SourceName.COMPANY_CAREERS
        started_at = datetime.now(UTC)
        source_config = self.client.source
        self.repository.initialize()
        received = 0
        run_started = False
        try:
            company_entities = company_entity_ids(
                self.registry,
                source_config.ticker,
                as_of=started_at.date(),
            )
            if source_config.entity_id not in company_entities:
                raise ValueError(
                    f"career source entity {source_config.entity_id} is not active and reachable "
                    f"for {source_config.ticker}"
                )
            self.repository.start_run(
                run_id,
                source,
                started_at,
                entity_id=source_config.entity_id,
                ticker=source_config.ticker,
            )
            run_started = True
            snapshot = self.client.fetch_snapshot()
            received = len(snapshot.postings)
            enrichment_cache = self.repository.job_enrichment_cache(source_config.source_id)
            cache_cutoff = started_at - timedelta(days=source_config.detail_cache_ttl_days)
            reusable: list[JobPosting] = []
            needs_detail: list[JobPosting] = []
            reused_detail_count = 0
            for posting in snapshot.postings:
                if posting.description:
                    reusable.append(posting)
                    continue
                cached = enrichment_cache.get(posting.source_job_id)
                if cached is None:
                    reusable.append(posting)
                    needs_detail.append(posting)
                    continue
                detail_retrieved_at = cached.get("detail_retrieved_at")
                cached_fields = {
                    field: value
                    for field, value in cached.items()
                    if field != "detail_retrieved_at"
                }
                cached_posting = posting.model_copy(update=cached_fields)
                reusable.append(cached_posting)
                reused_detail_count += 1
                if detail_retrieved_at is None or detail_retrieved_at < cache_cutoff:
                    needs_detail.append(cached_posting)

            detail_batch = self.client.fetch_details(
                needs_detail,
                limit=source_config.detail_fetch_limit_per_run,
            )
            detailed_by_id = {posting.source_job_id: posting for posting in detail_batch.postings}
            normalized_postings = [
                detailed_by_id.get(posting.source_job_id, posting) for posting in reusable
            ]
            if source_config.detail_json_ld:
                self.progress_fn(
                    f"{source_config.source_id}: reused {reused_detail_count:,} cached details; "
                    f"fetched {len(detail_batch.postings):,}"
                )
            taxonomy = self.config.company(source_config.ticker)
            classified = [
                classify_posting(
                    posting,
                    taxonomy,
                    self.registry,
                    classification_version=self.config.version,
                )
                for posting in normalized_postings
            ]

            payload_by_job: dict[str, str] = {}
            detail_page_urls = {page.source_url for page in detail_batch.pages}
            pages = [*snapshot.pages, *detail_batch.pages]
            archive_requests = [
                (
                    page.raw_payload,
                    (
                        f"career_detail:{source_config.source_id}"
                        if page.source_url in detail_page_urls
                        else f"career_snapshot:{source_config.source_id}"
                    ),
                    page.source_url,
                )
                for page in pages
            ]
            payload_hashes = self.repository.store_raw_payload_batch(
                run_id,
                source,
                archive_requests,
            )
            for page, payload_hash in zip(pages, payload_hashes, strict=True):
                is_detail = page.source_url in detail_page_urls
                for posting in page.postings:
                    if is_detail:
                        payload_by_job[posting.source_job_id] = payload_hash
                    else:
                        payload_by_job.setdefault(posting.source_job_id, payload_hash)

            written = self.repository.apply_job_snapshot(
                run_id,
                source_id=source_config.source_id,
                observed_at=snapshot.observed_at,
                postings=classified,
                payload_sha256_by_job=payload_by_job,
                missing_snapshots_before_close=self.config.missing_snapshots_before_close,
            )
            self.progress_fn(
                f"{source_config.source_id}: {received:,} active jobs; "
                f"{written:,} lifecycle changes"
            )
            completed_at = self.repository.finish_run(
                run_id,
                status=IngestionStatus.SUCCEEDED,
                records_received=received,
                records_written=written,
                records_rejected=detail_batch.records_skipped,
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
                self.repository.start_run(
                    run_id,
                    source,
                    started_at,
                    entity_id=source_config.entity_id,
                    ticker=source_config.ticker,
                )
            completed_at = self.repository.finish_run(
                run_id,
                status=IngestionStatus.FAILED,
                records_received=received,
                records_written=0,
                error_message=str(exc)[:2000],
            )
            raise RuntimeError(
                f"career ingestion run {run_id} failed at {completed_at.isoformat()}"
            ) from exc
