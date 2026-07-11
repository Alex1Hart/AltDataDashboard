from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from time import sleep

from portwatch.models import SourceName
from portwatch.project_config import PortWatchProjectConfig, iter_months
from portwatch.service import IngestionService
from portwatch.storage.duckdb import DuckDBRepository


@dataclass(frozen=True)
class BackfillSlice:
    month: date
    port_code: str
    commodity_code: str


@dataclass(frozen=True)
class BackfillSummary:
    planned: int
    succeeded: int
    skipped: int
    failed: int


class BackfillService:
    """Runs a deterministic, resumable Cartesian product of configured data slices."""

    def __init__(
        self,
        *,
        ingestion_service: IngestionService,
        repository: DuckDBRepository,
        sleep_fn: Callable[[float], None] = sleep,
    ) -> None:
        self.ingestion_service = ingestion_service
        self.repository = repository
        self.sleep_fn = sleep_fn

    def build_slices(
        self,
        config: PortWatchProjectConfig,
        *,
        today: date | None = None,
    ) -> list[BackfillSlice]:
        end_month = config.backfill.resolved_end_month(today)
        return [
            BackfillSlice(month=month, port_code=port.code, commodity_code=commodity.code)
            for month in iter_months(config.backfill.start_month, end_month)
            for port in config.ports
            for commodity in config.commodities
        ]

    def run(
        self,
        config: PortWatchProjectConfig,
        *,
        today: date | None = None,
        force: bool = False,
    ) -> BackfillSummary:
        self.repository.initialize()
        slices = self.build_slices(config, today=today)
        succeeded = skipped = failed = 0

        for position, item in enumerate(slices):
            if not force and self.repository.has_successful_trade_slice(
                source=SourceName.CENSUS_PORT_HS,
                month=item.month,
                port_code=item.port_code,
                commodity_code=item.commodity_code,
            ):
                skipped += 1
                continue

            try:
                self.ingestion_service.ingest_census_month(
                    month=item.month,
                    port_code=item.port_code,
                    commodity_code=item.commodity_code,
                )
                succeeded += 1
            except RuntimeError:
                failed += 1
                if not config.backfill.continue_on_error:
                    raise
            finally:
                if position < len(slices) - 1 and config.backfill.request_delay_seconds:
                    self.sleep_fn(config.backfill.request_delay_seconds)

        return BackfillSummary(
            planned=len(slices),
            succeeded=succeeded,
            skipped=skipped,
            failed=failed,
        )
