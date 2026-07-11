from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import Mock

from portwatch.backfill import BackfillService
from portwatch.models import IngestionStatus, SourceName
from portwatch.project_config import load_project_config


def test_project_config_resolves_lag_and_builds_cartesian_slices() -> None:
    project = load_project_config(Path("config/portwatch.yml"))
    service = BackfillService(
        ingestion_service=Mock(),
        repository=Mock(),
        sleep_fn=lambda seconds: None,
    )

    slices = service.build_slices(project, today=date(2026, 7, 10))

    assert project.backfill.resolved_end_month(date(2026, 7, 10)) == date(2026, 5, 1)
    assert slices[0].month == date(2024, 1, 1)
    assert slices[-1].month == date(2026, 5, 1)
    assert len(slices) == 29 * 2 * 9


def test_backfill_skips_successful_slices_and_runs_remaining_work() -> None:
    project = load_project_config(Path("config/portwatch.yml"))
    one_month = project.backfill.model_copy(
        update={
            "start_month": date(2026, 5, 1),
            "end_month": date(2026, 5, 1),
            "request_delay_seconds": 0,
        }
    )
    project = project.model_copy(
        update={
            "backfill": one_month,
            "commodities": project.commodities[:1],
        }
    )
    ingestion_service = Mock()
    ingestion_service.ingest_census_month.return_value = Mock(status=IngestionStatus.SUCCEEDED)
    repository = Mock()
    repository.has_successful_trade_slice.side_effect = [True, False]
    service = BackfillService(
        ingestion_service=ingestion_service,
        repository=repository,
        sleep_fn=lambda seconds: None,
    )

    summary = service.run(project)

    assert summary.planned == 2
    assert summary.skipped == 1
    assert summary.succeeded == 1
    assert summary.failed == 0
    ingestion_service.ingest_census_month.assert_called_once_with(
        month=date(2026, 5, 1),
        port_code="2709",
        commodity_code="72",
    )
    repository.has_successful_trade_slice.assert_any_call(
        source=SourceName.CENSUS_PORT_HS,
        month=date(2026, 5, 1),
        port_code="2704",
        commodity_code="72",
    )
