from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from portwatch.config import get_settings
from portwatch.dashboard.data import build_dashboard_data
from portwatch.dashboard.formatters import (
    format_fact_value,
    format_money,
    format_number,
    format_percent,
)
from portwatch.dashboard.pages.company import prepare_fact_history
from portwatch.storage.duckdb import DuckDBRepository

REGISTRY = Path("config/company_exposures.yml")


def test_dashboard_formatters_handle_missing_and_scaled_values() -> None:
    assert format_percent(0.125) == "12.5%"
    assert format_number(float("nan")) == "—"
    assert format_money(2_500_000_000) == "$2.50B"
    assert format_fact_value(3_500_000, "shares") == "3.5M shares"


def test_dashboard_data_bundle_has_named_empty_store_frames(tmp_path: Path) -> None:
    repository = DuckDBRepository(tmp_path / "dashboard-data.duckdb")
    repository.initialize()

    data = build_dashboard_data(repository, company_registry_path=REGISTRY)

    assert data.job_postings.empty
    assert data.contract_awards.empty
    assert data.contract_transactions.empty
    assert data.contract_transaction_revisions.empty
    assert not data.entity_registry.empty
    assert data.source_counts["active_job_postings"] == 0


def test_prepare_fact_history_deduplicates_point_in_time_observations() -> None:
    frame = pd.DataFrame(
        [
            {
                "form": "10-K",
                "fiscal_period": "FY",
                "period_start": "2025-01-01",
                "period_end": "2025-12-31",
                "value_numeric": 10.0,
                "accepted_at": datetime(2026, 1, 1, tzinfo=UTC),
            },
            {
                "form": "10-K",
                "fiscal_period": "FY",
                "period_start": "2025-01-01",
                "period_end": "2025-12-31",
                "value_numeric": 11.0,
                "accepted_at": datetime(2026, 2, 1, tzinfo=UTC),
            },
            {
                "form": "10-Q",
                "fiscal_period": "Q1",
                "period_start": "2026-01-01",
                "period_end": "2026-03-31",
                "value_numeric": 3.0,
                "accepted_at": datetime(2026, 4, 1, tzinfo=UTC),
            },
        ]
    )

    annual = prepare_fact_history(frame, "Annual (10-K)")
    quarterly = prepare_fact_history(frame, "Quarterly (10-Q)")

    assert annual["value_numeric"].tolist() == [11.0]
    assert quarterly["value_numeric"].tolist() == [3.0]


def test_streamlit_dashboard_smoke_test(tmp_path: Path, monkeypatch: object) -> None:
    database_path = tmp_path / "dashboard-smoke.duckdb"
    monkeypatch.setenv("PORTWATCH_DATABASE_PATH", str(database_path))
    get_settings.cache_clear()
    try:
        app = AppTest.from_file("src/portwatch/dashboard/app.py").run(timeout=30)
    finally:
        get_settings.cache_clear()

    assert not app.exception
    assert app.title[0].value == "Industrials Intelligence Platform"
    assert [tab.label for tab in app.tabs] == [
        "Company research",
        "HiringWatch",
        "ContractWatch",
        "Market overview",
        "Research signals",
        "Port operations",
        "Revisions",
        "Pipeline health",
    ]
