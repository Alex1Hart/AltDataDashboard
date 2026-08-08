from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from portwatch.analytics.contracts import compute_contract_company_signals
from portwatch.analytics.signals import compute_trade_signals
from portwatch.registry import (
    company_exposure_scores,
    load_company_registry,
    registry_entities_frame,
    registry_exposures_frame,
    registry_identifiers_frame,
    registry_relationships_frame,
)
from portwatch.storage.duckdb import DuckDBRepository


@dataclass(frozen=True)
class DashboardData:
    """Named dashboard datasets; avoids fragile, position-dependent tuple unpacking."""

    flows: pd.DataFrame
    trade_signals: pd.DataFrame
    operations: pd.DataFrame
    exposure_scores: pd.DataFrame
    exposure_registry: pd.DataFrame
    entity_registry: pd.DataFrame
    entity_identifiers: pd.DataFrame
    entity_relationships: pd.DataFrame
    sec_filings: pd.DataFrame
    sec_fact_catalog: pd.DataFrame
    contract_awards: pd.DataFrame
    contract_transactions: pd.DataFrame
    contract_signals: pd.DataFrame
    contract_revisions: pd.DataFrame
    contract_transaction_revisions: pd.DataFrame
    trade_revisions: pd.DataFrame
    job_postings: pd.DataFrame
    job_events: pd.DataFrame
    job_locations: pd.DataFrame
    labor_market: pd.DataFrame
    job_revisions: pd.DataFrame
    ingestion_runs: pd.DataFrame
    source_counts: dict[str, int]


def build_dashboard_data(
    repository: DuckDBRepository,
    *,
    company_registry_path: Path,
) -> DashboardData:
    """Load one consistent dashboard data bundle from the analytical store."""
    flows = repository.trade_flow_summary()
    trade_signals = compute_trade_signals(flows)
    registry = load_company_registry(company_registry_path)
    contract_awards = repository.federal_contract_awards_summary()
    contract_transactions = repository.federal_contract_transactions_summary()
    return DashboardData(
        flows=flows,
        trade_signals=trade_signals,
        operations=repository.port_operations_summary(),
        exposure_scores=company_exposure_scores(trade_signals, registry),
        exposure_registry=registry_exposures_frame(registry),
        entity_registry=registry_entities_frame(registry),
        entity_identifiers=registry_identifiers_frame(registry),
        entity_relationships=registry_relationships_frame(registry),
        sec_filings=repository.sec_filings_summary(forms=("10-K", "10-Q", "8-K")),
        sec_fact_catalog=repository.sec_company_fact_catalog(),
        contract_awards=contract_awards,
        contract_transactions=contract_transactions,
        contract_signals=compute_contract_company_signals(
            contract_awards,
            contract_transactions,
        ),
        contract_revisions=repository.federal_contract_award_revisions(),
        contract_transaction_revisions=(repository.federal_contract_transaction_revisions()),
        trade_revisions=repository.trade_flow_revisions(),
        job_postings=repository.job_postings_summary(),
        job_events=repository.job_events_summary(limit=100_000),
        job_locations=repository.job_locations_summary(),
        labor_market=repository.labor_market_summary(),
        job_revisions=repository.job_posting_revisions(),
        ingestion_runs=repository.recent_runs(),
        source_counts=repository.source_counts(),
    )
