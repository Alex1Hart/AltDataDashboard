from __future__ import annotations

import pandas as pd
import streamlit as st

from portwatch.config import get_settings
from portwatch.dashboard.components import render_coverage_metrics
from portwatch.dashboard.data import DashboardData, build_dashboard_data
from portwatch.dashboard.pages.audit import render_pipeline_health, render_revisions
from portwatch.dashboard.pages.company import render_company_research
from portwatch.dashboard.pages.contracts import render_contracts_page
from portwatch.dashboard.pages.hiring import render_hiring_page
from portwatch.dashboard.pages.market import (
    render_market_overview,
    render_port_operations,
    render_trade_signals,
)
from portwatch.storage.duckdb import DuckDBRepository

st.set_page_config(page_title="PortWatch", page_icon="⚓", layout="wide")


@st.cache_resource
def get_repository() -> DuckDBRepository:
    """Initialize the schema once per dashboard process."""
    repository = DuckDBRepository(get_settings().database_path)
    repository.initialize()
    return repository


@st.cache_data(ttl=60)
def load_dashboard_data() -> DashboardData:
    settings = get_settings()
    return build_dashboard_data(
        get_repository(),
        company_registry_path=settings.company_registry_path,
    )


@st.cache_data(ttl=60)
def load_fact_history(
    entity_id: str,
    taxonomy: str,
    tag: str,
    unit: str,
) -> pd.DataFrame:
    return get_repository().sec_company_fact_history(
        entity_id=entity_id,
        taxonomy=taxonomy,
        tag=tag,
        unit=unit,
    )


def main() -> None:
    data = load_dashboard_data()
    settings = get_settings()

    st.title("Industrials Intelligence Platform")
    st.caption(
        "Company-centered hiring, contracts, filings, facilities, ports, and trade evidence "
        "with vintage-aware provenance"
    )
    render_coverage_metrics(data)

    (
        company_tab,
        hiring_tab,
        contracts_tab,
        overview_tab,
        signals_tab,
        operations_tab,
        revisions_tab,
        health_tab,
    ) = st.tabs(
        [
            "Company research",
            "HiringWatch",
            "ContractWatch",
            "Market overview",
            "Research signals",
            "Port operations",
            "Revisions",
            "Pipeline health",
        ]
    )

    with company_tab:
        render_company_research(data, load_fact_history=load_fact_history)
    with hiring_tab:
        render_hiring_page(data)
    with contracts_tab:
        render_contracts_page(data)
    with overview_tab:
        render_market_overview(
            data,
            census_api_key_configured=bool(settings.census_api_key),
        )
    with signals_tab:
        render_trade_signals(data)
    with operations_tab:
        render_port_operations(data)
    with revisions_tab:
        render_revisions(data)
    with health_tab:
        render_pipeline_health(data)


main()
