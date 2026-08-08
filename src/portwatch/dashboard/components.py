from __future__ import annotations

import streamlit as st

from portwatch.dashboard.data import DashboardData


def render_coverage_metrics(data: DashboardData) -> None:
    columns = st.columns(7)
    columns[0].metric("Tracked companies", f"{data.exposure_registry['ticker'].nunique():,}")
    columns[1].metric("SEC filings", f"{data.source_counts['sec_filings']:,}")
    columns[2].metric("SEC facts", f"{data.source_counts['sec_company_facts']:,}")
    columns[3].metric(
        "Contract actions",
        f"{data.source_counts['federal_contract_transactions']:,}",
        help="Dated USAspending funding, modification, and deobligation actions.",
    )
    columns[4].metric("Active jobs", f"{data.source_counts['active_job_postings']:,}")
    columns[5].metric("Trade rows", f"{data.source_counts['trade_flows']:,}")
    columns[6].metric("Port observations", f"{data.source_counts['port_operations']:,}")
