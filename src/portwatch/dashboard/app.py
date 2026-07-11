from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from portwatch.analytics.signals import compute_trade_signals, latest_trade_signals
from portwatch.config import get_settings
from portwatch.registry import (
    company_exposure_scores,
    load_company_registry,
    registry_exposures_frame,
)
from portwatch.storage.duckdb import DuckDBRepository

st.set_page_config(page_title="PortWatch", page_icon="⚓", layout="wide")


def _format_percent(value: Any) -> str:
    return "—" if pd.isna(value) else f"{float(value):.1%}"


def _format_number(value: Any) -> str:
    return "—" if pd.isna(value) else f"{float(value):.2f}"


@st.cache_data(ttl=60)
def load_data() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    settings = get_settings()
    repository = DuckDBRepository(settings.database_path)
    repository.initialize()
    flows = repository.trade_flow_summary()
    signals = compute_trade_signals(flows)
    registry = load_company_registry(settings.company_registry_path)
    return (
        flows,
        signals,
        repository.port_operations_summary(),
        company_exposure_scores(signals, registry),
        registry_exposures_frame(registry),
        repository.trade_flow_revisions(),
    )


flows, signals, operations, exposure_scores, exposure_registry, revisions = load_data()
settings = get_settings()
repository = DuckDBRepository(settings.database_path)
runs = repository.recent_runs()

st.title("PortWatch")
st.caption(
    "Observed U.S. port and industrial trade flows with vintage-aware provenance "
    "and explicitly inferred company exposure"
)

overview_tab, signals_tab, operations_tab, company_tab, revisions_tab, health_tab = st.tabs(
    [
        "Market overview",
        "Research signals",
        "Port operations",
        "Company exposure",
        "Revisions",
        "Pipeline health",
    ]
)

with overview_tab:
    if flows.empty:
        st.info("No Census observations have been ingested. Run the configured backfill first.")
    else:
        latest_month = flows["month"].max()
        latest = flows[flows["month"] == latest_month]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Latest trade month", str(latest_month)[:10])
        containerized_value_billions = latest["containerized_value_usd"].sum() / 1e9
        col2.metric("Containerized value", f"${containerized_value_billions:,.2f}B")
        col3.metric("Countries observed", f"{latest['country_name'].nunique():,}")
        col4.metric("Current revisions", f"{latest['revision_number'].max():,.0f}")

        by_commodity = (
            latest.groupby(["commodity_code", "commodity_description"], as_index=False)[
                "containerized_value_usd"
            ]
            .sum()
            .sort_values("containerized_value_usd", ascending=False)
        )
        st.plotly_chart(
            px.bar(
                by_commodity,
                x="commodity_code",
                y="containerized_value_usd",
                hover_data=["commodity_description"],
                labels={
                    "commodity_code": "HS commodity",
                    "containerized_value_usd": "Containerized value (USD)",
                },
                title="Latest containerized imports by industrial commodity",
            ),
            width="stretch",
        )

with signals_tab:
    if signals.empty:
        st.info("Signals require ingested trade-flow history.")
    else:
        port_options = sorted(signals["port_code"].astype(str).unique().tolist())
        commodity_options = sorted(signals["commodity_code"].astype(str).unique().tolist())
        selected_port = st.selectbox("Port", port_options)
        selected_commodity = st.selectbox("HS commodity", commodity_options)
        selected = signals[
            (signals["port_code"].astype(str) == selected_port)
            & (signals["commodity_code"].astype(str) == selected_commodity)
        ]
        latest_selected = latest_trade_signals(selected)
        if not latest_selected.empty:
            signal_row = latest_selected.iloc[0]
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("YoY", _format_percent(signal_row["value_yoy"]))
            col2.metric("3-month momentum", _format_percent(signal_row["value_3m_momentum"]))
            col3.metric("24-month z-score", _format_number(signal_row["value_24m_zscore"]))
            col4.metric("Country HHI", _format_number(signal_row["country_hhi"]))
        st.plotly_chart(
            px.line(
                selected,
                x="month",
                y="containerized_value_usd",
                title="Containerized import value",
            ),
            width="stretch",
        )
        st.dataframe(selected, width="stretch", hide_index=True)

with operations_tab:
    if operations.empty:
        st.info("Run `portwatch ingest port-la` to load the latest public port release.")
    else:
        st.plotly_chart(
            px.line(
                operations,
                x="period_start",
                y="value",
                color="metric",
                markers=True,
                title="Port of Los Angeles monthly TEU metrics",
            ),
            width="stretch",
        )
        st.dataframe(operations, width="stretch", hide_index=True)

with company_tab:
    st.warning(
        "Company scores are inferred economic exposure, not observed importer or "
        "shipment ownership."
    )
    st.subheader("Latest exposure signals")
    if exposure_scores.empty:
        st.info("Exposure scores require sufficient trade history for rolling z-scores.")
    else:
        st.dataframe(exposure_scores, width="stretch", hide_index=True)
    st.subheader("Reviewed exposure registry")
    st.dataframe(exposure_registry, width="stretch", hide_index=True)

with revisions_tab:
    st.caption(
        "`available_at` is the first time PortWatch could have used a vintage; "
        "`valid_until` closes it when a changed value arrives."
    )
    if revisions.empty:
        st.info("No observation vintages have been recorded.")
    else:
        st.dataframe(revisions, width="stretch", hide_index=True)

with health_tab:
    st.subheader("Recent ingestion runs")
    if runs.empty:
        st.info("No ingestion runs recorded yet.")
    else:
        st.dataframe(runs, width="stretch", hide_index=True)
    st.subheader("Provenance policy")
    st.markdown(
        "- **Observed:** retrieved from a named source and passed validation.\n"
        "- **Reported:** contained in a company, port, or regulator disclosure.\n"
        "- **Inferred:** deterministic or analyst-reviewed mapping; never shipment ownership."
    )
