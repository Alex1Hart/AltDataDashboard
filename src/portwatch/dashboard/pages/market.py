from __future__ import annotations

import plotly.express as px
import streamlit as st

from portwatch.analytics.signals import latest_trade_signals
from portwatch.dashboard.data import DashboardData
from portwatch.dashboard.formatters import format_number, format_percent


def render_market_overview(data: DashboardData, *, census_api_key_configured: bool) -> None:
    if data.flows.empty:
        st.info(
            "SEC company evidence is loaded, but the market overview requires Census "
            "trade-flow history. Add a Census API key to `.env`, then run:"
        )
        st.code("portwatch backfill --config config/portwatch.yml", language="powershell")
        if not census_api_key_configured:
            st.warning("`CENSUS_API_KEY` is currently blank.")
        return

    latest_month = data.flows["month"].max()
    latest = data.flows[data.flows["month"] == latest_month]
    columns = st.columns(4)
    columns[0].metric("Latest trade month", str(latest_month)[:10])
    columns[1].metric(
        "Containerized value",
        f"${latest['containerized_value_usd'].sum() / 1e9:,.2f}B",
    )
    columns[2].metric("Countries observed", f"{latest['country_name'].nunique():,}")
    columns[3].metric("Current revisions", f"{latest['revision_number'].max():,.0f}")

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


def render_trade_signals(data: DashboardData) -> None:
    if data.trade_signals.empty:
        st.info("Signals require ingested trade-flow history.")
        return
    port = st.selectbox(
        "Port",
        sorted(data.trade_signals["port_code"].astype(str).unique().tolist()),
    )
    commodity = st.selectbox(
        "HS commodity",
        sorted(data.trade_signals["commodity_code"].astype(str).unique().tolist()),
    )
    selected = data.trade_signals[
        (data.trade_signals["port_code"].astype(str) == port)
        & (data.trade_signals["commodity_code"].astype(str) == commodity)
    ]
    latest = latest_trade_signals(selected)
    if not latest.empty:
        signal = latest.iloc[0]
        columns = st.columns(4)
        columns[0].metric("YoY", format_percent(signal["value_yoy"]))
        columns[1].metric("3-month momentum", format_percent(signal["value_3m_momentum"]))
        columns[2].metric("24-month z-score", format_number(signal["value_24m_zscore"]))
        columns[3].metric("Country HHI", format_number(signal["country_hhi"]))
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


def render_port_operations(data: DashboardData) -> None:
    if data.operations.empty:
        st.info("Run `portwatch ingest port-la` to load the latest public port release.")
        return
    st.plotly_chart(
        px.line(
            data.operations,
            x="period_start",
            y="value",
            color="metric",
            markers=True,
            title="Port of Los Angeles monthly TEU metrics",
        ),
        width="stretch",
    )
    st.dataframe(data.operations, width="stretch", hide_index=True)
