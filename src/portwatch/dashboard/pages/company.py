from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import plotly.express as px
import streamlit as st

from portwatch.dashboard.data import DashboardData
from portwatch.dashboard.formatters import format_fact_value

FactHistoryLoader = Callable[[str, str, str, str], pd.DataFrame]

FACT_PRIORITY = {
    "Revenues": 0,
    "OperatingIncomeLoss": 1,
    "InventoryNet": 2,
    "RevenueRemainingPerformanceObligation": 3,
    "CostOfRevenue": 4,
    "NetIncomeLoss": 5,
    "NetCashProvidedByUsedInOperatingActivities": 6,
    "PaymentsToAcquirePropertyPlantAndEquipment": 7,
    "CashAndCashEquivalentsAtCarryingValue": 8,
    "Assets": 9,
    "LongTermDebtCurrent": 10,
}


def render_company_research(
    data: DashboardData,
    *,
    load_fact_history: FactHistoryLoader,
) -> None:
    st.subheader("Company research cockpit")
    st.warning(
        "SEC metrics are company-reported. Port and trade exposure scores are inferred "
        "economic linkages, not observed shipment ownership."
    )
    _render_company_facts(data, load_fact_history=load_fact_history)
    _render_filings(data)
    _render_alternative_data_linkage(data)


def prepare_fact_history(fact_history: pd.DataFrame, period_view: str) -> pd.DataFrame:
    """Normalize SEC fact observations for one annual or quarterly chart."""
    if period_view == "Annual (10-K)":
        history = fact_history[
            (fact_history["form"] == "10-K") & (fact_history["fiscal_period"] == "FY")
        ].copy()
    else:
        history = fact_history[
            (fact_history["form"] == "10-Q")
            & fact_history["fiscal_period"].isin(["Q1", "Q2", "Q3"])
        ].copy()
        duration_days = (
            pd.to_datetime(history["period_end"]) - pd.to_datetime(history["period_start"])
        ).dt.days
        history = history[duration_days.isna() | duration_days.le(120)].copy()
        history["duration_days"] = duration_days

    chart_data = history.dropna(subset=["period_end", "value_numeric"]).copy()
    if chart_data.empty:
        return chart_data
    if period_view == "Quarterly (10-Q)":
        return (
            chart_data.sort_values(
                ["period_end", "duration_days", "accepted_at"],
                ascending=[True, True, False],
                na_position="last",
            )
            .drop_duplicates(subset=["period_end"], keep="first")
            .sort_values("period_end")
        )
    return (
        chart_data.sort_values(["period_end", "accepted_at"])
        .drop_duplicates(subset=["period_end"], keep="last")
        .sort_values("period_end")
    )


def _render_company_facts(data: DashboardData, *, load_fact_history: FactHistoryLoader) -> None:
    st.markdown("#### Structured SEC fundamentals")
    if data.sec_fact_catalog.empty:
        st.info("Run `portwatch ingest sec --ticker CAT` to load reviewed SEC evidence.")
        return

    entity_names = dict(
        zip(data.entity_registry["entity_id"], data.entity_registry["name"], strict=True)
    )
    selected_entity = st.selectbox(
        "Issuer",
        sorted(data.sec_fact_catalog["entity_id"].unique().tolist()),
        format_func=lambda entity_id: entity_names.get(entity_id, entity_id),
        key="sec_issuer",
    )
    catalog = data.sec_fact_catalog[data.sec_fact_catalog["entity_id"] == selected_entity].copy()
    catalog["_priority"] = catalog["tag"].map(FACT_PRIORITY).fillna(1_000)
    catalog = catalog.sort_values(["_priority", "label", "taxonomy", "unit"]).reset_index(drop=True)

    def format_fact_option(position: int) -> str:
        concept = catalog.iloc[position]
        return f"{concept['label']} · {concept['tag']} [{concept['unit']}]"

    selected_position = st.selectbox(
        "Company fact",
        list(range(len(catalog))),
        format_func=format_fact_option,
        key="sec_company_fact",
    )
    concept = catalog.iloc[selected_position]
    fact_history = load_fact_history(
        str(concept["entity_id"]),
        str(concept["taxonomy"]),
        str(concept["tag"]),
        str(concept["unit"]),
    )
    period_view = st.selectbox(
        "Period view",
        ("Annual (10-K)", "Quarterly (10-Q)"),
        key="sec_period_view",
    )
    chart_data = prepare_fact_history(fact_history, period_view)
    if chart_data.empty:
        st.info(f"No {period_view.lower()} observations are available for this concept.")
        return

    latest = chart_data.iloc[-1]
    metrics = st.columns(4)
    metrics[0].metric(
        "Latest value", format_fact_value(latest["value_numeric"], str(latest["unit"]))
    )
    metrics[1].metric("Latest period", str(latest["period_end"])[:10])
    metrics[2].metric("Normalized observations", f"{int(concept['observation_count']):,}")
    metrics[3].metric("Source filings", f"{int(concept['filing_count']):,}")
    st.plotly_chart(
        px.line(
            chart_data,
            x="period_end",
            y="value_numeric",
            markers=True,
            hover_data=["form", "fiscal_period", "filed_on", "accession_number"],
            labels={"period_end": "Period end", "value_numeric": str(concept["label"])},
            title=f"{concept['label']} — {period_view}",
        ),
        width="stretch",
    )
    with st.expander("View reported observations"):
        st.dataframe(
            fact_history.sort_values("accepted_at", ascending=False),
            width="stretch",
            hide_index=True,
        )


def _render_filings(data: DashboardData) -> None:
    st.markdown("#### Recent material filings")
    if data.sec_filings.empty:
        st.info("No 10-K, 10-Q, or 8-K filing events are available.")
        return
    st.dataframe(
        data.sec_filings[
            ["filed_on", "form", "report_date", "accession_number", "primary_document_url"]
        ],
        width="stretch",
        hide_index=True,
        column_config={
            "primary_document_url": st.column_config.LinkColumn("SEC filing"),
        },
    )


def _render_alternative_data_linkage(data: DashboardData) -> None:
    st.markdown("#### Alternative-data linkage")
    if data.exposure_scores.empty:
        st.info(
            "The CAT-to-port signal requires monthly Census history. The reviewed mapping "
            "is ready, but no trade rows have been ingested yet."
        )
    else:
        st.dataframe(data.exposure_scores, width="stretch", hide_index=True)

    with st.expander("Reviewed exposure mapping and dated entity graph"):
        st.markdown("##### Commodity exposure registry")
        st.dataframe(data.exposure_registry, width="stretch", hide_index=True)
        st.markdown("##### Entities")
        st.caption(
            "Validity dates represent the period supported by cited evidence, "
            "not an assumed legal inception date."
        )
        st.dataframe(data.entity_registry, width="stretch", hide_index=True)
        st.markdown("##### External identifiers")
        st.dataframe(data.entity_identifiers, width="stretch", hide_index=True)
        st.markdown("##### Relationships")
        st.dataframe(data.entity_relationships, width="stretch", hide_index=True)
