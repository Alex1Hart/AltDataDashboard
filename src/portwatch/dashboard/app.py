from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from portwatch.analytics.contracts import (
    compute_contract_company_signals,
    monthly_contract_awards,
)
from portwatch.analytics.signals import compute_trade_signals, latest_trade_signals
from portwatch.config import get_settings
from portwatch.registry import (
    company_exposure_scores,
    load_company_registry,
    registry_entities_frame,
    registry_exposures_frame,
    registry_identifiers_frame,
    registry_relationships_frame,
)
from portwatch.storage.duckdb import DuckDBRepository

st.set_page_config(page_title="PortWatch", page_icon="⚓", layout="wide")

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


def _format_percent(value: Any) -> str:
    return "—" if pd.isna(value) else f"{float(value):.1%}"


def _format_number(value: Any) -> str:
    return "—" if pd.isna(value) else f"{float(value):.2f}"


def _format_money(value: Any) -> str:
    if pd.isna(value):
        return "—"
    numeric = float(value)
    if abs(numeric) >= 1e9:
        return f"${numeric / 1e9:,.2f}B"
    if abs(numeric) >= 1e6:
        return f"${numeric / 1e6:,.1f}M"
    return f"${numeric:,.0f}"


def _format_fact_value(value: Any, unit: str) -> str:
    if pd.isna(value):
        return "—"
    numeric = float(value)
    if unit == "USD":
        magnitude = abs(numeric)
        if magnitude >= 1e9:
            return f"${numeric / 1e9:,.2f}B"
        if magnitude >= 1e6:
            return f"${numeric / 1e6:,.1f}M"
        return f"${numeric:,.0f}"
    if abs(numeric) >= 1e9:
        return f"{numeric / 1e9:,.2f}B {unit}"
    if abs(numeric) >= 1e6:
        return f"{numeric / 1e6:,.1f}M {unit}"
    return f"{numeric:,.2f} {unit}"


@st.cache_data(ttl=60)
def load_data() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
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
    contract_awards = repository.federal_contract_awards_summary()
    return (
        flows,
        signals,
        repository.port_operations_summary(),
        company_exposure_scores(signals, registry),
        registry_exposures_frame(registry),
        registry_entities_frame(registry),
        registry_identifiers_frame(registry),
        registry_relationships_frame(registry),
        repository.sec_filings_summary(forms=("10-K", "10-Q", "8-K")),
        repository.sec_company_fact_catalog(),
        contract_awards,
        compute_contract_company_signals(contract_awards),
        repository.federal_contract_award_revisions(),
        repository.trade_flow_revisions(),
    )


@st.cache_data(ttl=60)
def load_source_counts() -> dict[str, int]:
    settings = get_settings()
    return DuckDBRepository(settings.database_path).source_counts()


@st.cache_data(ttl=60)
def load_fact_history(
    entity_id: str,
    taxonomy: str,
    tag: str,
    unit: str,
) -> pd.DataFrame:
    settings = get_settings()
    return DuckDBRepository(settings.database_path).sec_company_fact_history(
        entity_id=entity_id,
        taxonomy=taxonomy,
        tag=tag,
        unit=unit,
    )


(
    flows,
    signals,
    operations,
    exposure_scores,
    exposure_registry,
    entity_registry,
    entity_identifiers,
    entity_relationships,
    sec_filings,
    sec_fact_catalog,
    contract_awards,
    contract_signals,
    contract_revisions,
    revisions,
) = load_data()
settings = get_settings()
repository = DuckDBRepository(settings.database_path)
runs = repository.recent_runs()
source_counts = load_source_counts()

st.title("PortWatch")
st.caption(
    "Company-centered Industrials evidence with dated entity relationships, "
    "vintage-aware provenance, and contextual port and trade signals"
)

coverage_columns = st.columns(6)
coverage_columns[0].metric(
    "Tracked companies",
    f"{exposure_registry['ticker'].nunique():,}",
)
coverage_columns[1].metric("SEC filings", f"{source_counts['sec_filings']:,}")
coverage_columns[2].metric("SEC facts", f"{source_counts['sec_company_facts']:,}")
coverage_columns[3].metric(
    "Contract awards",
    f"{source_counts['federal_contract_awards']:,}",
)
coverage_columns[4].metric("Trade rows", f"{source_counts['trade_flows']:,}")
coverage_columns[5].metric("Port observations", f"{source_counts['port_operations']:,}")

(
    company_tab,
    contracts_tab,
    overview_tab,
    signals_tab,
    operations_tab,
    revisions_tab,
    health_tab,
) = st.tabs(
    [
        "Company research",
        "ContractWatch",
        "Market overview",
        "Research signals",
        "Port operations",
        "Revisions",
        "Pipeline health",
    ]
)

with contracts_tab:
    st.subheader("ContractWatch: company-linked federal prime awards")
    st.caption(
        "Award Amount is USAspending's current cumulative federal obligation for an award. "
        "It is not company revenue, funded backlog, or remaining contract value."
    )
    if contract_awards.empty:
        st.info(
            "Load a reviewed issuer's federal awards with "
            "`portwatch ingest contracts --ticker CAT`. No API key is required."
        )
    else:
        contract_tickers = sorted(contract_awards["ticker"].unique().tolist())
        selected_contract_ticker = st.selectbox(
            "Company",
            contract_tickers,
            key="contractwatch_ticker",
        )
        selected_awards = contract_awards[
            contract_awards["ticker"] == selected_contract_ticker
        ].copy()
        selected_signal = contract_signals[contract_signals["ticker"] == selected_contract_ticker]
        if not selected_signal.empty:
            signal = selected_signal.iloc[0]
            contract_metrics = st.columns(5)
            contract_metrics[0].metric(
                "Current obligations",
                _format_money(signal["total_current_obligations_usd"]),
                help="Cumulative federal obligations across the current award snapshots.",
            )
            contract_metrics[1].metric(
                "TTM new awards",
                _format_money(signal["ttm_new_award_obligations_usd"]),
                _format_percent(signal["ttm_new_award_yoy"]),
                help="Current award value grouped by each award's base obligation date.",
            )
            contract_metrics[2].metric("Awards", f"{int(signal['award_count']):,}")
            contract_metrics[3].metric(
                "12m expiration wall",
                _format_money(signal["next_12m_expiring_award_value_usd"]),
                help="Current value of awards whose period of performance ends in 12 months.",
            )
            contract_metrics[4].metric(
                "Agency HHI",
                _format_number(signal["agency_hhi"]),
                help="Concentration of positive current obligations by awarding agency.",
            )

        monthly_awards = monthly_contract_awards(selected_awards)
        st.plotly_chart(
            px.bar(
                monthly_awards,
                x="award_month",
                y="award_amount_usd",
                hover_data=["award_count"],
                labels={
                    "award_month": "Base obligation month",
                    "award_amount_usd": "Current award obligations (USD)",
                },
                title=f"{selected_contract_ticker} current awards by original award month",
            ),
            width="stretch",
        )

        agency_awards = (
            selected_awards.dropna(subset=["awarding_agency"])
            .groupby("awarding_agency", as_index=False)["award_amount_usd"]
            .sum()
            .sort_values("award_amount_usd", ascending=False)
            .head(12)
        )
        if not agency_awards.empty:
            st.plotly_chart(
                px.bar(
                    agency_awards,
                    x="award_amount_usd",
                    y="awarding_agency",
                    orientation="h",
                    labels={
                        "award_amount_usd": "Current obligations (USD)",
                        "awarding_agency": "Awarding agency",
                    },
                    title="Awarding-agency exposure",
                ),
                width="stretch",
            )

        st.markdown("#### Matched award evidence")
        st.dataframe(
            selected_awards[
                [
                    "base_obligation_date",
                    "award_id",
                    "recipient_name",
                    "award_amount_usd",
                    "end_date",
                    "awarding_agency",
                    "naics_code",
                    "psc_code",
                    "match_method",
                    "revision_number",
                    "source_url",
                ]
            ],
            width="stretch",
            hide_index=True,
            column_config={"source_url": st.column_config.LinkColumn("USAspending award")},
        )

with overview_tab:
    if flows.empty:
        st.info(
            "SEC company evidence is loaded, but the market overview requires Census "
            "trade-flow history. Add a Census API key to `.env`, then run:"
        )
        st.code("portwatch backfill --config config/portwatch.yml", language="powershell")
        if not settings.census_api_key:
            st.warning("`CENSUS_API_KEY` is currently blank.")
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
    st.subheader("Company research cockpit")
    st.warning(
        "SEC metrics are company-reported. Port and trade exposure scores are inferred "
        "economic linkages, not observed shipment ownership."
    )

    st.markdown("#### Structured SEC fundamentals")
    if sec_fact_catalog.empty:
        st.info("Run `portwatch ingest sec --ticker CAT` to load reviewed SEC evidence.")
    else:
        entity_names = dict(zip(entity_registry["entity_id"], entity_registry["name"], strict=True))
        entity_options = sorted(sec_fact_catalog["entity_id"].unique().tolist())
        selected_entity = st.selectbox(
            "Issuer",
            entity_options,
            format_func=lambda entity_id: entity_names.get(entity_id, entity_id),
            key="sec_issuer",
        )

        company_catalog = sec_fact_catalog[sec_fact_catalog["entity_id"] == selected_entity].copy()
        company_catalog["_priority"] = company_catalog["tag"].map(FACT_PRIORITY).fillna(1_000)
        company_catalog = company_catalog.sort_values(
            ["_priority", "label", "taxonomy", "unit"]
        ).reset_index(drop=True)
        fact_options = list(range(len(company_catalog)))

        def format_fact_option(position: int) -> str:
            concept = company_catalog.iloc[position]
            return f"{concept['label']} · {concept['tag']} [{concept['unit']}]"

        selected_fact_position = st.selectbox(
            "Company fact",
            fact_options,
            format_func=format_fact_option,
            key="sec_company_fact",
        )
        selected_concept = company_catalog.iloc[selected_fact_position]
        fact_history = load_fact_history(
            str(selected_concept["entity_id"]),
            str(selected_concept["taxonomy"]),
            str(selected_concept["tag"]),
            str(selected_concept["unit"]),
        )

        period_view = st.selectbox(
            "Period view",
            ("Annual (10-K)", "Quarterly (10-Q)"),
            key="sec_period_view",
        )
        if period_view == "Annual (10-K)":
            history_view = fact_history[
                (fact_history["form"] == "10-K") & (fact_history["fiscal_period"] == "FY")
            ].copy()
        else:
            history_view = fact_history[
                (fact_history["form"] == "10-Q")
                & fact_history["fiscal_period"].isin(["Q1", "Q2", "Q3"])
            ].copy()
            duration_days = (
                pd.to_datetime(history_view["period_end"])
                - pd.to_datetime(history_view["period_start"])
            ).dt.days
            history_view = history_view[duration_days.isna() | duration_days.le(120)].copy()
            history_view["duration_days"] = duration_days

        chart_data = history_view.dropna(subset=["period_end", "value_numeric"]).copy()
        if period_view == "Quarterly (10-Q)" and not chart_data.empty:
            chart_data = (
                chart_data.sort_values(
                    ["period_end", "duration_days", "accepted_at"],
                    ascending=[True, True, False],
                    na_position="last",
                )
                .drop_duplicates(subset=["period_end"], keep="first")
                .sort_values("period_end")
            )
        elif not chart_data.empty:
            chart_data = (
                chart_data.sort_values(["period_end", "accepted_at"])
                .drop_duplicates(subset=["period_end"], keep="last")
                .sort_values("period_end")
            )

        if chart_data.empty:
            st.info(f"No {period_view.lower()} observations are available for this concept.")
        else:
            latest_fact = chart_data.iloc[-1]
            metric_columns = st.columns(4)
            metric_columns[0].metric(
                "Latest value",
                _format_fact_value(latest_fact["value_numeric"], str(latest_fact["unit"])),
            )
            metric_columns[1].metric(
                "Latest period",
                str(latest_fact["period_end"])[:10],
            )
            metric_columns[2].metric(
                "Normalized observations",
                f"{int(selected_concept['observation_count']):,}",
            )
            metric_columns[3].metric(
                "Source filings",
                f"{int(selected_concept['filing_count']):,}",
            )
            st.plotly_chart(
                px.line(
                    chart_data,
                    x="period_end",
                    y="value_numeric",
                    markers=True,
                    hover_data=["form", "fiscal_period", "filed_on", "accession_number"],
                    labels={
                        "period_end": "Period end",
                        "value_numeric": str(selected_concept["label"]),
                    },
                    title=f"{selected_concept['label']} — {period_view}",
                ),
                width="stretch",
            )
            with st.expander("View reported observations"):
                st.dataframe(
                    fact_history.sort_values("accepted_at", ascending=False),
                    width="stretch",
                    hide_index=True,
                )

    st.markdown("#### Recent material filings")
    if sec_filings.empty:
        st.info("No 10-K, 10-Q, or 8-K filing events are available.")
    else:
        filing_columns = [
            "filed_on",
            "form",
            "report_date",
            "accession_number",
            "primary_document_url",
        ]
        st.dataframe(
            sec_filings[filing_columns],
            width="stretch",
            hide_index=True,
            column_config={
                "primary_document_url": st.column_config.LinkColumn("SEC filing"),
            },
        )

    st.markdown("#### Alternative-data linkage")
    if exposure_scores.empty:
        st.info(
            "The CAT-to-port signal requires monthly Census history. The reviewed mapping "
            "is ready, but no trade rows have been ingested yet."
        )
    else:
        st.dataframe(exposure_scores, width="stretch", hide_index=True)

    with st.expander("Reviewed exposure mapping and dated entity graph"):
        st.markdown("##### Commodity exposure registry")
        st.dataframe(exposure_registry, width="stretch", hide_index=True)
        st.markdown("##### Entities")
        st.caption(
            "Validity dates represent the period supported by cited evidence, "
            "not an assumed legal inception date."
        )
        st.dataframe(entity_registry, width="stretch", hide_index=True)
        st.markdown("##### External identifiers")
        st.dataframe(entity_identifiers, width="stretch", hide_index=True)
        st.markdown("##### Relationships")
        st.dataframe(entity_relationships, width="stretch", hide_index=True)

with revisions_tab:
    st.caption(
        "`available_at` is the first time PortWatch could have used a vintage; "
        "`valid_until` closes it when a changed value arrives."
    )
    st.markdown("#### Federal contract award vintages")
    if contract_revisions.empty:
        st.info("No federal contract award vintages have been recorded.")
    else:
        st.dataframe(contract_revisions, width="stretch", hide_index=True)

    st.markdown("#### Trade-flow vintages")
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
