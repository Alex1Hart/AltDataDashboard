from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from portwatch.analytics.contracts import (
    contract_award_detail,
    contract_obligation_breakdown,
    monthly_contract_awards,
    monthly_contract_transactions,
)
from portwatch.dashboard.data import DashboardData
from portwatch.dashboard.formatters import format_money, format_number, format_percent


def render_contracts_page(data: DashboardData) -> None:
    st.subheader("ContractWatch: company-linked federal contract actions")
    st.caption(
        "Signed USAspending actions measure dated funding changes: positive values add federal "
        "obligations and negative values are deobligations. Current award amounts remain a "
        "separate cumulative inventory measure; neither field is company revenue or backlog."
    )
    if data.contract_awards.empty:
        st.info(
            "Load a reviewed issuer's federal awards and action history with "
            "`portwatch ingest contracts --ticker CAT`. No API key is required."
        )
        return

    selected_ticker = st.selectbox(
        "Company",
        sorted(data.contract_awards["ticker"].unique().tolist()),
        key="contractwatch_ticker",
    )
    selected_awards = data.contract_awards[data.contract_awards["ticker"] == selected_ticker].copy()
    selected_transactions = data.contract_transactions[
        data.contract_transactions["ticker"] == selected_ticker
    ].copy()
    selected_signal = data.contract_signals[data.contract_signals["ticker"] == selected_ticker]
    if not selected_signal.empty:
        _render_contract_metrics(selected_signal.iloc[0])

    if selected_transactions.empty:
        _render_award_inventory_fallback(selected_ticker, selected_awards)
        return

    _render_action_history(selected_ticker, selected_transactions)
    _render_action_breakdowns(selected_awards, selected_transactions)
    _render_action_evidence(selected_transactions)
    _render_award_evidence(selected_awards, selected_transactions)


def _render_contract_metrics(signal: pd.Series[Any]) -> None:
    st.markdown("#### Funding momentum")
    flow_metrics = st.columns(5)
    flow_metrics[0].metric(
        "TTM net obligations",
        format_money(signal["ttm_net_obligations_usd"]),
        format_percent(signal["ttm_net_obligations_yoy"]),
        help="Signed funding actions in the trailing twelve months; delta is versus prior TTM.",
    )
    flow_metrics[1].metric(
        "TTM gross funding",
        format_money(signal["ttm_gross_obligations_usd"]),
        help="Positive federal obligation actions in the trailing twelve months.",
    )
    flow_metrics[2].metric(
        "TTM deobligations",
        format_money(signal["ttm_deobligations_usd"]),
        help="Absolute value of negative obligation actions in the trailing twelve months.",
    )
    flow_metrics[3].metric(
        "TTM modifications",
        f"{int(signal['ttm_modification_count']):,}",
        help="Trailing-twelve-month actions with a nonzero modification number.",
    )
    flow_metrics[4].metric(
        "Action coverage",
        format_percent(signal["transaction_coverage"]),
        help="Share of matched current awards with at least one ingested transaction.",
    )

    inventory_metrics = st.columns(5)
    inventory_metrics[0].metric(
        "Current award obligations",
        format_money(signal["total_current_obligations_usd"]),
        help="Cumulative federal obligations across current award snapshots.",
    )
    inventory_metrics[1].metric("Matched awards", f"{int(signal['award_count']):,}")
    inventory_metrics[2].metric(
        "All ingested actions",
        f"{int(signal['transaction_count']):,}",
    )
    inventory_metrics[3].metric(
        "12m expiration wall",
        format_money(signal["next_12m_expiring_award_value_usd"]),
        help="Current obligations on awards whose performance period ends in 12 months.",
    )
    inventory_metrics[4].metric(
        "Agency HHI",
        format_number(signal["agency_hhi"]),
        help="Concentration of positive current award obligations by awarding agency.",
    )


def _render_action_history(ticker: str, transactions: pd.DataFrame) -> None:
    st.markdown("#### Dated obligation flow")
    monthly = monthly_contract_transactions(transactions)
    monthly["flow_direction"] = monthly["net_obligations_usd"].map(
        lambda value: "Net funding" if value >= 0 else "Net deobligation"
    )
    st.plotly_chart(
        px.bar(
            monthly,
            x="action_month",
            y="net_obligations_usd",
            color="flow_direction",
            color_discrete_map={
                "Net funding": "#2E7D32",
                "Net deobligation": "#C62828",
            },
            hover_data=[
                "gross_obligations_usd",
                "deobligations_usd",
                "transaction_count",
            ],
            labels={
                "action_month": "Action month",
                "net_obligations_usd": "Net federal obligations (USD)",
                "flow_direction": "Direction",
            },
            title=f"{ticker} signed federal obligation actions by month",
        ),
        width="stretch",
    )


def _render_action_breakdowns(awards: pd.DataFrame, transactions: pd.DataFrame) -> None:
    st.markdown("#### Where the funding is moving")
    agency_tab, naics_tab, psc_tab = st.tabs(
        ["Awarding agency", "Industry (NAICS)", "Product/service (PSC)"]
    )
    with agency_tab:
        agency = contract_obligation_breakdown(
            awards,
            transactions,
            dimension="awarding_agency",
        ).head(15)
        _render_breakdown(agency, dimension="awarding_agency", label="Awarding agency")
    with naics_tab:
        naics = contract_obligation_breakdown(
            awards,
            transactions,
            dimension="naics_code",
        ).head(15)
        naics = _attach_description(naics, awards, "naics_code", "naics_description")
        _render_breakdown(naics, dimension="naics_label", label="NAICS")
    with psc_tab:
        psc = contract_obligation_breakdown(
            awards,
            transactions,
            dimension="psc_code",
        ).head(15)
        psc = _attach_description(psc, awards, "psc_code", "psc_description")
        _render_breakdown(psc, dimension="psc_label", label="PSC")


def _render_breakdown(frame: pd.DataFrame, *, dimension: str, label: str) -> None:
    if frame.empty:
        st.info(f"No {label} attribution is available for the ingested actions.")
        return
    st.plotly_chart(
        px.bar(
            frame.sort_values("gross_obligations_usd"),
            x="net_obligations_usd",
            y=dimension,
            orientation="h",
            hover_data=[
                "gross_obligations_usd",
                "deobligations_usd",
                "transaction_count",
            ],
            labels={
                dimension: label,
                "net_obligations_usd": "Net obligations (USD)",
            },
        ),
        width="stretch",
    )
    st.dataframe(
        frame,
        width="stretch",
        hide_index=True,
        column_config={
            "net_obligations_usd": st.column_config.NumberColumn("Net obligations", format="$%.0f"),
            "gross_obligations_usd": st.column_config.NumberColumn("Gross funding", format="$%.0f"),
            "deobligations_usd": st.column_config.NumberColumn("Deobligations", format="$%.0f"),
        },
    )


def _render_action_evidence(transactions: pd.DataFrame) -> None:
    st.markdown("#### Latest contract actions")
    columns = [
        "action_date",
        "award_id",
        "modification_number",
        "federal_action_obligation_usd",
        "action_type_description",
        "description",
        "revision_number",
        "source_url",
    ]
    st.dataframe(
        transactions.sort_values("action_date", ascending=False)[columns].head(250),
        width="stretch",
        hide_index=True,
        column_config={
            "federal_action_obligation_usd": st.column_config.NumberColumn(
                "Signed obligation", format="$%.0f"
            ),
            "source_url": st.column_config.LinkColumn("USAspending award"),
        },
    )


def _render_award_evidence(awards: pd.DataFrame, transactions: pd.DataFrame) -> None:
    st.markdown("#### Award inventory with action reconciliation")
    details = contract_award_detail(awards, transactions)
    columns = [
        "base_obligation_date",
        "award_id",
        "recipient_name",
        "award_amount_usd",
        "transaction_net_obligations_usd",
        "transaction_deobligations_usd",
        "transaction_count",
        "latest_action_date",
        "end_date",
        "awarding_agency",
        "naics_code",
        "psc_code",
        "match_method",
        "revision_number",
        "source_url",
    ]
    st.dataframe(
        details[columns],
        width="stretch",
        hide_index=True,
        column_config={
            "award_amount_usd": st.column_config.NumberColumn(
                "Current obligations", format="$%.0f"
            ),
            "transaction_net_obligations_usd": st.column_config.NumberColumn(
                "Action-history net", format="$%.0f"
            ),
            "transaction_deobligations_usd": st.column_config.NumberColumn(
                "Deobligations", format="$%.0f"
            ),
            "source_url": st.column_config.LinkColumn("USAspending award"),
        },
    )


def _render_award_inventory_fallback(ticker: str, awards: pd.DataFrame) -> None:
    st.warning(
        "This database has award snapshots but no signed action history. Re-run "
        f"`portwatch ingest contracts --ticker {ticker}` to populate the new transaction layer."
    )
    monthly = monthly_contract_awards(awards)
    st.plotly_chart(
        px.bar(
            monthly,
            x="award_month",
            y="award_amount_usd",
            hover_data=["award_count"],
            labels={
                "award_month": "Base obligation month",
                "award_amount_usd": "Current award obligations (USD)",
            },
            title=f"{ticker} current awards by original award month",
        ),
        width="stretch",
    )
    _render_award_evidence(awards, transactions=pd.DataFrame())


def _attach_description(
    breakdown: pd.DataFrame,
    awards: pd.DataFrame,
    code_column: str,
    description_column: str,
) -> pd.DataFrame:
    if breakdown.empty:
        return breakdown.assign(**{code_column.replace("code", "label"): pd.Series(dtype=str)})
    descriptions = awards[[code_column, description_column]].drop_duplicates(code_column)
    merged = breakdown.merge(descriptions, on=code_column, how="left")
    label_column = code_column.replace("code", "label")
    merged[label_column] = merged.apply(
        lambda row: (
            f"{row[code_column]} - {row[description_column]}"
            if pd.notna(row[description_column])
            else str(row[code_column])
        ),
        axis=1,
    )
    return merged
