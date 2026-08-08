from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from portwatch.analytics.hiring import (
    business_line_hiring_summary,
    compute_hiring_company_signals,
    function_hiring_summary,
    hiring_activity_daily,
    labor_market_wide,
    location_hiring_summary,
    theme_hiring_summary,
)
from portwatch.dashboard.data import DashboardData
from portwatch.dashboard.formatters import format_percent


def render_hiring_page(data: DashboardData) -> None:
    st.subheader("HiringWatch: company, business-line, and facility demand")
    st.caption(
        "First-party job postings are a measure of advertised labor demand, not completed hires. "
        "Closures require two complete snapshots in which a posting is absent."
    )
    if data.job_postings.empty:
        st.info(
            "Load the configured first-party career sources with "
            "`portwatch ingest hiring --ticker CAT`."
        )
        return

    selected_ticker = st.selectbox(
        "Company",
        sorted(data.job_postings["ticker"].unique().tolist()),
        key="hiringwatch_ticker",
    )
    company_jobs = data.job_postings[data.job_postings["ticker"] == selected_ticker].copy()
    company_events = data.job_events[data.job_events["ticker"] == selected_ticker].copy()
    company_locations = data.job_locations[data.job_locations["ticker"] == selected_ticker].copy()

    _render_signal_metrics(company_jobs, company_events, company_locations)
    _render_activity(company_events)
    _render_demand_mix(company_jobs)
    _render_locations(company_locations)
    _render_labor_benchmark(data)
    _render_data_quality(company_jobs, company_locations)


def _render_signal_metrics(
    company_jobs: pd.DataFrame,
    company_events: pd.DataFrame,
    company_locations: pd.DataFrame,
) -> None:
    signals = compute_hiring_company_signals(company_jobs, company_events, company_locations)
    if signals.empty:
        return
    signal = signals.iloc[0]
    metrics = st.columns(6)
    metrics[0].metric("Active postings", f"{int(signal['active_postings']):,}")
    metrics[1].metric("New / reopened (28d)", f"{int(signal['new_postings_28d']):,}")
    metrics[2].metric("Closed (28d)", f"{int(signal['closed_postings_28d']):,}")
    metrics[3].metric("Net change (28d)", f"{int(signal['net_posting_change_28d']):+,}")
    metrics[4].metric("Median posting age", f"{signal['median_active_age_days']:.0f} days")
    metrics[5].metric(
        "Line-of-business coverage",
        format_percent(signal["business_line_coverage"]),
    )


def _render_activity(company_events: pd.DataFrame) -> None:
    activity = hiring_activity_daily(company_events)
    if activity.empty:
        return
    st.plotly_chart(
        px.bar(
            activity,
            x="observed_date",
            y=["baseline", "opened", "reopened", "closed"],
            barmode="group",
            title="Observed posting lifecycle events",
            labels={"value": "Postings", "observed_date": "Observation date"},
        ),
        width="stretch",
    )


def _render_demand_mix(company_jobs: pd.DataFrame) -> None:
    business_lines = business_line_hiring_summary(company_jobs)
    functions = function_hiring_summary(company_jobs)
    themes = theme_hiring_summary(company_jobs)
    left, middle, right = st.columns(3)
    with left:
        st.plotly_chart(
            px.bar(
                business_lines,
                x="active_postings",
                y="business_line",
                orientation="h",
                title="Active demand by reported business line",
                labels={"business_line": "Business line", "active_postings": "Postings"},
            ),
            width="stretch",
        )
    with middle:
        st.plotly_chart(
            px.bar(
                functions,
                x="active_postings",
                y="job_function",
                orientation="h",
                title="Active demand by function",
                labels={"job_function": "Function", "active_postings": "Postings"},
            ),
            width="stretch",
        )
    with right:
        if themes.empty:
            st.info("No strategic-theme matches are available yet.")
        else:
            st.plotly_chart(
                px.bar(
                    themes,
                    x="active_postings",
                    y="theme",
                    orientation="h",
                    title="Active demand by strategic theme",
                    labels={"theme": "Theme", "active_postings": "Postings"},
                ),
                width="stretch",
            )


def _render_locations(company_locations: pd.DataFrame) -> None:
    locations = location_hiring_summary(company_locations).head(25)
    if locations.empty:
        return
    st.plotly_chart(
        px.bar(
            locations,
            x="active_postings",
            y="raw_location",
            orientation="h",
            title="Top hiring locations",
            labels={"raw_location": "Location", "active_postings": "Postings"},
        ),
        width="stretch",
    )


def _render_labor_benchmark(data: DashboardData) -> None:
    st.markdown("#### Industry labor benchmark")
    labor_wide = labor_market_wide(data.labor_market)
    if labor_wide.empty:
        st.info(
            "Add a company-relevant NAICS/geography quarter with, for example, "
            "`portwatch ingest qwi --year 2025 --quarter 1 --industry 333120 "
            "--geography-code 17`."
        )
        return
    selected_industry = st.selectbox(
        "NAICS benchmark",
        sorted(labor_wide["industry_code"].astype(str).unique().tolist()),
        key="hiringwatch_naics",
    )
    benchmark = labor_wide[labor_wide["industry_code"].astype(str) == selected_industry]
    metrics = [
        metric
        for metric in ("employment", "hires", "separations", "job_gains", "job_losses")
        if metric in benchmark.columns
    ]
    if metrics:
        st.plotly_chart(
            px.line(
                benchmark,
                x="period_start",
                y=metrics,
                markers=True,
                title=f"Census QWI — NAICS {selected_industry}",
            ),
            width="stretch",
        )


def _render_data_quality(
    company_jobs: pd.DataFrame,
    company_locations: pd.DataFrame,
) -> None:
    active_jobs = company_jobs[company_jobs["status"] == "active"]
    unclassified = int(active_jobs["business_line_name"].isna().sum())
    unmatched_locations = (
        int(
            company_locations[
                (company_locations["status"] == "active")
                & company_locations["facility_entity_id"].isna()
            ]["location_id"].nunique()
        )
        if not company_locations.empty
        else 0
    )
    with st.expander("Data quality and posting evidence"):
        st.write(
            f"{unclassified:,} active postings are not mapped to a reported business line; "
            f"{unmatched_locations:,} active locations are not linked to a reviewed facility."
        )
        st.dataframe(
            active_jobs[
                [
                    "title",
                    "business_line_name",
                    "department",
                    "job_function",
                    "seniority",
                    "first_seen_at",
                    "source_url",
                ]
            ],
            width="stretch",
            hide_index=True,
            column_config={"source_url": st.column_config.LinkColumn("Career posting")},
        )
