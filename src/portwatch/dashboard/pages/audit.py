from __future__ import annotations

import pandas as pd
import streamlit as st

from portwatch.dashboard.data import DashboardData


def render_revisions(data: DashboardData) -> None:
    st.caption(
        "`available_at` is the first time PortWatch could have used a vintage; "
        "`valid_until` closes it when a changed value arrives."
    )
    _render_revision_table(
        "Federal contract award vintages",
        data.contract_revisions,
        "No federal contract award vintages have been recorded.",
    )
    _render_revision_table(
        "Federal contract action vintages",
        data.contract_transaction_revisions,
        "No federal contract action vintages have been recorded.",
    )
    _render_revision_table(
        "Career-posting lifecycle vintages",
        data.job_revisions,
        "No career-posting vintages have been recorded.",
    )
    _render_revision_table(
        "Trade-flow vintages",
        data.trade_revisions,
        "No observation vintages have been recorded.",
    )


def render_pipeline_health(data: DashboardData) -> None:
    st.subheader("Recent ingestion runs")
    if data.ingestion_runs.empty:
        st.info("No ingestion runs recorded yet.")
    else:
        st.dataframe(data.ingestion_runs, width="stretch", hide_index=True)
    st.subheader("Provenance policy")
    st.markdown(
        "- **Observed:** retrieved from a named source and passed validation.\n"
        "- **Reported:** contained in a company, port, or regulator disclosure.\n"
        "- **Inferred:** deterministic or analyst-reviewed mapping; never shipment ownership."
    )


def _render_revision_table(title: str, frame: pd.DataFrame, empty_message: str) -> None:
    st.markdown(f"#### {title}")
    if frame.empty:
        st.info(empty_message)
    else:
        st.dataframe(frame, width="stretch", hide_index=True)
