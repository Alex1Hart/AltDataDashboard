from __future__ import annotations

import plotly.express as px
import streamlit as st

from portwatch.config import get_settings
from portwatch.storage.duckdb import DuckDBRepository

st.set_page_config(page_title="PortWatch", page_icon="⚓", layout="wide")


@st.cache_data(ttl=60)
def load_data() -> tuple[object, object]:
    settings = get_settings()
    repository = DuckDBRepository(settings.database_path)
    repository.initialize()
    return repository.trade_flow_summary(), repository.recent_runs()


flows, runs = load_data()

st.title("PortWatch")
st.caption("Observed U.S. port and industrial trade flows with explicit source provenance")

overview_tab, cargo_tab, health_tab = st.tabs(
    ["Market overview", "Cargo explorer", "Pipeline health"]
)

with overview_tab:
    if flows.empty:
        st.info(
            "No observations have been ingested. Add a Census API key and run the "
            "first ingestion command shown in the README."
        )
    else:
        latest_month = flows["month"].max()
        latest = flows[flows["month"] == latest_month]
        col1, col2, col3 = st.columns(3)
        col1.metric("Latest month", str(latest_month)[:10])
        col2.metric("Containerized value", f"${latest['containerized_value_usd'].sum() / 1e9:,.2f}B")
        col3.metric("Countries observed", f"{latest['country_name'].nunique():,}")

        by_commodity = (
            latest.groupby(["commodity_code", "commodity_description"], as_index=False)[
                "containerized_value_usd"
            ]
            .sum()
            .sort_values("containerized_value_usd", ascending=False)
        )
        figure = px.bar(
            by_commodity,
            x="commodity_code",
            y="containerized_value_usd",
            hover_data=["commodity_description"],
            labels={
                "commodity_code": "HS commodity",
                "containerized_value_usd": "Containerized value (USD)",
            },
            title="Containerized imports by industrial commodity",
        )
        st.plotly_chart(figure, width="stretch")

with cargo_tab:
    if not flows.empty:
        port_options = sorted(flows["port_name"].unique().tolist())
        selected_ports = st.multiselect("Ports", port_options, default=port_options)
        filtered = flows[flows["port_name"].isin(selected_ports)]
        st.dataframe(filtered, width="stretch", hide_index=True)
    else:
        st.info("Cargo drill-downs will appear after the first successful ingestion.")

with health_tab:
    st.subheader("Recent ingestion runs")
    if runs.empty:
        st.info("No ingestion runs recorded yet.")
    else:
        st.dataframe(runs, width="stretch", hide_index=True)

    st.subheader("Provenance policy")
    st.markdown(
        "- **Observed:** values retrieved from and validated against a named source.\n"
        "- **Reported:** company or port disclosures with a citation.\n"
        "- **Inferred:** analyst model output; never presented as direct shipment ownership."
    )
