from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from portwatch.analytics.signals import compute_trade_signals, latest_trade_signals
from portwatch.registry import company_exposure_scores, load_company_registry


def test_signals_and_company_exposure_are_deterministic() -> None:
    months = pd.date_range("2024-01-01", periods=24, freq="MS")
    rows = []
    for index, month in enumerate(months, start=1):
        for country_code, share in (("5700", 0.75), ("5880", 0.25)):
            rows.append(
                {
                    "month": date(month.year, month.month, 1),
                    "port_code": "2704",
                    "port_name": "Los Angeles, CA",
                    "commodity_code": "84",
                    "commodity_description": "Machinery",
                    "country_code": country_code,
                    "containerized_value_usd": index * 100 * share,
                    "containerized_weight_kg": index * 10 * share,
                }
            )
    flows = pd.DataFrame(rows)

    signals = compute_trade_signals(flows)
    latest = latest_trade_signals(signals).iloc[0]
    registry = load_company_registry(Path("config/company_exposures.yml"))
    scores = company_exposure_scores(signals, registry)

    assert latest["value_yoy"] == 1.0
    assert latest["country_hhi"] == 0.625
    assert latest["unit_value_usd_per_kg"] == 10.0
    assert scores.iloc[0]["ticker"] == "CAT"
    assert scores.iloc[0]["matched_observations"] == 1
