from __future__ import annotations

import numpy as np
import pandas as pd


def compute_trade_signals(flows: pd.DataFrame) -> pd.DataFrame:
    """Calculate transparent monthly signals without mutating source observations."""
    columns = [
        "month",
        "port_code",
        "port_name",
        "commodity_code",
        "commodity_description",
        "containerized_value_usd",
        "containerized_weight_kg",
    ]
    if flows.empty:
        return pd.DataFrame(
            columns=[
                *columns,
                "unit_value_usd_per_kg",
                "country_hhi",
                "value_yoy",
                "value_3m_momentum",
                "value_24m_zscore",
            ]
        )

    missing = set([*columns, "country_code"]) - set(flows.columns)
    if missing:
        raise ValueError(f"trade-flow data is missing signal inputs: {sorted(missing)}")

    group_keys = [
        "month",
        "port_code",
        "port_name",
        "commodity_code",
        "commodity_description",
    ]
    monthly = (
        flows.groupby(group_keys, as_index=False)[
            ["containerized_value_usd", "containerized_weight_kg"]
        ]
        .sum()
        .sort_values(["port_code", "commodity_code", "month"])
    )
    monthly["unit_value_usd_per_kg"] = np.where(
        monthly["containerized_weight_kg"] > 0,
        monthly["containerized_value_usd"] / monthly["containerized_weight_kg"],
        np.nan,
    )

    country_keys = ["month", "port_code", "commodity_code", "country_code"]
    country = flows.groupby(country_keys, as_index=False)["containerized_value_usd"].sum()
    concentration_keys = ["month", "port_code", "commodity_code"]
    country["total_value"] = country.groupby(concentration_keys)[
        "containerized_value_usd"
    ].transform("sum")
    country["share_squared"] = np.where(
        country["total_value"] > 0,
        (country["containerized_value_usd"] / country["total_value"]) ** 2,
        np.nan,
    )
    hhi = country.groupby(concentration_keys, as_index=False)["share_squared"].sum()
    hhi = hhi.rename(columns={"share_squared": "country_hhi"})
    monthly = monthly.merge(hhi, on=concentration_keys, how="left")

    signal_groups: list[pd.DataFrame] = []
    for _, group in monthly.groupby(["port_code", "commodity_code"], sort=False):
        group = group.sort_values("month").copy()
        values = group["containerized_value_usd"]
        group["value_yoy"] = values / values.shift(12) - 1
        trailing_three = values.rolling(3, min_periods=3).sum()
        group["value_3m_momentum"] = trailing_three / trailing_three.shift(3) - 1
        rolling_mean = values.rolling(24, min_periods=12).mean()
        rolling_std = values.rolling(24, min_periods=12).std(ddof=1)
        group["value_24m_zscore"] = (values - rolling_mean) / rolling_std.replace(0, np.nan)
        signal_groups.append(group)

    return pd.concat(signal_groups, ignore_index=True)


def latest_trade_signals(signals: pd.DataFrame) -> pd.DataFrame:
    if signals.empty:
        return signals.copy()
    latest_month = signals["month"].max()
    return signals[signals["month"] == latest_month].copy()
