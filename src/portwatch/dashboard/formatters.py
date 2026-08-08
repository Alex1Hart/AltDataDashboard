from __future__ import annotations

from typing import Any

import pandas as pd


def format_percent(value: Any) -> str:
    return "—" if pd.isna(value) else f"{float(value):.1%}"


def format_number(value: Any) -> str:
    return "—" if pd.isna(value) else f"{float(value):.2f}"


def format_money(value: Any) -> str:
    if pd.isna(value):
        return "—"
    numeric = float(value)
    if abs(numeric) >= 1e9:
        return f"${numeric / 1e9:,.2f}B"
    if abs(numeric) >= 1e6:
        return f"${numeric / 1e6:,.1f}M"
    return f"${numeric:,.0f}"


def format_fact_value(value: Any, unit: str) -> str:
    if pd.isna(value):
        return "—"
    numeric = float(value)
    if unit == "USD":
        return format_money(numeric)
    if abs(numeric) >= 1e9:
        return f"{numeric / 1e9:,.2f}B {unit}"
    if abs(numeric) >= 1e6:
        return f"{numeric / 1e6:,.1f}M {unit}"
    return f"{numeric:,.2f} {unit}"
