from __future__ import annotations

from datetime import date
from typing import Literal

import pandas as pd

CONTRACT_SIGNAL_COLUMNS = [
    "ticker",
    "as_of_date",
    "award_count",
    "transaction_count",
    "transaction_coverage",
    "total_current_obligations_usd",
    "ttm_net_obligations_usd",
    "prior_ttm_net_obligations_usd",
    "ttm_net_obligations_yoy",
    "ttm_gross_obligations_usd",
    "ttm_deobligations_usd",
    "ttm_modification_count",
    "ttm_new_award_obligations_usd",
    "prior_ttm_new_award_obligations_usd",
    "ttm_new_award_yoy",
    "active_award_obligations_usd",
    "next_12m_expiring_award_value_usd",
    "agency_hhi",
    "top_awarding_agency",
    "latest_action_date",
    "latest_source_modified_at",
]


def compute_contract_company_signals(
    awards: pd.DataFrame,
    transactions: pd.DataFrame | None = None,
    *,
    as_of: date | None = None,
) -> pd.DataFrame:
    """Summarize current award snapshots without treating obligations as revenue/backlog."""
    if awards.empty:
        return pd.DataFrame(columns=CONTRACT_SIGNAL_COLUMNS)

    analysis_date = pd.Timestamp(as_of or date.today())
    current_window_start = analysis_date - pd.DateOffset(years=1) + pd.Timedelta(1, unit="D")
    prior_window_start = current_window_start - pd.DateOffset(years=1)
    next_year_end = analysis_date + pd.DateOffset(years=1)

    working = awards.copy()
    for column in ("base_obligation_date", "start_date", "end_date", "source_modified_at"):
        working[column] = pd.to_datetime(working[column], errors="coerce", utc=False)
    working["award_amount_usd"] = pd.to_numeric(
        working["award_amount_usd"], errors="coerce"
    ).fillna(0.0)
    transaction_frame = _normalize_transactions(transactions)

    rows: list[dict[str, object]] = []
    for ticker, company_awards in working.groupby("ticker", sort=True):
        company_transactions = transaction_frame[transaction_frame["ticker"] == ticker]
        transaction_dates = company_transactions["action_date"]
        current_transactions = company_transactions[
            transaction_dates.between(current_window_start, analysis_date, inclusive="both")
        ]
        prior_transactions = company_transactions[
            transaction_dates.between(
                prior_window_start,
                current_window_start,
                inclusive="left",
            )
        ]
        current_obligations = current_transactions["federal_action_obligation_usd"]
        prior_net_obligations = float(prior_transactions["federal_action_obligation_usd"].sum())
        current_net_obligations = float(current_obligations.sum())
        covered_awards = int(company_transactions["award_key"].nunique())
        award_count = int(company_awards["award_key"].nunique())
        base_dates = company_awards["base_obligation_date"]
        current_ttm = company_awards[
            base_dates.between(current_window_start, analysis_date, inclusive="both")
        ]
        prior_ttm = company_awards[
            base_dates.between(prior_window_start, current_window_start, inclusive="left")
        ]
        current_amount = float(current_ttm["award_amount_usd"].sum())
        prior_amount = float(prior_ttm["award_amount_usd"].sum())
        start_dates = company_awards["start_date"]
        end_dates = company_awards["end_date"]
        active = company_awards[
            (start_dates.isna() | start_dates.le(analysis_date))
            & (end_dates.isna() | end_dates.ge(analysis_date))
        ]
        expiring = company_awards[end_dates.gt(analysis_date) & end_dates.le(next_year_end)]

        agency_totals = (
            company_awards.dropna(subset=["awarding_agency"])
            .groupby("awarding_agency")["award_amount_usd"]
            .sum()
        )
        positive_agency_totals = agency_totals[agency_totals > 0]
        agency_total = float(positive_agency_totals.sum())
        agency_hhi = (
            float(((positive_agency_totals / agency_total) ** 2).sum())
            if agency_total > 0
            else float("nan")
        )
        top_agency = str(agency_totals.idxmax()) if not agency_totals.empty else None
        rows.append(
            {
                "ticker": str(ticker),
                "as_of_date": analysis_date.date(),
                "award_count": award_count,
                "transaction_count": len(company_transactions),
                "transaction_coverage": covered_awards / award_count if award_count else 0.0,
                "total_current_obligations_usd": float(company_awards["award_amount_usd"].sum()),
                "ttm_net_obligations_usd": current_net_obligations,
                "prior_ttm_net_obligations_usd": prior_net_obligations,
                "ttm_net_obligations_yoy": (
                    current_net_obligations / prior_net_obligations - 1
                    if prior_net_obligations > 0
                    else float("nan")
                ),
                "ttm_gross_obligations_usd": float(current_obligations.clip(lower=0).sum()),
                "ttm_deobligations_usd": float(-current_obligations.clip(upper=0).sum()),
                "ttm_modification_count": int(
                    (~current_transactions["modification_number"].isin(["", "0"])).sum()
                ),
                "ttm_new_award_obligations_usd": current_amount,
                "prior_ttm_new_award_obligations_usd": prior_amount,
                "ttm_new_award_yoy": (
                    current_amount / prior_amount - 1 if prior_amount != 0 else float("nan")
                ),
                "active_award_obligations_usd": float(active["award_amount_usd"].sum()),
                "next_12m_expiring_award_value_usd": float(expiring["award_amount_usd"].sum()),
                "agency_hhi": agency_hhi,
                "top_awarding_agency": top_agency,
                "latest_action_date": company_transactions["action_date"].max(),
                "latest_source_modified_at": company_awards["source_modified_at"].max(),
            }
        )
    return pd.DataFrame(rows, columns=CONTRACT_SIGNAL_COLUMNS)


def monthly_contract_awards(awards: pd.DataFrame) -> pd.DataFrame:
    """Aggregate current award values by original obligation month for visualization."""
    if awards.empty:
        return pd.DataFrame(columns=["ticker", "award_month", "award_amount_usd", "award_count"])
    working = awards.copy()
    working["base_obligation_date"] = pd.to_datetime(
        working["base_obligation_date"], errors="coerce"
    )
    working["award_amount_usd"] = pd.to_numeric(
        working["award_amount_usd"], errors="coerce"
    ).fillna(0.0)
    working = working.dropna(subset=["base_obligation_date"])
    working["award_month"] = working["base_obligation_date"].dt.to_period("M").dt.to_timestamp()
    return (
        working.groupby(["ticker", "award_month"], as_index=False)
        .agg(
            award_amount_usd=("award_amount_usd", "sum"),
            award_count=("award_key", "nunique"),
        )
        .sort_values(["ticker", "award_month"])
    )


def monthly_contract_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    """Aggregate signed obligations and deobligations by action month."""
    columns = [
        "ticker",
        "action_month",
        "net_obligations_usd",
        "gross_obligations_usd",
        "deobligations_usd",
        "transaction_count",
    ]
    working = _normalize_transactions(transactions)
    if working.empty:
        return pd.DataFrame(columns=columns)
    working = working.dropna(subset=["action_date"])
    working["action_month"] = working["action_date"].dt.to_period("M").dt.to_timestamp()
    working["gross_obligations_usd"] = working["federal_action_obligation_usd"].clip(lower=0)
    working["deobligations_usd"] = -working["federal_action_obligation_usd"].clip(upper=0)
    return (
        working.groupby(["ticker", "action_month"], as_index=False)
        .agg(
            net_obligations_usd=("federal_action_obligation_usd", "sum"),
            gross_obligations_usd=("gross_obligations_usd", "sum"),
            deobligations_usd=("deobligations_usd", "sum"),
            transaction_count=("transaction_id", "nunique"),
        )
        .sort_values(["ticker", "action_month"])
    )


def contract_award_detail(
    awards: pd.DataFrame,
    transactions: pd.DataFrame,
) -> pd.DataFrame:
    """Attach signed transaction history to current award inventory without mixing grains."""
    if awards.empty:
        return awards.copy()
    details = awards.copy()
    working = _normalize_transactions(transactions)
    if working.empty:
        return details.assign(
            transaction_count=0,
            transaction_net_obligations_usd=0.0,
            transaction_gross_obligations_usd=0.0,
            transaction_deobligations_usd=0.0,
            latest_action_date=pd.NaT,
            latest_modification_number=None,
        )
    working["positive_obligation"] = working["federal_action_obligation_usd"].clip(lower=0)
    working["deobligation"] = -working["federal_action_obligation_usd"].clip(upper=0)
    aggregates = working.groupby("award_key", as_index=False).agg(
        transaction_count=("transaction_id", "nunique"),
        transaction_net_obligations_usd=("federal_action_obligation_usd", "sum"),
        transaction_gross_obligations_usd=("positive_obligation", "sum"),
        transaction_deobligations_usd=("deobligation", "sum"),
        latest_action_date=("action_date", "max"),
    )
    latest_modifications = (
        working.sort_values(["action_date", "transaction_id"])
        .drop_duplicates("award_key", keep="last")[["award_key", "modification_number"]]
        .rename(columns={"modification_number": "latest_modification_number"})
    )
    return details.merge(aggregates, on="award_key", how="left").merge(
        latest_modifications,
        on="award_key",
        how="left",
    )


def contract_obligation_breakdown(
    awards: pd.DataFrame,
    transactions: pd.DataFrame,
    *,
    dimension: Literal["awarding_agency", "naics_code", "psc_code"],
) -> pd.DataFrame:
    """Attribute signed transaction flows to one stable parent-award dimension."""
    columns = [
        dimension,
        "net_obligations_usd",
        "gross_obligations_usd",
        "deobligations_usd",
        "transaction_count",
    ]
    if awards.empty or transactions.empty:
        return pd.DataFrame(columns=columns)
    award_dimensions = awards[["award_key", dimension]].drop_duplicates("award_key")
    working = _normalize_transactions(transactions).merge(
        award_dimensions,
        on="award_key",
        how="left",
    )
    working = working.dropna(subset=[dimension])
    working["gross_obligations_usd"] = working["federal_action_obligation_usd"].clip(lower=0)
    working["deobligations_usd"] = -working["federal_action_obligation_usd"].clip(upper=0)
    return (
        working.groupby(dimension, as_index=False)
        .agg(
            net_obligations_usd=("federal_action_obligation_usd", "sum"),
            gross_obligations_usd=("gross_obligations_usd", "sum"),
            deobligations_usd=("deobligations_usd", "sum"),
            transaction_count=("transaction_id", "nunique"),
        )
        .sort_values("gross_obligations_usd", ascending=False)
    )


def _normalize_transactions(transactions: pd.DataFrame | None) -> pd.DataFrame:
    columns = [
        "transaction_id",
        "award_key",
        "ticker",
        "action_date",
        "federal_action_obligation_usd",
        "modification_number",
    ]
    if transactions is None or transactions.empty:
        return pd.DataFrame(columns=columns)
    working = transactions.copy()
    working["action_date"] = pd.to_datetime(working["action_date"], errors="coerce")
    working["federal_action_obligation_usd"] = pd.to_numeric(
        working["federal_action_obligation_usd"],
        errors="coerce",
    ).fillna(0.0)
    working["modification_number"] = working["modification_number"].fillna("").astype(str)
    return working
