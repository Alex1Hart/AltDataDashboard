from __future__ import annotations

from datetime import date

import pandas as pd

CONTRACT_SIGNAL_COLUMNS = [
    "ticker",
    "as_of_date",
    "award_count",
    "total_current_obligations_usd",
    "ttm_new_award_obligations_usd",
    "prior_ttm_new_award_obligations_usd",
    "ttm_new_award_yoy",
    "active_award_obligations_usd",
    "next_12m_expiring_award_value_usd",
    "agency_hhi",
    "top_awarding_agency",
    "latest_source_modified_at",
]


def compute_contract_company_signals(
    awards: pd.DataFrame,
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

    rows: list[dict[str, object]] = []
    for ticker, company_awards in working.groupby("ticker", sort=True):
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
                "award_count": int(company_awards["award_key"].nunique()),
                "total_current_obligations_usd": float(company_awards["award_amount_usd"].sum()),
                "ttm_new_award_obligations_usd": current_amount,
                "prior_ttm_new_award_obligations_usd": prior_amount,
                "ttm_new_award_yoy": (
                    current_amount / prior_amount - 1 if prior_amount != 0 else float("nan")
                ),
                "active_award_obligations_usd": float(active["award_amount_usd"].sum()),
                "next_12m_expiring_award_value_usd": float(expiring["award_amount_usd"].sum()),
                "agency_hhi": agency_hhi,
                "top_awarding_agency": top_agency,
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
