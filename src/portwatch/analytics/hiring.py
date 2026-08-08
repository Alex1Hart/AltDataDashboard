from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import pandas as pd


def compute_hiring_company_signals(
    postings: pd.DataFrame,
    events: pd.DataFrame,
    locations: pd.DataFrame,
    *,
    as_of: date | None = None,
) -> pd.DataFrame:
    columns = [
        "ticker",
        "active_postings",
        "new_postings_28d",
        "closed_postings_28d",
        "net_posting_change_28d",
        "median_active_age_days",
        "active_locations",
        "remote_share",
        "business_line_coverage",
    ]
    if postings.empty:
        return pd.DataFrame(columns=columns)
    cutoff_date = as_of or datetime.now(UTC).date()
    as_of_timestamp = pd.Timestamp(
        datetime(cutoff_date.year, cutoff_date.month, cutoff_date.day, tzinfo=UTC)
    )
    cutoff = as_of_timestamp - timedelta(days=28)
    frame = postings.copy()
    frame["first_seen_at"] = pd.to_datetime(frame["first_seen_at"], utc=True)
    active = frame[frame["status"] == "active"].copy()
    event_frame = events.copy()
    if not event_frame.empty:
        event_frame["observed_at"] = pd.to_datetime(event_frame["observed_at"], utc=True)

    rows: list[dict[str, object]] = []
    for ticker in sorted(frame["ticker"].unique()):
        company_active = active[active["ticker"] == ticker]
        company_events = (
            event_frame[event_frame["ticker"] == ticker] if not event_frame.empty else event_frame
        )
        recent_events = (
            company_events[company_events["observed_at"] >= cutoff]
            if not company_events.empty
            else company_events
        )
        opened = (
            int((recent_events["event_type"] == "opened").sum()) if not recent_events.empty else 0
        )
        reopened = (
            int((recent_events["event_type"] == "reopened").sum()) if not recent_events.empty else 0
        )
        closed = (
            int((recent_events["event_type"] == "closed").sum()) if not recent_events.empty else 0
        )
        age_days = (as_of_timestamp - company_active["first_seen_at"]).dt.days
        company_locations = (
            locations[(locations["ticker"] == ticker) & (locations["status"] == "active")]
            if not locations.empty
            else locations
        )
        if company_locations.empty:
            remote_share = 0.0
        else:
            remote_by_job = company_locations.groupby(
                ["source_id", "source_job_id"],
                sort=False,
            )["is_remote"].any()
            remote_share = float(remote_by_job.mean())
        classified_count = int(company_active["business_line_name"].notna().sum())
        rows.append(
            {
                "ticker": ticker,
                "active_postings": len(company_active),
                "new_postings_28d": opened + reopened,
                "closed_postings_28d": closed,
                "net_posting_change_28d": opened + reopened - closed,
                "median_active_age_days": float(age_days.median()) if not age_days.empty else 0.0,
                "active_locations": (
                    int(company_locations["location_id"].nunique())
                    if not company_locations.empty
                    else 0
                ),
                "remote_share": remote_share,
                "business_line_coverage": (
                    classified_count / len(company_active) if len(company_active) else 0.0
                ),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def hiring_activity_daily(events: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "observed_date",
        "baseline",
        "opened",
        "updated",
        "closed",
        "reopened",
        "net_change",
    ]
    if events.empty:
        return pd.DataFrame(columns=columns)
    frame = events.copy()
    frame["observed_date"] = pd.to_datetime(frame["observed_at"], utc=True).dt.date
    result = (
        frame.groupby(["observed_date", "event_type"]).size().unstack(fill_value=0).reset_index()
    )
    for event_type in ("baseline", "opened", "updated", "closed", "reopened"):
        if event_type not in result:
            result[event_type] = 0
    result["net_change"] = result["opened"] + result["reopened"] - result["closed"]
    return result[columns].sort_values("observed_date")


def business_line_hiring_summary(postings: pd.DataFrame) -> pd.DataFrame:
    if postings.empty:
        return pd.DataFrame(columns=["business_line", "active_postings", "share"])
    active = postings[postings["status"] == "active"].copy()
    if active.empty:
        return pd.DataFrame(columns=["business_line", "active_postings", "share"])
    active["business_line"] = active["business_line_name"].fillna("Unclassified")
    active["_job_key"] = active["source_id"] + "\x1f" + active["source_job_id"]
    result = (
        active.groupby("business_line", as_index=False)["_job_key"]
        .nunique()
        .rename(columns={"_job_key": "active_postings"})
    )
    result["share"] = result["active_postings"] / result["active_postings"].sum()
    return result.sort_values("active_postings", ascending=False)


def function_hiring_summary(postings: pd.DataFrame) -> pd.DataFrame:
    return _active_category_summary(postings, "job_function", "job_function")


def theme_hiring_summary(postings: pd.DataFrame) -> pd.DataFrame:
    columns = ["theme", "active_postings"]
    if postings.empty:
        return pd.DataFrame(columns=columns)
    active = postings[postings["status"] == "active"].copy()
    if active.empty:
        return pd.DataFrame(columns=columns)
    active["_job_key"] = active["source_id"] + "\x1f" + active["source_job_id"]
    active = active.assign(theme=active["themes_json"].map(json.loads)).explode("theme")
    return (
        active.dropna(subset=["theme"])
        .groupby("theme", as_index=False)["_job_key"]
        .nunique()
        .rename(columns={"_job_key": "active_postings"})
        .sort_values("active_postings", ascending=False)
    )


def _active_category_summary(
    postings: pd.DataFrame,
    source_column: str,
    result_column: str,
) -> pd.DataFrame:
    columns = [result_column, "active_postings"]
    if postings.empty:
        return pd.DataFrame(columns=columns)
    active = postings[postings["status"] == "active"].copy()
    if active.empty:
        return pd.DataFrame(columns=columns)
    active["_job_key"] = active["source_id"] + "\x1f" + active["source_job_id"]
    active[result_column] = active[source_column].fillna("Unclassified")
    return (
        active.groupby(result_column, as_index=False)["_job_key"]
        .nunique()
        .rename(columns={"_job_key": "active_postings"})
        .sort_values("active_postings", ascending=False)
    )


def location_hiring_summary(locations: pd.DataFrame) -> pd.DataFrame:
    columns = ["raw_location", "city", "region", "country", "active_postings"]
    if locations.empty:
        return pd.DataFrame(columns=columns)
    active = locations[locations["status"] == "active"].copy()
    if active.empty:
        return pd.DataFrame(columns=columns)
    active["_job_key"] = active["source_id"] + "\x1f" + active["source_job_id"]
    return (
        active.groupby(["raw_location", "city", "region", "country"], dropna=False)["_job_key"]
        .nunique()
        .rename("active_postings")
        .reset_index()
        .sort_values("active_postings", ascending=False)
    )


def labor_market_wide(observations: pd.DataFrame) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame()
    return (
        observations.pivot_table(
            index=[
                "period_start",
                "geography_name",
                "geography_code",
                "industry_code",
                "industry_name",
            ],
            columns="metric",
            values="value",
            aggfunc="last",
        )
        .reset_index()
        .sort_values("period_start")
    )
