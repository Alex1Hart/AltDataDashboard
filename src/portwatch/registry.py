from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from portwatch.models import CompanyExposureRegistry


def load_company_registry(path: Path) -> CompanyExposureRegistry:
    with path.open(encoding="utf-8") as registry_file:
        payload = yaml.safe_load(registry_file)
    return CompanyExposureRegistry.model_validate(payload)


def registry_exposures_frame(registry: CompanyExposureRegistry) -> pd.DataFrame:
    rows = [
        {
            "ticker": company.ticker,
            "company_name": company.company_name,
            "confidence": company.confidence.value,
            "hs_code": exposure.hs_code,
            "weight": exposure.weight,
            "direction": exposure.direction,
            "rationale": exposure.rationale,
            "limitations": company.limitations,
        }
        for company in registry.companies
        for exposure in company.commodity_exposures
    ]
    return pd.DataFrame(rows)


def company_exposure_scores(
    signals: pd.DataFrame,
    registry: CompanyExposureRegistry,
) -> pd.DataFrame:
    """Map the latest commodity z-scores to reviewed company exposure weights."""
    if signals.empty:
        return pd.DataFrame(
            columns=[
                "ticker",
                "company_name",
                "signal_month",
                "weighted_zscore",
                "matched_observations",
                "confidence",
            ]
        )

    latest_month = signals["month"].max()
    latest = signals[signals["month"] == latest_month]
    rows: list[dict[str, object]] = []
    for company in registry.companies:
        weighted_values: list[float] = []
        weights: list[float] = []
        for exposure in company.commodity_exposures:
            matches = latest[
                latest["commodity_code"].astype(str).str.startswith(exposure.hs_code)
            ].copy()
            if company.port_weights:
                matches = matches[matches["port_code"].isin(company.port_weights)]
            for match in matches.itertuples(index=False):
                zscore = match.value_24m_zscore
                if pd.isna(zscore):
                    continue
                port_weight = company.port_weights.get(str(match.port_code), 1.0)
                combined_weight = exposure.weight * port_weight
                weighted_values.append(float(zscore) * combined_weight)
                weights.append(combined_weight)

        rows.append(
            {
                "ticker": company.ticker,
                "company_name": company.company_name,
                "signal_month": latest_month,
                "weighted_zscore": (
                    sum(weighted_values) / sum(weights) if weights else float("nan")
                ),
                "matched_observations": len(weights),
                "confidence": company.confidence.value,
            }
        )
    return pd.DataFrame(rows)
