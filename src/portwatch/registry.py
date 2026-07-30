from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from portwatch.models import CompanyExposureRegistry, EntityIdentifierScheme


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


def registry_entities_frame(
    registry: CompanyExposureRegistry,
    *,
    as_of: date | None = None,
) -> pd.DataFrame:
    """Flatten graph nodes and retain their supported validity interval."""
    columns = [
        "entity_id",
        "entity_type",
        "name",
        "issuer_tickers",
        "aliases",
        "country_code",
        "region",
        "locality",
        "valid_from",
        "valid_to",
        "identifiers",
        "evidence_ids",
    ]
    issuer_tickers: dict[str, list[str]] = {}
    for company in registry.companies:
        if company.root_entity_id is not None:
            issuer_tickers.setdefault(company.root_entity_id, []).append(company.ticker)

    rows: list[dict[str, object]] = []
    for entity in registry.entities:
        if not _is_active(entity.valid_from, entity.valid_to, as_of):
            continue
        active_identifiers = [
            identifier
            for identifier in entity.identifiers
            if _is_active(identifier.valid_from, identifier.valid_to, as_of)
        ]
        rows.append(
            {
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type.value,
                "name": entity.name,
                "issuer_tickers": ", ".join(issuer_tickers.get(entity.entity_id, [])),
                "aliases": ", ".join(entity.aliases),
                "country_code": entity.country_code,
                "region": entity.region,
                "locality": entity.locality,
                "valid_from": entity.valid_from,
                "valid_to": entity.valid_to,
                "identifiers": ", ".join(
                    f"{identifier.scheme.value}:{identifier.value}"
                    for identifier in active_identifiers
                ),
                "evidence_ids": ", ".join(entity.evidence_ids),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def registry_identifiers_frame(
    registry: CompanyExposureRegistry,
    *,
    as_of: date | None = None,
) -> pd.DataFrame:
    """Flatten source identifiers for collector and entity-resolution joins."""
    columns = [
        "entity_id",
        "entity_name",
        "scheme",
        "value",
        "valid_from",
        "valid_to",
        "evidence_ids",
    ]
    rows = [
        {
            "entity_id": entity.entity_id,
            "entity_name": entity.name,
            "scheme": identifier.scheme.value,
            "value": identifier.value,
            "valid_from": identifier.valid_from,
            "valid_to": identifier.valid_to,
            "evidence_ids": ", ".join(identifier.evidence_ids),
        }
        for entity in registry.entities
        for identifier in entity.identifiers
        if _is_active(entity.valid_from, entity.valid_to, as_of)
        and _is_active(identifier.valid_from, identifier.valid_to, as_of)
    ]
    return pd.DataFrame(rows, columns=columns)


def registry_relationships_frame(
    registry: CompanyExposureRegistry,
    *,
    as_of: date | None = None,
) -> pd.DataFrame:
    """Flatten dated graph edges with human-readable endpoint metadata."""
    columns = [
        "relationship_id",
        "parent_entity_id",
        "parent_name",
        "relationship_type",
        "child_entity_id",
        "child_name",
        "child_type",
        "ownership_percent",
        "confidence",
        "valid_from",
        "valid_to",
        "evidence_ids",
    ]
    entity_by_id = {entity.entity_id: entity for entity in registry.entities}
    rows: list[dict[str, object]] = []
    for relationship in registry.relationships:
        if not _is_active(relationship.valid_from, relationship.valid_to, as_of):
            continue
        parent = entity_by_id[relationship.parent_entity_id]
        child = entity_by_id[relationship.child_entity_id]
        rows.append(
            {
                "relationship_id": relationship.relationship_id,
                "parent_entity_id": parent.entity_id,
                "parent_name": parent.name,
                "relationship_type": relationship.relationship_type.value,
                "child_entity_id": child.entity_id,
                "child_name": child.name,
                "child_type": child.entity_type.value,
                "ownership_percent": relationship.ownership_percent,
                "confidence": relationship.confidence.value,
                "valid_from": relationship.valid_from,
                "valid_to": relationship.valid_to,
                "evidence_ids": ", ".join(relationship.evidence_ids),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def company_entity_ids(
    registry: CompanyExposureRegistry,
    ticker: str,
    *,
    as_of: date | None = None,
) -> tuple[str, ...]:
    """Return active issuer, subsidiary, and facility IDs reachable for a ticker."""
    company = next((item for item in registry.companies if item.ticker == ticker), None)
    if company is None:
        raise KeyError(f"unknown company ticker: {ticker}")
    if company.root_entity_id is None:
        return ()

    entity_by_id = {entity.entity_id: entity for entity in registry.entities}
    root = entity_by_id[company.root_entity_id]
    if not _is_active(root.valid_from, root.valid_to, as_of):
        return ()

    adjacency: dict[str, list[str]] = {entity_id: [] for entity_id in entity_by_id}
    for relationship in registry.relationships:
        if _is_active(relationship.valid_from, relationship.valid_to, as_of):
            adjacency[relationship.parent_entity_id].append(relationship.child_entity_id)

    reachable: list[str] = []
    pending = [root.entity_id]
    while pending:
        entity_id = pending.pop(0)
        if entity_id in reachable:
            continue
        entity = entity_by_id[entity_id]
        if not _is_active(entity.valid_from, entity.valid_to, as_of):
            continue
        reachable.append(entity_id)
        pending.extend(adjacency[entity_id])
    return tuple(reachable)


def resolve_entity_id(
    registry: CompanyExposureRegistry,
    *,
    scheme: EntityIdentifierScheme,
    value: str,
    as_of: date | None = None,
) -> str | None:
    """Resolve one active external identifier to its reviewed internal entity ID."""
    for entity in registry.entities:
        if not _is_active(entity.valid_from, entity.valid_to, as_of):
            continue
        for identifier in entity.identifiers:
            if (
                identifier.scheme is scheme
                and identifier.value == value
                and _is_active(identifier.valid_from, identifier.valid_to, as_of)
            ):
                return entity.entity_id
    return None


def _is_active(valid_from: date, valid_to: date | None, as_of: date | None) -> bool:
    if as_of is None:
        return True
    return valid_from <= as_of and (valid_to is None or as_of <= valid_to)


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
