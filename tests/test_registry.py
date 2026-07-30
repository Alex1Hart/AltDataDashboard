from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from portwatch.models import (
    CompanyExposureRegistry,
    EntityIdentifierScheme,
    RegistryEntityType,
)
from portwatch.registry import (
    company_entity_ids,
    load_company_registry,
    registry_entities_frame,
    registry_identifiers_frame,
    registry_relationships_frame,
    resolve_entity_id,
)

REGISTRY_PATH = Path("config/company_exposures.yml")


def test_registry_loads_dated_company_entity_graph() -> None:
    registry = load_company_registry(REGISTRY_PATH)

    assert registry.version == 2
    assert registry.companies[0].root_entity_id == "cat_inc"
    assert {entity.entity_type for entity in registry.entities} == {
        RegistryEntityType.ISSUER,
        RegistryEntityType.SUBSIDIARY,
        RegistryEntityType.FACILITY,
    }

    entities = registry_entities_frame(registry)
    identifiers = registry_identifiers_frame(registry)
    relationships = registry_relationships_frame(registry)
    assert set(entities["entity_id"]) == {
        "cat_inc",
        "cat_financial",
        "cat_east_peoria_operations",
    }
    assert set(identifiers["scheme"]) == {"ticker", "sec_cik"}
    assert set(relationships["relationship_type"]) == {"owns", "operates"}

    historical_entities = registry_entities_frame(registry, as_of=date(2025, 1, 1))
    historical_relationships = registry_relationships_frame(registry, as_of=date(2025, 1, 1))
    assert historical_entities["entity_id"].tolist() == ["cat_inc"]
    assert historical_relationships.empty
    assert company_entity_ids(registry, "CAT") == (
        "cat_inc",
        "cat_financial",
        "cat_east_peoria_operations",
    )
    assert company_entity_ids(registry, "CAT", as_of=date(2025, 1, 1)) == ("cat_inc",)
    assert (
        resolve_entity_id(
            registry,
            scheme=EntityIdentifierScheme.SEC_CIK,
            value="0000018230",
        )
        == "cat_inc"
    )
    assert (
        resolve_entity_id(
            registry,
            scheme=EntityIdentifierScheme.SEC_CIK,
            value="0000018230",
            as_of=date(2025, 1, 1),
        )
        is None
    )


def test_registry_rejects_relationships_with_unknown_entities() -> None:
    payload = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    payload["relationships"][0]["child_entity_id"] = "missing_entity"

    with pytest.raises(ValidationError, match="unknown entity"):
        CompanyExposureRegistry.model_validate(payload)


def test_registry_rejects_cycles() -> None:
    payload = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    payload["relationships"].append(
        {
            "relationship_id": "cat_financial_owns_parent",
            "parent_entity_id": "cat_financial",
            "child_entity_id": "cat_inc",
            "relationship_type": "owns",
            "ownership_percent": 100,
            "confidence": "high",
            "valid_from": date(2025, 12, 31),
            "valid_to": None,
            "evidence_ids": ["cat_2025_subsidiaries"],
        }
    )

    with pytest.raises(ValidationError, match="acyclic"):
        CompanyExposureRegistry.model_validate(payload)


def test_version_one_registry_remains_loadable_without_graph_fields() -> None:
    payload = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8"))
    payload["version"] = 1
    payload.pop("entities")
    payload.pop("relationships")
    for company in payload["companies"]:
        company.pop("root_entity_id")

    registry = CompanyExposureRegistry.model_validate(payload)

    assert registry.version == 1
    assert registry.entities == ()
    assert registry.relationships == ()
