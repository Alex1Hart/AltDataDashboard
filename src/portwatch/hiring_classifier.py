from __future__ import annotations

import re
from datetime import date

from portwatch.hiring_config import ClassificationRule, CompanyHiringTaxonomy
from portwatch.models import (
    CompanyExposureRegistry,
    JobClassificationMethod,
    JobLocation,
    JobPosting,
    RegistryEntityType,
)

_SENIORITY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("executive", ("chief ", "vice president", "vp ", "executive director")),
    ("director", ("director", "head of")),
    ("manager", ("manager", "supervisor", "team lead")),
    ("senior", ("senior", "sr.", "sr ", "principal", "staff ", "lead ")),
    ("early_career", ("intern", "internship", "apprentice", "graduate", "entry level")),
)


def classify_posting(
    posting: JobPosting,
    taxonomy: CompanyHiringTaxonomy,
    registry: CompanyExposureRegistry,
    *,
    classification_version: int,
) -> JobPosting:
    """Apply explainable keyword rules and reviewed facility geography matches."""
    headline_text = " ".join(
        value
        for value in (
            posting.title,
            posting.department,
            posting.team,
        )
        if value
    ).casefold()
    description_text = posting.description.casefold()
    business_text = f"{headline_text} {description_text}"
    as_of = posting.ingested_at.date()
    active_business_lines = tuple(
        rule
        for rule in taxonomy.business_lines
        if rule.valid_from <= as_of and (rule.valid_to is None or as_of <= rule.valid_to)
    )
    business_line, business_score = _best_rule(business_text, active_business_lines)
    function, function_score = _best_rule(headline_text, taxonomy.functions)
    themes = tuple(
        rule.name
        for rule in taxonomy.themes
        if _rule_score(f"{headline_text} {description_text}", rule) > 0
    )
    locations = tuple(_match_facility(location, registry, as_of) for location in posting.locations)
    seniority = _classify_seniority(posting.title.casefold())
    classified = bool(business_line or function or themes or seniority)
    max_score = max(business_score, function_score, len(themes), 0)
    confidence = min(0.95, 0.5 + (0.1 * max_score)) if classified else 0.0
    return posting.model_copy(
        update={
            "locations": locations,
            "business_line_id": business_line.rule_id if business_line else None,
            "business_line_name": business_line.name if business_line else None,
            "job_function": function.name if function else None,
            "seniority": seniority,
            "themes": themes,
            "classification_method": (
                JobClassificationMethod.DETERMINISTIC_RULE
                if classified
                else JobClassificationMethod.UNCLASSIFIED
            ),
            "classification_confidence": confidence,
            "classification_version": classification_version,
        }
    )


def _best_rule(
    text: str,
    rules: tuple[ClassificationRule, ...],
) -> tuple[ClassificationRule | None, int]:
    scored = [(rule, _rule_score(text, rule)) for rule in rules]
    positive = [(rule, score) for rule, score in scored if score > 0]
    if not positive:
        return None, 0
    highest = max(score for _, score in positive)
    winners = [rule for rule, score in positive if score == highest]
    if len(winners) != 1:
        return None, highest
    return winners[0], highest


def _rule_score(text: str, rule: ClassificationRule) -> int:
    return sum(_contains_keyword(text, keyword) for keyword in rule.keywords)


def _contains_keyword(text: str, keyword: str) -> int:
    if len(keyword) <= 3 and keyword.isalnum():
        return int(re.search(rf"\b{re.escape(keyword)}\b", text) is not None)
    return int(keyword in text)


def _classify_seniority(title: str) -> str | None:
    for seniority, keywords in _SENIORITY_RULES:
        if any(keyword in title for keyword in keywords):
            return seniority
    return None


def _match_facility(
    location: JobLocation,
    registry: CompanyExposureRegistry,
    as_of: date,
) -> JobLocation:
    candidates: list[tuple[str, float]] = []
    raw = location.raw_location.casefold()
    for entity in registry.entities:
        if entity.entity_type is not RegistryEntityType.FACILITY:
            continue
        if entity.valid_from > as_of or (entity.valid_to is not None and as_of > entity.valid_to):
            continue
        if location.country_code and entity.country_code != location.country_code:
            continue
        locality_match = bool(entity.locality and entity.locality.casefold() in raw)
        region_match = bool(entity.region and entity.region.casefold() in raw)
        if locality_match:
            candidates.append((entity.entity_id, 0.95 if region_match else 0.85))
    if not candidates:
        return location
    highest = max(confidence for _, confidence in candidates)
    winners = [entity_id for entity_id, confidence in candidates if confidence == highest]
    if len(winners) != 1:
        return location
    return location.model_copy(
        update={"facility_entity_id": winners[0], "match_confidence": highest}
    )
