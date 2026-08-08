from __future__ import annotations

from datetime import date
from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class HiringConfigurationError(RuntimeError):
    pass


class CareerSourceType(StrEnum):
    GREENHOUSE = "greenhouse"
    LEVER = "lever"
    HTML_PAGINATED = "html_paginated"


class CareerSourceConfig(BaseModel):
    """A public first-party career feed configured without company-specific code."""

    model_config = ConfigDict(frozen=True)

    source_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    ticker: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,9}$")
    entity_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    source_type: CareerSourceType
    base_url: str = Field(pattern=r"^https://")
    enabled: bool = True
    api_identifier: str | None = None
    page_size: int = Field(default=100, ge=1, le=100)
    max_pages: int | None = Field(default=None, ge=1, le=1_000)
    page_parameter: str = "page"
    first_page: int = Field(default=1, ge=0)
    job_selector: str | None = None
    job_id_attribute: str | None = None
    title_selector: str | None = None
    link_selector: str | None = None
    location_selector: str | None = None
    department_selector: str | None = None
    last_page_selector: str | None = None
    location_delimiter: str | None = None
    detail_json_ld: bool = False
    detail_fetch_limit_per_run: int = Field(default=0, ge=0, le=1_000)
    detail_cache_ttl_days: int = Field(default=30, ge=1, le=365)

    @model_validator(mode="after")
    def adapter_fields_must_be_complete(self) -> CareerSourceConfig:
        if self.source_type in (CareerSourceType.GREENHOUSE, CareerSourceType.LEVER):
            if not self.api_identifier:
                raise ValueError(f"{self.source_type} sources require api_identifier")
        if self.source_type is CareerSourceType.HTML_PAGINATED:
            required = {
                "job_selector": self.job_selector,
                "title_selector": self.title_selector,
                "link_selector": self.link_selector,
                "location_selector": self.location_selector,
                "last_page_selector": self.last_page_selector,
            }
            missing = sorted(name for name, value in required.items() if not value)
            if missing:
                raise ValueError(f"HTML career source is missing selectors: {missing}")
        return self


class ClassificationRule(BaseModel):
    model_config = ConfigDict(frozen=True)

    rule_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    name: str = Field(min_length=1)
    keywords: tuple[str, ...] = Field(min_length=1)

    @field_validator("keywords")
    @classmethod
    def keywords_are_normalized(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(keyword.strip().lower() for keyword in value if keyword.strip())
        if not normalized:
            raise ValueError("classification rules require at least one non-empty keyword")
        return normalized


class BusinessLineRule(ClassificationRule):
    valid_from: date
    valid_to: date | None = None
    evidence_url: str = Field(pattern=r"^https://")
    naics_codes: tuple[str, ...] = ()

    @field_validator("naics_codes")
    @classmethod
    def naics_codes_are_valid(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not code.isdigit() or not 2 <= len(code) <= 6 for code in value):
            raise ValueError("NAICS codes must contain two to six digits")
        return value

    @model_validator(mode="after")
    def dates_must_be_ordered(self) -> BusinessLineRule:
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ValueError("business-line valid_to must not precede valid_from")
        return self


class CompanyHiringTaxonomy(BaseModel):
    model_config = ConfigDict(frozen=True)

    ticker: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,9}$")
    business_lines: tuple[BusinessLineRule, ...] = ()
    functions: tuple[ClassificationRule, ...] = ()
    themes: tuple[ClassificationRule, ...] = ()

    @model_validator(mode="after")
    def rule_ids_must_be_unique(self) -> CompanyHiringTaxonomy:
        rule_ids = [
            rule.rule_id
            for rules in (self.business_lines, self.functions, self.themes)
            for rule in rules
        ]
        if len(rule_ids) != len(set(rule_ids)):
            raise ValueError(f"classification rule ids must be unique for {self.ticker}")
        return self


class HiringWatchConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: int = Field(ge=1)
    missing_snapshots_before_close: int = Field(default=2, ge=1, le=10)
    sources: tuple[CareerSourceConfig, ...]
    companies: tuple[CompanyHiringTaxonomy, ...] = ()

    @model_validator(mode="after")
    def identifiers_must_be_unique(self) -> HiringWatchConfig:
        source_ids = [source.source_id for source in self.sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("career source_id values must be unique")
        tickers = [company.ticker for company in self.companies]
        if len(tickers) != len(set(tickers)):
            raise ValueError("company hiring taxonomies must have unique tickers")
        configured_tickers = set(tickers)
        missing = sorted({source.ticker for source in self.sources} - configured_tickers)
        if missing:
            raise ValueError(f"career sources require company taxonomies: {missing}")
        return self

    def source(self, source_id: str) -> CareerSourceConfig:
        for source in self.sources:
            if source.source_id == source_id:
                return source
        raise HiringConfigurationError(f"unknown career source: {source_id}")

    def company(self, ticker: str) -> CompanyHiringTaxonomy:
        normalized = ticker.upper()
        for company in self.companies:
            if company.ticker == normalized:
                return company
        raise HiringConfigurationError(f"unknown company hiring taxonomy: {ticker}")


def load_hiring_config(path: Path) -> HiringWatchConfig:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return HiringWatchConfig.model_validate(raw)
    except (OSError, yaml.YAMLError, ValueError) as exc:
        message = f"could not load hiring configuration {path}: {exc}"
        raise HiringConfigurationError(message) from exc
