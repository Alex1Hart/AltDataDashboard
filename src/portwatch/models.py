from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SourceName(StrEnum):
    CENSUS_PORT_HS = "census_port_hs"
    PORT_OF_LA_CONTAINER_STATS = "port_of_la_container_stats"


class IngestionStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class PortMetricName(StrEnum):
    LOADED_IMPORT_TEU = "loaded_import_teu"
    LOADED_EXPORT_TEU = "loaded_export_teu"
    TOTAL_LOADED_TEU = "total_loaded_teu"
    TOTAL_EMPTY_TEU = "total_empty_teu"
    TOTAL_TEU = "total_teu"


class EvidenceType(StrEnum):
    SEC_FILING = "sec_filing"
    COMPANY_DISCLOSURE = "company_disclosure"
    ANALYST_NOTE = "analyst_note"


class ConfidenceLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class TradeFlow(BaseModel):
    """Normalized monthly import observation at port, commodity, and country grain."""

    model_config = ConfigDict(frozen=True)

    month: date
    port_code: str = Field(pattern=r"^\d{4}$")
    port_name: str = Field(min_length=1)
    commodity_code: str = Field(pattern=r"^\d{2,6}$")
    commodity_description: str = Field(min_length=1)
    country_code: str = Field(pattern=r"^\d{4}$")
    country_name: str = Field(min_length=1)
    general_value_usd: Decimal = Field(ge=0)
    vessel_value_usd: Decimal = Field(ge=0)
    vessel_weight_kg: Decimal = Field(ge=0)
    containerized_value_usd: Decimal = Field(ge=0)
    containerized_weight_kg: Decimal = Field(ge=0)
    source: SourceName = SourceName.CENSUS_PORT_HS
    source_updated_at: datetime | None = None
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("month")
    @classmethod
    def month_must_be_first_day(cls, value: date) -> date:
        if value.day != 1:
            raise ValueError("month must be represented by its first day")
        return value

    @property
    def natural_key(self) -> tuple[date, str, str, str, SourceName]:
        return (
            self.month,
            self.port_code,
            self.commodity_code,
            self.country_code,
            self.source,
        )


class PortOperation(BaseModel):
    """Normalized public port metric with publication and availability timestamps."""

    model_config = ConfigDict(frozen=True)

    period_start: date
    frequency: str = Field(pattern=r"^(daily|weekly|monthly)$")
    port_code: str = Field(pattern=r"^\d{4}$")
    port_name: str = Field(min_length=1)
    metric: PortMetricName
    value: Decimal = Field(ge=0)
    unit: str = Field(min_length=1)
    source: SourceName
    source_url: str = Field(min_length=1)
    source_published_at: datetime | None = None
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def monthly_period_must_start_on_first_day(self) -> PortOperation:
        if self.frequency == "monthly" and self.period_start.day != 1:
            raise ValueError("period_start must be the first day for monthly observations")
        return self

    @property
    def natural_key(self) -> tuple[date, str, PortMetricName, SourceName]:
        return self.period_start, self.port_code, self.metric, self.source


class ExposureEvidence(BaseModel):
    model_config = ConfigDict(frozen=True)

    evidence_type: EvidenceType
    title: str = Field(min_length=1)
    url: str = Field(pattern=r"^https://")
    published_on: date
    excerpt_summary: str = Field(min_length=1)


class CommodityExposure(BaseModel):
    model_config = ConfigDict(frozen=True)

    hs_code: str = Field(pattern=r"^\d{2,6}$")
    weight: float = Field(gt=0, le=1)
    direction: str = Field(default="demand", pattern=r"^(demand|input|mixed)$")
    rationale: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = Field(min_length=1)


class CompanyExposure(BaseModel):
    model_config = ConfigDict(frozen=True)

    ticker: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,9}$")
    company_name: str = Field(min_length=1)
    confidence: ConfidenceLevel
    analyst_reviewed_on: date
    commodity_exposures: tuple[CommodityExposure, ...] = Field(min_length=1)
    port_weights: dict[str, float] = Field(default_factory=dict)
    country_weights: dict[str, float] = Field(default_factory=dict)
    evidence: dict[str, ExposureEvidence] = Field(min_length=1)
    limitations: str = Field(min_length=1)

    @field_validator("port_weights")
    @classmethod
    def validate_port_weights(cls, value: dict[str, float]) -> dict[str, float]:
        if any(len(code) != 4 or not code.isdigit() for code in value):
            raise ValueError("port weight keys must be four-digit Schedule D codes")
        if any(weight <= 0 or weight > 1 for weight in value.values()):
            raise ValueError("port weights must be greater than zero and at most one")
        return value

    @field_validator("country_weights")
    @classmethod
    def validate_country_weights(cls, value: dict[str, float]) -> dict[str, float]:
        if any(len(code) != 4 or not code.isdigit() for code in value):
            raise ValueError("country weight keys must be four-digit Schedule C codes")
        if any(weight <= 0 or weight > 1 for weight in value.values()):
            raise ValueError("country weights must be greater than zero and at most one")
        return value

    @model_validator(mode="after")
    def evidence_references_must_exist(self) -> CompanyExposure:
        missing = {
            evidence_id
            for exposure in self.commodity_exposures
            for evidence_id in exposure.evidence_ids
            if evidence_id not in self.evidence
        }
        if missing:
            raise ValueError(f"unknown evidence ids: {sorted(missing)}")
        return self


class CompanyExposureRegistry(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: int = Field(ge=1)
    companies: tuple[CompanyExposure, ...]


class IngestionResult(BaseModel):
    run_id: str
    source: SourceName
    status: IngestionStatus
    records_received: int = Field(ge=0)
    records_written: int = Field(ge=0)
    started_at: datetime
    completed_at: datetime
    error_message: str | None = None
