from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SourceName(StrEnum):
    CENSUS_PORT_HS = "census_port_hs"


class IngestionStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


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


class IngestionResult(BaseModel):
    run_id: str
    source: SourceName
    status: IngestionStatus
    records_received: int = Field(ge=0)
    records_written: int = Field(ge=0)
    started_at: datetime
    completed_at: datetime
    error_message: str | None = None
