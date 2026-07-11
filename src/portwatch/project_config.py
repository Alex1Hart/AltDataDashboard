from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class PortSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str = Field(pattern=r"^\d{4}$")
    name: str = Field(min_length=1)


class CommoditySpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str = Field(pattern=r"^\d{2,6}$")
    theme: str = Field(min_length=1)


class BackfillPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    start_month: date
    end_month: date | None = None
    publication_lag_months: int = Field(default=2, ge=1, le=12)
    request_delay_seconds: float = Field(default=0.5, ge=0, le=60)
    continue_on_error: bool = True

    @model_validator(mode="after")
    def validate_month_boundaries(self) -> BackfillPolicy:
        if self.start_month.day != 1:
            raise ValueError("backfill.start_month must be the first day of a month")
        if self.end_month is not None:
            if self.end_month.day != 1:
                raise ValueError("backfill.end_month must be the first day of a month")
            if self.end_month < self.start_month:
                raise ValueError("backfill.end_month must not precede start_month")
        return self

    def resolved_end_month(self, today: date | None = None) -> date:
        if self.end_month is not None:
            return self.end_month
        anchor = today or date.today()
        return shift_month(date(anchor.year, anchor.month, 1), -self.publication_lag_months)


class PortWatchProjectConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: int = Field(ge=1)
    backfill: BackfillPolicy
    ports: tuple[PortSpec, ...] = Field(min_length=1)
    commodities: tuple[CommoditySpec, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def dimensions_must_be_unique(self) -> PortWatchProjectConfig:
        port_codes = [port.code for port in self.ports]
        commodity_codes = [commodity.code for commodity in self.commodities]
        if len(port_codes) != len(set(port_codes)):
            raise ValueError("port codes must be unique")
        if len(commodity_codes) != len(set(commodity_codes)):
            raise ValueError("commodity codes must be unique")
        return self


def load_project_config(path: Path) -> PortWatchProjectConfig:
    with path.open(encoding="utf-8") as config_file:
        payload = yaml.safe_load(config_file)
    return PortWatchProjectConfig.model_validate(payload)


def iter_months(start: date, end: date) -> list[date]:
    months: list[date] = []
    current = start
    while current <= end:
        months.append(current)
        current = shift_month(current, 1)
    return months


def shift_month(value: date, months: int) -> date:
    absolute_month = value.year * 12 + (value.month - 1) + months
    year, zero_based_month = divmod(absolute_month, 12)
    return date(year, zero_based_month + 1, 1)
