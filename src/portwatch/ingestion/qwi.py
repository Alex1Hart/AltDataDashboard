from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from portwatch.config import Settings
from portwatch.models import LaborMarketObservation, LaborMetricName

QWI_URL = "https://api.census.gov/data/timeseries/qwi/rh"
QWI_METRICS: dict[str, tuple[LaborMetricName, str]] = {
    "Emp": (LaborMetricName.EMPLOYMENT, "jobs"),
    "EmpEnd": (LaborMetricName.ENDING_EMPLOYMENT, "jobs"),
    "HirA": (LaborMetricName.HIRES, "jobs"),
    "Sep": (LaborMetricName.SEPARATIONS, "jobs"),
    "FrmJbGn": (LaborMetricName.JOB_GAINS, "jobs"),
    "FrmJbLs": (LaborMetricName.JOB_LOSSES, "jobs"),
    "EarnS": (LaborMetricName.STABLE_JOBS_EARNINGS, "USD per month"),
    "Payroll": (LaborMetricName.PAYROLL, "USD per quarter"),
}
_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


class QWIResponseError(RuntimeError):
    pass


class QWITransientResponseError(RuntimeError):
    pass


@dataclass(frozen=True)
class QWIBatch:
    raw_payload: bytes
    source_url: str
    observations: tuple[LaborMarketObservation, ...]
    records_rejected: int


class CensusQWIClient:
    """Official Census QWI adapter for geography/NAICS labor-demand benchmarks."""

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self._client = httpx.Client(
            timeout=settings.http_timeout_seconds,
            headers={"User-Agent": settings.user_agent, "Accept": "application/json"},
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    @retry(
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.NetworkError, QWITransientResponseError)
        ),
        wait=wait_exponential_jitter(initial=1, max=10),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def fetch(
        self,
        *,
        year: int,
        quarter: int,
        industry_code: str,
        geography_level: str,
        geography_code: str,
        seasonally_adjusted: bool = False,
    ) -> QWIBatch:
        if not self.settings.census_api_key:
            raise ValueError("CENSUS_API_KEY is required for Census QWI")
        if quarter not in (1, 2, 3, 4):
            raise ValueError("quarter must be between 1 and 4")
        if geography_level not in {"state", "county"}:
            raise ValueError("QWI geography_level must be state or county")
        if geography_level == "state" and (
            len(geography_code) != 2 or not geography_code.isdigit()
        ):
            raise ValueError("state geography_code must be a two-digit FIPS code")
        if geography_level == "county" and (
            len(geography_code) != 5 or not geography_code.isdigit()
        ):
            raise ValueError("county geography_code must be a five-digit state+county FIPS code")
        if not industry_code.isdigit() or not 2 <= len(industry_code) <= 6:
            raise ValueError("industry_code must be a two- to six-digit NAICS code")

        params = {
            "get": ",".join(("geography", "industry", *QWI_METRICS)),
            "for": (
                f"state:{geography_code}"
                if geography_level == "state"
                else f"county:{geography_code[2:]}"
            ),
            "year": str(year),
            "quarter": str(quarter),
            "industry": industry_code,
            "ownercode": "A05",
            "seasonadj": "S" if seasonally_adjusted else "U",
            "key": self.settings.census_api_key,
        }
        if geography_level == "county":
            params["in"] = f"state:{geography_code[:2]}"
        response = self._client.get(QWI_URL, params=params)
        if response.status_code in _TRANSIENT_STATUS_CODES:
            raise QWITransientResponseError(f"Census QWI returned HTTP {response.status_code}")
        response.raise_for_status()
        if response.status_code == 204 or not response.content:
            raise QWIResponseError(
                "Census QWI returned no rows for the requested period/geography/industry"
            )
        observations, rejected = parse_qwi_payload(
            response.content,
            requested_year=year,
            requested_quarter=quarter,
            requested_industry_code=industry_code,
            requested_geography_level=geography_level,
            requested_geography_code=geography_code,
            seasonally_adjusted=seasonally_adjusted,
        )
        return QWIBatch(
            raw_payload=response.content,
            source_url=QWI_URL,
            observations=tuple(observations),
            records_rejected=rejected,
        )


def parse_qwi_payload(
    payload: bytes,
    *,
    requested_year: int,
    requested_quarter: int,
    requested_industry_code: str,
    requested_geography_level: str,
    requested_geography_code: str,
    seasonally_adjusted: bool,
    ingested_at: datetime | None = None,
) -> tuple[list[LaborMarketObservation], int]:
    try:
        data = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QWIResponseError("Census QWI returned invalid JSON") from exc
    if not isinstance(data, Sequence) or isinstance(data, (str, bytes)) or len(data) < 2:
        raise QWIResponseError("Census QWI response contained no data rows")
    header = data[0]
    if not isinstance(header, Sequence) or isinstance(header, (str, bytes)):
        raise QWIResponseError("Census QWI response header is malformed")
    names = [str(name) for name in header]
    required = {"geography", *QWI_METRICS}
    missing = sorted(required - set(names))
    if missing:
        raise QWIResponseError(f"Census QWI response is missing fields: {missing}")

    observed_at = ingested_at or datetime.now(UTC)
    period_start = date(requested_year, 1 + ((requested_quarter - 1) * 3), 1)
    observations: list[LaborMarketObservation] = []
    rejected = 0
    for raw_row in data[1:]:
        if not isinstance(raw_row, Sequence) or isinstance(raw_row, (str, bytes)):
            raise QWIResponseError("Census QWI data row is malformed")
        if len(raw_row) != len(names):
            raise QWIResponseError("Census QWI row length does not match its header")
        row: Mapping[str, Any] = dict(zip(names, raw_row, strict=True))
        geography_value = str(row.get("geography") or requested_geography_code)
        geography_name = (
            f"{requested_geography_level.title()} {requested_geography_code}"
            if geography_value == requested_geography_code
            else geography_value
        )
        industry_name_value = row.get("industry")
        industry_value = str(industry_name_value or requested_industry_code)
        industry_name = (
            f"NAICS {requested_industry_code}"
            if industry_value == requested_industry_code
            else industry_value
        )
        for source_field, (metric, unit) in QWI_METRICS.items():
            value = row.get(source_field)
            try:
                if value in (None, "", "N", "null"):
                    raise InvalidOperation
                numeric_value = Decimal(str(value).replace(",", ""))
            except InvalidOperation:
                rejected += 1
                continue
            observations.append(
                LaborMarketObservation(
                    period_start=period_start,
                    geography_level=requested_geography_level,
                    geography_code=requested_geography_code,
                    geography_name=geography_name,
                    industry_code=requested_industry_code,
                    industry_name=industry_name,
                    metric=metric,
                    value=numeric_value,
                    unit=unit,
                    seasonally_adjusted=seasonally_adjusted,
                    source_url=QWI_URL,
                    ingested_at=observed_at,
                )
            )
    if not observations:
        raise QWIResponseError("Census QWI response had no usable metric values")
    return observations, rejected
