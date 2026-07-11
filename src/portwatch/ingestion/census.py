from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from portwatch.config import Settings
from portwatch.models import TradeFlow

BASE_URL = "https://api.census.gov/data/timeseries/intltrade/imports/porthsimport"

REQUEST_FIELDS = (
    "PORT_NAME",
    "I_COMMODITY",
    "I_COMMODITY_LDESC",
    "CTY_CODE",
    "CTY_DESC",
    "YEAR",
    "MONTH",
    "GEN_VAL_MO",
    "VES_VAL_MO",
    "VES_WGT_MO",
    "CNT_VAL_MO",
    "CNT_WGT_MO",
    "LAST_UPDATE",
)


class CensusConfigurationError(RuntimeError):
    pass


class CensusResponseError(RuntimeError):
    pass


class CensusPortHSClient:
    """Rate-conscious client for the Census monthly port/HS import dataset."""

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self.transport = transport

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        wait=wait_exponential_jitter(initial=1, max=10),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def fetch_month(
        self,
        *,
        month: date,
        port_code: str,
        commodity_code: str,
        country_code: str | None = None,
    ) -> tuple[list[TradeFlow], bytes]:
        if not self.settings.census_api_key:
            raise CensusConfigurationError(
                "CENSUS_API_KEY is required; request a free key from api.census.gov"
            )
        if month.day != 1:
            raise ValueError("month must be the first day of a calendar month")
        if len(port_code) != 4 or not port_code.isdigit():
            raise ValueError("port_code must be a four-digit Schedule D code")
        if len(commodity_code) not in {2, 4, 6} or not commodity_code.isdigit():
            raise ValueError("commodity_code must be a two-, four-, or six-digit HS code")

        district_code = port_code[:2]
        geography_port = f"{district_code}{port_code}"
        params: dict[str, str] = {
            "get": ",".join(REQUEST_FIELDS),
            "for": f"port:{geography_port}",
            "in": f"customs district:{district_code}",
            "time": month.strftime("%Y-%m"),
            "I_COMMODITY": commodity_code,
            "key": self.settings.census_api_key,
        }
        if country_code:
            params["CTY_CODE"] = country_code

        with httpx.Client(
            timeout=self.settings.http_timeout_seconds,
            headers={"User-Agent": self.settings.user_agent},
            transport=self.transport,
        ) as client:
            response = client.get(BASE_URL, params=params)
            response.raise_for_status()

        try:
            payload = response.json()
        except ValueError as exc:
            raise CensusResponseError("Census returned non-JSON content") from exc

        return parse_census_payload(payload, requested_port_code=port_code), response.content


def parse_census_payload(
    payload: Any,
    *,
    requested_port_code: str,
) -> list[TradeFlow]:
    if not isinstance(payload, list) or len(payload) < 2:
        raise CensusResponseError("Census payload must contain a header and at least one row")
    header = payload[0]
    if not isinstance(header, list) or not all(isinstance(field, str) for field in header):
        raise CensusResponseError("Census payload header is malformed")

    missing = set(REQUEST_FIELDS) - set(header)
    if missing:
        raise CensusResponseError(f"Census payload is missing fields: {sorted(missing)}")

    flows: list[TradeFlow] = []
    for position, values in enumerate(payload[1:], start=1):
        if not isinstance(values, list) or len(values) != len(header):
            raise CensusResponseError(f"row {position} does not match the response header")
        row = dict(zip(header, values, strict=True))
        try:
            month = date(int(row["YEAR"]), int(row["MONTH"]), 1)
            source_updated = _parse_datetime(row.get("LAST_UPDATE"))
            flows.append(
                TradeFlow(
                    month=month,
                    port_code=requested_port_code,
                    port_name=str(row["PORT_NAME"]).strip(),
                    commodity_code=str(row["I_COMMODITY"]).strip(),
                    commodity_description=str(row["I_COMMODITY_LDESC"]).strip(),
                    country_code=str(row["CTY_CODE"]).strip(),
                    country_name=str(row["CTY_DESC"]).strip(),
                    general_value_usd=_decimal(row["GEN_VAL_MO"]),
                    vessel_value_usd=_decimal(row["VES_VAL_MO"]),
                    vessel_weight_kg=_decimal(row["VES_WGT_MO"]),
                    containerized_value_usd=_decimal(row["CNT_VAL_MO"]),
                    containerized_weight_kg=_decimal(row["CNT_WGT_MO"]),
                    source_updated_at=source_updated,
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CensusResponseError(f"could not normalize Census row {position}: {exc}") from exc
    return flows


def _decimal(value: object) -> Decimal:
    if value in (None, ""):
        return Decimal(0)
    return Decimal(str(value))


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    raw = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(raw)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
