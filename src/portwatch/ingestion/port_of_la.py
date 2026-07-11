from __future__ import annotations

import re
from datetime import date
from decimal import Decimal

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from portwatch.config import Settings
from portwatch.models import PortMetricName, PortOperation, SourceName

SOURCE_URL = "https://portoflosangeles.org/business/statistics/container-statistics"
PORT_CODE = "2704"
PORT_NAME = "Los Angeles, CA"

_MONTH_PATTERN = (
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
)
_METRIC_LABELS = {
    PortMetricName.LOADED_IMPORT_TEU: "Loaded Imports",
    PortMetricName.LOADED_EXPORT_TEU: "Loaded Exports",
    PortMetricName.TOTAL_LOADED_TEU: "Total Loaded",
    PortMetricName.TOTAL_EMPTY_TEU: "Total Empty",
    PortMetricName.TOTAL_TEU: "Total",
}


class PortOfLosAngelesResponseError(RuntimeError):
    pass


class PortOfLosAngelesClient:
    """Public HTML adapter for the Port of Los Angeles latest monthly TEU release."""

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
    def fetch_latest(self) -> tuple[list[PortOperation], bytes]:
        with httpx.Client(
            timeout=self.settings.http_timeout_seconds,
            headers={"User-Agent": self.settings.user_agent},
            transport=self.transport,
            follow_redirects=True,
        ) as client:
            response = client.get(SOURCE_URL)
            response.raise_for_status()
        return parse_container_statistics_html(response.text), response.content


def parse_container_statistics_html(html: str) -> list[PortOperation]:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    section_match = re.search(
        rf"Latest Monthly Container Counts.*?{_MONTH_PATTERN}\s+(20\d{{2}})",
        text,
        flags=re.IGNORECASE,
    )
    if section_match is None:
        raise PortOfLosAngelesResponseError("could not locate the latest monthly period")

    month_name = section_match.group(1)
    year = int(section_match.group(2))
    period_start = date(year, _month_number(month_name), 1)
    section_text = text[section_match.start() :]

    operations: list[PortOperation] = []
    for metric, label in _METRIC_LABELS.items():
        match = re.search(
            rf"\b{re.escape(label)}\b\s+([\d,]+(?:\.\d+)?)",
            section_text,
            flags=re.IGNORECASE,
        )
        if match is None:
            raise PortOfLosAngelesResponseError(f"could not locate metric: {label}")
        operations.append(
            PortOperation(
                period_start=period_start,
                frequency="monthly",
                port_code=PORT_CODE,
                port_name=PORT_NAME,
                metric=metric,
                value=Decimal(match.group(1).replace(",", "")),
                unit="TEU",
                source=SourceName.PORT_OF_LA_CONTAINER_STATS,
                source_url=SOURCE_URL,
            )
        )
    return operations


def _month_number(month_name: str) -> int:
    months = {
        name: number
        for number, name in enumerate(
            (
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December",
            ),
            start=1,
        )
    }
    try:
        return months[month_name.title()]
    except KeyError as exc:
        raise PortOfLosAngelesResponseError(f"unknown month name: {month_name}") from exc
