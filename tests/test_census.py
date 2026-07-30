from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from portwatch.config import Settings
from portwatch.ingestion.census import (
    CensusConfigurationError,
    CensusPortHSClient,
    parse_census_payload,
)
from portwatch.validation import DataValidationError, validate_trade_flows

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "census_porths_response.json"


def load_fixture() -> list[list[str]]:
    with FIXTURE_PATH.open(encoding="utf-8") as fixture_file:
        return json.load(fixture_file)


def test_parse_and_validate_census_payload() -> None:
    payload = load_fixture()
    other_port = payload[1].copy()
    other_port[0] = "LONG BEACH, CA"
    other_port[-1] = "2709"
    payload.append(other_port)
    aggregate_row = payload[1].copy()
    aggregate_row[3] = "-"
    aggregate_row[4] = "Total For All Countries"
    aggregate_row[12] = "0"
    payload.append(aggregate_row)

    flows = parse_census_payload(payload, requested_port_code="2704")

    assert len(flows) == 2
    assert flows[0].month == date(2026, 5, 1)
    assert flows[0].country_name == "CHINA"
    assert flows[0].containerized_value_usd == 1_320_000_000

    validate_trade_flows(
        flows,
        expected_month=date(2026, 5, 1),
        expected_port_code="2704",
        expected_commodity_code="84",
    )


def test_client_builds_port_geography_and_preserves_raw_payload() -> None:
    payload = load_fixture()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["for"] == "port:*"
        assert request.url.params["in"] == "customs district:27"
        assert request.url.params["time"] == "2026-05"
        assert request.url.params["I_COMMODITY"] == "84"
        assert request.url.params["key"] == "test-key"
        return httpx.Response(200, json=payload)

    settings = Settings(CENSUS_API_KEY="test-key")
    client = CensusPortHSClient(settings, transport=httpx.MockTransport(handler))
    flows, raw = client.fetch_month(
        month=date(2026, 5, 1),
        port_code="2704",
        commodity_code="84",
    )

    assert len(flows) == 2
    assert json.loads(raw) == payload


def test_client_treats_census_no_content_as_an_empty_slice() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204, content=b"")

    client = CensusPortHSClient(
        Settings(CENSUS_API_KEY="test-key"),
        transport=httpx.MockTransport(handler),
    )

    flows, raw = client.fetch_month(
        month=date(2026, 5, 1),
        port_code="2704",
        commodity_code="89",
    )

    assert flows == []
    assert raw == b""


def test_client_rejects_missing_api_key_before_request() -> None:
    client = CensusPortHSClient(Settings(CENSUS_API_KEY=None))

    with pytest.raises(CensusConfigurationError):
        client.fetch_month(
            month=date(2026, 5, 1),
            port_code="2704",
            commodity_code="84",
        )


def test_validator_rejects_duplicate_natural_keys() -> None:
    flow = parse_census_payload(load_fixture(), requested_port_code="2704")[0]

    with pytest.raises(DataValidationError, match="duplicate"):
        validate_trade_flows([flow, flow])
