from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import httpx
import pandas as pd
import pytest
from typer.testing import CliRunner

from portwatch.analytics.contracts import (
    compute_contract_company_signals,
    monthly_contract_awards,
)
from portwatch.cli import app
from portwatch.config import Settings
from portwatch.contract_service import FederalContractIngestionService
from portwatch.ingestion.usaspending import (
    USAspendingClient,
    USAspendingResponseError,
    company_contract_search_terms,
    parse_award_page,
)
from portwatch.models import ContractMatchMethod, IngestionStatus
from portwatch.registry import load_company_registry
from portwatch.storage.duckdb import DuckDBRepository

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "usaspending_awards.json"
REGISTRY_PATH = Path("config/company_exposures.yml")


def _transport(requests: list[httpx.Request]) -> httpx.MockTransport:
    payload = FIXTURE_PATH.read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=payload, headers={"Content-Type": "application/json"})

    return httpx.MockTransport(handler)


def test_contract_parser_matches_reviewed_entities_and_rejects_fuzzy_candidates() -> None:
    registry = load_company_registry(REGISTRY_PATH)
    payload = json.loads(FIXTURE_PATH.read_bytes())

    awards, received, unmatched, has_next = parse_award_page(
        payload,
        registry=registry,
        ticker="CAT",
        ingested_at=datetime(2026, 8, 7, tzinfo=UTC),
    )

    assert received == 3
    assert unmatched == 1
    assert not has_next
    assert {award.entity_id for award in awards} == {"cat_inc", "cat_financial"}
    assert {award.match_method for award in awards} == {ContractMatchMethod.REVIEWED_NAME}
    assert awards[0].naics_code == "333120"
    assert awards[0].psc_code == "3805"
    assert awards[0].source_modified_at == datetime(2026, 6, 30, tzinfo=UTC)


def test_contract_client_paginates_each_registered_legal_entity_with_guardrails() -> None:
    requests: list[httpx.Request] = []
    sleeps: list[float] = []
    registry = load_company_registry(REGISTRY_PATH)
    client = USAspendingClient(
        Settings(
            PORTWATCH_USASPENDING_REQUEST_INTERVAL_SECONDS=0.25,
            PORTWATCH_USASPENDING_PAGE_SIZE=100,
        ),
        registry,
        transport=_transport(requests),
        sleep_fn=sleeps.append,
        monotonic_fn=lambda: 100.0,
    )
    try:
        batch = client.fetch_company(
            "cat",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 8, 7),
        )
    finally:
        client.close()

    assert batch.root_entity_id == "cat_inc"
    assert len(requests) == 2
    assert len(batch.pages) == 2
    assert len(batch.awards) == 2
    assert batch.records_received == 6
    assert batch.records_unmatched == 2
    assert sleeps == [0.25]
    request_payload = json.loads(requests[0].content)
    assert request_payload["filters"]["award_type_codes"] == ["A", "B", "C", "D"]
    assert request_payload["filters"]["time_period"] == [
        {"start_date": "2026-01-01", "end_date": "2026-08-07"}
    ]
    assert "Recipient UEI" in request_payload["fields"]


def test_contract_service_archives_upserts_and_versions_awards(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    registry = load_company_registry(REGISTRY_PATH)
    repository = DuckDBRepository(tmp_path / "contracts.duckdb")
    client = USAspendingClient(
        Settings(PORTWATCH_USASPENDING_REQUEST_INTERVAL_SECONDS=0),
        registry,
        transport=_transport(requests),
        sleep_fn=lambda seconds: None,
        monotonic_fn=lambda: 100.0,
    )
    service = FederalContractIngestionService(client=client, repository=repository)
    try:
        first = service.ingest_company(
            "CAT",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 8, 7),
        )
        second = service.ingest_company(
            "CAT",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 8, 7),
        )
    finally:
        client.close()

    assert first.status is IngestionStatus.SUCCEEDED
    assert first.records_received == 6
    assert first.records_written == 2
    assert second.records_written == 0
    assert repository.execute_scalar("SELECT COUNT(*) FROM federal_contract_awards") == 2
    assert repository.execute_scalar("SELECT COUNT(*) FROM federal_contract_award_revisions") == 2
    assert repository.execute_scalar("SELECT COUNT(*) FROM raw_payloads") == 3
    assert repository.execute_scalar("SELECT COUNT(*) FROM raw_payload_links") == 6
    assert repository.execute_scalar("SELECT SUM(records_rejected) FROM ingestion_runs") == 4
    assert repository.source_counts()["federal_contract_awards"] == 2

    award = parse_award_page(
        json.loads(FIXTURE_PATH.read_bytes()),
        registry=registry,
        ticker="CAT",
        ingested_at=datetime(2026, 8, 8, tzinfo=UTC),
    )[0][0]
    revised = award.model_copy(
        update={
            "award_amount_usd": award.award_amount_usd + Decimal("1"),
            "ingested_at": award.ingested_at + timedelta(days=1),
        }
    )
    changed = repository.upsert_federal_contract_awards(
        "revision-run",
        [revised],
        payload_sha256_by_award={revised.award_key: "revision-payload"},
    )
    assert changed == 1
    assert (
        repository.execute_scalar(
            "SELECT revision_number FROM federal_contract_awards WHERE award_key = ?",
            [revised.award_key],
        )
        == 2
    )
    assert (
        repository.execute_scalar(
            "SELECT COUNT(*) FROM federal_contract_award_revisions WHERE valid_until IS NOT NULL"
        )
        == 1
    )


def test_contract_revision_write_rolls_back_atomically(tmp_path: Path) -> None:
    registry = load_company_registry(REGISTRY_PATH)
    award = parse_award_page(
        json.loads(FIXTURE_PATH.read_bytes()),
        registry=registry,
        ticker="CAT",
        ingested_at=datetime(2026, 8, 7, tzinfo=UTC),
    )[0][0]
    database_path = tmp_path / "contract-revision-rollback.duckdb"
    repository = DuckDBRepository(database_path)
    repository.initialize()
    repository.upsert_federal_contract_awards(
        "original-run",
        [award],
        payload_sha256_by_award={award.award_key: "original-payload"},
    )
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            INSERT INTO federal_contract_award_revisions
            SELECT * EXCLUDE (revision_number, payload_sha256, valid_from, valid_until),
                   2, 'reserved-payload', valid_from, valid_until
            FROM federal_contract_award_revisions
            WHERE award_key = ? AND revision_number = 1
            """,
            [award.award_key],
        )

    revised = award.model_copy(
        update={
            "award_amount_usd": award.award_amount_usd + Decimal("1"),
            "ingested_at": award.ingested_at + timedelta(days=1),
        }
    )
    with pytest.raises(duckdb.ConstraintException):
        repository.upsert_federal_contract_awards(
            "revised-run",
            [revised],
            payload_sha256_by_award={revised.award_key: "revised-payload"},
        )

    assert (
        repository.execute_scalar(
            "SELECT revision_number FROM federal_contract_awards WHERE award_key = ?",
            [award.award_key],
        )
        == 1
    )
    assert (
        repository.execute_scalar(
            "SELECT COUNT(*) FROM federal_contract_award_revisions "
            "WHERE revision_number = 1 AND valid_until IS NULL"
        )
        == 1
    )


def test_contract_client_fails_closed_before_truncating_pagination() -> None:
    registry = load_company_registry(REGISTRY_PATH)
    payload = json.loads(FIXTURE_PATH.read_bytes())
    payload["page_metadata"]["hasNext"] = True
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    client = USAspendingClient(
        Settings(
            PORTWATCH_USASPENDING_REQUEST_INTERVAL_SECONDS=0,
            PORTWATCH_USASPENDING_MAX_PAGES_PER_SEARCH=1,
        ),
        registry,
        transport=transport,
    )
    try:
        with pytest.raises(USAspendingResponseError, match="pagination limit"):
            client.fetch_company(
                "CAT",
                start_date=date(2026, 1, 1),
                end_date=date(2026, 8, 7),
            )
    finally:
        client.close()


def test_contract_signals_keep_obligation_semantics_explicit() -> None:
    registry = load_company_registry(REGISTRY_PATH)
    awards = parse_award_page(
        json.loads(FIXTURE_PATH.read_bytes()),
        registry=registry,
        ticker="CAT",
        ingested_at=datetime(2026, 8, 7, tzinfo=UTC),
    )[0]
    frame = pd.DataFrame([award.model_dump(mode="python") for award in awards])

    signals = compute_contract_company_signals(frame, as_of=date(2026, 8, 7))
    monthly = monthly_contract_awards(frame)

    assert signals.iloc[0]["total_current_obligations_usd"] == 140_000_000
    assert signals.iloc[0]["ttm_new_award_obligations_usd"] == 140_000_000
    assert signals.iloc[0]["award_count"] == 2
    assert signals.iloc[0]["top_awarding_agency"] == "Department of Defense"
    assert monthly["award_count"].sum() == 2


def test_company_contract_search_and_cli_are_discoverable() -> None:
    registry = load_company_registry(REGISTRY_PATH)
    root_entity_id, terms = company_contract_search_terms(
        registry,
        "CAT",
        as_of=date(2026, 8, 7),
    )
    result = CliRunner().invoke(app, ["ingest", "contracts", "--help"])

    assert root_entity_id == "cat_inc"
    assert terms == ("Caterpillar Inc.", "Caterpillar Financial Services Corporation")
    assert result.exit_code == 0
    assert "USAspending" in result.stdout
    assert "--ticker" in result.stdout
