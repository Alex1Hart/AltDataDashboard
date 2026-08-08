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
    contract_award_detail,
    contract_obligation_breakdown,
    monthly_contract_awards,
    monthly_contract_transactions,
)
from portwatch.cli import app
from portwatch.config import Settings
from portwatch.contract_service import FederalContractIngestionService
from portwatch.health import audit_contract_state
from portwatch.ingestion.usaspending import (
    USAspendingClient,
    USAspendingResponseError,
    company_contract_search_terms,
    parse_award_page,
    parse_transaction_page,
)
from portwatch.models import ContractMatchMethod, FederalContractAward, IngestionStatus
from portwatch.registry import load_company_registry
from portwatch.storage.duckdb import DuckDBRepository

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "usaspending_awards.json"
TRANSACTION_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "usaspending_transactions.json"
REGISTRY_PATH = Path("config/company_exposures.yml")


def _transport(requests: list[httpx.Request]) -> httpx.MockTransport:
    award_payload = FIXTURE_PATH.read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/transactions/"):
            request_payload = json.loads(request.content)
            award_key = str(request_payload["award_id"])
            payload = json.loads(TRANSACTION_FIXTURE_PATH.read_bytes())
            financial_award = "47QMCA26F0002" in award_key
            financial_amounts = (10_000_000, 5_000_000, 0)
            for position, result in enumerate(payload["results"]):
                result["id"] = f"{award_key}:transaction-{position + 1}"
                if financial_award:
                    result["federal_action_obligation"] = financial_amounts[position]
            return httpx.Response(200, json=payload)
        return httpx.Response(
            200,
            content=award_payload,
            headers={"Content-Type": "application/json"},
        )

    return httpx.MockTransport(handler)


def _transaction_frame_for_awards(awards: list[FederalContractAward]) -> pd.DataFrame:
    transactions: list[dict[str, object]] = []
    for award_position, award in enumerate(awards):
        payload = json.loads(TRANSACTION_FIXTURE_PATH.read_bytes())
        for transaction_position, result in enumerate(payload["results"]):
            result["id"] = f"award-{award_position}:transaction-{transaction_position}"
            if award_position == 1:
                result["federal_action_obligation"] = (10_000_000, 5_000_000, 0)[
                    transaction_position
                ]
        parsed, _, _ = parse_transaction_page(
            payload,
            award=award,
            ingested_at=datetime(2026, 8, 7, tzinfo=UTC),
        )
        transactions.extend(transaction.model_dump(mode="python") for transaction in parsed)
    return pd.DataFrame(transactions)


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

    transactions, transaction_count, transaction_has_next = parse_transaction_page(
        json.loads(TRANSACTION_FIXTURE_PATH.read_bytes()),
        award=awards[0],
        ingested_at=datetime(2026, 8, 7, tzinfo=UTC),
    )
    assert transaction_count == 3
    assert not transaction_has_next
    assert transactions[1].modification_number == "P00001"
    assert transactions[2].federal_action_obligation_usd == Decimal("-5000000.0")


def test_contract_client_paginates_each_registered_legal_entity_with_guardrails() -> None:
    requests: list[httpx.Request] = []
    sleeps: list[float] = []
    clock = [100.0]

    def advance_clock(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    registry = load_company_registry(REGISTRY_PATH)
    client = USAspendingClient(
        Settings(
            PORTWATCH_USASPENDING_REQUEST_INTERVAL_SECONDS=0.25,
            PORTWATCH_USASPENDING_PAGE_SIZE=100,
        ),
        registry,
        transport=_transport(requests),
        sleep_fn=advance_clock,
        monotonic_fn=lambda: clock[0],
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
    assert len(requests) == 4
    assert len(batch.pages) == 2
    assert len(batch.awards) == 2
    assert batch.records_received == 6
    assert batch.records_unmatched == 2
    assert len(batch.transactions) == 6
    assert batch.transaction_records_received == 6
    assert sleeps == [0.25, 0.25, 0.25]
    request_payload = json.loads(requests[0].content)
    assert request_payload["filters"]["award_type_codes"] == ["A", "B", "C", "D"]
    assert request_payload["filters"]["time_period"] == [
        {"start_date": "2026-01-01", "end_date": "2026-08-07"}
    ]
    assert "Recipient UEI" in request_payload["fields"]
    transaction_request = json.loads(requests[2].content)
    assert transaction_request["limit"] == 5_000
    assert transaction_request["sort"] == "action_date"


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
    assert first.records_received == 12
    assert first.records_written == 8
    assert second.records_written == 0
    assert repository.execute_scalar("SELECT COUNT(*) FROM federal_contract_awards") == 2
    assert repository.execute_scalar("SELECT COUNT(*) FROM federal_contract_award_revisions") == 2
    assert repository.execute_scalar("SELECT COUNT(*) FROM federal_contract_transactions") == 6
    assert (
        repository.execute_scalar("SELECT COUNT(*) FROM federal_contract_transaction_revisions")
        == 6
    )
    assert repository.execute_scalar("SELECT COUNT(*) FROM raw_payloads") == 7
    assert repository.execute_scalar("SELECT COUNT(*) FROM raw_payload_links") == 14
    assert repository.execute_scalar("SELECT SUM(records_rejected) FROM ingestion_runs") == 4
    assert repository.source_counts()["federal_contract_awards"] == 2
    assert repository.source_counts()["federal_contract_transactions"] == 6
    audit = audit_contract_state(repository)
    assert audit.healthy
    assert audit.transaction_coverage == 1
    assert audit.orphan_transactions == 0

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

    transaction = parse_transaction_page(
        json.loads(TRANSACTION_FIXTURE_PATH.read_bytes()),
        award=revised,
        ingested_at=datetime(2026, 8, 9, tzinfo=UTC),
    )[0][0]
    revised_transaction = transaction.model_copy(
        update={
            "federal_action_obligation_usd": (
                transaction.federal_action_obligation_usd + Decimal("1")
            ),
        }
    )
    changed = repository.upsert_federal_contract_awards(
        "transaction-revision-run",
        [revised],
        payload_sha256_by_award={revised.award_key: "revision-payload"},
        transactions=[revised_transaction],
        payload_sha256_by_transaction={
            revised_transaction.transaction_id: "transaction-revision-payload"
        },
    )
    assert changed == 1
    assert (
        repository.execute_scalar(
            "SELECT revision_number FROM federal_contract_transactions "
            "WHERE transaction_id = ?",
            [revised_transaction.transaction_id],
        )
        == 2
    )
    assert (
        repository.execute_scalar(
            "SELECT COUNT(*) FROM federal_contract_transaction_revisions "
            "WHERE transaction_id = ? AND valid_until IS NOT NULL",
            [revised_transaction.transaction_id],
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
    transaction_frame = _transaction_frame_for_awards(awards)

    signals = compute_contract_company_signals(
        frame,
        transaction_frame,
        as_of=date(2026, 8, 7),
    )
    monthly = monthly_contract_awards(frame)
    transaction_monthly = monthly_contract_transactions(transaction_frame)
    details = contract_award_detail(frame, transaction_frame)
    agencies = contract_obligation_breakdown(
        frame,
        transaction_frame,
        dimension="awarding_agency",
    )

    assert signals.iloc[0]["total_current_obligations_usd"] == 140_000_000
    assert signals.iloc[0]["ttm_new_award_obligations_usd"] == 140_000_000
    assert signals.iloc[0]["award_count"] == 2
    assert signals.iloc[0]["transaction_count"] == 6
    assert signals.iloc[0]["transaction_coverage"] == 1
    assert signals.iloc[0]["ttm_net_obligations_usd"] == 140_000_000
    assert signals.iloc[0]["ttm_gross_obligations_usd"] == 145_000_000
    assert signals.iloc[0]["ttm_deobligations_usd"] == 5_000_000
    assert signals.iloc[0]["ttm_modification_count"] == 4
    assert signals.iloc[0]["top_awarding_agency"] == "Department of Defense"
    assert monthly["award_count"].sum() == 2
    assert transaction_monthly["net_obligations_usd"].sum() == 140_000_000
    assert transaction_monthly["deobligations_usd"].sum() == 5_000_000
    assert details["transaction_count"].tolist() == [3, 3]
    assert details["transaction_net_obligations_usd"].sum() == 140_000_000
    assert agencies["net_obligations_usd"].sum() == 140_000_000


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
