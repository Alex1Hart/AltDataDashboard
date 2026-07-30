from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

from portwatch.cli import app
from portwatch.config import Settings
from portwatch.ingestion.sec import (
    SecConfigurationError,
    SecEdgarClient,
    SecResponseError,
    parse_sec_company_facts,
    parse_sec_submissions,
)
from portwatch.models import IngestionStatus
from portwatch.registry import load_company_registry
from portwatch.sec_service import SecEdgarIngestionService
from portwatch.storage.duckdb import DuckDBRepository

FIXTURES = Path(__file__).parent / "fixtures"
SUBMISSIONS_PATH = FIXTURES / "sec_submissions.json"
COMPANY_FACTS_PATH = FIXTURES / "sec_companyfacts.json"
REGISTRY_PATH = Path("config/company_exposures.yml")


def _sec_transport(requests: list[httpx.Request]) -> httpx.MockTransport:
    submissions = json.loads(SUBMISSIONS_PATH.read_bytes())
    company_facts = json.loads(COMPANY_FACTS_PATH.read_bytes())

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["user-agent"] == "PortWatch Test analyst@example.com"
        if request.url.path.startswith("/submissions/"):
            return httpx.Response(200, json=submissions, request=request)
        if request.url.path.startswith("/api/xbrl/companyfacts/"):
            return httpx.Response(200, json=company_facts, request=request)
        return httpx.Response(404, request=request)

    return httpx.MockTransport(handler)


def test_sec_client_resolves_cik_and_normalizes_filings_and_facts() -> None:
    requests: list[httpx.Request] = []
    sleeps: list[float] = []
    registry = load_company_registry(REGISTRY_PATH)
    client = SecEdgarClient(
        Settings(
            PORTWATCH_USER_AGENT="PortWatch Test analyst@example.com",
            PORTWATCH_SEC_REQUEST_INTERVAL_SECONDS=0.1,
        ),
        registry,
        transport=_sec_transport(requests),
        sleep_fn=sleeps.append,
        monotonic_fn=lambda: 100.0,
    )

    batch = client.fetch_company("cat")

    assert batch.entity_id == "cat_inc"
    assert batch.cik == "0000018230"
    assert batch.ticker == "CAT"
    assert len(batch.filings) == 2
    assert len(batch.facts) == 3
    assert requests[0].url.path == "/submissions/CIK0000018230.json"
    assert requests[1].url.path == "/api/xbrl/companyfacts/CIK0000018230.json"
    assert sleeps == [pytest.approx(0.1)]

    current_revenue = next(
        fact
        for fact in batch.facts
        if fact.tag == "RevenueFromContractWithCustomerExcludingAssessedTax"
        and fact.fiscal_year == 2025
    )
    prior_revenue = next(
        fact
        for fact in batch.facts
        if fact.tag == "RevenueFromContractWithCustomerExcludingAssessedTax"
        and fact.fiscal_year == 2024
    )
    assert current_revenue.accepted_at == datetime(2026, 2, 13, 15, 18, 27, tzinfo=UTC)
    assert not current_revenue.acceptance_is_estimated
    assert prior_revenue.acceptance_is_estimated
    assert prior_revenue.accepted_at == datetime(2025, 2, 14, tzinfo=UTC)


def test_sec_client_requires_declared_contact_user_agent() -> None:
    registry = load_company_registry(REGISTRY_PATH)
    client = SecEdgarClient(
        Settings(PORTWATCH_USER_AGENT="PortWatch Test research"),
        registry,
        transport=_sec_transport([]),
    )

    with pytest.raises(SecConfigurationError, match="contact email"):
        client.fetch_company("CAT")


def test_sec_parser_rejects_cik_outside_reviewed_registry() -> None:
    registry = load_company_registry(REGISTRY_PATH)
    payload = json.loads(SUBMISSIONS_PATH.read_bytes())
    payload["cik"] = "999999"

    with pytest.raises(SecResponseError, match="not registered"):
        parse_sec_submissions(
            payload,
            registry=registry,
            expected_entity_id="cat_inc",
            ticker="CAT",
            ingested_at=datetime.now(UTC),
        )


def test_sec_company_facts_parser_bounds_the_normalized_history() -> None:
    registry = load_company_registry(REGISTRY_PATH)
    payload = json.loads(COMPANY_FACTS_PATH.read_bytes())

    facts = parse_sec_company_facts(
        payload,
        registry=registry,
        expected_entity_id="cat_inc",
        ticker="CAT",
        accepted_by_accession={},
        ingested_at=datetime(2026, 7, 30, tzinfo=UTC),
        filed_on_or_after=date(2026, 1, 1),
    )

    assert len(facts) == 2
    assert {fact.filed_on for fact in facts} == {date(2026, 2, 13)}


def test_sec_service_archives_and_idempotently_stores_company_evidence(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    registry = load_company_registry(REGISTRY_PATH)
    repository = DuckDBRepository(tmp_path / "sec.duckdb")
    client = SecEdgarClient(
        Settings(
            PORTWATCH_USER_AGENT="PortWatch Test analyst@example.com",
            PORTWATCH_SEC_REQUEST_INTERVAL_SECONDS=0.1,
        ),
        registry,
        transport=_sec_transport(requests),
        sleep_fn=lambda seconds: None,
        monotonic_fn=lambda: 100.0,
    )
    service = SecEdgarIngestionService(client=client, repository=repository)

    first = service.ingest_company("CAT")
    second = service.ingest_company("CAT")

    assert first.status is IngestionStatus.SUCCEEDED
    assert first.records_received == 5
    assert first.records_written == 5
    assert second.records_received == 5
    assert second.records_written == 0
    assert repository.execute_scalar("SELECT COUNT(*) FROM sec_filings") == 2
    assert repository.execute_scalar("SELECT COUNT(*) FROM sec_company_facts") == 3
    assert repository.execute_scalar("SELECT COUNT(*) FROM raw_payloads") == 2
    assert repository.execute_scalar("SELECT COUNT(*) FROM raw_payload_links") == 4
    assert repository.execute_scalar("SELECT COUNT(*) FROM ingestion_runs") == 2
    source_counts = repository.source_counts()
    assert source_counts["sec_filings"] == 2
    assert source_counts["sec_company_facts"] == 3
    assert source_counts["sec_company_concepts"] == 2
    catalog = repository.sec_company_fact_catalog()
    assert set(catalog["tag"]) == {
        "Assets",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
    }
    revenue_history = repository.sec_company_fact_history(
        entity_id="cat_inc",
        taxonomy="us-gaap",
        tag="RevenueFromContractWithCustomerExcludingAssessedTax",
        unit="USD",
    )
    assert len(revenue_history) == 2
    assert set(repository.sec_filings_summary(forms=("10-K",))["form"]) == {"10-K"}
    assert (
        repository.execute_scalar(
            "SELECT COUNT(*) FROM sec_company_facts WHERE acceptance_is_estimated"
        )
        == 1
    )
    latest_run = repository.recent_runs(limit=1).iloc[0]
    assert latest_run["entity_id"] == "cat_inc"
    assert latest_run["ticker"] == "CAT"

    batch = client.fetch_company("CAT")
    revised_fact = batch.facts[0].model_copy(
        update={
            "value": batch.facts[0].value + Decimal("1"),
            "ingested_at": batch.facts[0].ingested_at + timedelta(days=1),
        }
    )
    revision_write = repository.store_sec_edgar_batch(
        "revision-run",
        batch.filings,
        [revised_fact],
        submissions_payload_sha256="submissions-revision",
        company_facts_payload_sha256="facts-revision",
    )
    assert revision_write == 1
    assert (
        repository.execute_scalar(
            "SELECT revision_number FROM sec_company_facts WHERE fact_id = ?",
            [revised_fact.fact_id],
        )
        == 2
    )
    assert repository.execute_scalar("SELECT COUNT(*) FROM sec_company_fact_revisions") == 4
    assert (
        repository.execute_scalar(
            "SELECT COUNT(*) FROM sec_company_fact_revisions WHERE valid_until IS NOT NULL"
        )
        == 1
    )


def test_sec_service_marks_an_interrupted_run_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = load_company_registry(REGISTRY_PATH)
    repository = DuckDBRepository(tmp_path / "interrupted-sec.duckdb")
    client = SecEdgarClient(
        Settings(PORTWATCH_USER_AGENT="PortWatch Test analyst@example.com"),
        registry,
    )

    def interrupt_fetch(ticker: str) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(client, "fetch_company", interrupt_fetch)
    service = SecEdgarIngestionService(client=client, repository=repository)

    with pytest.raises(KeyboardInterrupt):
        service.ingest_company("CAT")

    latest_run = repository.recent_runs(limit=1).iloc[0]
    assert latest_run["status"] == "failed"
    assert latest_run["error_message"] == "SEC ingestion interrupted by user"


def test_sec_cli_command_is_discoverable() -> None:
    result = CliRunner().invoke(app, ["ingest", "sec", "--help"])

    assert result.exit_code == 0
    assert "--ticker" in result.stdout
    assert "Company Facts" in result.stdout
