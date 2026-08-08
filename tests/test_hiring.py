from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import httpx
import pandas as pd
from typer.testing import CliRunner

from portwatch.analytics.hiring import (
    business_line_hiring_summary,
    compute_hiring_company_signals,
    function_hiring_summary,
    location_hiring_summary,
    theme_hiring_summary,
)
from portwatch.cli import app
from portwatch.config import Settings
from portwatch.health import audit_hiring_state
from portwatch.hiring_classifier import classify_posting
from portwatch.hiring_config import CareerSourceConfig, CareerSourceType, load_hiring_config
from portwatch.hiring_service import HiringIngestionService
from portwatch.ingestion.careers import (
    CareerSourceClient,
    parse_greenhouse_payload,
    parse_html_page,
    parse_job_detail_payload,
    parse_lever_payload,
)
from portwatch.models import IngestionStatus, JobStatus
from portwatch.registry import load_company_registry
from portwatch.storage.duckdb import DuckDBRepository

FIXTURES = Path(__file__).parent / "fixtures"
HIRING_CONFIG = Path("config/hiringwatch.yml")
REGISTRY = Path("config/company_exposures.yml")


def _source(source_type: CareerSourceType, identifier: str = "acme") -> CareerSourceConfig:
    return CareerSourceConfig(
        source_id=f"test_{source_type.value}",
        ticker="CAT",
        entity_id="cat_inc",
        source_type=source_type,
        base_url="https://careers.example.com/jobs/",
        api_identifier=identifier if source_type is not CareerSourceType.HTML_PAGINATED else None,
        page_size=100,
        max_pages=5,
        job_selector=".card.card-job",
        job_id_attribute="data-id",
        title_selector=".card-title a.js-view-job",
        link_selector=".card-title a.js-view-job",
        location_selector="ul.job-meta li:first-child",
        last_page_selector=".pagination a[rel~='last']",
        location_delimiter=" / ",
    )


def test_all_career_adapters_normalize_to_one_job_model() -> None:
    observed_at = datetime(2026, 8, 7, tzinfo=UTC)
    greenhouse = parse_greenhouse_payload(
        (FIXTURES / "greenhouse_jobs.json").read_bytes(),
        _source(CareerSourceType.GREENHOUSE),
        observed_at,
    )
    lever = parse_lever_payload(
        (FIXTURES / "lever_jobs.json").read_bytes(),
        _source(CareerSourceType.LEVER),
        observed_at,
    )
    html, last_page = parse_html_page(
        (FIXTURES / "caterpillar_jobs_page.html").read_bytes(),
        _source(CareerSourceType.HTML_PAGINATED),
        observed_at,
    )

    assert greenhouse[0].title == "Senior Manufacturing Engineer"
    assert greenhouse[0].description == (
        "Build battery-enabled equipment for Building Construction Products."
    )
    assert lever[0].team == "Supply Chain"
    assert html[0].source_job_id == "r0000000001"
    assert len(html[0].locations) == 2
    assert last_page == 2


def test_schema_org_detail_enrichment_extracts_description_and_career_area() -> None:
    posting = parse_html_page(
        (FIXTURES / "caterpillar_jobs_page.html").read_bytes(),
        _source(CareerSourceType.HTML_PAGINATED),
        datetime(2026, 8, 7, tzinfo=UTC),
    )[0][0]

    detailed = parse_job_detail_payload(
        (FIXTURES / "caterpillar_job_detail.html").read_bytes(),
        posting,
    )

    assert detailed.department == "Engineering"
    assert detailed.employment_type == "FULL_TIME"
    assert detailed.posted_at == datetime(2026, 8, 1, 12, tzinfo=UTC)
    assert "Resource Industries" in detailed.description


def test_html_client_paginates_complete_snapshot_and_rate_limits() -> None:
    requests: list[httpx.Request] = []
    sleeps: list[float] = []
    payload = (FIXTURES / "caterpillar_jobs_page.html").read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        response_payload = (
            payload.replace(b"?page=2", b"?page=1")
            .replace(b"r0000000001", b"r0000000003")
            .replace(b"r0000000002", b"r0000000004")
            if request.url.params.get("page") == "2"
            else payload
        )
        return httpx.Response(200, content=response_payload)

    client = CareerSourceClient(
        Settings(PORTWATCH_CAREERS_REQUEST_INTERVAL_SECONDS=0.25),
        _source(CareerSourceType.HTML_PAGINATED),
        transport=httpx.MockTransport(handler),
        sleep_fn=sleeps.append,
        monotonic_fn=lambda: 100.0,
    )
    try:
        snapshot = client.fetch_snapshot()
    finally:
        client.close()

    assert len(requests) == 2
    assert len(snapshot.pages) == 2
    assert len(snapshot.postings) == 4
    assert sleeps == [0.25]


def test_classification_maps_business_function_theme_and_reviewed_facility() -> None:
    config = load_hiring_config(HIRING_CONFIG)
    registry = load_company_registry(REGISTRY)
    posting = parse_greenhouse_payload(
        (FIXTURES / "greenhouse_jobs.json").read_bytes(),
        _source(CareerSourceType.GREENHOUSE),
        datetime(2026, 8, 7, tzinfo=UTC),
    )[0]

    classified = classify_posting(
        posting,
        config.company("CAT"),
        registry,
        classification_version=config.version,
    )

    assert classified.business_line_name == "Construction Industries"
    assert classified.job_function == "Manufacturing & Operations"
    assert classified.themes == ("Electrification",)
    assert classified.locations[0].facility_entity_id == "cat_east_peoria_operations"


def test_job_snapshot_tracks_two_miss_closure_and_reopening(tmp_path: Path) -> None:
    observed_at = datetime(2026, 8, 7, tzinfo=UTC)
    source = _source(CareerSourceType.GREENHOUSE)
    postings = parse_greenhouse_payload(
        (FIXTURES / "greenhouse_jobs.json").read_bytes(),
        source,
        observed_at,
    )
    repository = DuckDBRepository(tmp_path / "hiring.duckdb")
    repository.initialize()
    payloads = {postings[0].source_job_id: "payload-1"}

    assert (
        repository.apply_job_snapshot(
            "run-1",
            source_id=source.source_id,
            observed_at=observed_at,
            postings=postings,
            payload_sha256_by_job=payloads,
            missing_snapshots_before_close=2,
        )
        == 1
    )
    assert (
        repository.apply_job_snapshot(
            "run-2",
            source_id=source.source_id,
            observed_at=observed_at + timedelta(days=1),
            postings=[],
            payload_sha256_by_job={},
            missing_snapshots_before_close=2,
        )
        == 0
    )
    assert repository.execute_scalar("SELECT status FROM job_postings") == JobStatus.ACTIVE.value
    assert (
        repository.apply_job_snapshot(
            "run-3",
            source_id=source.source_id,
            observed_at=observed_at + timedelta(days=2),
            postings=[],
            payload_sha256_by_job={},
            missing_snapshots_before_close=2,
        )
        == 1
    )
    assert repository.execute_scalar("SELECT status FROM job_postings") == JobStatus.CLOSED.value
    assert (
        repository.apply_job_snapshot(
            "run-4",
            source_id=source.source_id,
            observed_at=observed_at + timedelta(days=3),
            postings=postings,
            payload_sha256_by_job=payloads,
            missing_snapshots_before_close=2,
        )
        == 1
    )
    assert repository.execute_scalar("SELECT status FROM job_postings") == JobStatus.ACTIVE.value
    assert repository.execute_scalar("SELECT MAX(revision_number) FROM job_postings") == 3
    assert repository.execute_scalar("SELECT COUNT(*) FROM job_events") == 3
    assert (
        repository.execute_scalar("SELECT event_type FROM job_events ORDER BY observed_at LIMIT 1")
        == "baseline"
    )


def test_new_job_after_baseline_emits_opened(tmp_path: Path) -> None:
    observed_at = datetime(2026, 8, 7, tzinfo=UTC)
    source = _source(CareerSourceType.GREENHOUSE)
    postings = parse_greenhouse_payload(
        (FIXTURES / "greenhouse_jobs.json").read_bytes(),
        source,
        observed_at,
    )
    new_posting = postings[0].model_copy(
        update={"source_job_id": "new-job", "source_url": "https://example.com/new-job"}
    )
    repository = DuckDBRepository(tmp_path / "new-job.duckdb")
    repository.initialize()

    repository.apply_job_snapshot(
        "run-1",
        source_id=source.source_id,
        observed_at=observed_at,
        postings=postings,
        payload_sha256_by_job={postings[0].source_job_id: "payload-1"},
        missing_snapshots_before_close=2,
    )
    repository.apply_job_snapshot(
        "run-2",
        source_id=source.source_id,
        observed_at=observed_at + timedelta(days=1),
        postings=[*postings, new_posting],
        payload_sha256_by_job={
            postings[0].source_job_id: "payload-1",
            new_posting.source_job_id: "payload-2",
        },
        missing_snapshots_before_close=2,
    )

    assert (
        repository.execute_scalar("SELECT COUNT(*) FROM job_events WHERE event_type = 'baseline'")
        == 1
    )
    assert (
        repository.execute_scalar("SELECT COUNT(*) FROM job_events WHERE event_type = 'opened'")
        == 1
    )


def test_hiring_service_archives_and_uses_bounded_detail_cache(tmp_path: Path) -> None:
    list_payload = (FIXTURES / "caterpillar_jobs_page.html").read_bytes()
    detail_payload = (FIXTURES / "caterpillar_job_detail.html").read_bytes()
    detail_requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if "/jobs/r" in request.url.path:
            detail_requests.append(request.url.path)
            return httpx.Response(200, content=detail_payload)
        response_payload = (
            list_payload.replace(b"r0000000001", b"r0000000003").replace(
                b"r0000000002", b"r0000000004"
            )
            if request.url.params.get("page") == "2"
            else list_payload
        )
        return httpx.Response(200, content=response_payload)

    source = _source(CareerSourceType.HTML_PAGINATED).model_copy(
        update={"detail_json_ld": True, "detail_fetch_limit_per_run": 2}
    )
    config = load_hiring_config(HIRING_CONFIG).model_copy(update={"sources": (source,)})
    registry = load_company_registry(REGISTRY)
    repository = DuckDBRepository(tmp_path / "service.duckdb")
    client = CareerSourceClient(
        Settings(PORTWATCH_CAREERS_REQUEST_INTERVAL_SECONDS=0),
        source,
        transport=httpx.MockTransport(handler),
    )
    service = HiringIngestionService(
        client=client,
        repository=repository,
        config=config,
        registry=registry,
    )
    try:
        first = service.ingest()
        second = service.ingest()
        third = service.ingest()
        with duckdb.connect(str(repository.database_path)) as connection:
            connection.execute(
                "UPDATE raw_payload_links SET retrieved_at = TIMESTAMPTZ '2020-01-01' "
                "WHERE resource_type LIKE 'career_detail:%'"
            )
        fourth = service.ingest()
    finally:
        client.close()

    assert first.status is IngestionStatus.SUCCEEDED
    assert first.records_received == 4
    assert first.records_written == 4
    assert second.records_written == 2
    assert third.records_written == 0
    assert fourth.records_written == 0
    assert len(detail_requests) == 6
    assert repository.execute_scalar("SELECT COUNT(*) FROM job_postings") == 4
    assert repository.execute_scalar("SELECT COUNT(*) FROM job_events") == 6
    assert repository.execute_scalar("SELECT COUNT(*) FROM raw_payloads") == 3
    assert repository.execute_scalar("SELECT COUNT(*) FROM raw_payload_links") == 14
    assert audit_hiring_state(repository).healthy


def test_hiring_analytics_keep_company_and_location_grains_distinct(tmp_path: Path) -> None:
    observed_at = datetime.now(UTC) - timedelta(days=5)
    source = _source(CareerSourceType.HTML_PAGINATED)
    postings, _ = parse_html_page(
        (FIXTURES / "caterpillar_jobs_page.html").read_bytes(),
        source,
        observed_at,
    )
    repository = DuckDBRepository(tmp_path / "analytics.duckdb")
    repository.initialize()
    repository.apply_job_snapshot(
        "run",
        source_id=source.source_id,
        observed_at=observed_at,
        postings=postings,
        payload_sha256_by_job={posting.source_job_id: "payload" for posting in postings},
        missing_snapshots_before_close=2,
    )
    jobs = repository.job_postings_summary()
    events = repository.job_events_summary()
    locations = repository.job_locations_summary()

    signals = compute_hiring_company_signals(jobs, events, locations)
    business_lines = business_line_hiring_summary(jobs)
    location_summary = location_hiring_summary(locations)

    assert signals.iloc[0]["active_postings"] == 2
    assert signals.iloc[0]["new_postings_28d"] == 0
    assert signals.iloc[0]["active_locations"] == 3
    assert business_lines.iloc[0]["business_line"] == "Unclassified"
    assert location_summary["active_postings"].sum() == 3

    duplicate_jobs = pd.concat([jobs, jobs.assign(source_id="second_source")])
    duplicate_locations = pd.concat(
        [locations, locations.assign(source_id="second_source")],
        ignore_index=True,
    )
    assert business_line_hiring_summary(duplicate_jobs).iloc[0]["active_postings"] == 4
    assert function_hiring_summary(duplicate_jobs)["active_postings"].sum() == 4
    assert location_hiring_summary(duplicate_locations)["active_postings"].sum() == 6
    themed_jobs = duplicate_jobs.assign(themes_json='["Automation"]')
    assert theme_hiring_summary(themed_jobs)["active_postings"].sum() == 4

    remote_locations = locations.copy()
    remote_locations["is_remote"] = False
    first_job = remote_locations.iloc[0]["source_job_id"]
    remote_locations.loc[
        remote_locations["source_job_id"] == first_job,
        "is_remote",
    ] = [True, False]
    remote_signals = compute_hiring_company_signals(jobs, events, remote_locations)
    assert remote_signals.iloc[0]["remote_share"] == 0.5


def test_hiring_cli_and_config_are_discoverable() -> None:
    config = load_hiring_config(HIRING_CONFIG)
    hiring_help = CliRunner().invoke(app, ["ingest", "hiring", "--help"])
    audit_help = CliRunner().invoke(app, ["audit", "hiring", "--help"])
    qwi_help = CliRunner().invoke(app, ["ingest", "qwi", "--help"])

    assert config.source("cat_careers").source_type is CareerSourceType.HTML_PAGINATED
    assert hiring_help.exit_code == 0
    assert "career" in hiring_help.stdout.lower()
    assert audit_help.exit_code == 0
    assert "persisted" in audit_help.stdout.lower()
    assert qwi_help.exit_code == 0
    assert "QWI" in qwi_help.stdout
