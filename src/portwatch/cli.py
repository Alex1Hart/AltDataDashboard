from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer

from portwatch.backfill import BackfillService
from portwatch.config import get_settings
from portwatch.contract_service import FederalContractIngestionService
from portwatch.health import audit_contract_state, audit_hiring_state
from portwatch.hiring_config import load_hiring_config
from portwatch.hiring_service import HiringIngestionService
from portwatch.ingestion.careers import CareerSourceClient
from portwatch.ingestion.census import CensusPortHSClient
from portwatch.ingestion.port_of_la import PortOfLosAngelesClient
from portwatch.ingestion.qwi import CensusQWIClient
from portwatch.ingestion.sec import SecEdgarClient
from portwatch.ingestion.usaspending import USAspendingClient
from portwatch.labor_service import LaborMarketIngestionService
from portwatch.port_service import PortOperationsIngestionService
from portwatch.project_config import load_project_config
from portwatch.registry import load_company_registry
from portwatch.sec_service import SecEdgarIngestionService
from portwatch.service import IngestionService
from portwatch.storage.duckdb import DuckDBRepository

app = typer.Typer(
    name="portwatch",
    help="Ingest and explore company evidence with contextual port and trade data.",
    no_args_is_help=True,
)
ingest_app = typer.Typer(help="Run a source-specific ingestion job.")
audit_app = typer.Typer(help="Validate persisted analytical state and invariants.")
app.add_typer(ingest_app, name="ingest")
app.add_typer(audit_app, name="audit")


def _parse_month(value: str) -> date:
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise typer.BadParameter("month must use YYYY-MM format") from exc
    return date(parsed.year, parsed.month, 1)


def _parse_date(value: str, *, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD format") from exc


@app.command("init-db")
def initialize_database() -> None:
    """Create or migrate the local analytical database."""
    settings = get_settings()
    repository = DuckDBRepository(settings.database_path)
    repository.initialize()
    typer.echo(f"Initialized {settings.database_path}")


@audit_app.command("hiring")
def audit_hiring() -> None:
    """Fail unless the persisted HiringWatch state is complete and internally consistent."""
    repository = DuckDBRepository(get_settings().database_path)
    repository.initialize()
    report = audit_hiring_state(repository)
    typer.echo(json.dumps({**asdict(report), "healthy": report.healthy}, indent=2))
    if not report.healthy:
        raise typer.Exit(1)


@audit_app.command("contracts")
def audit_contracts() -> None:
    """Fail unless persisted ContractWatch awards, actions, and vintages are consistent."""
    repository = DuckDBRepository(get_settings().database_path)
    repository.initialize()
    report = audit_contract_state(repository)
    typer.echo(json.dumps({**asdict(report), "healthy": report.healthy}, indent=2))
    if not report.healthy:
        raise typer.Exit(1)


@ingest_app.command("census")
def ingest_census(
    month: str = typer.Option(..., help="Calendar month in YYYY-MM format."),
    port: str = typer.Option(..., help="Four-digit Schedule D port code, e.g. 2704."),
    commodity: str = typer.Option(..., help="Two-, four-, or six-digit HS code."),
    country: str | None = typer.Option(None, help="Optional four-digit Schedule C code."),
) -> None:
    """Ingest one monthly Census port/commodity slice."""
    settings = get_settings()
    census_client = CensusPortHSClient(settings)
    service = IngestionService(
        census_client=census_client,
        repository=DuckDBRepository(settings.database_path),
    )
    try:
        result = service.ingest_census_month(
            month=_parse_month(month),
            port_code=port,
            commodity_code=commodity,
            country_code=country,
        )
    finally:
        census_client.close()
    typer.echo(result.model_dump_json(indent=2))


@ingest_app.command("port-la")
def ingest_port_of_los_angeles() -> None:
    """Ingest the latest public Port of Los Angeles monthly container statistics."""
    settings = get_settings()
    service = PortOperationsIngestionService(
        client=PortOfLosAngelesClient(settings),
        repository=DuckDBRepository(settings.database_path),
    )
    result = service.ingest_latest()
    typer.echo(result.model_dump_json(indent=2))


@ingest_app.command("sec")
def ingest_sec_edgar(
    ticker: str = typer.Option(..., help="Registered public-company ticker, e.g. CAT."),
) -> None:
    """Ingest SEC submissions and Company Facts for a reviewed registry issuer."""
    settings = get_settings()
    registry = load_company_registry(settings.company_registry_path)

    def progress(message: str) -> None:
        typer.echo(message, err=True)

    service = SecEdgarIngestionService(
        client=SecEdgarClient(settings, registry, progress_fn=progress),
        repository=DuckDBRepository(settings.database_path),
        progress_fn=progress,
    )
    result = service.ingest_company(ticker)
    typer.echo(result.model_dump_json(indent=2))


@ingest_app.command("contracts")
def ingest_federal_contracts(
    ticker: str = typer.Option(..., help="Registered public-company ticker, e.g. CAT."),
    start: str | None = typer.Option(
        None,
        help="Award action window start in YYYY-MM-DD; defaults to trailing three years.",
    ),
    end: str | None = typer.Option(
        None,
        help="Award action window end in YYYY-MM-DD; defaults to today.",
    ),
) -> None:
    """Ingest entity-resolved prime awards and signed actions from USAspending."""
    settings = get_settings()
    registry = load_company_registry(settings.company_registry_path)
    end_date = date.today() if end is None else _parse_date(end, option_name="end")
    start_date = (
        end_date - timedelta(days=3 * 365)
        if start is None
        else _parse_date(start, option_name="start")
    )

    def progress(message: str) -> None:
        typer.echo(message, err=True)

    client = USAspendingClient(settings, registry, progress_fn=progress)
    service = FederalContractIngestionService(
        client=client,
        repository=DuckDBRepository(settings.database_path),
        progress_fn=progress,
    )
    try:
        result = service.ingest_company(
            ticker,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        client.close()
    typer.echo(result.model_dump_json(indent=2))


@ingest_app.command("hiring")
def ingest_company_hiring(
    source_id: str | None = typer.Option(
        None,
        "--source",
        help="Configured career source ID; omit to run all enabled sources.",
    ),
    ticker: str | None = typer.Option(
        None,
        help="Run all enabled career sources for one registered ticker.",
    ),
    detail_limit: int | None = typer.Option(
        None,
        "--detail-limit",
        min=0,
        max=1_000,
        help="Override the per-source cap for new JSON-LD detail-page enrichments.",
    ),
) -> None:
    """Ingest complete first-party career snapshots and derive lifecycle events."""
    settings = get_settings()
    config = load_hiring_config(settings.hiring_config_path)
    registry = load_company_registry(settings.company_registry_path)
    selected = [career_source for career_source in config.sources if career_source.enabled]
    if source_id is not None:
        selected = [
            career_source for career_source in selected if career_source.source_id == source_id
        ]
    if ticker is not None:
        selected = [
            career_source for career_source in selected if career_source.ticker == ticker.upper()
        ]
    if detail_limit is not None:
        selected = [
            career_source.model_copy(update={"detail_fetch_limit_per_run": detail_limit})
            for career_source in selected
        ]
    if not selected:
        typer.echo("No enabled career sources matched the requested filters.", err=True)
        raise typer.Exit(2)

    def progress(message: str) -> None:
        typer.echo(message, err=True)

    for career_source in selected:
        client = CareerSourceClient(settings, career_source, progress_fn=progress)
        service = HiringIngestionService(
            client=client,
            repository=DuckDBRepository(settings.database_path),
            config=config,
            registry=registry,
            progress_fn=progress,
        )
        try:
            result = service.ingest()
        finally:
            client.close()
        typer.echo(result.model_dump_json(indent=2))


@ingest_app.command("qwi")
def ingest_census_qwi(
    year: int = typer.Option(..., min=1990, max=2200, help="Calendar year."),
    quarter: int = typer.Option(..., min=1, max=4, help="Calendar quarter, 1-4."),
    industry: str = typer.Option(..., help="Two- to six-digit NAICS industry code."),
    geography_code: str = typer.Option(..., "--geography-code", help="Census geography code."),
    geography_level: str = typer.Option(
        "state",
        "--geography-level",
        help="state or county.",
    ),
    seasonally_adjusted: bool = typer.Option(
        False,
        "--seasonally-adjusted",
        help="Request seasonally adjusted QWI values.",
    ),
) -> None:
    """Ingest official Census QWI employment, hiring, and separation benchmarks."""
    settings = get_settings()
    client = CensusQWIClient(settings)
    service = LaborMarketIngestionService(
        client=client,
        repository=DuckDBRepository(settings.database_path),
    )
    try:
        result = service.ingest_qwi(
            year=year,
            quarter=quarter,
            industry_code=industry,
            geography_level=geography_level,
            geography_code=geography_code,
            seasonally_adjusted=seasonally_adjusted,
        )
    finally:
        client.close()
    typer.echo(result.model_dump_json(indent=2))


@app.command()
def backfill(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Project YAML configuration; defaults to PORTWATCH_PROJECT_CONFIG_PATH.",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(help="Re-request slices already marked successful."),
    ] = False,
) -> None:
    """Run the resumable Census backfill defined by the project configuration."""
    settings = get_settings()
    if not settings.census_api_key:
        typer.echo("CENSUS_API_KEY is required before running a backfill.", err=True)
        raise typer.Exit(2)
    repository = DuckDBRepository(settings.database_path)
    census_client = CensusPortHSClient(settings)
    ingestion_service = IngestionService(
        census_client=census_client,
        repository=repository,
    )

    def progress(message: str) -> None:
        typer.echo(message, err=True)

    service = BackfillService(
        ingestion_service=ingestion_service,
        repository=repository,
        progress_fn=progress,
    )
    try:
        project = load_project_config(config_path or settings.project_config_path)
        summary = service.run(project, force=force)
    finally:
        census_client.close()
    typer.echo(
        f"planned={summary.planned} succeeded={summary.succeeded} "
        f"skipped={summary.skipped} failed={summary.failed} aborted={summary.aborted}"
    )


@app.command()
def dashboard() -> None:
    """Launch the local Streamlit research dashboard."""
    dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
    command = [sys.executable, "-m", "streamlit", "run", str(dashboard_path)]
    raise typer.Exit(subprocess.call(command))


if __name__ == "__main__":
    app()
