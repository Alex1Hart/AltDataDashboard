from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer

from portwatch.backfill import BackfillService
from portwatch.config import get_settings
from portwatch.contract_service import FederalContractIngestionService
from portwatch.ingestion.census import CensusPortHSClient
from portwatch.ingestion.port_of_la import PortOfLosAngelesClient
from portwatch.ingestion.sec import SecEdgarClient
from portwatch.ingestion.usaspending import USAspendingClient
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
app.add_typer(ingest_app, name="ingest")


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
    """Ingest entity-resolved prime contract awards from USAspending."""
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
