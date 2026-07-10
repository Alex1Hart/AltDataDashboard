from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import typer

from portwatch.config import get_settings
from portwatch.ingestion.census import CensusPortHSClient
from portwatch.service import IngestionService
from portwatch.storage.duckdb import DuckDBRepository

app = typer.Typer(
    name="portwatch",
    help="Ingest and explore U.S. port and industrial trade-flow data.",
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
    service = IngestionService(
        census_client=CensusPortHSClient(settings),
        repository=DuckDBRepository(settings.database_path),
    )
    result = service.ingest_census_month(
        month=_parse_month(month),
        port_code=port,
        commodity_code=commodity,
        country_code=country,
    )
    typer.echo(result.model_dump_json(indent=2))


@app.command()
def dashboard() -> None:
    """Launch the local Streamlit research dashboard."""
    dashboard_path = Path(__file__).parent / "dashboard" / "app.py"
    command = [sys.executable, "-m", "streamlit", "run", str(dashboard_path)]
    raise typer.Exit(subprocess.call(command))


if __name__ == "__main__":
    app()

