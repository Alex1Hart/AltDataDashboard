from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from portwatch.models import IngestionStatus, SourceName, TradeFlow


class DuckDBRepository:
    """Local analytical store with idempotent writes and ingestion audit history."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    run_id VARCHAR PRIMARY KEY,
                    source VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    records_received INTEGER NOT NULL DEFAULT 0,
                    records_written INTEGER NOT NULL DEFAULT 0,
                    started_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ,
                    error_message VARCHAR
                );

                CREATE TABLE IF NOT EXISTS raw_payloads (
                    payload_sha256 VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    retrieved_at TIMESTAMPTZ NOT NULL,
                    content BLOB NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trade_flows (
                    month DATE NOT NULL,
                    port_code VARCHAR NOT NULL,
                    port_name VARCHAR NOT NULL,
                    commodity_code VARCHAR NOT NULL,
                    commodity_description VARCHAR NOT NULL,
                    country_code VARCHAR NOT NULL,
                    country_name VARCHAR NOT NULL,
                    general_value_usd DECIMAL(38, 2) NOT NULL,
                    vessel_value_usd DECIMAL(38, 2) NOT NULL,
                    vessel_weight_kg DECIMAL(38, 3) NOT NULL,
                    containerized_value_usd DECIMAL(38, 2) NOT NULL,
                    containerized_weight_kg DECIMAL(38, 3) NOT NULL,
                    source VARCHAR NOT NULL,
                    source_updated_at TIMESTAMPTZ,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    PRIMARY KEY (month, port_code, commodity_code, country_code, source)
                );
                """
            )

    def start_run(self, run_id: str, source: SourceName, started_at: datetime) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_runs (run_id, source, status, started_at)
                VALUES (?, ?, ?, ?)
                """,
                [run_id, source.value, IngestionStatus.STARTED.value, started_at],
            )

    def finish_run(
        self,
        run_id: str,
        *,
        status: IngestionStatus,
        records_received: int,
        records_written: int,
        error_message: str | None = None,
    ) -> datetime:
        completed_at = datetime.now(UTC)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE ingestion_runs
                SET status = ?, records_received = ?, records_written = ?,
                    completed_at = ?, error_message = ?
                WHERE run_id = ?
                """,
                [
                    status.value,
                    records_received,
                    records_written,
                    completed_at,
                    error_message,
                    run_id,
                ],
            )
        return completed_at

    def store_raw_payload(self, run_id: str, source: SourceName, content: bytes) -> str:
        payload_hash = hashlib.sha256(content).hexdigest()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO raw_payloads
                    (payload_sha256, run_id, source, retrieved_at, content)
                VALUES (?, ?, ?, ?, ?)
                """,
                [payload_hash, run_id, source.value, datetime.now(UTC), content],
            )
        return payload_hash

    def upsert_trade_flows(self, run_id: str, flows: list[TradeFlow]) -> int:
        rows = [
            (
                flow.month,
                flow.port_code,
                flow.port_name,
                flow.commodity_code,
                flow.commodity_description,
                flow.country_code,
                flow.country_name,
                flow.general_value_usd,
                flow.vessel_value_usd,
                flow.vessel_weight_kg,
                flow.containerized_value_usd,
                flow.containerized_weight_kg,
                flow.source.value,
                flow.source_updated_at,
                flow.ingested_at,
                run_id,
            )
            for flow in flows
        ]
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT OR REPLACE INTO trade_flows VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                rows,
            )
        return len(rows)

    def trade_flow_summary(self) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT
                    month,
                    port_code,
                    port_name,
                    commodity_code,
                    commodity_description,
                    country_name,
                    CAST(containerized_value_usd AS DOUBLE) AS containerized_value_usd,
                    CAST(containerized_weight_kg AS DOUBLE) AS containerized_weight_kg
                FROM trade_flows
                ORDER BY month, port_code, commodity_code, containerized_value_usd DESC
                """
            ).fetchdf()

    def recent_runs(self, limit: int = 20) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT * FROM ingestion_runs
                ORDER BY started_at DESC
                LIMIT ?
                """,
                [limit],
            ).fetchdf()

    def execute_scalar(self, query: str, parameters: list[Any] | None = None) -> Any:
        with self._connect() as connection:
            row = connection.execute(query, parameters or []).fetchone()
        return None if row is None else row[0]

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.database_path))
