from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from portwatch.models import IngestionStatus, PortOperation, SourceName, TradeFlow


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
                    error_message VARCHAR,
                    period_start DATE,
                    port_code VARCHAR,
                    commodity_code VARCHAR,
                    country_code VARCHAR
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
                    publication_at TIMESTAMPTZ,
                    available_at TIMESTAMPTZ,
                    revision_number INTEGER NOT NULL DEFAULT 1,
                    payload_sha256 VARCHAR,
                    PRIMARY KEY (month, port_code, commodity_code, country_code, source)
                );

                CREATE TABLE IF NOT EXISTS trade_flow_revisions (
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
                    publication_at TIMESTAMPTZ,
                    available_at TIMESTAMPTZ,
                    revision_number INTEGER NOT NULL,
                    payload_sha256 VARCHAR,
                    valid_from TIMESTAMPTZ NOT NULL,
                    valid_until TIMESTAMPTZ,
                    PRIMARY KEY (
                        month, port_code, commodity_code, country_code,
                        source, revision_number
                    )
                );

                CREATE TABLE IF NOT EXISTS port_operations (
                    period_start DATE NOT NULL,
                    frequency VARCHAR NOT NULL,
                    port_code VARCHAR NOT NULL,
                    port_name VARCHAR NOT NULL,
                    metric VARCHAR NOT NULL,
                    value DECIMAL(38, 3) NOT NULL,
                    unit VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    source_url VARCHAR NOT NULL,
                    source_published_at TIMESTAMPTZ,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ,
                    available_at TIMESTAMPTZ,
                    revision_number INTEGER NOT NULL DEFAULT 1,
                    payload_sha256 VARCHAR,
                    PRIMARY KEY (period_start, port_code, metric, source)
                );

                CREATE TABLE IF NOT EXISTS port_operation_revisions (
                    period_start DATE NOT NULL,
                    frequency VARCHAR NOT NULL,
                    port_code VARCHAR NOT NULL,
                    port_name VARCHAR NOT NULL,
                    metric VARCHAR NOT NULL,
                    value DECIMAL(38, 3) NOT NULL,
                    unit VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    source_url VARCHAR NOT NULL,
                    source_published_at TIMESTAMPTZ,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ,
                    available_at TIMESTAMPTZ,
                    revision_number INTEGER NOT NULL,
                    payload_sha256 VARCHAR,
                    valid_from TIMESTAMPTZ NOT NULL,
                    valid_until TIMESTAMPTZ,
                    PRIMARY KEY (period_start, port_code, metric, source, revision_number)
                );
                """
            )

            # Forward-compatible, additive migration for databases created by v0.1.
            connection.execute(
                """
                ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS period_start DATE;
                ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS port_code VARCHAR;
                ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS commodity_code VARCHAR;
                ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS country_code VARCHAR;
                ALTER TABLE trade_flows ADD COLUMN IF NOT EXISTS publication_at TIMESTAMPTZ;
                ALTER TABLE trade_flows ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
                ALTER TABLE trade_flows ADD COLUMN IF NOT EXISTS revision_number INTEGER DEFAULT 1;
                ALTER TABLE trade_flows ADD COLUMN IF NOT EXISTS payload_sha256 VARCHAR;
                UPDATE trade_flows
                SET publication_at = COALESCE(publication_at, source_updated_at, ingested_at),
                    available_at = COALESCE(available_at, ingested_at),
                    revision_number = COALESCE(revision_number, 1);

                INSERT OR IGNORE INTO trade_flow_revisions
                SELECT *, ingested_at AS valid_from, CAST(NULL AS TIMESTAMPTZ) AS valid_until
                FROM trade_flows;
                """
            )

    def start_run(
        self,
        run_id: str,
        source: SourceName,
        started_at: datetime,
        *,
        period_start: date | None = None,
        port_code: str | None = None,
        commodity_code: str | None = None,
        country_code: str | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_runs (
                    run_id, source, status, started_at,
                    period_start, port_code, commodity_code, country_code
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    source.value,
                    IngestionStatus.STARTED.value,
                    started_at,
                    period_start,
                    port_code,
                    commodity_code,
                    country_code,
                ],
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

    def upsert_trade_flows(
        self,
        run_id: str,
        flows: list[TradeFlow],
        *,
        payload_sha256: str,
    ) -> int:
        changed = 0
        with self._connect() as connection, self._transaction(connection):
            for flow in flows:
                existing = connection.execute(
                    """
                    SELECT revision_number, port_name, commodity_description, country_name,
                           general_value_usd, vessel_value_usd, vessel_weight_kg,
                           containerized_value_usd, containerized_weight_kg
                    FROM trade_flows
                    WHERE month = ? AND port_code = ? AND commodity_code = ?
                      AND country_code = ? AND source = ?
                    """,
                    [
                        flow.month,
                        flow.port_code,
                        flow.commodity_code,
                        flow.country_code,
                        flow.source.value,
                    ],
                ).fetchone()
                candidate = (
                    flow.port_name,
                    flow.commodity_description,
                    flow.country_name,
                    flow.general_value_usd,
                    flow.vessel_value_usd,
                    flow.vessel_weight_kg,
                    flow.containerized_value_usd,
                    flow.containerized_weight_kg,
                )
                if existing is not None and tuple(existing[1:]) == candidate:
                    continue

                revision_number = 1 if existing is None else int(existing[0]) + 1
                publication_at = flow.source_updated_at or flow.ingested_at
                if existing is not None:
                    connection.execute(
                        """
                        UPDATE trade_flow_revisions SET valid_until = ?
                        WHERE month = ? AND port_code = ? AND commodity_code = ?
                          AND country_code = ? AND source = ? AND valid_until IS NULL
                        """,
                        [
                            flow.ingested_at,
                            flow.month,
                            flow.port_code,
                            flow.commodity_code,
                            flow.country_code,
                            flow.source.value,
                        ],
                    )

                row = (
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
                    publication_at,
                    flow.ingested_at,
                    revision_number,
                    payload_sha256,
                )
                connection.execute(
                    """
                    INSERT OR REPLACE INTO trade_flows VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    row,
                )
                connection.execute(
                    """
                    INSERT INTO trade_flow_revisions VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (*row, flow.ingested_at, None),
                )
                changed += 1
        return changed

    def upsert_port_operations(
        self,
        run_id: str,
        operations: list[PortOperation],
        *,
        payload_sha256: str,
    ) -> int:
        changed = 0
        with self._connect() as connection, self._transaction(connection):
            for operation in operations:
                existing = connection.execute(
                    """
                    SELECT revision_number, frequency, port_name, value, unit,
                           source_url, source_published_at
                    FROM port_operations
                    WHERE period_start = ? AND port_code = ? AND metric = ? AND source = ?
                    """,
                    [
                        operation.period_start,
                        operation.port_code,
                        operation.metric.value,
                        operation.source.value,
                    ],
                ).fetchone()
                candidate = (
                    operation.frequency,
                    operation.port_name,
                    operation.value,
                    operation.unit,
                    operation.source_url,
                    operation.source_published_at,
                )
                if existing is not None and tuple(existing[1:]) == candidate:
                    continue

                revision_number = 1 if existing is None else int(existing[0]) + 1
                publication_at = operation.source_published_at or operation.ingested_at
                if existing is not None:
                    connection.execute(
                        """
                        UPDATE port_operation_revisions SET valid_until = ?
                        WHERE period_start = ? AND port_code = ? AND metric = ?
                          AND source = ? AND valid_until IS NULL
                        """,
                        [
                            operation.ingested_at,
                            operation.period_start,
                            operation.port_code,
                            operation.metric.value,
                            operation.source.value,
                        ],
                    )

                row = (
                    operation.period_start,
                    operation.frequency,
                    operation.port_code,
                    operation.port_name,
                    operation.metric.value,
                    operation.value,
                    operation.unit,
                    operation.source.value,
                    operation.source_url,
                    operation.source_published_at,
                    operation.ingested_at,
                    run_id,
                    publication_at,
                    operation.ingested_at,
                    revision_number,
                    payload_sha256,
                )
                connection.execute(
                    """
                    INSERT OR REPLACE INTO port_operations VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    row,
                )
                connection.execute(
                    """
                    INSERT INTO port_operation_revisions VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (*row, operation.ingested_at, None),
                )
                changed += 1
        return changed

    def has_successful_trade_slice(
        self,
        *,
        source: SourceName,
        month: date,
        port_code: str,
        commodity_code: str,
        country_code: str | None = None,
    ) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM ingestion_runs
                WHERE source = ? AND period_start = ? AND port_code = ?
                  AND commodity_code = ?
                  AND country_code IS NOT DISTINCT FROM ?
                  AND status = ?
                LIMIT 1
                """,
                [
                    source.value,
                    month,
                    port_code,
                    commodity_code,
                    country_code,
                    IngestionStatus.SUCCEEDED.value,
                ],
            ).fetchone()
        return row is not None

    @staticmethod
    @contextmanager
    def _transaction(connection: duckdb.DuckDBPyConnection) -> Iterator[None]:
        connection.execute("BEGIN TRANSACTION")
        try:
            yield
        except Exception:
            connection.execute("ROLLBACK")
            raise
        else:
            connection.execute("COMMIT")

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
                    country_code,
                    country_name,
                    CAST(containerized_value_usd AS DOUBLE) AS containerized_value_usd,
                    CAST(containerized_weight_kg AS DOUBLE) AS containerized_weight_kg,
                    publication_at,
                    available_at,
                    revision_number
                FROM trade_flows
                ORDER BY month, port_code, commodity_code, containerized_value_usd DESC
                """
            ).fetchdf()

    def trade_flow_revisions(self, limit: int = 100) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT month, port_code, commodity_code, country_code, source,
                       revision_number, publication_at, available_at,
                       valid_from, valid_until, payload_sha256
                FROM trade_flow_revisions
                ORDER BY valid_from DESC
                LIMIT ?
                """,
                [limit],
            ).fetchdf()

    def port_operations_summary(self) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT period_start, frequency, port_code, port_name, metric,
                       CAST(value AS DOUBLE) AS value, unit, source_url,
                       publication_at, available_at, revision_number
                FROM port_operations
                ORDER BY period_start, metric
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
