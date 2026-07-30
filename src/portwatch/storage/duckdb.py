from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from portwatch.models import (
    IngestionStatus,
    PortOperation,
    SecCompanyFact,
    SecFiling,
    SourceName,
    TradeFlow,
)


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
                    country_code VARCHAR,
                    entity_id VARCHAR,
                    ticker VARCHAR
                );

                CREATE TABLE IF NOT EXISTS raw_payloads (
                    payload_sha256 VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    retrieved_at TIMESTAMPTZ NOT NULL,
                    content BLOB NOT NULL
                );

                CREATE TABLE IF NOT EXISTS raw_payload_links (
                    run_id VARCHAR NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    resource_type VARCHAR NOT NULL,
                    source_url VARCHAR,
                    retrieved_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (run_id, payload_sha256, resource_type)
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

                CREATE TABLE IF NOT EXISTS sec_filings (
                    accession_number VARCHAR PRIMARY KEY,
                    entity_id VARCHAR NOT NULL,
                    cik VARCHAR NOT NULL,
                    company_name VARCHAR NOT NULL,
                    form VARCHAR NOT NULL,
                    filed_on DATE NOT NULL,
                    report_date DATE,
                    accepted_at TIMESTAMPTZ NOT NULL,
                    primary_document VARCHAR NOT NULL,
                    primary_document_url VARCHAR NOT NULL,
                    is_xbrl BOOLEAN NOT NULL,
                    is_inline_xbrl BOOLEAN NOT NULL,
                    source VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    payload_sha256 VARCHAR NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sec_company_fact_revisions (
                    fact_id VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    cik VARCHAR NOT NULL,
                    taxonomy VARCHAR NOT NULL,
                    tag VARCHAR NOT NULL,
                    label VARCHAR NOT NULL,
                    description VARCHAR NOT NULL,
                    unit VARCHAR NOT NULL,
                    value_text VARCHAR NOT NULL,
                    period_start DATE,
                    period_end DATE NOT NULL,
                    filed_on DATE NOT NULL,
                    accepted_at TIMESTAMPTZ NOT NULL,
                    acceptance_is_estimated BOOLEAN NOT NULL,
                    accession_number VARCHAR NOT NULL,
                    fiscal_year INTEGER,
                    fiscal_period VARCHAR,
                    form VARCHAR NOT NULL,
                    frame VARCHAR,
                    source VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    revision_number INTEGER NOT NULL,
                    valid_from TIMESTAMPTZ NOT NULL,
                    valid_until TIMESTAMPTZ,
                    PRIMARY KEY (fact_id, revision_number)
                );

                CREATE TABLE IF NOT EXISTS sec_company_facts (
                    fact_id VARCHAR PRIMARY KEY,
                    entity_id VARCHAR NOT NULL,
                    cik VARCHAR NOT NULL,
                    taxonomy VARCHAR NOT NULL,
                    tag VARCHAR NOT NULL,
                    label VARCHAR NOT NULL,
                    description VARCHAR NOT NULL,
                    unit VARCHAR NOT NULL,
                    value_text VARCHAR NOT NULL,
                    period_start DATE,
                    period_end DATE NOT NULL,
                    filed_on DATE NOT NULL,
                    accepted_at TIMESTAMPTZ NOT NULL,
                    acceptance_is_estimated BOOLEAN NOT NULL,
                    accession_number VARCHAR NOT NULL,
                    fiscal_year INTEGER,
                    fiscal_period VARCHAR,
                    form VARCHAR NOT NULL,
                    frame VARCHAR,
                    source VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    revision_number INTEGER NOT NULL DEFAULT 1
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
                ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS entity_id VARCHAR;
                ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS ticker VARCHAR;
                ALTER TABLE trade_flows ADD COLUMN IF NOT EXISTS publication_at TIMESTAMPTZ;
                ALTER TABLE trade_flows ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
                ALTER TABLE trade_flows ADD COLUMN IF NOT EXISTS revision_number INTEGER DEFAULT 1;
                ALTER TABLE trade_flows ADD COLUMN IF NOT EXISTS payload_sha256 VARCHAR;
                ALTER TABLE sec_company_facts
                    ADD COLUMN IF NOT EXISTS revision_number INTEGER DEFAULT 1;
                UPDATE trade_flows
                SET publication_at = COALESCE(publication_at, source_updated_at, ingested_at),
                    available_at = COALESCE(available_at, ingested_at),
                    revision_number = COALESCE(revision_number, 1);

                INSERT OR IGNORE INTO trade_flow_revisions
                SELECT *, ingested_at AS valid_from, CAST(NULL AS TIMESTAMPTZ) AS valid_until
                FROM trade_flows;

                UPDATE sec_company_facts
                SET revision_number = COALESCE(revision_number, 1);
                INSERT OR IGNORE INTO sec_company_fact_revisions
                SELECT *, ingested_at AS valid_from, CAST(NULL AS TIMESTAMPTZ) AS valid_until
                FROM sec_company_facts;
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
        entity_id: str | None = None,
        ticker: str | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_runs (
                    run_id, source, status, started_at,
                    period_start, port_code, commodity_code, country_code,
                    entity_id, ticker
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    entity_id,
                    ticker,
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

    def store_raw_payload(
        self,
        run_id: str,
        source: SourceName,
        content: bytes,
        *,
        resource_type: str | None = None,
        source_url: str | None = None,
    ) -> str:
        payload_hash = hashlib.sha256(content).hexdigest()
        retrieved_at = datetime.now(UTC)
        with self._connect() as connection:
            with self._transaction(connection):
                connection.execute(
                    """
                    INSERT OR IGNORE INTO raw_payloads
                        (payload_sha256, run_id, source, retrieved_at, content)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [payload_hash, run_id, source.value, retrieved_at, content],
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO raw_payload_links
                        (run_id, payload_sha256, resource_type, source_url, retrieved_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        payload_hash,
                        resource_type or source.value,
                        source_url,
                        retrieved_at,
                    ],
                )
        return payload_hash

    def store_sec_edgar_batch(
        self,
        run_id: str,
        filings: list[SecFiling],
        facts: list[SecCompanyFact],
        *,
        submissions_payload_sha256: str,
        company_facts_payload_sha256: str,
    ) -> int:
        """Atomically persist immutable SEC filing metadata and accession-linked facts."""
        with self._connect() as connection, self._transaction(connection):
            filing_count_before = self._connection_scalar_int(
                connection,
                "SELECT COUNT(*) FROM sec_filings",
            )
            filing_rows = [
                (
                    filing.accession_number,
                    filing.entity_id,
                    filing.cik,
                    filing.company_name,
                    filing.form,
                    filing.filed_on,
                    filing.report_date,
                    filing.accepted_at,
                    filing.primary_document,
                    filing.primary_document_url,
                    filing.is_xbrl,
                    filing.is_inline_xbrl,
                    filing.source.value,
                    filing.ingested_at,
                    run_id,
                    filing.accepted_at,
                    filing.ingested_at,
                    submissions_payload_sha256,
                )
                for filing in filings
            ]
            if filing_rows:
                filing_frame = pd.DataFrame.from_records(
                    filing_rows,
                    columns=(
                        "accession_number",
                        "entity_id",
                        "cik",
                        "company_name",
                        "form",
                        "filed_on",
                        "report_date",
                        "accepted_at",
                        "primary_document",
                        "primary_document_url",
                        "is_xbrl",
                        "is_inline_xbrl",
                        "source",
                        "ingested_at",
                        "run_id",
                        "publication_at",
                        "available_at",
                        "payload_sha256",
                    ),
                )
                connection.register("_sec_filing_batch", filing_frame)
                try:
                    connection.execute(
                        "INSERT OR IGNORE INTO sec_filings SELECT * FROM _sec_filing_batch"
                    )
                finally:
                    connection.unregister("_sec_filing_batch")

            existing_by_id: dict[str, tuple[Any, ...]] = {}
            if facts:
                existing_rows = connection.execute(
                    """
                    SELECT fact_id, revision_number, label, description, value_text, accepted_at,
                           acceptance_is_estimated, filed_on, fiscal_year, fiscal_period,
                           form, frame
                    FROM sec_company_facts
                    WHERE entity_id = ?
                    """,
                    [facts[0].entity_id],
                ).fetchall()
                existing_by_id = {
                    str(existing_row[0]): tuple(existing_row[1:])
                    for existing_row in existing_rows
                }

            fact_changes = 0
            fact_rows: list[tuple[Any, ...]] = []
            fact_revision_rows: list[tuple[Any, ...]] = []
            revisions_to_close: list[tuple[datetime, str]] = []
            for fact in facts:
                existing = existing_by_id.get(fact.fact_id)
                value_text = format(fact.value, "f")
                candidate = (
                    fact.label,
                    fact.description,
                    value_text,
                    fact.accepted_at,
                    fact.acceptance_is_estimated,
                    fact.filed_on,
                    fact.fiscal_year,
                    fact.fiscal_period,
                    fact.form,
                    fact.frame,
                )
                if existing is not None and tuple(existing[1:]) == candidate:
                    continue
                revision_number = 1 if existing is None else int(existing[0]) + 1
                if existing is not None:
                    revisions_to_close.append((fact.ingested_at, fact.fact_id))
                row = (
                    fact.fact_id,
                    fact.entity_id,
                    fact.cik,
                    fact.taxonomy,
                    fact.tag,
                    fact.label,
                    fact.description,
                    fact.unit,
                    value_text,
                    fact.period_start,
                    fact.period_end,
                    fact.filed_on,
                    fact.accepted_at,
                    fact.acceptance_is_estimated,
                    fact.accession_number,
                    fact.fiscal_year,
                    fact.fiscal_period,
                    fact.form,
                    fact.frame,
                    fact.source.value,
                    fact.ingested_at,
                    run_id,
                    fact.accepted_at,
                    fact.ingested_at,
                    company_facts_payload_sha256,
                    revision_number,
                )
                fact_rows.append(row)
                fact_revision_rows.append((*row, fact.ingested_at, None))
                fact_changes += 1

            if revisions_to_close:
                closing_frame = pd.DataFrame.from_records(
                    revisions_to_close,
                    columns=("valid_until", "fact_id"),
                )
                connection.register("_sec_revisions_to_close", closing_frame)
                try:
                    connection.execute(
                        """
                        UPDATE sec_company_fact_revisions AS revisions
                        SET valid_until = changes.valid_until
                        FROM _sec_revisions_to_close AS changes
                        WHERE revisions.fact_id = changes.fact_id
                          AND revisions.valid_until IS NULL
                        """
                    )
                finally:
                    connection.unregister("_sec_revisions_to_close")
            if fact_rows:
                fact_columns = (
                    "fact_id",
                    "entity_id",
                    "cik",
                    "taxonomy",
                    "tag",
                    "label",
                    "description",
                    "unit",
                    "value_text",
                    "period_start",
                    "period_end",
                    "filed_on",
                    "accepted_at",
                    "acceptance_is_estimated",
                    "accession_number",
                    "fiscal_year",
                    "fiscal_period",
                    "form",
                    "frame",
                    "source",
                    "ingested_at",
                    "run_id",
                    "publication_at",
                    "available_at",
                    "payload_sha256",
                    "revision_number",
                )
                fact_frame = pd.DataFrame.from_records(fact_rows, columns=fact_columns)
                revision_frame = pd.DataFrame.from_records(
                    fact_revision_rows,
                    columns=(*fact_columns, "valid_from", "valid_until"),
                )
                connection.register("_sec_fact_batch", fact_frame)
                connection.register("_sec_fact_revision_batch", revision_frame)
                try:
                    connection.execute(
                        "INSERT OR REPLACE INTO sec_company_facts SELECT * FROM _sec_fact_batch"
                    )
                    connection.execute(
                        """
                        INSERT INTO sec_company_fact_revisions
                        SELECT * FROM _sec_fact_revision_batch
                        """
                    )
                finally:
                    connection.unregister("_sec_fact_revision_batch")
                    connection.unregister("_sec_fact_batch")
            filing_count_after = self._connection_scalar_int(
                connection,
                "SELECT COUNT(*) FROM sec_filings",
            )
        return (filing_count_after - filing_count_before) + fact_changes

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
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        else:
            connection.execute("COMMIT")

    @staticmethod
    def _connection_scalar_int(
        connection: duckdb.DuckDBPyConnection,
        query: str,
    ) -> int:
        row = connection.execute(query).fetchone()
        if row is None:
            raise RuntimeError("expected scalar database query to return one row")
        return int(row[0])

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

    def sec_filings_summary(
        self,
        limit: int = 100,
        *,
        forms: tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        where_clause = ""
        parameters: list[Any] = []
        if forms:
            placeholders = ", ".join("?" for _ in forms)
            where_clause = f"WHERE form IN ({placeholders})"
            parameters.extend(forms)
        parameters.append(limit)
        with self._connect() as connection:
            return connection.execute(
                f"""
                SELECT entity_id, cik, company_name, accession_number, form,
                       filed_on, report_date, accepted_at, primary_document_url,
                       is_xbrl, is_inline_xbrl, publication_at, available_at,
                       payload_sha256
                FROM sec_filings
                {where_clause}
                ORDER BY accepted_at DESC
                LIMIT ?
                """,
                parameters,
            ).fetchdf()

    def sec_company_facts_summary(self, limit: int = 500) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT entity_id, cik, taxonomy, tag, label, unit, value_text,
                       TRY_CAST(value_text AS DOUBLE) AS value_numeric,
                       period_start, period_end, filed_on, accepted_at,
                       acceptance_is_estimated, accession_number, fiscal_year,
                       fiscal_period, form, frame, publication_at, available_at,
                       payload_sha256, revision_number
                FROM sec_company_facts
                ORDER BY accepted_at DESC, taxonomy, tag
                LIMIT ?
                """,
                [limit],
            ).fetchdf()

    def sec_company_fact_catalog(self) -> pd.DataFrame:
        """Return one compact row per available company concept and unit."""
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT entity_id, cik, taxonomy, tag, label, unit,
                       COUNT(*) AS observation_count,
                       COUNT(DISTINCT accession_number) AS filing_count,
                       MIN(period_end) AS first_period_end,
                       MAX(period_end) AS latest_period_end,
                       MAX(filed_on) AS latest_filed_on
                FROM sec_company_facts
                GROUP BY entity_id, cik, taxonomy, tag, label, unit
                ORDER BY entity_id, taxonomy, label, unit
                """
            ).fetchdf()

    def sec_company_fact_history(
        self,
        *,
        entity_id: str,
        taxonomy: str,
        tag: str,
        unit: str,
        limit: int = 1_000,
    ) -> pd.DataFrame:
        """Query the complete normalized history for one selected company concept."""
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT entity_id, cik, taxonomy, tag, label, unit, value_text,
                       TRY_CAST(value_text AS DOUBLE) AS value_numeric,
                       period_start, period_end, filed_on, accepted_at,
                       acceptance_is_estimated, accession_number, fiscal_year,
                       fiscal_period, form, frame, publication_at, available_at,
                       payload_sha256, revision_number
                FROM sec_company_facts
                WHERE entity_id = ? AND taxonomy = ? AND tag = ? AND unit = ?
                ORDER BY period_end, accepted_at
                LIMIT ?
                """,
                [entity_id, taxonomy, tag, unit, limit],
            ).fetchdf()

    def source_counts(self) -> dict[str, int]:
        """Return full-table record counts for dashboard coverage indicators."""
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM trade_flows),
                    (SELECT COUNT(*) FROM port_operations),
                    (SELECT COUNT(*) FROM sec_filings),
                    (SELECT COUNT(*) FROM sec_company_facts),
                    (SELECT COUNT(DISTINCT tag) FROM sec_company_facts)
                """
            ).fetchone()
        if row is None:
            raise RuntimeError("expected source-count query to return one row")
        names = (
            "trade_flows",
            "port_operations",
            "sec_filings",
            "sec_company_facts",
            "sec_company_concepts",
        )
        return {name: int(value) for name, value in zip(names, row, strict=True)}

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
