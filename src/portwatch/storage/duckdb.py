from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from portwatch.models import (
    FederalContractAward,
    FederalContractTransaction,
    IngestionStatus,
    JobEventType,
    JobPosting,
    JobStatus,
    LaborMarketObservation,
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
                    ticker VARCHAR,
                    period_end DATE,
                    records_rejected INTEGER NOT NULL DEFAULT 0
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
                    source_url_key VARCHAR NOT NULL,
                    retrieved_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (run_id, payload_sha256, resource_type, source_url_key)
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

                CREATE TABLE IF NOT EXISTS federal_contract_awards (
                    award_key VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    ticker VARCHAR NOT NULL,
                    award_id VARCHAR NOT NULL,
                    recipient_name VARCHAR NOT NULL,
                    recipient_uei VARCHAR,
                    recipient_id VARCHAR,
                    match_method VARCHAR NOT NULL,
                    award_type VARCHAR NOT NULL,
                    description VARCHAR NOT NULL,
                    award_amount_usd DECIMAL(38, 2) NOT NULL,
                    total_outlays_usd DECIMAL(38, 2),
                    base_obligation_date DATE NOT NULL,
                    start_date DATE,
                    end_date DATE,
                    source_modified_at TIMESTAMPTZ,
                    awarding_agency VARCHAR,
                    awarding_sub_agency VARCHAR,
                    funding_agency VARCHAR,
                    funding_sub_agency VARCHAR,
                    naics_code VARCHAR,
                    naics_description VARCHAR,
                    psc_code VARCHAR,
                    psc_description VARCHAR,
                    place_of_performance_country VARCHAR,
                    place_of_performance_state VARCHAR,
                    source_url VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    revision_number INTEGER NOT NULL DEFAULT 1,
                    payload_sha256 VARCHAR NOT NULL,
                    PRIMARY KEY (award_key, source)
                );

                CREATE TABLE IF NOT EXISTS federal_contract_award_revisions (
                    award_key VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    ticker VARCHAR NOT NULL,
                    award_id VARCHAR NOT NULL,
                    recipient_name VARCHAR NOT NULL,
                    recipient_uei VARCHAR,
                    recipient_id VARCHAR,
                    match_method VARCHAR NOT NULL,
                    award_type VARCHAR NOT NULL,
                    description VARCHAR NOT NULL,
                    award_amount_usd DECIMAL(38, 2) NOT NULL,
                    total_outlays_usd DECIMAL(38, 2),
                    base_obligation_date DATE NOT NULL,
                    start_date DATE,
                    end_date DATE,
                    source_modified_at TIMESTAMPTZ,
                    awarding_agency VARCHAR,
                    awarding_sub_agency VARCHAR,
                    funding_agency VARCHAR,
                    funding_sub_agency VARCHAR,
                    naics_code VARCHAR,
                    naics_description VARCHAR,
                    psc_code VARCHAR,
                    psc_description VARCHAR,
                    place_of_performance_country VARCHAR,
                    place_of_performance_state VARCHAR,
                    source_url VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    revision_number INTEGER NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    valid_from TIMESTAMPTZ NOT NULL,
                    valid_until TIMESTAMPTZ,
                    PRIMARY KEY (award_key, source, revision_number)
                );

                CREATE TABLE IF NOT EXISTS federal_contract_transactions (
                    transaction_id VARCHAR NOT NULL,
                    award_key VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    ticker VARCHAR NOT NULL,
                    award_id VARCHAR NOT NULL,
                    action_date DATE NOT NULL,
                    federal_action_obligation_usd DECIMAL(38, 2) NOT NULL,
                    action_type VARCHAR,
                    action_type_description VARCHAR,
                    modification_number VARCHAR NOT NULL,
                    description VARCHAR NOT NULL,
                    award_type_code VARCHAR NOT NULL,
                    award_type_description VARCHAR,
                    source_url VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    revision_number INTEGER NOT NULL DEFAULT 1,
                    payload_sha256 VARCHAR NOT NULL,
                    PRIMARY KEY (transaction_id, source)
                );

                CREATE TABLE IF NOT EXISTS federal_contract_transaction_revisions (
                    transaction_id VARCHAR NOT NULL,
                    award_key VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    ticker VARCHAR NOT NULL,
                    award_id VARCHAR NOT NULL,
                    action_date DATE NOT NULL,
                    federal_action_obligation_usd DECIMAL(38, 2) NOT NULL,
                    action_type VARCHAR,
                    action_type_description VARCHAR,
                    modification_number VARCHAR NOT NULL,
                    description VARCHAR NOT NULL,
                    award_type_code VARCHAR NOT NULL,
                    award_type_description VARCHAR,
                    source_url VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    revision_number INTEGER NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    valid_from TIMESTAMPTZ NOT NULL,
                    valid_until TIMESTAMPTZ,
                    PRIMARY KEY (transaction_id, source, revision_number)
                );

                CREATE TABLE IF NOT EXISTS job_postings (
                    source_id VARCHAR NOT NULL,
                    source_job_id VARCHAR NOT NULL,
                    ticker VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    title VARCHAR NOT NULL,
                    description VARCHAR NOT NULL,
                    department VARCHAR,
                    team VARCHAR,
                    employment_type VARCHAR,
                    workplace_type VARCHAR,
                    posted_at TIMESTAMPTZ,
                    source_url VARCHAR NOT NULL,
                    locations_json VARCHAR NOT NULL,
                    business_line_id VARCHAR,
                    business_line_name VARCHAR,
                    job_function VARCHAR,
                    seniority VARCHAR,
                    themes_json VARCHAR NOT NULL,
                    classification_method VARCHAR NOT NULL,
                    classification_confidence DOUBLE NOT NULL,
                    classification_version INTEGER NOT NULL,
                    status VARCHAR NOT NULL,
                    first_seen_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ NOT NULL,
                    closed_at TIMESTAMPTZ,
                    missing_snapshot_count INTEGER NOT NULL,
                    source VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    revision_number INTEGER NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    PRIMARY KEY (source_id, source_job_id)
                );

                CREATE TABLE IF NOT EXISTS job_posting_revisions (
                    source_id VARCHAR NOT NULL,
                    source_job_id VARCHAR NOT NULL,
                    ticker VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    title VARCHAR NOT NULL,
                    description VARCHAR NOT NULL,
                    department VARCHAR,
                    team VARCHAR,
                    employment_type VARCHAR,
                    workplace_type VARCHAR,
                    posted_at TIMESTAMPTZ,
                    source_url VARCHAR NOT NULL,
                    locations_json VARCHAR NOT NULL,
                    business_line_id VARCHAR,
                    business_line_name VARCHAR,
                    job_function VARCHAR,
                    seniority VARCHAR,
                    themes_json VARCHAR NOT NULL,
                    classification_method VARCHAR NOT NULL,
                    classification_confidence DOUBLE NOT NULL,
                    classification_version INTEGER NOT NULL,
                    status VARCHAR NOT NULL,
                    first_seen_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ NOT NULL,
                    closed_at TIMESTAMPTZ,
                    missing_snapshot_count INTEGER NOT NULL,
                    source VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    revision_number INTEGER NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    valid_from TIMESTAMPTZ NOT NULL,
                    valid_until TIMESTAMPTZ,
                    PRIMARY KEY (source_id, source_job_id, revision_number)
                );

                CREATE TABLE IF NOT EXISTS job_posting_locations (
                    source_id VARCHAR NOT NULL,
                    source_job_id VARCHAR NOT NULL,
                    location_id VARCHAR NOT NULL,
                    raw_location VARCHAR NOT NULL,
                    city VARCHAR,
                    region VARCHAR,
                    country VARCHAR,
                    country_code VARCHAR,
                    facility_entity_id VARCHAR,
                    is_remote BOOLEAN NOT NULL,
                    match_confidence DOUBLE,
                    PRIMARY KEY (source_id, source_job_id, location_id)
                );

                CREATE TABLE IF NOT EXISTS job_events (
                    event_id VARCHAR PRIMARY KEY,
                    source_id VARCHAR NOT NULL,
                    source_job_id VARCHAR NOT NULL,
                    ticker VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    event_type VARCHAR NOT NULL,
                    observed_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    revision_number INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS labor_market_observations (
                    period_start DATE NOT NULL,
                    geography_level VARCHAR NOT NULL,
                    geography_code VARCHAR NOT NULL,
                    geography_name VARCHAR NOT NULL,
                    industry_code VARCHAR NOT NULL,
                    industry_name VARCHAR NOT NULL,
                    metric VARCHAR NOT NULL,
                    value DECIMAL(38, 4) NOT NULL,
                    unit VARCHAR NOT NULL,
                    seasonally_adjusted BOOLEAN NOT NULL,
                    source VARCHAR NOT NULL,
                    source_url VARCHAR NOT NULL,
                    source_updated_at TIMESTAMPTZ,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    revision_number INTEGER NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    PRIMARY KEY (
                        period_start, geography_level, geography_code,
                        industry_code, metric, source
                    )
                );

                CREATE TABLE IF NOT EXISTS labor_market_observation_revisions (
                    period_start DATE NOT NULL,
                    geography_level VARCHAR NOT NULL,
                    geography_code VARCHAR NOT NULL,
                    geography_name VARCHAR NOT NULL,
                    industry_code VARCHAR NOT NULL,
                    industry_name VARCHAR NOT NULL,
                    metric VARCHAR NOT NULL,
                    value DECIMAL(38, 4) NOT NULL,
                    unit VARCHAR NOT NULL,
                    seasonally_adjusted BOOLEAN NOT NULL,
                    source VARCHAR NOT NULL,
                    source_url VARCHAR NOT NULL,
                    source_updated_at TIMESTAMPTZ,
                    ingested_at TIMESTAMPTZ NOT NULL,
                    run_id VARCHAR NOT NULL,
                    publication_at TIMESTAMPTZ NOT NULL,
                    available_at TIMESTAMPTZ NOT NULL,
                    revision_number INTEGER NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    valid_from TIMESTAMPTZ NOT NULL,
                    valid_until TIMESTAMPTZ,
                    PRIMARY KEY (
                        period_start, geography_level, geography_code,
                        industry_code, metric, source, revision_number
                    )
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
                ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS period_end DATE;
                ALTER TABLE ingestion_runs
                    ADD COLUMN IF NOT EXISTS records_rejected INTEGER DEFAULT 0;
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
            self._migrate_raw_payload_links(connection)
            connection.execute(
                """
                UPDATE job_events AS events
                SET event_type = 'baseline'
                FROM (
                    SELECT source_id, MIN(observed_at) AS baseline_at
                    FROM job_events
                    GROUP BY source_id
                ) AS source_baselines
                WHERE events.source_id = source_baselines.source_id
                  AND events.observed_at = source_baselines.baseline_at
                  AND events.event_type = 'opened'
                """
            )

    @classmethod
    def _migrate_raw_payload_links(cls, connection: duckdb.DuckDBPyConnection) -> None:
        """Include URL identity in payload links without losing existing provenance."""
        columns = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'raw_payload_links'
                """
            ).fetchall()
        }
        if "source_url_key" in columns:
            return
        with cls._transaction(connection):
            connection.execute(
                """
                CREATE TABLE raw_payload_links_migrated (
                    run_id VARCHAR NOT NULL,
                    payload_sha256 VARCHAR NOT NULL,
                    resource_type VARCHAR NOT NULL,
                    source_url VARCHAR,
                    source_url_key VARCHAR NOT NULL,
                    retrieved_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (
                        run_id, payload_sha256, resource_type, source_url_key
                    )
                )
                """
            )
            connection.execute(
                """
                INSERT INTO raw_payload_links_migrated
                SELECT run_id, payload_sha256, resource_type, source_url,
                       COALESCE(source_url, ''), retrieved_at
                FROM raw_payload_links
                """
            )
            connection.execute("DROP TABLE raw_payload_links")
            connection.execute("ALTER TABLE raw_payload_links_migrated RENAME TO raw_payload_links")

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
        period_end: date | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_runs (
                    run_id, source, status, started_at,
                    period_start, port_code, commodity_code, country_code,
                    entity_id, ticker, period_end
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    period_end,
                ],
            )

    def finish_run(
        self,
        run_id: str,
        *,
        status: IngestionStatus,
        records_received: int,
        records_written: int,
        records_rejected: int = 0,
        error_message: str | None = None,
    ) -> datetime:
        completed_at = datetime.now(UTC)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE ingestion_runs
                SET status = ?, records_received = ?, records_written = ?,
                    completed_at = ?, error_message = ?, records_rejected = ?
                WHERE run_id = ?
                """,
                [
                    status.value,
                    records_received,
                    records_written,
                    completed_at,
                    error_message,
                    records_rejected,
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
        return self.store_raw_payload_batch(
            run_id,
            source,
            [(content, resource_type, source_url)],
        )[0]

    def store_raw_payload_batch(
        self,
        run_id: str,
        source: SourceName,
        payloads: Sequence[tuple[bytes, str | None, str | None]],
    ) -> list[str]:
        """Archive many resources in one transaction while retaining URL provenance."""
        if not payloads:
            return []

        retrieved_at = datetime.now(UTC)
        payload_hashes = [hashlib.sha256(content).hexdigest() for content, _, _ in payloads]
        raw_rows = [
            (payload_hash, run_id, source.value, retrieved_at, content)
            for payload_hash, (content, _, _) in zip(payload_hashes, payloads, strict=True)
        ]
        link_rows = [
            (
                run_id,
                payload_hash,
                resource_type or source.value,
                source_url,
                source_url or "",
                retrieved_at,
            )
            for payload_hash, (_, resource_type, source_url) in zip(
                payload_hashes, payloads, strict=True
            )
        ]
        with self._connect() as connection, self._transaction(connection):
            connection.executemany(
                """
                INSERT OR IGNORE INTO raw_payloads
                    (payload_sha256, run_id, source, retrieved_at, content)
                VALUES (?, ?, ?, ?, ?)
                """,
                raw_rows,
            )
            connection.executemany(
                """
                INSERT OR IGNORE INTO raw_payload_links
                    (run_id, payload_sha256, resource_type, source_url,
                     source_url_key, retrieved_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                link_rows,
            )
        return payload_hashes

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
                    str(existing_row[0]): tuple(existing_row[1:]) for existing_row in existing_rows
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

    def upsert_federal_contract_awards(
        self,
        run_id: str,
        awards: list[FederalContractAward],
        *,
        payload_sha256_by_award: dict[str, str],
        transactions: list[FederalContractTransaction] | None = None,
        payload_sha256_by_transaction: dict[str, str] | None = None,
    ) -> int:
        """Atomically store award inventory and signed transaction-flow vintages."""
        transactions = transactions or []
        payload_sha256_by_transaction = payload_sha256_by_transaction or {}
        missing_hashes = {
            award.award_key for award in awards if award.award_key not in payload_sha256_by_award
        }
        if missing_hashes:
            raise ValueError(f"missing raw payload hashes for awards: {sorted(missing_hashes)}")
        missing_transaction_hashes = {
            transaction.transaction_id
            for transaction in transactions
            if transaction.transaction_id not in payload_sha256_by_transaction
        }
        if missing_transaction_hashes:
            raise ValueError(
                "missing raw payload hashes for contract transactions: "
                f"{sorted(missing_transaction_hashes)}"
            )
        if transactions and not awards:
            raise ValueError("contract transactions require their parent award snapshots")
        awards_by_key = {award.award_key: award for award in awards}
        for transaction in transactions:
            parent = awards_by_key.get(transaction.award_key)
            if parent is None:
                raise ValueError(
                    "contract transaction references an award outside the atomic batch: "
                    f"{transaction.transaction_id} -> {transaction.award_key}"
                )
            if (
                transaction.entity_id,
                transaction.ticker,
                transaction.award_id,
            ) != (parent.entity_id, parent.ticker, parent.award_id):
                raise ValueError(
                    "contract transaction attribution does not match its parent award: "
                    f"{transaction.transaction_id}"
                )

        if not awards:
            return 0

        current_columns = (
            "award_key",
            "entity_id",
            "ticker",
            "award_id",
            "recipient_name",
            "recipient_uei",
            "recipient_id",
            "match_method",
            "award_type",
            "description",
            "award_amount_usd",
            "total_outlays_usd",
            "base_obligation_date",
            "start_date",
            "end_date",
            "source_modified_at",
            "awarding_agency",
            "awarding_sub_agency",
            "funding_agency",
            "funding_sub_agency",
            "naics_code",
            "naics_description",
            "psc_code",
            "psc_description",
            "place_of_performance_country",
            "place_of_performance_state",
            "source_url",
            "source",
            "ingested_at",
            "run_id",
            "publication_at",
            "available_at",
            "revision_number",
            "payload_sha256",
        )
        transaction_changes = 0
        with self._connect() as connection, self._transaction(connection):
            tickers = sorted({award.ticker for award in awards})
            ticker_placeholders = ", ".join("?" for _ in tickers)
            existing_rows = connection.execute(
                f"""
                SELECT award_key, source, revision_number, entity_id, ticker, award_id,
                       recipient_name, recipient_uei, recipient_id, match_method,
                       award_type, description, award_amount_usd, total_outlays_usd,
                       base_obligation_date, start_date, end_date, source_modified_at,
                       awarding_agency, awarding_sub_agency, funding_agency,
                       funding_sub_agency, naics_code, naics_description, psc_code,
                       psc_description, place_of_performance_country,
                       place_of_performance_state, source_url
                FROM federal_contract_awards
                WHERE ticker IN ({ticker_placeholders})
                """,
                tickers,
            ).fetchall()
            existing_by_key = {
                (str(existing[0]), str(existing[1])): tuple(existing[2:])
                for existing in existing_rows
            }
            rows: list[tuple[Any, ...]] = []
            revision_rows: list[tuple[Any, ...]] = []
            revisions_to_close: list[tuple[datetime, str, str]] = []
            for award in awards:
                existing = existing_by_key.get((award.award_key, award.source.value))
                candidate = (
                    award.entity_id,
                    award.ticker,
                    award.award_id,
                    award.recipient_name,
                    award.recipient_uei,
                    award.recipient_id,
                    award.match_method.value,
                    award.award_type,
                    award.description,
                    award.award_amount_usd,
                    award.total_outlays_usd,
                    award.base_obligation_date,
                    award.start_date,
                    award.end_date,
                    award.source_modified_at,
                    award.awarding_agency,
                    award.awarding_sub_agency,
                    award.funding_agency,
                    award.funding_sub_agency,
                    award.naics_code,
                    award.naics_description,
                    award.psc_code,
                    award.psc_description,
                    award.place_of_performance_country,
                    award.place_of_performance_state,
                    award.source_url,
                )
                if existing is not None and tuple(existing[1:]) == candidate:
                    continue

                revision_number = 1 if existing is None else int(existing[0]) + 1
                if existing is not None:
                    revisions_to_close.append(
                        (award.ingested_at, award.award_key, award.source.value)
                    )
                publication_at = award.source_modified_at or award.ingested_at
                row = (
                    award.award_key,
                    *candidate,
                    award.source.value,
                    award.ingested_at,
                    run_id,
                    publication_at,
                    award.ingested_at,
                    revision_number,
                    payload_sha256_by_award[award.award_key],
                )
                rows.append(row)
                revision_rows.append((*row, award.ingested_at, None))

            if revisions_to_close:
                closing_frame = pd.DataFrame.from_records(
                    revisions_to_close,
                    columns=("valid_until", "award_key", "source"),
                )
                connection.register("_contract_revisions_to_close", closing_frame)
                try:
                    connection.execute(
                        """
                        UPDATE federal_contract_award_revisions AS revisions
                        SET valid_until = changes.valid_until
                        FROM _contract_revisions_to_close AS changes
                        WHERE revisions.award_key = changes.award_key
                          AND revisions.source = changes.source
                          AND revisions.valid_until IS NULL
                        """
                    )
                finally:
                    connection.unregister("_contract_revisions_to_close")
            if rows:
                current_frame = pd.DataFrame.from_records(rows, columns=current_columns)
                revision_frame = pd.DataFrame.from_records(
                    revision_rows,
                    columns=(*current_columns, "valid_from", "valid_until"),
                )
                connection.register("_contract_award_batch", current_frame)
                connection.register("_contract_award_revision_batch", revision_frame)
                try:
                    connection.execute(
                        "INSERT OR REPLACE INTO federal_contract_awards "
                        "SELECT * FROM _contract_award_batch"
                    )
                    connection.execute(
                        "INSERT INTO federal_contract_award_revisions "
                        "SELECT * FROM _contract_award_revision_batch"
                    )
                finally:
                    connection.unregister("_contract_award_revision_batch")
                    connection.unregister("_contract_award_batch")
            transaction_changes = self._upsert_federal_contract_transactions(
                connection,
                run_id,
                transactions,
                payload_sha256_by_transaction=payload_sha256_by_transaction,
            )
        return len(rows) + transaction_changes

    @staticmethod
    def _upsert_federal_contract_transactions(
        connection: duckdb.DuckDBPyConnection,
        run_id: str,
        transactions: list[FederalContractTransaction],
        *,
        payload_sha256_by_transaction: dict[str, str],
    ) -> int:
        if not transactions:
            return 0
        current_columns = (
            "transaction_id",
            "award_key",
            "entity_id",
            "ticker",
            "award_id",
            "action_date",
            "federal_action_obligation_usd",
            "action_type",
            "action_type_description",
            "modification_number",
            "description",
            "award_type_code",
            "award_type_description",
            "source_url",
            "source",
            "ingested_at",
            "run_id",
            "publication_at",
            "available_at",
            "revision_number",
            "payload_sha256",
        )
        tickers = sorted({transaction.ticker for transaction in transactions})
        ticker_placeholders = ", ".join("?" for _ in tickers)
        existing_rows = connection.execute(
            f"""
            SELECT transaction_id, source, revision_number, award_key, entity_id, ticker,
                   award_id, action_date, federal_action_obligation_usd, action_type,
                   action_type_description, modification_number, description,
                   award_type_code, award_type_description, source_url
            FROM federal_contract_transactions
            WHERE ticker IN ({ticker_placeholders})
            """,
            tickers,
        ).fetchall()
        existing_by_key = {
            (str(existing[0]), str(existing[1])): tuple(existing[2:]) for existing in existing_rows
        }
        rows: list[tuple[Any, ...]] = []
        revision_rows: list[tuple[Any, ...]] = []
        revisions_to_close: list[tuple[datetime, str, str]] = []
        for transaction in transactions:
            existing = existing_by_key.get((transaction.transaction_id, transaction.source.value))
            candidate = (
                transaction.award_key,
                transaction.entity_id,
                transaction.ticker,
                transaction.award_id,
                transaction.action_date,
                transaction.federal_action_obligation_usd,
                transaction.action_type,
                transaction.action_type_description,
                transaction.modification_number,
                transaction.description,
                transaction.award_type_code,
                transaction.award_type_description,
                transaction.source_url,
            )
            if existing is not None and tuple(existing[1:]) == candidate:
                continue
            revision_number = 1 if existing is None else int(existing[0]) + 1
            if existing is not None:
                revisions_to_close.append(
                    (
                        transaction.ingested_at,
                        transaction.transaction_id,
                        transaction.source.value,
                    )
                )
            row = (
                transaction.transaction_id,
                *candidate,
                transaction.source.value,
                transaction.ingested_at,
                run_id,
                transaction.ingested_at,
                transaction.ingested_at,
                revision_number,
                payload_sha256_by_transaction[transaction.transaction_id],
            )
            rows.append(row)
            revision_rows.append((*row, transaction.ingested_at, None))

        if revisions_to_close:
            closing_frame = pd.DataFrame.from_records(
                revisions_to_close,
                columns=("valid_until", "transaction_id", "source"),
            )
            connection.register("_contract_transaction_revisions_to_close", closing_frame)
            try:
                connection.execute(
                    """
                    UPDATE federal_contract_transaction_revisions AS revisions
                    SET valid_until = changes.valid_until
                    FROM _contract_transaction_revisions_to_close AS changes
                    WHERE revisions.transaction_id = changes.transaction_id
                      AND revisions.source = changes.source
                      AND revisions.valid_until IS NULL
                    """
                )
            finally:
                connection.unregister("_contract_transaction_revisions_to_close")
        if rows:
            current_frame = pd.DataFrame.from_records(rows, columns=current_columns)
            revision_frame = pd.DataFrame.from_records(
                revision_rows,
                columns=(*current_columns, "valid_from", "valid_until"),
            )
            connection.register("_contract_transaction_batch", current_frame)
            connection.register("_contract_transaction_revision_batch", revision_frame)
            try:
                connection.execute(
                    "INSERT OR REPLACE INTO federal_contract_transactions "
                    "SELECT * FROM _contract_transaction_batch"
                )
                connection.execute(
                    "INSERT INTO federal_contract_transaction_revisions "
                    "SELECT * FROM _contract_transaction_revision_batch"
                )
            finally:
                connection.unregister("_contract_transaction_revision_batch")
                connection.unregister("_contract_transaction_batch")
        return len(rows)

    def apply_job_snapshot(
        self,
        run_id: str,
        *,
        source_id: str,
        observed_at: datetime,
        postings: list[JobPosting],
        payload_sha256_by_job: dict[str, str],
        missing_snapshots_before_close: int,
    ) -> int:
        """Atomically diff a complete career-site snapshot into lifecycle events."""
        if missing_snapshots_before_close < 1:
            raise ValueError("missing_snapshots_before_close must be positive")
        if any(posting.source_id != source_id for posting in postings):
            raise ValueError("all postings in a snapshot must share its source_id")
        missing_hashes = {
            posting.source_job_id
            for posting in postings
            if posting.source_job_id not in payload_sha256_by_job
        }
        if missing_hashes:
            raise ValueError(f"missing raw payload hashes for jobs: {sorted(missing_hashes)}")

        current_columns = (
            "source_id",
            "source_job_id",
            "ticker",
            "entity_id",
            "title",
            "description",
            "department",
            "team",
            "employment_type",
            "workplace_type",
            "posted_at",
            "source_url",
            "locations_json",
            "business_line_id",
            "business_line_name",
            "job_function",
            "seniority",
            "themes_json",
            "classification_method",
            "classification_confidence",
            "classification_version",
            "status",
            "first_seen_at",
            "last_seen_at",
            "closed_at",
            "missing_snapshot_count",
            "source",
            "ingested_at",
            "run_id",
            "available_at",
            "revision_number",
            "payload_sha256",
        )
        with self._connect() as connection, self._transaction(connection):
            existing_rows = connection.execute(
                "SELECT * FROM job_postings WHERE source_id = ?",
                [source_id],
            ).fetchall()
            existing_by_id = {
                str(row[1]): dict(zip(current_columns, row, strict=True)) for row in existing_rows
            }
            is_initial_snapshot = not existing_rows
            incoming_by_id = {posting.source_job_id: posting for posting in postings}
            if len(incoming_by_id) != len(postings):
                raise ValueError("job snapshot contains duplicate source_job_id values")

            final_rows: list[tuple[Any, ...]] = []
            revision_rows: list[tuple[Any, ...]] = []
            revisions_to_close: list[tuple[datetime, str, str]] = []
            event_rows: list[tuple[Any, ...]] = []

            for job_id, posting in incoming_by_id.items():
                existing = existing_by_id.get(job_id)
                content = self._job_content(posting)
                event_type: JobEventType | None = None
                if existing is None:
                    revision_number = 1
                    first_seen_at = observed_at
                    event_type = (
                        JobEventType.BASELINE if is_initial_snapshot else JobEventType.OPENED
                    )
                else:
                    revision_number = int(existing["revision_number"])
                    first_seen_at = existing["first_seen_at"]
                    existing_content = tuple(existing[column] for column in current_columns[2:21])
                    if existing["status"] == JobStatus.CLOSED.value:
                        revision_number += 1
                        event_type = JobEventType.REOPENED
                    elif existing_content != content:
                        revision_number += 1
                        event_type = JobEventType.UPDATED

                row = (
                    source_id,
                    job_id,
                    *content,
                    JobStatus.ACTIVE.value,
                    first_seen_at,
                    observed_at,
                    None,
                    0,
                    posting.source.value,
                    posting.ingested_at,
                    run_id,
                    observed_at,
                    revision_number,
                    payload_sha256_by_job[job_id],
                )
                final_rows.append(row)
                if event_type is not None:
                    if existing is not None:
                        revisions_to_close.append((observed_at, source_id, job_id))
                    revision_rows.append((*row, observed_at, None))
                    event_rows.append(
                        self._job_event_row(
                            source_id=source_id,
                            source_job_id=job_id,
                            ticker=posting.ticker,
                            entity_id=posting.entity_id,
                            event_type=event_type,
                            observed_at=observed_at,
                            run_id=run_id,
                            revision_number=revision_number,
                        )
                    )

            for job_id, existing in existing_by_id.items():
                if job_id in incoming_by_id:
                    continue
                row_values = [existing[column] for column in current_columns]
                if existing["status"] == JobStatus.ACTIVE.value:
                    missing_count = int(existing["missing_snapshot_count"]) + 1
                    row_values[25] = missing_count
                    row_values[27] = observed_at
                    row_values[28] = run_id
                    if missing_count >= missing_snapshots_before_close:
                        revision_number = int(existing["revision_number"]) + 1
                        row_values[21] = JobStatus.CLOSED.value
                        row_values[24] = observed_at
                        row_values[29] = observed_at
                        row_values[30] = revision_number
                        revisions_to_close.append((observed_at, source_id, job_id))
                        revision_rows.append((*row_values, observed_at, None))
                        event_rows.append(
                            self._job_event_row(
                                source_id=source_id,
                                source_job_id=job_id,
                                ticker=str(existing["ticker"]),
                                entity_id=str(existing["entity_id"]),
                                event_type=JobEventType.CLOSED,
                                observed_at=observed_at,
                                run_id=run_id,
                                revision_number=revision_number,
                            )
                        )
                final_rows.append(tuple(row_values))

            if revisions_to_close:
                connection.executemany(
                    """
                    UPDATE job_posting_revisions SET valid_until = ?
                    WHERE source_id = ? AND source_job_id = ? AND valid_until IS NULL
                    """,
                    revisions_to_close,
                )
            connection.execute("DELETE FROM job_posting_locations WHERE source_id = ?", [source_id])
            connection.execute("DELETE FROM job_postings WHERE source_id = ?", [source_id])
            if final_rows:
                placeholders = ", ".join("?" for _ in current_columns)
                connection.executemany(
                    f"INSERT INTO job_postings VALUES ({placeholders})",
                    final_rows,
                )
                location_rows = self._job_location_rows(final_rows)
                if location_rows:
                    connection.executemany(
                        "INSERT INTO job_posting_locations VALUES "
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        location_rows,
                    )
            if revision_rows:
                placeholders = ", ".join("?" for _ in range(len(current_columns) + 2))
                connection.executemany(
                    f"INSERT INTO job_posting_revisions VALUES ({placeholders})",
                    revision_rows,
                )
                connection.executemany(
                    "INSERT INTO job_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    event_rows,
                )
        return len(event_rows)

    @staticmethod
    def _job_content(posting: JobPosting) -> tuple[Any, ...]:
        locations_json = json.dumps(
            [location.model_dump(mode="json") for location in posting.locations],
            sort_keys=True,
            separators=(",", ":"),
        )
        themes_json = json.dumps(sorted(posting.themes), separators=(",", ":"))
        return (
            posting.ticker,
            posting.entity_id,
            posting.title,
            posting.description,
            posting.department,
            posting.team,
            posting.employment_type,
            posting.workplace_type,
            posting.posted_at,
            posting.source_url,
            locations_json,
            posting.business_line_id,
            posting.business_line_name,
            posting.job_function,
            posting.seniority,
            themes_json,
            posting.classification_method.value,
            posting.classification_confidence,
            posting.classification_version,
        )

    @staticmethod
    def _job_event_row(
        *,
        source_id: str,
        source_job_id: str,
        ticker: str,
        entity_id: str,
        event_type: JobEventType,
        observed_at: datetime,
        run_id: str,
        revision_number: int,
    ) -> tuple[Any, ...]:
        identity = (
            f"{source_id}|{source_job_id}|{event_type.value}|"
            f"{observed_at.isoformat()}|{revision_number}"
        )
        event_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        return (
            event_id,
            source_id,
            source_job_id,
            ticker,
            entity_id,
            event_type.value,
            observed_at,
            run_id,
            revision_number,
        )

    @staticmethod
    def _job_location_rows(current_rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        rows: list[tuple[Any, ...]] = []
        for current in current_rows:
            source_id = str(current[0])
            source_job_id = str(current[1])
            locations = json.loads(str(current[12]))
            for location in locations:
                rows.append(
                    (
                        source_id,
                        source_job_id,
                        location["location_id"],
                        location["raw_location"],
                        location.get("city"),
                        location.get("region"),
                        location.get("country"),
                        location.get("country_code"),
                        location.get("facility_entity_id"),
                        bool(location["is_remote"]),
                        location.get("match_confidence"),
                    )
                )
        return rows

    def upsert_labor_market_observations(
        self,
        run_id: str,
        observations: list[LaborMarketObservation],
        *,
        payload_sha256: str,
    ) -> int:
        """Atomically version official labor-market benchmark observations."""
        changed = 0
        with self._connect() as connection, self._transaction(connection):
            for observation in observations:
                key = (
                    observation.period_start,
                    observation.geography_level,
                    observation.geography_code,
                    observation.industry_code,
                    observation.metric.value,
                    observation.source.value,
                )
                existing = connection.execute(
                    """
                    SELECT revision_number, geography_name, industry_name, value, unit,
                           seasonally_adjusted, source_url, source_updated_at
                    FROM labor_market_observations
                    WHERE period_start = ? AND geography_level = ? AND geography_code = ?
                      AND industry_code = ? AND metric = ? AND source = ?
                    """,
                    list(key),
                ).fetchone()
                candidate = (
                    observation.geography_name,
                    observation.industry_name,
                    observation.value,
                    observation.unit,
                    observation.seasonally_adjusted,
                    observation.source_url,
                    observation.source_updated_at,
                )
                if existing is not None and tuple(existing[1:]) == candidate:
                    continue
                revision_number = 1 if existing is None else int(existing[0]) + 1
                if existing is not None:
                    connection.execute(
                        """
                        UPDATE labor_market_observation_revisions SET valid_until = ?
                        WHERE period_start = ? AND geography_level = ? AND geography_code = ?
                          AND industry_code = ? AND metric = ? AND source = ?
                          AND valid_until IS NULL
                        """,
                        [observation.ingested_at, *key],
                    )
                publication_at = observation.source_updated_at or observation.ingested_at
                row = (
                    observation.period_start,
                    observation.geography_level,
                    observation.geography_code,
                    observation.geography_name,
                    observation.industry_code,
                    observation.industry_name,
                    observation.metric.value,
                    observation.value,
                    observation.unit,
                    observation.seasonally_adjusted,
                    observation.source.value,
                    observation.source_url,
                    observation.source_updated_at,
                    observation.ingested_at,
                    run_id,
                    publication_at,
                    observation.ingested_at,
                    revision_number,
                    payload_sha256,
                )
                connection.execute(
                    "INSERT OR REPLACE INTO labor_market_observations VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    row,
                )
                connection.execute(
                    "INSERT INTO labor_market_observation_revisions VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (*row, observation.ingested_at, None),
                )
                changed += 1
        return changed

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

    def federal_contract_awards_summary(
        self,
        *,
        ticker: str | None = None,
    ) -> pd.DataFrame:
        where_clause = "" if ticker is None else "WHERE ticker = ?"
        parameters: list[Any] = [] if ticker is None else [ticker.upper()]
        with self._connect() as connection:
            return connection.execute(
                f"""
                SELECT award_key, entity_id, ticker, award_id, recipient_name,
                       recipient_uei, recipient_id, match_method, award_type, description,
                       CAST(award_amount_usd AS DOUBLE) AS award_amount_usd,
                       CAST(total_outlays_usd AS DOUBLE) AS total_outlays_usd,
                       base_obligation_date, start_date, end_date, source_modified_at,
                       awarding_agency, awarding_sub_agency, funding_agency,
                       funding_sub_agency, naics_code, naics_description, psc_code,
                       psc_description, place_of_performance_country,
                       place_of_performance_state, source_url, publication_at,
                       available_at, revision_number, payload_sha256
                FROM federal_contract_awards
                {where_clause}
                ORDER BY base_obligation_date DESC, award_amount_usd DESC
                """,
                parameters,
            ).fetchdf()

    def federal_contract_award_revisions(self, limit: int = 100) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT award_key, entity_id, ticker, award_id, recipient_name,
                       CAST(award_amount_usd AS DOUBLE) AS award_amount_usd,
                       source_modified_at, revision_number, publication_at, available_at,
                       valid_from, valid_until, payload_sha256
                FROM federal_contract_award_revisions
                ORDER BY valid_from DESC
                LIMIT ?
                """,
                [limit],
            ).fetchdf()

    def federal_contract_transactions_summary(
        self,
        *,
        ticker: str | None = None,
    ) -> pd.DataFrame:
        where_clause = "" if ticker is None else "WHERE ticker = ?"
        parameters: list[Any] = [] if ticker is None else [ticker.upper()]
        with self._connect() as connection:
            return connection.execute(
                f"""
                SELECT transaction_id, award_key, entity_id, ticker, award_id,
                       action_date,
                       CAST(federal_action_obligation_usd AS DOUBLE)
                           AS federal_action_obligation_usd,
                       action_type, action_type_description, modification_number,
                       description, award_type_code, award_type_description, source_url,
                       publication_at, available_at, revision_number, payload_sha256
                FROM federal_contract_transactions
                {where_clause}
                ORDER BY action_date DESC, ABS(federal_action_obligation_usd) DESC
                """,
                parameters,
            ).fetchdf()

    def federal_contract_transaction_revisions(self, limit: int = 100) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT transaction_id, award_key, entity_id, ticker, award_id, action_date,
                       CAST(federal_action_obligation_usd AS DOUBLE)
                           AS federal_action_obligation_usd,
                       modification_number, revision_number, publication_at, available_at,
                       valid_from, valid_until, payload_sha256
                FROM federal_contract_transaction_revisions
                ORDER BY valid_from DESC
                LIMIT ?
                """,
                [limit],
            ).fetchdf()

    def job_postings_summary(
        self,
        *,
        ticker: str | None = None,
        status: JobStatus | None = None,
    ) -> pd.DataFrame:
        filters: list[str] = []
        parameters: list[Any] = []
        if ticker is not None:
            filters.append("ticker = ?")
            parameters.append(ticker.upper())
        if status is not None:
            filters.append("status = ?")
            parameters.append(status.value)
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        with self._connect() as connection:
            return connection.execute(
                f"""
                SELECT source_id, source_job_id, ticker, entity_id, title, department,
                       team, employment_type, workplace_type, posted_at, source_url,
                       business_line_id, business_line_name, job_function, seniority,
                       themes_json, classification_method, classification_confidence,
                       status, first_seen_at, last_seen_at, closed_at,
                       missing_snapshot_count, revision_number, payload_sha256
                FROM job_postings
                {where_clause}
                ORDER BY status, first_seen_at DESC, title
                """,
                parameters,
            ).fetchdf()

    def job_enrichment_cache(self, source_id: str) -> dict[str, dict[str, Any]]:
        """Return reusable detail fields so unchanged jobs are not fetched every day."""
        detail_resource_type = f"career_detail:{source_id}"
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT postings.source_job_id, postings.title, postings.description,
                       postings.department, postings.employment_type, postings.posted_at,
                       MAX(payload_links.retrieved_at) AS detail_retrieved_at
                FROM job_postings AS postings
                LEFT JOIN raw_payload_links AS payload_links
                  ON payload_links.source_url = postings.source_url
                 AND payload_links.resource_type = ?
                WHERE postings.source_id = ?
                  AND postings.status = ?
                  AND postings.description <> ''
                GROUP BY postings.source_job_id, postings.title, postings.description,
                         postings.department, postings.employment_type, postings.posted_at
                """,
                [detail_resource_type, source_id, JobStatus.ACTIVE.value],
            ).fetchall()
        return {
            str(row[0]): {
                "title": row[1],
                "description": row[2],
                "department": row[3],
                "employment_type": row[4],
                "posted_at": row[5],
                "detail_retrieved_at": row[6],
            }
            for row in rows
        }

    def job_locations_summary(self, *, ticker: str | None = None) -> pd.DataFrame:
        where_clause = "" if ticker is None else "WHERE postings.ticker = ?"
        parameters: list[Any] = [] if ticker is None else [ticker.upper()]
        with self._connect() as connection:
            return connection.execute(
                f"""
                SELECT postings.ticker, postings.status, locations.source_id,
                       locations.source_job_id, locations.location_id,
                       locations.raw_location, locations.city, locations.region,
                       locations.country, locations.country_code,
                       locations.facility_entity_id, locations.is_remote,
                       locations.match_confidence
                FROM job_posting_locations AS locations
                JOIN job_postings AS postings USING (source_id, source_job_id)
                {where_clause}
                ORDER BY postings.ticker, locations.country, locations.region, locations.city
                """,
                parameters,
            ).fetchdf()

    def job_events_summary(
        self,
        *,
        ticker: str | None = None,
        limit: int | None = 5_000,
    ) -> pd.DataFrame:
        where_clause = "" if ticker is None else "WHERE ticker = ?"
        parameters: list[Any] = [] if ticker is None else [ticker.upper()]
        limit_clause = ""
        if limit is not None:
            if limit < 1:
                raise ValueError("job event limit must be positive")
            limit_clause = "LIMIT ?"
            parameters.append(limit)
        with self._connect() as connection:
            return connection.execute(
                f"""
                SELECT event_id, source_id, source_job_id, ticker, entity_id,
                       event_type, observed_at, run_id, revision_number
                FROM job_events
                {where_clause}
                ORDER BY observed_at DESC
                {limit_clause}
                """,
                parameters,
            ).fetchdf()

    def job_posting_revisions(self, limit: int = 100) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT source_id, source_job_id, ticker, title, status,
                       business_line_name, job_function, revision_number,
                       available_at, valid_from, valid_until, payload_sha256
                FROM job_posting_revisions
                ORDER BY valid_from DESC
                LIMIT ?
                """,
                [limit],
            ).fetchdf()

    def labor_market_summary(self) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(
                """
                SELECT period_start, geography_level, geography_code, geography_name,
                       industry_code, industry_name, metric,
                       CAST(value AS DOUBLE) AS value, unit, seasonally_adjusted,
                       source_url, publication_at, available_at, revision_number
                FROM labor_market_observations
                ORDER BY period_start, geography_level, geography_code, industry_code, metric
                """
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
                    (SELECT COUNT(DISTINCT tag) FROM sec_company_facts),
                    (SELECT COUNT(*) FROM federal_contract_awards),
                    (SELECT COUNT(DISTINCT ticker) FROM federal_contract_awards),
                    (SELECT COUNT(*) FROM federal_contract_transactions),
                    (SELECT COUNT(*) FROM job_postings),
                    (SELECT COUNT(*) FROM job_postings WHERE status = 'active'),
                    (SELECT COUNT(DISTINCT ticker) FROM job_postings),
                    (SELECT COUNT(*) FROM labor_market_observations)
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
            "federal_contract_awards",
            "contract_companies",
            "federal_contract_transactions",
            "job_postings",
            "active_job_postings",
            "hiring_companies",
            "labor_market_observations",
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
