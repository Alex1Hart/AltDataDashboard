from __future__ import annotations

from dataclasses import dataclass

from portwatch.models import IngestionStatus, SourceName
from portwatch.storage.duckdb import DuckDBRepository


@dataclass(frozen=True)
class HiringStateAudit:
    active_postings: int
    baseline_events: int
    duplicate_postings: int
    orphan_locations: int
    multiple_open_revisions: int
    latest_run_status: str | None

    @property
    def healthy(self) -> bool:
        return (
            self.active_postings > 0
            and self.baseline_events > 0
            and self.duplicate_postings == 0
            and self.orphan_locations == 0
            and self.multiple_open_revisions == 0
            and self.latest_run_status == IngestionStatus.SUCCEEDED.value
        )


@dataclass(frozen=True)
class ContractStateAudit:
    awards: int
    transactions: int
    awards_with_transactions: int
    orphan_transactions: int
    duplicate_transactions: int
    multiple_open_award_revisions: int
    multiple_open_transaction_revisions: int
    latest_run_status: str | None

    @property
    def transaction_coverage(self) -> float:
        return self.awards_with_transactions / self.awards if self.awards else 0.0

    @property
    def healthy(self) -> bool:
        return (
            self.awards > 0
            and self.transactions > 0
            and self.orphan_transactions == 0
            and self.duplicate_transactions == 0
            and self.multiple_open_award_revisions == 0
            and self.multiple_open_transaction_revisions == 0
            and self.latest_run_status == IngestionStatus.SUCCEEDED.value
        )


def audit_hiring_state(repository: DuckDBRepository) -> HiringStateAudit:
    """Check the invariants required before publishing a scheduled HiringWatch state."""
    return HiringStateAudit(
        active_postings=_scalar_int(
            repository,
            "SELECT COUNT(*) FROM job_postings WHERE status = 'active'",
        ),
        baseline_events=_scalar_int(
            repository,
            "SELECT COUNT(*) FROM job_events WHERE event_type = 'baseline'",
        ),
        duplicate_postings=_scalar_int(
            repository,
            """
            SELECT COUNT(*) FROM (
                SELECT source_id, source_job_id
                FROM job_postings
                GROUP BY source_id, source_job_id
                HAVING COUNT(*) > 1
            )
            """,
        ),
        orphan_locations=_scalar_int(
            repository,
            """
            SELECT COUNT(*)
            FROM job_posting_locations AS locations
            LEFT JOIN job_postings AS postings USING (source_id, source_job_id)
            WHERE postings.source_job_id IS NULL
            """,
        ),
        multiple_open_revisions=_scalar_int(
            repository,
            """
            SELECT COUNT(*) FROM (
                SELECT source_id, source_job_id
                FROM job_posting_revisions
                WHERE valid_until IS NULL
                GROUP BY source_id, source_job_id
                HAVING COUNT(*) > 1
            )
            """,
        ),
        latest_run_status=repository.execute_scalar(
            """
            SELECT status
            FROM ingestion_runs
            WHERE source = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            [SourceName.COMPANY_CAREERS.value],
        ),
    )


def audit_contract_state(repository: DuckDBRepository) -> ContractStateAudit:
    """Check ContractWatch referential, uniqueness, revision, and run invariants."""
    return ContractStateAudit(
        awards=_scalar_int(repository, "SELECT COUNT(*) FROM federal_contract_awards"),
        transactions=_scalar_int(
            repository,
            "SELECT COUNT(*) FROM federal_contract_transactions",
        ),
        awards_with_transactions=_scalar_int(
            repository,
            "SELECT COUNT(DISTINCT award_key) FROM federal_contract_transactions",
        ),
        orphan_transactions=_scalar_int(
            repository,
            """
            SELECT COUNT(*)
            FROM federal_contract_transactions AS transactions
            LEFT JOIN federal_contract_awards AS awards USING (award_key)
            WHERE awards.award_key IS NULL
            """,
        ),
        duplicate_transactions=_scalar_int(
            repository,
            """
            SELECT COUNT(*) FROM (
                SELECT transaction_id, source
                FROM federal_contract_transactions
                GROUP BY transaction_id, source
                HAVING COUNT(*) > 1
            )
            """,
        ),
        multiple_open_award_revisions=_scalar_int(
            repository,
            """
            SELECT COUNT(*) FROM (
                SELECT award_key, source
                FROM federal_contract_award_revisions
                WHERE valid_until IS NULL
                GROUP BY award_key, source
                HAVING COUNT(*) > 1
            )
            """,
        ),
        multiple_open_transaction_revisions=_scalar_int(
            repository,
            """
            SELECT COUNT(*) FROM (
                SELECT transaction_id, source
                FROM federal_contract_transaction_revisions
                WHERE valid_until IS NULL
                GROUP BY transaction_id, source
                HAVING COUNT(*) > 1
            )
            """,
        ),
        latest_run_status=repository.execute_scalar(
            """
            SELECT status
            FROM ingestion_runs
            WHERE source = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            [SourceName.USA_SPENDING_CONTRACT_AWARDS.value],
        ),
    )


def _scalar_int(repository: DuckDBRepository, query: str) -> int:
    return int(repository.execute_scalar(query) or 0)
