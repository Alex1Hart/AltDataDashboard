from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SourceName(StrEnum):
    CENSUS_PORT_HS = "census_port_hs"
    PORT_OF_LA_CONTAINER_STATS = "port_of_la_container_stats"
    SEC_EDGAR = "sec_edgar"
    USA_SPENDING_CONTRACT_AWARDS = "usaspending_contract_awards"


class IngestionStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class PortMetricName(StrEnum):
    LOADED_IMPORT_TEU = "loaded_import_teu"
    LOADED_EXPORT_TEU = "loaded_export_teu"
    TOTAL_LOADED_TEU = "total_loaded_teu"
    TOTAL_EMPTY_TEU = "total_empty_teu"
    TOTAL_TEU = "total_teu"


class EvidenceType(StrEnum):
    SEC_FILING = "sec_filing"
    COMPANY_DISCLOSURE = "company_disclosure"
    ANALYST_NOTE = "analyst_note"


class ConfidenceLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class RegistryEntityType(StrEnum):
    ISSUER = "issuer"
    SUBSIDIARY = "subsidiary"
    FACILITY = "facility"


class EntityIdentifierScheme(StrEnum):
    TICKER = "ticker"
    SEC_CIK = "sec_cik"
    LEI = "lei"
    UEI = "uei"
    USA_SPENDING_RECIPIENT_ID = "usaspending_recipient_id"
    EPA_FRS = "epa_frs"
    OSHA_ESTABLISHMENT = "osha_establishment"


class ContractMatchMethod(StrEnum):
    UEI = "uei"
    RECIPIENT_ID = "recipient_id"
    REVIEWED_NAME = "reviewed_name"


class EntityRelationshipType(StrEnum):
    OWNS = "owns"
    OPERATES = "operates"


class TradeFlow(BaseModel):
    """Normalized monthly import observation at port, commodity, and country grain."""

    model_config = ConfigDict(frozen=True)

    month: date
    port_code: str = Field(pattern=r"^\d{4}$")
    port_name: str = Field(min_length=1)
    commodity_code: str = Field(pattern=r"^\d{2,6}$")
    commodity_description: str = Field(min_length=1)
    country_code: str = Field(pattern=r"^\d{4}$")
    country_name: str = Field(min_length=1)
    general_value_usd: Decimal = Field(ge=0)
    vessel_value_usd: Decimal = Field(ge=0)
    vessel_weight_kg: Decimal = Field(ge=0)
    containerized_value_usd: Decimal = Field(ge=0)
    containerized_weight_kg: Decimal = Field(ge=0)
    source: SourceName = SourceName.CENSUS_PORT_HS
    source_updated_at: datetime | None = None
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("month")
    @classmethod
    def month_must_be_first_day(cls, value: date) -> date:
        if value.day != 1:
            raise ValueError("month must be represented by its first day")
        return value

    @property
    def natural_key(self) -> tuple[date, str, str, str, SourceName]:
        return (
            self.month,
            self.port_code,
            self.commodity_code,
            self.country_code,
            self.source,
        )


class PortOperation(BaseModel):
    """Normalized public port metric with publication and availability timestamps."""

    model_config = ConfigDict(frozen=True)

    period_start: date
    frequency: str = Field(pattern=r"^(daily|weekly|monthly)$")
    port_code: str = Field(pattern=r"^\d{4}$")
    port_name: str = Field(min_length=1)
    metric: PortMetricName
    value: Decimal = Field(ge=0)
    unit: str = Field(min_length=1)
    source: SourceName
    source_url: str = Field(min_length=1)
    source_published_at: datetime | None = None
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def monthly_period_must_start_on_first_day(self) -> PortOperation:
        if self.frequency == "monthly" and self.period_start.day != 1:
            raise ValueError("period_start must be the first day for monthly observations")
        return self

    @property
    def natural_key(self) -> tuple[date, str, PortMetricName, SourceName]:
        return self.period_start, self.port_code, self.metric, self.source


class SecFiling(BaseModel):
    """Normalized SEC submission metadata linked to a reviewed registry entity."""

    model_config = ConfigDict(frozen=True)

    entity_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    cik: str = Field(pattern=r"^\d{10}$")
    company_name: str = Field(min_length=1)
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    form: str = Field(min_length=1)
    filed_on: date
    report_date: date | None = None
    accepted_at: datetime
    primary_document: str = Field(min_length=1)
    primary_document_url: str = Field(pattern=r"^https://www\.sec\.gov/Archives/")
    is_xbrl: bool
    is_inline_xbrl: bool
    source: SourceName = SourceName.SEC_EDGAR
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("accepted_at", "ingested_at")
    @classmethod
    def timestamps_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("SEC timestamps must be timezone-aware")
        return value


class SecCompanyFact(BaseModel):
    """One immutable, accession-linked XBRL fact from the SEC Company Facts API."""

    model_config = ConfigDict(frozen=True)

    fact_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    entity_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    cik: str = Field(pattern=r"^\d{10}$")
    taxonomy: str = Field(min_length=1)
    tag: str = Field(min_length=1)
    label: str = Field(min_length=1)
    description: str = ""
    unit: str = Field(min_length=1)
    value: Decimal
    period_start: date | None = None
    period_end: date
    filed_on: date
    accepted_at: datetime
    acceptance_is_estimated: bool
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    fiscal_year: int | None = Field(default=None, ge=1900, le=2200)
    fiscal_period: str | None = None
    form: str = Field(min_length=1)
    frame: str | None = None
    source: SourceName = SourceName.SEC_EDGAR
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("accepted_at", "ingested_at")
    @classmethod
    def fact_timestamps_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("SEC timestamps must be timezone-aware")
        return value

    @field_validator("value")
    @classmethod
    def value_must_be_finite(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("SEC fact value must be finite")
        return value

    @model_validator(mode="after")
    def period_must_be_ordered(self) -> SecCompanyFact:
        if self.period_start is not None and self.period_start > self.period_end:
            raise ValueError("SEC fact period_start must not follow period_end")
        return self


class FederalContractAward(BaseModel):
    """Current USAspending prime-contract award linked to a reviewed company entity."""

    model_config = ConfigDict(frozen=True)

    award_key: str = Field(min_length=1, max_length=300)
    entity_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    ticker: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,9}$")
    award_id: str = Field(min_length=1, max_length=200)
    recipient_name: str = Field(min_length=1)
    recipient_uei: str | None = Field(default=None, min_length=12, max_length=12)
    recipient_id: str | None = Field(default=None, min_length=1)
    match_method: ContractMatchMethod
    award_type: str = Field(min_length=1)
    description: str = ""
    award_amount_usd: Decimal
    total_outlays_usd: Decimal | None = None
    base_obligation_date: date
    start_date: date | None = None
    end_date: date | None = None
    source_modified_at: datetime | None = None
    awarding_agency: str | None = None
    awarding_sub_agency: str | None = None
    funding_agency: str | None = None
    funding_sub_agency: str | None = None
    naics_code: str | None = Field(default=None, pattern=r"^\d{2,6}$")
    naics_description: str | None = None
    psc_code: str | None = Field(default=None, min_length=1, max_length=8)
    psc_description: str | None = None
    place_of_performance_country: str | None = None
    place_of_performance_state: str | None = None
    source_url: str = Field(pattern=r"^https://www\.usaspending\.gov/award/")
    source: SourceName = SourceName.USA_SPENDING_CONTRACT_AWARDS
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("award_amount_usd", "total_outlays_usd")
    @classmethod
    def contract_amounts_must_be_finite(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and not value.is_finite():
            raise ValueError("contract award amounts must be finite")
        return value

    @field_validator("source_modified_at", "ingested_at")
    @classmethod
    def contract_timestamps_must_be_timezone_aware(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("contract timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def contract_dates_must_be_ordered(self) -> FederalContractAward:
        if self.start_date is not None and self.end_date is not None:
            if self.end_date < self.start_date:
                raise ValueError("contract end_date must not precede start_date")
        return self


class ExposureEvidence(BaseModel):
    model_config = ConfigDict(frozen=True)

    evidence_type: EvidenceType
    title: str = Field(min_length=1)
    url: str = Field(pattern=r"^https://")
    published_on: date
    excerpt_summary: str = Field(min_length=1)


class CommodityExposure(BaseModel):
    model_config = ConfigDict(frozen=True)

    hs_code: str = Field(pattern=r"^\d{2,6}$")
    weight: float = Field(gt=0, le=1)
    direction: str = Field(default="demand", pattern=r"^(demand|input|mixed)$")
    rationale: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = Field(min_length=1)


class EntityIdentifier(BaseModel):
    """A source identifier whose effective dates are supported by cited evidence."""

    model_config = ConfigDict(frozen=True)

    scheme: EntityIdentifierScheme
    value: str = Field(min_length=1)
    valid_from: date
    valid_to: date | None = None
    evidence_ids: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def valid_dates_must_be_ordered(self) -> EntityIdentifier:
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ValueError("identifier valid_to must not precede valid_from")
        if self.scheme is EntityIdentifierScheme.SEC_CIK and (
            len(self.value) != 10 or not self.value.isdigit()
        ):
            raise ValueError("SEC CIK identifiers must contain exactly 10 digits")
        if self.scheme is EntityIdentifierScheme.TICKER and (
            len(self.value) > 10
            or not self.value[0].isalpha()
            or self.value != self.value.upper()
            or not all(character.isalnum() or character in ".-" for character in self.value)
        ):
            raise ValueError("ticker identifiers must be uppercase ticker symbols")
        return self


class RegistryEntity(BaseModel):
    """A reviewed issuer, subsidiary, or facility node in the company graph."""

    model_config = ConfigDict(frozen=True)

    entity_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    entity_type: RegistryEntityType
    name: str = Field(min_length=1)
    aliases: tuple[str, ...] = ()
    country_code: str = Field(pattern=r"^[A-Z]{2}$")
    region: str | None = None
    locality: str | None = None
    valid_from: date
    valid_to: date | None = None
    identifiers: tuple[EntityIdentifier, ...] = ()
    evidence_ids: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def dates_and_identifiers_must_be_consistent(self) -> RegistryEntity:
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ValueError("entity valid_to must not precede valid_from")
        for identifier in self.identifiers:
            if identifier.valid_from < self.valid_from:
                raise ValueError("identifier valid_from must not precede entity valid_from")
            if self.valid_to is not None and (
                identifier.valid_to is None or identifier.valid_to > self.valid_to
            ):
                raise ValueError("identifier validity must not extend beyond entity validity")
        return self


class EntityRelationship(BaseModel):
    """A directed, dated edge between reviewed registry entities."""

    model_config = ConfigDict(frozen=True)

    relationship_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    parent_entity_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    child_entity_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$")
    relationship_type: EntityRelationshipType
    ownership_percent: float | None = Field(default=None, gt=0, le=100)
    confidence: ConfidenceLevel
    valid_from: date
    valid_to: date | None = None
    evidence_ids: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def relationship_fields_must_be_consistent(self) -> EntityRelationship:
        if self.parent_entity_id == self.child_entity_id:
            raise ValueError("entity relationship cannot reference itself")
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ValueError("relationship valid_to must not precede valid_from")
        if (
            self.relationship_type is not EntityRelationshipType.OWNS
            and self.ownership_percent is not None
        ):
            raise ValueError("ownership_percent is valid only for owns relationships")
        return self


class CompanyExposure(BaseModel):
    model_config = ConfigDict(frozen=True)

    ticker: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,9}$")
    company_name: str = Field(min_length=1)
    root_entity_id: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9][a-z0-9_-]{2,79}$",
    )
    confidence: ConfidenceLevel
    analyst_reviewed_on: date
    commodity_exposures: tuple[CommodityExposure, ...] = Field(min_length=1)
    port_weights: dict[str, float] = Field(default_factory=dict)
    country_weights: dict[str, float] = Field(default_factory=dict)
    evidence: dict[str, ExposureEvidence] = Field(min_length=1)
    limitations: str = Field(min_length=1)

    @field_validator("port_weights")
    @classmethod
    def validate_port_weights(cls, value: dict[str, float]) -> dict[str, float]:
        if any(len(code) != 4 or not code.isdigit() for code in value):
            raise ValueError("port weight keys must be four-digit Schedule D codes")
        if any(weight <= 0 or weight > 1 for weight in value.values()):
            raise ValueError("port weights must be greater than zero and at most one")
        return value

    @field_validator("country_weights")
    @classmethod
    def validate_country_weights(cls, value: dict[str, float]) -> dict[str, float]:
        if any(len(code) != 4 or not code.isdigit() for code in value):
            raise ValueError("country weight keys must be four-digit Schedule C codes")
        if any(weight <= 0 or weight > 1 for weight in value.values()):
            raise ValueError("country weights must be greater than zero and at most one")
        return value

    @model_validator(mode="after")
    def evidence_references_must_exist(self) -> CompanyExposure:
        missing = {
            evidence_id
            for exposure in self.commodity_exposures
            for evidence_id in exposure.evidence_ids
            if evidence_id not in self.evidence
        }
        if missing:
            raise ValueError(f"unknown evidence ids: {sorted(missing)}")
        return self


class CompanyExposureRegistry(BaseModel):
    model_config = ConfigDict(frozen=True)

    version: int = Field(ge=1)
    entities: tuple[RegistryEntity, ...] = ()
    relationships: tuple[EntityRelationship, ...] = ()
    companies: tuple[CompanyExposure, ...]

    @model_validator(mode="after")
    def entity_graph_must_be_valid(self) -> CompanyExposureRegistry:
        graph_fields_present = bool(self.entities or self.relationships) or any(
            company.root_entity_id is not None for company in self.companies
        )
        if self.version < 2:
            if graph_fields_present:
                raise ValueError("entity graph fields require registry version 2 or newer")
            return self

        entity_by_id = {entity.entity_id: entity for entity in self.entities}
        if len(entity_by_id) != len(self.entities):
            raise ValueError("registry entity_id values must be unique")
        if not entity_by_id:
            raise ValueError("registry version 2 requires at least one entity")

        tickers = [company.ticker for company in self.companies]
        if len(set(tickers)) != len(tickers):
            raise ValueError("registry company tickers must be unique")

        known_evidence: set[str] = set()
        for company in self.companies:
            duplicates = known_evidence.intersection(company.evidence)
            if duplicates:
                raise ValueError(f"evidence ids must be globally unique: {sorted(duplicates)}")
            known_evidence.update(company.evidence)

            root = entity_by_id.get(company.root_entity_id or "")
            if root is None:
                raise ValueError(f"unknown root_entity_id for {company.ticker}")
            if root.entity_type is not RegistryEntityType.ISSUER:
                raise ValueError(f"root entity for {company.ticker} must be an issuer")
            root_identifiers = {
                (identifier.scheme, identifier.value) for identifier in root.identifiers
            }
            if (EntityIdentifierScheme.TICKER, company.ticker) not in root_identifiers:
                raise ValueError(f"root entity for {company.ticker} must carry its ticker")

        identifiers: set[tuple[EntityIdentifierScheme, str]] = set()
        for entity in self.entities:
            self._check_evidence(entity.evidence_ids, known_evidence, entity.entity_id)
            for identifier in entity.identifiers:
                key = (identifier.scheme, identifier.value)
                if key in identifiers:
                    formatted_identifier = f"{identifier.scheme}:{identifier.value}"
                    raise ValueError(f"duplicate external identifier: {formatted_identifier}")
                identifiers.add(key)
                self._check_evidence(
                    identifier.evidence_ids,
                    known_evidence,
                    f"{entity.entity_id} identifier",
                )

        relationship_ids: set[str] = set()
        adjacency: dict[str, list[str]] = {entity_id: [] for entity_id in entity_by_id}
        for relationship in self.relationships:
            if relationship.relationship_id in relationship_ids:
                raise ValueError("registry relationship_id values must be unique")
            relationship_ids.add(relationship.relationship_id)
            parent = entity_by_id.get(relationship.parent_entity_id)
            child = entity_by_id.get(relationship.child_entity_id)
            if parent is None or child is None:
                raise ValueError(f"relationship {relationship.relationship_id} has unknown entity")
            if parent.entity_type is RegistryEntityType.FACILITY:
                raise ValueError("facility entities cannot be relationship parents")
            if (
                relationship.relationship_type is EntityRelationshipType.OPERATES
                and child.entity_type is not RegistryEntityType.FACILITY
            ):
                raise ValueError("operates relationships must target a facility")
            if (
                relationship.relationship_type is EntityRelationshipType.OWNS
                and child.entity_type is RegistryEntityType.FACILITY
            ):
                raise ValueError("facility relationships must use operates, not owns")
            if relationship.valid_from < max(parent.valid_from, child.valid_from):
                raise ValueError("relationship valid_from must not precede its entities")
            bounded_ends = [
                end_date for end_date in (parent.valid_to, child.valid_to) if end_date is not None
            ]
            if bounded_ends and (
                relationship.valid_to is None or relationship.valid_to > min(bounded_ends)
            ):
                raise ValueError("relationship validity must not extend beyond its entities")
            self._check_evidence(
                relationship.evidence_ids,
                known_evidence,
                relationship.relationship_id,
            )
            adjacency[relationship.parent_entity_id].append(relationship.child_entity_id)

        self._check_acyclic(adjacency)
        reachable: set[str] = set()
        for company in self.companies:
            self._collect_reachable(company.root_entity_id or "", adjacency, reachable)
        unreachable = set(entity_by_id) - reachable
        if unreachable:
            formatted_entities = sorted(unreachable)
            raise ValueError(
                f"registry entities must be reachable from an issuer: {formatted_entities}"
            )
        return self

    @staticmethod
    def _check_evidence(
        evidence_ids: tuple[str, ...],
        known_evidence: set[str],
        owner: str,
    ) -> None:
        missing = set(evidence_ids) - known_evidence
        if missing:
            raise ValueError(f"unknown evidence ids for {owner}: {sorted(missing)}")

    @staticmethod
    def _check_acyclic(adjacency: dict[str, list[str]]) -> None:
        visited: set[str] = set()
        visiting: set[str] = set()

        def visit(entity_id: str) -> None:
            if entity_id in visiting:
                raise ValueError("registry entity relationships must be acyclic")
            if entity_id in visited:
                return
            visiting.add(entity_id)
            for child_id in adjacency[entity_id]:
                visit(child_id)
            visiting.remove(entity_id)
            visited.add(entity_id)

        for entity_id in adjacency:
            visit(entity_id)

    @staticmethod
    def _collect_reachable(
        entity_id: str,
        adjacency: dict[str, list[str]],
        reachable: set[str],
    ) -> None:
        if entity_id in reachable:
            return
        reachable.add(entity_id)
        for child_id in adjacency[entity_id]:
            CompanyExposureRegistry._collect_reachable(child_id, adjacency, reachable)


class IngestionResult(BaseModel):
    run_id: str
    source: SourceName
    status: IngestionStatus
    records_received: int = Field(ge=0)
    records_written: int = Field(ge=0)
    started_at: datetime
    completed_at: datetime
    error_message: str | None = None
