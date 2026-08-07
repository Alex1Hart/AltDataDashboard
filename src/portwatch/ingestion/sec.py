from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from time import monotonic, sleep
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from portwatch.config import Settings
from portwatch.models import (
    CompanyExposureRegistry,
    EntityIdentifierScheme,
    SecCompanyFact,
    SecFiling,
)
from portwatch.registry import company_entity_ids, resolve_entity_id

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"
_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


class SecConfigurationError(RuntimeError):
    pass


class SecResponseError(RuntimeError):
    pass


class SecTransientResponseError(RuntimeError):
    pass


@dataclass(frozen=True)
class SecEdgarBatch:
    entity_id: str
    cik: str
    ticker: str
    filings: list[SecFiling]
    facts: list[SecCompanyFact]
    submissions_url: str
    submissions_payload: bytes
    company_facts_url: str
    company_facts_payload: bytes


class SecEdgarClient:
    """Registry-resolved, fair-access client for SEC submissions and Company Facts."""

    def __init__(
        self,
        settings: Settings,
        registry: CompanyExposureRegistry,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep_fn: Callable[[float], None] = sleep,
        monotonic_fn: Callable[[], float] = monotonic,
        progress_fn: Callable[[str], None] | None = None,
    ) -> None:
        self.settings = settings
        self.registry = registry
        self.transport = transport
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.progress_fn = progress_fn or (lambda message: None)
        self._last_request_at: float | None = None

    def registered_identity(self, ticker: str) -> tuple[str, str]:
        normalized_ticker = ticker.upper()
        company = next(
            (item for item in self.registry.companies if item.ticker == normalized_ticker),
            None,
        )
        if company is None or company.root_entity_id is None:
            raise SecConfigurationError(f"ticker is not registered for SEC ingestion: {ticker}")
        root = next(
            entity
            for entity in self.registry.entities
            if entity.entity_id == company.root_entity_id
        )
        cik_identifier = next(
            (
                identifier
                for identifier in root.identifiers
                if identifier.scheme is EntityIdentifierScheme.SEC_CIK
            ),
            None,
        )
        if cik_identifier is None:
            raise SecConfigurationError(f"registered issuer has no SEC CIK: {ticker}")
        resolved_entity_id = resolve_entity_id(
            self.registry,
            scheme=EntityIdentifierScheme.SEC_CIK,
            value=cik_identifier.value,
        )
        if resolved_entity_id != root.entity_id:
            raise SecConfigurationError(f"SEC CIK does not resolve to {ticker}'s root entity")
        return root.entity_id, cik_identifier.value

    def fetch_company(self, ticker: str) -> SecEdgarBatch:
        self._validate_user_agent()
        normalized_ticker = ticker.upper()
        expected_entity_id, cik = self.registered_identity(normalized_ticker)
        submissions_url = SUBMISSIONS_URL.format(cik=cik)
        company_facts_url = COMPANY_FACTS_URL.format(cik=cik)
        self.progress_fn(f"SEC 1/4: downloading submissions for {normalized_ticker} ({cik})")
        submissions_payload, submissions_raw = self._get_json(submissions_url)
        ingested_at = datetime.now(UTC)
        filings = parse_sec_submissions(
            submissions_payload,
            registry=self.registry,
            expected_entity_id=expected_entity_id,
            ticker=normalized_ticker,
            ingested_at=ingested_at,
        )
        self.progress_fn(f"SEC 2/4: normalized {len(filings):,} recent filings")
        accepted_by_accession = {filing.accession_number: filing.accepted_at for filing in filings}
        self.progress_fn("SEC 3/4: downloading and scanning Company Facts")
        company_facts_payload, company_facts_raw = self._get_json(company_facts_url)
        filed_on_or_after = date(
            ingested_at.year - self.settings.sec_fact_history_years,
            1,
            1,
        )
        facts = parse_sec_company_facts(
            company_facts_payload,
            registry=self.registry,
            expected_entity_id=expected_entity_id,
            ticker=normalized_ticker,
            accepted_by_accession=accepted_by_accession,
            ingested_at=ingested_at,
            filed_on_or_after=filed_on_or_after,
            progress_fn=self.progress_fn,
        )
        self.progress_fn(
            f"SEC 4/4: normalized {len(facts):,} facts filed since {filed_on_or_after.isoformat()}"
        )
        return SecEdgarBatch(
            entity_id=expected_entity_id,
            cik=cik,
            ticker=normalized_ticker,
            filings=filings,
            facts=facts,
            submissions_url=submissions_url,
            submissions_payload=submissions_raw,
            company_facts_url=company_facts_url,
            company_facts_payload=company_facts_raw,
        )

    @retry(
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.NetworkError, SecTransientResponseError)
        ),
        wait=wait_exponential_jitter(initial=1, max=10),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _get_json(self, url: str) -> tuple[Mapping[str, Any], bytes]:
        self._rate_limit()
        with httpx.Client(
            timeout=self.settings.http_timeout_seconds,
            headers={
                "User-Agent": self.settings.user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
            transport=self.transport,
        ) as client:
            response = client.get(url)
        if response.status_code in _TRANSIENT_STATUS_CODES:
            raise SecTransientResponseError(
                f"SEC returned transient HTTP {response.status_code} for {url}"
            )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise SecResponseError(f"SEC returned non-JSON content for {url}") from exc
        if not isinstance(payload, Mapping):
            raise SecResponseError(f"SEC returned a non-object JSON payload for {url}")
        return payload, response.content

    def _rate_limit(self) -> None:
        now = self.monotonic_fn()
        if self._last_request_at is not None:
            elapsed = now - self._last_request_at
            remaining = self.settings.sec_request_interval_seconds - elapsed
            if remaining > 0:
                self.sleep_fn(remaining)
                now += remaining
        self._last_request_at = now

    def _validate_user_agent(self) -> None:
        if "@" not in self.settings.user_agent:
            raise SecConfigurationError(
                "PORTWATCH_USER_AGENT must identify the project and include a contact email "
                "before accessing SEC EDGAR"
            )


def parse_sec_submissions(
    payload: Mapping[str, Any],
    *,
    registry: CompanyExposureRegistry,
    expected_entity_id: str,
    ticker: str,
    ingested_at: datetime,
) -> list[SecFiling]:
    cik = _resolve_payload_cik(payload, registry, expected_entity_id, ticker)
    company_name = _required_text(payload, "name", "SEC submissions")
    filings = payload.get("filings")
    if not isinstance(filings, Mapping):
        raise SecResponseError("SEC submissions payload is missing filings")
    recent = filings.get("recent")
    if not isinstance(recent, Mapping):
        raise SecResponseError("SEC submissions payload is missing recent filings")

    required_columns = (
        "accessionNumber",
        "filingDate",
        "reportDate",
        "acceptanceDateTime",
        "form",
        "primaryDocument",
        "isXBRL",
        "isInlineXBRL",
    )
    columns: dict[str, list[Any]] = {}
    for column_name in required_columns:
        values = recent.get(column_name)
        if not isinstance(values, list):
            raise SecResponseError(f"SEC recent filings column is missing: {column_name}")
        columns[column_name] = values
    row_count = len(columns["accessionNumber"])
    inconsistent = [name for name, values in columns.items() if len(values) != row_count]
    if inconsistent:
        raise SecResponseError(
            f"SEC recent filing columns have inconsistent lengths: {inconsistent}"
        )

    normalized: list[SecFiling] = []
    for position in range(row_count):
        accession_number = str(columns["accessionNumber"][position])
        primary_document = str(columns["primaryDocument"][position]).strip()
        normalized.append(
            SecFiling(
                entity_id=expected_entity_id,
                cik=cik,
                company_name=company_name,
                accession_number=accession_number,
                form=str(columns["form"][position]).strip(),
                filed_on=_parse_date(columns["filingDate"][position], "filingDate"),
                report_date=_parse_optional_date(columns["reportDate"][position], "reportDate"),
                accepted_at=_parse_datetime(
                    columns["acceptanceDateTime"][position],
                    "acceptanceDateTime",
                ),
                primary_document=primary_document,
                primary_document_url=_filing_url(cik, accession_number, primary_document),
                is_xbrl=_parse_bool(columns["isXBRL"][position], "isXBRL"),
                is_inline_xbrl=_parse_bool(
                    columns["isInlineXBRL"][position],
                    "isInlineXBRL",
                ),
                ingested_at=ingested_at,
            )
        )
    return normalized


def parse_sec_company_facts(
    payload: Mapping[str, Any],
    *,
    registry: CompanyExposureRegistry,
    expected_entity_id: str,
    ticker: str,
    accepted_by_accession: Mapping[str, datetime],
    ingested_at: datetime,
    filed_on_or_after: date | None = None,
    progress_fn: Callable[[str], None] | None = None,
) -> list[SecCompanyFact]:
    cik = _resolve_payload_cik(payload, registry, expected_entity_id, ticker)
    facts_payload = payload.get("facts")
    if not isinstance(facts_payload, Mapping):
        raise SecResponseError("SEC Company Facts payload is missing facts")

    facts_by_id: dict[str, SecCompanyFact] = {}
    observations_scanned = 0
    for taxonomy, taxonomy_payload in facts_payload.items():
        if not isinstance(taxonomy_payload, Mapping):
            raise SecResponseError(f"SEC taxonomy payload is malformed: {taxonomy}")
        for tag, concept_payload in taxonomy_payload.items():
            if not isinstance(concept_payload, Mapping):
                raise SecResponseError(f"SEC concept payload is malformed: {taxonomy}.{tag}")
            label = str(concept_payload.get("label") or tag).strip()
            description = str(concept_payload.get("description") or "").strip()
            units = concept_payload.get("units")
            if not isinstance(units, Mapping):
                raise SecResponseError(f"SEC concept has no units: {taxonomy}.{tag}")
            for unit, observations in units.items():
                if not isinstance(observations, list):
                    raise SecResponseError(f"SEC fact observations are malformed: {taxonomy}.{tag}")
                for observation in observations:
                    observations_scanned += 1
                    if progress_fn is not None and observations_scanned % 25_000 == 0:
                        progress_fn(
                            f"SEC 3/4: scanned {observations_scanned:,} observations; "
                            f"retained {len(facts_by_id):,}"
                        )
                    if not isinstance(observation, Mapping):
                        raise SecResponseError(
                            f"SEC fact observation is malformed: {taxonomy}.{tag}"
                        )
                    filed_on = _parse_date(observation.get("filed"), "filed")
                    if filed_on_or_after is not None and filed_on < filed_on_or_after:
                        continue
                    fact = _parse_company_fact_observation(
                        observation,
                        entity_id=expected_entity_id,
                        cik=cik,
                        taxonomy=str(taxonomy),
                        tag=str(tag),
                        label=label,
                        description=description,
                        unit=str(unit),
                        accepted_by_accession=accepted_by_accession,
                        ingested_at=ingested_at,
                        filed_on=filed_on,
                    )
                    existing = facts_by_id.get(fact.fact_id)
                    if existing is not None and existing.value != fact.value:
                        raise SecResponseError(
                            f"SEC returned conflicting values for fact context: {fact.fact_id}"
                        )
                    facts_by_id[fact.fact_id] = fact
    return list(facts_by_id.values())


def _parse_company_fact_observation(
    payload: object,
    *,
    entity_id: str,
    cik: str,
    taxonomy: str,
    tag: str,
    label: str,
    description: str,
    unit: str,
    accepted_by_accession: Mapping[str, datetime],
    ingested_at: datetime,
    filed_on: date | None = None,
) -> SecCompanyFact:
    if not isinstance(payload, Mapping):
        raise SecResponseError(f"SEC fact observation is malformed: {taxonomy}.{tag}")
    try:
        value = Decimal(str(payload["val"]))
    except (KeyError, InvalidOperation, ValueError) as exc:
        raise SecResponseError(f"SEC fact value is invalid: {taxonomy}.{tag}") from exc
    accession_number = _required_text(payload, "accn", f"SEC fact {taxonomy}.{tag}")
    filed_on = filed_on or _parse_date(payload.get("filed"), "filed")
    accepted_at = accepted_by_accession.get(accession_number)
    acceptance_is_estimated = accepted_at is None
    if accepted_at is None:
        accepted_at = datetime.combine(filed_on, time.min, tzinfo=UTC)
    period_start = _parse_optional_date(payload.get("start"), "start")
    period_end = _parse_date(payload.get("end"), "end")
    fiscal_year_raw = payload.get("fy")
    try:
        fiscal_year = None if fiscal_year_raw in (None, "") else int(str(fiscal_year_raw))
    except ValueError as exc:
        raise SecResponseError(f"SEC fact fiscal year is invalid: {taxonomy}.{tag}") from exc
    fiscal_period_raw = payload.get("fp")
    fiscal_period = None if fiscal_period_raw in (None, "") else str(fiscal_period_raw)
    form = _required_text(payload, "form", f"SEC fact {taxonomy}.{tag}")
    frame_raw = payload.get("frame")
    frame = None if frame_raw in (None, "") else str(frame_raw)
    fact_id = _fact_id(
        entity_id=entity_id,
        taxonomy=taxonomy,
        tag=tag,
        unit=unit,
        period_start=period_start,
        period_end=period_end,
        accession_number=accession_number,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        form=form,
        frame=frame,
    )
    return SecCompanyFact(
        fact_id=fact_id,
        entity_id=entity_id,
        cik=cik,
        taxonomy=taxonomy,
        tag=tag,
        label=label,
        description=description,
        unit=unit,
        value=value,
        period_start=period_start,
        period_end=period_end,
        filed_on=filed_on,
        accepted_at=accepted_at,
        acceptance_is_estimated=acceptance_is_estimated,
        accession_number=accession_number,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        form=form,
        frame=frame,
        ingested_at=ingested_at,
    )


def _resolve_payload_cik(
    payload: Mapping[str, Any],
    registry: CompanyExposureRegistry,
    expected_entity_id: str,
    ticker: str,
) -> str:
    cik = _normalize_cik(payload.get("cik"))
    resolved_entity_id = resolve_entity_id(
        registry,
        scheme=EntityIdentifierScheme.SEC_CIK,
        value=cik,
    )
    if resolved_entity_id != expected_entity_id:
        raise SecResponseError(f"SEC payload CIK is not registered to {ticker}: {cik}")
    if expected_entity_id not in company_entity_ids(registry, ticker):
        raise SecResponseError(f"SEC payload entity is outside {ticker}'s reviewed graph")
    return cik


def _fact_id(**dimensions: object) -> str:
    encoded = json.dumps(dimensions, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _filing_url(cik: str, accession_number: str, primary_document: str) -> str:
    accession_path = accession_number.replace("-", "")
    return f"{ARCHIVES_URL}/{int(cik)}/{accession_path}/{primary_document}"


def _normalize_cik(value: object) -> str:
    raw = str(value).strip()
    if not raw.isdigit() or len(raw) > 10:
        raise SecResponseError(f"SEC payload contains an invalid CIK: {value}")
    return raw.zfill(10)


def _required_text(payload: Mapping[str, Any], key: str, owner: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise SecResponseError(f"{owner} is missing {key}")
    return value


def _parse_date(value: object, field_name: str) -> date:
    if value in (None, ""):
        raise SecResponseError(f"SEC field is missing: {field_name}")
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise SecResponseError(f"SEC field has an invalid date: {field_name}") from exc


def _parse_optional_date(value: object, field_name: str) -> date | None:
    if value in (None, ""):
        return None
    return _parse_date(value, field_name)


def _parse_datetime(value: object, field_name: str) -> datetime:
    if value in (None, ""):
        raise SecResponseError(f"SEC field is missing: {field_name}")
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise SecResponseError(f"SEC field has an invalid timestamp: {field_name}") from exc
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _parse_bool(value: object, field_name: str) -> bool:
    if value in (True, 1, "1"):
        return True
    if value in (False, 0, "0"):
        return False
    raise SecResponseError(f"SEC field has an invalid boolean: {field_name}")
