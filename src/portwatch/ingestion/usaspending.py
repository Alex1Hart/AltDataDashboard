from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from time import monotonic, sleep
from typing import Any
from urllib.parse import quote

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from portwatch.config import Settings
from portwatch.models import (
    CompanyExposureRegistry,
    ContractMatchMethod,
    EntityIdentifierScheme,
    FederalContractAward,
    FederalContractTransaction,
    RegistryEntityType,
)
from portwatch.registry import company_entity_ids, resolve_entity_id

AWARD_SEARCH_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
TRANSACTION_SEARCH_URL = "https://api.usaspending.gov/api/v2/transactions/"
CONTRACT_AWARD_TYPE_CODES = ("A", "B", "C", "D")
AWARD_FIELDS = (
    "Award ID",
    "Recipient Name",
    "Recipient UEI",
    "recipient_id",
    "generated_internal_id",
    "Award Amount",
    "Total Outlays",
    "Description",
    "Base Obligation Date",
    "Start Date",
    "End Date",
    "Last Modified Date",
    "Awarding Agency",
    "Awarding Sub Agency",
    "Funding Agency",
    "Funding Sub Agency",
    "Contract Award Type",
    "NAICS",
    "PSC",
    "Place of Performance Country Code",
    "Place of Performance State Code",
)
_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
_NORMALIZE_NAME_PATTERN = re.compile(r"[^A-Z0-9]+")


class USAspendingConfigurationError(RuntimeError):
    pass


class USAspendingResponseError(RuntimeError):
    pass


class USAspendingTransientResponseError(RuntimeError):
    pass


@dataclass(frozen=True)
class USAspendingAwardPage:
    search_term: str
    page_number: int
    request_payload: bytes
    response_payload: bytes
    source_url: str
    awards: tuple[FederalContractAward, ...]
    records_received: int
    records_unmatched: int
    has_next: bool


@dataclass(frozen=True)
class USAspendingTransactionPage:
    award_key: str
    page_number: int
    request_payload: bytes
    response_payload: bytes
    source_url: str
    transactions: tuple[FederalContractTransaction, ...]
    records_received: int
    has_next: bool


@dataclass(frozen=True)
class USAspendingAwardBatch:
    ticker: str
    root_entity_id: str
    start_date: date
    end_date: date
    pages: tuple[USAspendingAwardPage, ...]
    awards: tuple[FederalContractAward, ...]
    transaction_pages: tuple[USAspendingTransactionPage, ...]
    transactions: tuple[FederalContractTransaction, ...]
    records_received: int
    records_unmatched: int
    transaction_records_received: int


class USAspendingClient:
    """Rate-limited USAspending award search with reviewed company attribution."""

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
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.progress_fn = progress_fn or (lambda message: None)
        self._last_request_at: float | None = None
        self._client = httpx.Client(
            timeout=settings.http_timeout_seconds,
            headers={
                "User-Agent": settings.user_agent,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def fetch_company(
        self,
        ticker: str,
        *,
        start_date: date,
        end_date: date,
    ) -> USAspendingAwardBatch:
        if end_date < start_date:
            raise USAspendingConfigurationError("end_date must not precede start_date")
        normalized_ticker = ticker.upper()
        root_entity_id, search_terms = company_contract_search_terms(
            self.registry,
            normalized_ticker,
            as_of=end_date,
        )
        ingested_at = datetime.now(UTC)
        pages: list[USAspendingAwardPage] = []
        awards_by_key: dict[str, FederalContractAward] = {}
        records_received = 0
        records_unmatched = 0

        for search_position, search_term in enumerate(search_terms, start=1):
            page_number = 1
            while True:
                self.progress_fn(
                    f"USAspending search {search_position}/{len(search_terms)}, "
                    f"page {page_number}: {search_term}"
                )
                request = _award_search_request(
                    search_term=search_term,
                    start_date=start_date,
                    end_date=end_date,
                    page=page_number,
                    limit=self.settings.usaspending_page_size,
                )
                request_payload = json.dumps(
                    request,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                response, response_payload = self._post_json(AWARD_SEARCH_URL, request)
                page_awards, page_received, page_unmatched, has_next = parse_award_page(
                    response,
                    registry=self.registry,
                    ticker=normalized_ticker,
                    ingested_at=ingested_at,
                )
                page = USAspendingAwardPage(
                    search_term=search_term,
                    page_number=page_number,
                    request_payload=request_payload,
                    response_payload=response_payload,
                    source_url=AWARD_SEARCH_URL,
                    awards=tuple(page_awards),
                    records_received=page_received,
                    records_unmatched=page_unmatched,
                    has_next=has_next,
                )
                pages.append(page)
                records_received += page_received
                records_unmatched += page_unmatched
                for award in page_awards:
                    existing = awards_by_key.get(award.award_key)
                    if existing is not None and existing != award:
                        raise USAspendingResponseError(
                            f"conflicting award snapshots returned for {award.award_key}"
                        )
                    awards_by_key[award.award_key] = award
                if not has_next:
                    break
                if page_number >= self.settings.usaspending_max_pages_per_search:
                    raise USAspendingResponseError(
                        "USAspending pagination limit reached before the result set completed; "
                        "narrow the date range or increase "
                        "PORTWATCH_USASPENDING_MAX_PAGES_PER_SEARCH"
                    )
                page_number += 1

        transaction_pages, transactions = self._fetch_transaction_history(
            tuple(awards_by_key.values()),
            ingested_at=ingested_at,
        )
        return USAspendingAwardBatch(
            ticker=normalized_ticker,
            root_entity_id=root_entity_id,
            start_date=start_date,
            end_date=end_date,
            pages=tuple(pages),
            awards=tuple(awards_by_key.values()),
            transaction_pages=transaction_pages,
            transactions=transactions,
            records_received=records_received,
            records_unmatched=records_unmatched,
            transaction_records_received=sum(page.records_received for page in transaction_pages),
        )

    def _fetch_transaction_history(
        self,
        awards: tuple[FederalContractAward, ...],
        *,
        ingested_at: datetime,
    ) -> tuple[
        tuple[USAspendingTransactionPage, ...],
        tuple[FederalContractTransaction, ...],
    ]:
        pages: list[USAspendingTransactionPage] = []
        transactions_by_id: dict[str, FederalContractTransaction] = {}
        for award_position, award in enumerate(awards, start=1):
            page_number = 1
            while True:
                self.progress_fn(
                    f"USAspending transaction history {award_position}/{len(awards)}, "
                    f"page {page_number}: {award.award_id}"
                )
                request = _transaction_search_request(
                    award_key=award.award_key,
                    page=page_number,
                    limit=self.settings.usaspending_transaction_page_size,
                )
                request_payload = json.dumps(
                    request,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                response, response_payload = self._post_json(TRANSACTION_SEARCH_URL, request)
                page_transactions, received, has_next = parse_transaction_page(
                    response,
                    award=award,
                    ingested_at=ingested_at,
                )
                pages.append(
                    USAspendingTransactionPage(
                        award_key=award.award_key,
                        page_number=page_number,
                        request_payload=request_payload,
                        response_payload=response_payload,
                        source_url=TRANSACTION_SEARCH_URL,
                        transactions=tuple(page_transactions),
                        records_received=received,
                        has_next=has_next,
                    )
                )
                for transaction in page_transactions:
                    existing = transactions_by_id.get(transaction.transaction_id)
                    if existing is not None and existing != transaction:
                        raise USAspendingResponseError(
                            "conflicting transaction snapshots returned for "
                            f"{transaction.transaction_id}"
                        )
                    transactions_by_id[transaction.transaction_id] = transaction
                if not has_next:
                    break
                if page_number >= self.settings.usaspending_max_transaction_pages_per_award:
                    raise USAspendingResponseError(
                        "USAspending transaction pagination limit reached for award "
                        f"{award.award_id}; increase "
                        "PORTWATCH_USASPENDING_MAX_TRANSACTION_PAGES_PER_AWARD"
                    )
                page_number += 1
        return tuple(pages), tuple(transactions_by_id.values())

    @retry(
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.NetworkError, USAspendingTransientResponseError)
        ),
        wait=wait_exponential_jitter(initial=1, max=10),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _post_json(
        self,
        url: str,
        request: Mapping[str, Any],
    ) -> tuple[Mapping[str, Any], bytes]:
        self._rate_limit()
        response = self._client.post(url, json=request)
        if response.status_code in _TRANSIENT_STATUS_CODES:
            raise USAspendingTransientResponseError(
                f"USAspending returned transient HTTP {response.status_code}"
            )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise USAspendingResponseError("USAspending returned non-JSON content") from exc
        if not isinstance(payload, Mapping):
            raise USAspendingResponseError("USAspending returned a non-object JSON payload")
        return payload, response.content

    def _rate_limit(self) -> None:
        now = self.monotonic_fn()
        if self._last_request_at is not None:
            remaining = self.settings.usaspending_request_interval_seconds - (
                now - self._last_request_at
            )
            if remaining > 0:
                self.sleep_fn(remaining)
                now += remaining
        self._last_request_at = now


def company_contract_search_terms(
    registry: CompanyExposureRegistry,
    ticker: str,
    *,
    as_of: date,
) -> tuple[str, tuple[str, ...]]:
    """Return stable search terms for active non-facility entities in a company graph."""
    company = next((item for item in registry.companies if item.ticker == ticker), None)
    if company is None or company.root_entity_id is None:
        raise USAspendingConfigurationError(
            f"ticker is not registered for ContractWatch ingestion: {ticker}"
        )
    active_ids = set(company_entity_ids(registry, ticker, as_of=as_of))
    entities = [
        entity
        for entity in registry.entities
        if entity.entity_id in active_ids and entity.entity_type is not RegistryEntityType.FACILITY
    ]
    if not entities:
        raise USAspendingConfigurationError(
            f"ticker has no active legal entities on {as_of.isoformat()}: {ticker}"
        )

    identifier_terms: list[str] = []
    name_terms: list[str] = []
    for entity in entities:
        for identifier in entity.identifiers:
            if (
                identifier.valid_from <= as_of
                and (identifier.valid_to is None or as_of <= identifier.valid_to)
                and identifier.scheme
                in {
                    EntityIdentifierScheme.UEI,
                    EntityIdentifierScheme.USA_SPENDING_RECIPIENT_ID,
                }
            ):
                identifier_terms.append(identifier.value)
        name_terms.append(entity.name)
    terms = tuple(dict.fromkeys((*identifier_terms, *name_terms)))
    return company.root_entity_id, terms


def parse_award_page(
    payload: Mapping[str, Any],
    *,
    registry: CompanyExposureRegistry,
    ticker: str,
    ingested_at: datetime,
) -> tuple[list[FederalContractAward], int, int, bool]:
    results = payload.get("results")
    if not isinstance(results, list):
        raise USAspendingResponseError("USAspending award response is missing results")
    page_metadata = payload.get("page_metadata")
    if not isinstance(page_metadata, Mapping):
        raise USAspendingResponseError("USAspending award response is missing page_metadata")
    has_next = page_metadata.get("hasNext")
    if not isinstance(has_next, bool):
        raise USAspendingResponseError("USAspending page_metadata.hasNext is not a boolean")

    awards: list[FederalContractAward] = []
    unmatched = 0
    for result in results:
        if not isinstance(result, Mapping):
            raise USAspendingResponseError("USAspending award result is not an object")
        base_obligation_date = _required_date(result, "Base Obligation Date")
        resolution = _resolve_recipient(
            result,
            registry=registry,
            ticker=ticker,
            as_of=base_obligation_date,
        )
        if resolution is None:
            unmatched += 1
            continue
        entity_id, match_method = resolution
        generated_internal_id = _required_text(result, "generated_internal_id")
        naics_code, naics_description = _code_and_description(result.get("NAICS"), digits=True)
        psc_code, psc_description = _code_and_description(result.get("PSC"), digits=False)
        recipient_uei = _optional_text(result, "Recipient UEI")
        if recipient_uei is not None:
            recipient_uei = recipient_uei.upper()
            if len(recipient_uei) != 12:
                recipient_uei = None
        awards.append(
            FederalContractAward(
                award_key=generated_internal_id,
                entity_id=entity_id,
                ticker=ticker,
                award_id=_required_text(result, "Award ID"),
                recipient_name=_required_text(result, "Recipient Name"),
                recipient_uei=recipient_uei,
                recipient_id=_optional_text(result, "recipient_id"),
                match_method=match_method,
                award_type=_required_text(result, "Contract Award Type"),
                description=_optional_text(result, "Description") or "",
                award_amount_usd=_required_decimal(result, "Award Amount"),
                total_outlays_usd=_optional_decimal(result, "Total Outlays"),
                base_obligation_date=base_obligation_date,
                start_date=_optional_date(result, "Start Date"),
                end_date=_optional_date(result, "End Date"),
                source_modified_at=_optional_datetime(result, "Last Modified Date"),
                awarding_agency=_optional_text(result, "Awarding Agency"),
                awarding_sub_agency=_optional_text(result, "Awarding Sub Agency"),
                funding_agency=_optional_text(result, "Funding Agency"),
                funding_sub_agency=_optional_text(result, "Funding Sub Agency"),
                naics_code=naics_code,
                naics_description=naics_description,
                psc_code=psc_code,
                psc_description=psc_description,
                place_of_performance_country=_optional_text(
                    result,
                    "Place of Performance Country Code",
                ),
                place_of_performance_state=_optional_text(
                    result,
                    "Place of Performance State Code",
                ),
                source_url=(
                    f"https://www.usaspending.gov/award/{quote(generated_internal_id, safe='')}/"
                ),
                ingested_at=ingested_at,
            )
        )
    return awards, len(results), unmatched, has_next


def parse_transaction_page(
    payload: Mapping[str, Any],
    *,
    award: FederalContractAward,
    ingested_at: datetime,
) -> tuple[list[FederalContractTransaction], int, bool]:
    """Normalize one complete parent-award transaction-history page."""
    results = payload.get("results")
    if not isinstance(results, list):
        raise USAspendingResponseError("USAspending transaction response is missing results")
    page_metadata = payload.get("page_metadata")
    if not isinstance(page_metadata, Mapping):
        raise USAspendingResponseError("USAspending transaction response is missing page_metadata")
    has_next = page_metadata.get("hasNext")
    if not isinstance(has_next, bool):
        raise USAspendingResponseError(
            "USAspending transaction page_metadata.hasNext is not a boolean"
        )

    transactions: list[FederalContractTransaction] = []
    for result in results:
        if not isinstance(result, Mapping):
            raise USAspendingResponseError("USAspending transaction result is not an object")
        transactions.append(
            FederalContractTransaction(
                transaction_id=_required_text(result, "id"),
                award_key=award.award_key,
                entity_id=award.entity_id,
                ticker=award.ticker,
                award_id=award.award_id,
                action_date=_required_date(result, "action_date"),
                federal_action_obligation_usd=_required_decimal(
                    result,
                    "federal_action_obligation",
                ),
                action_type=_optional_text(result, "action_type"),
                action_type_description=_optional_text(
                    result,
                    "action_type_description",
                ),
                modification_number=_optional_text(result, "modification_number") or "",
                description=_optional_text(result, "description") or "",
                award_type_code=_required_text(result, "type"),
                award_type_description=_optional_text(result, "type_description"),
                source_url=award.source_url,
                ingested_at=ingested_at,
            )
        )
    return transactions, len(results), has_next


def _resolve_recipient(
    result: Mapping[str, Any],
    *,
    registry: CompanyExposureRegistry,
    ticker: str,
    as_of: date,
) -> tuple[str, ContractMatchMethod] | None:
    reachable_ids = set(company_entity_ids(registry, ticker, as_of=as_of))
    if not reachable_ids:
        return None
    uei = _optional_text(result, "Recipient UEI")
    if uei is not None:
        entity_id = resolve_entity_id(
            registry,
            scheme=EntityIdentifierScheme.UEI,
            value=uei.upper(),
            as_of=as_of,
        )
        if entity_id in reachable_ids:
            return entity_id, ContractMatchMethod.UEI
    recipient_id = _optional_text(result, "recipient_id")
    if recipient_id is not None:
        entity_id = resolve_entity_id(
            registry,
            scheme=EntityIdentifierScheme.USA_SPENDING_RECIPIENT_ID,
            value=recipient_id,
            as_of=as_of,
        )
        if entity_id in reachable_ids:
            return entity_id, ContractMatchMethod.RECIPIENT_ID

    recipient_name = _optional_text(result, "Recipient Name")
    if recipient_name is None:
        return None
    normalized_recipient = _normalize_name(recipient_name)
    matches: set[str] = set()
    for entity in registry.entities:
        if entity.entity_id not in reachable_ids:
            continue
        for reviewed_name in (entity.name, *entity.aliases):
            if _normalize_name(reviewed_name) == normalized_recipient:
                matches.add(entity.entity_id)
    if len(matches) == 1:
        return matches.pop(), ContractMatchMethod.REVIEWED_NAME
    return None


def _award_search_request(
    *,
    search_term: str,
    start_date: date,
    end_date: date,
    page: int,
    limit: int,
) -> dict[str, Any]:
    return {
        "subawards": False,
        "spending_level": "awards",
        "page": page,
        "limit": limit,
        "sort": "Award Amount",
        "order": "desc",
        "filters": {
            "award_type_codes": list(CONTRACT_AWARD_TYPE_CODES),
            "recipient_search_text": [search_term],
            "time_period": [
                {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                }
            ],
        },
        "fields": list(AWARD_FIELDS),
    }


def _transaction_search_request(
    *,
    award_key: str,
    page: int,
    limit: int,
) -> dict[str, Any]:
    return {
        "award_id": award_key,
        "page": page,
        "limit": limit,
        "sort": "action_date",
        "order": "desc",
    }


def _normalize_name(value: str) -> str:
    return " ".join(_NORMALIZE_NAME_PATTERN.sub(" ", value.upper()).split())


def _required_text(payload: Mapping[str, Any], key: str) -> str:
    value = _optional_text(payload, key)
    if value is None:
        raise USAspendingResponseError(f"USAspending award is missing {key}")
    return value


def _optional_text(payload: Mapping[str, Any], key: str) -> str | None:
    raw = payload.get(key)
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _required_decimal(payload: Mapping[str, Any], key: str) -> Decimal:
    value = _optional_decimal(payload, key)
    if value is None:
        raise USAspendingResponseError(f"USAspending award is missing {key}")
    return value


def _optional_decimal(payload: Mapping[str, Any], key: str) -> Decimal | None:
    raw = payload.get(key)
    if raw in (None, ""):
        return None
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise USAspendingResponseError(f"USAspending award has invalid {key}") from exc
    if not value.is_finite():
        raise USAspendingResponseError(f"USAspending award has non-finite {key}")
    return value


def _required_date(payload: Mapping[str, Any], key: str) -> date:
    value = _optional_date(payload, key)
    if value is None:
        raise USAspendingResponseError(f"USAspending award is missing {key}")
    return value


def _optional_date(payload: Mapping[str, Any], key: str) -> date | None:
    raw = payload.get(key)
    if raw in (None, ""):
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError as exc:
        raise USAspendingResponseError(f"USAspending award has invalid {key}") from exc


def _optional_datetime(payload: Mapping[str, Any], key: str) -> datetime | None:
    raw = payload.get(key)
    if raw in (None, ""):
        return None
    value = str(raw)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        parsed_date = _optional_date(payload, key)
        return None if parsed_date is None else datetime.combine(parsed_date, time.min, tzinfo=UTC)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _code_and_description(value: object, *, digits: bool) -> tuple[str | None, str | None]:
    if value in (None, ""):
        return None, None
    if isinstance(value, Mapping):
        raw_code = value.get("code")
        raw_description = value.get("description")
        code = "" if raw_code is None else str(raw_code).strip().upper()
        description = None if raw_description in (None, "") else str(raw_description).strip()
        fallback_description = json.dumps(value, sort_keys=True, default=str)
    else:
        text = str(value).strip()
        match = re.match(r"^([A-Za-z0-9]+)(?:\s*[-:]\s*(.*))?$", text)
        if match is None:
            return None, text
        code = match.group(1).upper()
        description = (match.group(2) or "").strip() or None
        fallback_description = text
    if digits and (not code.isdigit() or not 2 <= len(code) <= 6):
        return None, fallback_description
    if not digits and len(code) > 8:
        return None, fallback_description
    return code, description
