from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic, sleep
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from portwatch.config import Settings
from portwatch.hiring_config import CareerSourceConfig, CareerSourceType
from portwatch.models import JobLocation, JobPosting

GREENHOUSE_JOBS_URL = "https://boards-api.greenhouse.io/v1/boards/{identifier}/jobs"
LEVER_POSTINGS_URL = "https://api.lever.co/v0/postings/{identifier}"
_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
_COUNTRY_CODES = {
    "united states": "US",
    "united states of america": "US",
    "usa": "US",
    "canada": "CA",
    "mexico": "MX",
    "brazil": "BR",
    "united kingdom": "GB",
    "uk": "GB",
    "germany": "DE",
    "france": "FR",
    "india": "IN",
    "china": "CN",
    "japan": "JP",
    "australia": "AU",
    "singapore": "SG",
    "switzerland": "CH",
    "netherlands": "NL",
    "belgium": "BE",
    "poland": "PL",
}


class CareerSourceError(RuntimeError):
    pass


class CareerSourceTransientError(RuntimeError):
    pass


@dataclass(frozen=True)
class CareerPage:
    page_number: int
    source_url: str
    raw_payload: bytes
    postings: tuple[JobPosting, ...]


@dataclass(frozen=True)
class CareerSnapshot:
    source_id: str
    ticker: str
    entity_id: str
    observed_at: datetime
    pages: tuple[CareerPage, ...]
    postings: tuple[JobPosting, ...]


@dataclass(frozen=True)
class CareerDetailBatch:
    pages: tuple[CareerPage, ...]
    postings: tuple[JobPosting, ...]
    records_skipped: int


class CareerSourceClient:
    """Rate-limited adapter for public Greenhouse, Lever, and company HTML feeds."""

    def __init__(
        self,
        settings: Settings,
        source: CareerSourceConfig,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep_fn: Callable[[float], None] = sleep,
        monotonic_fn: Callable[[], float] = monotonic,
        progress_fn: Callable[[str], None] | None = None,
    ) -> None:
        self.settings = settings
        self.source = source
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.progress_fn = progress_fn or (lambda message: None)
        self._last_request_at: float | None = None
        self._client = httpx.Client(
            timeout=settings.http_timeout_seconds,
            headers={
                "User-Agent": f"Mozilla/5.0 (compatible; {settings.user_agent})",
                "Accept": "application/json,text/html",
            },
            transport=transport,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def fetch_snapshot(self) -> CareerSnapshot:
        observed_at = datetime.now(UTC)
        if self.source.source_type is CareerSourceType.GREENHOUSE:
            pages = self._fetch_greenhouse(observed_at)
        elif self.source.source_type is CareerSourceType.LEVER:
            pages = self._fetch_lever(observed_at)
        else:
            pages = self._fetch_html(observed_at)

        postings_by_key: dict[str, JobPosting] = {}
        for page in pages:
            for posting in page.postings:
                existing = postings_by_key.get(posting.source_job_id)
                if existing is not None and existing != posting:
                    raise CareerSourceError(
                        f"conflicting snapshots returned for job {posting.source_job_id}"
                    )
                postings_by_key[posting.source_job_id] = posting
        return CareerSnapshot(
            source_id=self.source.source_id,
            ticker=self.source.ticker,
            entity_id=self.source.entity_id,
            observed_at=observed_at,
            pages=tuple(pages),
            postings=tuple(postings_by_key.values()),
        )

    def fetch_details(
        self,
        postings: Sequence[JobPosting],
        *,
        limit: int,
    ) -> CareerDetailBatch:
        """Enrich a bounded set of HTML-list jobs from first-party JSON-LD detail pages."""
        if not self.source.detail_json_ld or limit <= 0:
            return CareerDetailBatch((), (), 0)
        pages: list[CareerPage] = []
        enriched: list[JobPosting] = []
        skipped = 0
        selected = list(postings[:limit])
        for position, posting in enumerate(selected, start=1):
            self.progress_fn(
                f"{self.source.source_id}: detail {position}/{len(selected)} "
                f"({posting.source_job_id})"
            )
            try:
                payload = self._get(posting.source_url)
                detailed = parse_job_detail_payload(payload, posting)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in {404, 410}:
                    self.progress_fn(
                        f"{self.source.source_id}: detail enrichment stopped after HTTP "
                        f"{exc.response.status_code}"
                    )
                    break
                skipped += 1
                continue
            except CareerSourceError as exc:
                self.progress_fn(
                    f"{self.source.source_id}: skipped malformed detail "
                    f"{posting.source_job_id}: {exc}"
                )
                skipped += 1
                continue
            except (
                httpx.TimeoutException,
                httpx.NetworkError,
                CareerSourceTransientError,
            ) as exc:
                self.progress_fn(
                    f"{self.source.source_id}: detail enrichment stopped after network error: {exc}"
                )
                break
            enriched.append(detailed)
            pages.append(CareerPage(position, posting.source_url, payload, (detailed,)))
        return CareerDetailBatch(tuple(pages), tuple(enriched), skipped)

    def _fetch_greenhouse(self, observed_at: datetime) -> list[CareerPage]:
        identifier = self.source.api_identifier or ""
        url = GREENHOUSE_JOBS_URL.format(identifier=identifier)
        payload = self._get(url, params={"content": "true"})
        postings = parse_greenhouse_payload(payload, self.source, observed_at)
        return [CareerPage(1, url, payload, tuple(postings))]

    def _fetch_lever(self, observed_at: datetime) -> list[CareerPage]:
        identifier = self.source.api_identifier or ""
        url = LEVER_POSTINGS_URL.format(identifier=identifier)
        pages: list[CareerPage] = []
        maximum = self.source.max_pages or self.settings.careers_max_pages
        for page_number in range(maximum):
            skip = page_number * self.source.page_size
            self.progress_fn(f"{self.source.source_id}: Lever page {page_number + 1}")
            payload = self._get(
                url,
                params={"mode": "json", "skip": str(skip), "limit": str(self.source.page_size)},
            )
            postings = parse_lever_payload(payload, self.source, observed_at)
            pages.append(CareerPage(page_number + 1, url, payload, tuple(postings)))
            if len(postings) < self.source.page_size:
                return pages
        raise CareerSourceError(
            f"{self.source.source_id} reached its Lever pagination limit before completion"
        )

    def _fetch_html(self, observed_at: datetime) -> list[CareerPage]:
        first_page_number = self.source.first_page
        first_url = _page_url(
            self.source.base_url,
            self.source.page_parameter,
            first_page_number,
        )
        self.progress_fn(f"{self.source.source_id}: career page {first_page_number}")
        first_payload = self._get(first_url)
        first_postings, last_page = parse_html_page(first_payload, self.source, observed_at)
        maximum = self.source.max_pages or self.settings.careers_max_pages
        page_count = last_page - first_page_number + 1
        if page_count < 1:
            raise CareerSourceError("career pagination ended before its configured first page")
        if page_count > maximum:
            raise CareerSourceError(
                f"{self.source.source_id} requires {page_count} pages, above limit {maximum}"
            )
        pages = [CareerPage(first_page_number, first_url, first_payload, tuple(first_postings))]
        first_page_job_ids = {posting.source_job_id for posting in first_postings}
        page_number = first_page_number + 1
        target_last_page = last_page
        while page_number <= target_last_page:
            self.progress_fn(
                f"{self.source.source_id}: career page {page_number}/{target_last_page}"
            )
            url = _page_url(self.source.base_url, self.source.page_parameter, page_number)
            payload = self._get(url)
            postings, reported_last_page = parse_html_page(payload, self.source, observed_at)
            if not postings:
                raise CareerSourceError(
                    f"{self.source.source_id} returned an empty page before pagination completed"
                )
            page_job_ids = {posting.source_job_id for posting in postings}
            if page_job_ids == first_page_job_ids:
                raise CareerSourceError(
                    f"{self.source.source_id} repeated its first result page; "
                    "the source may be ignoring pagination"
                )
            if reported_last_page > target_last_page:
                expanded_page_count = reported_last_page - first_page_number + 1
                if expanded_page_count > maximum:
                    raise CareerSourceError(
                        f"{self.source.source_id} grew above its pagination limit during the crawl"
                    )
                target_last_page = reported_last_page
            pages.append(CareerPage(page_number, url, payload, tuple(postings)))
            page_number += 1
        return pages

    @retry(
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.NetworkError, CareerSourceTransientError)
        ),
        wait=wait_exponential_jitter(initial=1, max=10),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _get(self, url: str, *, params: Mapping[str, str] | None = None) -> bytes:
        self._rate_limit()
        response = self._client.get(url, params=params)
        if response.status_code in _TRANSIENT_STATUS_CODES:
            raise CareerSourceTransientError(
                f"{self.source.source_id} returned transient HTTP {response.status_code}"
            )
        response.raise_for_status()
        return response.content

    def _rate_limit(self) -> None:
        now = self.monotonic_fn()
        if self._last_request_at is not None:
            remaining = self.settings.careers_request_interval_seconds - (
                now - self._last_request_at
            )
            if remaining > 0:
                self.sleep_fn(remaining)
        self._last_request_at = self.monotonic_fn()


def parse_greenhouse_payload(
    payload: bytes,
    source: CareerSourceConfig,
    ingested_at: datetime,
) -> list[JobPosting]:
    data = _json_object(payload, "Greenhouse")
    raw_jobs = data.get("jobs")
    if not isinstance(raw_jobs, Sequence) or isinstance(raw_jobs, (str, bytes)):
        raise CareerSourceError("Greenhouse payload is missing a jobs array")
    postings: list[JobPosting] = []
    for raw in raw_jobs:
        if not isinstance(raw, Mapping):
            raise CareerSourceError("Greenhouse job is not an object")
        location = raw.get("location")
        location_text = location.get("name") if isinstance(location, Mapping) else None
        departments = raw.get("departments")
        department_names = _mapping_names(departments)
        content = raw.get("content")
        postings.append(
            _posting(
                source=source,
                source_job_id=str(_required(raw, "id", "Greenhouse job")),
                title=str(_required(raw, "title", "Greenhouse job")),
                description=_html_text(content) if isinstance(content, str) else "",
                department="; ".join(department_names) or None,
                team=None,
                employment_type=None,
                workplace_type=None,
                posted_at=_parse_datetime(raw.get("updated_at")),
                source_url=str(_required(raw, "absolute_url", "Greenhouse job")),
                location_text=str(location_text or "Unspecified"),
                ingested_at=ingested_at,
            )
        )
    return postings


def parse_lever_payload(
    payload: bytes,
    source: CareerSourceConfig,
    ingested_at: datetime,
) -> list[JobPosting]:
    try:
        data = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CareerSourceError("Lever returned invalid JSON") from exc
    if not isinstance(data, list):
        raise CareerSourceError("Lever payload is not an array")
    postings: list[JobPosting] = []
    for raw in data:
        if not isinstance(raw, Mapping):
            raise CareerSourceError("Lever job is not an object")
        categories = raw.get("categories")
        category_map = categories if isinstance(categories, Mapping) else {}
        created = raw.get("createdAt")
        posted_at = (
            datetime.fromtimestamp(float(created) / 1000, tz=UTC)
            if isinstance(created, (int, float))
            else None
        )
        description_parts = [
            value
            for value in (raw.get("descriptionPlain"), raw.get("additionalPlain"))
            if isinstance(value, str)
        ]
        postings.append(
            _posting(
                source=source,
                source_job_id=str(_required(raw, "id", "Lever job")),
                title=str(_required(raw, "text", "Lever job")),
                description="\n".join(description_parts),
                department=_optional_string(category_map.get("department")),
                team=_optional_string(category_map.get("team")),
                employment_type=_optional_string(category_map.get("commitment")),
                workplace_type=_optional_string(raw.get("workplaceType")),
                posted_at=posted_at,
                source_url=str(_required(raw, "hostedUrl", "Lever job")),
                location_text=str(category_map.get("location") or "Unspecified"),
                ingested_at=ingested_at,
            )
        )
    return postings


def parse_html_page(
    payload: bytes,
    source: CareerSourceConfig,
    ingested_at: datetime,
) -> tuple[list[JobPosting], int]:
    try:
        html = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CareerSourceError("career HTML was not UTF-8") from exc
    soup = BeautifulSoup(html, "html.parser")
    postings: list[JobPosting] = []
    for card in soup.select(source.job_selector or ""):
        title_node = card.select_one(source.title_selector or "")
        link_node = card.select_one(source.link_selector or "")
        location_node = card.select_one(source.location_selector or "")
        if title_node is None or link_node is None or location_node is None:
            raise CareerSourceError("career job card is missing a configured field")
        href = link_node.get("href")
        if not isinstance(href, str) or not href.strip():
            raise CareerSourceError("career job card has no link")
        job_id: str | None = None
        if source.job_id_attribute:
            raw_id = card.get(source.job_id_attribute)
            job_id = raw_id if isinstance(raw_id, str) else None
        if not job_id:
            job_id = _job_id_from_url(href)
        department_node = (
            card.select_one(source.department_selector) if source.department_selector else None
        )
        postings.append(
            _posting(
                source=source,
                source_job_id=job_id,
                title=title_node.get_text(" ", strip=True),
                description="",
                department=(
                    department_node.get_text(" ", strip=True)
                    if department_node is not None
                    else None
                ),
                team=None,
                employment_type=None,
                workplace_type=None,
                posted_at=None,
                source_url=urljoin(source.base_url, href),
                location_text=location_node.get_text(" ", strip=True) or "Unspecified",
                ingested_at=ingested_at,
            )
        )
    if not postings:
        raise CareerSourceError("career page contained no jobs matching the configured selector")

    last_page = source.first_page
    last_node = soup.select_one(source.last_page_selector or "")
    if last_node is not None:
        href = last_node.get("href")
        if isinstance(href, str):
            query = dict(parse_qsl(urlsplit(urljoin(source.base_url, href)).query))
            raw_page = query.get(source.page_parameter)
            if raw_page is not None:
                try:
                    last_page = int(raw_page)
                except ValueError as exc:
                    raise CareerSourceError("career last-page link is not numeric") from exc
    return postings, last_page


def parse_job_detail_payload(payload: bytes, posting: JobPosting) -> JobPosting:
    """Read a schema.org JobPosting JSON-LD object without source-specific selectors."""
    try:
        html = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CareerSourceError("career detail HTML was not UTF-8") from exc
    soup = BeautifulSoup(html, "html.parser")
    job_data: Mapping[str, Any] | None = None
    for script in soup.select("script[type='application/ld+json']"):
        if not script.string:
            continue
        try:
            raw = json.loads(script.string)
        except json.JSONDecodeError:
            continue
        for candidate in _json_ld_objects(raw):
            candidate_type = candidate.get("@type")
            types = candidate_type if isinstance(candidate_type, list) else [candidate_type]
            if "JobPosting" in types:
                job_data = candidate
                break
        if job_data is not None:
            break
    if job_data is None:
        raise CareerSourceError("career detail has no schema.org JobPosting JSON-LD")

    raw_description = job_data.get("description")
    description_html = raw_description if isinstance(raw_description, str) else ""
    description = _html_text(description_html) if description_html else posting.description
    career_area = _career_area(description_html)
    employment = job_data.get("employmentType")
    employment_type: str | None
    if isinstance(employment, list):
        employment_type = "; ".join(str(value) for value in employment)
    else:
        employment_type = _optional_string(employment)
    return posting.model_copy(
        update={
            "title": str(job_data.get("title") or posting.title),
            "description": description,
            "department": career_area or posting.department,
            "employment_type": employment_type or posting.employment_type,
            "posted_at": _parse_datetime(job_data.get("datePosted")) or posting.posted_at,
        }
    )


def normalize_locations(raw_text: str, delimiter: str | None = None) -> tuple[JobLocation, ...]:
    raw_parts = raw_text.split(delimiter) if delimiter and delimiter in raw_text else [raw_text]
    unique_parts = list(dict.fromkeys(part.strip() for part in raw_parts if part.strip()))
    if not unique_parts:
        unique_parts = ["Unspecified"]
    return tuple(_normalize_location(part) for part in unique_parts)


def _normalize_location(raw_location: str) -> JobLocation:
    normalized = " ".join(raw_location.split())
    lower = normalized.casefold()
    is_remote = any(token in lower for token in ("remote", "home based", "home-based"))
    parts = [part.strip() for part in normalized.split(",") if part.strip()]
    country = parts[-1] if len(parts) >= 2 else None
    country_code = _COUNTRY_CODES.get(country.casefold()) if country else None
    city = parts[0] if len(parts) >= 2 else None
    region = parts[-2] if len(parts) >= 3 else None
    location_id = hashlib.sha256(lower.encode("utf-8")).hexdigest()
    return JobLocation(
        location_id=location_id,
        raw_location=normalized,
        city=city,
        region=region,
        country=country,
        country_code=country_code,
        is_remote=is_remote,
    )


def _posting(
    *,
    source: CareerSourceConfig,
    source_job_id: str,
    title: str,
    description: str,
    department: str | None,
    team: str | None,
    employment_type: str | None,
    workplace_type: str | None,
    posted_at: datetime | None,
    source_url: str,
    location_text: str,
    ingested_at: datetime,
) -> JobPosting:
    return JobPosting(
        source_id=source.source_id,
        source_job_id=source_job_id,
        ticker=source.ticker,
        entity_id=source.entity_id,
        title=title,
        description=description,
        department=department,
        team=team,
        employment_type=employment_type,
        workplace_type=workplace_type,
        posted_at=posted_at,
        source_url=source_url,
        locations=normalize_locations(location_text, source.location_delimiter),
        ingested_at=ingested_at,
    )


def _json_object(payload: bytes, source_name: str) -> Mapping[str, Any]:
    try:
        data = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CareerSourceError(f"{source_name} returned invalid JSON") from exc
    if not isinstance(data, Mapping):
        raise CareerSourceError(f"{source_name} payload is not an object")
    return data


def _required(mapping: Mapping[str, Any], key: str, context: str) -> Any:
    value = mapping.get(key)
    if value is None or value == "":
        raise CareerSourceError(f"{context} is missing {key}")
    return value


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _mapping_names(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [str(item["name"]) for item in value if isinstance(item, Mapping) and item.get("name")]


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _html_text(value: str) -> str:
    return BeautifulSoup(value, "html.parser").get_text(" ", strip=True)


def _json_ld_objects(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        graph = value.get("@graph")
        if isinstance(graph, list):
            return [item for item in graph if isinstance(item, Mapping)]
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _career_area(description_html: str) -> str | None:
    if not description_html:
        return None
    soup = BeautifulSoup(description_html, "html.parser")
    for label in soup.find_all(["strong", "b"]):
        if label.get_text(" ", strip=True).rstrip(":").casefold() != "career area":
            continue
        parent_text = label.parent.get_text(" ", strip=True) if label.parent else ""
        value = re.sub(r"^Career Area\s*:\s*", "", parent_text, flags=re.IGNORECASE).strip()
        if value:
            return value
        sibling = label.parent.next_sibling if label.parent else None
        while sibling is not None:
            sibling_text = (
                sibling.get_text(" ", strip=True)
                if hasattr(sibling, "get_text")
                else str(sibling).strip()
            )
            if sibling_text:
                return sibling_text
            sibling = sibling.next_sibling
    plain = soup.get_text(" ", strip=True)
    match = re.search(r"Career Area\s*:\s*([^:]{2,80}?)(?:Job Description\s*:|$)", plain, re.I)
    return match.group(1).strip() if match else None


def _job_id_from_url(url: str) -> str:
    path_parts = [part for part in urlsplit(url).path.split("/") if part]
    if not path_parts:
        raise CareerSourceError("could not derive a stable job id from its URL")
    for part in reversed(path_parts):
        match = re.search(r"(?i)(r\d{5,}|[a-f0-9]{8}-[a-f0-9-]{20,})", part)
        if match:
            return match.group(1)
    return path_parts[-1]


def _page_url(base_url: str, page_parameter: str, page_number: int) -> str:
    split = urlsplit(base_url)
    query = dict(parse_qsl(split.query, keep_blank_values=True))
    query[page_parameter] = str(page_number)
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(query), split.fragment))
