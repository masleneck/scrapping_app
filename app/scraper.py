from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from app.config import settings
from app.models import Direction, FlightSnapshot, FlightStatus

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
SCRAPER_USER_AGENT = "scrapping-app/0.1 (+diploma research)"
SCRAPER_HEADER_PROFILES: tuple[dict[str, str], ...] = (
    {
        "User-Agent": SCRAPER_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.5",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru,en-US;q=0.8,en;q=0.6",
        "Referer": "https://www.svo.aero/",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_7) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
        ),
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.8,en;q=0.6",
        "Referer": "https://rasp.yandex.ru/",
    },
)
ANTI_BOT_MARKERS = (
    "challenge validation",
    "challenge",
    "captcha",
    "cloudflare",
    "cf-chl",
    "access denied",
    "enable javascript",
)


@dataclass(frozen=True, slots=True)
class RealSourceConfig:
    source: str
    url: str
    strategy: str


REAL_SOURCE_CONFIGS: tuple[RealSourceConfig, ...] = (
    RealSourceConfig(
        source="svo_official",
        url="https://www.svo.aero/bitrix/timetable/",
        strategy="svo_bitrix",
    ),
    RealSourceConfig(
        source="kupibilet_aggregator",
        url="https://www.kupibilet.ru/gid/rossiia/moskva/sheremetevo/raspisanie",
        strategy="kupibilet_html",
    ),
    RealSourceConfig(
        source="yandex_rasp_aggregator",
        url="https://rasp.yandex.ru/station/9600213/",
        strategy="generic_html",
    ),
    RealSourceConfig(
        source="tripcom_aggregator",
        url="https://ru.trip.com/flights/status/svo/",
        strategy="generic_html",
    ),
)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None

    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None


def _parse_iso_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_local_dt(
    date_value: str | None,
    time_value: str | None,
    timezone_value: str | None,
) -> datetime | None:
    if not date_value or not time_value:
        return None
    try:
        parsed = datetime.strptime(f"{date_value} {time_value}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None
    try:
        tz = ZoneInfo(timezone_value) if timezone_value else MOSCOW_TZ
    except Exception:
        tz = UTC
    return parsed.replace(tzinfo=tz).astimezone(UTC)


def _parse_status(value: str) -> FlightStatus:
    normalized = value.strip().upper()
    mapping = {
        "SCHEDULED": FlightStatus.SCHEDULED,
        "DELAYED": FlightStatus.DELAYED,
        "LANDED": FlightStatus.LANDED,
        "BOARDING": FlightStatus.BOARDING,
        "DEPARTED": FlightStatus.DEPARTED,
        "ПО РАСПИСАНИЮ": FlightStatus.SCHEDULED,
        "ЗАДЕРЖИВАЕТСЯ": FlightStatus.DELAYED,
        "ПРИБЫЛ": FlightStatus.LANDED,
        "ПОСАДКА": FlightStatus.BOARDING,
        "ВЫЛЕТЕЛ": FlightStatus.DEPARTED,
        "ACTIVE": FlightStatus.SCHEDULED,
        "CANCELLED": FlightStatus.UNKNOWN,
        "UNKNOWN": FlightStatus.UNKNOWN,
    }
    if normalized in mapping:
        return mapping[normalized]

    fuzzy_mapping = (
        ("DELAY", FlightStatus.DELAYED),
        ("ЗАДЕРЖ", FlightStatus.DELAYED),
        ("BOARD", FlightStatus.BOARDING),
        ("ПОСАДК", FlightStatus.BOARDING),
        ("ARRIVED", FlightStatus.LANDED),
        ("ПРИБЫЛ", FlightStatus.LANDED),
        ("DEPARTED", FlightStatus.DEPARTED),
        ("ВЫЛЕТ", FlightStatus.DEPARTED),
        ("CANCEL", FlightStatus.UNKNOWN),
        ("ОТМЕН", FlightStatus.UNKNOWN),
    )
    for marker, status in fuzzy_mapping:
        if marker in normalized:
            return status
    return FlightStatus.UNKNOWN


def _parse_direction(value: str) -> Direction:
    normalized = value.strip().upper()
    if normalized in {"ARR", "ARRIVAL", "ПРИЛЕТ", "ПРИБЫТИЕ"}:
        return Direction.ARR
    return Direction.DEP


def _is_cancel_status(value: str) -> bool:
    normalized = value.strip().upper()
    return "CANCEL" in normalized or "ОТМЕН" in normalized


def _detect_anti_bot_markers(html: str) -> list[str]:
    body = html.lower()
    return [marker for marker in ANTI_BOT_MARKERS if marker in body]


def _extract_json_array_after_marker(payload: str, marker: str) -> str | None:
    marker_index = payload.find(marker)
    if marker_index < 0:
        return None

    array_start = payload.find("[", marker_index + len(marker))
    if array_start < 0:
        return None

    depth = 0
    in_string = False
    escaped = False

    for index in range(array_start, len(payload)):
        char = payload[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue
        if char == "[":
            depth += 1
            continue
        if char == "]":
            depth -= 1
            if depth == 0:
                return payload[array_start : index + 1]

    return None


def _build_svo_day_window(target_day: date | None = None) -> tuple[str, str]:
    day = target_day or datetime.now(MOSCOW_TZ).date()
    start = datetime(day.year, day.month, day.day, tzinfo=MOSCOW_TZ)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return start.isoformat(), end.isoformat()


def parse_svo_bitrix_payload(payload: dict[str, Any], source: str) -> list[FlightSnapshot]:
    snapshots: list[FlightSnapshot] = []
    source_ts = datetime.now(UTC)

    for item in payload.get("items", []):
        company_code = ((item.get("co") or {}).get("code") or "").strip()
        flight_number_raw = str(item.get("flt") or "").strip()
        if not flight_number_raw:
            continue

        flight_number = f"{company_code}{flight_number_raw}" if company_code else flight_number_raw
        direction = Direction.ARR if str(item.get("ad") or "").upper() == "A" else Direction.DEP

        scheduled_time = _parse_iso_dt(item.get("t_st"))
        estimated_time = (
            _parse_iso_dt(item.get("t_et"))
            or _parse_iso_dt(item.get("t_prb"))
            or _parse_iso_dt(item.get("t_otpr"))
        )
        actual_time = (
            _parse_iso_dt(item.get("t_at"))
            or _parse_iso_dt(item.get("t_otpr"))
            or _parse_iso_dt(item.get("t_prb"))
        )

        status_text = str(
            item.get("vip_status")
            or item.get("vip_status_rus")
            or item.get("status_id")
            or item.get("status_code")
            or "UNKNOWN"
        )
        status = _parse_status(status_text)
        if status == FlightStatus.UNKNOWN and not _is_cancel_status(status_text):
            if actual_time:
                status = (
                    FlightStatus.LANDED
                    if direction == Direction.ARR
                    else FlightStatus.DEPARTED
                )
            elif (
                estimated_time
                and scheduled_time
                and estimated_time - scheduled_time > timedelta(minutes=15)
            ):
                status = FlightStatus.DELAYED
            else:
                status = FlightStatus.SCHEDULED

        snapshots.append(
            FlightSnapshot(
                flight_number=flight_number,
                direction=direction,
                scheduled_time=scheduled_time,
                estimated_time=estimated_time,
                actual_time=actual_time,
                aircraft_type=item.get("aircraft_type_name"),
                terminal=item.get("term") or item.get("term_gate"),
                status=status,
                source=source,
                source_timestamp=source_ts,
            )
        )
    return snapshots


def parse_kupibilet_embedded_schedule(html: str, source: str) -> list[FlightSnapshot]:
    encoded_marker = 'ff.fetchScheduleQuery.$data\\":'
    encoded_end = ',\\"online-table/schedule\\":'
    encoded_schedule = None

    encoded_marker_index = html.find(encoded_marker)
    if encoded_marker_index >= 0:
        data_start = encoded_marker_index + len(encoded_marker)
        data_end = html.find(encoded_end, data_start)
        if data_end > data_start:
            encoded_schedule = html[data_start:data_end]

    if encoded_schedule is None:
        encoded_schedule = _extract_json_array_after_marker(
            html,
            'ff.fetchScheduleQuery.$data":',
        )
    if encoded_schedule is None:
        raise ValueError("kupibilet_schedule_not_found")

    decoded_schedule = encoded_schedule.replace('\\"', '"').replace("\\/", "/")
    records = json.loads(decoded_schedule)
    source_ts = datetime.now(UTC)
    snapshots: list[FlightSnapshot] = []

    for item in records:
        flights = item.get("flights") or []
        flight_number = None
        if flights:
            flight_number = flights[0].get("flight_number")
        if not flight_number:
            carrier = item.get("marketing_carrier") or item.get("operating_carrier") or ""
            number = item.get("number") or ""
            if carrier and number:
                flight_number = f"{carrier} {number}"
        if not flight_number:
            continue
        flight_number = flight_number.replace(" ", "")

        departure_airport = str(item.get("departure_airport") or "").upper()
        arrival_airport = str(item.get("arrival_airport") or "").upper()
        if arrival_airport == "SVO":
            direction = Direction.ARR
        elif departure_airport == "SVO":
            direction = Direction.DEP
        else:
            direction = Direction.DEP
        phase = "arrival" if direction == Direction.ARR else "departure"

        scheduled_time = _parse_local_dt(
            item.get(f"{phase}_date"),
            item.get(f"{phase}_time"),
            item.get(f"{phase}_timezone"),
        )
        estimated_time = _parse_local_dt(
            item.get(f"{phase}_date"),
            item.get(f"{phase}_estimated_time"),
            item.get(f"{phase}_timezone"),
        )
        actual_time = _parse_local_dt(
            item.get(f"{phase}_date"),
            item.get(f"{phase}_actual_time"),
            item.get(f"{phase}_timezone"),
        )

        raw_status = str(item.get("last_status") or "UNKNOWN")
        status = _parse_status(raw_status)
        if status == FlightStatus.UNKNOWN and not _is_cancel_status(raw_status):
            if actual_time:
                status = (
                    FlightStatus.LANDED
                    if direction == Direction.ARR
                    else FlightStatus.DEPARTED
                )
            else:
                status = FlightStatus.SCHEDULED

        snapshots.append(
            FlightSnapshot(
                flight_number=flight_number,
                direction=direction,
                scheduled_time=scheduled_time,
                estimated_time=estimated_time,
                actual_time=actual_time,
                aircraft_type=item.get("equipment"),
                terminal=item.get(f"{phase}_terminal"),
                status=status,
                source=source,
                source_timestamp=source_ts,
            )
        )

    return snapshots


def parse_flights_html(html: str, source: str) -> list[FlightSnapshot]:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table#flights tbody tr")

    snapshots: list[FlightSnapshot] = []
    for row in rows:
        cells = [cell.get_text(strip=True) for cell in row.find_all("td")]
        if len(cells) < 8:
            continue

        snapshots.append(
            FlightSnapshot(
                flight_number=cells[0],
                direction=_parse_direction(cells[1]),
                scheduled_time=_parse_dt(cells[2]),
                estimated_time=_parse_dt(cells[3]),
                actual_time=_parse_dt(cells[4]),
                aircraft_type=cells[5] or None,
                terminal=cells[6] or None,
                status=_parse_status(cells[7]),
                source=source,
                source_timestamp=datetime.now(UTC),
            )
        )

    return snapshots


def parse_flights_html_generic_table(html: str, source: str) -> list[FlightSnapshot]:
    soup = BeautifulSoup(html, "html.parser")

    for table in soup.find_all("table"):
        header_cells = [
            cell.get_text(strip=True).lower()
            for cell in table.select("thead tr th, tr th")
        ]
        if not header_cells:
            continue

        header_map: dict[str, int] = {}
        for index, header in enumerate(header_cells):
            if "рейс" in header or "flight" in header:
                header_map["flight_number"] = index
            elif "направ" in header or "direction" in header or "arr/dep" in header:
                header_map["direction"] = index
            elif "стат" in header or "status" in header:
                header_map["status"] = index
            elif "scheduled" in header or "план" in header:
                header_map["scheduled_time"] = index
            elif "estimated" in header or "оцен" in header:
                header_map["estimated_time"] = index
            elif "actual" in header or "факт" in header:
                header_map["actual_time"] = index
            elif "terminal" in header or "термин" in header:
                header_map["terminal"] = index
            elif "aircraft" in header or "самолет" in header:
                header_map["aircraft_type"] = index

        if "flight_number" not in header_map:
            continue

        snapshots: list[FlightSnapshot] = []
        for row in table.select("tbody tr"):
            cells = [cell.get_text(strip=True) for cell in row.find_all("td")]
            if not cells:
                continue

            def get(name: str) -> str | None:
                idx = header_map.get(name)
                if idx is None or idx >= len(cells):
                    return None
                return cells[idx]

            flight_number = get("flight_number")
            if not flight_number:
                continue

            snapshots.append(
                FlightSnapshot(
                    flight_number=flight_number,
                    direction=_parse_direction(get("direction") or "DEP"),
                    scheduled_time=_parse_dt(get("scheduled_time")),
                    estimated_time=_parse_dt(get("estimated_time")),
                    actual_time=_parse_dt(get("actual_time")),
                    aircraft_type=get("aircraft_type"),
                    terminal=get("terminal"),
                    status=_parse_status(get("status") or "UNKNOWN"),
                    source=source,
                    source_timestamp=datetime.now(UTC),
                )
            )

        if snapshots:
            return snapshots

    return []


def parse_flights_html_auto(html: str, source: str) -> list[FlightSnapshot]:
    snapshots = parse_flights_html(html=html, source=source)
    if snapshots:
        return snapshots
    return parse_flights_html_generic_table(html=html, source=source)


async def _request_with_retries(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    max_attempts: int | None = None,
    backoff_seconds: float | None = None,
) -> httpx.Response:
    attempts = max(1, max_attempts or settings.real_scrape_retry_attempts)
    backoff = max(0.0, backoff_seconds or settings.real_scrape_retry_backoff_seconds)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        headers = SCRAPER_HEADER_PROFILES[(attempt - 1) % len(SCRAPER_HEADER_PROFILES)]
        try:
            response = await client.get(url, params=params, headers=headers)
            if response.status_code in {429, 500, 502, 503, 504} and attempt < attempts:
                await asyncio.sleep(backoff * attempt)
                continue
            response.raise_for_status()
            return response
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_error = exc
            if attempt >= attempts:
                break
            await asyncio.sleep(backoff * attempt)

    if last_error is None:
        raise RuntimeError(f"request_failed_without_exception url={url}")
    raise last_error


async def _fetch_flights_with_client(
    client: httpx.AsyncClient, url: str, source: str
) -> list[FlightSnapshot]:
    response = await _request_with_retries(client=client, url=url)
    return parse_flights_html_auto(response.text, source=source)


async def fetch_flights(url: str, source: str, timeout_s: float = 15.0) -> list[FlightSnapshot]:
    async with httpx.AsyncClient(
        timeout=timeout_s,
        headers=SCRAPER_HEADER_PROFILES[0],
        follow_redirects=True,
    ) as client:
        return await _fetch_flights_with_client(client=client, url=url, source=source)


async def fetch_flights_multi(
    urls: list[str], source_prefix: str = "web_source", timeout_s: float = 15.0
) -> list[FlightSnapshot]:
    async with httpx.AsyncClient(
        timeout=timeout_s,
        headers=SCRAPER_HEADER_PROFILES[0],
        follow_redirects=True,
    ) as client:
        tasks = [
            _fetch_flights_with_client(client=client, url=url, source=f"{source_prefix}_{index}")
            for index, url in enumerate(urls, start=1)
        ]
        batches = await asyncio.gather(*tasks)

    snapshots: list[FlightSnapshot] = []
    for batch in batches:
        snapshots.extend(batch)
    return snapshots


async def _fetch_svo_bitrix_snapshots(
    client: httpx.AsyncClient,
    source: str,
    url: str,
    target_day: date | None = None,
    per_page: int = 100,
    max_pages: int = 20,
) -> list[FlightSnapshot]:
    start_date, end_date = _build_svo_day_window(target_day)
    snapshots: list[FlightSnapshot] = []

    for direction in ("departure", "arrival"):
        page = 1
        total_pages = 1
        while page <= total_pages and page <= max_pages:
            response = await _request_with_retries(
                client=client,
                url=url,
                params={
                    "direction": direction,
                    "startDate": start_date,
                    "endDate": end_date,
                    "perPage": per_page,
                    "page": page,
                },
            )
            payload = response.json()
            snapshots.extend(parse_svo_bitrix_payload(payload, source=source))
            total_pages = int((payload.get("pagination") or {}).get("pageCount") or 1)
            page += 1

    return snapshots


async def _fetch_kupibilet_snapshots(
    client: httpx.AsyncClient,
    source: str,
    url: str,
) -> tuple[list[FlightSnapshot], list[str]]:
    response = await _request_with_retries(client=client, url=url)
    markers = _detect_anti_bot_markers(response.text)
    snapshots = parse_kupibilet_embedded_schedule(response.text, source=source)
    return snapshots, markers


async def _fetch_generic_html_snapshots(
    client: httpx.AsyncClient,
    source: str,
    url: str,
) -> tuple[list[FlightSnapshot], list[str]]:
    response = await _request_with_retries(client=client, url=url)
    markers = _detect_anti_bot_markers(response.text)
    snapshots = parse_flights_html_auto(response.text, source=source)
    return snapshots, markers


async def _scrape_real_source(
    client: httpx.AsyncClient,
    config: RealSourceConfig,
) -> tuple[dict[str, Any], list[FlightSnapshot]]:
    report: dict[str, Any] = {
        "source": config.source,
        "url": config.url,
        "strategy": config.strategy,
        "status": "error",
        "snapshots": 0,
        "blocked_markers": [],
    }
    try:
        if config.strategy == "svo_bitrix":
            snapshots = await _fetch_svo_bitrix_snapshots(
                client=client,
                source=config.source,
                url=config.url,
            )
            report["status"] = "ok" if snapshots else "empty"
            report["snapshots"] = len(snapshots)
            return report, snapshots

        if config.strategy == "kupibilet_html":
            snapshots, markers = await _fetch_kupibilet_snapshots(
                client=client,
                source=config.source,
                url=config.url,
            )
            report["blocked_markers"] = markers
            if snapshots:
                report["status"] = "ok"
            elif markers:
                report["status"] = "blocked"
            else:
                report["status"] = "empty"
            report["snapshots"] = len(snapshots)
            return report, snapshots

        snapshots, markers = await _fetch_generic_html_snapshots(
            client=client,
            source=config.source,
            url=config.url,
        )
        report["blocked_markers"] = markers
        if snapshots:
            report["status"] = "ok"
        elif markers:
            report["status"] = "blocked"
        else:
            report["status"] = "empty"
        report["snapshots"] = len(snapshots)
        return report, snapshots
    except Exception as exc:
        report["error"] = str(exc)
        return report, []


async def fetch_flights_real_sources(
    timeout_s: float = 25.0,
) -> tuple[list[FlightSnapshot], list[dict[str, Any]]]:
    async with httpx.AsyncClient(
        timeout=timeout_s,
        headers=SCRAPER_HEADER_PROFILES[0],
        follow_redirects=True,
    ) as client:
        tasks = [
            _scrape_real_source(client=client, config=config)
            for config in REAL_SOURCE_CONFIGS
        ]
        batches = await asyncio.gather(*tasks)

    all_snapshots: list[FlightSnapshot] = []
    reports: list[dict[str, Any]] = []
    for report, snapshots in batches:
        reports.append(report)
        all_snapshots.extend(snapshots)
    return all_snapshots, reports
