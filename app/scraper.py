from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
from bs4 import BeautifulSoup

from app.models import Direction, FlightSnapshot, FlightStatus


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
    }
    return mapping.get(normalized, FlightStatus.UNKNOWN)


def _parse_direction(value: str) -> Direction:
    normalized = value.strip().upper()
    if normalized in {"ARR", "ARRIVAL", "ПРИЛЕТ", "ПРИБЫТИЕ"}:
        return Direction.ARR
    return Direction.DEP


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


async def _fetch_flights_with_client(
    client: httpx.AsyncClient, url: str, source: str
) -> list[FlightSnapshot]:
    response = await client.get(url)
    response.raise_for_status()
    return parse_flights_html_auto(response.text, source=source)


async def fetch_flights(url: str, source: str, timeout_s: float = 15.0) -> list[FlightSnapshot]:
    async with httpx.AsyncClient(
        timeout=timeout_s,
        headers={"User-Agent": "scrapping-app/0.1 (+diploma research)"},
        follow_redirects=True,
    ) as client:
        return await _fetch_flights_with_client(client=client, url=url, source=source)


async def fetch_flights_multi(
    urls: list[str], source_prefix: str = "web_source", timeout_s: float = 15.0
) -> list[FlightSnapshot]:
    async with httpx.AsyncClient(
        timeout=timeout_s,
        headers={"User-Agent": "scrapping-app/0.1 (+diploma research)"},
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
