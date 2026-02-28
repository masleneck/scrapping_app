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
    }
    return mapping.get(normalized, FlightStatus.UNKNOWN)


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
                direction=Direction.ARR if cells[1].upper() == "ARR" else Direction.DEP,
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


async def _fetch_flights_with_client(
    client: httpx.AsyncClient, url: str, source: str
) -> list[FlightSnapshot]:
    response = await client.get(url)
    response.raise_for_status()
    return parse_flights_html(response.text, source=source)


async def fetch_flights(url: str, source: str, timeout_s: float = 15.0) -> list[FlightSnapshot]:
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        return await _fetch_flights_with_client(client=client, url=url, source=source)


async def fetch_flights_multi(
    urls: list[str], source_prefix: str = "web_source", timeout_s: float = 15.0
) -> list[FlightSnapshot]:
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        tasks = [
            _fetch_flights_with_client(client=client, url=url, source=f"{source_prefix}_{index}")
            for index, url in enumerate(urls, start=1)
        ]
        batches = await asyncio.gather(*tasks)

    snapshots: list[FlightSnapshot] = []
    for batch in batches:
        snapshots.extend(batch)
    return snapshots
