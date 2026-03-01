from __future__ import annotations

import re
from datetime import UTC, datetime

from app.models import Direction, FlightSnapshot

FLIGHT_NUMBER_RE = re.compile(r"([A-Z0-9]{2,3})\s*[- ]?\s*(\d{1,4}[A-Z]?)")
ICAO_TO_IATA = {
    "AFL": "SU",
    "SDM": "FV",
    "SVR": "U6",
    "SBI": "S7",
    "UTA": "UT",
    "SSF": "D2",
    "PBD": "DP",
    "PGT": "PC",
}
IATA_TO_AIRLINE = {
    "SU": "Аэрофлот",
    "FV": "Россия",
    "D2": "Severstal Air Company",
    "S7": "S7 Airlines",
    "U6": "Уральские авиалинии",
    "DP": "Победа",
}


def normalize_flight_number(raw_flight_number: str) -> str:
    raw = (raw_flight_number or "").upper()
    # Keep only latin letters, digits, spaces and separators for regex extraction.
    cleaned = re.sub(r"[^A-Z0-9/ -]+", " ", raw)
    match = FLIGHT_NUMBER_RE.search(cleaned)
    if not match:
        compact = re.sub(r"[^A-Z0-9]+", "", cleaned)
        return compact[:16] if compact else "UNKNOWN"

    prefix = match.group(1)
    suffix = match.group(2)
    if len(prefix) == 3 and prefix in ICAO_TO_IATA:
        prefix = ICAO_TO_IATA[prefix]
    return f"{prefix}{suffix}"


def infer_airline(normalized_flight_number: str) -> tuple[str | None, str | None]:
    prefix = normalized_flight_number[:2] if len(normalized_flight_number) >= 2 else ""
    if prefix in IATA_TO_AIRLINE:
        return prefix, IATA_TO_AIRLINE[prefix]
    return (prefix or None, None)


def build_flight_instance_key(snapshot: FlightSnapshot) -> str:
    normalized_flight = snapshot.normalized_flight_number or normalize_flight_number(
        snapshot.flight_number
    )

    anchor_dt: datetime | None = (
        snapshot.scheduled_time or snapshot.estimated_time or snapshot.actual_time
    )
    if anchor_dt is not None:
        anchor = anchor_dt.astimezone(UTC).strftime("%Y%m%d%H%M")
    else:
        # Without schedule-like timestamps we intentionally bucket by date
        # to avoid duplicating the same flight from multiple parser variants.
        anchor = snapshot.source_timestamp.astimezone(UTC).strftime("%Y%m%d")

    return "|".join((normalized_flight, snapshot.direction.value, anchor))


def snapshot_completeness(snapshot: FlightSnapshot) -> int:
    score = 0
    for value in (
        snapshot.scheduled_time,
        snapshot.estimated_time,
        snapshot.actual_time,
        snapshot.aircraft_type,
        snapshot.terminal,
        snapshot.airline_name,
        snapshot.airline_iata,
    ):
        if value:
            score += 1
    return score


def choose_more_actual_snapshot(left: FlightSnapshot, right: FlightSnapshot) -> FlightSnapshot:
    def key(snapshot: FlightSnapshot) -> tuple[datetime, int, int]:
        update_time = snapshot.actual_time or snapshot.estimated_time or snapshot.scheduled_time
        if update_time is None:
            update_time = snapshot.source_timestamp
        return (
            update_time.astimezone(UTC),
            int(snapshot.source_priority),
            snapshot_completeness(snapshot),
        )

    return right if key(right) > key(left) else left


def ensure_snapshot_identity(snapshot: FlightSnapshot) -> FlightSnapshot:
    normalized = snapshot.normalized_flight_number or normalize_flight_number(
        snapshot.flight_number
    )
    airline_iata, airline_name = infer_airline(normalized)
    if not snapshot.airline_iata:
        snapshot.airline_iata = airline_iata
    if not snapshot.airline_name and airline_name:
        snapshot.airline_name = airline_name
    snapshot.normalized_flight_number = normalized
    return snapshot


def infer_direction_from_route(origin_icao: str | None, destination_icao: str | None) -> Direction:
    if (destination_icao or "").upper() == "UUEE":
        return Direction.ARR
    if (origin_icao or "").upper() == "UUEE":
        return Direction.DEP
    return Direction.DEP
