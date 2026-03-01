from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.models import FlightEvent, FlightSnapshot

RMSEVENT_ADD = "RMSEVENT_ADD"
RMSEVENT_UPDATE = "RMSEVENT_UPDATE"
RMSEVENT_DELETE = "RMSEVENT_DELETE"
RMSEVENT_TYPES = {RMSEVENT_ADD, RMSEVENT_UPDATE, RMSEVENT_DELETE}


@dataclass(slots=True)
class RealSourceProcessingResult:
    events: list[FlightEvent]
    upserts: list[dict[str, Any]]
    deleted_keys: list[str]
    counts: dict[str, int]
    counts_by_source: dict[str, dict[str, int]]


def build_snapshot_event_key(snapshot: FlightSnapshot) -> str:
    flight_number = snapshot.flight_number.strip().upper()
    scheduled = snapshot.scheduled_time.isoformat() if snapshot.scheduled_time else "none"
    return "|".join((snapshot.source, flight_number, snapshot.direction.value, scheduled))


def _snapshot_payload(snapshot: FlightSnapshot) -> dict[str, Any]:
    payload = snapshot.model_dump(mode="json")
    # source_timestamp reflects fetch time, not semantic flight state.
    payload.pop("source_timestamp", None)
    return payload


def _snapshot_hash(snapshot_payload: dict[str, Any]) -> str:
    payload_json = json.dumps(snapshot_payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _ensure_source_bucket(counts_by_source: dict[str, dict[str, int]], source: str) -> None:
    if source not in counts_by_source:
        counts_by_source[source] = {"add": 0, "upd": 0, "del": 0, "unchanged": 0}


def snapshot_to_events(snapshot: FlightSnapshot) -> list[FlightEvent]:
    payload = snapshot.model_dump(mode="json")
    base = {
        "flight_number": snapshot.flight_number,
        "payload": payload,
        "observed_at": datetime.now(UTC),
        "source": snapshot.source,
    }

    return [
        FlightEvent(event_type="FLIGHT_DISCOVERED", confidence_score=0.9, **base),
        FlightEvent(event_type="FLIGHT_TIME_UPDATED", confidence_score=0.85, **base),
        FlightEvent(event_type="FLIGHT_STATUS_UPDATED", confidence_score=0.85, **base),
    ]


async def snapshots_to_events(snapshots: list[FlightSnapshot]) -> list[FlightEvent]:
    events: list[FlightEvent] = []
    for snapshot in snapshots:
        events.extend(snapshot_to_events(snapshot))
    return events


async def snapshots_to_real_source_events(
    snapshots: list[FlightSnapshot],
    previous_states: list[dict[str, Any]],
    deletable_sources: set[str],
    observed_at: datetime | None = None,
) -> RealSourceProcessingResult:
    observed_ts = observed_at or datetime.now(UTC)
    events: list[FlightEvent] = []
    upserts: list[dict[str, Any]] = []
    deleted_keys: list[str] = []
    counts = {"add": 0, "upd": 0, "del": 0, "unchanged": 0}
    counts_by_source: dict[str, dict[str, int]] = {}

    previous_map: dict[str, dict[str, Any]] = {}
    for state in previous_states:
        event_key = str(state["event_key"])
        previous_map[event_key] = state
        _ensure_source_bucket(counts_by_source, str(state["source"]))

    # Last snapshot wins for duplicate keys in a single run.
    current_map: dict[str, FlightSnapshot] = {}
    for snapshot in snapshots:
        current_map[build_snapshot_event_key(snapshot)] = snapshot
        _ensure_source_bucket(counts_by_source, snapshot.source)

    for event_key, snapshot in current_map.items():
        payload = _snapshot_payload(snapshot)
        new_hash = _snapshot_hash(payload)
        state = previous_map.get(event_key)

        upserts.append(
            {
                "event_key": event_key,
                "source": snapshot.source,
                "flight_number": snapshot.flight_number,
                "snapshot_payload": payload,
                "snapshot_hash": new_hash,
                "last_seen_at": observed_ts,
                "is_deleted": False,
            }
        )

        if state is None:
            events.append(
                FlightEvent(
                    event_type=RMSEVENT_ADD,
                    flight_number=snapshot.flight_number,
                    payload={"change": "add", "event_key": event_key, "snapshot": payload},
                    confidence_score=0.97,
                    observed_at=observed_ts,
                    source=snapshot.source,
                )
            )
            counts["add"] += 1
            counts_by_source[snapshot.source]["add"] += 1
            continue

        old_hash = str(state.get("snapshot_hash") or "")
        if old_hash == new_hash:
            counts["unchanged"] += 1
            counts_by_source[snapshot.source]["unchanged"] += 1
            continue

        events.append(
            FlightEvent(
                event_type=RMSEVENT_UPDATE,
                flight_number=snapshot.flight_number,
                payload={
                    "change": "upd",
                    "event_key": event_key,
                    "before": state.get("snapshot_payload") or {},
                    "after": payload,
                },
                confidence_score=0.94,
                observed_at=observed_ts,
                source=snapshot.source,
            )
        )
        counts["upd"] += 1
        counts_by_source[snapshot.source]["upd"] += 1

    current_keys = set(current_map.keys())
    for event_key, state in previous_map.items():
        source = str(state["source"])
        if event_key in current_keys or source not in deletable_sources:
            continue

        deleted_keys.append(event_key)
        old_payload = state.get("snapshot_payload") or {}
        events.append(
            FlightEvent(
                event_type=RMSEVENT_DELETE,
                flight_number=str(
                    state.get("flight_number") or old_payload.get("flight_number") or ""
                ),
                payload={"change": "del", "event_key": event_key, "snapshot": old_payload},
                confidence_score=0.92,
                observed_at=observed_ts,
                source=source,
            )
        )
        counts["del"] += 1
        counts_by_source[source]["del"] += 1

    return RealSourceProcessingResult(
        events=events,
        upserts=upserts,
        deleted_keys=deleted_keys,
        counts=counts,
        counts_by_source=counts_by_source,
    )
