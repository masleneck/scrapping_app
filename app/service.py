from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.flight_identity import (
    build_flight_instance_key,
    choose_more_actual_snapshot,
    ensure_snapshot_identity,
)
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
    current_rows: list[dict[str, Any]]


def _snapshot_payload(snapshot: FlightSnapshot) -> dict[str, Any]:
    payload = snapshot.model_dump(mode="json")
    payload.pop("source_timestamp", None)
    payload.pop("source_priority", None)
    payload.pop("provider", None)
    payload.pop("parser_strategy", None)
    return payload


def _snapshot_semantic_hash(snapshot_payload: dict[str, Any]) -> str:
    payload_json = json.dumps(snapshot_payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _ensure_source_bucket(counts_by_source: dict[str, dict[str, int]], source: str) -> None:
    if source not in counts_by_source:
        counts_by_source[source] = {"add": 0, "upd": 0, "del": 0, "unchanged": 0}


def _build_current_row(
    snapshot: FlightSnapshot,
    *,
    flight_instance_key: str,
    snapshot_payload: dict[str, Any],
    observed_ts: datetime,
) -> dict[str, Any]:
    return {
        "flight_instance_key": flight_instance_key,
        "flight_number": snapshot.flight_number,
        "normalized_flight_number": snapshot.normalized_flight_number,
        "direction": snapshot.direction.value,
        "scheduled_time": snapshot.scheduled_time,
        "estimated_time": snapshot.estimated_time,
        "actual_time": snapshot.actual_time,
        "status": snapshot.status.value,
        "terminal": snapshot.terminal,
        "aircraft_type": snapshot.aircraft_type,
        "airline_name": snapshot.airline_name,
        "airline_iata": snapshot.airline_iata,
        "source": snapshot.source,
        "provider": snapshot.provider,
        "strategy": snapshot.parser_strategy,
        "source_priority": snapshot.source_priority,
        "source_timestamp": snapshot.source_timestamp,
        "info_url": snapshot.info_url,
        "payload": snapshot_payload,
        "updated_at": observed_ts,
    }


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
    *,
    allow_deletes: bool,
    observed_at: datetime | None = None,
) -> RealSourceProcessingResult:
    observed_ts = observed_at or datetime.now(UTC)
    events: list[FlightEvent] = []
    upserts: list[dict[str, Any]] = []
    deleted_keys: list[str] = []
    current_rows: list[dict[str, Any]] = []

    counts = {"add": 0, "upd": 0, "del": 0, "unchanged": 0}
    counts_by_source: dict[str, dict[str, int]] = {}

    previous_map: dict[str, dict[str, Any]] = {}
    for state in previous_states:
        event_key = str(state["event_key"])
        previous_map[event_key] = state
        _ensure_source_bucket(counts_by_source, str(state.get("source") or "unknown"))

    # 1) Normalize snapshots and collapse duplicates across sources by flight instance.
    normalized: list[FlightSnapshot] = [ensure_snapshot_identity(s) for s in snapshots]
    current_best_map: dict[str, FlightSnapshot] = {}
    for snapshot in normalized:
        event_key = build_flight_instance_key(snapshot)
        existing = current_best_map.get(event_key)
        if existing is None:
            current_best_map[event_key] = snapshot
        else:
            current_best_map[event_key] = choose_more_actual_snapshot(existing, snapshot)

    for snapshot in current_best_map.values():
        _ensure_source_bucket(counts_by_source, snapshot.source)

    # 2) Compare with previous state and emit add/upd/unchanged.
    for event_key, snapshot in current_best_map.items():
        payload = _snapshot_payload(snapshot)
        new_hash = _snapshot_semantic_hash(payload)
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
        current_rows.append(
            _build_current_row(
                snapshot,
                flight_instance_key=event_key,
                snapshot_payload=payload,
                observed_ts=observed_ts,
            )
        )

        if state is None:
            events.append(
                FlightEvent(
                    event_type=RMSEVENT_ADD,
                    flight_number=snapshot.flight_number,
                    payload={
                        "change": "add",
                        "event_key": event_key,
                        "snapshot": payload,
                    },
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

    # 3) Emit deletes only if at least one source was read successfully.
    if allow_deletes:
        current_keys = set(current_best_map.keys())
        for event_key, state in previous_map.items():
            if event_key in current_keys:
                continue

            source = str(state.get("source") or "unknown")
            _ensure_source_bucket(counts_by_source, source)
            deleted_keys.append(event_key)
            old_payload = state.get("snapshot_payload") or {}
            events.append(
                FlightEvent(
                    event_type=RMSEVENT_DELETE,
                    flight_number=str(
                        state.get("flight_number")
                        or old_payload.get("flight_number")
                        or ""
                    ),
                    payload={
                        "change": "del",
                        "event_key": event_key,
                        "snapshot": old_payload,
                    },
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
        current_rows=current_rows,
    )
