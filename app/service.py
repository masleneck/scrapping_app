from datetime import UTC, datetime

from app.models import FlightEvent, FlightSnapshot


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
