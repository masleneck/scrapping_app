from datetime import UTC, datetime

import pytest

from app.models import FlightEvent
from app.repository import SQLiteEventRepository


@pytest.mark.asyncio
async def test_repository_persists_and_lists_events(tmp_path) -> None:
    repo = SQLiteEventRepository(db_path=str(tmp_path / "events.db"))
    await repo.init()

    events = [
        FlightEvent(
            event_type="FLIGHT_DISCOVERED",
            flight_number="SU100",
            payload={"status": "SCHEDULED"},
            confidence_score=0.9,
            observed_at=datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
            source="test_source",
        ),
        FlightEvent(
            event_type="FLIGHT_STATUS_UPDATED",
            flight_number="SU245",
            payload={"status": "BOARDING"},
            confidence_score=0.85,
            observed_at=datetime(2026, 3, 1, 10, 5, tzinfo=UTC),
            source="test_source",
        ),
    ]

    saved_count = await repo.save_events(events)
    listed = await repo.list_events(limit=10)

    assert saved_count == 2
    assert len(listed) == 2
    assert listed[0].id > listed[1].id
    assert listed[0].flight_number == "SU245"
    assert listed[1].flight_number == "SU100"


@pytest.mark.asyncio
async def test_repository_filters_events(tmp_path) -> None:
    repo = SQLiteEventRepository(db_path=str(tmp_path / "events.db"))
    await repo.init()

    await repo.save_events(
        [
            FlightEvent(
                event_type="FLIGHT_DISCOVERED",
                flight_number="SU100",
                payload={"status": "SCHEDULED"},
                confidence_score=0.9,
                observed_at=datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
                source="test_source",
            ),
            FlightEvent(
                event_type="FLIGHT_STATUS_UPDATED",
                flight_number="SU100",
                payload={"status": "DELAYED"},
                confidence_score=0.85,
                observed_at=datetime(2026, 3, 1, 10, 1, tzinfo=UTC),
                source="test_source",
            ),
            FlightEvent(
                event_type="FLIGHT_DISCOVERED",
                flight_number="SU245",
                payload={"status": "SCHEDULED"},
                confidence_score=0.9,
                observed_at=datetime(2026, 3, 1, 10, 2, tzinfo=UTC),
                source="test_source",
            ),
        ]
    )

    su100_events = await repo.list_events(limit=10, flight_number="SU100")
    discovered_events = await repo.list_events(limit=10, event_type="FLIGHT_DISCOVERED")

    assert len(su100_events) == 2
    assert all(event.flight_number == "SU100" for event in su100_events)

    assert len(discovered_events) == 2
    assert all(event.event_type == "FLIGHT_DISCOVERED" for event in discovered_events)
