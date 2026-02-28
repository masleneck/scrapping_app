import pytest

from app.models import Direction, FlightSnapshot, FlightStatus
from app.scraper import fetch_flights_multi
from app.service import snapshots_to_events


@pytest.mark.asyncio
async def test_snapshots_to_events_async() -> None:
    snapshots = [
        FlightSnapshot(
            flight_number="SU100",
            direction=Direction.ARR,
            status=FlightStatus.DELAYED,
            source="test",
            source_timestamp="2026-03-01T10:00:00Z",
        )
    ]

    events = await snapshots_to_events(snapshots)

    assert len(events) == 3
    assert {event.event_type for event in events} == {
        "FLIGHT_DISCOVERED",
        "FLIGHT_TIME_UPDATED",
        "FLIGHT_STATUS_UPDATED",
    }


@pytest.mark.asyncio
async def test_fetch_flights_multi_uses_all_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(*args, **kwargs):
        source = kwargs.get("source")
        if source is None and len(args) >= 3:
            source = args[2]

        return [
            FlightSnapshot(
                flight_number=f"{source}-SU",
                direction=Direction.ARR,
                status=FlightStatus.SCHEDULED,
                source=source,
                source_timestamp="2026-03-01T10:00:00Z",
            )
        ]

    monkeypatch.setattr("app.scraper._fetch_flights_with_client", fake_fetch)

    snapshots = await fetch_flights_multi(
        urls=["http://source1.local/flights", "http://source2.local/flights"],
        source_prefix="src",
    )

    assert len(snapshots) == 2
    assert snapshots[0].source == "src_1"
    assert snapshots[1].source == "src_2"
