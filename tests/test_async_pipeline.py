import pytest

from app.models import Direction, FlightSnapshot, FlightStatus
from app.scraper import RealSourceConfig, fetch_flights_multi, fetch_flights_real_sources
from app.service import (
    RMSEVENT_ADD,
    RMSEVENT_DELETE,
    RMSEVENT_UPDATE,
    snapshots_to_events,
    snapshots_to_real_source_events,
)


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


@pytest.mark.asyncio
async def test_fetch_flights_real_sources_collects_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_scrape(*, client, config):
        snapshots = [
            FlightSnapshot(
                flight_number=f"{config.source}-SU100",
                direction=Direction.DEP,
                status=FlightStatus.SCHEDULED,
                source=config.source,
                source_timestamp="2026-03-01T10:00:00Z",
            )
        ]
        return (
            {
                "source": config.source,
                "url": config.url,
                "strategy": config.strategy,
                "status": "ok",
                "snapshots": len(snapshots),
                "blocked_markers": [],
            },
            snapshots,
        )

    monkeypatch.setattr(
        "app.scraper.REAL_SOURCE_CONFIGS",
        (
            RealSourceConfig(source="s1", url="http://s1.local", strategy="mock"),
            RealSourceConfig(source="s2", url="http://s2.local", strategy="mock"),
        ),
    )
    monkeypatch.setattr("app.scraper._scrape_real_source", fake_scrape)

    snapshots, reports = await fetch_flights_real_sources(timeout_s=0.1)

    assert len(snapshots) == 2
    assert len(reports) == 2
    assert {report["source"] for report in reports} == {"s1", "s2"}


@pytest.mark.asyncio
async def test_snapshots_to_real_source_events_add_and_unchanged() -> None:
    snapshot = FlightSnapshot(
        flight_number="SU100",
        direction=Direction.DEP,
        status=FlightStatus.SCHEDULED,
        scheduled_time="2026-03-01T12:00:00+00:00",
        source="svo_official",
        source_timestamp="2026-03-01T12:00:00+00:00",
    )
    first = await snapshots_to_real_source_events(
        snapshots=[snapshot],
        previous_states=[],
        deletable_sources={"svo_official"},
    )
    assert first.counts["add"] == 1
    assert first.counts["upd"] == 0
    assert first.counts["del"] == 0
    assert len(first.events) == 1
    assert first.events[0].event_type == RMSEVENT_ADD

    previous_state = first.upserts[0]
    same_state_new_timestamp = FlightSnapshot(
        flight_number="SU100",
        direction=Direction.DEP,
        status=FlightStatus.SCHEDULED,
        scheduled_time="2026-03-01T12:00:00+00:00",
        source="svo_official",
        source_timestamp="2026-03-01T12:05:00+00:00",
    )
    second = await snapshots_to_real_source_events(
        snapshots=[same_state_new_timestamp],
        previous_states=[previous_state],
        deletable_sources={"svo_official"},
    )
    assert second.counts["add"] == 0
    assert second.counts["upd"] == 0
    assert second.counts["del"] == 0
    assert second.counts["unchanged"] == 1
    assert second.events == []


@pytest.mark.asyncio
async def test_snapshots_to_real_source_events_update_and_delete() -> None:
    old_snapshot = FlightSnapshot(
        flight_number="SU245",
        direction=Direction.DEP,
        status=FlightStatus.SCHEDULED,
        scheduled_time="2026-03-01T14:00:00+00:00",
        source="kupibilet_aggregator",
        source_timestamp="2026-03-01T14:00:00+00:00",
    )
    baseline = await snapshots_to_real_source_events(
        snapshots=[old_snapshot],
        previous_states=[],
        deletable_sources={"kupibilet_aggregator"},
    )
    baseline_state = baseline.upserts[0]

    updated_snapshot = FlightSnapshot(
        flight_number="SU245",
        direction=Direction.DEP,
        status=FlightStatus.DELAYED,
        scheduled_time="2026-03-01T14:00:00+00:00",
        source="kupibilet_aggregator",
        source_timestamp="2026-03-01T14:10:00+00:00",
    )
    updated = await snapshots_to_real_source_events(
        snapshots=[updated_snapshot],
        previous_states=[baseline_state],
        deletable_sources={"kupibilet_aggregator"},
    )
    assert updated.counts["upd"] == 1
    assert updated.events[0].event_type == RMSEVENT_UPDATE

    deleted = await snapshots_to_real_source_events(
        snapshots=[],
        previous_states=[updated.upserts[0]],
        deletable_sources={"kupibilet_aggregator"},
    )
    assert deleted.counts["del"] == 1
    assert deleted.events[0].event_type == RMSEVENT_DELETE
