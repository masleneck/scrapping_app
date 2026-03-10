from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from sqlalchemy import text

from app.db import close_engine, create_engine, ping_database
from app.models import FlightEvent
from app.repository import PostgresEventRepository

pytestmark = pytest.mark.asyncio
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")


@pytest_asyncio.fixture
async def repository() -> PostgresEventRepository:
    if not TEST_DATABASE_URL:
        pytest.skip(
            "TEST_DATABASE_URL or DATABASE_URL is not set for repository integration tests."
        )

    engine = create_engine(TEST_DATABASE_URL)
    try:
        await ping_database(engine)
    except Exception as exc:  # pragma: no cover - depends on external infra
        await close_engine(engine)
        pytest.skip(f"PostgreSQL is unavailable for repository tests: {exc}")

    yield PostgresEventRepository(engine)
    await close_engine(engine)


async def _cleanup_by_source(repository: PostgresEventRepository, source: str) -> None:
    async with repository.engine.begin() as conn:
        await conn.execute(
            text("DELETE FROM flight_events WHERE source = :source"),
            {"source": source},
        )


async def test_repository_save_and_filter(repository: PostgresEventRepository) -> None:
    source = f"pytest_save_{uuid.uuid4().hex}"
    events = [
        FlightEvent(
            event_type="FLIGHT_DISCOVERED",
            flight_number="SU100",
            payload={"status": "SCHEDULED"},
            confidence_score=0.9,
            observed_at=datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
            source=source,
        ),
        FlightEvent(
            event_type="FLIGHT_STATUS_UPDATED",
            flight_number="SU245",
            payload={"status": "BOARDING"},
            confidence_score=0.85,
            observed_at=datetime(2026, 3, 1, 10, 5, tzinfo=UTC),
            source=source,
        ),
    ]
    try:
        saved_count = await repository.save_events(events)
        listed = await repository.list_events(limit=10, source=source)
    finally:
        await _cleanup_by_source(repository, source)

    assert saved_count == 2
    assert len(listed) == 2
    assert listed[0].id > listed[1].id
    assert listed[0].flight_number == "SU245"
    assert listed[1].flight_number == "SU100"


async def test_repository_crud_flow(repository: PostgresEventRepository) -> None:
    source = f"pytest_crud_{uuid.uuid4().hex}"
    created: FlightEvent | None = None

    try:
        created = await repository.create_event(
            FlightEvent(
                event_type="FLIGHT_DISCOVERED",
                flight_number="SU777",
                payload={"status": "SCHEDULED"},
                confidence_score=0.8,
                observed_at=datetime(2026, 3, 1, 11, 0, tzinfo=UTC),
                source=source,
            )
        )
        fetched = await repository.get_event(created.id)
        updated = await repository.update_event(
            event_id=created.id,
            event_type="FLIGHT_STATUS_UPDATED",
            payload={"status": "DELAYED"},
            confidence_score=0.95,
        )
        deleted = await repository.delete_event(created.id)
        after_delete = await repository.get_event(created.id)
    finally:
        await _cleanup_by_source(repository, source)

    assert fetched is not None
    assert fetched.flight_number == "SU777"
    assert updated is not None
    assert updated.event_type == "FLIGHT_STATUS_UPDATED"
    assert updated.payload["status"] == "DELAYED"
    assert deleted is True
    assert after_delete is None


async def test_repository_stats(repository: PostgresEventRepository) -> None:
    source = f"pytest_stats_{uuid.uuid4().hex}"
    events = [
        FlightEvent(
            event_type="FLIGHT_DISCOVERED",
            flight_number="SU100",
            payload={"status": "SCHEDULED"},
            confidence_score=0.91,
            observed_at=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
            source=source,
        ),
        FlightEvent(
            event_type="FLIGHT_STATUS_UPDATED",
            flight_number="SU100",
            payload={"status": "DELAYED"},
            confidence_score=0.94,
            observed_at=datetime(2026, 3, 1, 12, 5, tzinfo=UTC),
            source=source,
        ),
        FlightEvent(
            event_type="FLIGHT_STATUS_UPDATED",
            flight_number="SU245",
            payload={"status": "BOARDING"},
            confidence_score=0.9,
            observed_at=datetime(2026, 3, 1, 12, 10, tzinfo=UTC),
            source=source,
        ),
    ]

    try:
        await repository.save_events(events)
        stats = await repository.get_events_stats(source=source, top_flights_limit=2)
    finally:
        await _cleanup_by_source(repository, source)

    assert stats["total_events"] == 3
    assert stats["by_event_type"][0]["event_type"] == "FLIGHT_STATUS_UPDATED"
    assert stats["top_flights"][0]["flight_number"] == "SU100"
