from contextlib import asynccontextmanager
from datetime import UTC, datetime

import httpx
from fastapi import FastAPI, HTTPException, Query, status
from loguru import logger

from app.config import settings
from app.db import close_engine, create_engine, ping_database
from app.models import FlightEvent, FlightEventCreate, FlightEventUpdate
from app.repository import PostgresEventRepository
from app.scraper import fetch_flights, fetch_flights_multi
from app.service import snapshots_to_events


def _mask_database_url(database_url: str) -> str:
    if "@" not in database_url or "://" not in database_url:
        return database_url

    scheme, rest = database_url.split("://", 1)
    return f"{scheme}://***:***@{rest.split('@', 1)[1]}"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db_engine = create_engine(settings.database_url)
    await ping_database(db_engine)
    _app.state.db_engine = db_engine
    _app.state.event_repository = PostgresEventRepository(db_engine)

    logger.info(
        "Application startup completed. "
        "source_url={source_url} source_urls={source_urls} database_url={database_url}",
        source_url=settings.source_url,
        source_urls=settings.get_source_urls(),
        database_url=_mask_database_url(settings.database_url),
    )
    yield
    await close_engine(db_engine)
    logger.info("Application shutdown completed.")


app = FastAPI(title="scrapping_app", version="0.3.0", lifespan=lifespan)


def get_event_repository() -> PostgresEventRepository:
    repository = getattr(app.state, "event_repository", None)
    if repository is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "repository_unavailable",
                "message": "Event repository is not initialized.",
            },
        )
    return repository


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "mode": "async", "source_url": settings.source_url}


@app.get("/flights/scrape")
async def scrape_flights() -> dict:
    try:
        snapshots = await fetch_flights(url=settings.source_url, source="web_source")
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "source_unavailable",
                "message": str(exc),
                "source_url": settings.source_url,
            },
        ) from exc

    events = await snapshots_to_events(snapshots)
    persisted_events = await get_event_repository().save_events(events)

    return {
        "source_url": settings.source_url,
        "counts": {
            "snapshots": len(snapshots),
            "events": len(events),
            "persisted_events": persisted_events,
        },
        "snapshots": [snapshot.model_dump(mode="json") for snapshot in snapshots],
        "events": [event.model_dump(mode="json") for event in events],
    }


@app.get("/flights/scrape-all")
async def scrape_all_flights() -> dict:
    source_urls = settings.get_source_urls()
    try:
        snapshots = await fetch_flights_multi(urls=source_urls, source_prefix="web_source")
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "source_unavailable",
                "message": str(exc),
                "source_urls": source_urls,
            },
        ) from exc

    events = await snapshots_to_events(snapshots)
    persisted_events = await get_event_repository().save_events(events)

    return {
        "source_urls": source_urls,
        "counts": {
            "sources": len(source_urls),
            "snapshots": len(snapshots),
            "events": len(events),
            "persisted_events": persisted_events,
        },
        "snapshots": [snapshot.model_dump(mode="json") for snapshot in snapshots],
        "events": [event.model_dump(mode="json") for event in events],
    }


@app.get("/events")
async def list_events(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    flight_number: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    source: str | None = Query(default=None),
    observed_from: datetime | None = Query(default=None),
    observed_to: datetime | None = Query(default=None),
) -> dict:
    events = await get_event_repository().list_events(
        limit=limit,
        offset=offset,
        flight_number=flight_number,
        event_type=event_type,
        source=source,
        observed_from=observed_from,
        observed_to=observed_to,
    )
    return {
        "count": len(events),
        "events": [event.model_dump(mode="json") for event in events],
    }


@app.get("/events/{event_id}")
async def get_event(event_id: int) -> dict:
    event = await get_event_repository().get_event(event_id)
    if event is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "event_not_found", "event_id": event_id},
        )
    return {"event": event.model_dump(mode="json")}


@app.post("/events", status_code=status.HTTP_201_CREATED)
async def create_event(payload: FlightEventCreate) -> dict:
    event = FlightEvent(
        event_type=payload.event_type,
        flight_number=payload.flight_number,
        payload=payload.payload,
        confidence_score=payload.confidence_score,
        observed_at=payload.observed_at or datetime.now(UTC),
        source=payload.source,
    )
    created = await get_event_repository().create_event(event)
    return {"event": created.model_dump(mode="json")}


@app.patch("/events/{event_id}")
async def update_event(event_id: int, payload: FlightEventUpdate) -> dict:
    updated = await get_event_repository().update_event(
        event_id=event_id,
        event_type=payload.event_type,
        flight_number=payload.flight_number,
        payload=payload.payload,
        confidence_score=payload.confidence_score,
        observed_at=payload.observed_at,
        source=payload.source,
    )
    if updated is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "event_not_found", "event_id": event_id},
        )
    return {"event": updated.model_dump(mode="json")}


@app.delete("/events/{event_id}")
async def delete_event(event_id: int) -> dict:
    deleted = await get_event_repository().delete_event(event_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "event_not_found", "event_id": event_id},
        )
    return {"deleted": True, "event_id": event_id}
