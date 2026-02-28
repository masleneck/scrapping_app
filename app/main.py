from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query
from loguru import logger

from app.config import settings
from app.repository import SQLiteEventRepository
from app.scraper import fetch_flights, fetch_flights_multi
from app.service import snapshots_to_events

event_repository = SQLiteEventRepository(db_path=settings.events_db_path)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await event_repository.init()
    logger.info(
        "Application startup completed. "
        "source_url={source_url} source_urls={source_urls} events_db_path={events_db_path}",
        source_url=settings.source_url,
        source_urls=settings.get_source_urls(),
        events_db_path=settings.events_db_path,
    )
    yield
    logger.info("Application shutdown completed.")


app = FastAPI(title="scrapping_app", version="0.2.1", lifespan=lifespan)


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
    persisted_events = await event_repository.save_events(events)

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
    persisted_events = await event_repository.save_events(events)

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
    flight_number: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
) -> dict:
    events = await event_repository.list_events(
        limit=limit,
        flight_number=flight_number,
        event_type=event_type,
    )
    return {
        "count": len(events),
        "events": [event.model_dump(mode="json") for event in events],
    }
