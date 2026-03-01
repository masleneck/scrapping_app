from contextlib import asynccontextmanager
from datetime import UTC, datetime

import httpx
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import HTMLResponse
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


app = FastAPI(title="scrapping_app", version="0.4.0", lifespan=lifespan)


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


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> str:
    return """
    <!DOCTYPE html>
    <html lang="ru">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>scrapping_app dashboard</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 24px; }
          h1 { margin-bottom: 8px; }
          .stats { display: flex; gap: 16px; margin: 12px 0 20px; }
          .card { border: 1px solid #ddd; border-radius: 8px; padding: 12px; min-width: 160px; }
          table { border-collapse: collapse; width: 100%; }
          th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
          th { background: #f5f5f5; }
          code { background: #f0f0f0; padding: 2px 4px; border-radius: 4px; }
        </style>
      </head>
      <body>
        <h1>Flight Events Dashboard</h1>
        <p>Быстрый просмотр текущих событий и агрегатов из PostgreSQL.</p>

        <div class="stats">
          <div class="card"><div>Total events</div><strong id="total-events">...</strong></div>
          <div class="card"><div>Top event type</div><strong id="top-type">...</strong></div>
          <div class="card"><div>Top source</div><strong id="top-source">...</strong></div>
        </div>

        <p><code>GET /events?limit=20</code></p>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Flight</th>
              <th>Type</th>
              <th>Status</th>
              <th>Observed</th>
              <th>Source</th>
            </tr>
          </thead>
          <tbody id="events-body"></tbody>
        </table>

        <script>
          async function loadDashboard() {
            const [statsRes, eventsRes] = await Promise.all([
              fetch('/events/stats'),
              fetch('/events?limit=20'),
            ]);
            const stats = await statsRes.json();
            const eventsPayload = await eventsRes.json();

            document.getElementById('total-events').textContent = stats.total_events;
            document.getElementById('top-type').textContent =
              stats.by_event_type[0]?.event_type || '-';
            document.getElementById('top-source').textContent =
              stats.by_source[0]?.source || '-';

            const body = document.getElementById('events-body');
            body.innerHTML = '';

            for (const event of eventsPayload.events) {
              const tr = document.createElement('tr');
              tr.innerHTML = `
                <td>${event.id}</td>
                <td>${event.flight_number}</td>
                <td>${event.event_type}</td>
                <td>${event.payload?.status ?? '-'}</td>
                <td>${event.observed_at}</td>
                <td>${event.source}</td>
              `;
              body.appendChild(tr);
            }
          }

          loadDashboard().catch((err) => {
            console.error(err);
            document.getElementById('events-body').innerHTML =
              '<tr><td colspan="6">Ошибка загрузки dashboard</td></tr>';
          });
        </script>
      </body>
    </html>
    """


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


@app.get("/flights/scrape-url")
async def scrape_flights_by_url(
    url: str = Query(..., min_length=10),
    source: str = Query(default="real_web_source"),
) -> dict:
    try:
        snapshots = await fetch_flights(url=url, source=source)
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail={"error": "source_unavailable", "message": str(exc), "source_url": url},
        ) from exc

    if not snapshots:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "source_parse_error",
                "message": "Страница загружена, но структура с рейсами не распознана.",
                "source_url": url,
            },
        )

    events = await snapshots_to_events(snapshots)
    persisted_events = await get_event_repository().save_events(events)

    return {
        "source_url": url,
        "source": source,
        "counts": {
            "snapshots": len(snapshots),
            "events": len(events),
            "persisted_events": persisted_events,
        },
        "snapshots": [snapshot.model_dump(mode="json") for snapshot in snapshots],
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


@app.get("/events/stats")
async def get_events_stats(
    flight_number: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    source: str | None = Query(default=None),
    observed_from: datetime | None = Query(default=None),
    observed_to: datetime | None = Query(default=None),
    top_flights_limit: int = Query(default=5, ge=1, le=50),
) -> dict:
    stats = await get_event_repository().get_events_stats(
        flight_number=flight_number,
        event_type=event_type,
        source=source,
        observed_from=observed_from,
        observed_to=observed_to,
        top_flights_limit=top_flights_limit,
    )
    return stats


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
