from __future__ import annotations

import asyncio
import contextlib
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from loguru import logger

from app.config import settings
from app.db import close_engine, create_engine, ping_database
from app.models import FlightEvent, FlightEventCreate, FlightEventUpdate
from app.repository import PostgresEventRepository
from app.scraper import fetch_flights, fetch_flights_multi, fetch_flights_real_sources
from app.service import snapshots_to_events, snapshots_to_real_source_events


def _mask_database_url(database_url: str) -> str:
    if "@" not in database_url or "://" not in database_url:
        return database_url

    scheme, rest = database_url.split("://", 1)
    return f"{scheme}://***:***@{rest.split('@', 1)[1]}"


def _default_source_change_counts() -> dict[str, int]:
    return {"add": 0, "upd": 0, "del": 0, "unchanged": 0}


async def _run_real_source_ingestion(
    *,
    repository: PostgresEventRepository,
    trigger: str,
    persist: bool,
) -> dict[str, Any]:
    lock: asyncio.Lock = app.state.real_scrape_lock

    async with lock:
        started_at = datetime.now(UTC)
        run_id = str(uuid.uuid4())

        snapshots, source_reports = await fetch_flights_real_sources()
        source_names = [str(report["source"]) for report in source_reports]
        previous_states = await repository.get_real_source_states(sources=source_names)
        allow_deletes = any(
            str(report.get("status")) in {"ok", "empty"} for report in source_reports
        )

        processed = await snapshots_to_real_source_events(
            snapshots=snapshots,
            previous_states=previous_states,
            allow_deletes=allow_deletes,
            observed_at=started_at,
        )

        persisted_events = 0
        current_upserted = 0
        current_deleted = 0
        if persist:
            persisted_events = await repository.save_events(processed.events)
            await repository.upsert_real_source_states(processed.upserts)
            await repository.mark_real_source_states_deleted(
                processed.deleted_keys,
                deleted_at=started_at,
            )
            current_upserted = await repository.upsert_current_flights(processed.current_rows)
            current_deleted = await repository.delete_current_flights(processed.deleted_keys)

        finished_at = datetime.now(UTC)

        source_reports_enriched: list[dict[str, Any]] = []
        run_reports: list[dict[str, Any]] = []
        for report in source_reports:
            source = str(report["source"])
            change_counts = processed.counts_by_source.get(
                source,
                _default_source_change_counts(),
            )
            enriched = {
                **report,
                "add_count": change_counts["add"],
                "upd_count": change_counts["upd"],
                "del_count": change_counts["del"],
                "unchanged_count": change_counts["unchanged"],
                "success": str(report.get("status")) in {"ok", "empty"},
            }
            source_reports_enriched.append(enriched)
            run_reports.append(
                {
                    "run_id": run_id,
                    "trigger": trigger,
                    "source": source,
                    "provider": str(report.get("provider") or ""),
                    "strategy": str(report.get("strategy") or ""),
                    "status": str(report.get("status") or "error"),
                    "snapshots": int(report.get("snapshots") or 0),
                    "blocked_markers": report.get("blocked_markers") or [],
                    "error": report.get("error"),
                    "add_count": change_counts["add"],
                    "upd_count": change_counts["upd"],
                    "del_count": change_counts["del"],
                    "unchanged_count": change_counts["unchanged"],
                    "success": str(report.get("status")) in {"ok", "empty"},
                    "started_at": started_at,
                    "finished_at": finished_at,
                }
            )

        if persist:
            await repository.save_real_source_run_reports(run_reports)

        sources_ok = sum(
            1 for report in source_reports_enriched if str(report["status"]) == "ok"
        )
        sources_empty = sum(
            1 for report in source_reports_enriched if str(report["status"]) == "empty"
        )
        sources_blocked = sum(
            1 for report in source_reports_enriched if str(report["status"]) == "blocked"
        )
        sources_error = sum(
            1 for report in source_reports_enriched if str(report["status"]) == "error"
        )

        logger.info(
            "rms_ingest trigger={trigger} success={success} add={add} upd={upd} del={del_count} "
            "unchanged={unchanged} snapshots={snapshots} events={events} "
            "current_upserted={current_upserted} current_deleted={current_deleted} "
            "sources_ok={sources_ok} sources_empty={sources_empty} "
            "sources_blocked={sources_blocked} sources_error={sources_error}",
            trigger=trigger,
            success=(sources_error == 0 and sources_blocked == 0),
            add=processed.counts["add"],
            upd=processed.counts["upd"],
            del_count=processed.counts["del"],
            unchanged=processed.counts["unchanged"],
            snapshots=len(snapshots),
            events=len(processed.events),
            current_upserted=current_upserted,
            current_deleted=current_deleted,
            sources_ok=sources_ok,
            sources_empty=sources_empty,
            sources_blocked=sources_blocked,
            sources_error=sources_error,
        )

        for report in source_reports_enriched:
            logger.info(
                "rms_source source={source} provider={provider} strategy={strategy} "
                "status={status} success={success} "
                "snapshots={snapshots} add={add} upd={upd} del={del_count} error={error}",
                source=report["source"],
                provider=report.get("provider"),
                strategy=report.get("strategy"),
                status=report["status"],
                success=report["success"],
                snapshots=report["snapshots"],
                add=report["add_count"],
                upd=report["upd_count"],
                del_count=report["del_count"],
                error=report.get("error"),
            )

        return {
            "run_id": run_id,
            "trigger": trigger,
            "started_at": started_at,
            "finished_at": finished_at,
            "sources": source_reports_enriched,
            "counts": {
                "sources_total": len(source_reports_enriched),
                "sources_ok": sources_ok,
                "sources_empty": sources_empty,
                "sources_blocked": sources_blocked,
                "sources_error": sources_error,
                "snapshots": len(snapshots),
                "events": len(processed.events),
                "persisted_events": persisted_events,
                "add": processed.counts["add"],
                "upd": processed.counts["upd"],
                "del": processed.counts["del"],
                "unchanged": processed.counts["unchanged"],
                "current_upserted": current_upserted,
                "current_deleted": current_deleted,
            },
        }


async def _real_source_scheduler_loop(_app: FastAPI, stop_event: asyncio.Event) -> None:
    interval_seconds = max(30, settings.real_scrape_interval_seconds)
    logger.info(
        "Real-source scheduler started. interval_seconds={interval_seconds}",
        interval_seconds=interval_seconds,
    )

    while not stop_event.is_set():
        try:
            await _run_real_source_ingestion(
                repository=_app.state.event_repository,
                trigger="scheduler",
                persist=True,
            )
        except Exception:
            logger.exception("Real-source scheduler cycle failed.")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue

    logger.info("Real-source scheduler stopped.")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    db_engine = create_engine(settings.database_url)
    await ping_database(db_engine)
    _app.state.db_engine = db_engine
    _app.state.event_repository = PostgresEventRepository(db_engine)
    _app.state.real_scrape_lock = asyncio.Lock()
    _app.state.real_scrape_stop_event = asyncio.Event()
    _app.state.real_scrape_task = None

    if settings.real_scrape_scheduler_enabled:
        _app.state.real_scrape_task = asyncio.create_task(
            _real_source_scheduler_loop(_app, _app.state.real_scrape_stop_event)
        )

    logger.info(
        "Application startup completed. "
        "source_url={source_url} source_urls={source_urls} database_url={database_url} "
        "real_scheduler_enabled={scheduler_enabled} real_scheduler_interval_s={interval_s}",
        source_url=settings.source_url,
        source_urls=settings.get_source_urls(),
        database_url=_mask_database_url(settings.database_url),
        scheduler_enabled=settings.real_scrape_scheduler_enabled,
        interval_s=settings.real_scrape_interval_seconds,
    )
    yield

    stop_event: asyncio.Event | None = getattr(_app.state, "real_scrape_stop_event", None)
    task: asyncio.Task | None = getattr(_app.state, "real_scrape_task", None)
    if stop_event is not None:
        stop_event.set()
    if task is not None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=10)
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    await close_engine(db_engine)
    logger.info("Application shutdown completed.")


app = FastAPI(title="scrapping_app", version="0.6.0", lifespan=lifespan)


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
          h2 { margin-top: 28px; }
          .stats { display: flex; gap: 16px; margin: 12px 0 20px; flex-wrap: wrap; }
          .card { border: 1px solid #ddd; border-radius: 8px; padding: 12px; min-width: 160px; }
          .filters { display: flex; gap: 8px; flex-wrap: wrap; margin: 12px 0; }
          .filters input, .filters select, .filters button { padding: 8px; }
          table { border-collapse: collapse; width: 100%; margin-top: 8px; }
          th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
          th { background: #f5f5f5; }
          code { background: #f0f0f0; padding: 2px 4px; border-radius: 4px; }
          .chart-wrap {
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 12px;
            margin-top: 8px;
          }
          canvas { width: 100%; height: 220px; }
        </style>
      </head>
      <body>
        <h1>Flight Data Dashboard</h1>
        <p>Актуальные рейсы, события RMSEVENT и сравнение парсеров по real-source.</p>

        <div class="stats">
          <div class="card"><div>Total events</div><strong id="total-events">...</strong></div>
          <div class="card">
            <div>Total current flights</div><strong id="current-total">...</strong>
          </div>
          <div class="card">
            <div>ADD / UPD / DEL (24h)</div><strong id="change-totals">...</strong>
          </div>
          <div class="card"><div>Total runs (24h)</div><strong id="real-runs">...</strong></div>
        </div>

        <h2>Поиск Актуальных Рейсов</h2>
        <div class="filters">
          <input id="q" placeholder="Номер рейса / авиакомпания" />
          <select id="status-filter"><option value="">Статус: все</option></select>
          <select id="provider-filter"><option value="">Источник: все</option></select>
          <button id="apply-filters">Применить</button>
        </div>
        <table>
          <thead>
            <tr>
              <th>Instance Key</th>
              <th>Flight</th>
              <th>Airline</th>
              <th>Direction</th>
              <th>Scheduled</th>
              <th>Status</th>
              <th>Provider/Strategy</th>
              <th>Info</th>
            </tr>
          </thead>
          <tbody id="current-body"></tbody>
        </table>

        <h2>График Статусов (Current)</h2>
        <div class="chart-wrap"><canvas id="status-chart" width="1200" height="220"></canvas></div>

        <h2>Сравнение Парсеров</h2>
        <table>
          <thead>
            <tr>
              <th>Provider</th>
              <th>Strategy</th>
              <th>Total runs</th>
              <th>Success rate</th>
              <th>Avg snapshots</th>
              <th>ADD</th>
              <th>UPD</th>
              <th>DEL</th>
            </tr>
          </thead>
          <tbody id="parser-body"></tbody>
        </table>

        <h2>Последние События</h2>
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
          function drawBarChart(canvasId, points) {
            const canvas = document.getElementById(canvasId);
            const ctx = canvas.getContext('2d');
            const w = canvas.width;
            const h = canvas.height;
            ctx.clearRect(0, 0, w, h);

            if (!points.length) {
              ctx.fillStyle = '#666';
              ctx.fillText('Нет данных', 16, 24);
              return;
            }

            const maxVal = Math.max(...points.map((p) => p.value), 1);
            const barW = Math.max(30, Math.floor((w - 40) / points.length) - 8);

            points.forEach((point, idx) => {
              const x = 20 + idx * (barW + 8);
              const barH = Math.round(((h - 45) * point.value) / maxVal);
              const y = h - 25 - barH;
              ctx.fillStyle = '#3b82f6';
              ctx.fillRect(x, y, barW, barH);
              ctx.fillStyle = '#222';
              ctx.font = '12px sans-serif';
              ctx.fillText(String(point.value), x, y - 4);
              ctx.fillText(point.label.slice(0, 12), x, h - 8);
            });
          }

          async function loadCurrentFlights() {
            const q = document.getElementById('q').value.trim();
            const status = document.getElementById('status-filter').value;
            const provider = document.getElementById('provider-filter').value;

            const params = new URLSearchParams({ limit: '100' });
            if (q) params.set('q', q);
            if (status) params.set('status', status);
            if (provider) params.set('provider', provider);

            const res = await fetch(`/flights/current?${params.toString()}`);
            const payload = await res.json();

            document.getElementById('current-total').textContent = payload.total;

            const statusSelect = document.getElementById('status-filter');
            const providerSelect = document.getElementById('provider-filter');
            const selectedStatus = statusSelect.value;
            const selectedProvider = providerSelect.value;

            statusSelect.innerHTML = '<option value="">Статус: все</option>';
            for (const item of payload.filters.statuses) {
              statusSelect.innerHTML += `<option value="${item}">${item}</option>`;
            }
            providerSelect.innerHTML = '<option value="">Источник: все</option>';
            for (const item of payload.filters.providers) {
              providerSelect.innerHTML += `<option value="${item}">${item}</option>`;
            }
            statusSelect.value = selectedStatus;
            providerSelect.value = selectedProvider;

            const body = document.getElementById('current-body');
            body.innerHTML = '';
            for (const row of payload.items) {
              const tr = document.createElement('tr');
              const info = row.info_url
                ? `<a href="${row.info_url}" target="_blank">info</a>`
                : '-';
              tr.innerHTML = `
                <td>${row.flight_instance_key}</td>
                <td>${row.normalized_flight_number}</td>
                <td>${row.airline_name ?? '-'}</td>
                <td>${row.direction}</td>
                <td>${row.scheduled_time ?? '-'}</td>
                <td>${row.status}</td>
                <td>${row.provider}/${row.strategy}</td>
                <td>${info}</td>
              `;
              body.appendChild(tr);
            }
            if (!payload.items.length) {
              body.innerHTML = '<tr><td colspan="8">Нет данных</td></tr>';
            }

            const statusMap = new Map();
            for (const row of payload.items) {
              const key = row.status || 'UNKNOWN';
              statusMap.set(key, (statusMap.get(key) || 0) + 1);
            }
            drawBarChart(
              'status-chart',
              [...statusMap.entries()].map(([label, value]) => ({ label, value }))
            );
          }

          async function loadGlobalStats() {
            const [statsRes, eventsRes, realRes] = await Promise.all([
              fetch('/events/stats'),
              fetch('/events?limit=20'),
              fetch('/events/real-stats?hours=24'),
            ]);
            const stats = await statsRes.json();
            const eventsPayload = await eventsRes.json();
            const real = await realRes.json();

            document.getElementById('total-events').textContent = stats.total_events;
            document.getElementById('real-runs').textContent = real.total_runs;

            const addTotal = real.changes_by_source.reduce((acc, row) => acc + row.add_total, 0);
            const updTotal = real.changes_by_source.reduce((acc, row) => acc + row.upd_total, 0);
            const delTotal = real.changes_by_source.reduce((acc, row) => acc + row.del_total, 0);
            document.getElementById('change-totals').textContent =
              `${addTotal} / ${updTotal} / ${delTotal}`;

            const parserBody = document.getElementById('parser-body');
            parserBody.innerHTML = '';
            for (const row of real.parser_performance) {
              const tr = document.createElement('tr');
              tr.innerHTML = `
                <td>${row.provider}</td>
                <td>${row.strategy}</td>
                <td>${row.total_runs}</td>
                <td>${(row.success_rate * 100).toFixed(1)}%</td>
                <td>${row.avg_snapshots}</td>
                <td>${row.add_total}</td>
                <td>${row.upd_total}</td>
                <td>${row.del_total}</td>
              `;
              parserBody.appendChild(tr);
            }
            if (!real.parser_performance.length) {
              parserBody.innerHTML = '<tr><td colspan="8">Нет данных</td></tr>';
            }

            const eventsBody = document.getElementById('events-body');
            eventsBody.innerHTML = '';
            for (const event of eventsPayload.events) {
              const tr = document.createElement('tr');
              tr.innerHTML = `
                <td>${event.id}</td>
                <td>${event.flight_number}</td>
                <td>${event.event_type}</td>
                <td>${event.payload?.snapshot?.status ?? event.payload?.status ?? '-'}</td>
                <td>${event.observed_at}</td>
                <td>${event.source}</td>
              `;
              eventsBody.appendChild(tr);
            }
            if (!eventsPayload.events.length) {
              eventsBody.innerHTML = '<tr><td colspan="6">Нет событий</td></tr>';
            }
          }

          async function initDashboard() {
            await Promise.all([loadCurrentFlights(), loadGlobalStats()]);
            document.getElementById('apply-filters').addEventListener('click', () => {
              loadCurrentFlights().catch(console.error);
            });
          }

          initDashboard().catch((err) => {
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


@app.get("/flights/scrape-real")
async def scrape_real_flights(
    persist: bool = Query(default=True),
) -> dict:
    return await _run_real_source_ingestion(
        repository=get_event_repository(),
        trigger="api",
        persist=persist,
    )


@app.get("/flights/current")
async def list_current_flights(
    q: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    provider: str | None = Query(default=None),
    source: str | None = Query(default=None),
    direction: str | None = Query(default=None),
    scheduled_from: datetime | None = Query(default=None),
    scheduled_to: datetime | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return await get_event_repository().list_current_flights(
        q=q,
        status=status_filter,
        provider=provider,
        source=source,
        direction=direction,
        scheduled_from=scheduled_from,
        scheduled_to=scheduled_to,
        limit=limit,
        offset=offset,
    )


@app.post("/admin/cleanup-real-data")
async def cleanup_real_data() -> dict:
    await get_event_repository().cleanup_real_data()
    return {"ok": True}


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
    return await get_event_repository().get_events_stats(
        flight_number=flight_number,
        event_type=event_type,
        source=source,
        observed_from=observed_from,
        observed_to=observed_to,
        top_flights_limit=top_flights_limit,
    )


@app.get("/events/real-stats")
async def get_real_source_stats(
    hours: int = Query(default=24, ge=1, le=24 * 30),
) -> dict:
    return await get_event_repository().get_real_source_stats(hours=hours)


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
