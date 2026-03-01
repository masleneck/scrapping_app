from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.models import FlightEvent, StoredFlightEvent

REAL_CHANGE_EVENT_TYPES = ("RMSEVENT_ADD", "RMSEVENT_UPDATE", "RMSEVENT_DELETE")


class PostgresEventRepository:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def save_events(self, events: list[FlightEvent]) -> int:
        if not events:
            return 0

        query = text(
            """
            INSERT INTO flight_events (
                event_type,
                flight_number,
                payload,
                confidence_score,
                observed_at,
                source
            )
            VALUES (
                :event_type,
                :flight_number,
                CAST(:payload_json AS JSONB),
                :confidence_score,
                :observed_at,
                :source
            )
            """
        )
        payload = [
            {
                "event_type": event.event_type,
                "flight_number": event.flight_number,
                "payload_json": json.dumps(event.payload, ensure_ascii=False),
                "confidence_score": event.confidence_score,
                "observed_at": event.observed_at,
                "source": event.source,
            }
            for event in events
        ]

        async with self.engine.begin() as conn:
            await conn.execute(query, payload)

        return len(events)

    async def get_real_source_states(
        self,
        sources: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        query = text(
            """
            SELECT
                event_key,
                source,
                flight_number,
                snapshot_payload::text AS snapshot_payload_json,
                snapshot_hash,
                last_seen_at,
                is_deleted
            FROM real_source_state
            WHERE is_deleted = FALSE
            """
        )
        async with self.engine.connect() as conn:
            rows = (await conn.execute(query)).mappings().all()

        source_filter = set(sources or [])
        states: list[dict[str, Any]] = []
        for row in rows:
            source = str(row["source"])
            if source_filter and source not in source_filter:
                continue
            snapshot_payload = row["snapshot_payload_json"]
            if isinstance(snapshot_payload, str):
                payload_data = json.loads(snapshot_payload)
            else:
                payload_data = snapshot_payload
            states.append(
                {
                    "event_key": row["event_key"],
                    "source": source,
                    "flight_number": row["flight_number"],
                    "snapshot_payload": payload_data,
                    "snapshot_hash": row["snapshot_hash"],
                    "last_seen_at": row["last_seen_at"],
                    "is_deleted": bool(row["is_deleted"]),
                }
            )
        return states

    async def upsert_real_source_states(self, states: list[dict[str, Any]]) -> int:
        if not states:
            return 0

        query = text(
            """
            INSERT INTO real_source_state (
                event_key,
                source,
                flight_number,
                snapshot_payload,
                snapshot_hash,
                last_seen_at,
                is_deleted,
                deleted_at
            )
            VALUES (
                :event_key,
                :source,
                :flight_number,
                CAST(:snapshot_payload_json AS JSONB),
                :snapshot_hash,
                :last_seen_at,
                :is_deleted,
                :deleted_at
            )
            ON CONFLICT (event_key)
            DO UPDATE SET
                source = EXCLUDED.source,
                flight_number = EXCLUDED.flight_number,
                snapshot_payload = EXCLUDED.snapshot_payload,
                snapshot_hash = EXCLUDED.snapshot_hash,
                last_seen_at = EXCLUDED.last_seen_at,
                is_deleted = EXCLUDED.is_deleted,
                deleted_at = EXCLUDED.deleted_at,
                updated_at = NOW()
            """
        )
        payload = [
            {
                "event_key": state["event_key"],
                "source": state["source"],
                "flight_number": state["flight_number"],
                "snapshot_payload_json": json.dumps(
                    state["snapshot_payload"], ensure_ascii=False
                ),
                "snapshot_hash": state["snapshot_hash"],
                "last_seen_at": state["last_seen_at"],
                "is_deleted": bool(state.get("is_deleted", False)),
                "deleted_at": (
                    state["last_seen_at"] if bool(state.get("is_deleted", False)) else None
                ),
            }
            for state in states
        ]

        async with self.engine.begin() as conn:
            await conn.execute(query, payload)
        return len(states)

    async def mark_real_source_states_deleted(
        self,
        event_keys: list[str],
        deleted_at: datetime | None = None,
    ) -> int:
        if not event_keys:
            return 0

        query = text(
            """
            UPDATE real_source_state
            SET
                is_deleted = TRUE,
                deleted_at = :deleted_at,
                updated_at = NOW()
            WHERE event_key = ANY(:event_keys) AND is_deleted = FALSE
            """
        )
        params = {"event_keys": event_keys, "deleted_at": deleted_at or datetime.now(UTC)}
        async with self.engine.begin() as conn:
            result = await conn.execute(query, params)
        return result.rowcount

    async def save_real_source_run_reports(self, reports: list[dict[str, Any]]) -> int:
        if not reports:
            return 0

        query = text(
            """
            INSERT INTO real_source_runs (
                run_id,
                trigger,
                source,
                status,
                snapshots,
                blocked_markers,
                error,
                add_count,
                upd_count,
                del_count,
                unchanged_count,
                success,
                started_at,
                finished_at
            )
            VALUES (
                :run_id,
                :trigger,
                :source,
                :status,
                :snapshots,
                CAST(:blocked_markers_json AS JSONB),
                :error,
                :add_count,
                :upd_count,
                :del_count,
                :unchanged_count,
                :success,
                :started_at,
                :finished_at
            )
            """
        )
        payload = [
            {
                "run_id": report["run_id"],
                "trigger": report["trigger"],
                "source": report["source"],
                "status": report["status"],
                "snapshots": int(report.get("snapshots") or 0),
                "blocked_markers_json": json.dumps(
                    report.get("blocked_markers") or [], ensure_ascii=False
                ),
                "error": report.get("error"),
                "add_count": int(report.get("add_count") or 0),
                "upd_count": int(report.get("upd_count") or 0),
                "del_count": int(report.get("del_count") or 0),
                "unchanged_count": int(report.get("unchanged_count") or 0),
                "success": bool(report.get("success", False)),
                "started_at": report["started_at"],
                "finished_at": report["finished_at"],
            }
            for report in reports
        ]
        async with self.engine.begin() as conn:
            await conn.execute(query, payload)
        return len(reports)

    async def get_real_source_stats(self, hours: int = 24) -> dict[str, Any]:
        window_hours = max(1, hours)
        params = {"hours": window_hours}

        async with self.engine.connect() as conn:
            total_row = (
                await conn.execute(
                    text(
                        """
                        SELECT COUNT(*)::bigint AS total_runs
                        FROM real_source_runs
                        WHERE finished_at >= NOW() - (:hours * INTERVAL '1 hour')
                        """
                    ),
                    params,
                )
            ).mappings().one()

            status_rows = (
                await conn.execute(
                    text(
                        """
                        SELECT status, COUNT(*)::bigint AS total
                        FROM real_source_runs
                        WHERE finished_at >= NOW() - (:hours * INTERVAL '1 hour')
                        GROUP BY status
                        ORDER BY total DESC, status ASC
                        """
                    ),
                    params,
                )
            ).mappings().all()

            latest_rows = (
                await conn.execute(
                    text(
                        """
                        SELECT DISTINCT ON (source)
                            source,
                            status,
                            snapshots,
                            add_count,
                            upd_count,
                            del_count,
                            unchanged_count,
                            success,
                            finished_at
                        FROM real_source_runs
                        ORDER BY source, finished_at DESC
                        """
                    )
                )
            ).mappings().all()

            timeline_rows = (
                await conn.execute(
                    text(
                        """
                        SELECT
                            date_trunc('hour', finished_at) AS bucket,
                            source,
                            status,
                            COUNT(*)::bigint AS total
                        FROM real_source_runs
                        WHERE finished_at >= NOW() - (:hours * INTERVAL '1 hour')
                        GROUP BY bucket, source, status
                        ORDER BY bucket ASC, source ASC, status ASC
                        """
                    ),
                    params,
                )
            ).mappings().all()

            change_rows = (
                await conn.execute(
                    text(
                        """
                        SELECT
                            source,
                            SUM(
                                CASE WHEN event_type = 'RMSEVENT_ADD' THEN 1 ELSE 0 END
                            )::bigint AS add_total,
                            SUM(
                                CASE WHEN event_type = 'RMSEVENT_UPDATE' THEN 1 ELSE 0 END
                            )::bigint AS upd_total,
                            SUM(
                                CASE WHEN event_type = 'RMSEVENT_DELETE' THEN 1 ELSE 0 END
                            )::bigint AS del_total
                        FROM flight_events
                        WHERE
                            observed_at >= NOW() - (:hours * INTERVAL '1 hour')
                            AND event_type = ANY(:event_types)
                        GROUP BY source
                        ORDER BY source ASC
                        """
                    ),
                    {**params, "event_types": list(REAL_CHANGE_EVENT_TYPES)},
                )
            ).mappings().all()

            active_rows = (
                await conn.execute(
                    text(
                        """
                        SELECT
                            source,
                            COALESCE(snapshot_payload->>'status', 'UNKNOWN') AS status,
                            COUNT(*)::bigint AS total
                        FROM real_source_state
                        WHERE is_deleted = FALSE
                        GROUP BY source, COALESCE(snapshot_payload->>'status', 'UNKNOWN')
                        ORDER BY source ASC, status ASC
                        """
                    )
                )
            ).mappings().all()

        return {
            "window_hours": window_hours,
            "total_runs": int(total_row["total_runs"]),
            "runs_by_status": [
                {"status": row["status"], "total": int(row["total"])}
                for row in status_rows
            ],
            "latest_by_source": [
                {
                    "source": row["source"],
                    "status": row["status"],
                    "snapshots": int(row["snapshots"]),
                    "add_count": int(row["add_count"]),
                    "upd_count": int(row["upd_count"]),
                    "del_count": int(row["del_count"]),
                    "unchanged_count": int(row["unchanged_count"]),
                    "success": bool(row["success"]),
                    "finished_at": row["finished_at"],
                }
                for row in latest_rows
            ],
            "timeline": [
                {
                    "bucket": row["bucket"],
                    "source": row["source"],
                    "status": row["status"],
                    "total": int(row["total"]),
                }
                for row in timeline_rows
            ],
            "changes_by_source": [
                {
                    "source": row["source"],
                    "add_total": int(row["add_total"]),
                    "upd_total": int(row["upd_total"]),
                    "del_total": int(row["del_total"]),
                }
                for row in change_rows
            ],
            "active_by_status": [
                {
                    "source": row["source"],
                    "status": row["status"],
                    "total": int(row["total"]),
                }
                for row in active_rows
            ],
        }

    async def create_event(self, event: FlightEvent) -> StoredFlightEvent:
        query = text(
            """
            INSERT INTO flight_events (
                event_type,
                flight_number,
                payload,
                confidence_score,
                observed_at,
                source
            )
            VALUES (
                :event_type,
                :flight_number,
                CAST(:payload_json AS JSONB),
                :confidence_score,
                :observed_at,
                :source
            )
            RETURNING
                id,
                event_type,
                flight_number,
                payload::text AS payload_json,
                confidence_score,
                observed_at,
                source
            """
        )
        params = {
            "event_type": event.event_type,
            "flight_number": event.flight_number,
            "payload_json": json.dumps(event.payload, ensure_ascii=False),
            "confidence_score": event.confidence_score,
            "observed_at": event.observed_at,
            "source": event.source,
        }

        async with self.engine.begin() as conn:
            row = (await conn.execute(query, params)).mappings().one()

        return self._map_row(row)

    async def get_event(self, event_id: int) -> StoredFlightEvent | None:
        query = text(
            """
            SELECT
                id,
                event_type,
                flight_number,
                payload::text AS payload_json,
                confidence_score,
                observed_at,
                source
            FROM flight_events
            WHERE id = :event_id
            """
        )

        async with self.engine.connect() as conn:
            row = (await conn.execute(query, {"event_id": event_id})).mappings().first()

        if row is None:
            return None
        return self._map_row(row)

    async def list_events(
        self,
        limit: int = 100,
        offset: int = 0,
        flight_number: str | None = None,
        event_type: str | None = None,
        source: str | None = None,
        observed_from: datetime | None = None,
        observed_to: datetime | None = None,
    ) -> list[StoredFlightEvent]:
        where_sql, params = _build_filters(
            flight_number=flight_number,
            event_type=event_type,
            source=source,
            observed_from=observed_from,
            observed_to=observed_to,
            extra={"limit": limit, "offset": offset},
        )

        query = text(
            f"""
            SELECT
                id,
                event_type,
                flight_number,
                payload::text AS payload_json,
                confidence_score,
                observed_at,
                source
            FROM flight_events
            {where_sql}
            ORDER BY id DESC
            LIMIT :limit OFFSET :offset
            """
        )

        async with self.engine.connect() as conn:
            rows = (await conn.execute(query, params)).mappings().all()

        return [self._map_row(row) for row in rows]

    async def get_events_stats(
        self,
        flight_number: str | None = None,
        event_type: str | None = None,
        source: str | None = None,
        observed_from: datetime | None = None,
        observed_to: datetime | None = None,
        top_flights_limit: int = 5,
    ) -> dict[str, Any]:
        where_sql, base_params = _build_filters(
            flight_number=flight_number,
            event_type=event_type,
            source=source,
            observed_from=observed_from,
            observed_to=observed_to,
        )

        async with self.engine.connect() as conn:
            total_row = (
                await conn.execute(
                    text(f"SELECT COUNT(*)::bigint AS total_events FROM flight_events {where_sql}"),
                    base_params,
                )
            ).mappings().one()

            by_type_rows = (
                await conn.execute(
                    text(
                        f"""
                        SELECT event_type, COUNT(*)::bigint AS total
                        FROM flight_events
                        {where_sql}
                        GROUP BY event_type
                        ORDER BY total DESC, event_type ASC
                        """
                    ),
                    base_params,
                )
            ).mappings().all()

            by_source_rows = (
                await conn.execute(
                    text(
                        f"""
                        SELECT source, COUNT(*)::bigint AS total
                        FROM flight_events
                        {where_sql}
                        GROUP BY source
                        ORDER BY total DESC, source ASC
                        """
                    ),
                    base_params,
                )
            ).mappings().all()

            top_params = {**base_params, "top_flights_limit": top_flights_limit}
            top_flights_rows = (
                await conn.execute(
                    text(
                        f"""
                        SELECT flight_number, COUNT(*)::bigint AS total
                        FROM flight_events
                        {where_sql}
                        GROUP BY flight_number
                        ORDER BY total DESC, flight_number ASC
                        LIMIT :top_flights_limit
                        """
                    ),
                    top_params,
                )
            ).mappings().all()

        return {
            "total_events": int(total_row["total_events"]),
            "by_event_type": [
                {"event_type": row["event_type"], "total": int(row["total"])}
                for row in by_type_rows
            ],
            "by_source": [
                {"source": row["source"], "total": int(row["total"])} for row in by_source_rows
            ],
            "top_flights": [
                {"flight_number": row["flight_number"], "total": int(row["total"])}
                for row in top_flights_rows
            ],
        }

    async def update_event(
        self,
        event_id: int,
        event_type: str | None = None,
        flight_number: str | None = None,
        payload: dict | None = None,
        confidence_score: float | None = None,
        observed_at: datetime | None = None,
        source: str | None = None,
    ) -> StoredFlightEvent | None:
        set_parts: list[str] = []
        params: dict[str, Any] = {"event_id": event_id}

        if event_type is not None:
            set_parts.append("event_type = :event_type")
            params["event_type"] = event_type
        if flight_number is not None:
            set_parts.append("flight_number = :flight_number")
            params["flight_number"] = flight_number
        if payload is not None:
            set_parts.append("payload = CAST(:payload_json AS JSONB)")
            params["payload_json"] = json.dumps(payload, ensure_ascii=False)
        if confidence_score is not None:
            set_parts.append("confidence_score = :confidence_score")
            params["confidence_score"] = confidence_score
        if observed_at is not None:
            set_parts.append("observed_at = :observed_at")
            params["observed_at"] = observed_at
        if source is not None:
            set_parts.append("source = :source")
            params["source"] = source

        if not set_parts:
            return await self.get_event(event_id)

        query = text(
            f"""
            UPDATE flight_events
            SET {", ".join(set_parts)}
            WHERE id = :event_id
            RETURNING
                id,
                event_type,
                flight_number,
                payload::text AS payload_json,
                confidence_score,
                observed_at,
                source
            """
        )

        async with self.engine.begin() as conn:
            row = (await conn.execute(query, params)).mappings().first()

        if row is None:
            return None
        return self._map_row(row)

    async def delete_event(self, event_id: int) -> bool:
        query = text("DELETE FROM flight_events WHERE id = :event_id")

        async with self.engine.begin() as conn:
            result = await conn.execute(query, {"event_id": event_id})

        return result.rowcount > 0

    @staticmethod
    def _map_row(row: Any) -> StoredFlightEvent:
        payload = row["payload_json"]
        if isinstance(payload, str):
            payload_data = json.loads(payload)
        else:
            payload_data = payload

        return StoredFlightEvent(
            id=row["id"],
            event_type=row["event_type"],
            flight_number=row["flight_number"],
            payload=payload_data,
            confidence_score=row["confidence_score"],
            observed_at=row["observed_at"],
            source=row["source"],
        )


def _build_filters(
    flight_number: str | None = None,
    event_type: str | None = None,
    source: str | None = None,
    observed_from: datetime | None = None,
    observed_to: datetime | None = None,
    extra: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    where_clauses: list[str] = []
    params: dict[str, Any] = {}

    if flight_number:
        where_clauses.append("flight_number = :flight_number")
        params["flight_number"] = flight_number
    if event_type:
        where_clauses.append("event_type = :event_type")
        params["event_type"] = event_type
    if source:
        where_clauses.append("source = :source")
        params["source"] = source
    if observed_from:
        where_clauses.append("observed_at >= :observed_from")
        params["observed_from"] = observed_from
    if observed_to:
        where_clauses.append("observed_at <= :observed_to")
        params["observed_to"] = observed_to

    if extra:
        params.update(extra)

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return where_sql, params
