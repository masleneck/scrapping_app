from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.models import FlightEvent, StoredFlightEvent


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
