from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

from app.models import FlightEvent, StoredFlightEvent


class SQLiteEventRepository:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)

    async def init(self) -> None:
        await asyncio.to_thread(self._init_sync)

    async def save_events(self, events: list[FlightEvent]) -> int:
        if not events:
            return 0

        serialized = [
            (
                event.event_type,
                event.flight_number,
                json.dumps(event.payload, ensure_ascii=False),
                event.confidence_score,
                event.observed_at.isoformat(),
                event.source,
            )
            for event in events
        ]
        return await asyncio.to_thread(self._save_events_sync, serialized)

    async def list_events(
        self, limit: int = 100, flight_number: str | None = None, event_type: str | None = None
    ) -> list[StoredFlightEvent]:
        return await asyncio.to_thread(
            self._list_events_sync,
            limit,
            flight_number,
            event_type,
        )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_sync(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS flight_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    flight_number TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    confidence_score REAL NOT NULL,
                    observed_at TEXT NOT NULL,
                    source TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_flight_events_flight_number
                ON flight_events(flight_number)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_flight_events_event_type
                ON flight_events(event_type)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_flight_events_observed_at
                ON flight_events(observed_at)
                """
            )
            conn.commit()

    def _save_events_sync(self, serialized_events: list[tuple]) -> int:
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO flight_events (
                    event_type,
                    flight_number,
                    payload_json,
                    confidence_score,
                    observed_at,
                    source
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                serialized_events,
            )
            conn.commit()
        return len(serialized_events)

    def _list_events_sync(
        self, limit: int, flight_number: str | None, event_type: str | None
    ) -> list[StoredFlightEvent]:
        where_clauses: list[str] = []
        params: list[object] = []

        if flight_number:
            where_clauses.append("flight_number = ?")
            params.append(flight_number)
        if event_type:
            where_clauses.append("event_type = ?")
            params.append(event_type)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        params.append(limit)

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    event_type,
                    flight_number,
                    payload_json,
                    confidence_score,
                    observed_at,
                    source
                FROM flight_events
                {where_sql}
                ORDER BY id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return [
            StoredFlightEvent(
                id=row["id"],
                event_type=row["event_type"],
                flight_number=row["flight_number"],
                payload=json.loads(row["payload_json"]),
                confidence_score=row["confidence_score"],
                observed_at=row["observed_at"],
                source=row["source"],
            )
            for row in rows
        ]
