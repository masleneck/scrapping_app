"""create flight_events table

Revision ID: 20260228_0001
Revises: 
Create Date: 2026-02-28 20:45:00
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260228_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS flight_events (
            id BIGSERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            flight_number TEXT NOT NULL,
            payload JSONB NOT NULL,
            confidence_score DOUBLE PRECISION NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            source TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_flight_events_confidence
                CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_flight_events_flight_number
        ON flight_events(flight_number)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_flight_events_event_type
        ON flight_events(event_type)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_flight_events_observed_at
        ON flight_events(observed_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_flight_events_source
        ON flight_events(source)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS flight_events")
