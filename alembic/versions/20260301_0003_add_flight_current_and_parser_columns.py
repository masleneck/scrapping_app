"""add flight_current table and parser columns

Revision ID: 20260301_0003
Revises: 20260301_0002
Create Date: 2026-03-01 21:10:00
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260301_0003"
down_revision = "20260301_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE real_source_runs
        ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT ''
        """
    )
    op.execute(
        """
        ALTER TABLE real_source_runs
        ADD COLUMN IF NOT EXISTS strategy TEXT NOT NULL DEFAULT ''
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_real_source_runs_provider_strategy
        ON real_source_runs(provider, strategy)
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS flight_current (
            flight_instance_key TEXT PRIMARY KEY,
            flight_number TEXT NOT NULL,
            normalized_flight_number TEXT NOT NULL,
            direction TEXT NOT NULL,
            scheduled_time TIMESTAMPTZ NULL,
            estimated_time TIMESTAMPTZ NULL,
            actual_time TIMESTAMPTZ NULL,
            status TEXT NOT NULL,
            terminal TEXT NULL,
            aircraft_type TEXT NULL,
            airline_name TEXT NULL,
            airline_iata TEXT NULL,
            source TEXT NOT NULL,
            provider TEXT NOT NULL,
            strategy TEXT NOT NULL,
            source_priority INTEGER NOT NULL DEFAULT 0,
            source_timestamp TIMESTAMPTZ NOT NULL,
            info_url TEXT NULL,
            payload JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_flight_current_normalized_flight_number
        ON flight_current(normalized_flight_number)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_flight_current_status
        ON flight_current(status)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_flight_current_provider
        ON flight_current(provider)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_flight_current_source
        ON flight_current(source)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_flight_current_updated_at
        ON flight_current(updated_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS flight_current")
    op.execute("DROP INDEX IF EXISTS idx_real_source_runs_provider_strategy")
    op.execute("ALTER TABLE real_source_runs DROP COLUMN IF EXISTS strategy")
    op.execute("ALTER TABLE real_source_runs DROP COLUMN IF EXISTS provider")
