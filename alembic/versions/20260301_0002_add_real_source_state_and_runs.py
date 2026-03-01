"""add real source state and runs tables

Revision ID: 20260301_0002
Revises: 20260228_0001
Create Date: 2026-03-01 18:50:00
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260301_0002"
down_revision = "20260228_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS real_source_state (
            event_key TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            flight_number TEXT NOT NULL,
            snapshot_payload JSONB NOT NULL,
            snapshot_hash TEXT NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL,
            is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
            deleted_at TIMESTAMPTZ NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_real_source_state_source
        ON real_source_state(source)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_real_source_state_last_seen_at
        ON real_source_state(last_seen_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_real_source_state_is_deleted
        ON real_source_state(is_deleted)
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS real_source_runs (
            id BIGSERIAL PRIMARY KEY,
            run_id UUID NOT NULL,
            trigger TEXT NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            snapshots INTEGER NOT NULL DEFAULT 0,
            blocked_markers JSONB NOT NULL DEFAULT '[]'::jsonb,
            error TEXT NULL,
            add_count INTEGER NOT NULL DEFAULT 0,
            upd_count INTEGER NOT NULL DEFAULT 0,
            del_count INTEGER NOT NULL DEFAULT 0,
            unchanged_count INTEGER NOT NULL DEFAULT 0,
            success BOOLEAN NOT NULL DEFAULT FALSE,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_real_source_runs_run_id
        ON real_source_runs(run_id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_real_source_runs_source
        ON real_source_runs(source)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_real_source_runs_status
        ON real_source_runs(status)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_real_source_runs_finished_at
        ON real_source_runs(finished_at DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_real_source_runs_success
        ON real_source_runs(success)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS real_source_runs")
    op.execute("DROP TABLE IF EXISTS real_source_state")
