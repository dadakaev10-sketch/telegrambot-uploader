"""Minimal versioned PostgreSQL migration runner."""
from __future__ import annotations

from collections.abc import Iterable


MIGRATIONS: tuple[tuple[str, str], ...] = (
    (
        '001_paused_job_status',
        """
        ALTER TABLE download_jobs DROP CONSTRAINT IF EXISTS download_jobs_status_check;
        ALTER TABLE download_jobs ADD CONSTRAINT download_jobs_status_check
            CHECK (status IN ('queued','processing','paused','completed','dead','cancelled'));
        DROP INDEX IF EXISTS download_jobs_open_dedupe_uq;
        CREATE UNIQUE INDEX download_jobs_open_dedupe_uq
            ON download_jobs (dedupe_key)
            WHERE status IN ('queued','processing','paused');
        """,
    ),
    (
        '002_queue_maintenance_indexes',
        """
        CREATE INDEX IF NOT EXISTS download_batches_status_updated_idx
            ON download_batches (status, updated_at, id);
        CREATE INDEX IF NOT EXISTS download_jobs_batch_status_idx
            ON download_jobs (batch_id, status, id);
        """,
    ),
)


def apply_migrations(connection, migrations: Iterable[tuple[str, str]] = MIGRATIONS) -> list[str]:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    applied = {
        row['version']
        for row in connection.execute('SELECT version FROM schema_migrations').fetchall()
    }
    executed: list[str] = []
    for version, sql in migrations:
        if version in applied:
            continue
        connection.execute(sql)
        connection.execute(
            'INSERT INTO schema_migrations(version) VALUES (%s)', (version,)
        )
        executed.append(version)
    return executed
