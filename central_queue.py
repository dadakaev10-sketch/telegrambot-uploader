from __future__ import annotations

import hashlib
import json
from datetime import timedelta

import psycopg
from psycopg.rows import dict_row


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS download_jobs (
    id BIGSERIAL PRIMARY KEY,
    dedupe_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'processing', 'completed', 'dead', 'cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by TEXT,
    locked_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS download_jobs_claim_idx
    ON download_jobs (status, available_at, id);
CREATE UNIQUE INDEX IF NOT EXISTS download_jobs_open_dedupe_uq
    ON download_jobs (dedupe_key)
    WHERE status IN ('queued', 'processing');
"""


class CentralQueue:
    def __init__(self, database_url: str, lease_seconds: int = 3600):
        self.database_url = database_url
        self.lease_seconds = max(60, lease_seconds)

    def connect(self):
        return psycopg.connect(self.database_url, connect_timeout=10, row_factory=dict_row)

    def init(self) -> None:
        with self.connect() as con:
            con.execute(SCHEMA_SQL)

    @staticmethod
    def dedupe_key(payload: dict) -> str:
        identity = {
            'type': payload.get('type'),
            'url': payload.get('url'),
            'content_type': payload.get('content_type'),
            'manual_title': payload.get('manual_title'),
            'is_anime': bool(payload.get('is_anime')),
        }
        raw = json.dumps(identity, ensure_ascii=False, sort_keys=True).encode('utf-8')
        return hashlib.sha256(raw).hexdigest()

    def enqueue(self, payload: dict) -> tuple[int, bool]:
        clean_payload = {k: v for k, v in payload.items() if not k.startswith('_central_')}
        key = self.dedupe_key(clean_payload)
        with self.connect() as con:
            con.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
            existing = con.execute(
                "SELECT id FROM download_jobs WHERE dedupe_key=%s AND status IN ('queued','processing')",
                (key,),
            ).fetchone()
            if existing:
                return existing['id'], False
            row = con.execute(
                "INSERT INTO download_jobs(dedupe_key,payload) VALUES (%s,%s::jsonb) RETURNING id",
                (key, json.dumps(clean_payload, ensure_ascii=False)),
            ).fetchone()
            return row['id'], True

    def claim(self, worker_id: str) -> dict | None:
        with self.connect() as con:
            row = con.execute(
                """
                WITH candidate AS (
                    SELECT id FROM download_jobs
                    WHERE (status='queued' AND available_at <= now())
                       OR (status='processing' AND locked_at < now() - (%s * interval '1 second'))
                    ORDER BY available_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE download_jobs AS job
                SET status='processing', locked_by=%s, locked_at=now(),
                    attempts=attempts+1, updated_at=now()
                FROM candidate
                WHERE job.id=candidate.id
                RETURNING job.id, job.payload, job.attempts
                """,
                (self.lease_seconds, worker_id),
            ).fetchone()
            return dict(row) if row else None

    def complete(self, job_id: int, worker_id: str) -> None:
        with self.connect() as con:
            con.execute(
                "UPDATE download_jobs SET status='completed', completed_at=now(), updated_at=now(), locked_by=NULL, locked_at=NULL WHERE id=%s AND locked_by=%s",
                (job_id, worker_id),
            )

    def heartbeat(self, job_id: int, worker_id: str) -> bool:
        with self.connect() as con:
            result = con.execute(
                "UPDATE download_jobs SET locked_at=now(), updated_at=now() WHERE id=%s AND status='processing' AND locked_by=%s",
                (job_id, worker_id),
            )
            return result.rowcount == 1

    def retry(self, job_id: int, worker_id: str, error: str, delay_seconds: int) -> None:
        with self.connect() as con:
            con.execute(
                "UPDATE download_jobs SET status='queued', available_at=now()+(%s * interval '1 second'), last_error=%s, updated_at=now(), locked_by=NULL, locked_at=NULL WHERE id=%s AND locked_by=%s",
                (delay_seconds, error[:1000], job_id, worker_id),
            )

    def dead(self, job_id: int, worker_id: str, error: str) -> None:
        with self.connect() as con:
            con.execute(
                "UPDATE download_jobs SET status='dead', last_error=%s, updated_at=now(), locked_by=NULL, locked_at=NULL WHERE id=%s AND locked_by=%s",
                (error[:1000], job_id, worker_id),
            )

    def open_jobs(self, limit: int = 50) -> list[dict]:
        with self.connect() as con:
            rows = con.execute(
                "SELECT id,status,payload,attempts,available_at,locked_by FROM download_jobs WHERE status IN ('queued','processing') ORDER BY available_at,id LIMIT %s",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
