from __future__ import annotations

import hashlib
import json
from datetime import timedelta

import psycopg
from psycopg.rows import dict_row


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS download_batches (
    id BIGSERIAL PRIMARY KEY,
    source_key TEXT NOT NULL,
    title TEXT NOT NULL,
    source_url TEXT NOT NULL,
    chat_id BIGINT,
    status TEXT NOT NULL DEFAULT 'collecting'
        CHECK (status IN ('collecting', 'ready', 'completed', 'cancelled')),
    expected_jobs INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS download_batches_collecting_source_uq
    ON download_batches (source_key) WHERE status='collecting';

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

-- Migration für ältere download_jobs-Versionen. CREATE TABLE IF NOT EXISTS
-- verändert eine bereits vorhandene Tabelle nicht, daher jede benötigte
-- Spalte ausdrücklich und zerstörungsfrei ergänzen.
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS dedupe_key TEXT;
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'queued';
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS attempts INTEGER DEFAULT 0;
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ DEFAULT now();
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS locked_by TEXT;
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS batch_id BIGINT REFERENCES download_batches(id);

-- Ältere Versionen hatten eine globale UNIQUE-Constraint auf dedupe_key.
-- Dadurch konnten abgeschlossene oder abgebrochene Jobs nie erneut eingestellt
-- werden. Die aktuelle Queue dedupliziert ausschließlich offene Jobs über den
-- partiellen Index weiter unten.
ALTER TABLE download_jobs DROP CONSTRAINT IF EXISTS download_jobs_dedupe_key_key;

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

    def enqueue_many(self, payloads: list[dict]) -> dict[str, int]:
        """Veröffentlicht einen vollständigen Satz Jobs atomar in einer Transaktion."""
        created = 0
        existing = 0
        with self.connect() as con:
            for payload in payloads:
                clean = {k: v for k, v in payload.items() if not k.startswith('_central_')}
                key = self.dedupe_key(clean)
                con.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
                row = con.execute(
                    "SELECT id FROM download_jobs WHERE dedupe_key=%s AND status IN ('queued','processing')",
                    (key,),
                ).fetchone()
                if row:
                    existing += 1
                    continue
                con.execute(
                    "INSERT INTO download_jobs(dedupe_key,payload) VALUES (%s,%s::jsonb)",
                    (key, json.dumps(clean, ensure_ascii=False)),
                )
                created += 1
        return {'created': created, 'existing': existing}

    def get_or_create_batch(
        self, source_key: str, title: str, source_url: str,
        chat_id: int | None, expected_jobs: int,
    ) -> dict:
        with self.connect() as con:
            con.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (source_key,))
            row = con.execute(
                "SELECT * FROM download_batches WHERE source_key=%s AND status='collecting' ORDER BY id DESC LIMIT 1",
                (source_key,),
            ).fetchone()
            if row:
                con.execute(
                    "UPDATE download_batches SET title=%s,source_url=%s,chat_id=%s,expected_jobs=%s,updated_at=now() WHERE id=%s",
                    (title, source_url, chat_id, expected_jobs, row['id']),
                )
                return dict(row)
            row = con.execute(
                "INSERT INTO download_batches(source_key,title,source_url,chat_id,expected_jobs) VALUES (%s,%s,%s,%s,%s) RETURNING *",
                (source_key, title, source_url, chat_id, expected_jobs),
            ).fetchone()
            return dict(row)

    def batch_job_keys(self, batch_id: int) -> set[str]:
        with self.connect() as con:
            rows = con.execute(
                "SELECT dedupe_key FROM download_jobs WHERE batch_id=%s AND status <> 'cancelled'",
                (batch_id,),
            ).fetchall()
            return {row['dedupe_key'] for row in rows}

    def batch_job_titles(self, batch_id: int) -> set[str]:
        with self.connect() as con:
            rows = con.execute(
                "SELECT payload->>'manual_title' AS title FROM download_jobs WHERE batch_id=%s AND status <> 'cancelled'",
                (batch_id,),
            ).fetchall()
            return {row['title'] for row in rows if row['title']}

    def released_job_titles(self, source_key: str) -> set[str]:
        """Titel, die bereits in einem freigegebenen Staffel-Batch liegen."""
        with self.connect() as con:
            rows = con.execute(
                """
                SELECT j.payload->>'manual_title' AS title
                FROM download_jobs j
                JOIN download_batches b ON b.id=j.batch_id
                WHERE b.source_key=%s AND b.status IN ('ready','completed')
                  AND j.status <> 'cancelled'
                """,
                (source_key,),
            ).fetchall()
            return {row['title'] for row in rows if row['title']}

    def enqueue_batch_job(self, batch_id: int, payload: dict) -> tuple[int, bool]:
        clean = {k: v for k, v in payload.items() if not k.startswith('_central_')}
        key = self.dedupe_key(clean)
        with self.connect() as con:
            batch = con.execute(
                "SELECT status FROM download_batches WHERE id=%s FOR UPDATE",
                (batch_id,),
            ).fetchone()
            if not batch or batch['status'] != 'collecting':
                raise RuntimeError("Remote-Queue-Batch wurde abgebrochen")
            con.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
            row = con.execute(
                "SELECT id FROM download_jobs WHERE dedupe_key=%s AND status IN ('queued','processing')",
                (key,),
            ).fetchone()
            if row:
                return row['id'], False
            row = con.execute(
                "INSERT INTO download_jobs(dedupe_key,payload,batch_id) VALUES (%s,%s::jsonb,%s) RETURNING id",
                (key, json.dumps(clean, ensure_ascii=False), batch_id),
            ).fetchone()
            con.execute("UPDATE download_batches SET updated_at=now() WHERE id=%s", (batch_id,))
            return row['id'], True

    def release_batch(self, batch_id: int) -> int:
        with self.connect() as con:
            count = con.execute(
                "SELECT count(*) AS n FROM download_jobs WHERE batch_id=%s AND status='queued'",
                (batch_id,),
            ).fetchone()['n']
            con.execute(
                "UPDATE download_batches SET status='ready',released_at=now(),updated_at=now() WHERE id=%s AND status='collecting'",
                (batch_id,),
            )
            return count

    def collecting_batches(self) -> list[dict]:
        with self.connect() as con:
            rows = con.execute(
                """
                SELECT b.id,b.title,b.source_url,b.expected_jobs,count(j.id) AS stored_jobs
                FROM download_batches b
                LEFT JOIN download_jobs j ON j.batch_id=b.id AND j.status <> 'cancelled'
                WHERE b.status='collecting'
                GROUP BY b.id ORDER BY b.created_at
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def claim(self, worker_id: str) -> dict | None:
        with self.connect() as con:
            row = con.execute(
                """
                WITH candidate AS (
                    SELECT id FROM download_jobs
                    WHERE (status='queued' AND available_at <= now()
                           AND (batch_id IS NULL OR EXISTS (
                               SELECT 1 FROM download_batches b WHERE b.id=download_jobs.batch_id AND b.status='ready'
                           )))
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

    def recover_worker_jobs(
        self, worker_id: str, legacy_worker_prefix: str | None = None
    ) -> int:
        """Gibt Jobs einer vorherigen Instanz desselben Rechners sofort frei."""
        with self.connect() as con:
            if legacy_worker_prefix:
                result = con.execute(
                    """
                    UPDATE download_jobs
                    SET status='queued', available_at=now(), updated_at=now(),
                        locked_by=NULL, locked_at=NULL,
                        last_error='Worker wurde neu gestartet'
                    WHERE status='processing'
                      AND (locked_by=%s OR locked_by LIKE %s)
                    """,
                    (worker_id, f'{legacy_worker_prefix}%'),
                )
            else:
                result = con.execute(
                    """
                    UPDATE download_jobs
                    SET status='queued', available_at=now(), updated_at=now(),
                        locked_by=NULL, locked_at=NULL,
                        last_error='Worker wurde neu gestartet'
                    WHERE status='processing' AND locked_by=%s
                    """,
                    (worker_id,),
                )
            return int(result.rowcount)

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

    def clear_waiting_jobs(self) -> dict[str, int]:
        """Leert alle offenen Jobs und Batch-Entwürfe der zentralen Queue."""
        with self.connect() as con:
            cancelled = con.execute(
                """
                UPDATE download_jobs
                SET status='cancelled', updated_at=now(),
                    locked_by=NULL, locked_at=NULL
                WHERE status IN ('queued','processing')
                """,
            ).rowcount
            cancelled_batches = con.execute(
                """
                UPDATE download_batches
                SET status='cancelled', updated_at=now()
                WHERE status IN ('collecting','ready')
                """
            ).rowcount
            return {
                'cancelled': int(cancelled),
                'cancelled_batches': int(cancelled_batches),
            }
