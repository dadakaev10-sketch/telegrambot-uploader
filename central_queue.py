from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import timedelta

import psycopg
from psycopg.rows import dict_row
from app.database.migrations import apply_migrations


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
        CHECK (status IN ('queued', 'processing', 'paused', 'completed', 'dead', 'cancelled')),
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
ALTER TABLE download_jobs ADD COLUMN IF NOT EXISTS queue_position BIGINT;
ALTER TABLE download_jobs DROP CONSTRAINT IF EXISTS download_jobs_status_check;
ALTER TABLE download_jobs ADD CONSTRAINT download_jobs_status_check
    CHECK (status IN ('queued', 'processing', 'paused', 'completed', 'dead', 'cancelled'));

-- Bestehende Jobs einmalig in ihrer bisherigen effektiven Reihenfolge
-- nummerieren. Danach kann die Reihenfolge unabhängig von IDs gespeichert
-- und über den Telegram-Bot verändert werden.
WITH ranked AS (
    SELECT id,
           row_number() OVER (
               ORDER BY
                   CASE
                       WHEN payload->>'priority' ~ '^-?[0-9]+$'
                       THEN (payload->>'priority')::integer
                       ELSE 0
                   END DESC,
                   available_at,
                   id
           ) * 1000 AS position
    FROM download_jobs
    WHERE queue_position IS NULL
)
UPDATE download_jobs AS job
SET queue_position=ranked.position
FROM ranked
WHERE job.id=ranked.id;

-- Ältere Versionen hatten eine globale UNIQUE-Constraint auf dedupe_key.
-- Dadurch konnten abgeschlossene oder abgebrochene Jobs nie erneut eingestellt
-- werden. Die aktuelle Queue dedupliziert ausschließlich offene Jobs über den
-- partiellen Index weiter unten.
ALTER TABLE download_jobs DROP CONSTRAINT IF EXISTS download_jobs_dedupe_key_key;

CREATE INDEX IF NOT EXISTS download_jobs_claim_idx
    ON download_jobs (status, available_at, id);
CREATE INDEX IF NOT EXISTS download_jobs_queue_position_idx
    ON download_jobs (status, queue_position, id);
DROP INDEX IF EXISTS download_jobs_open_dedupe_uq;
CREATE UNIQUE INDEX download_jobs_open_dedupe_uq
    ON download_jobs (dedupe_key)
    WHERE status IN ('queued', 'processing', 'paused');
"""


class CentralQueue:
    def __init__(self, database_url: str, lease_seconds: int = 3600):
        self.database_url = database_url
        self.lease_seconds = max(60, lease_seconds)

    def connect(self):
        return psycopg.connect(self.database_url, connect_timeout=10, row_factory=dict_row)

    @staticmethod
    def _append_position(con) -> int:
        """Reserviert unter Transaktions-Lock eine Position am Queue-Ende."""
        con.execute("SELECT pg_advisory_xact_lock(hashtext('download_jobs_queue_order'))")
        row = con.execute(
            "SELECT COALESCE(max(queue_position),0)+1000 AS position FROM download_jobs"
        ).fetchone()
        return int(row['position'])

    def init(self) -> None:
        with self.connect() as con:
            con.execute(SCHEMA_SQL)
            apply_migrations(con)
            self._migrate_open_film_dedupe_keys(con)

    @staticmethod
    def _film_identity(payload: dict) -> dict | None:
        """Stabile Filmidentität ohne Quelle, Genres oder Satzzeichen."""
        content_type = payload.get('content_type') or payload.get('db_content_type')
        if content_type != 'film':
            return None
        title = str(payload.get('manual_title') or '').strip()
        match = re.search(r'^(.*?)\s*\((\d{4})\)', title)
        if not match:
            return None
        name = unicodedata.normalize('NFKC', match.group(1)).casefold()
        name = name.replace('&', ' und ')
        name = ''.join(re.findall(r'\w+', name, flags=re.UNICODE))
        if not name:
            return None
        return {'kind': 'film', 'title': name, 'year': int(match.group(2))}

    def _migrate_open_film_dedupe_keys(self, con) -> None:
        """Stellt bestehende offene Filme auf Titel+Jahr um und entfernt Dubletten."""
        con.execute("SELECT pg_advisory_xact_lock(hashtext('download_jobs_film_dedupe'))")
        rows = con.execute(
            """
            SELECT id,payload,status FROM download_jobs
            WHERE status IN ('queued','processing')
            ORDER BY CASE status WHEN 'processing' THEN 0 ELSE 1 END,
                     COALESCE(queue_position,id),id
            FOR UPDATE
            """
        ).fetchall()
        groups: dict[str, list[dict]] = {}
        for raw in rows:
            row = dict(raw)
            identity = self._film_identity(row.get('payload') or {})
            if identity is None:
                continue
            key = hashlib.sha256(
                json.dumps(identity, ensure_ascii=False, sort_keys=True).encode('utf-8')
            ).hexdigest()
            groups.setdefault(key, []).append(row)

        # Temporäre Schlüssel verhindern Konflikte mit dem partiellen UNIQUE-Index,
        # während alte URL-basierte Schlüssel auf die neue Identität wechseln.
        for jobs in groups.values():
            for job in jobs:
                con.execute(
                    "UPDATE download_jobs SET dedupe_key=%s WHERE id=%s",
                    (f"migration-{job['id']}-{job.get('status')}", job['id']),
                )
        for key, jobs in groups.items():
            keeper, *duplicates = jobs
            con.execute(
                "UPDATE download_jobs SET dedupe_key=%s,updated_at=now() WHERE id=%s",
                (key, keeper['id']),
            )
            # Einen bereits von einem Worker verarbeiteten zweiten Job niemals
            # unter ihm abbrechen. Nur noch wartende Dubletten werden entfernt.
            queued_duplicates = [job for job in duplicates if job['status'] == 'queued']
            processing_duplicates = [job for job in duplicates if job['status'] == 'processing']
            for job in processing_duplicates:
                con.execute(
                    "UPDATE download_jobs SET dedupe_key=%s WHERE id=%s",
                    (f"processing-film-{job['id']}", job['id']),
                )
            if queued_duplicates:
                duplicate_ids = [int(job['id']) for job in queued_duplicates]
                con.execute(
                    """
                    UPDATE download_jobs
                    SET status='cancelled',locked_by=NULL,locked_at=NULL,
                        last_error='Doppelter offener Filmjob (gleicher Titel und Jahr)',
                        updated_at=now()
                    WHERE id=ANY(%s)
                    """,
                    (duplicate_ids,),
                )

    def next_front_priority(self) -> int:
        """Reserviert eine Priorität oberhalb aller momentan offenen Jobs."""
        with self.connect() as con:
            con.execute("SELECT pg_advisory_xact_lock(hashtext('download_jobs_front_priority'))")
            row = con.execute(
                """
                SELECT COALESCE(max(
                    CASE
                        WHEN payload->>'priority' ~ '^-?[0-9]+$'
                        THEN (payload->>'priority')::integer
                        ELSE 0
                    END
                ),0)+1000 AS priority
                FROM download_jobs
                WHERE status IN ('queued','processing')
                """
            ).fetchone()
            return int(row['priority'])

    @staticmethod
    def dedupe_key(payload: dict) -> str:
        identity = CentralQueue._film_identity(payload)
        if identity is None:
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
                "SELECT id FROM download_jobs WHERE dedupe_key=%s AND status IN ('queued','processing','paused')",
                (key,),
            ).fetchone()
            if existing:
                if int(clean_payload.get('priority', 0) or 0) > 0:
                    con.execute(
                        "UPDATE download_jobs SET payload=payload || %s::jsonb,updated_at=now() WHERE id=%s",
                        (json.dumps({'priority': int(clean_payload['priority'])}), existing['id']),
                    )
                return existing['id'], False
            position = self._append_position(con)
            row = con.execute(
                "INSERT INTO download_jobs(dedupe_key,payload,queue_position) VALUES (%s,%s::jsonb,%s) RETURNING id",
                (key, json.dumps(clean_payload, ensure_ascii=False), position),
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
                    "SELECT id FROM download_jobs WHERE dedupe_key=%s AND status IN ('queued','processing','paused')",
                    (key,),
                ).fetchone()
                if row:
                    if int(clean.get('priority', 0) or 0) > 0:
                        con.execute(
                            "UPDATE download_jobs SET payload=payload || %s::jsonb,updated_at=now() WHERE id=%s",
                            (json.dumps({'priority': int(clean['priority'])}), row['id']),
                        )
                    existing += 1
                    continue
                position = self._append_position(con)
                con.execute(
                    "INSERT INTO download_jobs(dedupe_key,payload,queue_position) VALUES (%s,%s::jsonb,%s)",
                    (key, json.dumps(clean, ensure_ascii=False), position),
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
                "SELECT id FROM download_jobs WHERE dedupe_key=%s AND status IN ('queued','processing','paused')",
                (key,),
            ).fetchone()
            if row:
                if int(clean.get('priority', 0) or 0) > 0:
                    con.execute(
                        "UPDATE download_jobs SET payload=payload || %s::jsonb,updated_at=now() WHERE id=%s",
                        (json.dumps({'priority': int(clean['priority'])}), row['id']),
                    )
                return row['id'], False
            position = self._append_position(con)
            row = con.execute(
                "INSERT INTO download_jobs(dedupe_key,payload,batch_id,queue_position) VALUES (%s,%s::jsonb,%s,%s) RETURNING id",
                (key, json.dumps(clean, ensure_ascii=False), batch_id, position),
            ).fetchone()
            con.execute("UPDATE download_batches SET updated_at=now() WHERE id=%s", (batch_id,))
            return row['id'], True

    def release_batch(self, batch_id: int) -> int:
        with self.connect() as con:
            con.execute("SELECT pg_advisory_xact_lock(hashtext('download_jobs_queue_order'))")
            count = con.execute(
                "SELECT count(*) AS n FROM download_jobs WHERE batch_id=%s AND status='queued'",
                (batch_id,),
            ).fetchone()['n']
            # Parallele Extraktion kann Links in zufälliger Reihenfolge liefern.
            # Beim Freigeben wird der Staffelblock deshalb nach SxxExx sortiert
            # und geschlossen ans Ende der aktuellen Queue gesetzt.
            jobs = con.execute(
                """
                SELECT id FROM download_jobs
                WHERE batch_id=%s AND status='queued'
                ORDER BY payload->>'manual_title',id
                FOR UPDATE
                """,
                (batch_id,),
            ).fetchall()
            end_position = int(con.execute(
                "SELECT COALESCE(max(queue_position),0) AS position FROM download_jobs"
            ).fetchone()['position'])
            for index, job in enumerate(jobs, 1):
                con.execute(
                    "UPDATE download_jobs SET queue_position=%s WHERE id=%s",
                    (end_position + index * 1000, job['id']),
                )
            con.execute(
                "UPDATE download_batches SET status='ready',released_at=now(),updated_at=now() WHERE id=%s AND status='collecting'",
                (batch_id,),
            )
            return count

    def promote_source_jobs(self, source_key: str, priority: int = 100) -> int:
        """Zieht alle wartenden Jobs eines Staffel-Batches nach vorne."""
        with self.connect() as con:
            result = con.execute(
                """
                UPDATE download_jobs AS job
                SET payload=job.payload || %s::jsonb, updated_at=now()
                FROM download_batches AS batch
                WHERE job.batch_id=batch.id
                  AND batch.source_key=%s
                  AND job.status='queued'
                """,
                (json.dumps({'priority': int(priority)}), source_key),
            )
            return int(result.rowcount)

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
                    ORDER BY
                        CASE
                            WHEN payload->>'priority' ~ '^[0-9]+$'
                            THEN (payload->>'priority')::integer
                            ELSE 0
                        END DESC,
                        COALESCE(queue_position,id),
                        available_at,
                        id
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

    def open_jobs(self, limit: int = 500) -> list[dict]:
        with self.connect() as con:
            rows = con.execute(
                """
                SELECT j.id,j.status,j.payload,j.attempts,j.available_at,
                       j.locked_by,j.batch_id,j.queue_position,b.title AS batch_title
                FROM download_jobs j
                LEFT JOIN download_batches b ON b.id=j.batch_id
                WHERE j.status IN ('processing','paused')
                   OR (
                       j.status='queued'
                       AND (j.batch_id IS NULL OR b.status='ready')
                   )
                ORDER BY
                    CASE WHEN j.status='processing' THEN 0 ELSE 1 END,
                    CASE
                        WHEN j.payload->>'priority' ~ '^-?[0-9]+$'
                        THEN (j.payload->>'priority')::integer
                        ELSE 0
                    END DESC,
                    COALESCE(j.queue_position,j.id),j.id
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def set_group_paused(self, anchor_job_id: int, paused: bool) -> dict | None:
        """Pausiert oder reaktiviert einen vollständigen wartenden Queue-Block."""
        with self.connect() as con:
            con.execute("SELECT pg_advisory_xact_lock(hashtext('download_jobs_queue_order'))")
            rows = con.execute(
                """
                SELECT j.id,j.status,j.payload,j.batch_id,j.queue_position
                FROM download_jobs j
                LEFT JOIN download_batches b ON b.id=j.batch_id
                WHERE j.status IN ('queued','paused')
                  AND (j.batch_id IS NULL OR b.status='ready')
                ORDER BY COALESCE(j.queue_position,j.id),j.id
                FOR UPDATE OF j
                """
            ).fetchall()
            rows = [dict(row) for row in rows]
            anchor = next(
                (row for row in rows if int(row['id']) == int(anchor_job_id)), None
            )
            if not anchor:
                return None
            key = self._queue_group_key(anchor)
            selected = [row for row in rows if self._queue_group_key(row) == key]
            ids = [int(row['id']) for row in selected]
            target = 'paused' if paused else 'queued'
            con.execute(
                """
                UPDATE download_jobs
                SET status=%s,updated_at=now(),locked_by=NULL,locked_at=NULL,
                    last_error=%s
                WHERE id=ANY(%s)
                """,
                (target, 'Vom Benutzer pausiert' if paused else None, ids),
            )
            return {'count': len(ids), 'jobs': selected, 'paused': paused}

    @staticmethod
    def _queue_group_key(row: dict) -> tuple:
        """Staffel-Batches bzw. SxxExx-Titel zu einem verschiebbaren Block bündeln."""
        if row.get('batch_id') is not None:
            return ('batch', int(row['batch_id']))
        payload = row.get('payload') or {}
        title = payload.get('manual_title') or ''
        match = re.match(r'^(.*?)\s+S(\d{1,3})E\d{1,4}(?:\b|\s)', title, re.IGNORECASE)
        if match:
            return (
                'season',
                match.group(1).strip().casefold(),
                int(match.group(2)),
                bool(payload.get('is_anime')),
            )
        return ('job', int(row['id']))

    def reorder_group(self, anchor_job_id: int, action: str) -> dict | None:
        """Verschiebt einen kompletten Staffelblock atomar in der Remote Queue."""
        if action not in ('up', 'down', 'front', 'back'):
            raise ValueError('Ungültige Queue-Aktion')
        with self.connect() as con:
            con.execute("SELECT pg_advisory_xact_lock(hashtext('download_jobs_queue_order'))")
            rows = con.execute(
                """
                SELECT j.id,j.payload,j.batch_id,j.queue_position,b.title AS batch_title
                FROM download_jobs j
                LEFT JOIN download_batches b ON b.id=j.batch_id
                WHERE j.status IN ('queued','paused')
                  AND (j.batch_id IS NULL OR b.status='ready')
                ORDER BY
                    CASE
                        WHEN j.payload->>'priority' ~ '^-?[0-9]+$'
                        THEN (j.payload->>'priority')::integer
                        ELSE 0
                    END DESC,
                    COALESCE(j.queue_position,j.id),j.id
                FOR UPDATE OF j
                """
            ).fetchall()
            rows = [dict(row) for row in rows]
            anchor = next((row for row in rows if int(row['id']) == int(anchor_job_id)), None)
            if not anchor:
                return None

            groups: list[list[dict]] = []
            group_indexes: dict[tuple, int] = {}
            for row in rows:
                key = self._queue_group_key(row)
                if key not in group_indexes:
                    group_indexes[key] = len(groups)
                    groups.append([])
                groups[group_indexes[key]].append(row)

            anchor_key = self._queue_group_key(anchor)
            old_index = group_indexes[anchor_key]
            new_index = old_index
            if action == 'up':
                new_index = max(0, old_index - 1)
            elif action == 'down':
                new_index = min(len(groups) - 1, old_index + 1)
            elif action == 'front':
                new_index = 0
            elif action == 'back':
                new_index = len(groups) - 1

            moved = groups.pop(old_index)
            groups.insert(new_index, moved)
            position = 0
            for group in groups:
                for row in group:
                    position += 1000
                    # Nach einer manuellen Sortierung ist die sichtbare Position
                    # maßgeblich; alte Prioritätswerte dürfen sie nicht überstimmen.
                    con.execute(
                        """
                        UPDATE download_jobs
                        SET queue_position=%s,
                            payload=(payload - 'priority') || '{"priority": 0}'::jsonb,
                            updated_at=now()
                        WHERE id=%s
                        """,
                        (position, row['id']),
                    )
            return {
                'from': old_index + 1,
                'to': new_index + 1,
                'groups': len(groups),
                'jobs': len(moved),
            }

    def cancel_group(self, anchor_job_id: int, reason: str = 'Vom Benutzer abgebrochen') -> dict | None:
        """Bricht den sichtbaren Queue-Block des Anker-Jobs atomar ab."""
        with self.connect() as con:
            con.execute("SELECT pg_advisory_xact_lock(hashtext('download_jobs_queue_order'))")
            rows = con.execute(
                """
                SELECT j.id,j.payload,j.batch_id,j.queue_position
                FROM download_jobs j
                LEFT JOIN download_batches b ON b.id=j.batch_id
                WHERE j.status IN ('queued','paused')
                  AND (j.batch_id IS NULL OR b.status='ready')
                ORDER BY COALESCE(j.queue_position,j.id),j.id
                FOR UPDATE OF j
                """
            ).fetchall()
            rows = [dict(row) for row in rows]
            anchor = next(
                (row for row in rows if int(row['id']) == int(anchor_job_id)),
                None,
            )
            if not anchor:
                return None
            key = self._queue_group_key(anchor)
            selected = [row for row in rows if self._queue_group_key(row) == key]
            ids = [int(row['id']) for row in selected]
            con.execute(
                """
                UPDATE download_jobs
                SET status='cancelled',last_error=%s,updated_at=now(),
                    locked_by=NULL,locked_at=NULL
                WHERE id=ANY(%s) AND status='queued'
                """,
                (reason[:1000], ids),
            )
            batch_ids = sorted({
                int(row['batch_id']) for row in selected
                if row.get('batch_id') is not None
            })
            for batch_id in batch_ids:
                con.execute(
                    """
                    UPDATE download_batches b
                    SET status='cancelled',updated_at=now()
                    WHERE b.id=%s
                      AND NOT EXISTS (
                          SELECT 1 FROM download_jobs j
                          WHERE j.batch_id=b.id
                            AND j.status IN ('queued','processing')
                      )
                    """,
                    (batch_id,),
                )
            return {'jobs': selected, 'count': len(selected)}

    def expire_stale_collecting_batches(self, max_age_hours: int = 24) -> int:
        """Markiert leere, veraltete Extraktionsentwürfe als abgelaufen."""
        hours = max(1, int(max_age_hours))
        with self.connect() as con:
            result = con.execute(
                """
                UPDATE download_batches b
                SET status='cancelled',updated_at=now()
                WHERE b.status='collecting'
                  AND b.updated_at < now() - (%s * interval '1 hour')
                  AND NOT EXISTS (
                      SELECT 1 FROM download_jobs j
                      WHERE j.batch_id=b.id AND j.status <> 'cancelled'
                  )
                """,
                (hours,),
            )
            return int(result.rowcount)

    def status_counts(self) -> dict[str, int]:
        with self.connect() as con:
            rows = con.execute(
                "SELECT status,count(*) AS count FROM download_jobs GROUP BY status"
            ).fetchall()
            return {str(row['status']): int(row['count']) for row in rows}

    def clear_waiting_jobs(self, include_paused: bool = False) -> dict[str, int]:
        """Leert nur offene Download-Jobs der zentralen Queue.

        Staffel-Batches in ``download_batches`` sind Entwürfe/Link-Sammlungen,
        keine sichtbaren Queue-Downloadjobs. Sie dürfen beim normalen
        "Remote Queue leeren" nicht abgebrochen werden, weil Nutzer damit nur
        die aktuell sichtbaren Downloadjobs entfernen wollen.
        """
        with self.connect() as con:
            statuses = ['queued', 'processing']
            if include_paused:
                statuses.append('paused')
            cancelled = con.execute(
                """
                UPDATE download_jobs
                SET status='cancelled', updated_at=now(),
                    locked_by=NULL, locked_at=NULL
                WHERE status=ANY(%s)
                """,
                (statuses,),
            ).rowcount
            return {
                'cancelled': int(cancelled),
                'cancelled_batches': 0,
            }
