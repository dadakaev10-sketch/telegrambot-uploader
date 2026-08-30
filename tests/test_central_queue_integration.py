import os
import uuid

import psycopg
import pytest
from dotenv import load_dotenv
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from central_queue import CentralQueue


load_dotenv('.env')


@pytest.fixture()
def queue_db():
    base_url = os.getenv('DATABASE_URL')
    if not base_url:
        pytest.skip('DATABASE_URL fehlt')
    schema = f"test_uploader_{uuid.uuid4().hex}"
    with psycopg.connect(base_url, autocommit=True) as con:
        con.execute(f'CREATE SCHEMA "{schema}"')
    params = conninfo_to_dict(base_url)
    params['options'] = f'-c search_path={schema}'
    queue = CentralQueue(make_conninfo(**params), lease_seconds=60)
    queue.init()
    try:
        yield queue
    finally:
        with psycopg.connect(base_url, autocommit=True) as con:
            con.execute(f'DROP SCHEMA "{schema}" CASCADE')


def payload(title='Testserie S01E01'):
    return {
        'type': 'video',
        'url': f'https://example.invalid/{title}',
        'content_type': 'serie',
        'manual_title': title,
        'is_anime': False,
    }


def test_pause_resume_and_claim(queue_db):
    job_id, created = queue_db.enqueue(payload())
    assert created
    paused = queue_db.set_group_paused(job_id, True)
    assert paused and paused['count'] == 1 and paused['paused']
    assert queue_db.claim('test-worker') is None

    resumed = queue_db.set_group_paused(job_id, False)
    assert resumed and resumed['count'] == 1 and not resumed['paused']
    claimed = queue_db.claim('test-worker')
    assert claimed and claimed['id'] == job_id


def test_paused_job_remains_deduplicated(queue_db):
    job_id, created = queue_db.enqueue(payload())
    assert created
    queue_db.set_group_paused(job_id, True)
    duplicate_id, duplicate_created = queue_db.enqueue(payload())
    assert duplicate_id == job_id
    assert not duplicate_created


def test_whole_season_group_is_paused(queue_db):
    first_id, _ = queue_db.enqueue(payload('Testserie S02E01'))
    queue_db.enqueue(payload('Testserie S02E02'))
    result = queue_db.set_group_paused(first_id, True)
    assert result and result['count'] == 2
    jobs = queue_db.open_jobs()
    assert {job['status'] for job in jobs} == {'paused'}


def test_expire_only_empty_stale_collecting_batches(queue_db):
    batch = queue_db.get_or_create_batch(
        'test:stale', 'Stale Staffel', 'https://example.invalid/series', None, 10
    )
    with queue_db.connect() as con:
        con.execute(
            "UPDATE download_batches SET updated_at=now()-interval '48 hours' WHERE id=%s",
            (batch['id'],),
        )
    assert queue_db.expire_stale_collecting_batches(24) == 1
    assert queue_db.collecting_batches() == []
