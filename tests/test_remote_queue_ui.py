from app.telegram.remote_queue_ui import build_remote_queue_groups


def key(job):
    title = (job.get('payload') or {}).get('manual_title', '')
    if ' S01E' in title:
        return ('season', title.split(' S01E')[0], 1)
    return ('job', job['id'])


def label(payload):
    return payload.get('manual_title', 'Unbekannt')


def test_paused_season_is_grouped_and_marked():
    jobs = [
        {'id': 1, 'status': 'paused', 'payload': {'manual_title': 'Serie S01E01'}},
        {'id': 2, 'status': 'paused', 'payload': {'manual_title': 'Serie S01E02'}},
    ]
    processing, groups = build_remote_queue_groups(jobs, key, label)
    assert processing == []
    assert len(groups) == 1
    assert groups[0]['paused'] is True
    assert groups[0]['label'] == '📺 Serie · Staffel 1 · 2 Folgen · E01–E02'


def test_processing_jobs_are_not_rendered_as_waiting_groups():
    jobs = [
        {'id': 1, 'status': 'processing', 'payload': {'manual_title': 'Film'}},
    ]
    processing, groups = build_remote_queue_groups(jobs, key, label)
    assert len(processing) == 1
    assert groups == []
