from app.queue.runtime import PipelineRuntime


def test_worker_statuses_are_isolated():
    runtime = PipelineRuntime()
    runtime.bind_worker(1)
    runtime.status()['filename'] = 'one.mp4'
    runtime.bind_worker(2)
    runtime.status()['filename'] = 'two.mp4'
    assert runtime.statuses[1]['filename'] == 'one.mp4'
    assert runtime.statuses[2]['filename'] == 'two.mp4'


def test_clear_worker_does_not_touch_other_worker():
    runtime = PipelineRuntime()
    runtime.bind_worker(1)
    runtime.status()['phase'] = 'download'
    runtime.bind_worker(2)
    runtime.status()['phase'] = 'upload'
    runtime.clear_worker(1)
    assert 1 not in runtime.statuses
    assert runtime.statuses[2]['phase'] == 'upload'
