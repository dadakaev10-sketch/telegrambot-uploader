import asyncio

import pytest

from app.runtime import TaskSupervisor


@pytest.mark.asyncio
async def test_supervisor_tracks_and_removes_finished_task():
    supervisor = TaskSupervisor()
    task = supervisor.spawn(asyncio.sleep(0), 'short-task')
    await task
    await asyncio.sleep(0)
    assert not supervisor.tasks


@pytest.mark.asyncio
async def test_supervisor_cancels_tasks():
    supervisor = TaskSupervisor()
    task = supervisor.spawn(asyncio.sleep(60), 'long-task')
    await supervisor.cancel_all()
    assert task.cancelled()


@pytest.mark.asyncio
async def test_supervisor_keeps_selected_worker_prefixes():
    supervisor = TaskSupervisor()
    worker = supervisor.spawn(asyncio.sleep(60), 'download-worker-1')
    watchdog = supervisor.spawn(asyncio.sleep(60), 'telegram-polling-watchdog')
    await supervisor.cancel_except_prefixes(('download-worker-',))
    assert not worker.done()
    assert watchdog.cancelled()
    await supervisor.cancel_all()
