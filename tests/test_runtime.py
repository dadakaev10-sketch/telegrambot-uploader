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
