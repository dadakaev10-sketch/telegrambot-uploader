"""Worker-local runtime state for concurrent download pipelines."""
from __future__ import annotations

import asyncio
from contextvars import ContextVar


class PipelineRuntime:
    def __init__(self) -> None:
        self.statuses: dict[int, dict] = {}
        self.processes: dict[int, asyncio.subprocess.Process] = {}
        self._current_worker: ContextVar[int | None] = ContextVar(
            'pipeline_worker_id', default=None
        )

    def bind_worker(self, worker_id: int) -> None:
        self._current_worker.set(worker_id)

    def worker_id(self) -> int:
        return int(self._current_worker.get() or 0)

    def status(self) -> dict:
        return self.statuses.setdefault(self.worker_id(), {})

    def set_process(self, process: asyncio.subprocess.Process | None) -> None:
        worker_id = self.worker_id()
        if process is None:
            self.processes.pop(worker_id, None)
        else:
            self.processes[worker_id] = process

    def process_for(self, worker_id: int) -> asyncio.subprocess.Process | None:
        return self.processes.get(worker_id)

    def clear_worker(self, worker_id: int) -> None:
        self.statuses.pop(worker_id, None)
        self.processes.pop(worker_id, None)
