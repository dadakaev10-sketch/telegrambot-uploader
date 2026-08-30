"""Lifecycle supervision for background asyncio tasks."""
from __future__ import annotations

import asyncio
from collections.abc import Coroutine
import logging
from typing import Any


class TaskSupervisor:
    def __init__(self) -> None:
        self.tasks: set[asyncio.Task] = set()

    def spawn(self, coroutine: Coroutine[Any, Any, Any], name: str) -> asyncio.Task:
        task = asyncio.create_task(coroutine, name=name)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        task.add_done_callback(self._log_result)
        return task

    @staticmethod
    def _log_result(task: asyncio.Task) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logging.exception("Background task '%s' beendet", task.get_name())

    async def cancel_all(self, exclude: asyncio.Task | None = None) -> None:
        remaining = [
            task for task in list(self.tasks)
            if task is not exclude and not task.done()
        ]
        for task in remaining:
            task.cancel()
        if remaining:
            await asyncio.gather(*remaining, return_exceptions=True)
