"""Typed queue models shared by worker and Telegram UI."""
from __future__ import annotations

from enum import StrEnum
from typing import Literal, NotRequired, TypedDict


class JobStatus(StrEnum):
    QUEUED = 'queued'
    PROCESSING = 'processing'
    PAUSED = 'paused'
    COMPLETED = 'completed'
    DEAD = 'dead'
    CANCELLED = 'cancelled'


class VideoJobPayload(TypedDict):
    type: Literal['video']
    url: str
    content_type: Literal['film', 'serie', 'anime', 'filmpalast']
    manual_title: str | None
    is_anime: bool
    chat_id: NotRequired[int | None]
    preferred_height: NotRequired[int | None]
    quality_mode: NotRequired[Literal['best', 'max_height', 'lowest', 'source']]
    allow_transcode: NotRequired[bool]
    priority: NotRequired[int]
