"""Common extractor contracts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Episode:
    season: int
    number: int
    url: str
    title: str | None = None


class SeriesExtractor(Protocol):
    async def list_episodes(self, url: str) -> tuple[str, list[Episode]]: ...

    async def player_url(self, episode_url: str) -> str: ...
