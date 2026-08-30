"""Download command policy helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


QualityMode = Literal['best', 'max_height', 'lowest', 'source']


@dataclass(frozen=True)
class DownloadPolicy:
    quality_mode: QualityMode = 'best'
    preferred_height: int | None = None
    allow_transcode: bool = False

    def ytdlp_format(self) -> str | None:
        if self.quality_mode == 'source':
            return None
        if self.quality_mode == 'lowest':
            return 'worstvideo+worstaudio/worst'
        if self.preferred_height:
            height = max(1, int(self.preferred_height))
            return f'bestvideo[height<={height}]+bestaudio/best[height<={height}]'
        return None


def policy_from_payload(payload: dict) -> DownloadPolicy:
    preferred = payload.get('preferred_height')
    default_mode: QualityMode = 'max_height' if preferred else 'best'
    mode = payload.get('quality_mode', default_mode)
    if mode not in {'best', 'max_height', 'lowest', 'source'}:
        raise ValueError(f'Unbekannter Qualitätsmodus: {mode}')
    return DownloadPolicy(
        quality_mode=mode,
        preferred_height=int(preferred) if preferred else None,
        allow_transcode=bool(payload.get('allow_transcode', False)),
    )
