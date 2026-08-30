"""Safe media-file retention helpers."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time


TEMP_SUFFIXES = ('.ts', '.part', '.m3u8')
TEMP_NAME_MARKERS = ('_scaled.mp4', '_scaled2.mp4', '_clean.mp4')


@dataclass(frozen=True)
class CleanupResult:
    candidates: tuple[Path, ...]
    deleted: tuple[Path, ...]
    reclaimed_bytes: int


def stale_temp_candidates(
    directory: Path,
    max_age_seconds: int,
    protected_paths: set[Path] | None = None,
    now: float | None = None,
) -> list[Path]:
    protected = {path.resolve() for path in (protected_paths or set())}
    cutoff = (now if now is not None else time.time()) - max_age_seconds
    candidates: list[Path] = []
    if not directory.exists():
        return candidates
    for path in directory.iterdir():
        if not path.is_file() or path.resolve() in protected:
            continue
        is_temp = path.suffix.lower() in TEMP_SUFFIXES or any(
            path.name.endswith(marker) for marker in TEMP_NAME_MARKERS
        )
        if is_temp and path.stat().st_mtime < cutoff:
            candidates.append(path)
    return sorted(candidates)


def cleanup_stale_temp_files(
    directory: Path,
    max_age_seconds: int,
    protected_paths: set[Path] | None = None,
    dry_run: bool = True,
) -> CleanupResult:
    candidates = stale_temp_candidates(
        directory, max_age_seconds, protected_paths=protected_paths
    )
    deleted: list[Path] = []
    reclaimed = 0
    if not dry_run:
        for path in candidates:
            size = path.stat().st_size
            path.unlink()
            deleted.append(path)
            reclaimed += size
    return CleanupResult(tuple(candidates), tuple(deleted), reclaimed)
