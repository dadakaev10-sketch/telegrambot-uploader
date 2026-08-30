"""Bounded journal rotation."""
from __future__ import annotations

from pathlib import Path
import os


def rotate_file(path: Path, max_bytes: int, backups: int = 3) -> bool:
    if max_bytes < 1 or backups < 1:
        raise ValueError('max_bytes und backups müssen positiv sein')
    if not path.exists() or path.stat().st_size <= max_bytes:
        return False
    oldest = path.with_name(f'{path.name}.{backups}')
    if oldest.exists():
        oldest.unlink()
    for index in range(backups - 1, 0, -1):
        source = path.with_name(f'{path.name}.{index}')
        target = path.with_name(f'{path.name}.{index + 1}')
        if source.exists():
            os.replace(source, target)
    os.replace(path, path.with_name(f'{path.name}.1'))
    path.touch(mode=0o600)
    return True
