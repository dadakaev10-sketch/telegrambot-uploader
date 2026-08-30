from pathlib import Path

import pytest

from app.maintenance import rotate_file


def test_rotate_file_keeps_bounded_backups(tmp_path: Path):
    journal = tmp_path / 'queue.jsonl'
    journal.write_bytes(b'x' * 20)
    assert rotate_file(journal, max_bytes=10, backups=2)
    assert journal.exists() and journal.stat().st_size == 0
    assert (tmp_path / 'queue.jsonl.1').stat().st_size == 20

    journal.write_bytes(b'y' * 20)
    rotate_file(journal, max_bytes=10, backups=2)
    assert (tmp_path / 'queue.jsonl.2').read_bytes() == b'x' * 20


def test_rotation_validates_limits(tmp_path: Path):
    with pytest.raises(ValueError):
        rotate_file(tmp_path / 'x', 0)
