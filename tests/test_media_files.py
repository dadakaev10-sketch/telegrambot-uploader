from pathlib import Path

from app.media.files import cleanup_stale_temp_files, stale_temp_candidates


def test_stale_cleanup_respects_age_and_protection(tmp_path: Path):
    old_ts = tmp_path / 'old.ts'
    old_part = tmp_path / 'protected.part'
    fresh_ts = tmp_path / 'fresh.ts'
    normal_video = tmp_path / 'movie.mp4'
    for path in (old_ts, old_part, fresh_ts, normal_video):
        path.write_bytes(b'x' * 10)
    old_time = 1000
    old_ts.touch()
    old_part.touch()
    import os
    os.utime(old_ts, (old_time, old_time))
    os.utime(old_part, (old_time, old_time))

    candidates = stale_temp_candidates(
        tmp_path, 100, protected_paths={old_part}, now=2000
    )
    assert candidates == [old_ts]
    assert fresh_ts.exists() and normal_video.exists()


def test_cleanup_defaults_to_dry_run(tmp_path: Path):
    stale = tmp_path / 'video.part'
    stale.write_bytes(b'12345')
    import os
    os.utime(stale, (1000, 1000))
    result = cleanup_stale_temp_files(tmp_path, 100, dry_run=True)
    assert stale in result.candidates
    assert not result.deleted
    assert stale.exists()


def test_cleanup_deletes_candidates_when_enabled(tmp_path: Path):
    stale = tmp_path / 'video.ts'
    stale.write_bytes(b'12345')
    import os
    os.utime(stale, (1000, 1000))
    result = cleanup_stale_temp_files(tmp_path, 100, dry_run=False)
    assert result.deleted == (stale,)
    assert result.reclaimed_bytes == 5
    assert not stale.exists()
