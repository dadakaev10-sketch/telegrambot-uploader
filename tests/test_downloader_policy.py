import pytest

from app.media.downloader import DownloadPolicy, policy_from_payload


def test_ytdlp_height_selector():
    policy = DownloadPolicy('max_height', 480, False)
    assert policy.ytdlp_format() == 'bestvideo[height<=480]+bestaudio/best[height<=480]'


def test_source_mode_does_not_force_format():
    assert DownloadPolicy('source', 480, False).ytdlp_format() is None


def test_payload_defaults_to_non_transcoding_height_policy():
    policy = policy_from_payload({'preferred_height': 720})
    assert policy.quality_mode == 'max_height'
    assert policy.allow_transcode is False


def test_invalid_quality_mode_is_rejected():
    with pytest.raises(ValueError):
        policy_from_payload({'quality_mode': 'magic'})
