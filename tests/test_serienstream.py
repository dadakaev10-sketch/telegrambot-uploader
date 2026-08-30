from app.extractors.serienstream import (
    EpisodeIdentity,
    is_seriesstream_url,
    normalize_seriesstream_url,
    parse_episode_identity,
)


def test_recognizes_current_and_legacy_hosts():
    assert is_seriesstream_url('https://serienstream.to/serie/test')
    assert is_seriesstream_url('https://s.to/serie/test')
    assert not is_seriesstream_url('https://example.com/serie/test')


def test_normalizes_legacy_host_without_losing_path_or_query():
    result = normalize_seriesstream_url('https://s.to/serie/test/staffel-1?x=1')
    assert result == 'https://serienstream.to/serie/test/staffel-1?x=1'


def test_parses_episode_identity():
    identity = parse_episode_identity(
        'https://serienstream.to/serie/swat/staffel-7/episode-22'
    )
    assert identity == EpisodeIdentity(
        'https://serienstream.to/serie/swat', 7, 22
    )
