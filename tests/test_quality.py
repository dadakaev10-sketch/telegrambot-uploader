from app.media.quality import (
    HlsVariant,
    choose_hls_variant,
    one_time_preferred_height,
    parse_hls_variants,
)


def test_selects_highest_variant_at_or_below_limit():
    variants = [
        HlsVariant('360.m3u8', 360, 800),
        HlsVariant('480.m3u8', 480, 1200),
        HlsVariant('720.m3u8', 720, 2400),
    ]
    assert choose_hls_variant(variants, 480).url == '480.m3u8'


def test_selects_lowest_source_when_none_matches_limit():
    variants = [
        HlsVariant('720.m3u8', 720, 2400),
        HlsVariant('1080.m3u8', 1080, 5000),
    ]
    assert choose_hls_variant(variants, 480).url == '720.m3u8'


def test_parses_relative_and_absolute_hls_variants():
    playlist = '''#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=854x480
480/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2500,RESOLUTION=1280x720
https://cdn.example/720.m3u8
'''
    variants = parse_hls_variants('https://host.example/path/master.m3u8', playlist)
    assert [(v.height, v.bandwidth, v.url) for v in variants] == [
        (480, 1000, 'https://host.example/path/480/index.m3u8'),
        (720, 2500, 'https://cdn.example/720.m3u8'),
    ]


def test_swat_exception_only_matches_seasons_one_to_seven():
    assert one_time_preferred_height('S.W.A.T. S01E01') == 480
    assert one_time_preferred_height('SWAT S07E22') == 480
    assert one_time_preferred_height('S.W.A.T. S08E01') is None
    assert one_time_preferred_height('Andere Serie S01E01') is None


def test_quality_override_can_be_configured(monkeypatch):
    monkeypatch.setenv(
        'QUALITY_OVERRIDES_JSON',
        '[{"pattern":"^testseries01e","height":360}]',
    )
    assert one_time_preferred_height('Testserie S01E03') == 360
    assert one_time_preferred_height('S.W.A.T. S01E01') is None
