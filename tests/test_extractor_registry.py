from app.extractors.registry import detect_provider


def test_detects_supported_sources():
    assert detect_provider('https://aniworld.to/anime/stream/test') == 'aniworld'
    assert detect_provider('https://filmpalast.to/stream/test') == 'filmpalast'
    assert detect_provider('https://voe.sx/e/abc') == 'voe'
    assert detect_provider('https://doodstream.com/e/abc') == 'doodstream'


def test_unknown_source_returns_none():
    assert detect_provider('https://example.com/video') is None
