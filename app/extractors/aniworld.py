"""AniWorld source identity."""
from app.extractors.registry import detect_provider


def is_aniworld_url(url: str) -> bool:
    return detect_provider(url) == 'aniworld'
