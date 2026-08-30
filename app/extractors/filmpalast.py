"""Filmpalast source identity."""
import re
from app.extractors.registry import detect_provider


def is_filmpalast_url(url: str) -> bool:
    return detect_provider(url) == 'filmpalast'


def is_series_episode(url: str) -> bool:
    return is_filmpalast_url(url) and bool(re.search(r'-s\d{2}e\d{2}', url, re.I))
