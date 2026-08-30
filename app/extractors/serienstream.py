"""SerienStream URL identity and canonical episode parsing."""
from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urlparse, urlsplit, urlunsplit


HOSTS = {'s.to', 'www.s.to', 'serienstream.to', 'www.serienstream.to'}


@dataclass(frozen=True)
class EpisodeIdentity:
    series_base_url: str
    season: int
    episode: int


def is_seriesstream_url(url: str) -> bool:
    try:
        return (urlparse(url).hostname or '').lower() in HOSTS
    except (TypeError, ValueError):
        return False


def normalize_seriesstream_url(url: str) -> str:
    if not is_seriesstream_url(url):
        return url
    parsed = urlsplit(url)
    return urlunsplit((
        parsed.scheme or 'https',
        'serienstream.to',
        parsed.path,
        parsed.query,
        parsed.fragment,
    ))


def parse_episode_identity(url: str) -> EpisodeIdentity | None:
    normalized = normalize_seriesstream_url(url).rstrip('/')
    match = re.search(r'/staffel-(\d+)/episode-(\d+)(?:/|$)', normalized, re.I)
    if not match:
        return None
    return EpisodeIdentity(
        series_base_url=normalized[:match.start()],
        season=int(match.group(1)),
        episode=int(match.group(2)),
    )
