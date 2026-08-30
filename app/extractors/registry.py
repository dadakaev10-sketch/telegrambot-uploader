"""Source-provider detection registry."""
from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True)
class Provider:
    name: str
    host_markers: tuple[str, ...]
    path_markers: tuple[str, ...] = ()

    def matches(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except (TypeError, ValueError):
            return False
        host = (parsed.hostname or '').lower()
        path = parsed.path.lower()
        host_match = any(marker in host for marker in self.host_markers)
        path_match = not self.path_markers or any(marker in path for marker in self.path_markers)
        return host_match and path_match


PROVIDERS: tuple[Provider, ...] = (
    Provider('aniworld', ('aniworld.to',)),
    Provider('serienstream', ('serienstream.to', 's.to')),
    Provider('filmpalast', ('filmpalast.to',)),
    Provider('megakino', ('megakino',)),
    Provider('kinoger', ('kinoger', 'kino-ger')),
    Provider('voe', ('voe.',), ('/e/', '/embed-')),
    Provider('doodstream', ('dood', 'd0od', 'dr0pstream', 'dropstream')),
)


def detect_provider(url: str) -> str | None:
    return next((provider.name for provider in PROVIDERS if provider.matches(url)), None)
