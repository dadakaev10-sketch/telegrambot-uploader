"""VOE and Dood-compatible player source identity."""
import re
from urllib.parse import urlparse


def is_remote_player_url(url: str | None) -> bool:
    if not url or not url.startswith(('http://', 'https://')):
        return False
    parsed = urlparse(url)
    host = (parsed.hostname or '').lower()
    path = parsed.path.lower()
    return (
        bool(re.match(r'^/(?:e|d)/[^/]+/?$', path))
        or bool(re.match(r'^/embed-[^/]+\.html$', path))
        or ('voe.' in host and bool(path.strip('/')))
        or (
            any(marker in host for marker in ('dood', 'd0od', 'dr0pstream', 'dropstream'))
            and bool(path.strip('/'))
        )
    )
