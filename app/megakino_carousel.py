"""Source-only carousel snapshot; no queue, catalog or network side effects."""
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

DEFAULT_PATH = Path(__file__).resolve().parents[1] / 'bot' / 'megakino_carousel.json'


def persist_from_html(html, base_url, path=None):
    """Replace atomically only after a nonempty, recognizable film carousel."""
    items = []
    seen = set()
    soup = BeautifulSoup(html, 'html.parser')
    for clone in soup.select('#owl-carou .owl-item.cloned'):
        clone.decompose()
    for anchor in soup.select('#owl-carou a.top[href]'):
        title = anchor.select_one('.top__title')
        url = urljoin(base_url, anchor['href'])
        if not title or not urlsplit(url).path.startswith('/films/'):
            continue
        text = title.get_text(' ', strip=True)
        if not text or url in seen:
            continue
        seen.add(url)
        items.append({'title': text, 'url': url})
    if not items:
        return False
    target = Path(path if path is not None else os.getenv('MEGAKINO_CAROUSEL_PATH', str(DEFAULT_PATH)))
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {'version': 1, 'source': 'megakino', 'updated_at': datetime.now(timezone.utc).isoformat(), 'items': items}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=target.parent, prefix='.megakino-', delete=False) as f:
            temporary = f.name
            json.dump(payload, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, target)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
    return True
