import ast
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import json
import pytest

from app import megakino_carousel as carousel


def test_ordered_snapshot_and_failed_parse_preserves_last_good(tmp_path):
    path = tmp_path / 'snapshot.json'
    html = '''<div id="owl-carou"><a class="top" href="/films/2-b.html"><div class="top__title">B</div></a><a class="top" href="/films/1-a.html"><div class="top__title">A</div></a><a class="top" href="/films/2-b.html"><div class="top__title">B</div></a></div>'''
    assert carousel.persist_from_html(html, 'https://example.org/', path)
    saved = path.read_bytes()
    assert [i['title'] for i in json.loads(saved)['items']] == ['B', 'A']
    assert not carousel.persist_from_html('<html>challenge</html>', 'https://example.org/', path)
    assert path.read_bytes() == saved


def test_real_getter_publishes_redirected_carousel_without_queue(tmp_path, monkeypatch):
    path = tmp_path / 'snapshot.json'
    monkeypatch.setenv('MEGAKINO_CAROUSEL_PATH', str(path))
    html = '<div id="owl-carou"><a class="top" href="/films/1-a.html"><div class="top__title">A</div></a></div>'
    page = SimpleNamespace(url='https://redirect.example/', goto=AsyncMock(), wait_for_selector=AsyncMock(), content=AsyncMock(return_value=html))
    browser = SimpleNamespace(new_context=AsyncMock(return_value=SimpleNamespace(new_page=AsyncMock(return_value=page))), close=AsyncMock())
    @asynccontextmanager
    async def opened(**kwargs):
        yield browser
    async def no_sleep(*args):
        pass
    source = Path(__file__).resolve().parents[1] / 'bot.py'
    node = next(n for n in ast.parse(source.read_text()).body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'get_megakino_last_added')
    namespace = dict(MEGAKINO_HOME_URL='https://old.example/', logging=logging, _open_browser=opened,
                     asyncio=SimpleNamespace(sleep=no_sleep), _parse_megakino_last_added_html=lambda html, base: [])
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), 'exec'), namespace)
    assert asyncio.run(namespace['get_megakino_last_added']()) == []
    assert json.loads(path.read_text())['items'] == [{'title': 'A', 'url': 'https://redirect.example/films/1-a.html'}]
    saved = path.read_bytes()
    page.goto.side_effect = RuntimeError('source unavailable')
    with pytest.raises(RuntimeError):
        asyncio.run(namespace['get_megakino_last_added']())
    assert path.read_bytes() == saved


def test_owl_clones_do_not_rotate_source_order(tmp_path):
    path = tmp_path / 'snapshot.json'
    card = lambda title: f'<a class="top" href="/films/{title}.html"><div class="top__title">{title}</div></a>'
    html = '<div id="owl-carou"><div class="owl-item cloned">'+card('B')+'</div><div class="owl-item">'+card('A')+'</div><div class="owl-item">'+card('B')+'</div><div class="owl-item cloned">'+card('A')+'</div></div>'
    assert carousel.persist_from_html(html, 'https://example.org/', path)
    assert [i['title'] for i in json.loads(path.read_text())['items']] == ['A','B']


def test_failed_atomic_replace_preserves_snapshot(tmp_path, monkeypatch):
    path = tmp_path / 'snapshot.json'
    path.write_text('last good')
    def fail(*args):
        raise OSError('publication denied')
    monkeypatch.setattr(carousel.os, 'replace', fail)
    with pytest.raises(OSError):
        carousel.persist_from_html('<div id="owl-carou"><a class="top" href="/films/1-a.html"><div class="top__title">A</div></a></div>', 'https://example.org/', path)
    assert path.read_text() == 'last good'
    assert list(tmp_path.iterdir()) == [path]
