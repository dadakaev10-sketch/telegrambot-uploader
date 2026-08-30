import pytest

from app.config import load_settings


BASE = {
    'BOT_TOKEN': 'token',
    'API_ID': '123',
    'API_HASH': 'hash',
    'CHANNEL_ID': '-100123',
    'DATABASE_URL': 'postgresql://example.invalid/db',
    'CENTRAL_QUEUE_ROLE': 'worker',
    'RUN_TELEGRAM_POLLING': 'true',
    'ALLOWED_CHAT_IDS': '42,43',
    'PIPELINE_CONCURRENCY': '2',
}


def apply(monkeypatch, values=BASE):
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_load_settings_validates_and_converts(monkeypatch):
    apply(monkeypatch)
    settings = load_settings()
    assert settings.api_id == 123
    assert settings.channel_id == -100123
    assert settings.allowed_chat_ids == frozenset({42, 43})
    assert settings.pipeline_concurrency == 2


def test_polling_requires_allowlist(monkeypatch):
    apply(monkeypatch)
    monkeypatch.setenv('ALLOWED_CHAT_IDS', '')
    with pytest.raises(ValueError, match='ALLOWED_CHAT_IDS'):
        load_settings()


def test_remote_role_requires_database(monkeypatch):
    apply(monkeypatch)
    monkeypatch.setenv('DATABASE_URL', '')
    with pytest.raises(ValueError, match='DATABASE_URL'):
        load_settings()
