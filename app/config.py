"""Validated application settings loaded from environment variables."""
from __future__ import annotations

from dataclasses import dataclass
import os


_TRUE = {'1', 'true', 'yes', 'on'}


@dataclass(frozen=True)
class Settings:
    bot_token: str
    api_id: int
    api_hash: str
    channel_id: int | str
    database_url: str
    central_queue_role: str
    run_telegram_polling: bool
    allowed_chat_ids: frozenset[int]
    pipeline_concurrency: int


def _required(name: str) -> str:
    value = os.getenv(name, '').strip()
    if not value:
        raise ValueError(f'{name} fehlt')
    return value


def load_settings() -> Settings:
    channel_raw = _required('CHANNEL_ID')
    role = os.getenv('CENTRAL_QUEUE_ROLE', 'local').strip().lower()
    if role not in {'local', 'producer', 'worker'}:
        raise ValueError('CENTRAL_QUEUE_ROLE muss local, producer oder worker sein')
    database_url = os.getenv('DATABASE_URL', '').strip()
    if role != 'local' and not database_url:
        raise ValueError('DATABASE_URL wird für die zentrale Queue benötigt')
    try:
        allowed = frozenset(
            int(value.strip())
            for value in os.getenv('ALLOWED_CHAT_IDS', '').split(',')
            if value.strip()
        )
    except ValueError as error:
        raise ValueError('ALLOWED_CHAT_IDS muss numerische IDs enthalten') from error
    polling = os.getenv('RUN_TELEGRAM_POLLING', 'true').lower() in _TRUE
    if polling and not allowed:
        raise ValueError('Telegram-Polling benötigt ALLOWED_CHAT_IDS')
    try:
        pipeline = max(1, min(4, int(os.getenv('PIPELINE_CONCURRENCY', '2'))))
    except ValueError as error:
        raise ValueError('PIPELINE_CONCURRENCY muss eine Zahl von 1 bis 4 sein') from error
    return Settings(
        bot_token=_required('BOT_TOKEN'),
        api_id=int(_required('API_ID')),
        api_hash=_required('API_HASH'),
        channel_id=int(channel_raw) if channel_raw.lstrip('-').isdigit() else channel_raw,
        database_url=database_url,
        central_queue_role=role,
        run_telegram_polling=polling,
        allowed_chat_ids=allowed,
        pipeline_concurrency=pipeline,
    )
