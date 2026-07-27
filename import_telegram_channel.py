#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError

from bot import build_caption


ROOT = Path(__file__).resolve().parent
STATE_PATH = ROOT / "bot" / "telegram_channel_import_state.json"
SOURCE = "Kinderkanal_Serien"


def _target_from_env():
    raw = os.environ["CHANNEL_ID"].strip()
    return int(raw) if raw.lstrip("-").isdigit() else raw


def _clean_series_header(text: str) -> str | None:
    if not re.search(r"(?i)#serien?\b", text):
        return None
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"(?i)#serien?\b", " ", text)
    text = text.replace("⬇️", " ")
    text = re.sub(r"\s+", " ", text).strip(" -–—|:")
    match = re.match(r"(.+?)\s*\(\d{4}(?:-\d{4})?\)", text)
    return match.group(1).strip() if match else None


def _film_filename(text: str) -> str | None:
    text = re.sub(r"(?i)#filme?\b", " ", text)
    text = re.sub(r"(?i)(?:disney\s*)?#pixar(?:'s)?", " ", text)
    text = re.sub(r"(?i)#disney(?:'s)?", " ", text)
    text = text.replace("⬇️", " ")
    text = re.sub(r"\s+", " ", text).strip(" -–—|:")
    match = re.match(r"(.+?)\s*\((\d{4})(?:-\d{4})?\)", text)
    if not match:
        return None
    title = match.group(1).strip(" -–—|:")
    return f"{title} ({match.group(2)})"


def _episode_number(text: str) -> int | None:
    match = re.match(r"^\s*0*(\d{1,3})\s*(?:[-–—.:]|$)", text)
    return int(match.group(1)) if match else None


def _named_episode(text: str) -> tuple[str, int] | None:
    match = re.match(
        r"^\s*(.+?)\s+E0*(\d{1,3})\s*(?:[-–—.:]|$)",
        text,
        re.IGNORECASE,
    )
    return (match.group(1).strip(), int(match.group(2))) if match else None


def _load_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "source": SOURCE,
            "last_message_id": 0,
            "sent": 0,
            "skipped": 0,
            "current_series": None,
        }


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(STATE_PATH)
    os.chmod(STATE_PATH, 0o600)


async def _send_with_flood_wait(client, target, media, caption: str):
    while True:
        try:
            return await client.send_file(
                target,
                media,
                caption=caption,
                supports_streaming=True,
            )
        except FloodWaitError as error:
            delay = int(error.seconds) + 2
            print(f"[RATE LIMIT] Telegram verlangt {delay}s Pause", flush=True)
            await asyncio.sleep(delay)


async def run(dry_run: bool) -> None:
    load_dotenv(ROOT / ".env")
    session = os.getenv("TELETHON_SESSION", "uploader_session")
    client = TelegramClient(
        str(ROOT / session),
        int(os.environ["API_ID"]),
        os.environ["API_HASH"],
    )
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError(f"Telethon-Session {session!r} ist nicht angemeldet")

        source = await client.get_entity(SOURCE)
        target = await client.get_entity(_target_from_env())
        messages = [message async for message in client.iter_messages(source, reverse=True)]

        group_captions: dict[int, str] = {}
        grouped = defaultdict(list)
        for message in messages:
            if message.grouped_id:
                grouped[message.grouped_id].append(message)
        for group_id, members in grouped.items():
            group_captions[group_id] = next(
                (
                    (member.message or "").strip()
                    for member in members
                    if (member.message or "").strip()
                ),
                "",
            )

        state = _load_state()
        if state.get("source") != SOURCE:
            raise RuntimeError("Checkpoint gehört zu einer anderen Quelle")
        current_series = state.get("current_series")
        planned: list[tuple[object, str]] = []
        skipped_reasons = defaultdict(int)

        for message in messages:
            own_caption = (message.message or "").strip()
            effective_caption = own_caption or (
                group_captions.get(message.grouped_id, "") if message.grouped_id else ""
            )
            series_header = _clean_series_header(own_caption)
            if series_header:
                current_series = series_header
            elif re.search(r"(?i)#filme?\b", own_caption):
                current_series = None

            if message.id <= int(state.get("last_message_id", 0)):
                continue

            extension = (message.file.ext or "").lower() if message.file else ""
            mime_type = (message.file.mime_type or "").lower() if message.file else ""
            if not message.video or extension != ".mp4" or mime_type != "video/mp4":
                skipped_reasons["kein_abspielbares_mp4"] += 1
                continue
            if not effective_caption:
                skipped_reasons["keine_caption"] += 1
                continue

            episode = _episode_number(effective_caption)
            if episode is not None and current_series:
                filename = f"{current_series} S01E{episode:02d}"
                caption = build_caption(filename, "serie")
            else:
                named_episode = _named_episode(effective_caption)
                if named_episode:
                    name, episode = named_episode
                    caption = build_caption(
                        f"{name} S01E{episode:02d}",
                        "serie",
                    )
                else:
                    film_filename = _film_filename(effective_caption)
                    if not film_filename:
                        skipped_reasons["nicht_normalisierbar"] += 1
                        continue
                    caption = build_caption(film_filename, "film")
                    current_series = None

            planned.append((message, caption))

        print(
            f"Quelle: {getattr(source, 'title', SOURCE)}\n"
            f"Ziel: {getattr(target, 'title', None) or getattr(target, 'username', None) or getattr(target, 'first_name', target)}\n"
            f"Checkpoint: Nachricht #{state.get('last_message_id', 0)}\n"
            f"Geplant: {len(planned)} Videos\n"
            f"Übersprungen: {dict(skipped_reasons)}",
            flush=True,
        )
        if dry_run:
            for message, caption in planned[:10]:
                print(f"  #{message.id}: {caption}", flush=True)
            return

        delay = max(0.5, float(os.getenv("TELEGRAM_IMPORT_DELAY_SECONDS", "1.5")))
        for index, (message, caption) in enumerate(planned, 1):
            await _send_with_flood_wait(client, target, message.media, caption)
            state.update(
                {
                    "source": SOURCE,
                    "target": str(_target_from_env()),
                    "last_message_id": message.id,
                    "sent": int(state.get("sent", 0)) + 1,
                    "current_series": current_series,
                }
            )
            _save_state(state)
            print(
                f"[{index}/{len(planned)}] #{message.id} → {caption}",
                flush=True,
            )
            await asyncio.sleep(delay)
    finally:
        await client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Nur prüfen; keine Telegram-Nachrichten senden",
    )
    args = parser.parse_args()
    asyncio.run(run(args.dry_run))


if __name__ == "__main__":
    main()
