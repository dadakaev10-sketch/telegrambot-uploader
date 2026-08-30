#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import dataclass

import psycopg
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
load_dotenv(os.path.join(ROOT, ".env"))

import bot  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


@dataclass
class StatusMsg:
    title: str

    async def edit_text(self, text: str, **kwargs):
        first_line = (text or "").splitlines()[0] if text else ""
        logging.info("[%s] %s", self.title, first_line[:180])


def load_collecting_sources() -> list[dict]:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL fehlt")
    with psycopg.connect(database_url, connect_timeout=10) as con:
        rows = con.execute(
            """
            SELECT source_url,
                   max(chat_id) AS chat_id,
                   min(id) AS first_batch_id,
                   string_agg(title, ', ' ORDER BY id) AS titles,
                   count(*) AS batch_count
            FROM download_batches
            WHERE status='collecting'
            GROUP BY source_url
            ORDER BY min(id)
            """
        ).fetchall()
    return [
        {
            "source_url": row[0],
            "chat_id": row[1] or int(os.getenv("ADMIN_CHAT_ID", "7496298562")),
            "first_batch_id": row[2],
            "titles": row[3],
            "batch_count": row[4],
        }
        for row in rows
    ]


async def main() -> None:
    sources = load_collecting_sources()
    logging.info("Starte erneute Extraktion für %s Serien-Quelle(n)", len(sources))
    ok = 0
    failed = 0
    for index, item in enumerate(sources, 1):
        source_url = item["source_url"]
        title = item["titles"]
        logging.info(
            "[%s/%s] Retry source=%s batches=%s titles=%s",
            index,
            len(sources),
            source_url,
            item["batch_count"],
            title,
        )
        try:
            await bot._extract_central_series(
                app=None,
                chat_id=int(item["chat_id"]),
                series_url=source_url,
                priority=0,
                is_anime=False,
                status_msg=StatusMsg(title=title),
            )
            ok += 1
        except Exception:
            failed += 1
            logging.exception("Retry fehlgeschlagen für %s", source_url)
    logging.info("Fertig: ok=%s failed=%s", ok, failed)


if __name__ == "__main__":
    asyncio.run(main())
