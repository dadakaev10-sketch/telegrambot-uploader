"""Erstellt eine neue Telethon-Session."""
import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()
API_ID = int(os.environ['API_ID'])
API_HASH = os.environ['API_HASH']
SESSION_NAME = os.getenv('TELETHON_SESSION', 'uploader_session')

async def main():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    me = await client.get_me()
    print(f"\nSession '{SESSION_NAME}' erstellt! Eingeloggt als: {me.first_name} (@{me.username})")
    await client.disconnect()

asyncio.run(main())
