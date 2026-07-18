# Telegrambot Camoufox

Telegram-Uploader mit lokaler Queue, gemeinsamem PostgreSQL-Medienbestand und
Camoufox als Browser-Backend.

## Installation auf einem Mac

```bash
git clone https://github.com/dadakaev10-sketch/telegrambot-uploader.git
cd telegrambot-uploader
./setup_mac.sh
```

Danach `.env` ausfüllen und eine eigene Telethon-Session erstellen:

```bash
./venv/bin/python create_session.py
```

Start per Terminal:

```bash
./venv/bin/python bot.py
```

Das Setup erzeugt außerdem `~/Applications/Telegrambot Camoufox.app`. Die App
kann per Doppelklick gestartet oder ins Dock gezogen werden.

Der automatische Watchcheck ist deaktiviert. Die Watchlist wird nur über den
Button `Watchcheck` oder `/watchcheck` geprüft.

Lokale Zugangsdaten, Sessions, Queue, Watchlist, Datenbanken, Downloads und Logs
werden nicht über GitHub übertragen.
# Zentrale PostgreSQL-Queue

Die GitHub-Version kann Eingabe und Download auf zwei Rechner verteilen. Die
Jobs liegen dann in PostgreSQL; `FOR UPDATE SKIP LOCKED` stellt sicher, dass ein
Job nur von einem Worker gleichzeitig reserviert wird.

Auf dem Bot, dem du fertige VOE-Links sendest:

```env
CENTRAL_QUEUE_ROLE=producer
RUN_TELEGRAM_POLLING=true
```

Auf dem Remote-Laptop, der herunterladen und hochladen soll:

```env
CENTRAL_QUEUE_ROLE=worker
CENTRAL_QUEUE_WORKER_ID=remote-macbook-1
RUN_TELEGRAM_POLLING=false
```

Beide Geräte verwenden dieselbe `DATABASE_URL`, aber weiterhin eigene
`BOT_TOKEN`- und `TELETHON_SESSION`-Werte. Der Worker fragt PostgreSQL alle fünf
Sekunden ab. Fehlgeschlagene Jobs werden mit wachsendem Abstand wieder
freigegeben; dauerhaft gelöschte Medien landen im Status `dead`. Nach einem
Worker-Absturz kann ein reservierter Job nach Ablauf der Lease von einem
anderen Worker übernommen werden.
