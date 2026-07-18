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
