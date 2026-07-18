# Telegrambot

Der Bot verwendet eine lokale Queue pro Mac und einen gemeinsamen PostgreSQL-Medienbestand.

## Installation auf einem weiteren Mac

```bash
git clone <REPOSITORY-URL>
cd telegrambot
./setup_mac.sh
```

Danach `.env` ausfüllen. Jeder Mac benötigt:

- einen eigenen `BOT_TOKEN`, wenn beide Bots gleichzeitig Telegram-Polling verwenden;
- einen eigenen Wert für `TELETHON_SESSION`;
- dieselbe `DATABASE_URL`, damit beide Bots denselben Erledigt-Stand sehen.

Eine neue Telethon-Session wird so erstellt:

```bash
./venv/bin/python create_session.py
```

Bot starten:

```bash
./venv/bin/python bot.py
```

Das Setup erstellt außerdem `~/Applications/Telegrambot Uploader.app`. Diese App
kann per Doppelklick gestartet oder in das Dock gezogen werden. Sie öffnet ein
Terminalfenster mit dem Bot-Log und verhindert einen zweiten gleichzeitigen Start.

Der automatische Watchcheck ist deaktiviert. Er wird ausschließlich über den Button
`Watchcheck` oder den Befehl `/watchcheck` gestartet.

## Nicht im Repository

Folgende Daten bleiben absichtlich lokal:

- `.env`;
- `venv/`;
- Telethon-Sessiondateien;
- `bot/queue.json`, Watchlist und Fortschritt;
- lokale Datenbanken, Downloads und Logs.

Zusätzlich zu den Python-Paketen werden Homebrew, FFmpeg und ffprobe benötigt. Das
macOS-Setup-Skript installiert beziehungsweise prüft diese Abhängigkeiten.
