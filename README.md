# Telegrambot Uploader

Telegram-Uploader mit lokaler Queue, gemeinsamem PostgreSQL-Medienbestand und
Getrennte Browser-Backends: Camoufox wird ausschließlich für SerienStream
verwendet. Filmpalast, Megakino, AniWorld und Browser-Downloads laufen über
Playwright Chromium.

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

`ALLOWED_CHAT_IDS` muss die eigene Telegram-Chat-ID enthalten. Ohne diese
Allowlist startet ein Bot mit aktivem Telegram-Polling absichtlich nicht. So
können fremde Nutzer keine Links, Imports oder Watchchecks auslösen.

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
CENTRAL_QUEUE_AUTOSTART=false
RUN_TELEGRAM_POLLING=true
```

Beide Geräte verwenden dieselbe `DATABASE_URL`, aber weiterhin eigene
`BOT_TOKEN`- und `TELETHON_SESSION`-Werte. Der Worker startet nach Tippen auf
**▶️ Remote Queue Download** und fragt PostgreSQL danach alle fünf Sekunden ab.
Mit `CENTRAL_QUEUE_AUTOSTART=true` kann der frühere automatische Start wieder
aktiviert werden. Fehlgeschlagene Jobs werden mit wachsendem Abstand wieder
freigegeben; dauerhaft gelöschte Medien landen im Status `dead`. Nach einem
Worker-Absturz kann ein reservierter Job nach Ablauf der Lease von einem
anderen Worker übernommen werden.

Die Worker-ID bleibt standardmäßig pro Rechner stabil. Nach einem Bot-Neustart
werden Jobs, die derselbe Rechner beim Abbruch als `processing` hinterlassen
hat, sofort wieder auf `queued` gesetzt. Dadurch muss der Rechner nicht mehr
auf den Ablauf der Lease warten. Bei mehreren unterschiedlichen Bots auf
demselben Rechner muss jeder Bot eine eigene `CENTRAL_QUEUE_WORKER_ID` erhalten.

## Lokal extrahieren, danach remote laden

Ein normaler lokaler Bot darf `CENTRAL_QUEUE_ROLE=local` behalten. Dieser
Befehl extrahiert zuerst alle fehlenden VOE-Links einer SerienStream-Serie:

```text
/centralserie https://serienstream.to/serie/serienname
```

Während der Extraktion wird noch kein Job veröffentlicht. Erst wenn alle
VOE-Links erfolgreich gesammelt wurden, schreibt der Bot sie gemeinsam in
einer PostgreSQL-Transaktion in die zentrale Queue. Ein Remote-Bot mit
`CENTRAL_QUEUE_ROLE=worker` beginnt deshalb erst danach mit dem Download.

Ein direkt in den Telegram-Chat gesendeter SerienStream-Link startet
automatisch die Camoufox-Extraktion. Er landet nicht in der lokalen
`queue.json`: In PostgreSQL werden nur die aufgelösten VOE-Links gespeichert.
Camoufox lädt keine Videodatei herunter. Filmpalast, Megakino und AniWorld
nutzen diesen Ablauf nicht und laufen weiterhin direkt über Playwright
Chromium.

Standardmäßig extrahiert Camoufox bis zu vier SerienStream-Folgen gleichzeitig.
Der Wert ist über `SERIENSTREAM_EXTRACTION_CONCURRENCY=4` konfigurierbar
(zulässig: 1 bis 8). Download und Upload aus der Remote Queue bleiben davon
getrennt.

Mit **🗑 Remote Queue leeren** werden nach einer Sicherheitsabfrage alle
offenen PostgreSQL-Jobs entfernt – einschließlich `processing`-Jobs und noch
nicht freigegebener Staffel-Entwürfe. Ein auf einem anderen Rechner bereits
physisch gestarteter Download kann dort noch bis zum Abschluss oder nächsten
Fehler weiterlaufen, ist danach aber kein offener PostgreSQL-Auftrag mehr.

Mit **⚡ Serie vorreihen** wird die anschließend gesendete SerienStream-Serie
mit hoher Priorität eingetragen. Ein bereits aktiver Download läuft zu Ende;
danach werden die priorisierten Folgen vor allen normal wartenden Jobs
reserviert.
