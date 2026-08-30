# Verbesserungs- und Modularisierungsplan

**Projekt:** Telegrambot Uploader  
**Stand:** 30. August 2026  
**Status:** Planungsdokument – noch nicht vollständig umgesetzt

## 1. Ausgangslage

Der Bot verfügt bereits über viele robuste Funktionen:

- zentrale PostgreSQL-Queue mit atomarem Claiming
- Worker-Leases und Heartbeats
- Retry-Mechanismus mit exponentiellem Backoff
- Dead-Letter-Behandlung
- Deduplizierung offener Jobs
- Telegram-Chat-Allowlist
- parallele Download-/Upload-Pipeline
- Extraktion aus SerienStream, AniWorld, Filmpalast, Megakino und weiteren Quellen
- HLS-, yt-dlp- und FFmpeg-Downloadpfade
- Telethon-Upload
- Queue-Sortierung, Löschen, „vorhanden“-Markierung und Pause/Fortsetzen

Der größte aktuelle Schwachpunkt ist die stark gewachsene Gesamtstruktur:

- `bot.py`: etwa 8.669 Zeilen
- etwa 189 Funktionen
- `get_stream_url()`: etwa 982 Zeilen
- `get_series_episodes()`: etwa 428 Zeilen
- `process_video()`: etwa 351 Zeilen
- `_watchlist_check_once()`: etwa 305 Zeilen
- zahlreiche breite `except Exception`-Blöcke
- keine reguläre automatisierte Testsuite

Das zentrale Ziel ist deshalb eine schrittweise Modularisierung ohne riskanten Komplettumbau.

---

## 2. Höchste Priorität: parallele Worker vollständig trennen

### Problem

Die zwei parallelen Worker teilen sich derzeit globale Laufzeitvariablen:

```python
_active_proc
_dl_status
_dl_active
```

Mögliche Folgen:

- Worker überschreiben gegenseitig ihren Status.
- `/status` zeigt gemischte oder falsche Informationen.
- „Aktiven Download abbrechen“ kann den falschen Subprozess beenden.
- `_active_proc` kann auf `None` gesetzt werden, obwohl der andere Worker noch läuft.

### Ziel

Jeder aktive Job erhält einen eigenen Laufzeitstatus.

```python
@dataclass
class ActiveJob:
    worker_id: int
    queue_item: QueueItem
    process: asyncio.subprocess.Process | None = None
    phase: str = "starting"
    filename: str | None = None
    size_mb: float = 0
    percent: int | None = None
    started_at: float = field(default_factory=time.monotonic)
```

Zentrale Verwaltung:

```python
active_jobs: dict[int, ActiveJob] = {}
```

### Akzeptanzkriterien

- `/status` zeigt alle aktiven Slots getrennt.
- Jeder Abbruch-Button referenziert eine eindeutige Job-ID.
- Ein Abbruch beendet ausschließlich den zugehörigen Prozess.
- Ein Worker kann den Status eines anderen Workers nicht überschreiben.

---

## 3. Kontrolliertes Herunterfahren

### Problem

Ein systemd-Neustart kann laufende Downloads oder Telethon-Uploads abrupt mit `SIGINT` beenden. Die Queue kann die Jobs wiederherstellen, aber große Dateien und Upload-Arbeit gehen teilweise verloren.

### Ziel

Graceful Shutdown:

1. keine neuen Jobs mehr claimen
2. Telegram-Polling stoppen
3. laufende Jobs innerhalb eines definierten Zeitfensters abschließen lassen
4. Heartbeats bis zum tatsächlichen Ende weiterführen
5. nach Timeout kontrolliert abbrechen
6. Telethon sauber trennen

### Umsetzungsidee

```python
shutdown_event = asyncio.Event()
accept_new_jobs = True
```

Bei `SIGTERM` oder `SIGINT`:

```python
accept_new_jobs = False
shutdown_event.set()
await wait_for_active_jobs(timeout=600)
await telegram_client.disconnect()
```

### Systemd

Prüfen, ob `KillSignal=SIGTERM` geeigneter als `SIGINT` ist. `TimeoutStopSec` muss zum Graceful-Shutdown-Zeitfenster passen.

### Akzeptanzkriterien

- Neustart reserviert keine neuen Jobs.
- Laufende Uploads werden nicht sofort abgebrochen.
- Nach Timeout fährt der Dienst zuverlässig herunter.
- Keine verwaisten Heartbeats oder Locks.

---

## 4. Zielstruktur der Modularisierung

```text
telegrambot-uploader/
├── main.py
├── requirements.txt
├── .env.example
│
├── app/
│   ├── __init__.py
│   ├── config.py
│   ├── logging_setup.py
│   ├── runtime.py
│   │
│   ├── telegram/
│   │   ├── __init__.py
│   │   ├── application.py
│   │   ├── commands.py
│   │   ├── callbacks.py
│   │   ├── remote_queue_ui.py
│   │   └── messages.py
│   │
│   ├── queue/
│   │   ├── __init__.py
│   │   ├── central_queue.py
│   │   ├── worker.py
│   │   ├── models.py
│   │   └── retry_policy.py
│   │
│   ├── media/
│   │   ├── __init__.py
│   │   ├── downloader.py
│   │   ├── uploader.py
│   │   ├── ffmpeg.py
│   │   ├── metadata.py
│   │   ├── quality.py
│   │   └── files.py
│   │
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── voe.py
│   │   ├── serienstream.py
│   │   ├── aniworld.py
│   │   ├── filmpalast.py
│   │   ├── megakino.py
│   │   └── kinoger.py
│   │
│   ├── database/
│   │   ├── __init__.py
│   │   ├── media_repository.py
│   │   ├── migrations.py
│   │   └── models.py
│   │
│   └── watchlist/
│       ├── __init__.py
│       ├── repository.py
│       ├── checker.py
│       └── scheduler.py
│
└── tests/
    ├── test_quality.py
    ├── test_queue.py
    ├── test_titles.py
    ├── test_extractors.py
    └── test_remote_queue_ui.py
```

### Vorgesehene `main.py`

```python
import asyncio

from app.config import load_settings
from app.logging_setup import configure_logging
from app.telegram.application import create_application
from app.runtime import BotRuntime


async def main() -> None:
    settings = load_settings()
    configure_logging(settings)

    runtime = BotRuntime(settings)
    await runtime.initialize()

    application = create_application(runtime)
    await runtime.run(application)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## 5. Sichere Reihenfolge der Modularisierung

### Phase 1: reine Hilfsfunktionen

Zuerst Funktionen ohne globale Laufzeitzustände verschieben:

- Titelnormalisierung
- URL-Erkennung
- Qualitätsauswahl
- HLS-Playlist-Parsing
- Caption-Erstellung
- Videometadaten
- Statusformatierung

Zielmodule:

```text
app/utils/titles.py
app/utils/urls.py
app/media/quality.py
app/media/metadata.py
```

Diese Phase ist risikoarm und gut testbar.

### Phase 2: Queue und Datenbank

- `CentralQueue` in `app/queue/central_queue.py`
- Statusmodelle in `app/queue/models.py`
- Retry-Regeln in `app/queue/retry_policy.py`
- Medien-DB in ein Repository
- versionierte Migrationen einführen

### Phase 3: Download und Upload

Trennen in:

- `HLSDownloader`
- `YtDlpDownloader`
- `FFmpegDownloader`
- `TelegramUploader`
- `MediaFileManager`

### Phase 4: Quellenextraktoren

Jede Quelle erhält ein eigenes Modul und eine einheitliche Schnittstelle.

```python
class SeriesExtractor(Protocol):
    async def list_episodes(self, url: str) -> list[Episode]: ...
    async def get_player_url(self, episode_url: str) -> str: ...
```

### Phase 5: Telegram-Schicht

- Commands
- Callback-Handler
- Remote-Queue-UI
- Nachrichtentemplates
- Fehlerhandler

### Phase 6: `bot.py` entfernen oder auf Kompatibilitätswrapper reduzieren

Erst wenn alle Module getestet und produktiv verifiziert wurden.

---

## 6. Qualitätsauswahl allgemein konfigurierbar machen

### Problem

Einzelne Qualitätsausnahmen sind aktuell titelabhängig im Code hinterlegt.

### Ziel

Qualität als Teil des Queue-Payloads speichern:

```json
{
  "manual_title": "S.W.A.T. S04E03",
  "preferred_height": 480,
  "quality_mode": "max_height",
  "allow_transcode": false
}
```

Mögliche Qualitätsmodi:

```text
best
max_height
lowest
source
```

Transcoding separat steuern:

```text
allow_transcode=true
allow_transcode=false
```

Für die aktuelle S.W.A.T.-Regel:

```text
preferred_height=480
quality_mode=max_height
allow_transcode=false
```

### Telegram-UI, optional

```text
Qualität wählen:
[Beste] [1080p] [720p] [480p]
```

---

## 7. Automatisierte Tests

### Mindestumfang

#### Queue-Status

```python
def test_pause_and_resume_group():
    ...
```

#### Claiming

```python
def test_paused_job_cannot_be_claimed():
    ...
```

#### Deduplizierung

```python
def test_paused_job_is_still_deduplicated():
    ...
```

#### Staffelgruppierung

```python
def test_all_episodes_of_season_are_grouped():
    ...
```

#### Qualität

```python
def test_best_variant_up_to_480p():
    ...
```

#### Kein Transcoding

```python
def test_select_only_does_not_call_ffmpeg_scale():
    ...
```

#### Telegram-UI

```python
def test_paused_group_shows_resume_button():
    ...
```

#### Worker-Wiederherstellung

```python
def test_processing_job_is_released_after_worker_restart():
    ...
```

#### Parallelität

```python
def test_cancel_job_only_kills_its_own_process():
    ...
```

### Teststrategie

- Unit-Tests für Parser, Qualität und Titel
- Integrationstests für PostgreSQL-Queue
- gemockte Telegram-Callbacks
- gemockte Subprozesse für FFmpeg und yt-dlp
- keine echten Medienuploads in normalen Tests

---

## 8. Alte Staffel-Entwürfe bereinigen

### Befund

Es existieren zahlreiche alte `collecting`-Batches, teilweise seit mehreren Wochen unverändert und ohne gespeicherte Jobs.

### Zielstatus

```text
collecting
ready
completed
cancelled
expired
```

### Bereinigungsregel

```sql
UPDATE download_batches
SET status = 'expired', updated_at = now()
WHERE status = 'collecting'
  AND updated_at < now() - interval '24 hours'
  AND NOT EXISTS (
      SELECT 1
      FROM download_jobs
      WHERE batch_id = download_batches.id
  );
```

### Telegram-Verwaltung

```text
🧩 Offene Extraktionen
[fortsetzen] [erneut versuchen] [löschen]
```

---

## 9. Dateiaufräumung und Speicherverwaltung

### Befund

Das Projektverzeichnis belegt mehrere Gigabyte. Im Downloadordner liegen fertige MP4-Dateien, temporäre `.ts`-Dateien und Debug-Artefakte.

### Ziel

Ein zentraler `MediaFileManager`:

```python
class MediaFileManager:
    def cleanup_success(self, job: Job) -> None: ...
    def cleanup_failed(self, job: Job) -> None: ...
    def cleanup_stale_temp_files(self) -> None: ...
    def available_disk_space(self) -> int: ...
```

### Regeln

- `.ts`, `.part`, `_scaled.mp4` nach einer definierten Frist löschen
- aktive Jobdateien niemals löschen
- fertige MP4 nach erfolgreichem Upload entfernen
- fehlgeschlagene Dateien zeitlich begrenzt behalten
- vor jedem Download freien Speicher prüfen
- unter einem Mindestwert keine neuen großen Jobs starten
- Debug-Screenshots rotieren
- Journale rotieren oder begrenzen

---

## 10. Blockierende HTTP-Aufrufe entfernen

### Problem

Mehrere `async def`-Funktionen rufen direkt `requests.get()` auf. Das blockiert den Event-Loop und kann Telegram-Callbacks verzögern.

Betroffene Bereiche umfassen unter anderem:

- Serienepisoden-Ermittlung
- Watchlist-Überblick
- HDFilme-/Filmpalast-/Megakino-Abfragen

### Kurzfristige Lösung

```python
response = await asyncio.to_thread(
    requests.get,
    url,
    headers=headers,
    timeout=30,
)
```

### Langfristige Lösung

Gemeinsamer `httpx.AsyncClient`:

```python
async with httpx.AsyncClient(timeout=30) as client:
    response = await client.get(url, headers=headers)
```

### Zusätzliche Anforderungen

- definierte Connect-/Read-Timeouts
- Retry nur bei transienten Fehlern
- keine Wiederholung bei permanenten 404-Fehlern
- gemeinsame Header-/Cookie-Verwaltung

---

## 11. Fehlerbehandlung strukturieren

### Problem

Viele breite Exception-Blöcke und einige nackte `except:`-Blöcke erschweren die Diagnose. Fehler können verschwinden, ohne dass der Benutzer eine verständliche Rückmeldung erhält.

### Eigene Fehlerklassen

```python
class ExtractionError(Exception):
    pass

class TemporaryDownloadError(Exception):
    pass

class PermanentMediaError(Exception):
    pass

class QueueStateError(Exception):
    pass

class UploadError(Exception):
    pass
```

### Zentraler Telegram-Error-Handler

```python
async def handle_application_error(update, context):
    logging.exception("Telegram-Handler fehlgeschlagen", exc_info=context.error)
```

Registrierung:

```python
app.add_error_handler(handle_application_error)
```

### Ziel

Jeder Fehler wird in genau eine Kategorie eingeordnet:

- Benutzerfehler
- temporärer Fehler mit Retry
- permanenter Medienfehler
- Queue-Zustandsfehler
- interner Programmfehler

---

## 12. Queue-Status zentral definieren

### Problem

Neue Zustände wie `paused` müssen aktuell an vielen Stellen separat ergänzt werden.

### Ziel

```python
class JobStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    PAUSED = "paused"
    COMPLETED = "completed"
    DEAD = "dead"
    CANCELLED = "cancelled"
```

### Versionierte Migrationen

```text
migrations/
├── 001_initial.sql
├── 002_queue_position.sql
├── 003_paused_status.sql
└── 004_batch_expiry.sql
```

Eine Migrationstabelle speichert die bereits ausgeführten Versionen.

### Zu prüfende Stellen pro Statusänderung

- PostgreSQL-Constraint
- Unique-Index
- Claim-Abfrage
- Deduplizierung
- Anzeige
- Sortierung
- Löschen
- „Queue leeren“
- Batch-Abschluss
- Wiederherstellung
- Tests

---

## 13. Verhalten von „Remote Queue leeren“

Aktuell muss explizit entschieden werden, ob pausierte Jobs beim Leeren erhalten bleiben.

Empfohlene UI:

```text
🗑 Wartende Jobs leeren
🗑 Wartende und pausierte Jobs leeren
```

Vor Ausführung immer eine Bestätigung mit exakter Anzahl anzeigen.

---

## 14. Git und Deployment

### Ziel

Jeder produktive Stand muss reproduzierbar sein.

### Maßnahmen

- funktionierende Änderungen committen
- Sicherungsdateien nicht im Projektstamm behalten
- `*.pre-*` in `.gitignore` aufnehmen
- temporäre Skripte entweder versionieren oder entfernen
- keine Medien- oder Sessiondateien in Git
- `.env` und Telethon-Session weiterhin mit Modus `0600`

### Deployment-Ablauf

```text
1. Syntaxprüfung
2. Unit-Tests
3. Integrationstests
4. Datenbankmigration
5. Graceful Restart
6. systemctl is-active
7. Logprüfung
8. Queue- und Telegram-Health-Check
```

---

## 15. Typisierung und statische Analyse

### Ziel

Neue Module schrittweise typisieren. Nicht versuchen, alle bestehenden Fehler in der Monolith-Datei sofort zu beheben.

### `pyrightconfig.json`

```json
{
  "venvPath": ".",
  "venv": "venv",
  "pythonVersion": "3.12",
  "typeCheckingMode": "basic",
  "include": ["app", "main.py"],
  "exclude": ["venv", "backups"]
}
```

### Job-Payloads typisieren

```python
class VideoJobPayload(TypedDict):
    type: Literal["video"]
    url: str
    content_type: Literal["film", "serie", "anime"]
    manual_title: str | None
    preferred_height: int | None
    quality_mode: Literal["best", "max_height", "lowest", "source"]
    allow_transcode: bool
```

---

## 16. Health- und Statusanzeige

Neuer Telegram-Befehl:

```text
/health
```

Beispiel:

```text
✅ Bot aktiv
▶️ 2 Jobs aktiv
⏳ 13 wartend
⏸ 71 pausiert
☠️ 4 dead
🧩 alte Extraktionsentwürfe vorhanden
💾 freier Speicher
🗄 PostgreSQL erreichbar
📡 Telegram verbunden
```

### Metriken

- Downloads erfolgreich/fehlgeschlagen
- Uploads erfolgreich/fehlgeschlagen
- Retry-Anzahl
- Dead Jobs
- durchschnittliche Downloadzeit
- durchschnittliche Uploadzeit
- Fehler je Quelle
- freie Festplattenkapazität
- Queue-Länge nach Status

---

## 17. Umsetzungspakete

### Paket 1: Betriebssicherheit

1. Worker-spezifische Prozesse und Statusobjekte
2. eindeutiger Abbruch pro Job
3. kontrollierter Shutdown
4. zentraler Telegram-Error-Handler
5. Queue- und Parallelitätstests

### Paket 2: erste Modularisierung

Auslagern:

```text
app/config.py
app/media/quality.py
app/media/metadata.py
app/media/files.py
app/queue/worker.py
app/telegram/remote_queue_ui.py
```

### Paket 3: Extraktoren

```text
app/extractors/serienstream.py
app/extractors/aniworld.py
app/extractors/filmpalast.py
app/extractors/megakino.py
app/extractors/kinoger.py
app/extractors/voe.py
```

### Paket 4: Datenbank und Wartung

1. versionierte Migrationen
2. alte `collecting`-Batches bereinigen
3. Dateibereinigung
4. Health-Status
5. Journalrotation

---

## 18. Definition of Done pro Arbeitspaket

Ein Arbeitspaket gilt erst als abgeschlossen, wenn:

- Syntaxprüfung erfolgreich ist
- Tests erfolgreich sind
- keine neue Pyright-/Lint-Kernwarnung eingeführt wurde
- Datenbankmigration erfolgreich durchgelaufen ist
- der Bot kontrolliert neu gestartet wurde
- systemd den Dienst als `active` meldet
- Telegram-Polling aktiv ist
- zentrale Queue erreichbar ist
- bestehende Jobs nicht verloren wurden
- relevante Telegram-Buttons praktisch getestet wurden

---

## 19. Empfohlener nächster Schritt

Als Erstes **Paket 1 – Betriebssicherheit** umsetzen:

1. `_active_proc`, `_dl_status` und `_dl_active` durch worker-spezifische Laufzeitobjekte ersetzen
2. `/status` für mehrere aktive Jobs erweitern
3. Abbruch exakt an einen Job binden
4. Graceful Shutdown implementieren
5. Tests dafür schreiben

Erst danach sollte die größere Modularisierung beginnen. So wird verhindert, dass während des Umbaus ein bereits vorhandener Parallelitätsfehler in mehrere neue Module verteilt wird.
