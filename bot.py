#!/usr/bin/env python3
from __future__ import annotations
import os, re, logging, subprocess, requests, asyncio, platform, json, sqlite3, sys, traceback, ast, socket, uuid, time as _time
from contextlib import asynccontextmanager

# Windows cp1252 kann keine Emojis → UTF-8 erzwingen
if sys.stdout.encoding and sys.stdout.encoding.lower().replace('-','') != 'utf8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
from datetime import UTC, datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ─── Titel-Normalisierung für robustes Matching ────
def normalize_title(title: str) -> str:
    """Normalisiert Titel: entfernt ':', '-', mehrere Spaces → 1 Space.
    Damit matchen 'Avatar - Fire and Ash', 'Avatar: Fire and Ash' etc."""
    return re.sub(r'[\s:\-]+', ' ', title).strip().lower()
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, ReplyKeyboardMarkup
from telegram.error import BadRequest as TgBadRequest, RetryAfter as TgRetryAfter
from telegram.ext import (
    Application, ApplicationHandlerStop, MessageHandler, CallbackQueryHandler,
    CommandHandler, TypeHandler, filters, ContextTypes,
)
from telethon import TelegramClient
from camoufox.async_api import AsyncCamoufox
from playwright.async_api import async_playwright
from central_media_db import CentralMediaDB
from central_queue import CentralQueue

# ─── Config aus .env laden ────────────────────────
load_dotenv()
BOT_TOKEN    = os.getenv("BOT_TOKEN")
API_ID       = int(os.getenv("API_ID"))
API_HASH     = os.getenv("API_HASH")
_cid = os.getenv("CHANNEL_ID")
CHANNEL_ID   = int(_cid) if _cid.lstrip('-').isdigit() else _cid
DOWNLOAD_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bot')
QUEUE_FILE    = os.path.join(DOWNLOAD_DIR, 'queue.json')
QUEUE_BACKUP_FILE = os.path.join(DOWNLOAD_DIR, 'queue.json.bak')
QUEUE_JOURNAL_FILE = os.path.join(DOWNLOAD_DIR, 'queue_journal.jsonl')
RECEIVED_LINKS_FILE = os.path.join(DOWNLOAD_DIR, 'received_links.jsonl')
DEAD_JOBS_FILE = os.path.join(DOWNLOAD_DIR, 'dead_jobs.jsonl')
PROGRESS_FILE = os.path.join(DOWNLOAD_DIR, 'series_progress.json')
WATCHLIST_FILE = os.path.join(DOWNLOAD_DIR, 'watchlist.json')
DB_PATH       = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'media.db')
DATABASE_URL  = os.getenv('DATABASE_URL', '').strip()
_central_media_db = CentralMediaDB(DATABASE_URL) if DATABASE_URL else None
CENTRAL_QUEUE_ROLE = os.getenv('CENTRAL_QUEUE_ROLE', 'local').strip().lower()
if CENTRAL_QUEUE_ROLE not in ('local', 'producer', 'worker'):
    raise ValueError("CENTRAL_QUEUE_ROLE muss local, producer oder worker sein")
if CENTRAL_QUEUE_ROLE != 'local' and not DATABASE_URL:
    raise ValueError("DATABASE_URL wird für die zentrale Queue benötigt")
_central_queue = CentralQueue(
    DATABASE_URL,
    int(os.getenv('CENTRAL_QUEUE_LEASE_SECONDS', '7200')),
) if DATABASE_URL else None
CENTRAL_QUEUE_WORKER_ID = (
    os.getenv('CENTRAL_QUEUE_WORKER_ID', '').strip()
    or f'{socket.gethostname()}-{uuid.uuid4().hex[:8]}'
)
CENTRAL_QUEUE_AUTOSTART = os.getenv(
    'CENTRAL_QUEUE_AUTOSTART', 'false'
).lower() in ('1', 'true', 'yes', 'on')
RUN_TELEGRAM_POLLING = os.getenv('RUN_TELEGRAM_POLLING', 'true').lower() in ('1', 'true', 'yes', 'on')
try:
    ALLOWED_CHAT_IDS = {
        int(value.strip())
        for value in os.getenv('ALLOWED_CHAT_IDS', '').split(',')
        if value.strip()
    }
except ValueError as error:
    raise ValueError("ALLOWED_CHAT_IDS muss kommaseparierte Telegram-Chat-IDs enthalten") from error
if RUN_TELEGRAM_POLLING and not ALLOWED_CHAT_IDS:
    raise ValueError(
        "Telegram-Polling ist aktiv, aber ALLOWED_CHAT_IDS fehlt. "
        "Der Bot startet aus Sicherheitsgründen nicht öffentlich."
    )
# camoufox läuft headless (kein Fenster pro Folge). Falls ALTCHA/VOE mal hängt,
# SERIENSTREAM_HEADFUL=1 setzen → Browser wird sichtbar (zur Diagnose/Edge-Case).
SERIENSTREAM_HEADFUL = os.getenv('SERIENSTREAM_HEADFUL', '').lower() in ('1', 'true', 'yes', 'on')

# Einheitlicher User-Agent für Playwright + yt-dlp + ffmpeg + requests
# Muss überall gleich sein, sonst lehnt der CDN auth-token URLs ab
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
MEGAKINO_HOME_URL = os.getenv("MEGAKINO_HOME_URL", "https://megakino5.org/")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

ANIME_BASE = "https://aniworld.to"

# ─── Download-Queue ───────────────────────────────
_dl_queue: asyncio.Queue = asyncio.Queue()
_dl_pending: list[dict] = []
_dl_active:  dict | None = None          # gerade verarbeitetes Item
_active_proc: asyncio.subprocess.Process | None = None  # laufender ffmpeg/yt-dlp Prozess
_dl_status: dict = {}                   # aktueller Download-Status für /status
_series_progress: dict[str, int] = {}
_watchlist: list[dict] = []
_reupload_events:  dict[str, asyncio.Event] = {}
_reupload_choices: dict[str, bool]          = {}
_central_worker_task: asyncio.Task | None = None

PERMANENT_JOB_FAILURE = 'permanent_job_failure'


def _utc_now_iso() -> str:
    """Aktueller UTC-Zeitpunkt als ISO-String mit Z-Suffix."""
    return datetime.now(UTC).isoformat(timespec='seconds').replace('+00:00', 'Z')


class PermanentMediaUnavailable(RuntimeError):
    """Der Hoster bestätigt, dass die Mediendatei dauerhaft entfernt wurde."""


class _LazyTelegramMessage:
    """Erzeugt die Telegram-Statusnachricht erst beim ersten echten Job-Update."""

    def __init__(self, bot, chat_id: int):
        self._bot = bot
        self._chat_id = chat_id
        self._message = None
        self._lock = asyncio.Lock()

    async def edit_text(self, text: str, **kwargs):
        async with self._lock:
            while True:
                try:
                    if self._message is None:
                        self._message = await self._bot.send_message(
                            self._chat_id, text, **kwargs
                        )
                        return self._message
                    return await self._message.edit_text(text, **kwargs)
                except TgRetryAfter as exc:
                    retry_after = getattr(exc, 'retry_after', 1)
                    delay = retry_after.total_seconds() if hasattr(retry_after, 'total_seconds') else float(retry_after)
                    logging.warning("Telegram Rate-Limit: warte %.1fs", delay)
                    await asyncio.sleep(delay + 0.5)


def _spawn_task(coro, name: str):
    task = asyncio.create_task(coro, name=name)

    def _log_task_result(done: asyncio.Task):
        try:
            done.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logging.error(f"Background task '{name}' beendet:\n{traceback.format_exc()}")

    task.add_done_callback(_log_task_result)
    return task

_sys = platform.system()
_NO_WINDOW = subprocess.CREATE_NO_WINDOW if _sys == "Windows" else 0

# ──────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
# Session-Name pro Gerät konfigurierbar: verhindert AUTH_KEY_DUPLICATED,
# wenn der Bot auf mehreren Geräten gleichzeitig läuft (jede Instanz braucht
# ihren eigenen Auth-Key – mehrere Instanzen auf derselben Session-Datei
# werden von Telegram als Session-Hijack erkannt und gekillt).
# Setze in deiner .env z.B.  TELETHON_SESSION=uploader_mac  bzw. =uploader_server
_TG_SESSION_NAME = os.getenv("TELETHON_SESSION", "uploader_session")
tg_client = TelegramClient(_TG_SESSION_NAME, API_ID, API_HASH)
logging.info(f"Telethon-Session: {_TG_SESSION_NAME}.session")

MAIN_MENU = ReplyKeyboardMarkup(
    [
        ["▶️ Remote Queue Download", "🗑 Remote Queue leeren"],
        ["🆕 Megakino Last Added", "🆕 HDFilme Latest"],
        ["🆕 Filmpalast Import"],
        ["🔍 Watchcheck", "📋 Queue"],
        ["📊 Status"],
    ],
    resize_keyboard=True,
    is_persistent=True,
)


# ─── Scraping ────────────────────────────────────

def scrape_title(url: str) -> str:
    # aniworld.to Episode-URL: direkt aus URL parsen (Seite liefert deutsche Titel)
    m = re.search(r'/anime/stream/([^/]+)/staffel-(\d+)/episode-(\d+)', url)
    if m:
        slug, season, episode = m.group(1), int(m.group(2)), int(m.group(3))
        series_name = slug.replace('-', ' ').title()
        return f"{series_name} S{season:02d}E{episode:02d}"

    # s.to Episode-URL: auch direkt aus URL parsen
    m = re.search(r'/serie/stream/([^/]+)/staffel-(\d+)/episode-(\d+)', url)
    if m:
        slug, season, episode = m.group(1), int(m.group(2)), int(m.group(3))
        series_name = slug.replace('-', ' ').title()
        return f"{series_name} S{season:02d}E{episode:02d}"

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.content, 'html.parser')
        if soup.find('meta', property='og:title'):
            return soup.find('meta', property='og:title')['content'].strip()
        if soup.find('h1'):
            return soup.find('h1').text.strip()
        if soup.title:
            return soup.title.text.strip()
    except Exception as e:
        logging.error(f"Scraping Fehler: {e}")
    return "Video"


# ─── Lokale Media-DB ──────────────────────────────

def _db_init():
    """Erstellt media.db und importiert einmalig aus series.db."""
    if _central_media_db:
        _central_media_db.init()
        logging.info("Medien-Datenbank: zentrales PostgreSQL aktiv")
        return
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript('''
        CREATE TABLE IF NOT EXISTS movies (
            title TEXT NOT NULL, year INTEGER,
            PRIMARY KEY (title, year)
        );
        CREATE TABLE IF NOT EXISTS episodes (
            series TEXT NOT NULL, season INTEGER NOT NULL, episode INTEGER NOT NULL,
            PRIMARY KEY (series, season, episode)
        );
        CREATE TABLE IF NOT EXISTS anime_episodes (
            anime TEXT NOT NULL, season INTEGER NOT NULL, episode INTEGER NOT NULL,
            PRIMARY KEY (anime, season, episode)
        );
    ''')
    # Migration: voe_url Spalte hinzufügen (für Qualitäts-Updates)
    for table in ('movies', 'episodes', 'anime_episodes'):
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN voe_url TEXT")
            logging.info(f"DB-Migration: voe_url Spalte zu {table} hinzugefügt")
        except sqlite3.OperationalError:
            pass  # Spalte existiert schon
    con.commit()

    # Einmaliger Import aus series.db falls vorhanden und media.db noch leer
    src_path = os.path.expanduser("~/Downloads/series.db")
    if os.path.exists(src_path):
        try:
            already = cur.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
            if already == 0:
                src = sqlite3.connect(src_path)
                movies = src.execute("SELECT title, year FROM movies WHERE title IS NOT NULL").fetchall()
                eps    = src.execute("SELECT series, season, episode FROM episodes").fetchall()
                anime  = src.execute("SELECT anime, season, episode FROM anime_episodes").fetchall()
                src.close()
                cur.executemany("INSERT OR IGNORE INTO movies VALUES (?, ?)", movies)
                cur.executemany("INSERT OR IGNORE INTO episodes VALUES (?, ?, ?)", eps)
                cur.executemany("INSERT OR IGNORE INTO anime_episodes VALUES (?, ?, ?)", anime)
                con.commit()
                logging.info(f"media.db: {len(movies)} Filme, {len(eps)} Serien-Eps, {len(anime)} Anime-Eps importiert")
        except Exception as e:
            logging.warning(f"series.db Import Fehler: {e}")
    con.close()


def _db_exists(content_type: str, filename: str, is_anime: bool = False) -> bool:
    """Prüft ob Eintrag bereits in media.db vorhanden ist.
    Normalisiert Titel-Matching: ignoriert ':' und '-' Unterschiede."""
    if _central_media_db:
        return _central_media_db.exists(content_type, filename, is_anime)
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        if content_type == 'film':
            m = re.search(r'^(.*?)\s*\((\d{4})\)', filename)
            if m:
                title, year = m.group(1).strip(), int(m.group(2))
                norm_title = normalize_title(title)

                # Alle Filme im Jahr-Fenster holen und Python-seitig normalisiert vergleichen
                rows = cur.execute(
                    """SELECT title FROM movies
                       WHERE year BETWEEN ? AND ?""",
                    (year - 1, year + 1)
                ).fetchall()

                for (db_title,) in rows:
                    if normalize_title(db_title) == norm_title:
                        logging.info(f"[MATCH] DB '{db_title}' ≈ '{title}'")
                        return True

                logging.debug(f"[NO-MATCH] Kein Film gefunden für '{title}' ({year})")
                return False
        else:
            m = re.search(r'^(.*?)\s+S(\d{2})E(\d{2})', filename, re.IGNORECASE)
            if m:
                name, season, episode = m.group(1).strip(), int(m.group(2)), int(m.group(3))
                table = "anime_episodes" if is_anime else "episodes"
                col   = "anime"          if is_anime else "series"
                row = cur.execute(
                    f"SELECT 1 FROM {table} WHERE LOWER({col})=LOWER(?) AND season=? AND episode=?",
                    (name, season, episode)
                ).fetchone()
                return row is not None
    except Exception as e:
        logging.warning(f"DB-Check Fehler: {e}")
    finally:
        try: con.close()
        except: pass
    return False


def _db_exists_batch(content_type: str, filenames: list[str], is_anime: bool = False) -> dict[str, bool]:
    """Prüft mehrere Einträge auf einmal.
    Normalisiert Titel-Matching: ignoriert ':' und '-' Unterschiede."""
    result = {fn: False for fn in filenames}
    if not filenames:
        return result
    if _central_media_db:
        return _central_media_db.exists_batch(content_type, filenames, is_anime)

    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()

        if content_type == 'film':
            # Alle Jahre-Fenster holen
            all_years = set()
            for fn in filenames:
                m = re.search(r'^(.*?)\s*\((\d{4})\)', fn)
                if m:
                    year = int(m.group(2))
                    all_years.update(range(year - 1, year + 2))

            # Alle relevanten Filme in einem Rutsch holen
            if all_years:
                placeholders = ','.join('?' * len(all_years))
                rows = cur.execute(
                    f"SELECT title, year FROM movies WHERE year IN ({placeholders})",
                    list(all_years)
                ).fetchall()

                # Dictionary für schnelle Lookups: norm_title → (orig_title, year)
                db_dict = {}
                for orig_title, db_year in rows:
                    norm = normalize_title(orig_title)
                    if norm not in db_dict:
                        db_dict[norm] = []
                    db_dict[norm].append((orig_title, db_year))

                # Nun alle Filenames gegen DB vergleichen
                for fn in filenames:
                    m = re.search(r'^(.*?)\s*\((\d{4})\)', fn)
                    if m:
                        title, year = m.group(1).strip(), int(m.group(2))
                        norm_title = normalize_title(title)
                        if norm_title in db_dict:
                            # Prüfe Jahr-Fenster
                            for db_orig, db_year in db_dict[norm_title]:
                                if db_year - 1 <= year <= year + 1:
                                    logging.info(f"[BATCH-MATCH] '{title}' ≈ DB '{db_orig}'")
                                    result[fn] = True
                                    break
        else:
            # Für Serien/Anime: einfaches Matching wie vorher
            for fn in filenames:
                m = re.search(r'^(.*?)\s+S(\d{2})E(\d{2})', fn, re.IGNORECASE)
                if m:
                    name, season, episode = m.group(1).strip(), int(m.group(2)), int(m.group(3))
                    table = "anime_episodes" if is_anime else "episodes"
                    col = "anime" if is_anime else "series"
                    row = cur.execute(
                        f"SELECT 1 FROM {table} WHERE LOWER({col})=LOWER(?) AND season=? AND episode=?",
                        (name, season, episode)
                    ).fetchone()
                    result[fn] = row is not None
    except Exception as e:
        logging.warning(f"DB-Batch-Check Fehler: {e}")
    finally:
        try: con.close()
        except: pass

    return result


def _db_add(content_type: str, filename: str, is_anime: bool = False, overwrite: bool = False, voe_url: str | None = None):
    """Fügt neuen Eintrag nach erfolgreichem Upload in media.db ein.
    voe_url wird mit gespeichert, damit Re-Uploads mit anderer Quelle erkannt werden."""
    if _central_media_db:
        _central_media_db.add(content_type, filename, is_anime, overwrite, voe_url)
        return
    mode = "INSERT OR REPLACE" if overwrite else "INSERT OR IGNORE"
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        if content_type == 'film':
            m = re.search(r'^(.*?)\s*\((\d{4})\)', filename)
            if m:
                cur.execute(f"{mode} INTO movies(title, year, voe_url) VALUES (?, ?, ?)",
                            (m.group(1).strip(), int(m.group(2)), voe_url))
        else:
            m = re.search(r'^(.*?)\s+S(\d{2})E(\d{2})', filename, re.IGNORECASE)
            if m:
                name, season, episode = m.group(1).strip(), int(m.group(2)), int(m.group(3))
                if is_anime:
                    cur.execute(f"{mode} INTO anime_episodes(anime, season, episode, voe_url) VALUES (?, ?, ?, ?)",
                                (name, season, episode, voe_url))
                else:
                    cur.execute(f"{mode} INTO episodes(series, season, episode, voe_url) VALUES (?, ?, ?, ?)",
                                (name, season, episode, voe_url))
        con.commit()
    except Exception as e:
        logging.warning(f"DB-Insert Fehler: {e}")
    finally:
        try: con.close()
        except: pass


def _db_get_voe(content_type: str, filename: str, is_anime: bool = False) -> str | None:
    """Holt die gespeicherte voe_url für ein bekanntes Item. None wenn nicht vorhanden."""
    if _central_media_db:
        return _central_media_db.get_voe(content_type, filename, is_anime)
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        if content_type == 'film':
            m = re.search(r'^(.*?)\s*\((\d{4})\)', filename)
            if m:
                row = cur.execute(
                    "SELECT voe_url FROM movies WHERE LOWER(title)=LOWER(?) AND year=?",
                    (m.group(1).strip(), int(m.group(2)))
                ).fetchone()
                return row[0] if row else None
        else:
            m = re.search(r'^(.*?)\s+S(\d{2})E(\d{2})', filename, re.IGNORECASE)
            if m:
                name, season, episode = m.group(1).strip(), int(m.group(2)), int(m.group(3))
                table = 'anime_episodes' if is_anime else 'episodes'
                col   = 'anime'          if is_anime else 'series'
                row = cur.execute(
                    f"SELECT voe_url FROM {table} WHERE LOWER({col})=LOWER(?) AND season=? AND episode=?",
                    (name, season, episode)
                ).fetchone()
                return row[0] if row else None
    except Exception as e:
        logging.warning(f"DB-Voe-Check Fehler: {e}")
    finally:
        try: con.close()
        except: pass
    return None



def build_caption(filename: str, content_type: str) -> str:
    """Erstellt die Telegram-Caption im gewünschten Format."""
    if content_type in ('serie', 'anime'):
        m = re.search(r'^(.*?)\s+S(\d{2})E(\d{2})\s*(.*)', filename, re.IGNORECASE)
        if m:
            name    = m.group(1).strip()
            season  = int(m.group(2))
            episode = int(m.group(3))
            suffix  = m.group(4).strip().lower()
            tag     = 'anime' if (content_type == 'anime' or 'anime' in suffix) else 'series'
            return f"{name} | {season} | {episode} | {tag}"

    if content_type in ('film', 'filmpalast'):
        m = re.search(r'^(.*?)\s*\((\d{4})\)\s*(.*)', filename)
        if m:
            name   = m.group(1).strip()
            year   = m.group(2)
            genres = m.group(3).strip()
            if year == str(datetime.now().year):
                genres = ('New, ' + genres) if genres else 'New'
            return f"{name} | {year} | {genres}" if genres else f"{name} | {year}"

    return filename  # Fallback


def format_filename(title: str) -> str:
    title = re.sub(r'[<>:"/\\|?*]', '', title).strip()

    m = re.search(r'(.*?)\s*(S\d{2}E\d{2})(.*)', title, re.IGNORECASE)
    if m:
        suffix = m.group(3).strip()
        # Seitennamen wie "S.to", "Kinox.to" am Ende entfernen
        if re.match(r'^[A-Za-z]+\.[a-z]{2,}$', suffix):
            suffix = ''
        return f"{m.group(1).strip()} {m.group(2).upper()}{' ' + suffix if suffix else ''}"

    m = re.search(r'(.*?)\s*\((\d{4})\)\s*(.*)', title)
    if m:
        genres = m.group(3).strip()
        return f"{m.group(1).strip()} ({m.group(2)}){' ' + genres if genres else ''}"

    return title


# ─── Stream & Download ───────────────────────────

@asynccontextmanager
async def _open_browser(*, camoufox: bool, headless: bool = True):
    """Öffnet Camoufox nur für SerienStream, sonst Playwright Chromium."""
    if camoufox:
        async with AsyncCamoufox(headless=headless) as browser:
            yield browser
        return

    async with async_playwright() as playwright:
        launch_options = {'headless': headless}
        if platform.system() == 'Linux':
            launch_options['args'] = ['--no-sandbox', '--disable-dev-shm-usage']
        browser = await playwright.chromium.launch(**launch_options)
        try:
            yield browser
        finally:
            if browser.is_connected():
                await browser.close()


async def get_stream_url(page_url: str, content_type: str, early_db_check=None,
                         source_only: bool = False) -> tuple[str | None, str | None, str | None, str, str | None]:
    """Playwright öffnet URL, extrahiert ggf. iframe-URL, klickt Play, fängt Stream ab.
    Gibt (stream_url, film_title_or_None, referer_or_None, cookie_str) zurück.

    early_db_check: optionaler Callback, der nach Titel-Extraktion aufgerufen wird.
      Wenn er True liefert, wird der VOE-Abruf übersprungen und cookie_str='__DB_SKIP__'
      zurückgegeben – damit das Öffnen des Players und die m3u8-Auflösung entfallen."""
    stream_url = None
    film_title = None
    referer = None
    cookie_str = ""
    source_voe_url = None  # zum DB-Vergleich (erkennt Re-Uploads in besserer Qualität)

    use_camoufox = _is_seriesstream_url(page_url)
    logging.info(
        "Browser-Backend: %s für %s",
        "Camoufox" if use_camoufox else "Playwright Chromium",
        page_url,
    )
    async with _open_browser(
        camoufox=use_camoufox,
        headless=not SERIENSTREAM_HEADFUL if use_camoufox else True,
    ) as browser:
        # ALTCHA rechnet den Proof-of-Work im sichtbaren Browser zuverlässig aus;
        # camoufox löst das auch headless → kein Fenster pro Folge. Bei Problemen
        # SERIENSTREAM_HEADFUL=1 setzen, dann läuft der Browser sichtbar.
        context = await browser.new_context()

        BLOCKED_SCRIPT_HOSTS = (
            'rutseasickmunch.com',
            'outofthebluethudboard.com',
            'chaliceguzzlerlandlord.com',
            'imasdk.googleapis.com',
            'static.ads-twitter.com',
            'googlesyndication.com',
            'doubleclick.net',
            'adnxs.com',
            'pubmatic.com',
            'rubiconproject.com',
            'openx.net',
            'adform.net',
            'bidster.net',
            'trafficbass.com',
            'yellowblue.io',
            'ingage.tech',
            'connatix.com',
            'minutemedia',
            'id5-sync.com',
            'liadm.com',
            'gumgum.com',
        )

        async def _block_bad_requests(route):
            req = route.request
            host = _urlparse(req.url).hostname or ''
            if any(blocked in host for blocked in BLOCKED_SCRIPT_HOSTS):
                logging.info(f"BLOCKED external script/ad: {req.url[:160]}")
                await route.abort()
                return
            await route.continue_()

        await context.route("**/*", _block_bad_requests)

        # JW Player 6 (VOE) prüft `video.canPlayType('application/vnd.apple.mpegurl')`
        # und wählt nur dann die master.m3u8. Chromium sagt dort '' → Player fällt
        # auf MP4 _h (höchste Qualität) zurück, und dessen CDN-Token ist an den Pfad
        # gebunden → kein Quality-Downgrade möglich. Wir patchen canPlayType so,
        # dass HLS als unterstützt gemeldet wird – Playwright fängt die m3u8 ab,
        # die wir dann in `resolve_normal_quality()` auf 720p runterbrechen.
        await context.add_init_script('''
            (() => {
              const orig = HTMLMediaElement.prototype.canPlayType;
              HTMLMediaElement.prototype.canPlayType = function(type) {
                if (typeof type === 'string' &&
                    /application\\/(vnd\\.apple\\.mpegurl|x-mpegurl)/i.test(type)) {
                  return 'maybe';
                }
                return orig.call(this, type);
              };
            })();
        ''')

        page = await context.new_page()

        # Separate Slots pro Stream-Typ (m3u8 bevorzugt über MP4)
        _streams: dict[str, str | None] = {'m3u8': None, 'mpd': None, 'mp4': None}

        from urllib.parse import urlparse as _urlparse

        async def handle_request(request):
            url = request.url
            rtype = request.resource_type

            # Nur Pfad gegen Extensions prüfen! Tracking-GIFs von VOE enthalten die
            # echte m3u8-URL als URL-encoded Query-Parameter (…&mu=https%3A%2F%2F…m3u8…),
            # und ein naiver `.m3u8 in url`-Check würde die GIF als HLS-Stream erkennen.
            try:
                path_lower = (_urlparse(url).path or '').lower()
            except Exception:
                path_lower = url.lower()

            if any(path_lower.endswith(ext) for ext in ('.m3u8', '.mp4', '.ts', '.mpd')) \
                    or any(ext in path_lower for ext in ('.m3u8', '.mp4', '.mpd')):
                logging.info(f"VIDEO: [{rtype}] {url[:200]}")

            if path_lower.endswith('.m3u8') or '.m3u8' in path_lower:
                if not _streams['m3u8']:
                    _streams['m3u8'] = url
                    logging.info(f">>> HLS Stream: {url}")
            elif path_lower.endswith('.mpd') or '.mpd' in path_lower:
                if not _streams['mpd']:
                    _streams['mpd'] = url
                    logging.info(f">>> DASH Stream: {url}")
            elif ('.mp4' in path_lower and rtype in ('media', 'video', 'xmlhttprequest', 'other', 'fetch')
                  and not _streams['mp4']):
                _streams['mp4'] = url
                logging.info(f">>> MP4 Stream: {url}")

        context.on('request', handle_request)

        def _current_stream() -> str | None:
            return _streams['m3u8'] or _streams['mpd'] or _streams['mp4']

        # ─── Film: Titel + data-src gleichzeitig per Playwright extrahieren ───
        if content_type == 'film':
            is_hdfilme = 'hdfilme.win' in page_url
            logging.info(f"Film: Öffne Seite: {page_url}")
            await page.goto(page_url, timeout=60000, wait_until='domcontentloaded')
            await asyncio.sleep(2)

            if is_hdfilme:
                result = await page.evaluate('''() => {
                    const h1 = document.querySelector('h1');
                    let title = h1 ? h1.textContent.trim() : null;
                    if (title) title = title.replace(/\\s+hdfilme\\s*$/i, '').trim();

                    let year = null;
                    const og = document.querySelector('meta[property="og:title"]');
                    const titleText = og ? og.getAttribute('content') : (document.title || '');
                    const ym = titleText.match(/\\((\\d{4})\\)/);
                    if (ym) year = ym[1];

                    const genreBox = document.querySelector('.info .border-b span:first-child');
                    const genres = Array.from(genreBox ? genreBox.querySelectorAll('a[href*="/"]') : [])
                        .map(a => a.textContent.trim())
                        .filter(Boolean)
                        .filter(g => !['Filme', 'Serien', 'kinofilme'].includes(g.toLowerCase()));

                    const iframe = document.querySelector('iframe[src*="meinecloud.click/movie/"]');
                    const playerSrc = iframe ? iframe.getAttribute('src') : null;
                    return { title, year, genres, playerSrc };
                }''')

                if result.get('title') and result.get('year'):
                    genre_str = ', '.join(result['genres']) if result.get('genres') else ''
                    film_title = f"{result['title']} ({result['year']}){' ' + genre_str if genre_str else ''}"
                    logging.info(f"HDFilme Film-Titel: {film_title}")

                    if early_db_check and early_db_check(film_title):
                        logging.info(f"[EARLY-SKIP] '{film_title}' bereits in DB → Player-Abruf übersprungen")
                        await browser.close()
                        return (None, film_title, None, "__DB_SKIP__", source_voe_url)

                player_url = result.get('playerSrc')
                if player_url:
                    if player_url.startswith('//'):
                        player_url = 'https:' + player_url
                    logging.info(f"HDFilme Film-Player: {player_url}")
                    await page.goto(player_url, timeout=60000, wait_until='domcontentloaded')
                    await asyncio.sleep(2)
                    drop_url = await page.evaluate('''() => {
                        const direct = document.querySelector('#stream_yes_access[onclick]');
                        if (direct) {
                            const onclick = direct.getAttribute('onclick') || '';
                            const m = onclick.match(/window\\.open\\(\\s*['"]([^'"]+)['"]/);
                            if (m) return m[1];
                        }
                        const active = document.querySelector('li.active[data-link]');
                        const any = document.querySelector('li[data-link*="dr0pstream.com/e/"], li[data-link*="drop"]');
                        const el = active || any;
                        return el ? el.getAttribute('data-link') : null;
                    }''')
                    if drop_url:
                        if drop_url.startswith('//'):
                            drop_url = 'https:' + drop_url
                        source_voe_url = drop_url
                        logging.info(f"HDFilme Film dr0pstream: {drop_url}")
                        await page.goto(drop_url, timeout=60000, wait_until='domcontentloaded')
                        await asyncio.sleep(3)
                    else:
                        logging.warning("HDFilme Film: Kein dr0pstream-Link im Player gefunden")
                else:
                    logging.warning("HDFilme Film: Kein meinecloud-Player gefunden")

            else:
                result = await page.evaluate('''() => {
                const IGNORE = new Set(['filme', 'kinofilme']);

                // Titel
                const h1 = document.querySelector('h1[itemprop="name"]');
                const title = h1 ? h1.textContent.trim() : null;

                // Jahr
                const dateEl = document.querySelector('span[itemprop="dateCreated"]');
                const yearMatch = dateEl ? dateEl.textContent.match(/\\d{4}/) : null;
                const year = yearMatch ? yearMatch[0] : null;

                // Genres
                const genreEl = document.querySelector('[itemprop="genre"]');
                let genres = [];
                if (genreEl) {
                    genres = genreEl.textContent.trim()
                        .split(/[/,]/)
                        .map(g => g.trim())
                        .filter(g => g && !IGNORE.has(g.toLowerCase()));
                }

                // VOE iframe data-src
                const iframe = document.querySelector('#film_main');
                let voeSrc = null;
                if (iframe) {
                    voeSrc = iframe.getAttribute('data-src') || iframe.getAttribute('src') || null;
                }
                if (!voeSrc) {
                    const all = document.querySelectorAll('iframe[data-src]');
                    for (const f of all) {
                        const src = f.getAttribute('data-src');
                        if (src && src.includes('/e/')) { voeSrc = src; break; }
                    }
                }

                return { title, year, genres, voeSrc };
                }''')

                # Film-Titel zusammenbauen
                if result.get('title') and result.get('year'):
                    genre_str = ', '.join(result['genres']) if result.get('genres') else ''
                    film_title = f"{result['title']} ({result['year']}){' ' + genre_str if genre_str else ''}"
                    logging.info(f"Film-Titel: {film_title}")

                    # ⚡ Early-DB-Check: Wenn Film schon in DB, VOE-Abruf sparen
                    if early_db_check and early_db_check(film_title):
                        logging.info(f"[EARLY-SKIP] '{film_title}' bereits in DB → VOE-Abruf übersprungen")
                        await browser.close()
                        return (None, film_title, None, "__DB_SKIP__", source_voe_url)

                voe_url = result.get('voeSrc')
                source_voe_url = voe_url
                if voe_url:
                    logging.info(f"Film: VOE URL extrahiert: {voe_url}")
                    await page.goto(voe_url, timeout=60000, wait_until='domcontentloaded')
                    await asyncio.sleep(3)
                else:
                    logging.warning("Film: Kein data-src gefunden, versuche Seite direkt")
                    await page.goto(page_url, timeout=60000, wait_until='domcontentloaded')
                    await asyncio.sleep(3)

        # ─── Filmpalast: Titel/Jahr/Genre + direkten VOE-Link extrahieren ───
        elif content_type == 'filmpalast':
            logging.info(f"Filmpalast: Öffne Seite: {page_url}")
            await page.goto(page_url, timeout=60000, wait_until='domcontentloaded')
            await asyncio.sleep(2)

            result = await page.evaluate('''() => {
                // Titel
                const h2 = document.querySelector('h2.bgDark');
                const title = h2 ? h2.textContent.trim() : null;

                // Jahr
                let year = null;
                document.querySelectorAll('li').forEach(li => {
                    const m = li.textContent.match(/Veröffentlicht[:\\s]*(\\d{4})/);
                    if (m) year = m[1];
                });

                // Genre – nur aus dem Kategorien-Block
                let genres = [];
                document.querySelectorAll('#detail-content-list li').forEach(li => {
                    const p = li.querySelector('p');
                    if (p && p.textContent.includes('Kategorien')) {
                        genres = Array.from(li.querySelectorAll('a[href*="/search/genre/"]'))
                            .map(a => a.textContent.trim()).filter(Boolean);
                    }
                });

                // Direkter VOE-Link aus Play-Button
                const playBtn = document.querySelector('a.button.rb.iconPlay[href*="voe.sx"]');
                const voeUrl = playBtn ? playBtn.getAttribute('href') : null;

                return { title, year, genres, voeUrl };
            }''')

            title = result.get('title')
            year = result.get('year')
            genres = result.get('genres') or []
            voe_url = result.get('voeUrl')
            source_voe_url = voe_url

            if title and year:
                genre_str = ', '.join(genres) if genres else ''
                film_title = f"{title} ({year}){' ' + genre_str if genre_str else ''}"
                logging.info(f"Filmpalast Titel: {film_title}")

                # ⚡ Early-DB-Check: Wenn Film schon in DB, VOE-Abruf sparen
                if early_db_check and early_db_check(film_title):
                    logging.info(f"[EARLY-SKIP] '{film_title}' bereits in DB → VOE-Abruf übersprungen")
                    await browser.close()
                    return (None, film_title, None, "__DB_SKIP__", source_voe_url)

            if voe_url:
                logging.info(f"Filmpalast: VOE URL: {voe_url}")
                await page.goto(voe_url, timeout=60000, wait_until='domcontentloaded')
                await asyncio.sleep(3)
            else:
                logging.warning("Filmpalast: Kein VOE-Link gefunden")

        # ─── Anime (aniworld.to Episode): marissasharecareer-Link direkt finden ───
        elif content_type == 'anime':
            logging.info(f"Anime: Öffne Episode-Seite: {page_url}")
            await page.goto(page_url, timeout=60000, wait_until='domcontentloaded')
            await asyncio.sleep(2)

            # 1) Direkt nach marissasharecareer-Link suchen
            direct_url = await page.evaluate('''() => {
                // Direkter Link im DOM
                const a = document.querySelector('a[href*="marissasharecareer.com/e/"]');
                if (a) return a.getAttribute('href');
                // Oder iframe src
                const f = document.querySelector('iframe[src*="marissasharecareer.com/e/"]');
                if (f) return f.getAttribute('src');
                return null;
            }''')

            if direct_url:
                logging.info(f"Anime: Direkter Marissa-Link: {direct_url}")
                await page.goto(direct_url, timeout=60000, wait_until='domcontentloaded')
                await asyncio.sleep(4)
            else:
                # 2) Fallback: /redirect/ iframe folgen
                redirect_path = await page.evaluate('''() => {
                    const iframe = document.querySelector('iframe[src*="/redirect/"]');
                    if (iframe) return iframe.getAttribute('src');
                    return null;
                }''')

                if redirect_path:
                    target = "https://aniworld.to" + redirect_path
                    logging.info(f"Anime: Folge Redirect: {target}")
                    # Redirect öffnen → landet auf marissasharecareer
                    await page.goto(target, timeout=60000, wait_until='domcontentloaded')
                    await asyncio.sleep(2)
                    # Jetzt sollten wir auf marissasharecareer sein → nochmal prüfen
                    final_url = page.url
                    logging.info(f"Anime: Nach Redirect: {final_url}")
                    await asyncio.sleep(3)
                else:
                    logging.warning("Anime: Weder direkter Link noch redirect gefunden")
                    await asyncio.sleep(3)

        # ─── SerienStream: ALTCHA bestätigen und VOE-Redirect öffnen ───
        elif _is_seriesstream_url(page_url):
            page_url = _normalize_seriesstream_url(page_url)
            logging.info(f"SerienStream: Öffne Episode: {page_url}")
            await page.goto(page_url, timeout=120000, wait_until='domcontentloaded')
            await asyncio.sleep(2)

            # Die Seite lädt den bevorzugten deutschen VOE-Hoster automatisch.
            # Falls das Modal noch nicht offen ist, den deutschen VOE-Button auslösen.
            modal = page.locator('#playerPrepareModal.show')
            if await modal.count() == 0:
                voe_button = page.locator(
                    '.link-box[data-provider-name="VOE"][data-language-id="1"]'
                )
                if await voe_button.count() == 1:
                    try:
                        await voe_button.click(timeout=15000)
                    except Exception:
                        clicked = await page.evaluate("""() => {
                            const b = document.querySelector('.link-box[data-provider-name="VOE"][data-language-id="1"]');
                            if (!b) return false;
                            b.click();
                            return true;
                        }""")
                        if not clicked:
                            raise RuntimeError("SerienStream: VOE-Button nicht auslösbar")
            try:
                await modal.wait_for(timeout=30000)
            except Exception:
                logging.warning("SerienStream: Player-Modal nicht sichtbar geworden")
            await asyncio.sleep(1)

            captcha = page.locator(
                '#playerPrepareModal altcha-widget input[type="checkbox"]'
            )
            if await captcha.count() == 1 and not await captcha.is_checked():
                logging.info("SerienStream: ALTCHA wird bestätigt")
                # ALTCHA-Proof-of-Work auslösen: Das Widget lauscht auf das
                # change-Event der Checkbox. Ein Playwright-Klick auf das Label
                # timed out unter proot/Android (Actionability-Check); wir setzen
                # die Checkbox per JS und dispatchen change/input → der PoW
                # startet zuverlässig (Desktop UND Handy). Label-Klick als Fallback.
                triggered = await page.evaluate("""() => {
                    const inp = document.querySelector('#playerPrepareModal altcha-widget input[type="checkbox"]');
                    if (!inp) return false;
                    if (!inp.checked) {
                        inp.checked = true;
                        inp.dispatchEvent(new Event('change', {bubbles: true}));
                        inp.dispatchEvent(new Event('input', {bubbles: true}));
                    }
                    return true;
                }""")
                if not triggered:
                    captcha_label = page.locator(
                        '#playerPrepareModal altcha-widget label[for^="altcha_checkbox_"]'
                    )
                    if await captcha_label.count() == 1:
                        try:
                            await captcha_label.click(force=True, timeout=30000)
                        except Exception:
                            pass
                try:
                    await page.wait_for_function(
                        """() => {
                            const widget = document.querySelector('#playerPrepareModal altcha-widget .altcha');
                            const payload = document.querySelector('#playerPrepareModal input[name="altcha"]');
                            return widget?.dataset?.state === 'verified' && !!payload?.value;
                        }""",
                        timeout=180000,
                    )
                except Exception as e:
                    raise RuntimeError(f"SerienStream ALTCHA fehlgeschlagen: {e}") from e

            # Das Formular navigiert gezielt das Player-iframe. Auf dessen finaler
            # URL liegt der VOE-Player (die Domain wechselt regelmäßig).
            async def _current_voe_frame_url() -> str | None:
                for frame in page.frames:
                    frame_url = frame.url or ''
                    if '/e/' in frame_url and not _is_seriesstream_url(frame_url):
                        return frame_url
                iframe = page.locator('#player-iframe')
                if await iframe.count() == 1:
                    iframe_src = await iframe.get_attribute('src')
                    if iframe_src and '/e/' in iframe_src and not _is_seriesstream_url(iframe_src):
                        return iframe_src
                return None

            voe_url = await _current_voe_frame_url()
            submit = page.locator('#playerPrepareModal button[type="submit"]')
            for submit_attempt in range(1, 4):
                if voe_url:
                    break
                if await submit.count() != 1:
                    logging.warning("SerienStream: Weiter-Button fehlt (Versuch %s)", submit_attempt)
                    await asyncio.sleep(2)
                    continue
                try:
                    await submit.wait_for(state='visible', timeout=15000)
                except Exception:
                    pass

                # requestSubmit() durchläuft die native Formularvalidierung und
                # übergibt den geklickten Button. Das ist zuverlässiger als ein
                # synthetischer click(), besonders nach dem ALTCHA-Event.
                submitted = await page.evaluate("""() => {
                    const b = document.querySelector('#playerPrepareModal button[type="submit"]');
                    const form = b?.form || document.querySelector('#playerPrepareModal form');
                    if (!b || !form) return false;
                    if (typeof form.requestSubmit === 'function') form.requestSubmit(b);
                    else b.click();
                    return true;
                }""")
                if not submitted:
                    try:
                        await submit.click(force=True, timeout=15000)
                    except Exception:
                        pass

                for _ in range(60):
                    voe_url = await _current_voe_frame_url()
                    if voe_url:
                        break
                    await asyncio.sleep(0.5)
                if not voe_url:
                    logging.warning(
                        "SerienStream: Keine Iframe-Navigation nach Submit-Versuch %s/3",
                        submit_attempt,
                    )

            if not voe_url:
                raise RuntimeError("SerienStream: Keine VOE-Weiterleitung nach ALTCHA gefunden")

            source_voe_url = voe_url
            logging.info(f"SerienStream: VOE URL extrahiert: {voe_url}")
            if source_only:
                logging.info("SerienStream: Source-only – überspringe Player/HLS-Auflösung")
                return (None, film_title, None, "", source_voe_url)
            await page.goto(voe_url, timeout=120000, wait_until='domcontentloaded')
            await asyncio.sleep(3)

        # ─── Serie: Seite direkt öffnen ───
        else:
            logging.info(f"Serie: Öffne direkt: {page_url}")
            await page.goto(page_url, timeout=60000, wait_until='domcontentloaded')
            await asyncio.sleep(3)

        # Referer + Cookies für ffmpeg sammeln
        referer = page.url
        # Für Serien/Anime ist die finale Player-URL die "Quell-URL" (zum DB-Vergleich)
        if not source_voe_url and content_type in ('serie', 'anime'):
            source_voe_url = page.url
        logging.info(f"Seite nach Redirect: {referer}")
        all_cookies = await context.cookies()
        # Nur Cookies der Player-Domain (marissasharecareer / voe.sx) – nicht alle 60+ Tracking-Cookies
        from urllib.parse import urlparse as _up
        referer_host = _up(referer).hostname or ''
        player_domain = '.'.join(referer_host.split('.')[-2:]) if referer_host else ''
        relevant = [c for c in all_cookies if player_domain and player_domain in c.get('domain', '')]
        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in (relevant or all_cookies[:5]))
        logging.info(f"Cookies: {len(all_cookies)} gesamt → {len(relevant or all_cookies[:5])} relevant ({len(cookie_str)} Zeichen)")

        # Frame-URLs loggen
        all_frames = page.frames
        for i, frame in enumerate(all_frames):
            logging.info(f"Frame {i}: {frame.url[:120]}")

        # Play-Button suchen – nur in "echten" Player-Frames (keine Ad/Sync-Iframes)
        # Ad-Domains überspringen, das spart bis zu Minuten an Click-Timeouts
        AD_HOSTS = ('pubmatic', 'rubiconproject', 'connatix', 'openx', 'adnxs',
                    'adform', 'ingage.tech', 'yellowblue', 'id5-sync', 'minutemedia')
        player_frames = [f for f in all_frames
                         if f.url and 'about:blank' not in f.url
                         and not any(h in f.url for h in AD_HOSTS)]
        # Plus den Top-Frame
        if page.main_frame not in player_frames:
            player_frames.insert(0, page.main_frame)

        logging.info(f"Play-Loop: {len(player_frames)} relevante Frames (von {len(all_frames)})")
        for frame in player_frames:
            if _streams['m3u8'] or _streams['mpd']:
                break
            for selector in ['video', '.vjs-big-play-button', '[class*="play"]', '.plyr__control--overlaid', 'button[class*="play"]']:
                try:
                    await frame.click(selector, timeout=1000)
                    logging.info(f"Play geklickt: {selector} in {frame.url[:60]}")
                    await asyncio.sleep(3)
                    if _streams['m3u8'] or _streams['mpd']:
                        break
                except:
                    pass

        # Warten auf Stream – m3u8/mpd bevorzugen, MP4 nur als Fallback nach 8 Sekunden
        for i in range(20):
            if _streams['m3u8'] or _streams['mpd']:
                break
            if _streams['mp4'] and i >= 8:
                break
            await asyncio.sleep(1)

        # JW Player 6 bei VOE entscheidet sich im Playwright-Chromium oft direkt für MP4,
        # obwohl die master.m3u8 in den Sources liegt. Scan DOM/JW-Playlist nach m3u8,
        # denn die ist kleiner und lässt sich sauber auf 720p runterbrechen.
        if not _streams['m3u8']:
            try:
                for frame in page.frames:
                    try:
                        m3u8_from_dom = await frame.evaluate('''() => {
                            const found = new Set();
                            // 1) JW Player Playlist (falls Setup fertig)
                            try {
                                if (typeof jwplayer !== 'undefined') {
                                    const pl = jwplayer().getPlaylist() || [];
                                    for (const item of pl) {
                                        for (const src of (item.sources || [])) {
                                            if (src && src.file && src.file.includes('.m3u8')) found.add(src.file);
                                        }
                                    }
                                }
                            } catch(e) {}
                            // 2) Regex über HTML / inline JS nach .m3u8-URLs
                            // WICHTIG: Stop-Chars umfassen auch '%' und '&', damit wir nicht
                            // URL-encoded m3u8-Werte aus Tracking-Querys matchen
                            // (z. B. https://tracker.gif?mu=https%3A%2F%2F…m3u8…).
                            try {
                                const html = document.documentElement ? document.documentElement.outerHTML : '';
                                const rx = /https?:\\/\\/[^"'\\s<>&%]+\\.m3u8(?:\\?[^"'\\s<>]*)?/gi;
                                const matches = html.match(rx) || [];
                                for (const m of matches) found.add(m);
                            } catch(e) {}
                            return Array.from(found);
                        }''')
                        for u in (m3u8_from_dom or []):
                            if u and '.m3u8' in u:
                                _streams['m3u8'] = u
                                logging.info(f">>> HLS aus DOM gefunden: {u[:200]}")
                                break
                        if _streams['m3u8']:
                            break
                    except Exception:
                        pass
            except Exception as e:
                logging.warning(f"DOM-Scan nach m3u8 fehlgeschlagen: {e}")

        stream_url = _current_stream()

        if not stream_url:
            try:
                page_text = (await page.locator('body').inner_text(timeout=3000)).lower()
            except Exception:
                page_text = ''
            permanent_markers = (
                'datei wurde gelöscht', 'datei wurde entfernt',
                'video wurde gelöscht', 'video wurde entfernt',
                'nicht mehr verfügbar', 'existiert nicht mehr',
                'file was deleted', 'file has been deleted',
                'video was deleted', 'video has been deleted',
                'file was removed', 'video was removed',
                'file no longer exists', 'no longer available',
            )
            if any(marker in page_text for marker in permanent_markers):
                await browser.close()
                raise PermanentMediaUnavailable('Hoster meldet: Datei wurde dauerhaft entfernt')
            screenshot_path = os.path.join(DOWNLOAD_DIR, "debug_screenshot.png")
            await page.screenshot(path=screenshot_path, full_page=True)
            logging.warning(f"Kein Stream! Screenshot: {screenshot_path}")
        else:
            stream_type = 'HLS' if _streams['m3u8'] else ('DASH' if _streams['mpd'] else 'MP4')
            logging.info(f"Stream gewählt: {stream_type} – {stream_url[:120]}")

        await browser.close()

    return stream_url, film_title, referer, cookie_str, source_voe_url


def resolve_normal_quality(master_url: str, referer: str | None, cookie_str: str) -> str:
    """Parsed die master.m3u8 und gibt die URL der 'n' (normal/720p) Qualität zurück.
    Fallback: original master_url wenn Parsing fehlschlägt."""
    try:
        headers = {'User-Agent': USER_AGENT}
        if referer:
            headers['Referer'] = referer
        if cookie_str:
            headers['Cookie'] = cookie_str

        r = requests.get(master_url, headers=headers, timeout=10)
        r.raise_for_status()

        lines = [l.strip() for l in r.text.splitlines() if l.strip()]
        # Stream-URLs = Zeilen ohne '#'
        stream_lines = [l for l in lines if not l.startswith('#')]

        if len(stream_lines) < 2:
            logging.warning("resolve_normal_quality: Weniger als 2 Streams gefunden, nutze master")
            return master_url

        # Index 1 = zweiter Stream = n (normal/720p)
        # Index 0 = h (high), Index 1 = n (normal), Index 2 = l (low)
        chosen = stream_lines[1]

        # Relative URL → absolute URL (urlparse um Query-Params mit '/' zu ignorieren)
        if not chosen.startswith('http'):
            from urllib.parse import urlparse
            parsed = urlparse(master_url)
            base_path = parsed.path.rsplit('/', 1)[0]
            base = f"{parsed.scheme}://{parsed.netloc}{base_path}"
            chosen = f"{base}/{chosen}"

        logging.info(f"resolve_normal_quality: {len(stream_lines)} Streams gefunden, wähle Index 1: {chosen[:120]}")
        return chosen

    except Exception as e:
        logging.warning(f"resolve_normal_quality fehlgeschlagen: {e} – nutze master_url")
        return master_url


def resolve_mp4_quality(mp4_url: str, preferred: str = 'n') -> str:
    """Tauscht den VOE/CDN-Qualitäts-Suffix einer MP4-URL aus.
    preferred: 'h' (high), 'n' (normal/mittel), 'l' (low). Default: 'n'."""
    try:
        m = re.search(r'_([hnl])\.mp4', mp4_url, re.IGNORECASE)
        if not m:
            return mp4_url
        current = m.group(1).lower()
        if current == preferred:
            return mp4_url
        new_url = mp4_url.replace(f'_{current}.mp4', f'_{preferred}.mp4', 1)
        logging.info(f"resolve_mp4_quality: {current} → {preferred}")
        return new_url
    except Exception as e:
        logging.warning(f"resolve_mp4_quality fehlgeschlagen: {e} – nutze original")
        return mp4_url


async def download_with_browser(stream_url: str, filepath: str, referer: str | None, cookie_str: str = "") -> bool:
    """Lädt HLS-Stream via Playwright-Chromium herunter.
    Umgeht TLS-Fingerprint-Erkennung & Single-Use-Token-Schutz."""
    if '.m3u8' not in stream_url:
        return False

    logging.info(f"Browser-Download startet: {stream_url[:120]}")
    import time as _time

    async with _open_browser(camoufox=False) as browser:

        cookies_for_context = []
        if cookie_str and referer:
            from urllib.parse import urlparse as _up
            host = _up(referer).hostname
            for c in cookie_str.split('; '):
                if '=' in c:
                    k, v = c.split('=', 1)
                    cookies_for_context.append({'name': k, 'value': v, 'domain': host, 'path': '/'})

        ctx = await browser.new_context(user_agent=USER_AGENT)
        if cookies_for_context:
            try:
                await ctx.add_cookies(cookies_for_context)
            except Exception as e:
                logging.warning(f"Cookies setzen fehlgeschlagen: {e}")

        try:
            extra = {'Referer': referer} if referer else {}

            async def _fetch_m3u8(url: str) -> str | None:
                r = await ctx.request.get(url, headers=extra, timeout=30000)
                if not r.ok:
                    logging.error(f"Browser-Download: m3u8 status={r.status} bei {url[:80]}")
                    return None
                # binary-safe: bytes holen, dann decodieren mit Fallback
                raw = await r.body()
                for enc in ('utf-8', 'latin-1'):
                    try:
                        return raw.decode(enc)
                    except UnicodeDecodeError:
                        continue
                return raw.decode('utf-8', errors='replace')

            from urllib.parse import urljoin

            master_text = await _fetch_m3u8(stream_url)
            if master_text is None:
                return False

            # Nur dann als Master behandeln, wenn EXT-X-STREAM-INF vorhanden
            is_master = '#EXT-X-STREAM-INF' in master_text
            if is_master:
                variants = []
                current_bw = None
                for line in master_text.splitlines():
                    line = line.strip()
                    if line.startswith('#EXT-X-STREAM-INF'):
                        m = re.search(r'BANDWIDTH=(\d+)', line)
                        current_bw = int(m.group(1)) if m else 0
                    elif line and not line.startswith('#') and current_bw is not None:
                        variants.append((current_bw, urljoin(stream_url, line)))
                        current_bw = None

                if variants:
                    variants.sort()
                    chosen_url = variants[len(variants) // 2][1]
                    logging.info(f"Browser-Download: {len(variants)} Varianten, gewählt #{len(variants)//2}")
                    playlist_text = await _fetch_m3u8(chosen_url)
                    if playlist_text is None:
                        return False
                    playlist_url = chosen_url
                else:
                    playlist_text = master_text
                    playlist_url = stream_url
            else:
                logging.info("Browser-Download: bereits Variant-Playlist")
                playlist_text = master_text
                playlist_url = stream_url

            segments = [urljoin(playlist_url, ln.strip()) for ln in playlist_text.splitlines()
                        if ln.strip() and not ln.strip().startswith('#')]
            if not segments:
                logging.error("Browser-Download: keine Segmente gefunden")
                return False
            logging.info(f"Browser-Download: {len(segments)} Segmente")

            ts_path = filepath.replace('.mp4', '.ts')
            seg_data = [None] * len(segments)
            sem = asyncio.Semaphore(8)
            start = _time.monotonic()

            async def fetch_seg(idx, url):
                async with sem:
                    for _ in range(3):
                        try:
                            r = await ctx.request.get(url, headers=extra, timeout=30000)
                            if r.ok:
                                seg_data[idx] = await r.body()
                                return
                            logging.warning(f"Seg {idx} status={r.status}")
                        except Exception as e:
                            logging.warning(f"Seg {idx}: {e}")
                        await asyncio.sleep(1)

            await asyncio.gather(*[fetch_seg(i, u) for i, u in enumerate(segments)])

            failed = sum(1 for d in seg_data if d is None)
            if failed > 0:
                logging.error(f"Browser-Download: {failed}/{len(segments)} Segmente fehlen")
                return False

            with open(ts_path, 'wb') as f:
                for data in seg_data:
                    f.write(data)

            dl_time = _time.monotonic() - start
            ts_mb = os.path.getsize(ts_path) / (1024 * 1024)
            logging.info(f"Browser-Download fertig: {ts_mb:.0f} MB in {dl_time:.0f}s")

            cmd = ['ffmpeg', '-i', ts_path, '-c', 'copy', '-bsf:a', 'aac_adtstoasc',
                   '-loglevel', 'error', filepath, '-y']
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                creationflags=_NO_WINDOW,
            )
            _, stderr = await proc.communicate()
            try:
                os.remove(ts_path)
            except Exception:
                pass

            if proc.returncode != 0:
                logging.error(f"ffmpeg-mux Fehler: {stderr.decode(errors='replace')[-300:]}")
                return False
            return True
        finally:
            await browser.close()


async def download_with_ytdlp(stream_url: str, filepath: str, referer: str | None = None, cookie_str: str = "", preferred_height: int | None = None) -> bool:
    """Primärer Download via yt-dlp mit parallelen Fragmenten (schneller als ffmpeg)."""
    logging.info(f"yt-dlp startet: {stream_url[:100]} | quality={preferred_height or 'best'}")
    import sys
    import shutil
    ytdlp_bin = 'yt-dlp'
    if _sys == "Windows":
        candidates = [
            os.path.join(os.path.dirname(sys.executable), 'Scripts', 'yt-dlp.exe'),
            os.path.join(os.path.dirname(sys.executable), 'yt-dlp.exe'),
        ]
        for c in candidates:
            if os.path.exists(c):
                ytdlp_bin = c
                break
    else:
        candidates = [
            os.path.join(os.path.dirname(sys.executable), 'yt-dlp'),
            os.path.expanduser('~/Library/Python/3.9/bin/yt-dlp'),
            os.path.expanduser('~/Library/Python/3.11/bin/yt-dlp'),
            shutil.which('yt-dlp') or '',
        ]
        for c in candidates:
            if c and os.path.exists(c):
                ytdlp_bin = c
                break

    # Zieldatei ohne Extension angeben, yt-dlp ergänzt sie selbst
    out_template = filepath.removesuffix('.mp4')

    cmd = [
        ytdlp_bin,
        '--concurrent-fragments', '16',
        '--no-warnings',
        '--no-playlist',
        '-o', out_template + '.%(ext)s',
        '--merge-output-format', 'mp4',
        '--user-agent', USER_AGENT,
        '--impersonate', 'chrome',  # TLS-Fingerprint von Chrome (umgeht JA3/JA4 CDN-Blocking)
    ]
    if preferred_height:
        cmd += ['-f', f'bestvideo[height<={preferred_height}]+bestaudio/best[height<={preferred_height}]']
    if referer:
        from urllib.parse import urlparse as _up
        _o = _up(referer)
        origin = f"{_o.scheme}://{_o.netloc}"
        cmd += [
            '--add-header', f'Referer:{referer}',
            '--add-header', f'Origin:{origin}',
            '--add-header', 'Sec-Fetch-Mode:cors',
            '--add-header', 'Sec-Fetch-Site:cross-site',
            '--add-header', 'Sec-Fetch-Dest:empty',
            '--add-header', 'Accept:*/*',
            '--add-header', 'Accept-Language:de-DE,de;q=0.9,en;q=0.8',
        ]
    if cookie_str:
        cmd += ['--add-header', f'Cookie:{cookie_str}']
    cmd.append(stream_url)

    global _active_proc
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        creationflags=_NO_WINDOW,
    )
    _active_proc = proc
    _, stderr = await proc.communicate()
    _active_proc = None

    if proc.returncode == 0:
        logging.info("yt-dlp: fertig")
        return True
    if proc.returncode in (-9, -15):
        logging.info("yt-dlp: abgebrochen")
        return False
    error_msg = stderr.decode(errors='replace')
    logging.error(f"yt-dlp Fehler (returncode={proc.returncode}):\n{error_msg}")
    return False


async def download_with_ffmpeg(stream_url: str, filepath: str, referer: str | None = None, cookie_str: str = "") -> bool:
    logging.info(f"ffmpeg startet: {stream_url[:100]}")
    logging.info(f"ffmpeg Ziel: {filepath}")

    from urllib.parse import urlparse
    cmd = ['ffmpeg']
    if referer:
        origin = f"{urlparse(referer).scheme}://{urlparse(referer).netloc}"
        if 'remote_control.php' in stream_url and 'filevideo1.com' in stream_url:
            headers = (
                f"Referer: {referer}\r\n"
                f"User-Agent: {USER_AGENT}\r\n"
                "Accept: */*\r\n"
                "Accept-Encoding: identity;q=1, *;q=0\r\n"
                "Accept-Language: de-DE,de;q=0.5\r\n"
                "Sec-Fetch-Dest: video\r\n"
                "Sec-Fetch-Mode: no-cors\r\n"
                "Sec-Fetch-Site: cross-site\r\n"
                "Sec-GPC: 1\r\n"
                'sec-ch-ua: "Brave";v="149", "Chromium";v="149", "Not)A;Brand";v="24"' + "\r\n"
                "sec-ch-ua-mobile: ?0\r\n"
                'sec-ch-ua-platform: "macOS"' + "\r\n"
            )
        else:
            headers = (
                f"Referer: {referer}\r\n"
                f"Origin: {origin}\r\n"
                f"User-Agent: {USER_AGENT}\r\n"
                "Sec-Fetch-Mode: cors\r\n"
                "Sec-Fetch-Site: cross-site\r\n"
                "Sec-Fetch-Dest: empty\r\n"
                "Accept: */*\r\n"
                "Accept-Language: de-DE,de;q=0.9,en;q=0.8\r\n"
            )
        if cookie_str:
            headers += f"Cookie: {cookie_str}\r\n"
        cmd += ['-headers', headers]
        logging.info(f"ffmpeg Referer: {referer}, Cookies: {len(cookie_str)} Zeichen")
    cmd += ['-i', stream_url, '-c', 'copy', '-bsf:a', 'aac_adtstoasc',
            '-aspect', '16:9', '-loglevel', 'error', filepath, '-y']

    global _active_proc
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        creationflags=_NO_WINDOW,
    )
    _active_proc = proc
    _, stderr = await proc.communicate()
    _active_proc = None

    if proc.returncode in (-9, -15):
        logging.info("ffmpeg: abgebrochen")
        return False
    if proc.returncode != 0:
        logging.error(f"ffmpeg Fehler: {stderr.decode(errors='replace')[-500:]}")
        return False
    logging.info("ffmpeg: fertig")
    return True


# ─── Upload ──────────────────────────────────────

def get_video_metadata(filepath: str) -> dict:
    """Liest Width, Height, Duration via ffprobe"""
    try:
        import json
        result = subprocess.run([
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_streams', '-select_streams', 'v:0', filepath
        ], capture_output=True, text=True, timeout=30, creationflags=_NO_WINDOW)
        data = json.loads(result.stdout)
        stream = data.get('streams', [{}])[0]
        width    = stream.get('width', 1280)
        height   = stream.get('height', 720)
        duration = float(stream.get('duration', 0))
        # DAR (Display Aspect Ratio) korrigieren
        dar = stream.get('display_aspect_ratio', '')
        if dar and ':' in dar and dar != '0:1':
            try:
                w, h = map(int, dar.split(':'))
                if h > 0:
                    width = int(height * w / h)
            except Exception:
                pass
        return {'width': width, 'height': height, 'duration': int(duration)}
    except Exception as e:
        logging.warning(f"ffprobe Fehler: {e}")
        return {'width': 1280, 'height': 720, 'duration': 0}


SIZE_LIMIT_MB = 2048  # Telegram-Limit: 2 GB

async def downscale_if_needed(filepath: str, msg) -> str:
    """Skaliert Video herunter wenn > SIZE_LIMIT_MB. Gibt den (ggf. neuen) Pfad zurück."""
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    logging.info(f"downscale_if_needed: {size_mb:.0f} MB (Limit: {SIZE_LIMIT_MB} MB)")
    if size_mb <= SIZE_LIMIT_MB:
        logging.info("downscale_if_needed: Datei im Limit, kein Downscale nötig")
        return filepath

    meta = get_video_metadata(filepath)
    height = meta['height']

    if height > 1080:
        target_h = 1080
    elif height > 720:
        target_h = 720
    else:
        target_h = None

    if target_h:
        scale_filter = f'scale=-2:{target_h}'
        label = f"{height}p → {target_h}p"
    else:
        scale_filter = None
        label = "Bitrate reduzieren"

    await msg.edit_text(
        f"⚠️ Datei zu groß ({size_mb:.0f} MB)\n🔄 Komprimiere: {label}...",
        parse_mode='Markdown'
    )

    scaled_path = filepath.replace('.mp4', '_scaled.mp4')

    cmd = ['ffmpeg', '-i', filepath]
    if scale_filter:
        cmd += ['-vf', scale_filter]
    cmd += [
        '-c:v', 'libx264', '-crf', '23', '-preset', 'fast',
        '-c:a', 'copy',
        '-loglevel', 'error',
        scaled_path, '-y'
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        creationflags=_NO_WINDOW,
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0 or not os.path.exists(scaled_path):
        logging.error(f"Downscale fehlgeschlagen (rc={proc.returncode}): {stderr.decode(errors='replace')[-1000:]}")
        await msg.edit_text("⚠️ Komprimierung fehlgeschlagen – versuche Upload mit Originaldatei...")
        return filepath

    new_size_mb = os.path.getsize(scaled_path) / (1024 * 1024)
    logging.info(f"Downscale: {size_mb:.0f} MB → {new_size_mb:.0f} MB ({label})")
    os.replace(scaled_path, filepath)

    # Immer noch zu groß? Nochmal mit höherem CRF (stärkere Kompression)
    if new_size_mb > SIZE_LIMIT_MB:
        logging.warning(f"Immer noch zu groß ({new_size_mb:.0f} MB) – komprimiere stärker (CRF 28)...")
        await msg.edit_text(
            f"⚠️ Noch zu groß ({new_size_mb:.0f} MB)\n🔄 Komprimiere stärker...",
            parse_mode='Markdown'
        )
        scaled_path2 = filepath.replace('.mp4', '_scaled2.mp4')
        cmd2 = ['ffmpeg', '-i', filepath,
                '-c:v', 'libx264', '-crf', '28', '-preset', 'fast',
                '-c:a', 'copy', '-loglevel', 'error', scaled_path2, '-y']
        proc2 = await asyncio.create_subprocess_exec(
            *cmd2,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            creationflags=_NO_WINDOW,
        )
        _, stderr2 = await proc2.communicate()
        if proc2.returncode == 0 and os.path.exists(scaled_path2):
            final_mb = os.path.getsize(scaled_path2) / (1024 * 1024)
            logging.info(f"Zweiter Downscale: {new_size_mb:.0f} MB → {final_mb:.0f} MB")
            os.replace(scaled_path2, filepath)
        else:
            logging.error("Zweiter Downscale fehlgeschlagen")

    return filepath


async def remux_video_audio_only(filepath: str, msg=None) -> str:
    """Entfernt Zusatzspuren aus MP4s, die Telegram-Clients beim Audio stören können."""
    cleaned_path = filepath.replace('.mp4', '_clean.mp4')
    cmd = [
        'ffmpeg', '-i', filepath,
        '-map', '0:v:0',
        '-map', '0:a:0',
        '-sn',
        '-dn',
        '-map_chapters', '-1',
        '-map_metadata', '-1',
        '-c:v', 'copy',
        '-c:a', 'copy',
        '-movflags', '+faststart',
        '-loglevel', 'error',
        cleaned_path, '-y',
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        creationflags=_NO_WINDOW,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0 or not os.path.exists(cleaned_path):
        logging.warning(f"MP4-Remux übersprungen: {stderr.decode(errors='replace')[-500:]}")
        return filepath

    os.replace(cleaned_path, filepath)
    logging.info("MP4-Remux: nur Video+Audio behalten, Zusatzspuren entfernt")
    return filepath


async def generate_thumbnail(filepath: str) -> str | None:
    """Extrahiert Frame bei 5s als Thumbnail"""
    try:
        thumb_path = filepath.replace('.mp4', '_thumb.jpg')
        proc = await asyncio.create_subprocess_exec(
            'ffmpeg', '-i', filepath,
            '-ss', '00:00:05', '-vframes', '1',
            '-vf', 'scale=320:320:force_original_aspect_ratio=decrease',
            '-q:v', '5',
            thumb_path, '-y',
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            creationflags=_NO_WINDOW,
        )
        await proc.wait()
        if os.path.exists(thumb_path):
            logging.info(f"Thumbnail erzeugt: {thumb_path} ({os.path.getsize(thumb_path)} Bytes)")
            return thumb_path
        return None
    except Exception as e:
        logging.warning(f"Thumbnail Fehler: {e}")
        return None


async def _parallel_upload_file(filepath: str, progress_callback=None):
    """Lädt Datei mit mehreren echten TCP-Verbindungen parallel hoch.

    Anders als 'nur' viele asyncio-Tasks auf einem Sender haben wir hier CONNECTIONS
    unabhängige MTProto-Senders (wie Telegram Desktop), jeder mit eigenem TCP-Fenster
    und Congestion-Control. Pro Sender laufen WORKERS_PER_CONN asyncio-Tasks.
    """
    import math, random
    from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
    from telethon.tl.types import InputFileBig, InputFile

    PART_SIZE         = 512 * 1024   # 512 KB (Telegram-Maximum)
    # Pro Gerät via .env tunen:
    #   UPLOAD_CONNECTIONS=8  (schnelle Home-Leitung, default)
    #   UPLOAD_CONNECTIONS=2  (Remote-Desktop / VPS mit Throttling)
    #   UPLOAD_CONNECTIONS=1  (Fallback wenn Provider parallele TCPs blockt)
    CONNECTIONS       = int(os.getenv("UPLOAD_CONNECTIONS", "8"))
    WORKERS_PER_CONN  = int(os.getenv("UPLOAD_WORKERS_PER_CONN", "4"))
    WORKERS           = CONNECTIONS * WORKERS_PER_CONN

    file_size  = os.path.getsize(filepath)
    part_count = math.ceil(file_size / PART_SIZE)
    file_id    = random.randint(-(2**63), 2**63 - 1)
    is_big     = file_size >= 10 * 1024 * 1024

    uploaded_bytes = [0]
    semaphore = asyncio.Semaphore(WORKERS)

    # ── Pool aus CONNECTIONS parallelen MTProto-Sendern zum SELBEN DC ─────────
    # WICHTIG: Für Same-DC dürfen wir _create_exported_sender NICHT nutzen – das
    # ruft intern ExportAuthorization auf, was Telegram für den eigenen DC
    # ablehnt ("authorization is tried to be exported for the same data center").
    # Stattdessen öffnen wir neue TCP-Verbindungen und wiederverwenden den
    # bestehenden Session-AuthKey. Genau das macht auch Telegram Desktop.
    from telethon.network import MTProtoSender
    dc_id = tg_client.session.dc_id
    senders: list = []
    senders_usable = True
    try:
        if not tg_client.is_connected():
            await tg_client.connect()

        dc = await tg_client._get_dc(dc_id)
        conn_cls = type(tg_client._sender._connection)  # ConnectionTcpFull o.ä.
        auth_key = tg_client.session.auth_key

        for idx in range(CONNECTIONS):
            sender = MTProtoSender(auth_key, loggers=tg_client._log)
            connection = conn_cls(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=tg_client._log,
                proxy=tg_client._proxy,
            )
            await sender.connect(connection)
            senders.append(sender)

        logging.info(f"Upload-Sender-Pool: {len(senders)} TCP-Verbindungen zu DC{dc_id}")
    except Exception as e:
        logging.warning(f"Same-DC-Sender-Pool fehlgeschlagen ({e}), Fallback auf 1 Connection")
        # aufräumen
        for s in senders:
            try: await s.disconnect()
            except Exception: pass
        senders = []
        senders_usable = False

    # ── File-Handle-Pool: 1 FD pro Worker (kein open() pro Part) ─────────────
    fd_pool: asyncio.Queue = asyncio.Queue()
    for _ in range(WORKERS):
        fd_pool.put_nowait(open(filepath, 'rb'))

    async def upload_part(part_index: int):
        async with semaphore:
            fd = await fd_pool.get()
            try:
                fd.seek(part_index * PART_SIZE)
                data = fd.read(PART_SIZE)
            finally:
                fd_pool.put_nowait(fd)

            req = (SaveBigFilePartRequest(
                       file_id=file_id, file_part=part_index,
                       file_total_parts=part_count, bytes=data)
                   if is_big else
                   SaveFilePartRequest(file_id=file_id, file_part=part_index, bytes=data))

            if senders_usable and senders:
                # Round-Robin: Part N geht an Sender N % CONNECTIONS
                sender = senders[part_index % len(senders)]
                await sender.send(req)
            else:
                # Fallback: Alles über den Haupt-Client
                await tg_client(req)

            # Kein Lock nötig: uploaded_bytes[0] += len(data) ist in CPython atomar,
            # solange es von async Code im selben Event-Loop aufgerufen wird (GIL schützt den Increment).
            uploaded_bytes[0] += len(data)
            if progress_callback:
                await progress_callback(uploaded_bytes[0], file_size)

    logging.info(f"Parallel-Upload: {part_count} Parts à 512 KB, "
                 f"{WORKERS} Workers über {max(1, len(senders))} Verbindungen")
    try:
        await asyncio.gather(*[upload_part(i) for i in range(part_count)])
    finally:
        # File-Handles schließen
        while not fd_pool.empty():
            try:
                fd_pool.get_nowait().close()
            except Exception:
                pass
        # Exported Senders disconnecten
        for s in senders:
            try:
                await s.disconnect()
            except Exception:
                pass

    filename = os.path.basename(filepath)
    if is_big:
        return InputFileBig(id=file_id, parts=part_count, name=filename)
    else:
        import hashlib
        md5 = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                md5.update(chunk)
        return InputFile(id=file_id, parts=part_count, name=filename, md5_checksum=md5.hexdigest())


async def upload_with_telethon(filepath: str, caption: str, progress_msg, progress_prefix: str = ""):
    from telethon.tl import functions, types
    from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo
    import time as _time

    meta       = get_video_metadata(filepath)
    thumb_path = await generate_thumbnail(filepath)
    last_time  = [0.0]
    last_progress_time = [_time.monotonic()]
    upload_start_time  = _time.monotonic()
    STALL_TIMEOUT = 300

    async def progress_callback(current, total):
        current_mb = current // 1024 // 1024
        total_mb   = total   // 1024 // 1024
        percent    = int(current / total * 100)
        now = _time.monotonic()
        last_progress_time[0] = now
        if now - last_time[0] >= 30.0:  # 20s → 30s: weniger Bot-API-Calls, robuster auf Remote
            last_time[0] = now
            bar = "█" * (percent // 10) + "░" * (10 - percent // 10)
            elapsed = max(0.1, now - upload_start_time)
            speed_mbps = (current / 1024 / 1024) / elapsed
            eta_s = int((total - current) / max(1, current / elapsed)) if current > 0 else 0
            eta_str = f"{eta_s // 60}m {eta_s % 60}s" if eta_s >= 60 else f"{eta_s}s"
            try:
                await progress_msg.edit_text(
                    f"{progress_prefix}📤 Upload: {bar} {percent}%\n"
                    f"{current_mb} MB / {total_mb} MB · {speed_mbps:.1f} MB/s · ETA {eta_str}"
                )
            except Exception as e:
                # Bot-API-Timeouts sind harmlos – der MTProto-Upload läuft davon
                # unabhängig weiter. Nur als DEBUG statt WARNING loggen.
                logging.debug(f"progress edit_text fehlgeschlagen (harmlos): {e}")

    async def watchdog(task: asyncio.Task):
        while not task.done():
            await asyncio.sleep(30)
            if task.done():
                break
            if _time.monotonic() - last_progress_time[0] > STALL_TIMEOUT:
                logging.warning("Upload eingefroren – breche ab")
                task.cancel()
                break

    try:
        # Phase 1: Datei parallel hochladen
        file_handle = await _parallel_upload_file(filepath, progress_callback)

        # Phase 2: Thumbnail explizit hochladen und Media direkt senden.
        # Bei unserem manuellen Parallel-Upload ist die Videodatei schon ein
        # InputFileBig; send_file sieht dann nicht mehr den lokalen MP4-Pfad.
        # Explizites InputMediaUploadedDocument macht Thumbnail/Attribute stabil.
        thumb_handle = None
        if thumb_path and os.path.exists(thumb_path):
            try:
                thumb_handle = await tg_client.upload_file(thumb_path)
                logging.info("Thumbnail explizit hochgeladen")
            except Exception as e:
                logging.warning(f"Thumbnail-Upload fehlgeschlagen, sende ohne Thumb: {e}")

        entity = await tg_client.get_input_entity(CHANNEL_ID)
        media = types.InputMediaUploadedDocument(
            file=file_handle,
            mime_type='video/mp4',
            attributes=[
                DocumentAttributeVideo(
                    duration=meta['duration'],
                    w=meta['width'],
                    h=meta['height'],
                    supports_streaming=True
                ),
                DocumentAttributeFilename(os.path.basename(filepath)),
            ],
            thumb=thumb_handle,
            force_file=False,
        )
        upload_coro = tg_client(functions.messages.SendMediaRequest(
            peer=entity,
            media=media,
            message=caption,
        ))
        upload_task = asyncio.ensure_future(upload_coro)
        watchdog_task = asyncio.create_task(watchdog(upload_task))
        try:
            await upload_task
        except asyncio.CancelledError:
            raise RuntimeError("Upload eingefroren (kein Fortschritt seit 5 Min.) – bitte erneut senden")
        finally:
            watchdog_task.cancel()
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass
    finally:
        if thumb_path and os.path.exists(thumb_path):
            os.remove(thumb_path)


# ─── Serien-Scraper ──────────────────────────────

_SERIESSTREAM_HOSTS = {'s.to', 'www.s.to', 'serienstream.to', 'www.serienstream.to'}


def _is_seriesstream_url(url: str) -> bool:
    """Erkennt die aktuelle Domain und alte gespeicherte s.to-Links."""
    try:
        from urllib.parse import urlparse
        return (urlparse(url).hostname or '').lower() in _SERIESSTREAM_HOSTS
    except Exception:
        return False


def _normalize_seriesstream_url(url: str) -> str:
    """Migriert alte s.to-Links transparent auf serienstream.to."""
    if not _is_seriesstream_url(url):
        return url
    from urllib.parse import urlsplit, urlunsplit
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme or 'https', 'serienstream.to', parsed.path, parsed.query, parsed.fragment))


def _fetch_seriesstream_series(series_url: str) -> tuple[str, list[dict]]:
    """Liest Titel, Staffeln und Episoden aus dem statischen SerienStream-HTML."""
    from urllib.parse import urljoin

    series_url = _normalize_seriesstream_url(series_url).rstrip('/')
    season_match = re.search(r'/staffel-(\d+)', series_url)
    only_season = int(season_match.group(1)) if season_match else None
    series_base_url = series_url[:season_match.start()] if season_match else series_url
    headers = {'User-Agent': USER_AGENT, 'Referer': 'https://serienstream.to/'}

    overview_res = requests.get(series_base_url, headers=headers, timeout=30)
    overview_res.raise_for_status()
    overview = _sto_parse_html(overview_res.text, series_base_url)
    season_nums = overview['season_nums']
    if only_season is not None:
        season_nums = [season for season in season_nums if season == only_season]

    raw_episodes: list[dict] = []
    for season_num in season_nums:
        season_url = f"{series_base_url}/staffel-{season_num}"
        season_res = requests.get(season_url, headers=headers, timeout=30)
        season_res.raise_for_status()
        soup = BeautifulSoup(season_res.text, 'html.parser')
        season_episodes: dict[int, str] = {}
        for link in soup.select('a[href*="/episode-"]'):
            href = link.get('href', '')
            episode_match = re.search(r'/episode-(\d+)', href)
            if not episode_match:
                continue
            episode_num = int(episode_match.group(1))
            season_episodes.setdefault(episode_num, urljoin('https://serienstream.to', href))

        logging.info(f"SerienStream Staffel {season_num}: {len(season_episodes)} Folgen")
        for episode_num, episode_url in sorted(season_episodes.items()):
            raw_episodes.append({
                'season': season_num,
                'num': episode_num,
                'url': episode_url,
            })

    return overview['title'], raw_episodes


async def get_series_episodes(series_url: str, is_anime: bool = False) -> tuple[str, list[dict]]:
    """Liest alle Folgen. Gibt (series_title, episodes) zurück."""
    episodes = []
    series_title = "Serie"

    if _is_seriesstream_url(series_url):
        normalized_url = _normalize_seriesstream_url(series_url)
        logging.info(f"Serien-Scraper (serienstream.to): {normalized_url}")
        series_title, raw_eps = await asyncio.get_event_loop().run_in_executor(
            None, _fetch_seriesstream_series, normalized_url
        )
        all_titles = [f"{series_title} S{e['season']:02d}E{e['num']:02d}" for e in raw_eps]
        existing_map = _db_exists_batch('serie', all_titles, is_anime)
        for ep in raw_eps:
            ep_title = f"{series_title} S{ep['season']:02d}E{ep['num']:02d}"
            episodes.append({
                'url': ep['url'],
                'season': ep['season'],
                'episode': ep['num'],
                'title': ep_title,
                'skip': existing_map.get(ep_title, False),
            })
        skip_count = sum(1 for ep in episodes if ep.get('skip', False))
        if skip_count:
            logging.info(f"Serie: {skip_count}/{len(episodes)} Folgen bereits vorhanden → werden übersprungen")
        return series_title, episodes

    # AniWorld-Übersichten sind statisches HTML. Dieser frühe Pfad vermeidet
    # unnötige Playwright-Starts beim täglichen Watchcheck.
    if 'aniworld.to' in series_url:
        logging.info(f"Serien-Scraper (aniworld.to): {series_url}")
        base = 'https://aniworld.to'
        only_season = None
        m = re.search(r'/staffel-(\d+)', series_url)
        if m:
            only_season = int(m.group(1))
            series_base_url = series_url[:series_url.index(f'/staffel-{m.group(1)}')]
        else:
            series_base_url = series_url.rstrip('/')

        def _fetch_aniworld_series() -> tuple[str, list[dict]]:
            headers = {'User-Agent': USER_AGENT, 'Referer': base + '/'}
            overview_res = requests.get(series_base_url, headers=headers, timeout=30)
            overview_res.raise_for_status()
            soup = BeautifulSoup(overview_res.text, 'html.parser')
            h1 = soup.select_one('h1[itemprop="name"]') or soup.select_one('h1')
            title = h1.get_text(' ', strip=True) if h1 else 'Anime'

            seasons = []
            for link in soup.select('a[href*="/staffel-"]'):
                href = link.get('href', '')
                if '/episode-' in href:
                    continue
                label = link.get('title', '') or link.get_text(' ', strip=True)
                if not re.search(r'\bStaffel\b', label, re.IGNORECASE):
                    continue
                sm = re.search(r'staffel-(\d+)', href)
                if sm:
                    seasons.append({'num': int(sm.group(1)), 'href': href})

            seen = set()
            unique_seasons = []
            for season in seasons:
                if season['num'] in seen:
                    continue
                seen.add(season['num'])
                unique_seasons.append(season)
            if only_season:
                unique_seasons = [s for s in unique_seasons if s['num'] == only_season]

            raw = []
            logging.info(f"aniworld Titel: {title}, Staffeln: {[s['num'] for s in unique_seasons]}")
            for season in unique_seasons:
                season_num = season['num']
                season_href = season['href']
                season_url = season_href if season_href.startswith('http') else base + season_href
                season_res = requests.get(season_url, headers=headers, timeout=30)
                season_res.raise_for_status()
                ssoup = BeautifulSoup(season_res.text, 'html.parser')
                ep_links = []
                for ep_link in ssoup.select('a[data-episode-id][href*="/episode-"]'):
                    href = ep_link.get('href', '')
                    try:
                        num = int(ep_link.get_text(' ', strip=True))
                    except ValueError:
                        continue
                    ep_links.append({'href': href, 'num': num})
                logging.info(f"aniworld Staffel {season_num}: {len(ep_links)} Folgen")
                for ep in ep_links:
                    raw.append({
                        'season': season_num,
                        'num': ep['num'],
                        'url': ep['href'] if ep['href'].startswith('http') else base + ep['href'],
                    })
            return title, raw

        series_title, raw_eps = await asyncio.get_event_loop().run_in_executor(None, _fetch_aniworld_series)
        all_titles = [f"{series_title} S{e['season']:02d}E{e['num']:02d}" for e in raw_eps]
        existing_map = _db_exists_batch('serie', all_titles, is_anime)
        skip_count_debug = sum(1 for v in existing_map.values() if v)
        logging.info(f"[DEBUG] DB-Check: {skip_count_debug}/{len(all_titles)} bereits vorhanden. Beispiel: {dict(list(existing_map.items())[:3])}")
        for ep in raw_eps:
            ep_title = f"{series_title} S{ep['season']:02d}E{ep['num']:02d}"
            episodes.append({
                'url': ep['url'],
                'season': ep['season'],
                'episode': ep['num'],
                'title': ep_title,
                'skip': existing_map.get(ep_title, False),
            })
        skip_count = sum(1 for ep in episodes if ep.get('skip', False))
        if skip_count:
            logging.info(f"Serie: {skip_count}/{len(episodes)} Folgen bereits vorhanden → werden übersprungen")
        return series_title, episodes

    async with _open_browser(camoufox=False) as browser:
        context = await browser.new_context()
        page = await context.new_page()

        # ── hdfilme.win Serien ──────────────────────────────────────────────
        if 'hdfilme.win' in series_url:
            logging.info(f"Serien-Scraper (hdfilme.win): {series_url}")

            def _fetch_hdfilme_series() -> tuple[str, list[dict]]:
                headers = {'User-Agent': USER_AGENT, 'Referer': 'https://hdfilme.win/'}
                res = requests.get(series_url, headers=headers, timeout=25)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, 'html.parser')

                h1 = soup.select_one('h1')
                title = h1.get_text(' ', strip=True) if h1 else 'Serie'
                title = re.sub(r'\s+hdfilme\s*$', '', title, flags=re.IGNORECASE).strip() or 'Serie'

                imdb_match = re.search(r"var\s+imdb\s*=\s*'([^']+)'", res.text)
                if not imdb_match:
                    return title, []

                imdb_id = imdb_match.group(1)
                check_url = f"https://meinecloud.click/serials.php?task=check&id_imdb={imdb_id}"
                check = requests.get(check_url, headers=headers, timeout=25)
                check.raise_for_status()
                data = check.json()
                player_url = data.get('player_url') if data.get('exists') else None
                if not player_url:
                    return title, []

                player = requests.get(player_url, headers={**headers, 'Referer': series_url}, timeout=25)
                player.raise_for_status()
                psoup = BeautifulSoup(player.text, 'html.parser')

                season_tabs = [
                    tab.get_text(' ', strip=True)
                    for tab in psoup.select('._stab[data-season]')
                ]
                parsed_eps: list[dict] = []
                for ep_el in psoup.select('._ep[data-link][data-label]'):
                    label = ep_el.get('data-label', '')
                    m = re.search(r'S(\d+)\s*E(\d+)', label, re.IGNORECASE)
                    if not m:
                        continue
                    season_num = int(m.group(1))
                    ep_num = int(m.group(2))
                    link = ep_el.get('data-link', '').strip()
                    if not link:
                        continue
                    if link.startswith('//'):
                        link = 'https:' + link
                    parsed_eps.append({
                        'season': season_num,
                        'num': ep_num,
                        'url': link,
                    })
                if season_tabs:
                    logging.info(
                        "HDFilme Staffeln gefunden: %s, Episodenlinks: %s",
                        ', '.join(season_tabs),
                        len(parsed_eps),
                    )
                return title, parsed_eps

            series_title, raw_eps = await asyncio.get_event_loop().run_in_executor(None, _fetch_hdfilme_series)
            await browser.close()

            all_titles = [f"{series_title} S{e['season']:02d}E{e['num']:02d}" for e in raw_eps]
            existing_map = _db_exists_batch('serie', all_titles, is_anime)
            logging.info(f"HDFilme Titel: {series_title}, Folgen: {len(raw_eps)}")
            for ep in raw_eps:
                ep_title = f"{series_title} S{ep['season']:02d}E{ep['num']:02d}"
                episodes.append({
                    'url': ep['url'],
                    'season': ep['season'],
                    'episode': ep['num'],
                    'title': ep_title,
                    'skip': existing_map.get(ep_title, False),
                })

        # ── aniworld.to ──────────────────────────────────────────────────────
        elif 'aniworld.to' in series_url:
            logging.info(f"Serien-Scraper (aniworld.to): {series_url}")
            parsed_url = series_url.replace('/staffel-', '/__STAFFEL__/').split('/__STAFFEL__/')[0]
            base = 'https://aniworld.to'

            # Staffel-Filter aus URL
            import re as _re
            only_season = None
            m = _re.search(r'/staffel-(\d+)', series_url)
            if m:
                only_season = int(m.group(1))
                series_base_url = series_url[:series_url.index(f'/staffel-{m.group(1)}')]
            else:
                series_base_url = series_url.rstrip('/')

            await page.goto(series_base_url, timeout=60000, wait_until='domcontentloaded')
            await asyncio.sleep(2)

            overview = await page.evaluate('''() => {
                const h1 = document.querySelector('h1[itemprop="name"]') || document.querySelector('h1');
                const seriesTitle = h1 ? h1.textContent.trim() : 'Anime';
                const seasonLinks = Array.from(
                    document.querySelectorAll('a[href*="/staffel-"][title^="Staffel"]')
                ).filter(a => !a.href.includes('/episode-'));
                return {
                    seriesTitle,
                    seasons: seasonLinks.map(a => {
                        const m = a.getAttribute('href').match(/staffel-(\\d+)/);
                        return { num: m ? parseInt(m[1]) : null, href: a.getAttribute('href') };
                    }).filter(s => s.num)
                };
            }''')

            series_title = overview.get('seriesTitle', 'Anime')
            seasons = overview.get('seasons', [])
            if only_season:
                seasons = [s for s in seasons if s['num'] == only_season]

            logging.info(f"aniworld Titel: {series_title}, Staffeln: {[s['num'] for s in seasons]}")

            raw_eps = []
            for season in seasons:
                season_num = season['num']
                season_href = season['href']
                season_url = season_href if season_href.startswith('http') else base + season_href
                await page.goto(season_url, timeout=60000, wait_until='domcontentloaded')
                await asyncio.sleep(1)

                ep_links = await page.evaluate('''() => {
                    return Array.from(
                        document.querySelectorAll('a[data-episode-id][href*="/episode-"]')
                    ).map(a => ({
                        href: a.getAttribute('href'),
                        num: parseInt(a.textContent.trim())
                    })).filter(e => !isNaN(e.num));
                }''')

                logging.info(f"aniworld Staffel {season_num}: {len(ep_links)} Folgen")
                for ep in ep_links:
                    raw_eps.append({
                        'season': season_num,
                        'num': ep['num'],
                        'url': ep['href'] if ep['href'].startswith('http') else base + ep['href'],
                    })

            await browser.close()

            all_titles = [f"{series_title} S{e['season']:02d}E{e['num']:02d}" for e in raw_eps]
            existing_map = _db_exists_batch('serie', all_titles, is_anime)
            skip_count_debug = sum(1 for v in existing_map.values() if v)
            logging.info(f"[DEBUG] DB-Check: {skip_count_debug}/{len(all_titles)} bereits vorhanden. Beispiel: {dict(list(existing_map.items())[:3])}")
            for ep in raw_eps:
                ep_title = f"{series_title} S{ep['season']:02d}E{ep['num']:02d}"
                episodes.append({
                    'url': ep['url'],
                    'season': ep['season'],
                    'episode': ep['num'],
                    'title': ep_title,
                    'skip': existing_map.get(ep_title, False),
                })

        # ── filmpalast.to Serien ─────────────────────────────────────────────
        elif 'filmpalast.to' in series_url:
            logging.info(f"Serien-Scraper (filmpalast.to): {series_url}")

            # Staffel und erste Episode aus URL ableiten
            _m_fp = re.search(r'-s(\d{2})e\d{2}', series_url, re.IGNORECASE)
            current_season = int(_m_fp.group(1)) if _m_fp else 1

            await page.goto(series_url, timeout=60000, wait_until='domcontentloaded')
            await asyncio.sleep(2)

            fp_result = await page.evaluate('''() => {
                // Serienname aus Seitentitel
                const h2 = document.querySelector('h2.bgDark');
                const pageTitle = h2 ? h2.textContent.trim() : null;

                // Alle Episoden aus der staffelEpisodenList
                const eps = Array.from(
                    document.querySelectorAll('ul.staffelEpisodenList li a.getStaffelStream')
                ).map(a => ({
                    href: a.getAttribute('href'),
                    num: parseInt(a.textContent.trim())
                })).filter(e => e.href && !isNaN(e.num));

                return { pageTitle, episodes: eps };
            }''')

            await browser.close()

            # Serienname: aus Seite oder Fallback aus URL
            page_title = fp_result.get('pageTitle') or ''
            if page_title:
                # Entferne Staffelzusatz z.B. "The Pitt – Staffel 2" → "The Pitt"
                _fp_series_title = re.sub(r'\s*[-–]\s*Staffel\s*\d+.*', '', page_title, flags=re.IGNORECASE).strip()
            else:
                _slug = series_url.rstrip('/').split('/')[-1]
                _slug = re.sub(r'-s\d{2}e\d{2}.*', '', _slug, flags=re.IGNORECASE)
                _fp_series_title = _slug.replace('-', ' ').title()

            series_title = _fp_series_title
            fp_ep_list = fp_result.get('episodes', [])
            logging.info(f"Filmpalast Serien-Titel: {series_title}, Staffel: {current_season}, {len(fp_ep_list)} Folgen")

            all_titles = [f"{series_title} S{current_season:02d}E{ep['num']:02d}" for ep in fp_ep_list]
            existing_map = _db_exists_batch('serie', all_titles, is_anime=False)

            for ep in fp_ep_list:
                ep_title = f"{series_title} S{current_season:02d}E{ep['num']:02d}"
                href = ep['href'] or ''
                if href.startswith('//'):
                    href = 'https:' + href
                elif href and not href.startswith('http'):
                    href = 'https://filmpalast.to' + href
                episodes.append({
                    'url': href,
                    'season': current_season,
                    'episode': ep['num'],
                    'title': ep_title,
                    'skip': existing_map.get(ep_title, False),
                    'content_type': 'filmpalast',  # VOE-Extraktion nutzen
                })

        # ── megakino2.net (original) ─────────────────────────────────────────
        else:
            logging.info(f"Serien-Scraper (megakino2.net): {series_url}")
            ep_data = None
            last_exc = None
            for attempt in range(3):
                try:
                    await page.goto(series_url, timeout=60000, wait_until='domcontentloaded')
                    try:
                        await page.wait_for_selector('h1[itemprop="name"], select[id^="ep"]', timeout=15000)
                    except Exception:
                        pass
                    await asyncio.sleep(2 + attempt)
                    try:
                        await page.wait_for_load_state('networkidle', timeout=3000)
                    except Exception:
                        pass

                    ep_data = await page.evaluate('''() => {
                        const h1 = document.querySelector('h1[itemprop="name"]');
                        let titleText = h1 ? h1.textContent.trim() : "Serie";

                        let seriesTitle = titleText;
                        let seasonNum = 1;
                        // Tolerant gegenüber Tippfehlern: "Staffel", "Stafffel" (3 f's), "Staffeln" etc.
                        // Trennzeichen kann '-' sein oder ganz fehlen: "Stranger Things 5 Staffel"
                        const staffelMatch = titleText.match(/^(.+?)\\s*[-–—]?\\s*(?:(\\d+)\\s*\\.?\\s*Staf+eln?|Staf+eln?\\s*(\\d+))\\s*$/i);
                        if (staffelMatch) {
                            seriesTitle = staffelMatch[1].trim().replace(/[-–—]\\s*$/, '').trim();
                            seasonNum = parseInt(staffelMatch[2] || staffelMatch[3]);
                        }

                        const epSelects = Array.from(document.querySelectorAll('select[id^="ep"]'));
                        const episodes = [];
                        epSelects.forEach((selectEl, idx) => {
                            const option = selectEl.querySelector('option');
                            if (option && option.value) {
                                episodes.push({ num: idx + 1, url: option.value });
                            }
                        });
                        return { seriesTitle, seasonNum, episodes };
                    }''')
                    break
                except Exception as e:
                    last_exc = e
                    if 'Execution context was destroyed' not in str(e) or attempt == 2:
                        try:
                            await browser.close()
                        except Exception:
                            pass
                        raise
                    logging.warning(f"Megakino Serien-Scraper Retry {attempt + 1}/3 nach Navigation-Race: {e}")
                    await asyncio.sleep(1.5 * (attempt + 1))

            if ep_data is None and last_exc:
                try:
                    await browser.close()
                except Exception:
                    pass
                raise last_exc

            series_title = ep_data.get('seriesTitle', 'Serie')
            season_num = ep_data.get('seasonNum', 1)
            ep_list = ep_data.get('episodes', [])

            logging.info(f"Serien-Titel: {series_title}, Staffel: {season_num}, Folgen: {len(ep_list)}")

            all_titles = [f"{series_title} S{season_num:02d}E{ep['num']:02d}" for ep in ep_list]
            existing_map = _db_exists_batch('serie', all_titles, is_anime)

            for ep in ep_list:
                ep_title = f"{series_title} S{season_num:02d}E{ep['num']:02d}"
                episodes.append({
                    'url': ep['url'],
                    'season': season_num,
                    'episode': ep['num'],
                    'title': ep_title,
                    'skip': existing_map.get(ep_title, False),
                })

            await browser.close()

    # Logging: wie viele übersprungen werden
    skip_count = sum(1 for ep in episodes if ep.get('skip', False))
    if skip_count > 0:
        logging.info(f"Serie: {skip_count}/{len(episodes)} Folgen bereits vorhanden → werden übersprungen")

    return series_title, episodes


# ─── Bot Handler ─────────────────────────────────

def _journal_received_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Jeden empfangenen Link sofort sichern, noch bevor ein Dialog abgeschlossen ist."""
    text = (getattr(update.message, 'text', None) or '').strip()
    urls = re.findall(r'https?://\S+', text)
    if not urls:
        return
    manuel = context.user_data.get('manuel') or {}
    episode = None
    episodes_todo = manuel.get('episodes_todo') or []
    todo_idx = int(manuel.get('todo_idx', 0))
    if todo_idx < len(episodes_todo):
        episode = episodes_todo[todo_idx]
    event = {
        'timestamp': _utc_now_iso(),
        'message_id': getattr(update.message, 'message_id', None),
        'chat_id': getattr(update.effective_chat, 'id', None),
        'urls': urls,
        'manual_series': manuel.get('title'),
        'manual_season': manuel.get('season'),
        'manual_episode': episode,
    }
    try:
        with open(RECEIVED_LINKS_FILE, 'a', encoding='utf-8') as journal:
            journal.write(json.dumps(event, ensure_ascii=False) + '\n')
            journal.flush()
            os.fsync(journal.fileno())
        os.chmod(RECEIVED_LINKS_FILE, 0o600)
    except Exception as error:
        logging.error(f"Empfangenen Link sichern fehlgeschlagen: {error}")

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 Video Bot bereit!\n\n"
        "Schick mir einen Link und wähle den Typ.\n"
        "Oder sende: `URL | Titel`",
        parse_mode='Markdown',
        reply_markup=MAIN_MENU,
    )


def _central_source_url_is_valid(source_url: str | None, page_url: str) -> bool:
    if not source_url or not source_url.startswith(('http://', 'https://')):
        return False
    return source_url.rstrip('/') != page_url.rstrip('/')


async def _extract_central_series(app: Application, chat_id: int, series_url: str) -> None:
    msg = await app.bot.send_message(chat_id, "🔎 Lese Staffeln und Folgen …")
    try:
        series_title, episodes = await get_series_episodes(series_url, False)
        missing = [ep for ep in episodes if not ep.get('skip')]
        if not missing:
            await msg.edit_text(f"✅ {series_title}: Alle Folgen sind bereits vorhanden.")
            return

        seasons: dict[int, list[dict]] = {}
        for episode in missing:
            seasons.setdefault(int(episode.get('season', 0)), []).append(episode)

        released_total = 0
        completed_seasons = 0
        incomplete_seasons = 0
        deferred_total = 0
        for season, season_episodes in sorted(seasons.items()):
            season_episodes.sort(key=lambda ep: int(ep.get('episode', 0)))
            source_key = f"series:{series_url.rstrip('/').lower()}:season:{season}"
            batch_title = f"{series_title} – Staffel {season}"
            released_titles = await asyncio.to_thread(
                _central_queue.released_job_titles, source_key
            )
            season_episodes = [
                episode for episode in season_episodes
                if episode['title'] not in released_titles
            ]
            if not season_episodes:
                completed_seasons += 1
                await msg.edit_text(
                    f"⏭ *{batch_title}* ist bereits in der Remote Queue\n"
                    f"📚 Staffeln geprüft: {completed_seasons}/{len(seasons)}",
                    parse_mode='Markdown',
                )
                continue
            season_total = len(season_episodes)
            batch = await asyncio.to_thread(
                _central_queue.get_or_create_batch,
                source_key, batch_title, series_url, chat_id, season_total,
            )
            batch_id = batch['id']
            stored_titles = await asyncio.to_thread(_central_queue.batch_job_titles, batch_id)
            stored_count = len(stored_titles)

            for index, episode in enumerate(season_episodes, 1):
                title = episode['title']
                if title in stored_titles:
                    logging.info("Central-Batch #%s: bereits extrahiert: %s", batch_id, title)
                    continue
                await msg.edit_text(
                    f"🦊 *{series_title} · Staffel {season}*\n"
                    f"Extrahiere Folge {index}/{season_total}\n`{title}`\n\n"
                    f"💾 Gespeichert: {stored_count}/{season_total}\n"
                    "Diese Staffel wartet bis zur vollständigen Freigabe.",
                    parse_mode='Markdown',
                )
                try:
                    extraction_type = episode.get('content_type') or 'serie'
                    _stream, _title, _referer, _cookies, source_url = await get_stream_url(
                        episode['url'], extraction_type, source_only=True
                    )
                    if not _central_source_url_is_valid(source_url, episode['url']):
                        raise RuntimeError(f"Keine gültige VOE-/Doodstream-URL für {title}")
                    payload = {
                        'type': 'video',
                        'url': source_url,
                        'content_type': 'serie',
                        'manual_title': title,
                        'is_anime': False,
                        'chat_id': chat_id,
                    }
                    _job_id, created = await asyncio.to_thread(
                        _central_queue.enqueue_batch_job, batch_id, payload
                    )
                    if created:
                        stored_count += 1
                        stored_titles.add(title)
                except Exception as episode_error:
                    deferred_total += 1
                    logging.error(
                        "Central-Batch #%s: %s wird später erneut versucht: %s",
                        batch_id, title, episode_error,
                    )
                    await msg.edit_text(
                        f"⏭ {title} vorläufig übersprungen\n"
                        f"Grund: {episode_error}\n\n"
                        "Der Bot macht mit der nächsten Folge weiter."
                    )
                    continue

            missing_after_pass = season_total - stored_count
            if stored_count == 0:
                incomplete_seasons += 1
                logging.warning(
                    "Central-Batch #%s bleibt offen: kein Link von %s gespeichert",
                    batch_id, season_total,
                )
                await msg.edit_text(
                    f"🧩 *{batch_title}* bleibt offen\n"
                    f"⏭ Alle {season_total} Folge(n) werden später erneut versucht.",
                    parse_mode='Markdown',
                )
                continue

            if missing_after_pass > 0:
                incomplete_seasons += 1
                logging.warning(
                    "Central-Batch #%s wird teilweise freigegeben: %s/%s Links gespeichert",
                    batch_id, stored_count, season_total,
                )

            released = await asyncio.to_thread(_central_queue.release_batch, batch_id)
            released_total += released
            completed_seasons += 1
            await msg.edit_text(
                f"{'✅' if missing_after_pass == 0 else '🧩'} *{batch_title}* freigegeben\n"
                f"☁️ {released} Folgen sind für Remote bereit\n"
                f"⏭ {missing_after_pass} Folge(n) werden später nachgereicht\n"
                f"📚 Staffeln abgeschlossen: {completed_seasons}/{len(seasons)}",
                parse_mode='Markdown',
            )

        result_icon = "✅" if incomplete_seasons == 0 else "🧩"
        await msg.edit_text(
            f"{result_icon} *{series_title}*\n"
            f"☁️ {completed_seasons} Staffel-Batches freigegeben · {released_total} Folgen\n"
            f"🧩 {incomplete_seasons} Staffel-Batches noch offen\n"
            f"⏭ {deferred_total} Folge(n) auf später verschoben",
            parse_mode='Markdown',
        )
    except Exception as error:
        logging.error("Zentrale Serien-Extraktion fehlgeschlagen:\n%s", traceback.format_exc())
        await msg.edit_text(
            f"❌ Extraktion abgebrochen: {error}\n\n"
            "Bereits extrahierte Links bleiben im PostgreSQL-Entwurf gespeichert. "
            "Starte dieselbe Serie später erneut; der Bot setzt automatisch fort."
        )


async def _extract_central_single(
    app: Application, chat_id: int, page_url: str, selected_type: str
) -> None:
    msg = await app.bot.send_message(chat_id, "🦊 Extrahiere Player-Link …")
    try:
        is_fp = _is_filmpalast_film(page_url) or _is_filmpalast_series(page_url)
        extraction_type = 'filmpalast' if is_fp else selected_type
        _stream, detected_title, _referer, _cookies, source_url = await get_stream_url(
            page_url, extraction_type, source_only=True
        )
        if not _central_source_url_is_valid(source_url, page_url):
            raise RuntimeError("Keine gültige VOE-/Doodstream-URL gefunden")

        db_type = 'serie' if selected_type == 'serie' else 'film'
        title = detected_title
        if not title:
            slug = page_url.rstrip('/').split('/')[-1]
            title = slug.removesuffix('.html').replace('-', ' ').strip().title()
        title = format_filename(title)
        payload = {
            'type': 'video',
            'url': source_url,
            'content_type': db_type,
            'manual_title': title,
            'is_anime': False,
            'chat_id': chat_id,
        }
        result = await asyncio.to_thread(_central_queue.enqueue_many, [payload])
        await msg.edit_text(
            f"✅ In zentrale Queue gestellt\n`{title}`\n"
            f"☁️ Neu: {result['created']} · Bereits vorhanden: {result['existing']}",
            parse_mode='Markdown',
        )
    except Exception as error:
        logging.error("Zentrale Einzel-Extraktion fehlgeschlagen:\n%s", traceback.format_exc())
        await msg.edit_text(f"❌ Player-Extraktion fehlgeschlagen: {error}")


async def _start_central_url(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str) -> None:
    chat_id = update.effective_chat.id
    if not _central_queue:
        await update.message.reply_text("❌ `DATABASE_URL` fehlt.", parse_mode='Markdown')
        return

    url = _normalize_seriesstream_url(url.strip())
    if not _is_seriesstream_url(url):
        await update.message.reply_text(
            "❌ Diese Funktion ist ausschließlich für SerienStream-Links.\n"
            "Filmpalast, Megakino und AniWorld laufen weiterhin normal über Chromium."
        )
        return

    if '/episode-' in url:
        _spawn_task(_extract_central_single(context.application, chat_id, url, 'serie'), f'central-single-{chat_id}')
    else:
        _spawn_task(_extract_central_series(context.application, chat_id, url), f'central-series-{chat_id}')
    await update.message.reply_text(
        "🦊 Camoufox extrahiert nur die VOE-Links und legt sie in die Remote Queue."
    )


async def cmd_remote_queue_download(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Startet den Chromium-Downloader für die zentrale Queue genau einmal."""
    global _central_worker_task
    if not _central_queue:
        await update.message.reply_text("❌ `DATABASE_URL` fehlt.", parse_mode='Markdown')
        return
    if _central_worker_task and not _central_worker_task.done():
        await update.message.reply_text("▶️ Remote Queue Download läuft bereits.")
        return

    jobs = await asyncio.to_thread(_central_queue.open_jobs)
    _central_worker_task = _spawn_task(
        _central_queue_worker(context.application),
        "central-queue-worker",
    )
    await update.message.reply_text(
        f"▶️ Remote Queue Download gestartet.\n"
        f"☁️ {len(jobs)} offene Jobs gefunden.\n"
        "🌐 Download und Upload werden mit Playwright Chromium verarbeitet."
    )


async def cmd_clear_remote_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _central_queue:
        await update.message.reply_text("❌ `DATABASE_URL` fehlt.", parse_mode='Markdown')
        return
    await update.message.reply_text(
        "⚠️ Wirklich alle wartenden Jobs aus der Remote Queue entfernen?\n"
        "Ein bereits aktiver Download und laufende Camoufox-Extraktionen werden nicht unterbrochen.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "🗑 Ja, wartende Jobs löschen",
                callback_data="clear_remote_queue_confirm",
            ),
            InlineKeyboardButton("Abbrechen", callback_data="clear_remote_queue_cancel"),
        ]]),
    )


async def handle_clear_remote_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    async def edit_once(text: str) -> None:
        try:
            await query.edit_message_text(text)
        except TgBadRequest as error:
            # Telegram kann denselben Callback bei einem Doppelklick zweimal
            # zustellen. Die Queue-Aktion ist idempotent; gleicher Text ist okay.
            if "Message is not modified" not in str(error):
                raise

    if query.data == "clear_remote_queue_cancel":
        await edit_once("Remote Queue wurde nicht verändert.")
        return
    if not _central_queue:
        await edit_once("❌ DATABASE_URL fehlt.")
        return
    result = await asyncio.to_thread(_central_queue.clear_waiting_jobs)
    suffix = []
    if result['processing']:
        suffix.append(f"▶️ {result['processing']} bereits aktive Jobs laufen weiter")
    if result['collecting']:
        suffix.append(f"🦊 {result['collecting']} laufende Serien-Extraktionen bleiben aktiv")
    details = "\n" + "\n".join(suffix) if suffix else ""
    await edit_once(
        f"🗑 {result['cancelled']} wartende Remote-Jobs entfernt.{details}"
    )


async def handle_central_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    url = context.user_data.pop('central_pending_url', None)
    if not url:
        await query.edit_message_text("❌ URL-Auswahl ist abgelaufen. Bitte erneut starten.")
        return
    selected_type = 'serie' if query.data == 'central_type_serie' else 'film'
    await query.edit_message_text(f"☁️ {selected_type.title()} wird für Remote Queue vorbereitet …")
    if selected_type == 'serie':
        _spawn_task(_extract_central_series(context.application, query.message.chat_id, url), f'central-series-{query.message.chat_id}')
    else:
        _spawn_task(_extract_central_single(context.application, query.message.chat_id, url, 'film'), f'central-film-{query.message.chat_id}')


async def cmd_centralserie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Extrahiert alle VOE-Links und veröffentlicht sie danach atomar."""
    if not _central_queue:
        await update.message.reply_text(
            "❌ Dieser Befehl benötigt eine gültige `DATABASE_URL`."
        )
        return
    if not context.args:
        await update.message.reply_text(
            "Verwendung: `/centralserie https://serienstream.to/serie/...`",
            parse_mode='Markdown',
        )
        return
    series_url = _normalize_seriesstream_url(context.args[0].strip())
    if not _is_seriesstream_url(series_url):
        await update.message.reply_text("❌ Bitte eine SerienStream-URL angeben.")
        return
    _spawn_task(
        _extract_central_series(context.application, update.effective_chat.id, series_url),
        f"central-series-{update.effective_chat.id}",
    )
    await update.message.reply_text("☁️ Zentrale VOE-Sammlung wurde gestartet.")


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _journal_received_link(update, context)
    # Manuel-Modus hat Vorrang
    if await handle_manuel_input(update, context):
        return

    text = update.message.text.strip()
    if text == "▶️ Remote Queue Download":
        await cmd_remote_queue_download(update, context)
        return
    if text == "🗑 Remote Queue leeren":
        await cmd_clear_remote_queue(update, context)
        return
    if text == "▶️ Start":
        await cmd_start(update, context)
        return
    if text == "📋 Queue":
        await cmd_queue(update, context)
        return
    if text == "📊 Status":
        await cmd_status(update, context)
        return
    if text == "🔍 Watchcheck":
        await cmd_watchcheck(update, context)
        return
    if text == "🆕 Megakino Last Added":
        await cmd_megakino_last_added(update, context)
        return
    if text == "🆕 HDFilme Latest":
        await cmd_hdfilme_latest(update, context)
        return
    if text == "🆕 Filmpalast Import":
        await cmd_filmpalast_import(update, context)
        return
    if text == "✍️ Manuel":
        await cmd_manuel(update, context)
        return

    if not text.startswith('http'):
        return

    manual_title = None
    if ' | ' in text:
        parts = text.split(' | ', 1)
        text = parts[0].strip()
        manual_title = parts[1].strip()

    context.user_data['pending_url'] = text
    context.user_data['manual_title'] = manual_title

    if _is_megakino_listing(text):
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📦 Alle Filme laden", callback_data="type_bulk_films"),
        ]])
        await update.message.reply_text(
            "📋 Listing-Seite erkannt. Alle Filme herunterladen?",
            reply_markup=keyboard
        )
        return

    if _is_filmpalast_series(text):
        chat_id = update.effective_chat.id
        msg = await update.message.reply_text("📺 Filmpalast Serie erkannt – lese Staffel & Folgen...")
        asyncio.create_task(_enqueue(chat_id, msg, process_full_series,
            {'type': 'series', 'url': text, 'is_anime': False, 'chat_id': chat_id},
            text, False))
        return

    if _is_filmpalast_film(text):
        chat_id = update.effective_chat.id
        msg = await update.message.reply_text("🎬 Filmpalast erkannt – lade Film...")
        asyncio.create_task(_enqueue(chat_id, msg, process_video,
            {'type': 'video', 'url': text, 'content_type': 'filmpalast', 'manual_title': manual_title, 'is_anime': False},
            text, 'filmpalast', manual_title))
        return

    if _is_kinoger_url(text):
        chat_id = update.effective_chat.id
        msg = await update.message.reply_text("🎬 Kinoger erkannt – lese Seite...")
        try:
            data = await asyncio.get_event_loop().run_in_executor(None, _kinoger_scrape, text)
        except Exception as e:
            await msg.edit_text(f"❌ Kinoger-Scrape Fehler: {e}")
            return

        if data['type'] == 'film':
            if not data.get('embed_url'):
                await msg.edit_text("❌ Kein Embed gefunden")
                return
            title = data['title']
            year = data.get('year')
            ft = manual_title or (f"{title} ({year})" if year else title)
            embed_url = data['embed_url']
            await msg.edit_text(f"🎬 *{ft}* → Embed gefunden\n`{embed_url[:80]}...`", parse_mode='Markdown')
            await _enqueue(chat_id, msg, process_kinoger_direct,
                {'type': 'kinoger_direct', 'url': embed_url, 'manual_title': ft, 'db_content_type': 'film'},
                embed_url, ft, 'film', False)
            return

        # Serie → Staffel-Wahl im /manuel-Stil aber mit vorbefüllten URLs
        title = data['title']
        seasons_urls = data['seasons']  # {1: [url, url, ...], ...}

        # DB-Check: pro Staffel fehlende Folgen
        missing_per_season = {}
        try:
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            for s, urls in seasons_urls.items():
                ep_count = len(urls)
                existing = set()
                for row in cur.execute(
                    "SELECT episode FROM episodes WHERE LOWER(series)=LOWER(?) AND season=?",
                    (title, s)):
                    existing.add(row[0])
                missing_per_season[s] = [e for e in range(1, ep_count + 1) if e not in existing]
            con.close()
        except Exception as e:
            logging.warning(f"DB-Check Fehler in Kinoger: {e}")
            missing_per_season = {s: list(range(1, len(u) + 1)) for s, u in seasons_urls.items()}

        def _fmt_missing(missing: list[int], total: int) -> str:
            if not missing: return "✅ komplett"
            if len(missing) == total: return "❌ keine vorhanden"
            return f"⚠️ fehlt: {', '.join(str(e) for e in missing)}"

        season_list = '\n'.join(
            f"  Staffel {s}: {len(urls)} Folgen – {_fmt_missing(missing_per_season[s], len(urls))}"
            for s, urls in seasons_urls.items()
        )

        context.user_data['kinoger'] = {
            'title': title,
            'seasons_urls': seasons_urls,
            'missing_per_season': missing_per_season,
            'state': 'waiting_season',
        }

        await msg.edit_text(
            f"📺 *{title}*\n\n{season_list}\n\n"
            f"Mit welcher Staffel wollen wir anfangen?\n"
            f"_Tipp: Nur Staffel-Nr (z.B. `2`) → nur fehlende Folgen.\n"
            f"Mit Spec wie `2 3,5-8` → genau diese Folgen.\n"
            f"`2 all` → alle Folgen erzwingen._",
            parse_mode='Markdown'
        )
        return

    if _is_seriesstream_url(text):
        text = _normalize_seriesstream_url(text)
        # SerienStream wird nie mehr lokal verarbeitet: Camoufox löst nur den
        # VOE-Link auf, der eigentliche Download erfolgt später aus PostgreSQL
        # über den manuell gestarteten Chromium-Worker.
        await _start_central_url(update, context, text)
        return

    if 'aniworld.to' in text:
        chat_id = update.effective_chat.id
        # Einzelne Episode (URL enthält /episode-) → nur diese Folge laden
        if '/episode-' in text:
            msg = await update.message.reply_text("🎌 AniWorld Episode erkannt – lade Folge...")
            asyncio.create_task(_enqueue(chat_id, msg, process_video,
                {'type': 'video', 'url': text, 'content_type': 'anime', 'manual_title': manual_title, 'is_anime': True},
                text, 'anime', manual_title, True))
            return
        # Serie / Staffel → alle Folgen laden
        msg = await update.message.reply_text("🎌 AniWorld erkannt – lese Staffeln & Folgen...")
        asyncio.create_task(_enqueue(chat_id, msg, process_full_series,
            {'type': 'series', 'url': text, 'is_anime': True, 'chat_id': chat_id},
            text, True))
        return

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎬 Film", callback_data="type_film"),
            InlineKeyboardButton("📺 Serie", callback_data="type_serie"),
            InlineKeyboardButton("🎌 Anime", callback_data="type_anime"),
        ]
    ])

    await update.message.reply_text("Was ist es?", reply_markup=keyboard)


async def process_video(msg, url: str, content_type: str, manual_title: str | None, is_anime: bool = False, progress_prefix: str = "", auto_skip_existing: bool = False):
    """Verarbeitung als Background-Task"""
    type_emoji = {"film": "🎬", "serie": "📺", "anime": "🎌", "filmpalast": "🎬"}

    # filmpalast verhält sich wie ein normaler Film (DB, Qualität, etc.)
    is_filmpalast = content_type == 'filmpalast'
    # filmpalast Serien-Episode: URL enthält -sXXeXX → als Serie speichern
    _is_fp_serie = is_filmpalast and bool(re.search(r'-s\d{2}e\d{2}', url, re.IGNORECASE))

    filename = None
    keep_downloaded_file = False
    try:
        # Vorläufiger Titel (für Serie/Anime oder wenn manuell gesetzt)
        if manual_title:
            title = manual_title
        elif content_type in ('film', 'filmpalast') and not _is_fp_serie:
            title = "..."  # wird von Playwright überschrieben
        elif _is_fp_serie and not manual_title:
            # Serientitel aus URL ableiten bis Playwright ihn liefert
            _slug = url.rstrip('/').split('/')[-1]
            _slug = re.sub(r'-s\d{2}e\d{2}.*', '', _slug, flags=re.IGNORECASE)
            title = _slug.replace('-', ' ').title()
        else:
            title = scrape_title(url)

        filename = format_filename(title) if title != "..." else "..."

        await msg.edit_text(f"{type_emoji[content_type]} Öffne Player...", parse_mode='Markdown')

        # Early-DB-Check Callback: prüft nach Titel-Extraktion OB Film in DB ist,
        # spart VOE-Abruf + m3u8-Auflösung wenn Film schon vorhanden
        def _early_check(extracted_title: str) -> bool:
            if manual_title:
                return False
            # Bei Bulk: Early-Skip aktiv (Speed). Bei Einzelupload: kein Early-Skip,
            # damit VOE extrahiert wird für Qualitäts-Vergleich (Re-Upload-Detection).
            if not auto_skip_existing:
                return False
            fn = format_filename(extracted_title)
            db_ct = 'serie' if _is_fp_serie else ('film' if is_filmpalast else content_type)
            return _db_exists(db_ct, fn, is_anime)

        # Stream abfangen (Playwright macht alles: Seite öffnen, iframe extrahieren, Play klicken)
        # Gibt (stream_url, film_title, referer, cookie_str, source_voe_url) zurück
        stream_url, playwright_title, referer, cookie_str, source_voe_url = await get_stream_url(url, content_type, early_db_check=_early_check)

        # Early-Skip-Signal: war bereits in DB, kein VOE extrahiert → einfach skippen
        if cookie_str == "__DB_SKIP__" and playwright_title:
            filename = format_filename(playwright_title)
            logging.info(f"[SKIP-EARLY] Bereits vorhanden: {filename}")
            await msg.edit_text(f"⏭ Bereits vorhanden: `{filename}`", parse_mode='Markdown')
            return True

        # Film-Titel von Playwright übernehmen
        if not manual_title and playwright_title:
            title = playwright_title
            filename = format_filename(title)
            logging.info(f"[{content_type}] Titel von Playwright: {title} → Datei: {filename}")

        # DB-Check mit VOE-Vergleich: gleiche Quelle = skip, andere Quelle = bessere Quali → update
        update_due_to_new_source = False
        if filename != "...":
            db_ct = 'serie' if _is_fp_serie else ('film' if is_filmpalast else content_type)
            exists = _db_exists(db_ct, filename, is_anime)
            logging.info(f"[DB-CHECK] {db_ct}: '{filename}' → exists={exists}")
            if exists:
                stored_voe = _db_get_voe(db_ct, filename, is_anime)
                logging.info(f"[VOE-CHECK] stored='{stored_voe}' new='{source_voe_url}'")
                if stored_voe and source_voe_url and stored_voe == source_voe_url:
                    logging.info(f"[SKIP] Gleiche Quelle: {filename}")
                    await msg.edit_text(f"⏭ Bereits vorhanden (gleiche Quelle): `{filename}`", parse_mode='Markdown')
                    return True
                elif not stored_voe:
                    # Alter Eintrag ohne voe_url → konservativ: skippen wie bisher
                    logging.info(f"[SKIP] Bereits vorhanden (kein VOE in DB): {filename}")
                    await msg.edit_text(f"⏭ Bereits vorhanden: `{filename}`", parse_mode='Markdown')
                    return True
                else:
                    # Quelle hat sich geändert → wahrscheinlich bessere Qualität, neu hochladen
                    update_due_to_new_source = True
                    logging.info(f"[UPDATE] Neue Quelle für '{filename}' – upload überschreibt")
                    await msg.edit_text(f"♻️ Update (neue Quelle): `{filename}`", parse_mode='Markdown')

        # resolve_normal_quality wird NUR für yt-dlp/ffmpeg-Fallback gebraucht.
        # download_with_browser parst die Master-Playlist selbst im Browser-Context.
        # Daher hier nur noch Titel-Fallback:
        if title == "...":
            title = scrape_title(url)
            filename = format_filename(title)

        # Anime-Suffix anhängen falls nötig
        if is_anime and 'anime' not in title.lower():
            title = title + ' anime'
            filename = format_filename(title)

        logging.info(f"[{content_type}] Datei: {filename}")

        # Für Filme: nochmal prüfen nachdem Playwright den echten Titel hat
        overwrite = False
        if content_type in ('film', 'filmpalast') and not _is_fp_serie and _db_exists('film', filename, is_anime):
            overwrite = True

        filepath = os.path.join(DOWNLOAD_DIR, f"{filename}.mp4")

        # Datei schon vorhanden? (z.B. Upload nach Neustart abgebrochen)
        file_ready = os.path.exists(filepath) and os.path.getsize(filepath) > 10 * 1024 * 1024

        if file_ready:
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            await msg.edit_text(
                f"♻️ Datei bereits vorhanden ({size_mb:.1f} MB)\nÜberspringe Download – starte Upload...",
                parse_mode='Markdown'
            )
        else:
            if not stream_url:
                await msg.edit_text("❌ Stream nicht gefunden!")
                return False

            await msg.edit_text(f"📥 Lade herunter...\n`{filename}`", parse_mode='Markdown')

            # Progress-Monitor: alle 5s Dateigröße in Telegram updaten
            dl_start = _time.monotonic()
            stop_progress = asyncio.Event()
            _dl_status.update({'filename': filename, 'start': dl_start, 'size_mb': 0, 'phase': 'download'})

            async def progress_monitor():
                while not stop_progress.is_set():
                    await asyncio.sleep(5)
                    if stop_progress.is_set():
                        break
                    try:
                        # yt-dlp schreibt zuerst in .part-Datei
                        part_file = filepath + '.part'
                        check = part_file if os.path.exists(part_file) else filepath
                        size_mb = os.path.getsize(check) / (1024 * 1024) if os.path.exists(check) else 0
                        elapsed = int(_time.monotonic() - dl_start)
                        m, s = divmod(elapsed, 60)
                        _dl_status['size_mb'] = size_mb
                        _dl_status['elapsed'] = elapsed
                        await msg.edit_text(
                            f"📥 Lade herunter... ⏱ {m:02d}:{s:02d}\n`{filename}`\n💾 {size_mb:.0f} MB",
                            parse_mode='Markdown'
                        )
                    except:
                        pass

            progress_task = asyncio.create_task(progress_monitor())
            # Strategie: HLS via Browser (umgeht TLS-Fingerprint-Blocking)
            success = False
            if '.m3u8' in stream_url:
                success = await download_with_browser(stream_url, filepath, referer, cookie_str)
                if not success:
                    logging.warning("Browser-Download fehlgeschlagen, Fallback auf yt-dlp...")
                    # Für yt-dlp/ffmpeg: Variant explizit auflösen (mittlere Qualität)
                    stream_url = resolve_normal_quality(stream_url, referer, cookie_str)
            if not success:
                success = await download_with_ytdlp(stream_url, filepath, referer, cookie_str)
            if not success:
                logging.warning("yt-dlp fehlgeschlagen, Fallback auf ffmpeg...")
                success = await download_with_ffmpeg(stream_url, filepath, referer, cookie_str)
            stop_progress.set()
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass

            if not success or not os.path.exists(filepath):
                await msg.edit_text("❌ Download fehlgeschlagen!")
                return False

        filepath = await downscale_if_needed(filepath, msg)
        # Ab hier liegt eine fertige Datei vor. Bei Telegram-/Netzwerkfehlern
        # behalten, damit der nächste Queue-Versuch nicht erneut herunterladen muss.
        keep_downloaded_file = True

        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        _dl_status.update({'phase': 'upload', 'size_mb': size_mb, 'elapsed': int(_time.monotonic() - _dl_status.get('start', _time.monotonic()))})
        await msg.edit_text(f"📤 Starte Upload... ({size_mb:.1f} MB)")

        _db_ct = 'serie' if _is_fp_serie else ('film' if is_filmpalast else content_type)
        caption = build_caption(filename, 'serie' if _is_fp_serie else content_type)
        await upload_with_telethon(filepath, caption, msg, progress_prefix=progress_prefix)
        _db_add(_db_ct, filename, is_anime, overwrite=(overwrite or update_due_to_new_source), voe_url=source_voe_url)
        keep_downloaded_file = False
        await msg.edit_text(f"✅ Fertig!\n`{filename}.mp4`", parse_mode='Markdown')
        return True

    except PermanentMediaUnavailable as e:
        logging.warning(f"Permanenter Medienfehler: {url} – {e}")
        try:
            await msg.edit_text(f"🗑 Dauerhaft nicht verfügbar: {e}\nDer Auftrag wird nicht erneut versucht.")
        except Exception:
            pass
        return PERMANENT_JOB_FAILURE
    except Exception as e:
        full_tb = traceback.format_exc()
        logging.error(f"process_video Fehler:\n{full_tb}")
        # Kein Markdown: Tracebacks enthalten häufig Zeichen, die Telegrams
        # Entity-Parser als unvollständige Formatierung interpretiert.
        error_text = f"❌ Fehler: {e}\n\n🔍 Debug:\n{full_tb[-2500:]}"
        try:
            await msg.edit_text(error_text)
        except Exception as notify_error:
            logging.warning(f"Telegram-Fehlermeldung konnte nicht gesendet werden: {notify_error}")
        return False
    finally:
        fp = os.path.join(DOWNLOAD_DIR, f"{filename}.mp4") if filename else None
        if fp and os.path.exists(fp) and not keep_downloaded_file:
            os.remove(fp)


async def process_full_series(msg, series_url: str, is_anime: bool = False, start_from: int = 0):
    """Alle Staffeln/Folgen einer Serie nacheinander herunterladen und hochladen."""
    try:
        await msg.edit_text("📺 Lese Staffeln und Folgen...")
        series_title, episodes = await get_series_episodes(series_url, is_anime)

        if not episodes:
            await msg.edit_text("❌ Keine Folgen gefunden!")
            return False

        total = len(episodes)
        _watchlist_update('series', series_url, is_anime, last_seen_total=total, last_series_title=series_title)
        logging.info(f"Serie '{series_title}': {total} Folgen gesamt")
        start_hint = f" (ab Folge {start_from + 1})" if start_from > 0 else ""
        await msg.edit_text(
            f"📺 *{series_title}*\n"
            f"✅ {total} Folgen gefunden. Starte...{start_hint}",
            parse_mode='Markdown'
        )
        await asyncio.sleep(2)

        # Staffeln zählen für Anzeige
        all_seasons = sorted(set(ep['season'] for ep in episodes))
        total_seasons = len(all_seasons)
        completed_all = True

        for i, ep in enumerate(episodes, 1):
            if i <= start_from:
                logging.info(f"Überspringe Folge {i} (bereits erledigt)")
                continue

            # ⏭ Schneller Skip wenn bereits in DB
            if ep.get('skip', False):
                logging.info(f"[{i}/{total}] ⏭ {ep['title']} – bereits vorhanden")
                season_num = ep.get('season', '?')
                ep_num = ep.get('episode', i)
                season_eps = [e for e in episodes if e.get('season') == season_num]
                ep_in_season = next((j+1 for j, e in enumerate(season_eps) if e == ep), ep_num)
                await msg.edit_text(
                    f"📺 *{series_title}*\n"
                    f"Staffel {season_num}/{total_seasons} · Folge {ep_in_season}/{len(season_eps)}\n"
                    f"⏭ `{ep['title']}` (bereits vorhanden)",
                    parse_mode='Markdown'
                )
                await asyncio.sleep(0.5)
                continue

            logging.info(f"[{i}/{total}] Starte: {ep['title']}")
            season_num = ep.get('season', '?')
            ep_num = ep.get('episode', i)
            season_eps = [e for e in episodes if e.get('season') == season_num]
            ep_in_season = next((j+1 for j, e in enumerate(season_eps) if e == ep), ep_num)
            await msg.edit_text(
                f"📺 *{series_title}*\n"
                f"Staffel {season_num}/{total_seasons} · Folge {ep_in_season}/{len(season_eps)}\n"
                f"`{ep['title']}`",
                parse_mode='Markdown'
            )
            await asyncio.sleep(1)
            ep_title = ep['title'] + (' anime' if is_anime else '')
            prefix = f"📺 S{season_num:02d}E{ep_num:02d} · Folge {ep_in_season}/{len(season_eps)} (Staffel {season_num}/{total_seasons})\n"
            ct = ep.get('content_type') or ('anime' if is_anime else 'serie')
            success = await process_video(msg, ep['url'], ct, ep_title, is_anime=is_anime, progress_prefix=prefix)
            if success is True:
                _series_progress[series_url] = i
                _persist_progress()
            else:
                completed_all = False
                logging.warning(f"Folge {i} fehlgeschlagen – Fortschritt NICHT gespeichert, wird beim nächsten Mal wiederholt")
                break
            await asyncio.sleep(15)

        if completed_all:
            _series_progress.pop(series_url, None)
            _persist_progress()
            _watchlist_update('series', series_url, is_anime, last_seen_total=total, last_series_title=series_title)
            await msg.edit_text(
                f"✅ *{series_title}*\nAlle {total} Folgen fertig!",
                parse_mode='Markdown'
            )
            return True
        await msg.edit_text(
            f"⚠️ *{series_title}*\nMindestens eine Folge ist fehlgeschlagen. Auftrag bleibt gespeichert.",
            parse_mode='Markdown'
        )
        return False

    except Exception as e:
        tb = traceback.format_exc()[-1500:]
        logging.error(f"process_full_series Fehler:\n{traceback.format_exc()}")
        await msg.edit_text(f"❌ Serien-Fehler: {str(e)}\n\n🔍 Debug:\n<pre>{tb}</pre>", parse_mode='HTML')
        return False


# ─── Bulk-Film-Upload (Listing-Seite) ─────────────

async def get_bulk_films_from_page(listing_url: str) -> list[dict]:
    """Scrapet alle Filmlinks von einer megakino1.club Listing-Seite."""
    print(f"[BULK] Playwright startet für: {listing_url}", flush=True)
    logging.info(f"Bulk-Filme: Lade Seite {listing_url}")
    async with _open_browser(camoufox=False) as browser:
        context = await browser.new_context()
        page = await context.new_page()
        print("[BULK] Browser geöffnet, lade Seite...", flush=True)
        await page.goto(listing_url, timeout=60000, wait_until='domcontentloaded')
        print("[BULK] domcontentloaded erreicht, warte auf Film-Kacheln...", flush=True)
        # Mehrere Selektor-Varianten (je nach Domain unterschiedlich)
        _sel = 'a.poster.grid-item, a.poster, .shortstory a.th-in, article a.th-in, .short a[href*="/films/"], .short a[href*="/kinofilme/"]'
        try:
            await page.wait_for_selector(_sel, timeout=15000)
            print("[BULK] Film-Kacheln gefunden!", flush=True)
        except Exception:
            print("[BULK] Timeout beim Warten auf Kacheln – fahre trotzdem fort", flush=True)

        # Megakino/DLE lädt Poster lazy per IntersectionObserver – ohne Scrollen bleiben
        # die unteren Kacheln als Platzhalter im DOM stehen. Einmal bis zum Seitenende
        # scrollen, dann kurz auf networkidle warten, damit alle Kacheln rendern.
        try:
            prev_count = 0
            for _ in range(8):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(0.7)
                curr = await page.evaluate(f"document.querySelectorAll({_sel!r}).length")
                if curr == prev_count and curr > 0:
                    break
                prev_count = curr
            try:
                await page.wait_for_load_state('networkidle', timeout=4000)
            except Exception:
                pass
            print(f"[BULK] Nach Scroll: {prev_count} Kacheln im DOM", flush=True)
        except Exception as e:
            print(f"[BULK] Scroll-Warmup fehlgeschlagen: {e}", flush=True)

        content = await page.content()
        await browser.close()
        print("[BULK] Browser geschlossen", flush=True)

    from urllib.parse import urlparse as _urlparse
    _p = _urlparse(listing_url)
    base = f"{_p.scheme}://{_p.netloc}"
    soup = BeautifulSoup(content, 'html.parser')
    urls: list[str] = []
    seen: set[str] = set()
    # 1) Klassische Kacheln (megakino1/2)
    anchors = list(soup.select('a.poster.grid-item'))
    # 2) Fallback-Selektoren (megakino3 & ähnliche DLE-basierte Seiten)
    if not anchors:
        anchors = list(soup.select('a.poster, article a.th-in, .short a.th-in, .shortstory a.th-in'))
    # 3) Letzter Fallback: alle Links auf Filmseiten (/films/..., /kinofilme/...) die auf .html enden
    if not anchors:
        anchors = [a for a in soup.select('a[href$=".html"]')
                   if any(p in (a.get('href') or '') for p in ('/films/', '/kinofilme/'))]

    for a in anchors:
        href = a.get('href', '') or ''
        if not href or '/page/' in href:
            continue
        full = href if href.startswith('http') else base + href
        if full in seen:
            continue
        seen.add(full)
        urls.append(full)

    logging.info(f"Bulk-Filme: {len(urls)} Filme gefunden auf {listing_url}")
    return urls


async def process_bulk_films(msg, page_url: str, start_from: int = 0):
    """Alle Filme einer Listing-Seite nacheinander herunterladen und hochladen."""
    print(f"[BULK] process_bulk_films gestartet: {page_url}", flush=True)
    try:
        try:
            await msg.edit_text("🎬 Lade Filmliste...")
        except TgBadRequest:
            pass
        films = await get_bulk_films_from_page(page_url)

        if not films:
            await msg.edit_text("❌ Keine Filme gefunden!")
            return False

        total = len(films)
        start_hint = f" (ab Film {start_from + 1})" if start_from > 0 else ""
        await msg.edit_text(
            f"🎬 *{total} Filme gefunden. Starte...{start_hint}*",
            parse_mode='Markdown'
        )
        await asyncio.sleep(2)

        completed_all = True
        for i, url in enumerate(films, 1):
            if i <= start_from:
                logging.info(f"Überspringe Film {i} (bereits erledigt)")
                continue
            logging.info(f"[{i}/{total}] Starte: {url}")
            _dl_status['bulk_current'] = i
            _dl_status['bulk_total'] = total
            try:
                await msg.edit_text(f"🎬 Film {i}/{total}\n`{url}`", parse_mode='Markdown')
            except TgBadRequest:
                pass
            await asyncio.sleep(1)
            prefix = f"🎬 Film {i}/{total}\n"
            success = await process_video(msg, url, 'film', None, is_anime=False, progress_prefix=prefix, auto_skip_existing=True)
            if success is True:
                _series_progress[page_url] = i
                _persist_progress()
            else:
                completed_all = False
                logging.warning(f"Film {i} fehlgeschlagen – wird beim nächsten Mal wiederholt")
                break
            await asyncio.sleep(5)

        if completed_all:
            _series_progress.pop(page_url, None)
            _persist_progress()
            await msg.edit_text(f"✅ Alle {total} Filme fertig!", parse_mode='Markdown')
            return True
        await msg.edit_text("⚠️ Mindestens ein Film ist fehlgeschlagen. Auftrag bleibt gespeichert.")
        return False

    except TgBadRequest as e:
        if 'not modified' in str(e).lower():
            return True  # Kein echter Fehler – identischer Text
        else:
            logging.error(f"process_bulk_films TgBadRequest:\n{traceback.format_exc()}")
            return False
    except Exception as e:
        tb = traceback.format_exc()[-1500:]
        logging.error(f"process_bulk_films Fehler:\n{traceback.format_exc()}")
        try:
            await msg.edit_text(f"❌ Bulk-Fehler: {str(e)}\n\n🔍 Debug:\n```\n{tb}\n```", parse_mode='Markdown')
        except Exception:
            pass
        return False


def _parse_megakino_last_added_html(html: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, 'html.parser')
    section = None
    for sec in soup.select('section.homeList'):
        title = sec.select_one('h2.sect__title')
        if title and 'zuletzt hinzugefügt' in title.get_text(' ', strip=True).lower():
            section = sec
            break

    if not section:
        return []

    items: list[dict] = []
    for block in section.select('.homeList-wrap .listItem'):
        a = block.select_one('.listItem-title a[href]')
        if not a:
            continue
        href = a.get('href', '').strip()
        if not href:
            continue
        full_url = href if href.startswith('http') else base_url.rstrip('/') + href
        title = a.get_text(' ', strip=True)
        episode_text = ''
        ep_el = block.select_one('.listItem-episode')
        if ep_el:
            episode_text = ep_el.get_text(' ', strip=True)
        item_type = 'series' if '/serials/' in href else 'film'
        items.append({
            'type': item_type,
            'url': full_url,
            'title': title,
            'episode_text': episode_text,
        })
    return items


async def get_megakino_last_added(home_url: str = MEGAKINO_HOME_URL) -> list[dict]:
    logging.info(f"Megakino Last Added: Lade Startseite {home_url}")
    async with _open_browser(camoufox=False) as browser:
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(home_url, timeout=60000, wait_until='domcontentloaded')
        await asyncio.sleep(3)
        try:
            await page.wait_for_selector('section.homeList, .homeList-wrap, .listItem', timeout=15000)
        except Exception:
            pass
        html = await page.content()
        await browser.close()
    from urllib.parse import urlparse as _urlparse
    _p = _urlparse(home_url)
    base = f"{_p.scheme}://{_p.netloc}"
    items = _parse_megakino_last_added_html(html, base)
    logging.info(f"Megakino Last Added: {len(items)} Einträge gefunden")
    return items


def _parse_hdfilme_latest_html(html: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, 'html.parser')
    items: list[dict] = []
    seen: set[tuple[str, str]] = set()

    for section in soup.select('aside.sidebar section.sidebar-section, aside section'):
        heading = section.select_one('h3')
        heading_text = heading.get_text(' ', strip=True).lower() if heading else ''
        if 'neueste filme' in heading_text:
            item_type = 'film'
        elif 'neueste serie' in heading_text or 'neueste serien' in heading_text:
            item_type = 'series'
        else:
            continue

        for a in section.select('.listing a[href]'):
            href = a.get('href', '').strip()
            if not href:
                continue
            full_url = href if href.startswith('http') else base_url.rstrip('/') + href
            title = a.get('title') or ''
            if not title:
                title_el = a.select_one('.movie-title')
                title = title_el.get_text(' ', strip=True) if title_el else full_url
            key = (item_type, full_url)
            if key in seen:
                continue
            seen.add(key)
            items.append({'type': item_type, 'url': full_url, 'title': title.strip()})

    return items


async def get_hdfilme_latest(home_url: str = "https://hdfilme.win/") -> list[dict]:
    logging.info(f"HDFilme Latest: Lade Startseite {home_url}")
    headers = {'User-Agent': USER_AGENT, 'Referer': 'https://hdfilme.win/'}
    response = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: requests.get(home_url, headers=headers, timeout=25)
    )
    response.raise_for_status()
    from urllib.parse import urlparse as _urlparse
    _p = _urlparse(home_url)
    base = f"{_p.scheme}://{_p.netloc}"
    items = _parse_hdfilme_latest_html(response.text, base)
    logging.info(f"HDFilme Latest: {len(items)} Einträge gefunden")
    return items


def _normalize_filmpalast_title(title: str) -> str:
    title = re.sub(r'\s+', ' ', (title or '').strip())
    m = re.search(r'^(.*?)\s+S(\d{1,2})E(\d{1,2})$', title, re.IGNORECASE)
    if not m:
        return title
    return f"{m.group(1).strip()} S{int(m.group(2)):02d}E{int(m.group(3)):02d}"


def _parse_filmpalast_latest_html(html: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, 'html.parser')
    items: list[dict] = []
    seen: set[str] = set()

    for article in soup.select('article.liste.rb.pHome, article.liste.pHome, article.pHome'):
        link = article.select_one('a[href*="/stream/"]')
        if not link:
            continue

        href = link.get('href', '').strip()
        if not href:
            continue
        if href.startswith('//'):
            full_url = 'https:' + href
        elif href.startswith('/'):
            full_url = base_url.rstrip('/') + href
        elif href.startswith('http'):
            full_url = href
        else:
            full_url = base_url.rstrip('/') + '/' + href.lstrip('/')

        if full_url in seen:
            continue
        seen.add(full_url)

        title = (link.get('title') or '').strip()
        if not title:
            title_el = article.select_one('h2.h2-start a')
            title = title_el.get_text(' ', strip=True) if title_el else ''
        if not title:
            title = full_url.rstrip('/').split('/')[-1].replace('-', ' ').title()
        title = _normalize_filmpalast_title(title)

        release_el = article.select_one('.releaseTitleHome')
        release = release_el.get_text(' ', strip=True) if release_el else ''

        year = None
        for detail_el in article.select('.coverDetails'):
            m_year = re.search(r'Jahr:\s*(\d{4})', detail_el.get_text(' ', strip=True))
            if m_year:
                year = m_year.group(1)
                break

        is_series = bool(
            re.search(r'-s\d{1,2}e\d{1,2}', full_url, re.IGNORECASE)
            or re.search(r'\bS\d{1,2}E\d{1,2}\b', title, re.IGNORECASE)
            or re.search(r'\bS\d{1,2}E\d{1,2}\b', release, re.IGNORECASE)
        )
        items.append({
            'type': 'series' if is_series else 'film',
            'url': full_url,
            'title': title,
            'year': year,
            'release': release,
        })

    return items


async def get_filmpalast_latest(home_url: str = "https://filmpalast.to/") -> list[dict]:
    logging.info(f"Filmpalast Import: Lade Startseite {home_url}")
    headers = {'User-Agent': USER_AGENT, 'Referer': 'https://filmpalast.to/'}
    response = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: requests.get(home_url, headers=headers, timeout=25)
    )
    response.raise_for_status()
    from urllib.parse import urlparse as _urlparse
    _p = _urlparse(home_url)
    base = f"{_p.scheme}://{_p.netloc}"
    items = _parse_filmpalast_latest_html(response.text, base)
    logging.info(f"Filmpalast Import: {len(items)} Einträge gefunden")
    return items


def _is_megakino_listing(url: str) -> bool:
    """Erkennt eine megakino Film-Listing-Seite (kein einzelner Film)."""
    # Listing-Muster: /films/page/N oder /kinofilme/page/N (domain-unabhängig)
    if re.search(r'/(films|kinofilme)/page/\d+/?$', url):
        return True
    # Fallback für klassische megakino-Hauptseiten / Kategorien
    is_mk = bool(re.search(r'megakino\d*\.(to|org|net|club|biz)', url))
    return is_mk and not url.endswith('.html')

def _is_filmpalast_film(url: str) -> bool:
    """Erkennt einen einzelnen Film auf filmpalast.to (kein Serien-Episode)."""
    return 'filmpalast.to/stream/' in url and not re.search(r'-s\d{2}e\d{2}', url, re.IGNORECASE)


# ─── Kinoger.ch Support (direkter MP4-Stream) ────

def _is_kinoger_url(url: str) -> bool:
    """Erkennt Kinoger.ch Stream-Seiten (Film oder Serie)."""
    return bool(re.search(r'kinoger\.\w+/stream/', url)) and url.endswith('.html')


def _kinoger_extract_mp4(embed_url: str) -> str | None:
    """Holt die beste direkte MP4-URL aus einer fsst.online/embed/N/ Seite."""
    try:
        r = requests.get(embed_url.strip(),
                         headers={'User-Agent': USER_AGENT, 'Referer': 'https://kinoger.ch/'},
                         timeout=30)
        html = r.text
        quality_urls = _kinoger_extract_quality_urls(html)
        for quality in ('720p', '1080p', '360p'):
            if quality in quality_urls:
                if quality != '720p':
                    logging.warning(f"Kinoger: 720p fehlt, nehme {quality} für {embed_url}")
                return quality_urls[quality]
        # Legacy fallback for unusual Playerjs markup.
        m360 = re.search(r'\[360p\](https?://[^,"\s\\]+)', html)
        if m360:
            return m360.group(1)
        # Fallback: direkte <video src=...>
        mv = re.search(r'<video[^>]+src="(https?://[^"]+\.mp4[^"]*)"', html)
        if mv:
            return mv.group(1)
    except Exception as e:
        logging.warning(f"Kinoger MP4-Extract Fehler: {e}")
    return None


def _kinoger_extract_quality_urls(html: str) -> dict[str, str]:
    """Parse Playerjs quality URLs, preserving the best URL per quality label."""
    quality_urls: dict[str, str] = {}
    for match in re.finditer(r'\[(\d+p)\](https?://[^,"\s\\]+)', html):
        quality_urls[match.group(1)] = match.group(2)
    return quality_urls


def _kinoger_quality_height_from_url(url: str | None) -> int | None:
    if not url:
        return None
    match = re.search(r'_(\d{3,4})p\.mp4', url)
    if match:
        return int(match.group(1))
    if re.search(r'/\d+\.mp4/?(?:$|[?&])', url):
        return 1080
    return None


def _kinoger_best_quality_height(embed_url: str) -> int | None:
    try:
        r = requests.get(embed_url.strip(),
                         headers={'User-Agent': USER_AGENT, 'Referer': 'https://kinoger.ch/'},
                         timeout=30)
        quality_urls = _kinoger_extract_quality_urls(r.text)
        for quality in ('720p', '1080p', '360p'):
            if quality in quality_urls:
                return int(quality[:-1])
    except Exception as e:
        logging.warning(f"Kinoger Qualitätscheck Fehler: {e}")
    return None


async def _kinoger_resolve_browser_media(embed_url: str, preferred_qualities: tuple[str, ...] = ('720p', '1080p', '360p')) -> str | None:
    """Resolve the short-lived playback URL that incvideo creates in a real browser."""
    try:
        async with _open_browser(camoufox=False) as browser:
            context = await browser.new_context()
            page = await context.new_page()
            media_urls: list[str] = []

            def _capture_media(response):
                content_type = response.headers.get('content-type', '')
                if 'remote_control.php' in response.url or content_type.startswith('video/'):
                    media_urls.append(response.url)

            page.on('response', _capture_media)
            await page.goto(embed_url, referer='https://kinoger.ch/', wait_until='networkidle', timeout=30000)
            html = await page.content()
            quality_urls = _kinoger_extract_quality_urls(html)
            for quality in preferred_qualities:
                direct_url = quality_urls.get(quality)
                if not direct_url:
                    continue
                logging.info(f"Kinoger Qualität gewählt: {quality}")
                media_urls.clear()
                await page.goto(direct_url, referer='https://incvideo1.online/', wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(2000)
                break
            else:
                await page.wait_for_timeout(3000)
            await browser.close()

            for media_url in media_urls:
                if 'remote_control.php' in media_url:
                    return media_url
            return media_urls[0] if media_urls else None
    except Exception as e:
        logging.warning(f"Kinoger Browser-Media Auflösung fehlgeschlagen: {e}")
        return None


def _extract_balanced_js_array(text: str, start_pos: int) -> str | None:
    """Return the JS array starting at/after start_pos, respecting nested arrays and strings."""
    start = text.find('[', start_pos)
    if start < 0:
        return None

    depth = 0
    quote = None
    escaped = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if quote:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
        elif ch == '[':
            depth += 1
        elif ch == ']':
            depth -= 1
            if depth == 0:
                return text[start:idx + 1]
    return None


def _parse_kinoger_pw_seasons(html: str) -> dict[int, list[str]]:
    show_match = re.search(r'pw\.show\(\s*\d+\s*,', html)
    if not show_match:
        return {}

    arr_text = _extract_balanced_js_array(html, show_match.end())
    if not arr_text:
        return {}

    try:
        raw_seasons = ast.literal_eval(arr_text)
    except Exception as e:
        logging.warning(f"Kinoger pw.show Array konnte nicht gelesen werden: {e}")
        return {}

    seasons: dict[int, list[str]] = {}
    for s_idx, raw_urls in enumerate(raw_seasons, start=1):
        if not isinstance(raw_urls, list):
            continue
        urls = []
        for raw_url in raw_urls:
            if not isinstance(raw_url, str):
                continue
            url = raw_url.strip()
            if url.startswith('https://fsst.online/embed/'):
                urls.append(url)
        if urls:
            seasons[s_idx] = urls
    return seasons


def _parse_kinoger_ollhd_seasons(html: str) -> dict[int, list[str]]:
    show_match = re.search(r'ollhd\.show\(\s*\d+\s*,', html)
    if not show_match:
        return {}

    arr_text = _extract_balanced_js_array(html, show_match.end())
    if not arr_text:
        return {}

    try:
        raw_seasons = ast.literal_eval(arr_text)
    except Exception as e:
        logging.warning(f"Kinoger ollhd.show Array konnte nicht gelesen werden: {e}")
        return {}

    seasons: dict[int, list[str]] = {}
    for s_idx, raw_urls in enumerate(raw_seasons, start=1):
        if not isinstance(raw_urls, list):
            continue
        urls = []
        for raw_url in raw_urls:
            if not isinstance(raw_url, str):
                continue
            url = raw_url.strip()
            if url.startswith('https://voe.sx/e/'):
                urls.append(url)
        if urls:
            seasons[s_idx] = urls
    return seasons


def _voe_link_available(url: str) -> bool:
    """Fast check for dead VOE links before spending time in Playwright."""
    if 'voe.sx/e/' not in url:
        return True
    try:
        r = requests.get(
            url.strip(),
            headers={'User-Agent': USER_AGENT, 'Referer': 'https://kinoger.ch/'},
            timeout=15,
            allow_redirects=True,
        )
        if r.status_code == 404:
            return False
        title = re.search(r'<title[^>]*>(.*?)</title>', r.text, re.IGNORECASE | re.DOTALL)
        if title and '404' in title.group(1):
            return False
        if '404 - Nicht gefunden' in r.text or 'angeforderte Ressource nicht finden' in r.text:
            return False
        return True
    except Exception as e:
        logging.warning(f"VOE-Link-Check fehlgeschlagen ({url}): {e}")
        return True


def _kinoger_scrape(url: str) -> dict:
    """Scrapt kinoger.ch Seite. Returns:
      Film:  {'type':'film',   'title':str, 'year':int|None, 'embed_url':str}
      Serie: {'type':'series', 'title':str, 'year':int|None, 'seasons':{1:[ep1_embed, ep2_embed, ...], ...}}"""
    r = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=30)
    html = r.text
    soup = BeautifulSoup(html, 'html.parser')

    # Titel + Jahr aus h1#news-title (z.B. "From (2022)" / "Mortal Kombat II (2026)")
    h1 = soup.find('h1', id='news-title')
    raw = h1.get_text(' ', strip=True) if h1 else ''
    tm = re.match(r'^(.*?)\s*\((\d{4})\)\s*$', raw)
    if tm:
        title = tm.group(1).strip()
        year = int(tm.group(2))
    else:
        title = raw or 'Kinoger Video'
        year = None

    # Serie? -> pw.show/ollhd.show(NUM, [[s1ep1, s1ep2, ...], [s2ep1, ...], ...])
    seasons = _parse_kinoger_pw_seasons(html) or _parse_kinoger_ollhd_seasons(html)
    if seasons:
        total_embeds = sum(len(urls) for urls in seasons.values())
        has_season_ui = bool(soup.select_one('#seasonspw')) or re.search(r'\bStaffel\s+\d+\b', html, re.IGNORECASE)
        # Kinoger uses pw.show(1, [[embed]]) for normal films too. Treat that
        # single-embed shape as film unless the page explicitly exposes seasons.
        if len(seasons) == 1 and total_embeds == 1 and not has_season_ui:
            embed = next(iter(seasons.values()))[0]
            return {'type': 'film', 'title': title, 'year': year, 'embed_url': embed}
        return {'type': 'series', 'title': title, 'year': year, 'seasons': seasons}

    # Film? → iframe mit /embed/ src
    iframe = soup.find('iframe', src=re.compile(r'/embed/'))
    embed = iframe.get('src') if iframe else None
    return {'type': 'film', 'title': title, 'year': year, 'embed_url': embed}


async def process_kinoger_direct(msg, mp4_url: str, manual_title: str, db_content_type: str = 'film',
                                  is_anime: bool = False, progress_prefix: str = ''):
    """Direkter Download für Kinoger: MP4-URL ist bekannt, kein Playwright nötig."""
    import time as _time
    type_emoji = {"film": "🎬", "serie": "📺", "anime": "🎌"}
    icon = type_emoji.get(db_content_type, "🎬")

    try:
        title = manual_title
        if is_anime and 'anime' not in title.lower():
            title = title + ' anime'
        filename = format_filename(title)

        # DB-Check + VOE-Vergleich (mp4_url als source-Identifier)
        update_due_to_new_source = False
        if _db_exists(db_content_type, filename, is_anime):
            stored = _db_get_voe(db_content_type, filename, is_anime)
            should_redownload_for_quality = False
            embed_for_quality = mp4_url
            if '/get_file/' in mp4_url and 'incvideo' in mp4_url:
                embed_match = re.search(r'/(\d+)/\1(?:_\d+p)?\.mp4/?$', mp4_url)
                if embed_match:
                    embed_for_quality = f"https://fsst.online/embed/{embed_match.group(1)}/"
            if '/embed/' in embed_for_quality:
                local_path = os.path.join(DOWNLOAD_DIR, f"{filename}.mp4")
                existing_height = None
                if os.path.exists(local_path):
                    existing_height = get_video_metadata(local_path).get('height')
                best_height = _kinoger_best_quality_height(embed_for_quality)
                stored_height = _kinoger_quality_height_from_url(stored)
                current_height = existing_height or stored_height
                if best_height and current_height and best_height > current_height + 40:
                    should_redownload_for_quality = True
                    update_due_to_new_source = True
                    await msg.edit_text(
                        f"♻️ Kinoger Qualitäts-Update: `{filename}`\n"
                        f"{current_height}p → {best_height}p",
                        parse_mode='Markdown'
                    )
                elif best_height and current_height is None:
                    # Alte Einträge ohne lokale Datei/Qualitätsinfo nicht blind überspringen.
                    should_redownload_for_quality = True
                    update_due_to_new_source = True
                    await msg.edit_text(
                        f"♻️ Kinoger Qualitäts-Check: `{filename}` wird neu geladen.",
                        parse_mode='Markdown'
                    )
            if should_redownload_for_quality:
                pass
            elif stored == mp4_url or not stored:
                await msg.edit_text(f"⏭ Bereits vorhanden: `{filename}`", parse_mode='Markdown')
                return True
            else:
                update_due_to_new_source = True
                await msg.edit_text(f"♻️ Update (neue Quelle): `{filename}`", parse_mode='Markdown')

        filepath = os.path.join(DOWNLOAD_DIR, f"{filename}.mp4")
        await msg.edit_text(f"{icon} {progress_prefix}📥 Lade herunter...\n`{filename}`", parse_mode='Markdown')

        download_url = mp4_url
        referer = 'https://kinoger.ch/'
        embed_url = mp4_url
        if '/get_file/' in mp4_url and 'incvideo' in mp4_url:
            embed_match = re.search(r'/(\d+)/\1(?:_\d+p)?\.mp4/?$', mp4_url)
            if embed_match:
                embed_url = f"https://fsst.online/embed/{embed_match.group(1)}/"

        if '/embed/' in embed_url:
            referer = embed_url
            await msg.edit_text(
                f"{icon} {progress_prefix}🔍 Löse Kinoger-Player auf...\n`{filename}`",
                parse_mode='Markdown'
            )
            browser_media = await _kinoger_resolve_browser_media(embed_url)
            if browser_media:
                download_url = browser_media
                referer = 'https://incvideo1.online/' if 'remote_control.php' in browser_media else embed_url
                logging.info(f"Kinoger Browser-Media URL: {download_url[:120]}")
            else:
                extracted_mp4 = await asyncio.get_event_loop().run_in_executor(None, _kinoger_extract_mp4, embed_url)
                if extracted_mp4:
                    download_url = extracted_mp4
                else:
                    await msg.edit_text("❌ Kinoger-Player konnte keine Video-URL liefern.")
                    return False

        # Progress-Monitor
        dl_start = _time.monotonic()
        stop_progress = asyncio.Event()
        _dl_status.update({'filename': filename, 'start': dl_start, 'size_mb': 0, 'phase': 'download'})

        async def progress_monitor():
            while not stop_progress.is_set():
                await asyncio.sleep(5)
                if stop_progress.is_set():
                    break
                try:
                    part = filepath + '.part'
                    check = part if os.path.exists(part) else filepath
                    size_mb = os.path.getsize(check) / (1024 * 1024) if os.path.exists(check) else 0
                    elapsed = int(_time.monotonic() - dl_start)
                    m, s = divmod(elapsed, 60)
                    _dl_status['size_mb'] = size_mb
                    _dl_status['elapsed'] = elapsed
                    await msg.edit_text(
                        f"{icon} {progress_prefix}📥 Lade herunter... ⏱ {m:02d}:{s:02d}\n`{filename}`\n💾 {size_mb:.0f} MB",
                        parse_mode='Markdown'
                    )
                except Exception:
                    pass

        progress_task = asyncio.create_task(progress_monitor())
        try:
            success = await download_with_ytdlp(download_url, filepath, referer, "")
            if not success:
                logging.warning("yt-dlp fehlgeschlagen, Fallback auf ffmpeg...")
                success = await download_with_ffmpeg(download_url, filepath, referer, "")
        finally:
            stop_progress.set()
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass

        if not success or not os.path.exists(filepath):
            await msg.edit_text("❌ Download fehlgeschlagen!")
            return False

        filepath = await remux_video_audio_only(filepath, msg)
        filepath = await downscale_if_needed(filepath, msg)
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        _dl_status.update({'phase': 'upload', 'size_mb': size_mb})
        await msg.edit_text(f"📤 Starte Upload... ({size_mb:.1f} MB)")

        caption = build_caption(filename, db_content_type)
        await upload_with_telethon(filepath, caption, msg, progress_prefix=progress_prefix)
        _db_add(db_content_type, filename, is_anime, overwrite=update_due_to_new_source, voe_url=mp4_url)
        await msg.edit_text(f"✅ Fertig!\n`{filename}.mp4`", parse_mode='Markdown')
        return True

    except Exception as e:
        tb = traceback.format_exc()[-1500:]
        logging.error(f"process_kinoger_direct Fehler:\n{traceback.format_exc()}")
        try:
            await msg.edit_text(f"❌ Kinoger-Fehler: {str(e)}\n\n<pre>{tb}</pre>", parse_mode='HTML')
        except Exception:
            pass
        return False

def _is_filmpalast_series(url: str) -> bool:
    """Erkennt eine Serien-Episode auf filmpalast.to (URL enthält -sXXeXX)."""
    return 'filmpalast.to/stream/' in url and bool(re.search(r'-s\d{2}e\d{2}', url, re.IGNORECASE))


# ─── Queue-Worker ─────────────────────────────────

def _normalize_watch_url(url: str) -> str:
    return url.strip().rstrip('/')

def _persist_watchlist():
    try:
        with open(WATCHLIST_FILE, 'w') as f:
            json.dump(_watchlist, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"Watchlist speichern Fehler: {e}")

def _load_watchlist() -> list[dict]:
    try:
        with open(WATCHLIST_FILE) as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []

def _watchlist_add(entry_type: str, url: str, chat_id: int | None, is_anime: bool = False) -> bool:
    normalized_url = _normalize_watch_url(url)
    for item in _watchlist:
        if (
            item.get('type') == entry_type and
            item.get('url') == normalized_url and
            bool(item.get('is_anime')) == bool(is_anime)
        ):
            if chat_id and not item.get('chat_id'):
                item['chat_id'] = chat_id
                _persist_watchlist()
            return False

    _watchlist.append({
        'type': entry_type,
        'url': normalized_url,
        'is_anime': bool(is_anime),
        'chat_id': chat_id,
        'enabled': True,
        'added_at': _utc_now_iso(),
    })
    _persist_watchlist()
    logging.info(f"Watchlist hinzugefügt: type={entry_type} anime={is_anime} url={normalized_url}")
    return True

def _watchlist_find(entry_type: str, url: str, is_anime: bool = False) -> dict | None:
    normalized_url = _normalize_watch_url(url)
    for item in _watchlist:
        if (
            item.get('type') == entry_type and
            item.get('url') == normalized_url and
            bool(item.get('is_anime')) == bool(is_anime)
        ):
            return item
    return None

def _watchlist_update(entry_type: str, url: str, is_anime: bool = False, **fields) -> bool:
    item = _watchlist_find(entry_type, url, is_anime)
    if not item:
        return False
    changed = False
    for key, value in fields.items():
        if item.get(key) != value:
            item[key] = value
            changed = True
    if changed:
        _persist_watchlist()
    return changed

def _watchlist_job_exists(entry_type: str, url: str, is_anime: bool = False) -> bool:
    normalized_url = _normalize_watch_url(url)
    candidates = list(_dl_pending)
    if _dl_active:
        candidates.append(_dl_active)
    for item in candidates:
        persist = item.get('persist', {})
        if (
            persist.get('type') == entry_type and
            _normalize_watch_url(persist.get('url', '')) == normalized_url and
            bool(persist.get('is_anime')) == bool(is_anime)
        ):
            return True
    return False

def _persist_queue(reason: str = 'update'):
    """Queue atomar speichern und jede Version in einem Journal festhalten."""
    try:
        data = [i['persist'] for i in _dl_pending if 'persist' in i]
        tmp_file = QUEUE_FILE + '.tmp'
        if os.path.exists(QUEUE_FILE):
            try:
                with open(QUEUE_FILE, 'rb') as src, open(QUEUE_BACKUP_FILE + '.tmp', 'wb') as dst:
                    dst.write(src.read())
                    dst.flush()
                    os.fsync(dst.fileno())
                os.replace(QUEUE_BACKUP_FILE + '.tmp', QUEUE_BACKUP_FILE)
            except Exception as backup_error:
                logging.warning(f"Queue-Backup Fehler: {backup_error}")
        with open(tmp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_file, QUEUE_FILE)
        event = {
            'timestamp': _utc_now_iso(),
            'reason': reason,
            'queue': data,
        }
        with open(QUEUE_JOURNAL_FILE, 'a', encoding='utf-8') as journal:
            journal.write(json.dumps(event, ensure_ascii=False) + '\n')
            journal.flush()
            os.fsync(journal.fileno())
    except Exception as e:
        logging.warning(f"Queue speichern Fehler: {e}")

def _persist_progress():
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(_series_progress, f)
    except Exception as e:
        logging.warning(f"Progress speichern Fehler: {e}")

def _load_queue() -> list[dict]:
    try:
        with open(QUEUE_FILE) as f:
            return json.load(f)
    except Exception:
        return []

def _load_progress() -> dict[str, int]:
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


async def _enqueue(chat_id: int, msg, coro_func, persist: dict, *coro_args,
                   notify_position: bool = True, persist_now: bool = True):
    """Fügt einen Download zur Warteschlange hinzu und speichert auf Disk."""
    persist['chat_id'] = chat_id
    if persist.get('type') == 'series':
        _watchlist_add('series', persist['url'], chat_id, persist.get('is_anime', False))
    elif persist.get('type') == 'bulk_films':
        _watchlist_add('bulk_films', persist['url'], chat_id, False)

    # Producer-Bots schreiben nur nach PostgreSQL. Sie laden selbst nichts und
    # verändern ihre lokale queue.json nicht. Worker-Jobs tragen bereits eine
    # Claim-ID und werden deshalb nicht erneut veröffentlicht.
    if CENTRAL_QUEUE_ROLE == 'producer' and not persist.get('_central_job_id'):
        job_id, created = await asyncio.to_thread(_central_queue.enqueue, persist)
        label = _queue_item_label(persist)
        if created:
            await msg.edit_text(f"☁️ Zentrale Queue #{job_id}: {label}")
        else:
            await msg.edit_text(f"ℹ️ Bereits in zentraler Queue #{job_id}: {label}")
        return
    item = {
        'chat_id': chat_id, 'msg': msg, 'coro': coro_func,
        'args': (msg,) + coro_args, 'persist': persist,
    }
    _dl_pending.append(item)
    if persist_now:
        _persist_queue('enqueue')
    pos = len(_dl_pending)
    if notify_position and pos > 1:
        await msg.edit_text(f"⏳ Warteschlange: Platz {pos} von {pos}\nWarte auf aktiven Download...")
    await _dl_queue.put(item)


def _failed_retry_delay(failure_count: int) -> int:
    """Exponentieller Retry-Abstand: standardmäßig 5, 10, 20 ... max. 60 Minuten."""
    base = max(1, int(os.getenv('FAILED_RETRY_BASE_SECONDS', '300')))
    return min(base * (2 ** max(0, failure_count - 1)), 3600)


async def _requeue_failed_after(item: dict, delay: int) -> None:
    await asyncio.sleep(delay)
    if item.get('cancelled') or item not in _dl_pending:
        return
    item['failed'] = False
    item.pop('retry_task', None)
    await _dl_queue.put(item)
    logging.info(
        "Fehlgeschlagener Queue-Eintrag wieder hinten eingereiht: %s",
        _queue_item_label(item.get('persist', {})),
    )


async def _central_job_heartbeat(job_id: int) -> None:
    """Hält lange Downloads reserviert, damit kein zweiter Worker sie übernimmt."""
    interval = max(30, int(os.getenv('CENTRAL_QUEUE_HEARTBEAT_SECONDS', '60')))
    while True:
        await asyncio.sleep(interval)
        renewed = await asyncio.to_thread(
            _central_queue.heartbeat, job_id, CENTRAL_QUEUE_WORKER_ID
        )
        if not renewed:
            logging.warning("Lease für zentralen Job #%s konnte nicht verlängert werden", job_id)
            return


def _archive_dead_job(persist: dict, reason: str) -> None:
    event = {
        'timestamp': _utc_now_iso(),
        'reason': reason,
        'job': persist,
    }
    try:
        with open(DEAD_JOBS_FILE, 'a', encoding='utf-8') as journal:
            journal.write(json.dumps(event, ensure_ascii=False) + '\n')
            journal.flush()
            os.fsync(journal.fileno())
        os.chmod(DEAD_JOBS_FILE, 0o600)
    except Exception as error:
        logging.error(f"Dead-Job sichern fehlgeschlagen: {error}")


async def _dl_worker():
    """Verarbeitet Downloads sequenziell."""
    global _dl_active
    while True:
        item = await _dl_queue.get()

        # Abgebrochen während Wartezeit → überspringen
        if item.get('cancelled'):
            _dl_queue.task_done()
            continue

        # Exakte Episodentitel zuerst lokal prüfen. So werden bereits erledigte
        # Jobs ohne Browser, VOE-Auflösung oder Telegram-Nachricht entfernt.
        if _queue_item_already_done(item.get('persist', {})):
            label = _queue_item_label(item['persist'])
            logging.info(f"[SKIP-PREFLIGHT] Bereits in DB: {label}")
            if item in _dl_pending:
                _dl_pending.remove(item)
            central_job_id = item.get('persist', {}).get('_central_job_id')
            if central_job_id:
                await asyncio.to_thread(
                    _central_queue.complete, central_job_id, CENTRAL_QUEUE_WORKER_ID
                )
            _persist_queue('preflight-already-done')
            _dl_queue.task_done()
            continue

        _dl_active = item
        central_job_id = item.get('persist', {}).get('_central_job_id')
        heartbeat_task = (
            _spawn_task(_central_job_heartbeat(central_job_id), f'central-heartbeat-{central_job_id}')
            if central_job_id else None
        )
        print(f"[WORKER] Starte: {item['coro'].__name__}", flush=True)
        succeeded = False
        permanent_failure = False
        failure_text = None
        try:
            result = await item['coro'](*item['args'])
            permanent_failure = result == PERMANENT_JOB_FAILURE
            succeeded = result is not False and not permanent_failure
            if permanent_failure:
                failure_text = 'Mediendatei wurde dauerhaft entfernt'
            elif not succeeded:
                failure_text = 'Verarbeitung meldete einen Fehler'
        except Exception as e:
            failure_text = str(e)
            tb = traceback.format_exc()[-1500:]
            logging.error(f"Queue-Worker Fehler:\n{traceback.format_exc()}")
            try:
                await item['msg'].edit_text(f"❌ Fehler: {e}\n\n🔍 Debug:\n```\n{tb}\n```", parse_mode='Markdown')
            except Exception:
                pass
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
            _dl_active = None
            if permanent_failure:
                persist = item.get('persist', {})
                persist['permanent_failure'] = True
                persist['last_error'] = failure_text
                persist['last_failed_at'] = _utc_now_iso()
                _archive_dead_job(persist, failure_text or 'Dauerhaft nicht verfügbar')
                if item in _dl_pending:
                    _dl_pending.remove(item)
                if persist.get('_central_job_id'):
                    await asyncio.to_thread(
                        _central_queue.dead,
                        persist['_central_job_id'], CENTRAL_QUEUE_WORKER_ID,
                        failure_text or 'Dauerhaft nicht verfügbar',
                    )
                _persist_queue('permanent-failure-archived')
                logging.warning("Dauerhaft entfernten Job archiviert: %s", _queue_item_label(persist))
            elif succeeded and item in _dl_pending:
                _dl_pending.remove(item)
                persist = item.get('persist', {})
                if persist.get('_central_job_id'):
                    await asyncio.to_thread(
                        _central_queue.complete,
                        persist['_central_job_id'], CENTRAL_QUEUE_WORKER_ID,
                    )
                _persist_queue('completed')
            elif not succeeded:
                persist = item.get('persist', {})
                persist['failure_count'] = int(persist.get('failure_count', 0)) + 1
                persist['last_error'] = (failure_text or 'Unbekannter Fehler')[:500]
                persist['last_failed_at'] = _utc_now_iso()
                item['failed'] = True
                delay = _failed_retry_delay(persist['failure_count'])
                persist['retry_delay_seconds'] = delay
                if persist.get('_central_job_id'):
                    await asyncio.to_thread(
                        _central_queue.retry,
                        persist['_central_job_id'], CENTRAL_QUEUE_WORKER_ID,
                        persist['last_error'], delay,
                    )
                    if item in _dl_pending:
                        _dl_pending.remove(item)
                    _persist_queue('central-failed-released')
                    logging.error(
                        "Zentraler Queue-Eintrag #%s in %ss erneut verfügbar: %s",
                        persist['_central_job_id'], delay, _queue_item_label(persist),
                    )
                    _dl_queue.task_done()
                    continue
                _persist_queue('failed-retained')
                retry_task = _spawn_task(
                    _requeue_failed_after(item, delay),
                    f"queue-retry-{persist['failure_count']}",
                )
                item['retry_task'] = retry_task
                logging.error(
                    "Queue-Eintrag bleibt erhalten und wird in %ss hinten eingereiht: %s (Fehler %s)",
                    delay, _queue_item_label(persist), persist['failure_count']
                )
            _dl_queue.task_done()
            # verbleibende Positionen aktualisieren
            for i, pending in enumerate(_dl_pending, 1):
                if isinstance(pending['msg'], _LazyTelegramMessage):
                    continue
                try:
                    await pending['msg'].edit_text(
                        f"⏳ Warteschlange: Platz {i} von {len(_dl_pending)}\nWarte auf aktiven Download..."
                    )
                except Exception:
                    pass


def _queue_item_label(persist: dict) -> str:
    """Lesbaren Namen für ein Queue-Item erzeugen."""
    if persist.get('type') == 'series':
        url   = persist.get('url', '')
        parts = [p for p in url.rstrip('/').split('/') if p]
        name  = parts[-1].replace('-', ' ').title() if parts else url
        emoji = '🎌' if persist.get('is_anime') else '📺'
        return f"{emoji} {name}"
    ct    = persist.get('content_type', 'film')
    title = persist.get('manual_title') or persist.get('url', '')[:50]
    emoji = '🎬' if ct == 'film' else ('🎌' if persist.get('is_anime') else '📺')
    return f"{emoji} {title}"


def _queue_item_already_done(persist: dict) -> bool:
    """Schneller DB-Check für eindeutig benannte Einzel-Episoden."""
    if persist.get('type') != 'video' or persist.get('force_reupload'):
        return False
    title = persist.get('manual_title')
    content_type = persist.get('content_type')
    if not title or content_type not in ('serie', 'anime'):
        return False
    return _db_exists(content_type, title, bool(persist.get('is_anime')))


async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt aktuelle Warteschlange mit Abbrechen-Buttons."""
    drafts = await asyncio.to_thread(_central_queue.collecting_batches) if _central_queue else []
    draft_lines = [
        f"🧩 *{batch['title']}* — {batch['stored_jobs']}/{batch['expected_jobs']} Links gespeichert"
        for batch in drafts
    ]
    if CENTRAL_QUEUE_ROLE == 'producer':
        jobs = await asyncio.to_thread(_central_queue.open_jobs)
        if not jobs and not drafts:
            await update.message.reply_text("✅ Zentrale Warteschlange ist leer.")
            return
        lines = []
        for job in jobs:
            marker = '▶️' if job['status'] == 'processing' else '☁️'
            lines.append(f"{marker} *#{job['id']}* {_queue_item_label(job['payload'])}")
        sections = lines + (["\n🧩 *Noch in Extraktion:*", *draft_lines] if draft_lines else [])
        await update.message.reply_text(
            "📋 *Zentrale PostgreSQL-Warteschlange:*\n" + "\n".join(sections),
            parse_mode='Markdown',
        )
        return
    if not _dl_pending and not drafts:
        await update.message.reply_text("✅ Warteschlange ist leer.")
        return

    if not _dl_pending and drafts:
        await update.message.reply_text(
            "🧩 *Zentrale Serien in Extraktion:*\n" + "\n".join(draft_lines),
            parse_mode='Markdown',
        )
        return

    lines = []
    keyboard_rows = []
    for i, item in enumerate(_dl_pending):
        label = _queue_item_label(item.get('persist', {}))
        if item is _dl_active:
            lines.append(f"▶️ *{i+1}.* {label}  _(aktiv)_")
            keyboard_rows.append([
                InlineKeyboardButton(
                    f"⛔ Aktiven Download abbrechen",
                    callback_data=f"cancel_queue_{id(item)}"
                )
            ])
        else:
            lines.append(f"⏳ *{i+1}.* {label}")
            keyboard_rows.append([
                InlineKeyboardButton(
                    f"❌ #{i+1} abbrechen",
                    callback_data=f"cancel_queue_{id(item)}"
                )
            ])

    text = "📋 *Warteschlange:*\n" + "\n".join(lines)
    if draft_lines:
        text += "\n\n🧩 *Zentrale Serien in Extraktion:*\n" + "\n".join(draft_lines)
    await update.message.reply_text(
        text, parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard_rows) if keyboard_rows else None
    )


async def _enqueue_series_episode_batch(app, chat_id: int, series_title: str, episodes: list[dict], is_anime: bool) -> int:
    queued = 0
    total = len(episodes)
    for idx, ep in enumerate(episodes, 1):
        title = ep.get('title') or f"{series_title} S{ep['season']:02d}E{ep['episode']:02d}"
        msg = await app.bot.send_message(
            chat_id,
            f"📥 Watchlist Queue: {title}\nNeue Folge {idx}/{total}"
        )
        await _enqueue(chat_id, msg, process_video,
            {
                'type': 'video',
                'url': ep['url'],
                'content_type': ep.get('content_type') or ('anime' if is_anime else 'serie'),
                'manual_title': title,
                'is_anime': is_anime,
            },
            ep['url'],
            ep.get('content_type') or ('anime' if is_anime else 'serie'),
            title,
            is_anime)
        queued += 1
    return queued


async def _watchlist_check_once(app, notify_empty: bool = False, manual_chat_id: int | None = None) -> tuple[int, int]:
    added_jobs = 0
    checked_items = 0

    if not manual_chat_id and (_dl_active or _dl_pending):
        logging.info("Watchlist-Check übersprungen: Queue/Download ist aktiv")
        return checked_items, added_jobs

    for entry in list(_watchlist):
        if not entry.get('enabled', True):
            continue

        entry_type = entry.get('type')
        url = entry.get('url')
        is_anime = bool(entry.get('is_anime', False))
        chat_id = manual_chat_id or entry.get('chat_id')
        if not url or not chat_id:
            continue

        checked_items += 1

        try:
            if entry_type == 'series':
                if _watchlist_job_exists('series', url, is_anime):
                    continue
                series_title, episodes = await get_series_episodes(url, is_anime)
                current_total = len(episodes)
                previous_total = entry.get('last_seen_total')
                _watchlist_update('series', url, is_anime, last_series_title=series_title)

                if previous_total is None:
                    _watchlist_update('series', url, is_anime, last_seen_total=current_total)
                    continue

                if current_total <= previous_total:
                    if current_total < previous_total:
                        _watchlist_update('series', url, is_anime, last_seen_total=current_total)
                    continue

                new_episodes = [ep for ep in episodes[previous_total:] if not ep.get('skip', False)]
                if not new_episodes:
                    _watchlist_update('series', url, is_anime, last_seen_total=current_total)
                    continue

                queued = await _enqueue_series_episode_batch(app, chat_id, series_title, new_episodes, is_anime)
                _watchlist_update('series', url, is_anime, last_seen_total=current_total)
                if queued:
                    await app.bot.send_message(
                        chat_id,
                        f"🆕 Watchlist: {series_title}\n{queued} neue Folge(n) ab Stand {previous_total} -> {current_total} in die Queue gelegt."
                    )
                    added_jobs += queued
            elif entry_type == 'bulk_films':
                if _watchlist_job_exists('bulk_films', url, False):
                    continue
                films = await get_bulk_films_from_page(url)
                if not films:
                    continue
                msg = await app.bot.send_message(
                    chat_id,
                    f"🆕 Watchlist: Listing erneut geprüft. Füge Seite zur Queue hinzu...\n`{url}`",
                    parse_mode='Markdown'
                )
                await _enqueue(chat_id, msg, process_bulk_films,
                    {'type': 'bulk_films', 'url': url},
                    url)
                added_jobs += 1
        except Exception:
            logging.warning(f"Watchlist-Check Fehler für {url}:\n{traceback.format_exc()}")

    if notify_empty and manual_chat_id:
        if added_jobs:
            await app.bot.send_message(
                manual_chat_id,
                f"✅ Watchlist geprüft: {checked_items} Einträge, {added_jobs} neuer Job in die Queue gelegt."
            )
        else:
            await app.bot.send_message(
                manual_chat_id,
                f"✅ Watchlist geprüft: {checked_items} Einträge, nichts Neues gefunden."
            )

    return checked_items, added_jobs


async def cmd_watchcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _watchlist:
        await update.message.reply_text("ℹ️ Watchlist ist leer.")
        return

    await update.message.reply_text("🔍 Prüfe Watchlist auf neue Folgen/Filme...")
    await _watchlist_check_once(
        context.application,
        notify_empty=True,
        manual_chat_id=update.effective_chat.id,
    )


async def cmd_megakino_last_added(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("🔍 Prüfe Megakino: Zuletzt hinzugefügt...")

    try:
        items = await get_megakino_last_added()
        if not items:
            await msg.edit_text("❌ Kein 'Zuletzt hinzugefügt'-Block gefunden.")
            return

        film_found = film_existing = film_queued = 0
        series_found = series_complete = series_queued = 0

        for item in items:
            if item['type'] == 'film':
                film_found += 1
                if _db_exists('film', item['title'], False):
                    film_existing += 1
                    continue
                qmsg = await update.message.reply_text(f"📥 In Queue: {item['title']}")
                await _enqueue(chat_id, qmsg, process_video,
                    {'type': 'video', 'url': item['url'], 'content_type': 'film', 'manual_title': item['title'], 'is_anime': False},
                    item['url'], 'film', item['title'], False)
                film_queued += 1
                continue

            series_found += 1
            if _watchlist_job_exists('series', item['url'], False):
                series_queued += 1
                continue

            series_title, episodes = await get_series_episodes(item['url'], False)
            missing_eps = [ep for ep in episodes if not ep.get('skip', False)]
            if not missing_eps:
                series_complete += 1
                _watchlist_add('series', item['url'], chat_id, False)
                _watchlist_update('series', item['url'], False, last_seen_total=len(episodes), last_series_title=series_title)
                continue

            qmsg = await update.message.reply_text(
                f"📥 In Queue: {series_title}\n{len(missing_eps)} fehlende/neue Folge(n)"
            )
            await _enqueue(chat_id, qmsg, process_full_series,
                {'type': 'series', 'url': item['url'], 'is_anime': False},
                item['url'], False)
            series_queued += 1

        await msg.edit_text(
            "✅ Megakino Last Added geprüft.\n\n"
            f"🎬 Filme: {film_found} gefunden, {film_existing} schon vorhanden, {film_queued} in Queue\n"
            f"📺 Serien: {series_found} gefunden, {series_complete} komplett, {series_queued} in Queue"
        )
    except Exception as e:
        tb = traceback.format_exc()[-1500:]
        logging.error(f"cmd_megakino_last_added Fehler:\n{traceback.format_exc()}")
        await msg.edit_text(f"❌ Megakino-Check Fehler: {e}\n\n```\n{tb}\n```", parse_mode='Markdown')


async def cmd_hdfilme_latest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("🔍 Prüfe HDFilme: neueste Filme und Serien...")

    try:
        items = await get_hdfilme_latest()
        if not items:
            await msg.edit_text("❌ Keine HDFilme-Latest-Einträge gefunden.")
            return

        film_found = film_existing = film_queued = 0
        series_found = series_complete = series_queued = 0

        for item in items:
            if item['type'] == 'film':
                film_found += 1
                if _db_exists('film', item['title'], False):
                    film_existing += 1
                    continue
                qmsg = await update.message.reply_text(f"📥 HDFilme Film in Queue: {item['title']}")
                await _enqueue(chat_id, qmsg, process_video,
                    {'type': 'video', 'url': item['url'], 'content_type': 'film', 'manual_title': None, 'is_anime': False},
                    item['url'], 'film', None, False)
                film_queued += 1
                continue

            series_found += 1
            if _watchlist_job_exists('series', item['url'], False):
                series_queued += 1
                continue

            series_title, episodes = await get_series_episodes(item['url'], False)
            missing_eps = [ep for ep in episodes if not ep.get('skip', False)]
            if not missing_eps:
                series_complete += 1
                _watchlist_add('series', item['url'], chat_id, False)
                _watchlist_update('series', item['url'], False, last_seen_total=len(episodes), last_series_title=series_title)
                continue

            qmsg = await update.message.reply_text(
                f"📥 HDFilme Serie in Queue: {series_title}\n{len(missing_eps)} fehlende/neue Folge(n)"
            )
            await _enqueue(chat_id, qmsg, process_full_series,
                {'type': 'series', 'url': item['url'], 'is_anime': False},
                item['url'], False)
            series_queued += 1

        await msg.edit_text(
            "✅ HDFilme Latest geprüft.\n\n"
            f"🎬 Filme: {film_found} gefunden, {film_existing} schon vorhanden, {film_queued} in Queue\n"
            f"📺 Serien: {series_found} gefunden, {series_complete} komplett, {series_queued} in Queue"
        )
    except Exception as e:
        tb = traceback.format_exc()[-1500:]
        logging.error(f"cmd_hdfilme_latest Fehler:\n{traceback.format_exc()}")
        await msg.edit_text(f"❌ HDFilme-Check Fehler: {e}\n\n```\n{tb}\n```", parse_mode='Markdown')


async def cmd_filmpalast_import(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("🔍 Prüfe Filmpalast: neue Filme und Folgen...")

    try:
        items = await get_filmpalast_latest()
        if not items:
            await msg.edit_text("❌ Keine Filmpalast-Import-Einträge gefunden.")
            return

        film_found = film_existing = film_queued = 0
        series_found = series_existing = series_queued = 0
        errors = 0

        for item in items:
            try:
                if item['type'] == 'film':
                    film_found += 1
                    db_title = f"{item['title']} ({item['year']})" if item.get('year') else item['title']
                    if item.get('year') and _db_exists('film', db_title, False):
                        film_existing += 1
                        continue

                    qmsg = await update.message.reply_text(f"📥 Filmpalast Film in Queue: {item['title']}")
                    await _enqueue(chat_id, qmsg, process_video,
                        {'type': 'video', 'url': item['url'], 'content_type': 'filmpalast', 'manual_title': None, 'is_anime': False},
                        item['url'], 'filmpalast', None, False, "", True)
                    film_queued += 1
                    continue

                series_found += 1
                episode_title = format_filename(_normalize_filmpalast_title(item['title']))
                if _db_exists('serie', episode_title, False):
                    series_existing += 1
                    continue

                qmsg = await update.message.reply_text(f"📥 Filmpalast Folge in Queue: {episode_title}")
                await _enqueue(chat_id, qmsg, process_video,
                    {'type': 'video', 'url': item['url'], 'content_type': 'filmpalast', 'manual_title': episode_title, 'is_anime': False},
                    item['url'], 'filmpalast', episode_title, False, "", True)
                series_queued += 1
            except Exception:
                errors += 1
                logging.error(f"Filmpalast Import Eintrag fehlgeschlagen ({item.get('url')}):\n{traceback.format_exc()}")

        text = (
            "✅ Filmpalast Import geprüft.\n\n"
            f"🎬 Filme: {film_found} gefunden, {film_existing} schon vorhanden, {film_queued} in Queue\n"
            f"📺 Folgen: {series_found} gefunden, {series_existing} schon vorhanden, {series_queued} in Queue"
        )
        if errors:
            text += f"\n⚠️ Fehler bei Einträgen: {errors}"
        await msg.edit_text(text)
    except Exception as e:
        tb = traceback.format_exc()[-1500:]
        logging.error(f"cmd_filmpalast_import Fehler:\n{traceback.format_exc()}")
        await msg.edit_text(f"❌ Filmpalast-Check Fehler: {e}\n\n```\n{tb}\n```", parse_mode='Markdown')


async def handle_cancel_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    item_id = int(query.data[len("cancel_queue_"):])

    for item in list(_dl_pending):
        if id(item) == item_id:
            if item is _dl_active:
                # Laufenden Prozess killen
                if _active_proc and _active_proc.returncode is None:
                    try:
                        _active_proc.kill()
                        logging.info("Aktiver Download abgebrochen (kill)")
                    except Exception:
                        pass
                item['cancelled'] = True
                _dl_pending.remove(item)
                _persist_queue()
                try:
                    await item['msg'].edit_text("❌ Download abgebrochen.")
                except Exception:
                    pass
                await query.edit_message_text("✅ Aktiver Download abgebrochen.")
                return
            item['cancelled'] = True
            _dl_pending.remove(item)
            _persist_queue()
            try:
                await item['msg'].edit_text("❌ Aus Warteschlange entfernt.")
            except Exception:
                pass
            await query.edit_message_text("✅ Eintrag aus Warteschlange entfernt.")
            # Positionen der restlichen Items aktualisieren
            for i, pending in enumerate(_dl_pending, 1):
                try:
                    await pending['msg'].edit_text(
                        f"⏳ Warteschlange: Platz {i} von {len(_dl_pending)}\nWarte auf aktiven Download..."
                    )
                except Exception:
                    pass
            return

    await query.answer("❓ Eintrag nicht mehr vorhanden.", show_alert=True)


async def handle_reupload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    uid  = data[len("reupload_yes_"):] if data.startswith("reupload_yes_") else data[len("reupload_no_"):]
    _reupload_choices[uid] = data.startswith("reupload_yes_")
    event = _reupload_events.get(uid)
    if event:
        event.set()
    label = "✅ Wird überschrieben..." if _reupload_choices.get(uid) else "⏭ Übersprungen."
    try:
        await query.edit_message_text(label)
    except Exception:
        pass


async def handle_type_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    content_type = query.data.replace("type_", "")
    url = context.user_data.get('pending_url')
    manual_title = context.user_data.get('manual_title')
    chat_id = update.effective_chat.id

    if not url:
        await query.edit_message_text("❌ Kein Link gefunden.")
        return

    if content_type == 'bulk_films':
        print(f"[BULK] Button geklickt, URL: {url}", flush=True)
        msg = await query.edit_message_text("🎬 Lade Filmliste...")
        asyncio.create_task(_enqueue(chat_id, msg, process_bulk_films,
            {'type': 'bulk_films', 'url': url},
            url))
        context.user_data.pop('pending_url', None)
        context.user_data.pop('manual_title', None)
        return

    if content_type in ('serie', 'anime'):
        is_anime = (content_type == 'anime')
        msg = await query.edit_message_text("🎌 Lese Seite..." if is_anime else "📺 Lese Seite...")
        if 'hdfilme.win' in url or '/episode-' not in url:
            asyncio.create_task(_enqueue(chat_id, msg, process_full_series,
                {'type': 'series', 'url': url, 'is_anime': is_anime},
                url, is_anime))
        else:
            asyncio.create_task(_enqueue(chat_id, msg, process_video,
                {'type': 'video', 'url': url, 'content_type': 'serie', 'manual_title': manual_title, 'is_anime': is_anime},
                url, 'serie', manual_title, is_anime))
        context.user_data.pop('pending_url', None)
        context.user_data.pop('manual_title', None)
        return

    msg = await query.edit_message_text("🎬 Lese Seite...")
    asyncio.create_task(_enqueue(chat_id, msg, process_video,
        {'type': 'video', 'url': url, 'content_type': content_type, 'manual_title': manual_title, 'is_anime': False},
        url, content_type, manual_title))

    context.user_data.pop('pending_url', None)
    context.user_data.pop('manual_title', None)


async def handle_serie_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    is_anime = query.data == 'serie_type_anime'
    url = context.user_data.get('pending_url')
    manual_title = context.user_data.get('manual_title')

    if not url:
        await query.edit_message_text("❌ Kein Link gefunden.")
        return

    msg = await query.edit_message_text("📺 Lese Seite...")

    chat_id = update.effective_chat.id
    if 'hdfilme.win' in url or '/episode-' not in url:
        asyncio.create_task(_enqueue(chat_id, msg, process_full_series,
            {'type': 'series', 'url': url, 'is_anime': is_anime},
            url, is_anime))
    else:
        ep_title = (manual_title or "") + (" anime" if is_anime else "")
        asyncio.create_task(_enqueue(chat_id, msg, process_video,
            {'type': 'video', 'url': url, 'content_type': 'serie', 'manual_title': ep_title.strip() or None, 'is_anime': is_anime},
            url, 'serie', ep_title.strip() or None))

    context.user_data.pop('pending_url', None)
    context.user_data.pop('manual_title', None)


# ─── Status Command ──────────────────────────────

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt den aktuellen Download/Upload-Status."""
    import time as _time

    if not _dl_active and not _dl_status:
        await update.message.reply_text("✅ Nichts aktiv – Bot ist idle.")
        return

    lines = []

    # Aktiver Task
    if _dl_status:
        filename = _dl_status.get('filename', '?')
        phase    = _dl_status.get('phase', '?')
        size_mb  = _dl_status.get('size_mb', 0)
        elapsed  = _dl_status.get('elapsed', 0)
        m, s     = divmod(int(elapsed), 60)

        phase_icon = "📥" if phase == 'download' else "📤"
        phase_text = "Lade herunter" if phase == 'download' else "Uploade"

        bulk_cur = _dl_status.get('bulk_current')
        bulk_tot = _dl_status.get('bulk_total')
        if bulk_cur and bulk_tot:
            lines.append(f"📦 *Bulk: {bulk_cur}/{bulk_tot} Filme*")

        lines.append(f"{phase_icon} *{phase_text}*")
        lines.append(f"📄 `{filename}`")
        lines.append(f"💾 {size_mb:.0f} MB")
        lines.append(f"⏱ {m:02d}:{s:02d}")
    elif _dl_active:
        label = _queue_item_label(_dl_active.get('persist', {}))
        lines.append(f"▶️ *Aktiv:* {label}")

    # Warteschlange
    waiting = [i for i in _dl_pending if i is not _dl_active]
    if waiting:
        lines.append(f"\n⏳ *Warteschlange:* {len(waiting)} weitere")
        for i, item in enumerate(waiting, 1):
            lines.append(f"  {i}. {_queue_item_label(item.get('persist', {}))}")

    await update.message.reply_text("\n".join(lines), parse_mode='Markdown')


# ─── Manuelle Serienliste ────────────────────────

def _sto_parse_html(html: str, base: str) -> dict:
    """Parst die s.to Hauptseite → Titel + Staffel-Nummern."""
    soup = BeautifulSoup(html, 'html.parser')
    title_tag = soup.find('title')
    raw_title = title_tag.text if title_tag else 'Serie'
    title = re.sub(r'\s*(Staffel\s*\d+\s*\|.*|S\.to.*)$', '', raw_title, flags=re.IGNORECASE).strip()

    season_nums = sorted(set(
        int(a['data-season-pill'])
        for a in soup.find_all('a', attrs={'data-season-pill': True})
        if int(a['data-season-pill']) > 0
    ))
    # Fallback: via href-Pattern
    if not season_nums:
        season_nums = sorted({
            int(m.group(1))
            for a in soup.find_all('a', href=True)
            for m in [re.search(r'/staffel-(\d+)$', a['href'])]
            if m and int(m.group(1)) > 0
        })
    return {'title': title, 'base_url': base, 'season_nums': season_nums}


def _sto_scrape(series_url: str) -> dict:
    """Scrapt serienstream.to: Serientitel + Staffeln mit Episodenanzahl.
    Erst per requests, bei DDoS-Guard-Block oder leerer Antwort → Playwright-Fallback."""
    headers = {'User-Agent': USER_AGENT}

    base = _normalize_seriesstream_url(series_url).rstrip('/')
    if '/staffel-' in base:
        base = base[:base.index('/staffel-')]

    def _seasons_via_requests() -> dict | None:
        try:
            r = requests.get(base, headers=headers, timeout=15)
            data = _sto_parse_html(r.text, base)
            if not data['season_nums']:
                return None
            seasons = {}
            for s in data['season_nums']:
                rs = requests.get(f"{base}/staffel-{s}", headers=headers, timeout=15)
                ss = BeautifulSoup(rs.text, 'html.parser')
                ep_nums = set(int(m.group(1))
                              for a in ss.select('a[href*="/episode-"]')
                              if (m := re.search(r'/episode-(\d+)', a.get('href', ''))))
                seasons[s] = max(ep_nums) if ep_nums else 0
            return {'title': data['title'], 'base_url': base, 'seasons': seasons}
        except Exception as e:
            logging.warning(f"_sto_scrape (requests): {e}")
            return None

    def _seasons_via_camoufox() -> dict | None:
        """DDoS-Guard umgehen: per echtem Browser (camoufox) laden. Sync-API (läuft im Thread)."""
        try:
            from camoufox.sync_api import Camoufox as _Camoufox
            import time as _t
            with _Camoufox(headless=True) as br:
                ctx = br.new_context()
                pg = ctx.new_page()
                pg.goto(base, timeout=60000, wait_until='domcontentloaded')
                _t.sleep(2)
                home_html = pg.content()
                data = _sto_parse_html(home_html, base)
                seasons = {}
                for s in data['season_nums']:
                    pg.goto(f"{base}/staffel-{s}", timeout=60000, wait_until='domcontentloaded')
                    _t.sleep(1)
                    s_html = pg.content()
                    ss = BeautifulSoup(s_html, 'html.parser')
                    ep_nums = set(int(m.group(1))
                                  for a in ss.select('a[href*="/episode-"]')
                                  if (m := re.search(r'/episode-(\d+)', a.get('href', ''))))
                    seasons[s] = max(ep_nums) if ep_nums else 0
                br.close()
                return {'title': data['title'], 'base_url': base, 'seasons': seasons}
        except Exception as e:
            logging.warning(f"_sto_scrape (camoufox): {e}")
            return None

    result = _seasons_via_requests()
    if result and result['seasons']:
        return result

    logging.info("_sto_scrape: Fallback auf camoufox (DDoS-Guard vermutet)")
    result = _seasons_via_camoufox()
    if result:
        return result

    return {'title': 'Serie', 'base_url': base, 'seasons': {}}


async def cmd_manuel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Startet die manuelle Serieneingabe: /manuel <serienstream.to-URL>"""
    args = context.args
    if not args:
        await update.message.reply_text("❌ Bitte URL angeben: /manuel https://serienstream.to/serie/...")
        return

    url = args[0].strip()
    if not _is_seriesstream_url(url):
        await update.message.reply_text("❌ Nur serienstream.to URLs unterstützt.")
        return
    url = _normalize_seriesstream_url(url)

    msg = await update.message.reply_text("🔍 Lese Staffeln von serienstream.to...")

    try:
        data = await asyncio.get_event_loop().run_in_executor(None, _sto_scrape, url)
    except Exception as e:
        await msg.edit_text(f"❌ Fehler beim Lesen: {e}")
        return

    if not data['seasons']:
        await msg.edit_text("❌ Keine Staffeln gefunden.")
        return

    seasons = data['seasons']
    title = data['title']

    # Pro Staffel: prüfen welche Folgen schon in der DB sind
    missing_per_season = {}  # {season: [missing_ep_nums]}
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        for s, ep_count in seasons.items():
            existing = set()
            for row in cur.execute(
                "SELECT episode FROM episodes WHERE LOWER(series)=LOWER(?) AND season=?",
                (title, s)
            ):
                existing.add(row[0])
            for row in cur.execute(
                "SELECT episode FROM anime_episodes WHERE LOWER(anime)=LOWER(?) AND season=?",
                (title, s)
            ):
                existing.add(row[0])
            missing_per_season[s] = [e for e in range(1, ep_count + 1) if e not in existing]
        con.close()
    except Exception as e:
        logging.warning(f"DB-Check Fehler in /manuel: {e}")
        missing_per_season = {s: list(range(1, n + 1)) for s, n in seasons.items()}

    # Schöne Anzeige: zeigt vorhandene/fehlende
    def _fmt_missing(missing: list[int], total: int) -> str:
        if not missing:
            return "✅ komplett"
        if len(missing) == total:
            return f"❌ keine vorhanden"
        return f"⚠️ fehlt: {', '.join(str(e) for e in missing)}"

    season_list = '\n'.join(
        f"  Staffel {s}: {ep_count} Folgen – {_fmt_missing(missing_per_season[s], ep_count)}"
        for s, ep_count in seasons.items()
    )

    context.user_data['manuel'] = {
        'title': title,
        'base_url': data['base_url'],
        'seasons': seasons,
        'missing_per_season': missing_per_season,
        'state': 'waiting_season',
    }

    await msg.edit_text(
        f"📺 *{title}*\n\n{season_list}\n\n"
        f"Mit welcher Staffel wollen wir anfangen?\n"
        f"_Tipp: Eingabe nur Staffel-Nr (z.B. `2`) → nimmt nur die fehlenden Folgen.\n"
        f"Mit Spec wie `2 3,5-8` werden genau diese verlangt._",
        parse_mode='Markdown'
    )


def _parse_episode_spec(spec: str, max_ep: int) -> list[int]:
    """Parst '1,3,5-8' → [1,3,5,6,7,8]. Leerer String → alle Folgen 1..max_ep."""
    spec = spec.strip()
    if not spec:
        return list(range(1, max_ep + 1))
    result = []
    for part in re.split(r'[,\s]+', spec):
        if not part:
            continue
        if '-' in part:
            a, b = part.split('-', 1)
            try:
                a, b = int(a), int(b)
                if a > b:
                    a, b = b, a
                result.extend(range(a, b + 1))
            except ValueError:
                continue
        else:
            try:
                result.append(int(part))
            except ValueError:
                continue
    # eindeutig, sortiert, im Bereich
    return sorted({e for e in result if 1 <= e <= max_ep})


async def _manuel_handle_season(update: Update, context: ContextTypes.DEFAULT_TYPE, manuel: dict):
    """Verarbeitet die Staffel-Antwort.
    Akzeptiert auch Folgen-Spec: '2 3,5,7-9' = Staffel 2, Folgen 3,5,7,8,9
    oder 'Staffel 2 nur 3,5' usw."""
    text = update.message.text.strip()
    m = re.search(r'\d+', text)
    if not m:
        await update.message.reply_text("❓ Bitte Staffelnummer eingeben (z.B. 2 oder Staffel 2).")
        return

    season = int(m.group())
    # Rest nach der ersten Zahl = Episoden-Spec
    rest = text[m.end():].strip()
    seasons = manuel['seasons']

    if season not in seasons:
        valid = ', '.join(str(s) for s in seasons)
        await update.message.reply_text(f"❌ Staffel {season} nicht vorhanden. Verfügbar: {valid}")
        return

    ep_count = seasons[season]
    missing_per_season = manuel.get('missing_per_season', {})
    missing = missing_per_season.get(season, list(range(1, ep_count + 1)))

    if not rest:
        # Kein Spec → automatisch nur die fehlenden Folgen
        episodes = missing
        if not episodes:
            context.user_data.pop('manuel', None)
            await update.message.reply_text(
                f"✅ Staffel {season} ist komplett – nichts zu tun."
            )
            return
        mode_hint = f" (nur fehlende Folgen)"
    else:
        episodes = _parse_episode_spec(rest, ep_count)
        mode_hint = ""
        if not episodes:
            await update.message.reply_text(
                f"❌ Keine gültigen Folgen erkannt. Beispiele:\n"
                f"• `{season}` – alle fehlenden Folgen\n"
                f"• `{season} 3,5,7` – nur Folge 3, 5, 7\n"
                f"• `{season} 5-8` – Folgen 5 bis 8\n"
                f"• `{season} all` – alle Folgen 1-{ep_count}",
                parse_mode='Markdown'
            )
            return
        # Sonderfall: 'all' → wirklich alle Folgen, nicht nur fehlende
        if rest.lower().strip() == 'all':
            episodes = list(range(1, ep_count + 1))
            mode_hint = " (alle Folgen erzwungen)"

    manuel['state'] = 'waiting_episode'
    manuel['season'] = season
    manuel['episodes_todo'] = episodes  # Liste der zu fragenden Folgen
    manuel['todo_idx'] = 0
    manuel['ep_count'] = ep_count

    spec_text = f"{len(episodes)} Folge(n): {', '.join(str(e) for e in episodes)}"
    first_ep = episodes[0]
    await update.message.reply_text(
        f"✅ Staffel {season}{mode_hint} – {spec_text}\n\n"
        f"🔗 Gib den VOE-Link für *S{season:02d}E{first_ep:02d}*:",
        parse_mode='Markdown'
    )


async def _manuel_handle_episode(update: Update, context: ContextTypes.DEFAULT_TYPE, manuel: dict):
    """Sammelt VOE-Links für die angegebenen Folgen und startet alle Downloads am Ende."""
    voe_url = update.message.text.strip()
    season = manuel['season']
    episodes_todo = manuel['episodes_todo']
    idx = manuel['todo_idx']
    current_ep = episodes_todo[idx]
    title = manuel['title']

    if not voe_url.startswith('http'):
        await update.message.reply_text("❌ Kein gültiger Link. Bitte VOE-URL eingeben.")
        return

    if 'links' not in manuel:
        manuel['links'] = []
    manuel['links'].append((current_ep, voe_url))

    next_idx = idx + 1
    if next_idx >= len(episodes_todo):
        # Alle Links gesammelt → in die Queue (sequenziell)
        context.user_data.pop('manuel', None)
        n = len(manuel['links'])
        await update.message.reply_text(f"✅ Alle {n} Links erhalten – stelle in Warteschlange...")
        chat_id = update.effective_chat.id
        for ep_num, url in manuel['links']:
            ep_name = f"{title} S{season:02d}E{ep_num:02d}"
            ep_msg = await update.message.reply_text(f"📥 {ep_name}")
            await _enqueue(chat_id, ep_msg, process_video,
                {'type': 'video', 'url': url, 'content_type': 'serie', 'manual_title': ep_name, 'is_anime': False},
                url, 'serie', ep_name)
    else:
        manuel['todo_idx'] = next_idx
        next_ep = episodes_todo[next_idx]
        remaining = len(episodes_todo) - next_idx
        await update.message.reply_text(
            f"✅ S{season:02d}E{current_ep:02d} gespeichert ({remaining} verbleiben).\n"
            f"🔗 Gib den VOE-Link für *S{season:02d}E{next_ep:02d}*:",
            parse_mode='Markdown'
        )


async def _kinoger_handle_season(update: Update, context: ContextTypes.DEFAULT_TYPE, kg: dict):
    """User hat Staffel angegeben für Kinoger-Serie. Episoden auflösen → in Queue."""
    text = update.message.text.strip()
    m = re.search(r'\d+', text)
    if not m:
        await update.message.reply_text("❓ Bitte Staffelnummer eingeben (z.B. 2).")
        return

    season = int(m.group())
    rest = text[m.end():].strip()
    seasons_urls = kg['seasons_urls']
    if season not in seasons_urls:
        valid = ', '.join(str(s) for s in seasons_urls)
        await update.message.reply_text(f"❌ Staffel {season} nicht vorhanden. Verfügbar: {valid}")
        return

    urls = seasons_urls[season]
    ep_count = len(urls)
    missing = kg.get('missing_per_season', {}).get(season, list(range(1, ep_count + 1)))

    if not rest:
        episodes = missing
        if not episodes:
            context.user_data.pop('kinoger', None)
            await update.message.reply_text(f"✅ Staffel {season} ist komplett – nichts zu tun.")
            return
        mode_hint = " (nur fehlende Folgen)"
    elif rest.lower().strip() == 'all':
        episodes = list(range(1, ep_count + 1))
        mode_hint = " (alle Folgen erzwungen)"
    else:
        episodes = _parse_episode_spec(rest, ep_count)
        mode_hint = ""
        if not episodes:
            await update.message.reply_text(
                f"❌ Keine gültigen Folgen. Beispiele: `{season}`, `{season} 3,5,7`, `{season} 5-8`, `{season} all`",
                parse_mode='Markdown'
            )
            return

    title = kg['title']
    context.user_data.pop('kinoger', None)

    spec_text = f"{len(episodes)} Folge(n): {', '.join(str(e) for e in episodes)}"
    await update.message.reply_text(
        f"✅ Staffel {season}{mode_hint} – {spec_text}\n\n"
        f"🔍 Prüfe Links und stelle Folgen in die Queue...",
        parse_mode='Markdown'
    )

    # Pro Episode: Embed in Queue. Die kurzlebige Browser-Media-URL wird direkt vor dem Download aufgelöst.
    chat_id = update.effective_chat.id
    loop = asyncio.get_event_loop()
    queued = 0
    for ep_num in episodes:
        embed_url = urls[ep_num - 1]
        ep_name = f"{title} S{season:02d}E{ep_num:02d}"
        if 'voe.sx/e/' in embed_url and not await loop.run_in_executor(None, _voe_link_available, embed_url):
            await update.message.reply_text(f"⚠️ {ep_name}: VOE-Link ist offline/404, übersprungen")
            continue
        ep_msg = await update.message.reply_text(f"📥 In Queue: {ep_name}")
        if 'voe.sx/e/' in embed_url:
            await _enqueue(chat_id, ep_msg, process_video,
                {'type': 'video', 'url': embed_url, 'content_type': 'serie', 'manual_title': ep_name, 'is_anime': False},
                embed_url, 'serie', ep_name, False)
        else:
            await _enqueue(chat_id, ep_msg, process_kinoger_direct,
                {'type': 'kinoger_direct', 'url': embed_url, 'manual_title': ep_name, 'db_content_type': 'serie'},
                embed_url, ep_name, 'serie', False)
        queued += 1

    await update.message.reply_text(f"✅ {queued}/{len(episodes)} Folgen in der Warteschlange.")


async def handle_manuel_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Prüft ob wir in einem manuellen Modus sind. Gibt True zurück wenn ja."""
    # Kinoger-Modus (Staffel-Wahl)
    kg = context.user_data.get('kinoger')
    if kg and kg.get('state') == 'waiting_season':
        await _kinoger_handle_season(update, context, kg)
        return True

    manuel = context.user_data.get('manuel')
    if not manuel:
        return False

    state = manuel.get('state')
    if state == 'waiting_season':
        await _manuel_handle_season(update, context, manuel)
        return True
    elif state == 'waiting_episode':
        await _manuel_handle_episode(update, context, manuel)
        return True

    return False


def _polling_error_callback(exc):
    """Kurzes Logging für Telegram-Polling-Netzwerkfehler.

    python-telegram-bot ruft diesen Callback synchron auf. Keine async-Aufrufe
    hier verwenden.
    """
    logging.warning(f"Telegram Polling Fehler: {exc.__class__.__name__}: {exc}")


async def _polling_watchdog(app: Application):
    """Startet Telegram-Polling und startet es neu, falls der interne Task stirbt."""
    while True:
        try:
            if not app.updater.running:
                logging.info("Telegram Polling wird gestartet")
                await app.updater.start_polling(
                    timeout=30,
                    bootstrap_retries=-1,
                    read_timeout=35,
                    write_timeout=30,
                    connect_timeout=30,
                    pool_timeout=30,
                    error_callback=_polling_error_callback,
                )

            await asyncio.sleep(30)

            polling_task = getattr(app.updater, "_Updater__polling_task", None)
            if polling_task and polling_task.done():
                try:
                    polling_task.result()
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logging.error(f"Telegram Polling Task beendet:\n{traceback.format_exc()}")

                try:
                    if app.updater.running:
                        await app.updater.stop()
                except Exception:
                    logging.warning(f"Telegram Polling Stop fehlgeschlagen:\n{traceback.format_exc()}")
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.error(f"Telegram Polling Watchdog Fehler:\n{traceback.format_exc()}")
            await asyncio.sleep(15)


async def _enforce_chat_allowlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stoppt sämtliche Commands, Nachrichten und Buttons fremder Chats."""
    chat = update.effective_chat
    if chat is None or chat.id in ALLOWED_CHAT_IDS:
        return
    logging.warning("Telegram-Update von nicht freigegebenem Chat verworfen")
    raise ApplicationHandlerStop


async def _restore_queue_job(app: Application, item_data: dict) -> None:
    chat_id = item_data.get('chat_id')
    if not chat_id:
        return
    msg = _LazyTelegramMessage(app.bot, chat_id)
    common = dict(notify_position=False, persist_now=False)
    if item_data['type'] == 'series':
        completed = _series_progress.get(item_data['url'], 0)
        await _enqueue(chat_id, msg, process_full_series, item_data,
                       item_data['url'], item_data.get('is_anime', False), completed, **common)
    elif item_data['type'] == 'bulk_films':
        completed = _series_progress.get(item_data['url'], 0)
        await _enqueue(chat_id, msg, process_bulk_films, item_data,
                       item_data['url'], completed, **common)
    elif item_data['type'] == 'kinoger_direct':
        await _enqueue(chat_id, msg, process_kinoger_direct, item_data,
                       item_data['url'], item_data['manual_title'],
                       item_data.get('db_content_type', 'film'),
                       item_data.get('is_anime', False), **common)
    else:
        await _enqueue(chat_id, msg, process_video, item_data,
                       item_data['url'], item_data['content_type'],
                       item_data.get('manual_title'), item_data.get('is_anime', False), **common)


async def _central_queue_worker(app: Application) -> None:
    """Reserviert atomar genau einen PostgreSQL-Job für diesen Rechner."""
    poll_seconds = max(1, int(os.getenv('CENTRAL_QUEUE_POLL_SECONDS', '5')))
    while True:
        try:
            if not _dl_pending and _dl_active is None:
                claimed = await asyncio.to_thread(
                    _central_queue.claim, CENTRAL_QUEUE_WORKER_ID
                )
                if claimed:
                    payload = dict(claimed['payload'])
                    payload['_central_job_id'] = claimed['id']
                    payload['_central_attempts'] = claimed['attempts']
                    logging.info(
                        "Zentralen Queue-Job #%s reserviert (Versuch %s)",
                        claimed['id'], claimed['attempts'],
                    )
                    await _restore_queue_job(app, payload)
                    continue
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.error("Central-Queue-Polling Fehler:\n%s", traceback.format_exc())
        await asyncio.sleep(poll_seconds)
# ─── Main ────────────────────────────────────────

async def main():
    print("[DEBUG] main() gestartet")
    _db_init()
    if _central_queue:
        await asyncio.to_thread(_central_queue.init)
        logging.info("Zentrale PostgreSQL-Queue aktiv: Rolle=%s", CENTRAL_QUEUE_ROLE)
    print("[DEBUG] DB initialisiert")
    _series_progress.update(_load_progress())
    _watchlist.extend(_load_watchlist())
    print("[DEBUG] Progress geladen")

    print("[DEBUG] Starte Telethon tg_client.start()...")
    await tg_client.start()
    print("✅ Telethon verbunden")

    # Großzügige Timeouts für die Bot-API (api.telegram.org) – hilft besonders
    # auf Remote-Desktop/Servern mit langsamer HTTP-Route zu Telegram. Der
    # MTProto-Upload (Telethon) ist davon unabhängig und nutzt DC4 direkt.
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(30.0)   # TCP-Connect zu api.telegram.org
        .read_timeout(30.0)      # Antwort-Lesen
        .write_timeout(30.0)     # Request-Body-Senden
        .pool_timeout(30.0)      # Warten auf freien HTTP-Pool-Slot
        .build()
    )
    # Gruppe -1 läuft zwingend vor allen normalen Handlern. Ein fremder Chat
    # erreicht dadurch weder Link-Queue noch Import-, Watchcheck- oder Buttons.
    app.add_handler(TypeHandler(Update, _enforce_chat_allowlist), group=-1)
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("queue", cmd_queue))
    app.add_handler(CommandHandler("watchcheck", cmd_watchcheck))
    app.add_handler(CommandHandler("megakino_last_added", cmd_megakino_last_added))
    app.add_handler(CommandHandler("hdfilme_latest", cmd_hdfilme_latest))
    app.add_handler(CommandHandler("filmpalast_import", cmd_filmpalast_import))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("manuel", cmd_manuel))
    app.add_handler(CommandHandler("centralserie", cmd_centralserie))
    app.add_handler(CommandHandler("remotequeue_download", cmd_remote_queue_download))
    app.add_handler(CommandHandler("remotequeue_clear", cmd_clear_remote_queue))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_handler(CallbackQueryHandler(handle_type_selection, pattern="^type_"))
    app.add_handler(CallbackQueryHandler(handle_serie_type, pattern="^serie_type_"))
    app.add_handler(CallbackQueryHandler(handle_reupload, pattern="^reupload_"))
    app.add_handler(CallbackQueryHandler(handle_cancel_queue, pattern="^cancel_queue_"))
    app.add_handler(CallbackQueryHandler(handle_clear_remote_queue, pattern="^clear_remote_queue_"))
    app.add_handler(CallbackQueryHandler(handle_central_type, pattern="^central_type_"))

    print("🤖 Bot läuft!")
    print("Link senden → Film / Serie / Anime wählen → fertig")
    async with app:
        await app.start()
        await app.bot.set_my_commands([
            BotCommand("start", "Hauptmenü"),
            BotCommand("queue", "📋 Warteschlange anzeigen & verwalten"),
            BotCommand("watchcheck", "🔍 Watchlist jetzt prüfen"),
            BotCommand("megakino_last_added", "🆕 Megakino Zuletzt hinzugefügt"),
            BotCommand("hdfilme_latest", "🆕 HDFilme neueste Filme/Serien"),
            BotCommand("filmpalast_import", "🆕 Filmpalast neue Filme/Folgen"),
            BotCommand("status", "📊 Aktueller Download-Status"),
            BotCommand("manuel", "✍️ Manuelle Serieneingabe (VOE-Links)"),
            BotCommand("centralserie", "☁️ Serie komplett in zentrale Queue"),
            BotCommand("remotequeue_download", "▶️ Remote Queue mit Chromium starten"),
            BotCommand("remotequeue_clear", "🗑 Wartende Remote Queue leeren"),
        ])
        _spawn_task(_dl_worker(), "download-worker")

        saved = _load_queue() if CENTRAL_QUEUE_ROLE == 'local' else []
        if saved:
            logging.info(f"Queue-Wiederherstellung: {len(saved)} lokale Einträge")
        for item_data in saved:
            try:
                await _restore_queue_job(app, item_data)
            except Exception as e:
                logging.error(f"Queue-Wiederherstellung Fehler: {e}")

        if CENTRAL_QUEUE_ROLE == 'worker' and CENTRAL_QUEUE_AUTOSTART:
            global _central_worker_task
            _central_worker_task = _spawn_task(
                _central_queue_worker(app), "central-queue-worker"
            )
        elif CENTRAL_QUEUE_ROLE == 'worker':
            logging.info(
                "Remote Queue wartet auf Button oder /remotequeue_download"
            )

        if RUN_TELEGRAM_POLLING:
            _spawn_task(_polling_watchdog(app), "telegram-polling-watchdog")
        else:
            logging.info("Worker-Modus: Telegram-Polling ist deaktiviert")
        await asyncio.Event().wait()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
