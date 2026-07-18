#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

info() { printf '\n==> %s\n' "$1"; }
fail() { printf '\nFEHLER: %s\n' "$1" >&2; exit 1; }

[[ "$(uname -s)" == "Darwin" ]] || fail "Dieses Skript ist nur für macOS gedacht."

if ! command -v brew >/dev/null 2>&1; then
  fail "Homebrew fehlt. Installiere es zuerst von https://brew.sh und starte das Skript erneut."
fi

info "Homebrew aktualisieren"
brew update

if ! brew list --versions python@3.12 >/dev/null 2>&1; then
  info "Python 3.12 installieren"
  brew install python@3.12
else
  info "Python 3.12 ist bereits installiert"
fi

if ! brew list --versions ffmpeg >/dev/null 2>&1; then
  info "FFmpeg und ffprobe installieren"
  brew install ffmpeg
else
  info "FFmpeg ist bereits installiert"
fi

PYTHON="$(brew --prefix python@3.12)/bin/python3.12"
[[ -x "$PYTHON" ]] || fail "Python 3.12 wurde nicht unter dem erwarteten Homebrew-Pfad gefunden."

if [[ -d venv ]]; then
  VENV_VERSION="$(./venv/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || true)"
  [[ "$VENV_VERSION" == "3.12" ]] || fail "Die bestehende venv verwendet Python ${VENV_VERSION:-unbekannt}. Bitte umbenennen und erneut starten."
  info "Vorhandene Python-3.12-Umgebung verwenden"
else
  info "Virtuelle Python-Umgebung erstellen"
  "$PYTHON" -m venv venv
fi

info "Python-Werkzeuge und Bot-Abhängigkeiten installieren"
./venv/bin/python -m pip install --upgrade pip setuptools wheel
./venv/bin/python -m pip install --requirement requirements.txt

info "Playwright Chromium installieren"
./venv/bin/playwright install chromium

if [[ ! -f .env ]]; then
  cp .env.example .env
  chmod 600 .env
  ENV_STATUS=".env wurde aus der Vorlage erstellt und muss ausgefüllt werden."
else
  ENV_STATUS="Vorhandene .env wurde unverändert beibehalten."
fi

info "Installation prüfen"
./venv/bin/python -m pip check
./venv/bin/python -m py_compile bot.py central_media_db.py create_session.py
ffmpeg -version >/dev/null
ffprobe -version >/dev/null
./venv/bin/yt-dlp --version >/dev/null

printf '\n✅ Installation erfolgreich.\n%s\n' "$ENV_STATUS"
printf '%s\n' "Danach bei Bedarf eine eigene Telethon-Session erstellen:"
printf '%s\n' "  ./venv/bin/python create_session.py"
printf '%s\n' "Bot starten:"
printf '%s\n' "  ./venv/bin/python bot.py"
