#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

python3 -m py_compile main.py bot.py central_queue.py
venv/bin/python -m pytest -q
npx --yes pyright
git diff --check
systemctl is-active --quiet telegrambot-uploader.service
printf 'verify: OK\n'
