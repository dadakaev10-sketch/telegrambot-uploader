# Telegrambot project guide

## Project overview

- The main application is `bot.py` in this directory.
- Runtime state is stored under `bot/`:
  - `bot/queue.json` contains persisted download jobs.
  - `bot/series_progress.json` contains resumable series progress.
  - `bot/watchlist.json` contains monitored sources and Telegram chat IDs.
- `media.db` is the authoritative local media database.
- Use the project virtual environment: `./venv/bin/python`.
- The bot may already be running and processing a long queue. Check the process and queue before restarting it.

## SerienStream domain handling

- The current site is `https://serienstream.to`.
- `s.to` no longer resolves reliably. Old `s.to` URLs from queues, progress files, or watchlists must be normalized to `serienstream.to`.
- URL recognition and normalization live in `_is_seriesstream_url()` and `_normalize_seriesstream_url()` in `bot.py`.
- Never detect the site with a loose substring check such as `'s.to' in url`; parse and compare the hostname.

## Crawling a series

The series overview and season pages currently return usable static HTML through `requests`.

1. Normalize the supplied URL to `serienstream.to`.
2. Remove `/staffel-N` and `/episode-N` to obtain the series base URL.
3. Read the title from `<title>` and remove the `Staffel N | S.to` suffix.
4. Read seasons from `a[data-season-pill]` and exclude season `0`.
5. Request each `/staffel-N` page.
6. Read episode URLs from `a[href*="/episode-"]`.
7. Deduplicate episodes by `(season, episode)` because links can occur more than once in the page.
8. Keep the canonical title format `Series Name S01E01`.

The implemented entry point is `_fetch_seriesstream_series()` in `bot.py`. Verify its output with both a current `serienstream.to` URL and an old `s.to` URL.

## ALTCHA and VOE extraction

SerienStream protects the player redirect with ALTCHA. In the observed workflow, a new CAPTCHA is commonly required about every second episode.

Relevant DOM contracts:

- German VOE button: `.link-box[data-provider-name="VOE"][data-language-id="1"]`
- Player iframe: `#player-iframe`
- Preparation modal: `#playerPrepareModal`
- ALTCHA checkbox: `altcha-widget input[type="checkbox"]`
- Verified state: `altcha-widget .altcha[data-state="verified"]`
- Continue button: `#playerPrepareModal button[type="submit"]`

Important limitations:

- The normal standalone Playwright instance used by `bot.py` has not reliably completed ALTCHA, even in headed mode. Do not claim that automatic CAPTCHA handling works unless a fresh end-to-end test proves it.
- Codex Browser Use with the in-app browser completed ALTCHA successfully. When Browser Use is needed, read and follow its skill instructions first.
- Browser Use requires explicit user confirmation before solving each CAPTCHA. Do not click a CAPTCHA without that confirmation.
- A successful CAPTCHA submission navigates the player iframe to a VOE embed URL. The VOE hostname rotates; do not hardcode `voe.sx` or the currently observed mirror hostname.
- Treat a valid player target as an iframe URL containing `/e/` whose hostname is not a SerienStream hostname.

Recommended Browser Use procedure:

1. Open one episode page in a persistent in-app browser tab.
2. Inspect a fresh DOM snapshot.
3. If `#playerPrepareModal` is visible, confirm permission with the user, click the unique ALTCHA checkbox, and verify `data-state="verified"`.
4. Click the unique `Weiter` submit button.
5. Read the final URL from the document loaded inside `#player-iframe`.
6. Reuse the same browser tab/session for subsequent episodes.
7. Expect another CAPTCHA roughly every second episode and handle it only when the modal is actually visible.
8. Store or queue only the final VOE embed URL, not the encrypted `/r?t=...` URL, CSRF token, CAPTCHA payload, or temporary CDN stream URL.

## Adding extracted episodes to the queue

For a final VOE embed URL, persist a video job with this shape:

```json
{
  "type": "video",
  "url": "https://<rotating-voe-host>/e/<id>",
  "content_type": "serie",
  "manual_title": "Series Name S01E01",
  "is_anime": false,
  "chat_id": 123456789
}
```

Queue rules:

- Preserve existing entries in `bot/queue.json`; never overwrite an active queue accidentally.
- Obtain the correct existing `chat_id` from project state. Never expose chat IDs, bot tokens, cookies, CAPTCHA payloads, or signed CDN URLs in user-facing output.
- Check `media.db` first and queue only missing episodes unless the user explicitly asks to re-upload existing ones.
- Direct VOE embed URLs bypass the SerienStream CAPTCHA during the actual download.
- The bot restores `bot/queue.json` only at startup. Editing the file does not inject jobs into an already running process.

## Starting and monitoring the bot

- First check for an existing instance: `ps aux | rg '[p]ython.*bot\.py'`.
- Do not run two polling instances with the same Telegram bot token; Telegram will report conflicting `getUpdates` requests.
- Validate code before restart: `./venv/bin/python -m py_compile bot.py`.
- Validate the queue with `jq` before restart.
- Start from this directory with `./venv/bin/python bot.py`.
- On startup, confirm that the log reports `Queue-Wiederherstellung: N Einträge` and `[WORKER] Starte: process_video`.
- Confirm a real stream was found before declaring success. A healthy VOE job logs an HLS or MP4 stream, downloads the file, uploads it, adds it to `media.db`, removes the completed queue entry, and starts the next job.
- Avoid restarting while an upload is active unless necessary; queue entries persist, but partially completed work may have to repeat.

## Verification checklist

- `./venv/bin/python -m py_compile bot.py`
- Series crawler returns the expected title, seasons, and episode count.
- Old `s.to` input produces canonical `serienstream.to` episode URLs.
- All queue entries have unique titles and URLs.
- A direct VOE URL reaches HLS or MP4 detection in `get_stream_url()`.
- After one completed job, the queue length decreases and the episode appears in `media.db`.
