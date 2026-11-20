# Megakino carousel → HDFilme Mini App (local implementation)

The existing scheduled/manual `get_megakino_last_added()` browser fetch now also
extracts **the top film carousel** (`#owl-carou a.top .top__title`), not the
separate “Zuletzt hinzugefügt” list or the news carousel. No extra fetch,
download, queue job or database write is introduced by snapshot publication.
The original daily scheduling, last-added downloads and quality policy remain
unchanged. A successful source extraction is independent of later queue errors.

## Contract

Uploader writes `bot/megakino_carousel.json` atomically (version 1,
source `megakino`, UTC `updated_at`, ordered `items` containing title and URL).
Empty/unrecognized source HTML, browser failure and failed replacement leave
the old file untouched. Duplicated source URLs are removed in first-seen order.
The source URL follows the actual browser redirect, not an obsolete domain.

Main Mini App reads that absolute uploader path by default. Both sides support
`MEGAKINO_CAROUSEL_PATH` if a different explicitly shared file is required.
Main never writes it and never connects to uploader PostgreSQL. GET resolution
uses current read-only `movies` rows with nonblank Telegram `file_id`, returning
the same fields as normal catalog cards (no file IDs or source metadata).

Matching is whole normalized title (Unicode/case/punctuation/whitespace), with
an optional trailing `(YYYY)` as an explicit year constraint. No substring,
fuzzy, discovery-hint alias or newest-remake guessing. Ambiguous identities and
missing/unplayable movies are omitted. Catalog imports resolve on the next GET,
without re-extracting the source. Keep all source entries, not only 12: missing
entries may resolve later. Duplicate resolved IDs appear once. Source order
replaces the old upload/year ordering; no current-year restriction applies.
Before the first good snapshot, the existing newest-current/previous-year rail
remains the fallback. Once a snapshot is present, zero matches means an empty
rail, not unrelated fallback movies. Bad/missing reader files retain the last
valid in-process source snapshot; writer atomic replacement is the durable
last-good guarantee across process restarts. Manual deletion/corruption across
a Mini App restart cannot recover an externally destroyed snapshot.

## Activation — explicit Go required (NOT performed)

1. Review only these scoped changes; uploader already has unrelated local edits,
   and `/opt/HDFilme_bot` is not a Git checkout. Do not replace whole entrypoints.
2. Check uploader is idle/safe to restart; restart only uploader and Mini App
   after approval. The polling HDFilme bot does not require a restart.
3. Shared default parent directories are currently 0755. Writer file mode is
   0644 for Mini App UID 501. Recheck effective service path access/sandbox after
   restart; no service-unit/config modifications are required by this patch.
4. Wait for the next existing scheduled Megakino run. Do not invoke its command
   for QA: it can queue downloads. If a separate read-only seed is authorized,
   call only the pure parser/writer with fetched homepage HTML, never the check.
5. Verify the actual snapshot and signed GET `/api/featured`: IDs in source
   order, catalog details/posters/play handoff unchanged. Do not trigger playback
   or send fixture uploads to demonstrate this change.

## Offline tests

Uploader: `./venv/bin/python tests/run_offline.py /opt/workspaces/telegrambot-uploader/tests -q`.
Main guarded harness: `/root/.hermes/cache/scratch/megakino-main-offline.py`;
rejects production SQLite and all socket connections, disables dotenv loading.
Cross-repository live-HTML fixture test:
`/root/.hermes/cache/scratch/test_megakino_cross_repo.py`.
These host-local harnesses and captured HTML are scratch artifacts, not runtime
files. The source HTML was retrieved read-only; no check/upload was invoked.
