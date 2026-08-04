from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable

import psycopg


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS movies (
    title TEXT NOT NULL,
    year INTEGER,
    voe_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS movies_title_ci_year_uq
    ON movies (lower(title), coalesce(year, 0));

CREATE TABLE IF NOT EXISTS episodes (
    series TEXT NOT NULL,
    season INTEGER NOT NULL,
    episode INTEGER NOT NULL,
    voe_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (series, season, episode)
);
CREATE UNIQUE INDEX IF NOT EXISTS episodes_series_ci_uq
    ON episodes (lower(series), season, episode);

CREATE TABLE IF NOT EXISTS anime_episodes (
    anime TEXT NOT NULL,
    season INTEGER NOT NULL,
    episode INTEGER NOT NULL,
    voe_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (anime, season, episode)
);
CREATE UNIQUE INDEX IF NOT EXISTS anime_episodes_anime_ci_uq
    ON anime_episodes (lower(anime), season, episode);
"""


class CentralMediaDB:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def connect(self):
        return psycopg.connect(self.database_url, connect_timeout=10)

    def init(self) -> None:
        with self.connect() as con:
            con.execute(SCHEMA_SQL)

    @staticmethod
    def _episode_parts(filename: str):
        match = re.search(r'^(.*?)\s+S(\d{2})E(\d{2})', filename, re.I)
        if not match:
            return None
        return match.group(1).strip(), int(match.group(2)), int(match.group(3))

    @staticmethod
    def _movie_parts(filename: str):
        match = re.search(r'^(.*?)\s*\((\d{4})\)', filename)
        if not match:
            return None
        return match.group(1).strip(), int(match.group(2))

    @staticmethod
    def _normalize_name(value: str) -> str:
        value = unicodedata.normalize('NFKC', value or '').casefold()
        value = value.replace('&', ' und ')
        value = re.sub(r'[^\w]+', ' ', value, flags=re.UNICODE)
        return ' '.join(value.split())

    def exists(self, content_type: str, filename: str, is_anime: bool = False) -> bool:
        with self.connect() as con:
            if content_type == 'film':
                parts = self._movie_parts(filename)
                if not parts:
                    return False
                title, year = parts
                rows = con.execute(
                    "SELECT title FROM movies WHERE year BETWEEN %s AND %s",
                    (year - 1, year + 1),
                ).fetchall()
                normalized = self._normalize_name(title)
                return any(self._normalize_name(row[0]) == normalized for row in rows)
            parts = self._episode_parts(filename)
            if not parts:
                return False
            name, season, episode = parts
            table, column = ('anime_episodes', 'anime') if is_anime else ('episodes', 'series')
            row = con.execute(
                f"SELECT 1 FROM {table} WHERE lower({column})=lower(%s) AND season=%s AND episode=%s",
                (name, season, episode),
            ).fetchone()
            return row is not None

    def exists_batch(self, content_type: str, filenames: list[str], is_anime: bool = False) -> dict[str, bool]:
        result = {filename: False for filename in filenames}
        if not filenames:
            return result
        with self.connect() as con:
            if content_type == 'film':
                for filename in filenames:
                    parts = self._movie_parts(filename)
                    if not parts:
                        continue
                    title, year = parts
                    rows = con.execute(
                        "SELECT title FROM movies WHERE year BETWEEN %s AND %s",
                        (year - 1, year + 1),
                    ).fetchall()
                    normalized = self._normalize_name(title)
                    result[filename] = any(
                        self._normalize_name(row[0]) == normalized for row in rows
                    )
            else:
                table, column = ('anime_episodes', 'anime') if is_anime else ('episodes', 'series')
                parsed = [(filename, self._episode_parts(filename)) for filename in filenames]
                for filename, parts in parsed:
                    if not parts:
                        continue
                    name, season, episode = parts
                    result[filename] = con.execute(
                        f"SELECT 1 FROM {table} WHERE lower({column})=lower(%s) AND season=%s AND episode=%s",
                        (name, season, episode),
                    ).fetchone() is not None
        return result

    def add(self, content_type: str, filename: str, is_anime: bool = False,
            overwrite: bool = False, voe_url: str | None = None) -> None:
        with self.connect() as con:
            if content_type == 'film':
                parts = self._movie_parts(filename)
                if not parts:
                    return
                title, year = parts
                if overwrite:
                    rows = con.execute(
                        "SELECT title FROM movies WHERE year=%s",
                        (year,),
                    ).fetchall()
                    normalized = self._normalize_name(title)
                    matched_title = next(
                        (row[0] for row in rows if self._normalize_name(row[0]) == normalized),
                        None,
                    )
                    updated = 0
                    if matched_title is not None:
                        updated = con.execute(
                            "UPDATE movies SET voe_url=%s, updated_at=now() WHERE title=%s AND year=%s",
                            (voe_url, matched_title, year),
                        ).rowcount
                    if updated:
                        return
                con.execute(
                    "INSERT INTO movies(title,year,voe_url) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                    (title, year, voe_url),
                )
                return
            parts = self._episode_parts(filename)
            if not parts:
                return
            name, season, episode = parts
            table, column = ('anime_episodes', 'anime') if is_anime else ('episodes', 'series')
            if overwrite:
                updated = con.execute(
                    f"UPDATE {table} SET voe_url=%s, updated_at=now() WHERE lower({column})=lower(%s) AND season=%s AND episode=%s",
                    (voe_url, name, season, episode),
                ).rowcount
                if updated:
                    return
            con.execute(
                f"INSERT INTO {table}({column},season,episode,voe_url) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (name, season, episode, voe_url),
            )

    def get_voe(self, content_type: str, filename: str, is_anime: bool = False) -> str | None:
        with self.connect() as con:
            if content_type == 'film':
                parts = self._movie_parts(filename)
                if not parts:
                    return None
                title, year = parts
                rows = con.execute(
                    "SELECT title, voe_url FROM movies WHERE year=%s",
                    (year,),
                ).fetchall()
                normalized = self._normalize_name(title)
                row = next(
                    (item for item in rows if self._normalize_name(item[0]) == normalized),
                    None,
                )
            else:
                parts = self._episode_parts(filename)
                if not parts:
                    return None
                table, column = ('anime_episodes', 'anime') if is_anime else ('episodes', 'series')
                row = con.execute(
                    f"SELECT voe_url FROM {table} WHERE lower({column})=lower(%s) AND season=%s AND episode=%s",
                    parts,
                ).fetchone()
            if not row:
                return None
            return row[1] if content_type == 'film' else row[0]

    def import_rows(self, movies: Iterable[tuple], episodes: Iterable[tuple], anime: Iterable[tuple]) -> dict[str, int]:
        movies = list(movies)
        episodes = list(episodes)
        anime = list(anime)
        before = self.counts()
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("CREATE TEMP TABLE import_movies (title text, year integer, voe_url text) ON COMMIT DROP")
                cur.execute("CREATE TEMP TABLE import_episodes (series text, season integer, episode integer, voe_url text) ON COMMIT DROP")
                cur.execute("CREATE TEMP TABLE import_anime (anime text, season integer, episode integer, voe_url text) ON COMMIT DROP")
                with cur.copy("COPY import_movies (title,year,voe_url) FROM STDIN") as copy:
                    for row in movies:
                        copy.write_row(row)
                with cur.copy("COPY import_episodes (series,season,episode,voe_url) FROM STDIN") as copy:
                    for row in episodes:
                        copy.write_row(row)
                with cur.copy("COPY import_anime (anime,season,episode,voe_url) FROM STDIN") as copy:
                    for row in anime:
                        copy.write_row(row)
                cur.execute("INSERT INTO movies(title,year,voe_url) SELECT title,year,voe_url FROM import_movies ON CONFLICT DO NOTHING")
                cur.execute("INSERT INTO episodes(series,season,episode,voe_url) SELECT series,season,episode,voe_url FROM import_episodes ON CONFLICT DO NOTHING")
                cur.execute("INSERT INTO anime_episodes(anime,season,episode,voe_url) SELECT anime,season,episode,voe_url FROM import_anime ON CONFLICT DO NOTHING")
        after = self.counts()
        return {table: after[table] - before[table] for table in after}

    def counts(self) -> dict[str, int]:
        with self.connect() as con:
            return {
                table: con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ('movies', 'episodes', 'anime_episodes')
            }
