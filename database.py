from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiosqlite


@dataclass
class MediaHashRecord:
    hash: str
    chat_id: int
    message_id: int
    file_size: int
    media_type: str


@dataclass
class ChannelRecord:
    chat_id: int
    title: str
    status: str


class Database:
    """
    Async wrapper around a single SQLite database file. Not thread-safe
    across multiple event loops, but safe for concurrent coroutines on
    one loop since aiosqlite serializes access through its own
    background thread/connection.
    """

    SCHEMA_VERSION = 1

    def __init__(self, db_path: str, logger: logging.Logger) -> None:
        self._db_path = db_path
        self._logger = logger
        self._conn: Optional[aiosqlite.Connection] = None

        # In-memory stat counters, flushed to the `stats` table
        # periodically instead of on every single increment.
        self._stats_dirty = False
        self._stats_cache: dict[str, int] = {"scanned": 0, "duplicates": 0}

        # Batched insert buffer for media_hashes, to avoid a disk commit
        # per file during a large scan.
        self._pending_hash_inserts: list[MediaHashRecord] = []
        self._pending_progress_updates: dict[int, int] = {}
        self._batch_lock = asyncio.Lock()
        self._batch_size_threshold = 200
        self._batch_interval_seconds = 2.0
        self._last_flush_time = time.monotonic()

    # ------------------------------------------------------------------ #
    # Connection lifecycle
    # ------------------------------------------------------------------ #

    async def connect(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._db_path)
        self._conn.row_factory = aiosqlite.Row

        # Pragmas tuned for a write-heavy, single-writer workload against
        # a database that must survive process crashes without corruption.
        await self._conn.execute("PRAGMA journal_mode = WAL;")
        await self._conn.execute("PRAGMA synchronous = NORMAL;")
        await self._conn.execute("PRAGMA foreign_keys = ON;")
        await self._conn.execute("PRAGMA temp_store = MEMORY;")
        await self._conn.execute("PRAGMA cache_size = -64000;")  # ~64MB page cache
        await self._conn.commit()
        self._logger.info("Database connection established at %s (WAL mode).", self._db_path)

    async def close(self) -> None:
        if self._conn is None:
            return
        await self.flush_pending_hashes(force=True)
        await self.flush_stats()
        await self._conn.close()
        self._conn = None

    def _require_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database.connect() must be called before use.")
        return self._conn

    # ------------------------------------------------------------------ #
    # Schema
    # ------------------------------------------------------------------ #

    async def init_schema(self) -> None:
        conn = self._require_conn()

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media_hashes (
                hash        TEXT PRIMARY KEY,
                chat_id     INTEGER NOT NULL,
                message_id  INTEGER NOT NULL,
                file_size   INTEGER NOT NULL,
                media_type  TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_media_hashes_chat ON media_hashes(chat_id);"
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS progress (
                chat_id         INTEGER PRIMARY KEY,
                last_message_id INTEGER NOT NULL DEFAULT 0,
                updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                chat_id  INTEGER PRIMARY KEY,
                title    TEXT NOT NULL DEFAULT '',
                added_at TEXT NOT NULL DEFAULT (datetime('now')),
                status   TEXT NOT NULL DEFAULT 'pending'
            );
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stats (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        await conn.execute(
            "INSERT OR IGNORE INTO stats(key, value) VALUES ('scanned', 0);"
        )
        await conn.execute(
            "INSERT OR IGNORE INTO stats(key, value) VALUES ('duplicates', 0);"
        )

        await conn.commit()

        # Load persisted stats into the in-memory cache.
        async with conn.execute("SELECT key, value FROM stats;") as cursor:
            async for row in cursor:
                self._stats_cache[row["key"]] = row["value"]

        self._logger.info("Schema ensured (media_hashes, progress, channels, stats).")

    # ------------------------------------------------------------------ #
    # media_hashes: lookup + batched insert
    # ------------------------------------------------------------------ #

    async def lookup_hash(self, file_hash: str) -> Optional[MediaHashRecord]:
        """
        Check whether a hash already exists in the global store. This
        also checks the not-yet-flushed in-memory batch, so a duplicate
        within the same in-flight batch is still caught correctly.
        """
        for pending in reversed(self._pending_hash_inserts):
            if pending.hash == file_hash:
                return pending

        conn = self._require_conn()
        async with conn.execute(
            "SELECT hash, chat_id, message_id, file_size, media_type "
            "FROM media_hashes WHERE hash = ?;",
            (file_hash,),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return MediaHashRecord(
                hash=row["hash"],
                chat_id=row["chat_id"],
                message_id=row["message_id"],
                file_size=row["file_size"],
                media_type=row["media_type"],
            )

    async def insert_hash(self, record: MediaHashRecord) -> None:
        """
        Queue a new unique hash for insertion. Actual disk write happens
        in a batch, either when the buffer is full or on a timer, via
        `flush_pending_hashes()`, which the scanner calls periodically.
        """
        async with self._batch_lock:
            self._pending_hash_inserts.append(record)
            should_flush = len(self._pending_hash_inserts) >= self._batch_size_threshold

        if should_flush:
            await self.flush_pending_hashes()

    async def maybe_flush_on_interval(self) -> None:
        """Call periodically (e.g. from the scanner's progress loop) to
        ensure pending inserts don't sit unflushed too long even under
        low throughput."""
        if time.monotonic() - self._last_flush_time >= self._batch_interval_seconds:
            await self.flush_pending_hashes()

    async def flush_pending_hashes(self, force: bool = False) -> int:
        """
        Write all buffered hash inserts (and progress updates) to disk
        in a single transaction. Returns the number of rows flushed.
        """
        async with self._batch_lock:
            if not self._pending_hash_inserts and not self._pending_progress_updates:
                self._last_flush_time = time.monotonic()
                return 0

            hashes_to_write = self._pending_hash_inserts
            progress_to_write = dict(self._pending_progress_updates)
            self._pending_hash_inserts = []
            self._pending_progress_updates = {}

        conn = self._require_conn()
        try:
            if hashes_to_write:
                await conn.executemany(
                    "INSERT OR IGNORE INTO media_hashes "
                    "(hash, chat_id, message_id, file_size, media_type) "
                    "VALUES (?, ?, ?, ?, ?);",
                    [
                        (r.hash, r.chat_id, r.message_id, r.file_size, r.media_type)
                        for r in hashes_to_write
                    ],
                )
            for chat_id, last_message_id in progress_to_write.items():
                await conn.execute(
                    "INSERT INTO progress (chat_id, last_message_id, updated_at) "
                    "VALUES (?, ?, datetime('now')) "
                    "ON CONFLICT(chat_id) DO UPDATE SET "
                    "last_message_id = excluded.last_message_id, "
                    "updated_at = excluded.updated_at;",
                    (chat_id, last_message_id),
                )
            await conn.commit()
        except Exception:
            # Put the un-flushed records back so nothing is silently lost;
            # the next flush attempt (or shutdown flush) will retry them.
            async with self._batch_lock:
                self._pending_hash_inserts = hashes_to_write + self._pending_hash_inserts
                self._pending_progress_updates = {**progress_to_write, **self._pending_progress_updates}
            self._logger.exception("Failed to flush pending hashes/progress; will retry.")
            raise

        self._last_flush_time = time.monotonic()
        if hashes_to_write:
            self._logger.debug("Flushed %d hash inserts to database.", len(hashes_to_write))
        return len(hashes_to_write)

    # ------------------------------------------------------------------ #
    # progress
    # ------------------------------------------------------------------ #

    def queue_progress_update(self, chat_id: int, last_message_id: int) -> None:
        """
        Record the checkpoint in memory; it's written on the next
        `flush_pending_hashes()` call alongside hash inserts so progress
        and hash data stay consistent with each other.
        """
        self._pending_progress_updates[chat_id] = last_message_id

    async def get_progress(self, chat_id: int) -> int:
        conn = self._require_conn()
        if chat_id in self._pending_progress_updates:
            return self._pending_progress_updates[chat_id]
        async with conn.execute(
            "SELECT last_message_id FROM progress WHERE chat_id = ?;", (chat_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row["last_message_id"] if row else 0

    async def reset_progress(self, chat_id: int) -> None:
        conn = self._require_conn()
        await conn.execute("DELETE FROM progress WHERE chat_id = ?;", (chat_id,))
        await conn.commit()
        self._pending_progress_updates.pop(chat_id, None)

    # ------------------------------------------------------------------ #
    # channels
    # ------------------------------------------------------------------ #

    async def add_channel(self, chat_id: int, title: str) -> bool:
        """Returns True if newly added, False if it already existed."""
        conn = self._require_conn()
        cursor = await conn.execute(
            "INSERT OR IGNORE INTO channels (chat_id, title, status) "
            "VALUES (?, ?, 'pending');",
            (chat_id, title),
        )
        await conn.commit()
        return cursor.rowcount > 0

    async def remove_channel(self, chat_id: int) -> bool:
        conn = self._require_conn()
        cursor = await conn.execute("DELETE FROM channels WHERE chat_id = ?;", (chat_id,))
        await conn.execute("DELETE FROM progress WHERE chat_id = ?;", (chat_id,))
        await conn.commit()
        return cursor.rowcount > 0

    async def list_channels(self) -> list[ChannelRecord]:
        conn = self._require_conn()
        results: list[ChannelRecord] = []
        async with conn.execute(
            "SELECT chat_id, title, status FROM channels ORDER BY added_at ASC;"
        ) as cursor:
            async for row in cursor:
                results.append(ChannelRecord(chat_id=row["chat_id"], title=row["title"], status=row["status"]))
        return results

    async def set_channel_status(self, chat_id: int, status: str) -> None:
        conn = self._require_conn()
        await conn.execute(
            "UPDATE channels SET status = ? WHERE chat_id = ?;", (status, chat_id)
        )
        await conn.commit()

    async def channel_exists(self, chat_id: int) -> bool:
        conn = self._require_conn()
        async with conn.execute(
            "SELECT 1 FROM channels WHERE chat_id = ?;", (chat_id,)
        ) as cursor:
            return await cursor.fetchone() is not None

    # ------------------------------------------------------------------ #
    # stats
    # ------------------------------------------------------------------ #

    def increment_scanned(self, count: int = 1) -> None:
        self._stats_cache["scanned"] = self._stats_cache.get("scanned", 0) + count
        self._stats_dirty = True

    def increment_duplicates(self, count: int = 1) -> None:
        self._stats_cache["duplicates"] = self._stats_cache.get("duplicates", 0) + count
        self._stats_dirty = True

    def get_cached_stats(self) -> dict[str, int]:
        return dict(self._stats_cache)

    async def flush_stats(self) -> None:
        if not self._stats_dirty:
            return
        conn = self._require_conn()
        for key, value in self._stats_cache.items():
            await conn.execute(
                "INSERT INTO stats (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value;",
                (key, value),
            )
        await conn.commit()
        self._stats_dirty = False

    async def reset_all(self) -> None:
        """Used by /resetdb — wipes all tables and in-memory caches."""
        conn = self._require_conn()
        async with self._batch_lock:
            self._pending_hash_inserts = []
            self._pending_progress_updates = {}
        await conn.execute("DELETE FROM media_hashes;")
        await conn.execute("DELETE FROM progress;")
        await conn.execute("DELETE FROM channels;")
        await conn.execute("UPDATE stats SET value = 0;")
        await conn.commit()
        self._stats_cache = {"scanned": 0, "duplicates": 0}
        self._stats_dirty = False
        self._logger.warning("Database has been fully reset via reset_all().")

    # ------------------------------------------------------------------ #
    # Maintenance
    # ------------------------------------------------------------------ #

    async def checkpoint_wal(self) -> None:
        conn = self._require_conn()
        await conn.execute("PRAGMA wal_checkpoint(PASSIVE);")

    async def get_database_size_bytes(self) -> int:
        path = Path(self._db_path)
        total = path.stat().st_size if path.exists() else 0
        for suffix in ("-wal", "-shm"):
            side_file = Path(str(path) + suffix)
            if side_file.exists():
                total += side_file.stat().st_size
        return total

    async def count_unique_hashes(self) -> int:
        conn = self._require_conn()
        async with conn.execute("SELECT COUNT(*) AS c FROM media_hashes;") as cursor:
            row = await cursor.fetchone()
            return row["c"] if row else 0
