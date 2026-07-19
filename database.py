from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, InsertOne, UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError


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
    Async wrapper around a MongoDB Atlas (or any MongoDB) database.
    Batches hash inserts and progress updates in memory and flushes
    them in bulk, exactly as the SQLite version did, to avoid one
    network round-trip per media item during a large scan.
    """

    def __init__(self, mongodb_uri: str, db_name: str, logger: logging.Logger) -> None:
        self._uri = mongodb_uri
        self._db_name = db_name
        self._logger = logger
        self._client: Optional[AsyncIOMotorClient] = None
        self._db = None

        self._stats_dirty = False
        self._stats_cache: dict[str, int] = {"scanned": 0, "duplicates": 0}

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
        self._client = AsyncIOMotorClient(self._uri, serverSelectionTimeoutMS=10000)
        self._db = self._client[self._db_name]
        # Fail fast with a clear error if the URI/credentials/network access
        # list are wrong, rather than surfacing a confusing error later.
        await self._client.admin.command("ping")
        self._logger.info("Connected to MongoDB database '%s'.", self._db_name)

    async def close(self) -> None:
        if self._client is None:
            return
        await self.flush_pending_hashes(force=True)
        await self.flush_stats()
        self._client.close()
        self._client = None
        self._db = None

    def _require_db(self):
        if self._db is None:
            raise RuntimeError("Database.connect() must be called before use.")
        return self._db

    # ------------------------------------------------------------------ #
    # Schema (indexes + seed documents)
    # ------------------------------------------------------------------ #

    async def init_schema(self) -> None:
        db = self._require_db()

        await db.media_hashes.create_index([("chat_id", ASCENDING)])
        await db.channels.create_index([("add_seq", ASCENDING)])

        for key in ("scanned", "duplicates"):
            await db.stats.update_one(
                {"_id": key}, {"$setOnInsert": {"value": 0}}, upsert=True
            )
        await db.counters.update_one(
            {"_id": "channel_add_seq"}, {"$setOnInsert": {"value": 0}}, upsert=True
        )

        async for doc in db.stats.find({}):
            self._stats_cache[doc["_id"]] = doc.get("value", 0)

        self._logger.info(
            "Schema ensured (media_hashes, progress, channels, stats indexes/seed docs)."
        )

    # ------------------------------------------------------------------ #
    # media_hashes: lookup + batched insert
    # ------------------------------------------------------------------ #

    async def lookup_hash(self, file_hash: str) -> Optional[MediaHashRecord]:
        """
        Check whether a hash already exists. Checks the not-yet-flushed
        in-memory batch first, so a duplicate within the same in-flight
        batch is still caught correctly before it ever hits the network.
        """
        for pending in reversed(self._pending_hash_inserts):
            if pending.hash == file_hash:
                self._logger.info(
                    "[DB LOOKUP] hash=%s matched an UNFLUSHED in-memory record "
                    "(chat=%s message=%s) — not yet written to MongoDB.",
                    file_hash, pending.chat_id, pending.message_id,
                )
                return pending

        db = self._require_db()
        doc = await db.media_hashes.find_one({"_id": file_hash})
        if doc is None:
            self._logger.info(
                "[DB LOOKUP] hash=%s: no match in MongoDB media_hashes collection.",
                file_hash,
            )
            return None
        self._logger.info(
            "[DB LOOKUP] hash=%s matched an existing MongoDB record "
            "(chat=%s message=%s, stored file_size=%s).",
            file_hash, doc["chat_id"], doc["message_id"], doc["file_size"],
        )
        return MediaHashRecord(
            hash=doc["_id"],
            chat_id=doc["chat_id"],
            message_id=doc["message_id"],
            file_size=doc["file_size"],
            media_type=doc["media_type"],
        )

    async def insert_hash(self, record: MediaHashRecord) -> None:
        async with self._batch_lock:
            self._pending_hash_inserts.append(record)
            should_flush = len(self._pending_hash_inserts) >= self._batch_size_threshold

        if should_flush:
            await self.flush_pending_hashes()

    async def maybe_flush_on_interval(self) -> None:
        if time.monotonic() - self._last_flush_time >= self._batch_interval_seconds:
            await self.flush_pending_hashes()

    async def flush_pending_hashes(self, force: bool = False) -> int:
        """
        Write all buffered hash inserts and progress updates in a single
        bulk operation each. Returns the number of hash rows flushed.
        Duplicate-key errors (the same hash inserted twice, e.g. a race
        between two workers) are treated as harmless and ignored — the
        record is already there, which is exactly what we want.
        """
        async with self._batch_lock:
            if not self._pending_hash_inserts and not self._pending_progress_updates:
                self._last_flush_time = time.monotonic()
                return 0

            hashes_to_write = self._pending_hash_inserts
            progress_to_write = dict(self._pending_progress_updates)
            self._pending_hash_inserts = []
            self._pending_progress_updates = {}

        db = self._require_db()
        try:
            if hashes_to_write:
                ops = [
                    InsertOne(
                        {
                            "_id": r.hash,
                            "chat_id": r.chat_id,
                            "message_id": r.message_id,
                            "file_size": r.file_size,
                            "media_type": r.media_type,
                        }
                    )
                    for r in hashes_to_write
                ]
                try:
                    await db.media_hashes.bulk_write(ops, ordered=False)
                except BulkWriteError as e:
                    non_duplicate_errors = [
                        err for err in e.details.get("writeErrors", [])
                        if err.get("code") != 11000  # 11000 = duplicate key, harmless here
                    ]
                    if non_duplicate_errors:
                        raise

            if progress_to_write:
                progress_ops = [
                    UpdateOne(
                        {"_id": chat_id},
                        {"$set": {"last_message_id": last_message_id}},
                        upsert=True,
                    )
                    for chat_id, last_message_id in progress_to_write.items()
                ]
                await db.progress.bulk_write(progress_ops, ordered=False)

        except PyMongoError:
            async with self._batch_lock:
                self._pending_hash_inserts = hashes_to_write + self._pending_hash_inserts
                self._pending_progress_updates = {**progress_to_write, **self._pending_progress_updates}
            self._logger.exception("Failed to flush pending hashes/progress; will retry.")
            raise

        self._last_flush_time = time.monotonic()
        if hashes_to_write:
            self._logger.debug("Flushed %d hash inserts to MongoDB.", len(hashes_to_write))
        return len(hashes_to_write)

    # ------------------------------------------------------------------ #
    # progress
    # ------------------------------------------------------------------ #

    def queue_progress_update(self, chat_id: int, last_message_id: int) -> None:
        self._pending_progress_updates[chat_id] = last_message_id

    async def get_progress(self, chat_id: int) -> int:
        if chat_id in self._pending_progress_updates:
            return self._pending_progress_updates[chat_id]
        db = self._require_db()
        doc = await db.progress.find_one({"_id": chat_id})
        return doc["last_message_id"] if doc else 0

    async def reset_progress(self, chat_id: int) -> None:
        db = self._require_db()
        await db.progress.delete_one({"_id": chat_id})
        self._pending_progress_updates.pop(chat_id, None)

    # ------------------------------------------------------------------ #
    # channels
    # ------------------------------------------------------------------ #

    async def add_channel(self, chat_id: int, title: str) -> bool:
        """Returns True if newly added, False if it already existed."""
        db = self._require_db()
        existing = await db.channels.find_one({"_id": chat_id})
        if existing is not None:
            return False

        counter_doc = await db.counters.find_one_and_update(
            {"_id": "channel_add_seq"},
            {"$inc": {"value": 1}},
            upsert=True,
            return_document=True,
        )
        add_seq = counter_doc["value"]

        try:
            await db.channels.insert_one(
                {
                    "_id": chat_id,
                    "title": title,
                    "status": "pending",
                    "add_seq": add_seq,
                }
            )
        except PyMongoError as e:
            if getattr(e, "code", None) == 11000:  # lost an insert race
                return False
            raise
        return True

    async def remove_channel(self, chat_id: int) -> bool:
        db = self._require_db()
        result = await db.channels.delete_one({"_id": chat_id})
        await db.progress.delete_one({"_id": chat_id})
        return result.deleted_count > 0

    async def list_channels(self) -> list[ChannelRecord]:
        db = self._require_db()
        results: list[ChannelRecord] = []
        async for doc in db.channels.find({}).sort("add_seq", ASCENDING):
            results.append(
                ChannelRecord(chat_id=doc["_id"], title=doc.get("title", ""), status=doc.get("status", "pending"))
            )
        return results

    async def set_channel_status(self, chat_id: int, status: str) -> None:
        db = self._require_db()
        await db.channels.update_one({"_id": chat_id}, {"$set": {"status": status}})

    async def channel_exists(self, chat_id: int) -> bool:
        db = self._require_db()
        return await db.channels.find_one({"_id": chat_id}) is not None

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
        db = self._require_db()
        for key, value in self._stats_cache.items():
            await db.stats.update_one({"_id": key}, {"$set": {"value": value}}, upsert=True)
        self._stats_dirty = False

    async def reset_all(self) -> None:
        """Used by /resetdb — wipes all collections and in-memory caches."""
        db = self._require_db()
        async with self._batch_lock:
            self._pending_hash_inserts = []
            self._pending_progress_updates = {}
        await db.media_hashes.delete_many({})
        await db.progress.delete_many({})
        await db.channels.delete_many({})
        await db.counters.update_one({"_id": "channel_add_seq"}, {"$set": {"value": 0}})
        for key in ("scanned", "duplicates"):
            await db.stats.update_one({"_id": key}, {"$set": {"value": 0}})
        self._stats_cache = {"scanned": 0, "duplicates": 0}
        self._stats_dirty = False
        self._logger.warning("Database has been fully reset via reset_all().")

    # ------------------------------------------------------------------ #
    # Maintenance
    # ------------------------------------------------------------------ #

    async def checkpoint_wal(self) -> None:
        """No-op for MongoDB — kept only so main.py's periodic maintenance
        loop (written against the SQLite backend) doesn't need changes."""
        return

    async def get_database_size_bytes(self) -> int:
        db = self._require_db()
        stats = await db.command("dbStats")
        return int(stats.get("storageSize", 0) + stats.get("indexSize", 0))

    async def count_unique_hashes(self) -> int:
        db = self._require_db()
        return await db.media_hashes.count_documents({})
