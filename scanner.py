from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

from config import Config
from database import Database, MediaHashRecord
from hasher import HashingError, MediaHasher


# --------------------------------------------------------------------------- #
# Status / stats primitives
# --------------------------------------------------------------------------- #

class ScanState(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"


@dataclass
class ChannelProgressSnapshot:
    chat_id: int
    title: str
    scanned: int
    duplicates: int
    last_message_id: int
    status: str


@dataclass
class ScanStatus:
    state: ScanState
    current_chat_id: Optional[int]
    current_title: str
    channel_scanned: int
    channel_duplicates: int
    total_scanned: int
    total_duplicates: int
    messages_per_second: float
    eta_seconds: Optional[float]
    queue_size: int
    database_size_bytes: int


class _SpeedTracker:
    """
    Rolling-window speed tracker: keeps timestamps of the last N
    processed items and derives a smoothed messages/second figure,
    rather than an all-time average that reacts too slowly to
    FloodWait slowdowns or speedups.
    """

    def __init__(self, window: int = 100) -> None:
        self._timestamps: deque[float] = deque(maxlen=window)

    def tick(self) -> None:
        self._timestamps.append(time.monotonic())

    def rate(self) -> float:
        if len(self._timestamps) < 2:
            return 0.0
        span = self._timestamps[-1] - self._timestamps[0]
        if span <= 0:
            return 0.0
        return (len(self._timestamps) - 1) / span


# --------------------------------------------------------------------------- #
# Per-channel scanning
# --------------------------------------------------------------------------- #

class ChannelScanner:
    """
    Scans a single channel's history from newest to oldest, hashing
    and deduplicating every supported media message, persisting
    progress after each one. Cooperatively checks pause/stop signals
    between messages so a pause or shutdown takes effect promptly
    without corrupting in-flight state.
    """

    def __init__(
        self,
        client: Client,
        db: Database,
        hasher: MediaHasher,
        logger: logging.Logger,
        config: Config,
        speed_tracker: _SpeedTracker,
        pause_event: asyncio.Event,
        stop_event: asyncio.Event,
    ) -> None:
        self._client = client
        self._db = db
        self._hasher = hasher
        self._logger = logger
        self._config = config
        self._speed_tracker = speed_tracker
        self._pause_event = pause_event
        self._stop_event = stop_event

        self.channel_scanned = 0
        self.channel_duplicates = 0

    async def scan(self, chat_id: int, title: str) -> bool:
        """
        Scan `chat_id` from its last checkpoint down to message id 1.
        Returns True if the channel was fully completed (reached id 1),
        False if scanning was interrupted by a pause or stop request (in
        which case it should be resumed later, not marked completed).

        Unlike a user session, a bot account cannot call get_chat_history()
        (Telegram rejects it with BOT_METHOD_INVALID — bots have no
        "browse history" permission at all). What bots CAN do is fetch
        specific messages by id via get_messages(), and send/delete
        messages. So instead of an incremental history iterator, this
        walks backward through explicit message-id batches starting from
        the channel's current highest id (discovered via a short-lived
        probe message on first scan of a channel) down to 1, skipping
        ids that come back empty (deleted/never-existed messages).
        """
        self.channel_scanned = 0
        self.channel_duplicates = 0

        progress = await self._db.get_progress(chat_id)
        if progress == 0:
            # First scan of this channel: discover the current highest
            # message id by sending a short-lived probe message and
            # reading back its id, then deleting it immediately.
            highest_id = await self._discover_highest_message_id(chat_id)
            start_id = highest_id
            self._logger.info(
                "Starting scan of channel %s (%s): discovered highest "
                "message id=%s, will walk backward to id=1.",
                chat_id, title, highest_id,
            )
        else:
            # Resuming: progress stores the last id we successfully
            # checked, so continue from just below it.
            start_id = progress - 1
            self._logger.info(
                "Resuming scan of channel %s (%s) from id=%s (down to 1).",
                chat_id, title, start_id,
            )

        await self._db.set_channel_status(chat_id, "scanning")

        try:
            async for message in self._iter_by_id_range_with_retry(chat_id, start_id):
                if self._stop_event.is_set():
                    self._logger.info(
                        "Stop requested; suspending scan of channel %s at message %s.",
                        chat_id, message.id,
                    )
                    return False

                if not self._pause_event.is_set():
                    self._logger.info(
                        "Pause requested; suspending scan of channel %s at message %s.",
                        chat_id, message.id,
                    )
                    await self._pause_event.wait()
                    if self._stop_event.is_set():
                        return False

                await self._process_message(chat_id, message)

                # Progress is the lowest message id checked so far (we
                # walk newest -> oldest), so it's the id we just handled.
                self._db.queue_progress_update(chat_id, message.id)
                await self._db.maybe_flush_on_interval()

            # Reached id 1 with no stop/pause interruption: the entire
            # channel, from its highest id at scan-start down to its
            # very first message, has now been scanned.
            await self._db.flush_pending_hashes(force=True)
            await self._db.set_channel_status(chat_id, "completed")
            self._logger.info(
                "Channel %s (%s) fully scanned: %s items, %s duplicates.",
                chat_id, title, self.channel_scanned, self.channel_duplicates,
            )
            return True

        except Exception:
            await self._db.flush_pending_hashes(force=True)
            await self._db.set_channel_status(chat_id, "error")
            self._logger.exception(
                "Unhandled error scanning channel %s (%s); progress preserved for resume.",
                chat_id, title,
            )
            raise

    async def _discover_highest_message_id(self, chat_id: int) -> int:
        """
        Bot accounts have no way to ask Telegram "what is the highest
        message id in this channel" directly, and can't browse history
        to find out. The reliable workaround: send a short, obviously
        bot-generated placeholder message, read back its id (message ids
        are strictly increasing, so this is guaranteed to be >= every
        existing message), then delete it immediately. The channel is
        visible with this placeholder for well under a second.
        """
        attempts = 0
        while True:
            try:
                probe = await self._client.send_message(
                    chat_id, "🔄 GlobalDuplicateBot: syncing…"
                )
                try:
                    await self._client.delete_messages(chat_id, message_ids=[probe.id])
                except Exception:
                    self._logger.warning(
                        "Could not delete probe message %s in chat %s; it may "
                        "remain visible.", probe.id, chat_id,
                    )
                return probe.id
            except FloodWait as e:
                attempts += 1
                if attempts > self._config.max_flood_wait_retries:
                    raise
                self._logger.warning(
                    "FloodWait sending probe message to channel %s: sleeping %ss.",
                    chat_id, e.value,
                )
                await asyncio.sleep(e.value + 1)
            except RPCError as e:
                attempts += 1
                if attempts > self._config.max_flood_wait_retries:
                    raise RuntimeError(
                        f"Could not discover highest message id for channel "
                        f"{chat_id}: {type(e).__name__}: {e}. The bot needs "
                        f"'Send Messages' and 'Delete Messages' permissions "
                        f"in this channel."
                    ) from e
                backoff = min(2 ** attempts, 60)
                self._logger.warning(
                    "RPC error sending probe message to channel %s: %s (%s). "
                    "Retrying in %ss (attempt %s/%s).",
                    chat_id, type(e).__name__, e, backoff,
                    attempts, self._config.max_flood_wait_retries,
                )
                await asyncio.sleep(backoff)

    async def _iter_by_id_range_with_retry(self, chat_id: int, start_id: int):
        """
        Walks message ids backward from `start_id` down to 1 in batches,
        fetching each batch via `client.get_messages(chat_id, ids)`. This
        is the bot-account-compatible replacement for get_chat_history()
        (which Telegram rejects for bot accounts with BOT_METHOD_INVALID
        — bots have no history-browsing permission at all, only the
        ability to fetch specific known message ids). Ids that come back
        empty (deleted, or never existed) are silently skipped, exactly
        as a real gap in a channel's message sequence should be handled.
        """
        batch_size = 200  # Telegram's practical batch limit for get_messages
        current_id = start_id

        while current_id >= 1:
            batch_ids = list(range(current_id, max(current_id - batch_size, 0), -1))
            attempts = 0
            while True:
                try:
                    messages = await self._client.get_messages(chat_id, batch_ids)
                    if not isinstance(messages, list):
                        messages = [messages]
                    break
                except FloodWait as e:
                    attempts += 1
                    if attempts > self._config.max_flood_wait_retries:
                        raise
                    self._logger.warning(
                        "FloodWait fetching id batch (%s..%s) in channel %s: "
                        "sleeping %ss (attempt %s/%s).",
                        batch_ids[0], batch_ids[-1], chat_id, e.value,
                        attempts, self._config.max_flood_wait_retries,
                    )
                    await asyncio.sleep(e.value + 1)
                except RPCError as e:
                    attempts += 1
                    if attempts > self._config.max_flood_wait_retries:
                        raise
                    backoff = min(2 ** attempts, 60)
                    self._logger.warning(
                        "RPC error fetching id batch (%s..%s) in channel %s: "
                        "%s (%s). Retrying in %ss (attempt %s/%s).",
                        batch_ids[0], batch_ids[-1], chat_id,
                        type(e).__name__, e, backoff,
                        attempts, self._config.max_flood_wait_retries,
                    )
                    await asyncio.sleep(backoff)
                except Exception as e:
                    attempts += 1
                    if attempts > self._config.max_flood_wait_retries:
                        raise
                    backoff = min(2 ** attempts, 60)
                    self._logger.warning(
                        "Unexpected error fetching id batch (%s..%s) in "
                        "channel %s: %s (%s). Retrying in %ss (attempt %s/%s).",
                        batch_ids[0], batch_ids[-1], chat_id,
                        type(e).__name__, e, backoff,
                        attempts, self._config.max_flood_wait_retries,
                    )
                    await asyncio.sleep(backoff)

            # Yield in strictly descending id order, skipping any id that
            # doesn't correspond to a real message (deleted or a gap).
            for message, message_id in zip(messages, batch_ids):
                if message is None or getattr(message, "empty", False):
                    continue
                yield message

            current_id = batch_ids[-1] - 1

    async def _process_message(self, chat_id: int, message) -> None:
        if not MediaHasher.has_supported_media(message):
            return

        try:
            hash_result = await self._hasher.hash_message(message)
        except HashingError as e:
            self._logger.warning(
                "Skipping unhashable message %s in chat %s: %s", message.id, chat_id, e
            )
            return

        self.channel_scanned += 1
        self._db.increment_scanned()
        self._speed_tracker.tick()

        existing = await self._db.lookup_hash(hash_result.file_hash)
        if existing is not None:
            await self._delete_duplicate(chat_id, message.id, hash_result.file_hash, existing)
            self.channel_duplicates += 1
            self._db.increment_duplicates()
        else:
            await self._db.insert_hash(
                MediaHashRecord(
                    hash=hash_result.file_hash,
                    chat_id=chat_id,
                    message_id=message.id,
                    file_size=hash_result.file_size,
                    media_type=hash_result.media_type,
                )
            )

    async def _delete_duplicate(
        self,
        chat_id: int,
        message_id: int,
        file_hash: str,
        original: MediaHashRecord,
    ) -> None:
        attempts = 0
        while True:
            try:
                await self._client.delete_messages(chat_id, message_ids=[message_id])
                self._logger.info(
                    "Deleted duplicate message %s in chat %s (hash=%s, original at "
                    "chat=%s message=%s).",
                    message_id, chat_id, file_hash[:16], original.chat_id, original.message_id,
                )
                return
            except FloodWait as e:
                attempts += 1
                if attempts > self._config.max_flood_wait_retries:
                    self._logger.error(
                        "Giving up deleting duplicate message %s in chat %s after "
                        "repeated FloodWait.", message_id, chat_id,
                    )
                    return
                await asyncio.sleep(e.value + 1)
            except RPCError as e:
                self._logger.error(
                    "Could not delete duplicate message %s in chat %s: %s",
                    message_id, chat_id, e,
                )
                return


# --------------------------------------------------------------------------- #
# Queue manager
# --------------------------------------------------------------------------- #

class ScanQueueManager:
    """
    Owns the cross-channel scan queue and the worker task(s) that drain
    it. Public methods here are what commands.py calls in response to
    /addchannel, /removechannel, /startscan, /pause, /resume, /status,
    /stats and /resetdb.
    """

    def __init__(
        self,
        client: Client,
        db: Database,
        logger: logging.Logger,
        config: Config,
    ) -> None:
        self._client = client
        self._db = db
        self._logger = logger
        self._config = config
        self._hasher = MediaHasher(
            client=client,
            logger=logger,
            chunk_size=config.hash_chunk_size,
            max_flood_wait_retries=config.max_flood_wait_retries,
        )

        self._queue: asyncio.Queue[tuple[int, str]] = asyncio.Queue()
        self._queued_chat_ids: set[int] = set()

        self._pause_event = asyncio.Event()
        self._pause_event.set()  # start un-paused
        self._stop_event = asyncio.Event()

        self._state = ScanState.IDLE
        self._current_chat_id: Optional[int] = None
        self._current_title: str = ""
        self._speed_tracker = _SpeedTracker()

        self._scanner = ChannelScanner(
            client=client,
            db=db,
            hasher=self._hasher,
            logger=logger,
            config=config,
            speed_tracker=self._speed_tracker,
            pause_event=self._pause_event,
            stop_event=self._stop_event,
        )

        self._worker_tasks: list[asyncio.Task] = []
        self._workers_started = False

    # ------------------------------------------------------------------ #
    # Channel management
    # ------------------------------------------------------------------ #

    async def add_channel(self, chat_id: int, title: str = "") -> bool:
        added = await self._db.add_channel(chat_id, title)
        if added:
            self._logger.info("Channel %s (%s) registered.", chat_id, title)
        return added

    async def remove_channel(self, chat_id: int) -> bool:
        removed = await self._db.remove_channel(chat_id)
        self._queued_chat_ids.discard(chat_id)
        if removed:
            self._logger.info("Channel %s unregistered.", chat_id)
        return removed

    async def list_channels(self) -> list:
        return await self._db.list_channels()

    # ------------------------------------------------------------------ #
    # Scan control
    # ------------------------------------------------------------------ #

    async def start_scan(self) -> int:
        """
        Enqueue every registered channel that isn't already completed
        or already queued. Starts the worker pool on first use. Returns
        the number of channels newly enqueued.
        """
        self._ensure_workers_started()
        self._pause_event.set()
        self._stop_event.clear()

        channels = await self._db.list_channels()
        enqueued = 0
        for channel in channels:
            if channel.status == "completed":
                continue
            if channel.chat_id in self._queued_chat_ids:
                continue
            await self._queue.put((channel.chat_id, channel.title))
            self._queued_chat_ids.add(channel.chat_id)
            enqueued += 1

        if enqueued:
            self._state = ScanState.RUNNING
        self._logger.info("start_scan: %s channel(s) enqueued.", enqueued)
        return enqueued

    async def resume_on_startup(self) -> None:
        """
        Called once at process start. Re-enqueues any channel that was
        left in 'scanning' or 'pending' state from a previous run (e.g.
        the process was killed mid-scan), so work continues exactly
        where it stopped without the user needing to run /startscan
        again.
        """
        channels = await self._db.list_channels()
        pending = [c for c in channels if c.status in ("scanning", "pending", "error")]
        if not pending:
            return

        self._ensure_workers_started()
        for channel in pending:
            if channel.chat_id in self._queued_chat_ids:
                continue
            await self._queue.put((channel.chat_id, channel.title))
            self._queued_chat_ids.add(channel.chat_id)

        self._state = ScanState.RUNNING
        self._logger.info(
            "resume_on_startup: re-enqueued %s channel(s) left in-progress.", len(pending)
        )

    def pause(self) -> None:
        self._pause_event.clear()
        if self._state == ScanState.RUNNING:
            self._state = ScanState.PAUSED
        self._logger.info("Scan paused.")

    def resume(self) -> None:
        self._pause_event.set()
        if self._state == ScanState.PAUSED:
            self._state = ScanState.RUNNING
        self._logger.info("Scan resumed.")

    async def request_stop(self) -> None:
        """
        Used during application shutdown: signals all in-progress scans
        to stop cleanly after the current message rather than being
        cancelled mid-write. Progress up to that point is already
        persisted, so this never loses work.
        """
        self._state = ScanState.STOPPING
        self._stop_event.set()
        self._pause_event.set()  # unblock anything waiting on pause so it can see the stop
        for task in self._worker_tasks:
            task.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)

    def _ensure_workers_started(self) -> None:
        if self._workers_started:
            return
        self._workers_started = True
        for i in range(self._config.scan_concurrency):
            task = asyncio.create_task(self._worker_body(), name=f"scan_worker_{i}")
            self._worker_tasks.append(task)

    # ------------------------------------------------------------------ #
    # Worker
    # ------------------------------------------------------------------ #

    async def worker_loop(self) -> None:
        """
        Public entry point started from main.py. Ensures the worker
        pool is running and then waits for it indefinitely; the actual
        per-item logic lives in `_worker_body()` so `resume_on_startup`
        / `start_scan` can also spin workers up on demand without
        depending on this specific task.
        """
        self._ensure_workers_started()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        else:
            # No channels yet; idle until one is added and start_scan()
            # spins up workers itself. Just keep this coroutine alive.
            await self._stop_event.wait()

    async def _worker_body(self) -> None:
        while not self._stop_event.is_set():
            try:
                chat_id, title = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            self._current_chat_id = chat_id
            self._current_title = title
            try:
                completed = await self._scanner.scan(chat_id, title)
                if not completed:
                    # Paused or stopped mid-channel: leave status as
                    # 'scanning' (already set inside scan()) so it's
                    # picked back up by start_scan()/resume_on_startup().
                    pass
            except Exception:
                self._logger.exception(
                    "Worker encountered an error scanning channel %s (%s); "
                    "moving on to next queued channel.", chat_id, title,
                )
            finally:
                self._queued_chat_ids.discard(chat_id)
                self._current_chat_id = None
                self._current_title = ""
                self._queue.task_done()

            if self._queue.empty() and self._state == ScanState.RUNNING:
                self._state = ScanState.IDLE

    # ------------------------------------------------------------------ #
    # Status / stats reporting
    # ------------------------------------------------------------------ #

    async def get_status(self) -> ScanStatus:
        cached = self._db.get_cached_stats()
        rate = self._speed_tracker.rate()

        eta_seconds: Optional[float] = None
        if rate > 0 and self._current_chat_id is not None:
            # Approximate remaining work using the current checkpoint's
            # message id as a proxy for "messages remaining to the start
            # of the channel" — message ids are roughly sequential, so
            # this gives a reasonable (if approximate) ETA without an
            # expensive full-channel count.
            remaining = await self._db.get_progress(self._current_chat_id)
            if remaining > 0:
                eta_seconds = remaining / rate

        return ScanStatus(
            state=self._state,
            current_chat_id=self._current_chat_id,
            current_title=self._current_title,
            channel_scanned=self._scanner.channel_scanned,
            channel_duplicates=self._scanner.channel_duplicates,
            total_scanned=cached.get("scanned", 0),
            total_duplicates=cached.get("duplicates", 0),
            messages_per_second=rate,
            eta_seconds=eta_seconds,
            queue_size=self._queue.qsize(),
            database_size_bytes=await self._db.get_database_size_bytes(),
        )

    def get_stats(self) -> dict:
        return self._db.get_cached_stats()

    async def reset_all(self) -> None:
        """Backing implementation for /resetdb — also clears in-memory queue state."""
        await self.request_stop()
        self._queue = asyncio.Queue()
        self._queued_chat_ids.clear()
        self._worker_tasks = []
        self._workers_started = False
        self._state = ScanState.IDLE
        self._stop_event = asyncio.Event()
        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._scanner._pause_event = self._pause_event
        self._scanner._stop_event = self._stop_event
        await self._db.reset_all()
