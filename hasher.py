from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from blake3 import blake3
from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.types import Message


class HashingError(RuntimeError):
    """Raised when a media item cannot be hashed after retries."""


@dataclass
class HashResult:
    file_hash: str
    bytes_hashed: int
    media_type: str
    file_size: int
    duration_seconds: float

    @property
    def throughput_mb_per_sec(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return (self.bytes_hashed / (1024 * 1024)) / self.duration_seconds


def _extract_media_info(message: Message) -> tuple[Optional[object], str, int]:
    """
    Identify which media field is populated on a message and return
    (media_object, media_type_name, declared_file_size).

    Returns (None, "", 0) if the message carries no supported media —
    callers should skip such messages rather than attempt to hash them.
    """
    if message.video:
        return message.video, "video", message.video.file_size or 0
    if message.animation:
        return message.animation, "animation", message.animation.file_size or 0
    if message.audio:
        return message.audio, "audio", message.audio.file_size or 0
    if message.document:
        return message.document, "document", message.document.file_size or 0
    if message.photo:
        # Photos don't carry a reliable file_size in all Bot API/MTProto
        # variants; 0 is fine here since we hash from the actual stream.
        return message.photo, "photo", getattr(message.photo, "file_size", 0) or 0
    return None, "", 0


class MediaHasher:
    """
    Computes a streaming BLAKE3 hash for a Telegram message's media by
    reading it chunk-by-chunk via `Client.stream_media()`. Never touches
    disk and never buffers more than one chunk in memory at a time.
    """

    def __init__(
        self,
        client: Client,
        logger: logging.Logger,
        chunk_size: int = 1024 * 1024,
        max_flood_wait_retries: int = 10,
    ) -> None:
        self._client = client
        self._logger = logger
        self._chunk_size = chunk_size
        self._max_flood_wait_retries = max_flood_wait_retries

    @staticmethod
    def has_supported_media(message: Message) -> bool:
        media, media_type, _ = _extract_media_info(message)
        return media is not None and media_type != ""

    async def hash_message(self, message: Message) -> HashResult:
        """
        Stream the media attached to `message` and return its BLAKE3
        hash plus metadata. Raises HashingError if the message has no
        supported media, or if streaming fails after retrying through
        FloodWait errors.
        """
        media, media_type, declared_size = _extract_media_info(message)
        if media is None:
            raise HashingError(
                f"Message {message.id} in chat {message.chat.id} has no supported media."
            )

        start_time = time.monotonic()
        hasher = blake3()
        bytes_hashed = 0
        attempt = 0

        while True:
            try:
                async for chunk in self._client.stream_media(
                    message, limit=0, offset=0
                ):
                    if not chunk:
                        continue
                    hasher.update(chunk)
                    bytes_hashed += len(chunk)
                break  # streaming completed without error
            except FloodWait as e:
                attempt += 1
                if attempt > self._max_flood_wait_retries:
                    raise HashingError(
                        f"Exceeded max FloodWait retries ({self._max_flood_wait_retries}) "
                        f"while streaming message {message.id} in chat {message.chat.id}."
                    ) from e
                self._logger.warning(
                    "FloodWait while streaming message %s (chat %s): sleeping %ss "
                    "(attempt %s/%s).",
                    message.id, message.chat.id, e.value, attempt, self._max_flood_wait_retries,
                )
                await asyncio.sleep(e.value + 1)
                # Reset and re-stream from scratch: a partial hash from an
                # interrupted stream would be incorrect, so we cannot resume
                # mid-file for hashing purposes (unlike scan/message-level
                # progress, which resumes fine at the message granularity).
                hasher = blake3()
                bytes_hashed = 0
            except Exception as e:
                attempt += 1
                if attempt > self._max_flood_wait_retries:
                    raise HashingError(
                        f"Failed to stream message {message.id} in chat {message.chat.id} "
                        f"after {attempt} attempts: {e}"
                    ) from e
                self._logger.warning(
                    "Transient error streaming message %s (chat %s), retrying "
                    "(attempt %s/%s): %s",
                    message.id, message.chat.id, attempt, self._max_flood_wait_retries, e,
                )
                await asyncio.sleep(min(2 ** attempt, 30))
                hasher = blake3()
                bytes_hashed = 0

        if bytes_hashed == 0:
            raise HashingError(
                f"Streamed zero bytes for message {message.id} in chat {message.chat.id}; "
                f"media may be inaccessible or empty."
            )

        duration = time.monotonic() - start_time
        file_hash = hasher.hexdigest()

        result = HashResult(
            file_hash=file_hash,
            bytes_hashed=bytes_hashed,
            media_type=media_type,
            file_size=declared_size or bytes_hashed,
            duration_seconds=duration,
        )
        self._logger.debug(
            "Hashed message %s (chat %s): type=%s size=%s bytes hash=%s "
            "in %.2fs (%.2f MB/s).",
            message.id, message.chat.id, media_type, result.file_size,
            file_hash[:16], duration, result.throughput_mb_per_sec,
        )
        return result
