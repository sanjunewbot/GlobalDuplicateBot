from __future__ import annotations

import logging

from pyrogram import Client, filters
from pyrogram.types import Message

from config import Config
from database import Database
from progress import build_stats_message, build_status_message
from scanner import ScanQueueManager


# Messages shown for common failure modes, centralized so wording stays
# consistent across handlers.
_NOT_ADMIN_MESSAGE = "You are not authorized to control this bot."
_USAGE_ADDCHANNEL = "Usage: /addchannel <chat_id>\nExample: /addchannel -1001234567890"
_USAGE_REMOVECHANNEL = "Usage: /removechannel <chat_id>\nExample: /removechannel -1001234567890"

# A short-lived confirmation token requirement for /resetdb, since it is
# destructive and irreversible.
_RESETDB_CONFIRM_ARG = "confirm"


class CommandRegistrar:
    """
    Binds all bot commands to their handlers on the given Pyrogram
    client. Constructed once in main.py's build() step and handed the
    fully-wired dependencies (db, queue_manager, config, logger) it
    needs — no module-level state, no globals.
    """

    def __init__(
        self,
        client: Client,
        db: Database,
        queue_manager: ScanQueueManager,
        config: Config,
        logger: logging.Logger,
    ) -> None:
        self._client = client
        self._db = db
        self._queue_manager = queue_manager
        self._config = config
        self._logger = logger

    def register_all(self) -> None:
        """Attach every command handler to the client. Call once at startup."""
        handlers = [
            ("addchannel", self.handle_addchannel),
            ("removechannel", self.handle_removechannel),
            ("listchannels", self.handle_listchannels),
            ("startscan", self.handle_startscan),
            ("pause", self.handle_pause),
            ("resume", self.handle_resume),
            ("status", self.handle_status),
            ("stats", self.handle_stats),
            ("resetdb", self.handle_resetdb),
        ]
        for command_name, handler in handlers:
            self._client.add_handler(
                _build_message_handler(command_name, handler)
            )
        self._logger.info(
            "Registered %d command handlers: %s",
            len(handlers), ", ".join(name for name, _ in handlers),
        )

    # ------------------------------------------------------------------ #
    # Authorization helper
    # ------------------------------------------------------------------ #

    async def _reject_if_not_admin(self, message: Message) -> bool:
        """Returns True (and replies) if the sender is NOT authorized."""
        user_id = message.from_user.id if message.from_user else None
        if user_id is None or not self._config.is_admin(user_id):
            await message.reply_text(_NOT_ADMIN_MESSAGE)
            self._logger.warning(
                "Rejected command from unauthorized user_id=%s in chat=%s.",
                user_id, message.chat.id,
            )
            return True
        return False

    @staticmethod
    def _parse_chat_id_arg(message: Message) -> int | None:
        parts = message.text.split(maxsplit=1) if message.text else []
        if len(parts) < 2:
            return None
        try:
            return int(parts[1].strip())
        except ValueError:
            return None

    # ------------------------------------------------------------------ #
    # /addchannel
    # ------------------------------------------------------------------ #

    async def handle_addchannel(self, message: Message) -> None:
        if await self._reject_if_not_admin(message):
            return

        chat_id = self._parse_chat_id_arg(message)
        if chat_id is None:
            await message.reply_text(_USAGE_ADDCHANNEL)
            return

        title = ""
        try:
            chat = await self._client.get_chat(chat_id)
            title = chat.title or chat.first_name or str(chat_id)
        except Exception as e:
            self._logger.warning(
                "Could not fetch chat info for %s while adding: %s", chat_id, e
            )
            title = str(chat_id)

        added = await self._queue_manager.add_channel(chat_id, title)
        if added:
            await message.reply_text(
                f"Channel added: {title} ({chat_id}).\n"
                f"Run /startscan to begin (or it will be included in the next scan)."
            )
        else:
            await message.reply_text(f"Channel {chat_id} is already registered.")

    # ------------------------------------------------------------------ #
    # /removechannel
    # ------------------------------------------------------------------ #

    async def handle_removechannel(self, message: Message) -> None:
        if await self._reject_if_not_admin(message):
            return

        chat_id = self._parse_chat_id_arg(message)
        if chat_id is None:
            await message.reply_text(_USAGE_REMOVECHANNEL)
            return

        removed = await self._queue_manager.remove_channel(chat_id)
        if removed:
            await message.reply_text(
                f"Channel {chat_id} removed. Its scan progress checkpoint was cleared; "
                f"re-adding it later will rescan its full history."
            )
        else:
            await message.reply_text(f"Channel {chat_id} was not registered.")

    # ------------------------------------------------------------------ #
    # /listchannels
    # ------------------------------------------------------------------ #

    async def handle_listchannels(self, message: Message) -> None:
        if await self._reject_if_not_admin(message):
            return

        channels = await self._queue_manager.list_channels()
        if not channels:
            await message.reply_text("No channels registered yet. Use /addchannel <chat_id>.")
            return

        lines = ["Registered channels:"]
        for c in channels:
            lines.append(f"  {c.chat_id} — {c.title or '(unknown title)'} [{c.status}]")
        await message.reply_text("\n".join(lines))

    # ------------------------------------------------------------------ #
    # /startscan
    # ------------------------------------------------------------------ #

    async def handle_startscan(self, message: Message) -> None:
        if await self._reject_if_not_admin(message):
            return

        enqueued = await self._queue_manager.start_scan()
        if enqueued == 0:
            await message.reply_text(
                "Nothing to scan: no registered channels are pending "
                "(all completed, or none added yet)."
            )
        else:
            await message.reply_text(
                f"Scan started: {enqueued} channel(s) queued. Use /status to follow progress."
            )

    # ------------------------------------------------------------------ #
    # /pause, /resume
    # ------------------------------------------------------------------ #

    async def handle_pause(self, message: Message) -> None:
        if await self._reject_if_not_admin(message):
            return
        self._queue_manager.pause()
        await message.reply_text(
            "Scan paused. It will halt after finishing the message currently in progress. "
            "Use /resume to continue."
        )

    async def handle_resume(self, message: Message) -> None:
        if await self._reject_if_not_admin(message):
            return
        self._queue_manager.resume()
        await message.reply_text("Scan resumed.")

    # ------------------------------------------------------------------ #
    # /status, /stats
    # ------------------------------------------------------------------ #

    async def handle_status(self, message: Message) -> None:
        if await self._reject_if_not_admin(message):
            return
        status = await self._queue_manager.get_status()
        await message.reply_text(build_status_message(status))

    async def handle_stats(self, message: Message) -> None:
        if await self._reject_if_not_admin(message):
            return
        stats = self._queue_manager.get_stats()
        db_size = await self._db.get_database_size_bytes()
        unique_count = await self._db.count_unique_hashes()
        await message.reply_text(build_stats_message(stats, db_size, unique_count))

    # ------------------------------------------------------------------ #
    # /resetdb (destructive — requires explicit confirmation argument)
    # ------------------------------------------------------------------ #

    async def handle_resetdb(self, message: Message) -> None:
        if await self._reject_if_not_admin(message):
            return

        parts = message.text.split(maxsplit=1) if message.text else []
        confirmed = len(parts) >= 2 and parts[1].strip().lower() == _RESETDB_CONFIRM_ARG

        if not confirmed:
            await message.reply_text(
                "This will permanently delete ALL stored hashes, channel "
                "registrations, and progress. This cannot be undone.\n\n"
                f"To confirm, send: /resetdb {_RESETDB_CONFIRM_ARG}"
            )
            return

        await self._queue_manager.reset_all()
        self._logger.warning(
            "Database reset via /resetdb by user_id=%s.",
            message.from_user.id if message.from_user else "unknown",
        )
        await message.reply_text("Database has been fully reset.")


def _build_message_handler(command_name: str, handler):
    """
    Build a Pyrogram MessageHandler for a given /command name, imported
    lazily to avoid a hard import-time dependency on pyrogram internals
    beyond what's needed (keeps this module's top import list minimal
    and explicit).
    """
    from pyrogram.handlers import MessageHandler

    return MessageHandler(handler, filters.command(command_name))
