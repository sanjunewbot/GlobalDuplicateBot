from __future__ import annotations

import asyncio
import logging
import signal
import sys
import traceback
from dataclasses import dataclass
from types import FrameType
from typing import Optional

from pyrogram import Client
from pyrogram.errors import RPCError, FloodWait

# --------------------------------------------------------------------------- #
# Pyrogram compatibility patch
# --------------------------------------------------------------------------- #
# Pyrogram's current stable release ships with outdated MIN_CHANNEL_ID /
# MIN_CHAT_ID boundary constants that predate Telegram's newer, larger
# channel-id numbering space. This causes a false "Peer id invalid" error
# for channels Telegram has legitimately assigned a lower (more negative)
# numeric id to, even though the id itself is valid and the account is a
# genuine member. This is a known upstream issue (pyrogram/pyrogram PR
# #1430 and #1435) that has not yet been merged into a released version,
# so the fix is applied directly here rather than waiting on a new release.
# Widened well beyond both proposed PR values to leave headroom for further
# growth of Telegram's id space.
import pyrogram.utils as _pyrogram_utils
_pyrogram_utils.MIN_CHANNEL_ID = -2000000000000
_pyrogram_utils.MIN_CHAT_ID = -999999999999

from config import Config
from logger import setup_logging
from database import Database
from scanner import ScanQueueManager
from commands import CommandRegistrar
from health_server import run_health_server


# --------------------------------------------------------------------------- #
# Application container
# --------------------------------------------------------------------------- #

@dataclass
class AppState:
    """
    Holds every long-lived object the application needs, constructed once
    at startup and torn down once at shutdown. Passed around via
    dependency injection rather than module-level globals.
    """
    config: Config
    client: Client
    db: Database
    queue_manager: ScanQueueManager
    logger: logging.Logger


class GlobalDuplicateBotApp:
    """
    Top-level application object. Owns the full lifecycle:

        build() -> start() -> run_forever() -> shutdown()

    A single instance of this class is created in main() and driven by
    asyncio. All coordination between subsystems (client, db, scanner
    queue, commands) happens through this class rather than through
    import-time globals.
    """

    def __init__(self) -> None:
        self.state: Optional[AppState] = None
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._background_tasks: list[asyncio.Task] = []
        self._restart_requested: bool = False
        self._shutting_down: bool = False

    # ------------------------------------------------------------------- #
    # Construction
    # ------------------------------------------------------------------- #

    async def build(self) -> AppState:
        """
        Construct every subsystem in dependency order:
            config -> logger -> database -> pyrogram client -> queue manager
        Nothing here starts network I/O except where unavoidable
        (Database.connect performs file I/O, not network I/O).
        """
        config = Config.load()

        logger = setup_logging(
            log_dir=config.log_dir,
            level=config.log_level,
            app_name="GlobalDuplicateBot",
        )
        logger.info("Configuration loaded. Building application...")

        db = Database(
            mongodb_uri=config.mongodb_uri,
            db_name=config.mongodb_db_name,
            logger=logger,
        )
        await db.connect()
        await db.init_schema()
        logger.info("Database connected and schema ensured.")

        client = Client(
            name=config.session_name,
            api_id=config.api_id,
            api_hash=config.api_hash,
            bot_token=config.bot_token if config.bot_token else None,
            workdir=config.workdir,
            sleep_threshold=config.flood_sleep_threshold,
        )

        queue_manager = ScanQueueManager(
            client=client,
            db=db,
            logger=logger,
            config=config,
        )

        registrar = CommandRegistrar(
            client=client,
            db=db,
            queue_manager=queue_manager,
            config=config,
            logger=logger,
        )
        registrar.register_all()

        state = AppState(
            config=config,
            client=client,
            db=db,
            queue_manager=queue_manager,
            logger=logger,
        )
        self.state = state
        return state

    # ------------------------------------------------------------------- #
    # Lifecycle
    # ------------------------------------------------------------------- #

    async def start(self) -> None:
        """Start the pyrogram client and any background tasks."""
        assert self.state is not None, "build() must be called before start()"
        state = self.state

        await self._start_client_with_retry(state)

        # Resume any scan that was in progress before a restart.
        self._background_tasks.append(
            asyncio.create_task(
                state.queue_manager.resume_on_startup(),
                name="resume_on_startup",
            )
        )

        # Periodic housekeeping: batched commits, stats flush.
        self._background_tasks.append(
            asyncio.create_task(
                self._periodic_maintenance(state),
                name="periodic_maintenance",
            )
        )

        # Worker pool that actually processes the channel queue.
        self._background_tasks.append(
            asyncio.create_task(
                state.queue_manager.worker_loop(),
                name="scan_worker_loop",
            )
        )

        # Trivial HTTP endpoint for external uptime pingers (e.g. UptimeRobot)
        # on hosts that sleep an inactive process. Purely a keep-alive
        # surface; safe to disable via HEALTH_CHECK_ENABLED=false on hosts
        # that don't need it (or that reject apps listening on a port).
        if state.config.health_check_enabled:
            self._background_tasks.append(
                asyncio.create_task(
                    run_health_server(state.config.health_check_port, state.logger),
                    name="health_check_server",
                )
            )

        state.logger.info("GlobalDuplicateBot started successfully.")

    async def _start_client_with_retry(
        self, state: AppState, max_attempts: int = 5
    ) -> None:
        """
        Start the pyrogram client, retrying with backoff on transient
        errors (network hiccups, FloodWait during initial auth, etc).
        """
        attempt = 0
        delay = 2
        while attempt < max_attempts:
            try:
                await state.client.start()
                me = await state.client.get_me()
                state.logger.info(
                    "Pyrogram client started as %s (id=%s).",
                    me.username or me.first_name,
                    me.id,
                )
                await self._hydrate_peer_cache(state)
                return
            except FloodWait as e:
                state.logger.warning(
                    "FloodWait during client start: sleeping %s seconds.", e.value
                )
                await asyncio.sleep(e.value + 1)
            except RPCError as e:
                attempt += 1
                state.logger.error(
                    "RPC error starting client (attempt %s/%s): %s",
                    attempt, max_attempts, e,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
            except Exception:
                attempt += 1
                state.logger.error(
                    "Unexpected error starting client (attempt %s/%s):\n%s",
                    attempt, max_attempts, traceback.format_exc(),
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

        raise RuntimeError(
            f"Failed to start Pyrogram client after {max_attempts} attempts."
        )

    async def _hydrate_peer_cache(self, state: AppState) -> None:
        """
        Pyrogram can only resolve a chat by its raw numeric ID once it has
        the chat's access hash cached in local session storage. That cache
        is populated either by resolving a chat via @username, or by
        iterating the account's dialog list. Since this bot is driven by
        /addchannel <numeric_id> rather than usernames, we proactively
        walk the full dialog list once at startup so every channel/group
        the account is already a member of becomes resolvable by ID —
        otherwise calls like get_chat_history(chat_id) fail with
        "Peer id invalid" even though the account really is a member.
        """
        try:
            dialog_count = 0
            async for _ in state.client.get_dialogs():
                dialog_count += 1
            state.logger.info(
                "Peer cache hydrated: synced %s dialog(s) so channels can be "
                "resolved by numeric id.", dialog_count,
            )
        except Exception:
            state.logger.warning(
                "Failed to hydrate peer cache via get_dialogs(); channels not "
                "yet resolvable by username may fail with 'Peer id invalid' "
                "until this succeeds:\n%s", traceback.format_exc(),
            )


        """
        Background loop: flushes stats periodically (checkpoint_wal() is a
        no-op under the MongoDB backend, kept only for interface parity)
        Runs until shutdown is signaled.
        """
        interval = state.config.maintenance_interval_seconds
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=interval)
                break  # shutdown was signaled
            except asyncio.TimeoutError:
                pass  # normal tick, continue with maintenance

            try:
                await state.db.checkpoint_wal()
                await state.db.flush_stats()
                state.logger.debug("Periodic maintenance: stats flush OK.")
            except Exception:
                state.logger.error(
                    "Periodic maintenance failed:\n%s", traceback.format_exc()
                )

    async def run_forever(self) -> None:
        """
        Block until a shutdown signal is received, then tear everything
        down. This is the "outer loop" of the process.
        """
        await self._shutdown_event.wait()
        await self.shutdown()

    async def shutdown(self) -> None:
        """
        Gracefully stop everything: signal background tasks to stop,
        wait for them, flush the database, stop the
        pyrogram client. Safe to call multiple times.
        """
        if self._shutting_down:
            return
        self._shutting_down = True

        assert self.state is not None
        state = self.state
        state.logger.info("Shutdown initiated. Stopping subsystems...")

        self._shutdown_event.set()

        # Ask the scanner to stop cleanly (finishes current message, then halts).
        try:
            await state.queue_manager.request_stop()
        except Exception:
            state.logger.error(
                "Error requesting scanner stop:\n%s", traceback.format_exc()
            )

        for task in self._background_tasks:
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        try:
            await state.db.flush_stats()
            await state.db.checkpoint_wal()
            await state.db.close()
            state.logger.info("Database flushed and closed.")
        except Exception:
            state.logger.error("Error closing database:\n%s", traceback.format_exc())

        try:
            if state.client.is_connected:
                await state.client.stop()
            state.logger.info("Pyrogram client stopped.")
        except Exception:
            state.logger.error("Error stopping client:\n%s", traceback.format_exc())

        state.logger.info("Shutdown complete.")

    def request_shutdown(self, restart: bool = False) -> None:
        """Thread/signal-safe request to begin shutdown."""
        self._restart_requested = restart
        self._shutdown_event.set()

    @property
    def restart_requested(self) -> bool:
        return self._restart_requested


# --------------------------------------------------------------------------- #
# Signal handling
# --------------------------------------------------------------------------- #

def install_signal_handlers(app: GlobalDuplicateBotApp, loop: asyncio.AbstractEventLoop) -> None:
    """
    Install SIGINT/SIGTERM handlers that trigger a graceful shutdown
    instead of an abrupt process kill. On platforms without signal
    support in the event loop (e.g. some Windows setups), falls back
    to the default signal module handler.
    """

    def _handler(signum: int, frame: Optional[FrameType] = None) -> None:
        logging.getLogger("GlobalDuplicateBot").warning(
            "Received signal %s. Beginning graceful shutdown...", signum
        )
        app.request_shutdown(restart=False)

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handler, sig, None)
    except NotImplementedError:
        # Fallback for platforms where loop.add_signal_handler isn't supported.
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, _handler)


# --------------------------------------------------------------------------- #
# Global error handling
# --------------------------------------------------------------------------- #

def install_global_exception_handler(app: GlobalDuplicateBotApp, loop: asyncio.AbstractEventLoop) -> None:
    """
    Catch otherwise-unhandled exceptions raised inside asyncio tasks so a
    single bug in a background task can't silently kill the process
    without logging, and so we get a clean shutdown instead of a hang.
    """

    def _exception_handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        logger = logging.getLogger("GlobalDuplicateBot")
        exception = context.get("exception")
        message = context.get("message")
        if exception:
            logger.error(
                "Unhandled exception in event loop: %s\n%s",
                message,
                "".join(traceback.format_exception(
                    type(exception), exception, exception.__traceback__
                )),
            )
        else:
            logger.error("Unhandled error in event loop: %s", message)

    loop.set_exception_handler(_exception_handler)


# --------------------------------------------------------------------------- #
# Supervisor: process-level auto-restart
# --------------------------------------------------------------------------- #

async def _run_once() -> bool:
    """
    Build, start and run one full lifecycle of the application.
    Returns True if a restart was explicitly requested (e.g. via a
    future /restart admin command hook), False for a normal exit.
    """
    app = GlobalDuplicateBotApp()
    loop = asyncio.get_running_loop()

    await app.build()
    install_signal_handlers(app, loop)
    install_global_exception_handler(app, loop)

    try:
        await app.start()
        await app.run_forever()
    except Exception:
        assert app.state is not None
        app.state.logger.critical(
            "Fatal error in application lifecycle:\n%s", traceback.format_exc()
        )
        await app.shutdown()
        # Fatal errors are treated as auto-restartable rather than a hard crash,
        # so a transient issue (e.g. a bad network blip during startup) doesn't
        # require manual intervention to recover.
        return True

    return app.restart_requested


async def supervisor_main(max_consecutive_failures: int = 5) -> None:
    """
    Outer supervisor loop providing automatic-restart support. If the
    bot exits due to a fatal, uncaught error, it is restarted with
    exponential backoff, up to `max_consecutive_failures` in a row
    before giving up (to avoid a crash-restart loop hammering Telegram).
    """
    consecutive_failures = 0
    backoff = 5

    while True:
        should_restart = await _run_once()

        if not should_restart:
            logging.getLogger("GlobalDuplicateBot").info(
                "Clean shutdown, not restarting. Exiting."
            )
            return

        consecutive_failures += 1
        if consecutive_failures > max_consecutive_failures:
            logging.getLogger("GlobalDuplicateBot").critical(
                "Too many consecutive failures (%s). Giving up.",
                consecutive_failures,
            )
            return

        logging.getLogger("GlobalDuplicateBot").warning(
            "Restarting application in %s seconds (failure %s/%s)...",
            backoff, consecutive_failures, max_consecutive_failures,
        )
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 120)


def main() -> None:
    """Synchronous process entry point."""
    try:
        asyncio.run(supervisor_main())
    except KeyboardInterrupt:
        print("Interrupted by user. Exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main()
