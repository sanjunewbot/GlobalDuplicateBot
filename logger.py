from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path


_CONSOLE_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_FILE_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | "
    "%(filename)s:%(lineno)d | %(message)s"
)
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class _LevelAwareFormatter(logging.Formatter):
    """
    Formatter that includes exception tracebacks cleanly and never
    raises on unicode/log-arg formatting errors (a broken log call
    should never crash the application).
    """

    def format(self, record: logging.LogRecord) -> str:
        try:
            return super().format(record)
        except Exception:
            # Last-resort fallback so a bad %-format string in a log
            # call never propagates into an application crash.
            return f"{record.levelname} | {record.name} | <log formatting error: {record.msg!r}>"


def setup_logging(
    log_dir: str,
    level: str = "INFO",
    app_name: str = "GlobalDuplicateBot",
    backup_days: int = 14,
) -> logging.Logger:
    """
    Configure the root logger for the whole application and return the
    application's top-level named logger.

    Parameters:
        log_dir: directory where log files are written (created if missing).
        level: one of DEBUG/INFO/WARNING/ERROR/CRITICAL.
        app_name: name of the returned top-level logger, and the log filename
            (e.g. "GlobalDuplicateBot.log").
        backup_days: how many days of rotated log files to retain.

    Returns:
        The named logger for `app_name`. Sub-modules should use
        `logging.getLogger(f"{app_name}.<module>")` or simply
        `logging.getLogger(__name__)` — both propagate up to the root
        handlers configured here.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / f"{app_name}.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Avoid duplicate handlers if setup_logging() is somehow called twice
    # (e.g. under a test harness or a supervisor restart within the same
    # process rather than a fresh interpreter).
    root_logger.handlers.clear()

    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(_LevelAwareFormatter(_CONSOLE_FORMAT, datefmt=_DATE_FORMAT))
    root_logger.addHandler(console_handler)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        interval=1,
        backupCount=backup_days,
        encoding="utf-8",
        utc=False,
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(_LevelAwareFormatter(_FILE_FORMAT, datefmt=_DATE_FORMAT))
    root_logger.addHandler(file_handler)

    # Quiet down noisy third-party loggers unless we're in DEBUG mode.
    if numeric_level > logging.DEBUG:
        for noisy_logger_name in ("pyrogram", "asyncio"):
            logging.getLogger(noisy_logger_name).setLevel(logging.WARNING)

    app_logger = logging.getLogger(app_name)
    app_logger.info(
        "Logging initialized. level=%s file=%s backup_days=%s",
        level.upper(), str(log_file), backup_days,
    )
    return app_logger


def get_logger(name: str) -> logging.Logger:
    """
    Convenience helper for modules that want a child logger without
    importing `logging` directly. Equivalent to `logging.getLogger(name)`;
    relies on `setup_logging()` having already configured the root logger.
    """
    return logging.getLogger(name)
