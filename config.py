from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


def _load_dotenv_if_present(path: Path) -> None:
    """
    Minimal .env loader: KEY=VALUE per line, '#' comments, no external
    dependency required. Does not override variables already set in
    the real environment (so `export FOO=bar` always wins over .env).
    """
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _require(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise ConfigError(
            f"Missing required environment variable: {name}. "
            f"Set it in your environment or in a .env file next to main.py."
        )
    return value.strip()


def _optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _optional_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError as e:
        raise ConfigError(f"Environment variable {name} must be an integer, got: {raw!r}") from e


def _optional_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Config:
    """
    Immutable configuration snapshot, built once at startup via
    `Config.load()` and passed by reference to every subsystem.
    """

    # --- Telegram API credentials (from https://my.telegram.org) ---
    api_id: int
    api_hash: str

    # --- Auth mode: either a bot token, OR a user session is used. ---
    # A bot account CANNOT read full history of channels it did not post
    # in unless it is an admin with the right rights, and bot accounts
    # have stricter rate limits. For scanning large channel histories,
    # most deployments use a *user* session (api_id/api_hash + login),
    # leaving bot_token empty. Both are supported here.
    bot_token: str
    session_name: str

    # --- Filesystem locations ---
    workdir: str
    database_path: str
    log_dir: str

    # --- Admin / access control ---
    admin_user_ids: tuple[int, ...]

    # --- Logging ---
    log_level: str

    # --- Scanning behavior ---
    hash_chunk_size: int
    db_batch_commit_size: int
    db_batch_commit_interval_seconds: float
    scan_concurrency: int
    flood_sleep_threshold: int
    max_flood_wait_retries: int

    # --- Background maintenance ---
    maintenance_interval_seconds: int

    # --- Supported media kinds to hash/dedupe ---
    media_types: tuple[str, ...] = field(
        default_factory=lambda: ("video", "document", "animation", "audio", "photo")
    )

    @staticmethod
    def load(env_path: str | None = None) -> "Config":
        """
        Build a Config from environment variables (and an optional .env
        file). Raises ConfigError with an actionable message if anything
        required is missing or malformed.
        """
        project_root = Path(__file__).resolve().parent
        dotenv_path = Path(env_path) if env_path else project_root / ".env"
        _load_dotenv_if_present(dotenv_path)

        try:
            api_id = int(_require("API_ID"))
        except ValueError as e:
            raise ConfigError("API_ID must be an integer (your Telegram api_id).") from e
        api_hash = _require("API_HASH")

        bot_token = _optional("BOT_TOKEN", default="")
        session_name = _optional("SESSION_NAME", default="global_duplicate_bot")

        workdir = _optional("WORKDIR", default=str(project_root / "session"))
        database_path = _optional("DATABASE_PATH", default=str(project_root / "data" / "media_hashes.db"))
        log_dir = _optional("LOG_DIR", default=str(project_root / "logs"))

        Path(workdir).mkdir(parents=True, exist_ok=True)
        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
        Path(log_dir).mkdir(parents=True, exist_ok=True)

        admin_ids_raw = _optional("ADMIN_USER_IDS", default="")
        admin_user_ids = tuple(
            int(part.strip())
            for part in admin_ids_raw.split(",")
            if part.strip()
        )
        if not admin_user_ids:
            raise ConfigError(
                "ADMIN_USER_IDS must contain at least one Telegram user id "
                "(comma-separated) — this restricts who can control the bot."
            )

        log_level = _optional("LOG_LEVEL", default="INFO").upper()
        if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ConfigError(f"LOG_LEVEL must be one of DEBUG/INFO/WARNING/ERROR/CRITICAL, got: {log_level}")

        hash_chunk_size = _optional_int("HASH_CHUNK_SIZE", default=1024 * 1024)  # 1 MiB
        db_batch_commit_size = _optional_int("DB_BATCH_COMMIT_SIZE", default=200)
        db_batch_commit_interval = float(_optional_int("DB_BATCH_COMMIT_INTERVAL_MS", default=2000)) / 1000.0
        scan_concurrency = _optional_int("SCAN_CONCURRENCY", default=1)
        flood_sleep_threshold = _optional_int("FLOOD_SLEEP_THRESHOLD", default=60)
        max_flood_wait_retries = _optional_int("MAX_FLOOD_WAIT_RETRIES", default=10)
        maintenance_interval_seconds = _optional_int("MAINTENANCE_INTERVAL_SECONDS", default=60)

        if scan_concurrency < 1:
            raise ConfigError("SCAN_CONCURRENCY must be >= 1.")
        if hash_chunk_size < 4096:
            raise ConfigError("HASH_CHUNK_SIZE is too small (must be >= 4096 bytes).")

        return Config(
            api_id=api_id,
            api_hash=api_hash,
            bot_token=bot_token,
            session_name=session_name,
            workdir=workdir,
            database_path=database_path,
            log_dir=log_dir,
            admin_user_ids=admin_user_ids,
            log_level=log_level,
            hash_chunk_size=hash_chunk_size,
            db_batch_commit_size=db_batch_commit_size,
            db_batch_commit_interval_seconds=db_batch_commit_interval,
            scan_concurrency=scan_concurrency,
            flood_sleep_threshold=flood_sleep_threshold,
            max_flood_wait_retries=max_flood_wait_retries,
            maintenance_interval_seconds=maintenance_interval_seconds,
        )

    def is_admin(self, user_id: int) -> bool:
        return user_id in self.admin_user_ids


def _self_check() -> None:
    """
    Allows running `python config.py` standalone to verify environment
    variables are set correctly before starting the full bot.
    """
    try:
        cfg = Config.load()
    except ConfigError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    print("Configuration OK:")
    print(f"  session_name = {cfg.session_name}")
    print(f"  database_path = {cfg.database_path}")
    print(f"  log_dir = {cfg.log_dir}")
    print(f"  admin_user_ids = {cfg.admin_user_ids}")
    print(f"  scan_concurrency = {cfg.scan_concurrency}")


if __name__ == "__main__":
    _self_check()
