import os


class Config:
    # Telegram Bot
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")

    # Database
    DATABASE_PATH = os.getenv("DATABASE_PATH", "videos.db")

    # Hashing
    HASH_ALGORITHM = "BLAKE3"

    # Scanner
    BATCH_SIZE = 200
    STATUS_UPDATE_EVERY = 25
    MAX_RETRIES = 5

    # Duplicate Policy
    DELETE_DUPLICATES = True
    KEEP_OLDEST = True

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # Performance
    STREAM_CLIENTS = 2
    HASH_CHUNK_SIZE = 1024 * 1024

    # Resume
    SAVE_PROGRESS_EVERY = 1

    # Health Check
    ENABLE_HEALTH_CHECK = True

    # Admins (comma separated Telegram IDs)
    ADMINS = [
        int(x)
        for x in os.getenv("ADMINS", "").split(",")
        if x.strip()
    ]

    @classmethod
    def is_admin(cls, user_id: int) -> bool:
        return user_id in cls.ADMINS
