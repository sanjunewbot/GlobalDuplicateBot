"""
Global Duplicate Scanner
Version: 2.0
"""

import asyncio
import logging

from alphagram import Client

from config import Config
from database import Database
from scanner import GlobalScanner
from DA_Koyeb.health import emit_positive_health

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)

log = logging.getLogger("GLOBAL-DUP")

bot = Client("GLOBAL-DUP", bot_token=Config.BOT_TOKEN, use_default_api=True)
stream_client = Client("GLOBAL-DUP-STREAM", bot_token=Config.BOT_TOKEN, use_default_api=True)

db = None
scanner = None
shutdown_event = asyncio.Event()

async def startup():
    global db, scanner
    db = Database(Config.DATABASE_PATH)
    await db.connect()
    scanner = GlobalScanner(client=bot, stream_client=stream_client, database=db)
    log.info("Startup completed.")

async def shutdown():
    global db, scanner
    if scanner:
        await scanner.stop()
    if db:
        await db.close()
    bot.stop()
    stream_client.stop()

def handle_signal(*_):
    shutdown_event.set()

if __name__ == "__main__":
    emit_positive_health()
    print("main.py Part 1")
