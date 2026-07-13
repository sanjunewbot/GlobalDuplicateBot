from alphagram import Client, filters, idle
from alphagram.errors import FloodWait
from alphagram.types import Message

import asyncio
import signal
import logging
import os
import sys

from database import Database
from scanner import GlobalScanner
from config import Config

from DA_Koyeb.health import emit_positive_health

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("GLOBAL-DUP")

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    print("BOT_TOKEN not found.")
    sys.exit(1)

app = Client(
    "GLOBAL-DUP",
    bot_token=BOT_TOKEN,
    use_default_api=True
)

stream_client = Client(
    "GLOBAL-DUP-STREAM",
    bot_token=BOT_TOKEN,
    use_default_api=True
)

db: Database | None = None
scanner: GlobalScanner | None = None

running_scan = False
stop_scan = False


async def startup():
    global db
    global scanner

    log.info("Initializing database...")
    db = Database("videos.db")
    await db.connect()

    log.info("Loading scanner...")
    scanner = GlobalScanner(
        client=app,
        stream_client=stream_client,
        database=db
    )

    log.info("Startup completed.")


async def shutdown():
    global db

    log.info("Stopping bot...")

    if db:
        await db.close()

    app.stop()
    stream_client.stop()


def stop_signal(*args):
    global stop_scan
    stop_scan = True
    log.warning("Stop signal received.")


signal.signal(signal.SIGINT, stop_signal)
signal.signal(signal.SIGTERM, stop_signal)


@app.on_message(filters.command("scan"))
async def scan_handler(client: Client, message: Message):
    global running_scan
    global stop_scan

    if running_scan:
        return await message.reply("⚠️ A scan is already running.")

    cmd = message.text.split()

    if len(cmd) < 2:
        return await message.reply("Usage:\n/scan chat_id")

    try:
        chat_id = int(cmd[1])
    except ValueError:
        return await message.reply("Invalid chat id.")

    running_scan = True
    stop_scan = False

    status = await message.reply("Preparing scan...")

    asyncio.create_task(
        run_scan(chat_id, status)
    )


@app.on_message(filters.command("stop"))
async def stop_handler(client: Client, message: Message):
    global stop_scan
    stop_scan = True
    await message.reply("Stopping after current media...")


@app.on_message(filters.command("status"))
async def status_handler(client: Client, message: Message):
    global running_scan
    global scanner

    if not running_scan:
        return await message.reply("No active scan.")

    stats = scanner.stats()

    text = (
        f"Videos : {stats['videos']}\n"
        f"Duplicates : {stats['duplicates']}\n"
        f"Hashed : {stats['hashed']}\n"
        f"Speed : {stats['speed']} videos/min\n"
        f"ETA : {stats['eta']}"
    )

    await message.reply(text)


async def run_scan(chat_id: int, status: Message):
    global running_scan
    global stop_scan
    global scanner

    try:
        await scanner.scan_chat(
            chat_id=chat_id,
            status=status,
            stop_event=lambda: stop_scan
        )
    except Exception as e:
        log.exception(e)
        try:
            await status.edit(f"❌ Scan failed\n\n{e}")
        except Exception:
            pass
    finally:
        running_scan = False
        stop_scan = False


@app.on_message(filters.command("resume"))
async def resume_handler(client: Client, message: Message):
    global running_scan

    if running_scan:
        return await message.reply("⚠️ A scan is already running.")

    row = await db.get_last_scan()

    if row is None:
        return await message.reply("No saved scan progress found.")

    chat_id = row["chat_id"]

    running_scan = True

    status = await message.reply(f"Resuming scan for {chat_id}...")

    asyncio.create_task(run_scan(chat_id, status))


@app.on_message(filters.command("resetdb"))
async def reset_db(client: Client, message: Message):
    await db.clear_hashes()
    await message.reply("✅ Hash database cleared.")


@app.on_message(filters.command("stats"))
async def stats_handler(client: Client, message: Message):
    total_hashes = await db.total_hashes()
    total_scanned = await db.total_scanned()
    total_duplicates = await db.total_duplicates()

    txt = (
        f"📊 Global Database\n\n"
        f"Hashes : {total_hashes:,}\n"
        f"Scanned : {total_scanned:,}\n"
        f"Duplicates : {total_duplicates:,}"
    )

    await message.reply(txt)


async def main():
    emit_positive_health()

    await startup()

    app.start()
    stream_client.start()

    log.info("Bot Started")

    await idle()

    await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
