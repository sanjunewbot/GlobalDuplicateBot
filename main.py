from alphagram import Client, filters, idle
from alphagram.types import Message
import asyncio, logging, sys
from database import Database
from scanner import GlobalScanner
from config import Config
from DA_Koyeb.health import emit_positive_health

logging.basicConfig(level=getattr(logging, Config.LOG_LEVEL, logging.INFO))
log = logging.getLogger("GLOBAL-DUP")

if not Config.BOT_TOKEN:
    print("BOT_TOKEN not found.")
    sys.exit(1)

app = Client("GLOBAL-DUP", bot_token=Config.BOT_TOKEN, use_default_api=True)
stream_client = Client("GLOBAL-DUP-STREAM", bot_token=Config.BOT_TOKEN, use_default_api=True)

db = Database(Config.DATABASE_PATH)
scanner = None
running_scan = False
stop_scan = False

async def startup():
    global scanner
    await db.connect()
    scanner = GlobalScanner(app, stream_client, db)

async def shutdown():
    await db.close()
    app.stop()
    stream_client.stop()

async def run_scan(chat_id:int,status:Message):
    global running_scan, stop_scan
    try:
        await scanner.scan_chat(chat_id=chat_id,status=status,stop_event=lambda: stop_scan)
    finally:
        running_scan=False
        stop_scan=False

@app.on_message(filters.command("scan"))
async def scan_handler(client, message):
    global running_scan, stop_scan
    if running_scan:
        return await message.reply("Scan already running.")
    args=message.text.split()
    if len(args)!=2:
        return await message.reply("Usage: /scan chat_id")
    running_scan=True
    stop_scan=False
    status=await message.reply("Starting scan...")
    asyncio.create_task(run_scan(int(args[1]),status))

@app.on_message(filters.command("stop"))
async def stop_handler(client,message):
    global stop_scan
    stop_scan=True
    await message.reply("Stopping...")

@app.on_message(filters.command("status"))
async def status_handler(client,message):
    if not running_scan:
        return await message.reply("No active scan.")
    s=scanner.stats()
    await message.reply(f"Videos: {s['videos']}\nDuplicates: {s['duplicates']}\nHashed: {s['hashed']}\nSpeed: {s['speed']} videos/min")

if __name__=="__main__":
    emit_positive_health()
    app.start()
    stream_client.start()
    loop=asyncio.get_event_loop()
    loop.run_until_complete(startup())
    try:
        idle()
    finally:
        loop.run_until_complete(shutdown())
