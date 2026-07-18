
# GlobalDuplicateBot

A Telegram bot that detects and removes duplicate media **across multiple
channels**, using a content hash rather than Telegram's `file_id` /
`file_unique_id` (which differ across re-uploads of the same file). It
remembers every unique file it has ever seen, forever, so a video posted
in Channel A will be caught and deleted if it's later reposted in Channel
B, C, or any other channel you've registered — even after a restart,
even years later.

## How it works

1. You register channels with `/addchannel`.
2. `/startscan` walks each channel's history, newest message to oldest,
   via Pyrogram's `get_chat_history()` iterator (never by guessing
   message ids).
3. Every video/document/photo/audio/animation is streamed — never
   downloaded to disk or fully buffered in memory — through
   `Client.stream_media()` directly into a BLAKE3 hasher.
4. The resulting hash is looked up in a local SQLite database:
   - **New hash** → stored permanently, alongside which channel/message
     it came from.
   - **Hash already seen** → the message is a duplicate and is deleted.
5. Progress is checkpointed after every single media item. If the
   process crashes or is restarted, it resumes from the exact next
   message — no rescanning, ever.

## Requirements

- Python 3.12+
- A Telegram **API ID and API hash** from <https://my.telegram.org>
- Either:
  - A **user account session** (recommended) — lets the bot read full
    channel history the way a normal member/admin would, or
  - A **bot token** from [@BotFather](https://t.me/BotFather) — note
    that bot accounts have tighter history-access and rate limits, and
    generally need to be a channel admin to delete messages there.

## Setup

```bash
git clone <your-repo-url> GlobalDuplicateBot
cd GlobalDuplicateBot
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file in the project root (or export these as real
environment variables):

```dotenv
API_ID=1234567
API_HASH=your_api_hash_here

# Leave BOT_TOKEN empty to log in as a user account instead (recommended
# for scanning full channel history). Pyrogram will prompt for your
# phone number / login code the first time you run main.py.
BOT_TOKEN=

SESSION_NAME=global_duplicate_bot

# Comma-separated Telegram user IDs allowed to control the bot.
ADMIN_USER_IDS=111111111,222222222

# MongoDB connection string (e.g. from a free MongoDB Atlas M0 cluster —
# see https://www.mongodb.com/cloud/atlas/register). No credit card
# required for the free tier.
MONGODB_URI=mongodb+srv://user:password@cluster0.xxxxx.mongodb.net/
MONGODB_DB_NAME=global_duplicate_bot

LOG_LEVEL=INFO
```

See `config.py` for every tunable (batch commit size, hash chunk size,
scan concurrency, FloodWait retry limits, etc.) — all optional with
sensible defaults.

Then run:

```bash
python3 main.py
```

The first run will prompt for Telegram login (if using a user session)
and create the SQLite database, session file, and log directory
automatically.

## Commands

All commands are restricted to the user IDs listed in `ADMIN_USER_IDS`.

| Command | Description |
|---|---|
| `/addchannel <chat_id>` | Register a channel for scanning. |
| `/removechannel <chat_id>` | Unregister a channel and clear its progress checkpoint. |
| `/listchannels` | List all registered channels and their status. |
| `/startscan` | Enqueue all pending/incomplete channels and begin scanning. |
| `/pause` | Pause after the current message finishes. |
| `/resume` | Resume a paused scan. |
| `/status` | Live progress: current channel, speed, ETA, queue size, db size. |
| `/stats` | All-time totals: videos scanned, duplicates removed, dedup rate. |
| `/resetdb` | Wipe the entire database. Requires `/resetdb confirm`. |

Channel IDs are the numeric Telegram chat ID (e.g. `-1001234567890` for
a supergroup/channel) — forward a message from the channel to
[@JsonDumpBot](https://t.me/JsonDumpBot) or similar if you don't already
know it.

## Project structure

```
GlobalDuplicateBot/
├── main.py         Bot startup, lifecycle, signal handling, auto-restart
├── config.py        Typed configuration loaded from environment / .env
├── logger.py        Console + daily rotating file logging
├── database.py       Async SQLite (WAL mode) wrapper: hashes, progress, channels, stats
├── hasher.py         Streaming BLAKE3 hashing via stream_media()
├── scanner.py        Queue manager + per-channel scan/dedup/delete logic
├── commands.py        All /command handlers
├── progress.py        /status and /stats text formatting
├── requirements.txt
└── README.md
```

## Database

Uses MongoDB (e.g. a free MongoDB Atlas M0 cluster) rather than a local
file, so the bot's memory survives redeploys/restarts even on hosts
with no persistent disk (like a free-tier Koyeb web service).

- **media_hashes** — `_id` (the hash itself), `chat_id`, `message_id`,
  `file_size`, `media_type`. The permanent global record of every
  unique file ever seen. Using the hash as `_id` gives a free unique
  index — MongoDB itself refuses a second document with the same hash.
- **progress** — `_id` (chat_id), `last_message_id`. Per-channel resume
  checkpoint, updated after every processed message.
- **channels** — `_id` (chat_id), `title`, `status`
  (`pending`/`scanning`/`completed`/`error`), `add_seq` (insertion order,
  from an atomic counter — guaranteed ordering regardless of chat_id
  value or timestamp collisions).
- **stats** — simple `scanned` / `duplicates` counters, persisted across
  restarts.

Hash inserts and progress checkpoints are batched in memory and flushed
via `bulk_write()` (default: every 200 inserts or 2 seconds, whichever
comes first) so a scan of 100,000+ videos doesn't do one network
round-trip per file.

## Operational notes

- **Admin rights**: to delete a duplicate in a channel, the account
  running the bot must have delete permissions there.
- **FloodWait**: handled automatically with retry + backoff throughout
  (client startup, history iteration, hashing, and message deletion).
- **Resuming**: you do not need to run `/startscan` again after a
  restart — `main.py` calls `resume_on_startup()` automatically, which
  re-queues any channel left `pending`, `scanning`, or `error`.
- **Logs**: written to the directory in `LOG_DIR` (default `./logs`),
  rotating daily, retained for 14 days by default.
- **Scaling**: the design (streaming hashes, batched commits, indexed
  lookups, checkpointed progress) targets 100,000+ videos comfortably
  and is architected to extend to millions; actual throughput will be
  bounded by Telegram's rate limits (FloodWait), not by this bot's
  logic.

## Disclaimer

Only point this at channels you own or administer, and make sure the
account running it has the rights Telegram requires to read history
and delete messages there. Deletions performed by `/startscan` are
immediate and are not reversible by this bot.
