GlobalDuplicateBot
A Telegram bot that detects and removes duplicate media across multiple
channels, using a content hash rather than Telegram's file_id /
file_unique_id (which differ across re-uploads of the same file). It
remembers every unique file it has ever seen, forever, so a video posted
in Channel A will be caught and deleted if it's later reposted in Channel
B, C, or any other channel you've registered — even after a restart,
even years later.
How it works
You register channels with /addchannel.
/startscan walks each channel's history, newest message to oldest,
via Pyrogram's get_chat_history() iterator (never by guessing
message ids).
Every video/document/photo/audio/animation is streamed — never
downloaded to disk or fully buffered in memory — through
Client.stream_media() directly into a BLAKE3 hasher.
The resulting hash is looked up in a local SQLite database:
New hash → stored permanently, alongside which channel/message
it came from.
Hash already seen → the message is a duplicate and is deleted.
Progress is checkpointed after every single media item. If the
process crashes or is restarted, it resumes from the exact next
message — no rescanning, ever.
Requirements
Python 3.12+
A Telegram API ID and API hash from https://my.telegram.org
Either:
A user account session (recommended) — lets the bot read full
channel history the way a normal member/admin would, or
A bot token from @BotFather — note
that bot accounts have tighter history-access and rate limits, and
generally need to be a channel admin to delete messages there.
Setup
Bash
Create a .env file in the project root (or export these as real
environment variables):
Dotenv
See config.py for every tunable (batch commit size, hash chunk size,
scan concurrency, FloodWait retry limits, etc.) — all optional with
sensible defaults.
Then run:
Bash
The first run will prompt for Telegram login (if using a user session)
and create the SQLite database, session file, and log directory
automatically.
Commands
All commands are restricted to the user IDs listed in ADMIN_USER_IDS.
Command
Description
/addchannel <chat_id>
Register a channel for scanning.
/removechannel <chat_id>
Unregister a channel and clear its progress checkpoint.
/listchannels
List all registered channels and their status.
/startscan
Enqueue all pending/incomplete channels and begin scanning.
/pause
Pause after the current message finishes.
/resume
Resume a paused scan.
/status
Live progress: current channel, speed, ETA, queue size, db size.
/stats
All-time totals: videos scanned, duplicates removed, dedup rate.
/resetdb
Wipe the entire database. Requires /resetdb confirm.
Channel IDs are the numeric Telegram chat ID (e.g. -1001234567890 for
a supergroup/channel) — forward a message from the channel to
@JsonDumpBot or similar if you don't already
know it.
Project structure
Code
Database schema
media_hashes — hash (PK), chat_id, message_id, file_size,
media_type, created_at. The permanent global record of every
unique file ever seen.
progress — chat_id (PK), last_message_id. Per-channel resume
checkpoint, updated after every processed message.
channels — chat_id (PK), title, status
(pending/scanning/completed/error), add_seq (insertion order).
stats — simple scanned / duplicates counters, persisted across
restarts.
The database uses WAL mode and batched commits (default: every 200
inserts or 2 seconds, whichever comes first) so a scan of 100,000+
videos doesn't fsync on every single file.
Operational notes
Admin rights: to delete a duplicate in a channel, the account
running the bot must have delete permissions there.
FloodWait: handled automatically with retry + backoff throughout
(client startup, history iteration, hashing, and message deletion).
Resuming: you do not need to run /startscan again after a
restart — main.py calls resume_on_startup() automatically, which
re-queues any channel left pending, scanning, or error.
Logs: written to the directory in LOG_DIR (default ./logs),
rotating daily, retained for 14 days by default.
Scaling: the design (streaming hashes, batched commits, indexed
lookups, checkpointed progress) targets 100,000+ videos comfortably
and is architected to extend to millions; actual throughput will be
bounded by Telegram's rate limits (FloodWait), not by this bot's
logic.
Disclaimer
Only point this at channels you own or administer, and make sure the
account running it has the rights Telegram requires to read history
and delete messages there. Deletions performed by /startscan are
immediate and are not reversible by this bot.
