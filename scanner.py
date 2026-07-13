import asyncio
import time

from alphagram.errors import FloodWait

from hasher import MediaHasher


class GlobalScanner:

    def __init__(self, client, stream_client, database):
        self.client = client
        self.stream_client = stream_client
        self.db = database
        self.hasher = MediaHasher(stream_client)

        self._videos = 0
        self._duplicates = 0
        self._hashed = 0
        self._start = 0

    def stats(self):
        elapsed = max(time.time() - self._start, 1)
        speed = round((self._videos / elapsed) * 60, 2)

        return {
            "videos": self._videos,
            "duplicates": self._duplicates,
            "hashed": self._hashed,
            "speed": speed,
            "eta": "Calculating..."
        }

    async def scan_chat(self, chat_id, status, stop_event):

        self._videos = 0
        self._duplicates = 0
        self._hashed = 0
        self._start = time.time()

        last = await self.db.get_progress(chat_id)

        current = last + 1

        while True:

            if stop_event():
                await status.edit("🛑 Scan stopped.")
                return

            try:
                msg = await self.client.get_messages(chat_id, current)

            except FloodWait as e:
                await asyncio.sleep(e.value)
                continue

            except Exception:
                current += 1
                continue

            if not msg:
                break

            current += 1

            if not getattr(msg, "media", None):
                await self.db.save_progress(chat_id, msg.id)
                continue

            try:
                h = await self.hasher.hash_message(msg)
                self._hashed += 1

                row = await self.db.hash_exists(h)

                if row:
                    try:
                        await self.client.delete_messages(chat_id, msg.id)
                        self._duplicates += 1
                        await self.db.increment_duplicates()
                    except Exception:
                        pass

                else:
                    size = 0
                    mtype = "unknown"

                    if msg.video:
                        size = msg.video.file_size
                        mtype = "video"
                    elif msg.document:
                        size = msg.document.file_size
                        mtype = "document"
                    elif msg.photo:
                        size = msg.photo.file_size
                        mtype = "photo"
                    elif msg.audio:
                        size = msg.audio.file_size
                        mtype = "audio"

                    await self.db.insert_hash(
                        h,
                        chat_id,
                        msg.id,
                        size,
                        mtype
                    )

                self._videos += 1

                await self.db.increment_scanned()
                await self.db.save_progress(chat_id, msg.id)

                if self._videos % 25 == 0:
                    s = self.stats()
                    await status.edit(
                        f"Scanning...\n\n"
                        f"Processed: {s['videos']}\n"
                        f"Duplicates: {s['duplicates']}\n"
                        f"Speed: {s['speed']} videos/min"
                    )

            except Exception as e:
                print(e)

        await status.edit(
            f"✅ Finished\n\n"
            f"Scanned: {self._videos}\n"
            f"Duplicates: {self._duplicates}"
        )
