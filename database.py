import aiosqlite

class Database:
    def __init__(self, db_path="videos.db"):
        self.db_path = db_path
        self.conn = None

    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.executescript("""
CREATE TABLE IF NOT EXISTS media_hashes(
 hash TEXT PRIMARY KEY,
 chat_id INTEGER NOT NULL,
 message_id INTEGER NOT NULL,
 file_size INTEGER,
 media_type TEXT
);
CREATE TABLE IF NOT EXISTS progress(
 chat_id INTEGER PRIMARY KEY,
 last_message_id INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS channels(
 chat_id INTEGER PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS stats(
 id INTEGER PRIMARY KEY CHECK(id=1),
 scanned INTEGER DEFAULT 0,
 duplicates INTEGER DEFAULT 0
);
INSERT OR IGNORE INTO stats(id,scanned,duplicates) VALUES(1,0,0);
""")
        await self.conn.commit()

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def hash_exists(self, h):
        c=await self.conn.execute("SELECT * FROM media_hashes WHERE hash=?",(h,))
        r=await c.fetchone()
        await c.close()
        return r

    async def insert_hash(self,h,chat_id,message_id,file_size,media_type):
        await self.conn.execute(
            "INSERT OR IGNORE INTO media_hashes VALUES(?,?,?,?,?)",
            (h,chat_id,message_id,file_size,media_type)
        )
        await self.conn.commit()

    async def save_progress(self,chat_id,message_id):
        await self.conn.execute("""
INSERT INTO progress(chat_id,last_message_id)
VALUES(?,?)
ON CONFLICT(chat_id)
DO UPDATE SET last_message_id=excluded.last_message_id
""",(chat_id,message_id))
        await self.conn.commit()

    async def get_progress(self,chat_id):
        c=await self.conn.execute("SELECT last_message_id FROM progress WHERE chat_id=?",(chat_id,))
        r=await c.fetchone()
        await c.close()
        return r["last_message_id"] if r else 0

    async def get_last_scan(self):
        c=await self.conn.execute("SELECT chat_id,last_message_id FROM progress ORDER BY rowid DESC LIMIT 1")
        r=await c.fetchone()
        await c.close()
        return r

    async def add_channel(self,chat_id):
        await self.conn.execute("INSERT OR IGNORE INTO channels VALUES(?)",(chat_id,))
        await self.conn.commit()

    async def remove_channel(self,chat_id):
        await self.conn.execute("DELETE FROM channels WHERE chat_id=?",(chat_id,))
        await self.conn.commit()

    async def list_channels(self):
        c=await self.conn.execute("SELECT chat_id FROM channels")
        rows=await c.fetchall()
        await c.close()
        return [x["chat_id"] for x in rows]

    async def increment_scanned(self):
        await self.conn.execute("UPDATE stats SET scanned=scanned+1 WHERE id=1")
        await self.conn.commit()

    async def increment_duplicates(self):
        await self.conn.execute("UPDATE stats SET duplicates=duplicates+1 WHERE id=1")
        await self.conn.commit()

    async def total_hashes(self):
        c=await self.conn.execute("SELECT COUNT(*) c FROM media_hashes")
        r=await c.fetchone()
        await c.close()
        return r["c"]

    async def total_scanned(self):
        c=await self.conn.execute("SELECT scanned FROM stats WHERE id=1")
        r=await c.fetchone()
        await c.close()
        return r["scanned"]

    async def total_duplicates(self):
        c=await self.conn.execute("SELECT duplicates FROM stats WHERE id=1")
        r=await c.fetchone()
        await c.close()
        return r["duplicates"]

    async def clear_hashes(self):
        await self.conn.execute("DELETE FROM media_hashes")
        await self.conn.execute("DELETE FROM progress")
        await self.conn.execute("DELETE FROM channels")
        await self.conn.execute("UPDATE stats SET scanned=0,duplicates=0 WHERE id=1")
        await self.conn.commit()
