#!/usr/bin/env python3
import base64
import hashlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import requests as rq
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, FileResponse

# ─── CONFIG ───
DATABASE_URL = os.environ.get("DATABASE_URL", "")
AVATAR_DIR = os.path.expanduser("~/avatars")
UPLOAD_DIR = os.path.expanduser("~/uploads")
STICKER_DIR = os.path.expanduser("~/stickers")
HTML_FILE = os.path.join(os.path.dirname(__file__), "index.html")
PORT = int(os.environ.get("PORT", 10000))

ALLOWED_MIME = {
    "jpeg": "jpg", "jpg": "jpg", "png": "png", "gif": "gif",
    "webp": "webp", "mp3": "mp3", "ogg": "ogg", "webm": "webm",
    "mp4": "mp4", "m4a": "m4a", "wav": "wav",
}

# Пул соединений
pool: asyncpg.Pool | None = None


# ─── POOL ───
async def get_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        # Render даёт URL вида postgres://...
        # asyncpg требует postgresql://...
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        pool = await asyncpg.create_pool(url, min_size=1, max_size=10)
    return pool


# ─── SCHEMA ───
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    first_name TEXT,
    bio TEXT,
    avatar TEXT,
    created_at TIMESTAMP,
    last_seen TIMESTAMP
);
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    from_user TEXT,
    to_user TEXT,
    text TEXT,
    ts TIMESTAMP,
    read BOOLEAN DEFAULT FALSE,
    deleted BOOLEAN DEFAULT FALSE,
    edited BOOLEAN DEFAULT FALSE,
    edited_at TIMESTAMP,
    reply_to INTEGER,
    forwarded_from TEXT,
    media_url TEXT,
    media_type TEXT
);
CREATE TABLE IF NOT EXISTS gifts (
    id SERIAL PRIMARY KEY,
    from_user TEXT,
    to_user TEXT,
    gift_name TEXT,
    gift_emoji TEXT,
    stars INTEGER,
    ts TIMESTAMP
);
CREATE TABLE IF NOT EXISTS usernames (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    username TEXT UNIQUE NOT NULL,
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS reactions (
    id SERIAL PRIMARY KEY,
    msg_id INTEGER,
    chat_id TEXT,
    user_id TEXT,
    emoji TEXT,
    ts TIMESTAMP,
    UNIQUE(msg_id, user_id, emoji)
);
CREATE TABLE IF NOT EXISTS pinned (
    chat_id TEXT PRIMARY KEY,
    msg_id INTEGER,
    pinned_by TEXT,
    pinned_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS contacts (
    owner TEXT,
    contact TEXT,
    ts TIMESTAMP,
    UNIQUE(owner, contact)
);
CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id TEXT,
    created_at TIMESTAMP,
    last_ip TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_pair ON messages(from_user, to_user, ts);
CREATE INDEX IF NOT EXISTS idx_messages_unread ON messages(to_user, read) WHERE read = FALSE;
CREATE INDEX IF NOT EXISTS idx_sessions_ip ON sessions(last_ip);
"""


async def init_db():
    p = await get_pool()
    async with p.acquire() as con:
        await con.execute(SCHEMA)


# ─── HELPERS ───
def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def clean_u(s: str) -> str:
    return (s or "").strip().lower().lstrip("@").strip()


def hash_pw(pw: str, salt: bytes | None = None) -> str:
    salt = salt or os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, 100_000)
    return salt.hex() + ":" + h.hex()


def check_pw(pw: str, stored: str) -> bool:
    try:
        salt_hex, h_hex = stored.split(":")
        salt = bytes.fromhex(salt_hex)
        calc = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, 100_000).hex()
        return calc == h_hex
    except Exception:
        return False


def save_file(folder, name_prefix, b64data):
    if not b64data or "," not in b64data:
        return None
    try:
        header, b64 = b64data.split(",", 1)
        mime = "jpg"
        if ":" in header and ";" in header:
            mime = header.split(":", 1)[1].split(";", 1)[0]
            if "/" in mime:
                mime = mime.split("/", 1)[1]
        mime = mime.lower().strip()
        ext = ALLOWED_MIME.get(mime)
        if not ext:
            print(f"[file] отвергнут MIME: {mime}")
            return None
        raw = base64.b64decode(b64)
        if len(raw) > 15 * 1024 * 1024:
            return None
        fname = f"{name_prefix}_{uuid.uuid4().hex[:12]}.{ext}"
        with open(os.path.join(folder, fname), "wb") as f:
            f.write(raw)
        return fname
    except Exception as e:
        print(f"[file] {e}")
        return None


clients: dict[str, WebSocket] = {}


async def send_to(uid, payload):
    ws = clients.get(uid)
    if ws:
        try:
            await ws.send_text(json.dumps(payload, default=str))
            return True
        except Exception:
            pass
    return False


# ─── LIFESPAN ───
@asynccontextmanager
async def lifespan(app: FastAPI):
    for d in (AVATAR_DIR, UPLOAD_DIR, STICKER_DIR):
        os.makedirs(d, exist_ok=True)
    if not DATABASE_URL:
        print("[server] ⚠️ DATABASE_URL не задан! Установите переменную окружения.")
    else:
        await init_db()
        print("[server] БД инициализирована")
    print(f"[server] порт {PORT}")
    yield
    global pool
    if pool:
        await pool.close()


app = FastAPI(lifespan=lifespan)


# ─── STATIC ───
@app.get("/")
def root():
    if os.path.exists(HTML_FILE):
        return FileResponse(HTML_FILE)
    return HTMLResponse("<h1>Messenger</h1>")


@app.get("/healthz")
def healthz():
    return {"ok": True, "db": bool(DATABASE_URL)}


@app.get("/avatars/{fname}")
def av_file(fname: str):
    p = os.path.join(AVATAR_DIR, fname)
    if os.path.exists(p):
        return FileResponse(p)
    return HTMLResponse("", status_code=404)


@app.get("/uploads/{fname}")
def up_file(fname: str):
    p = os.path.join(UPLOAD_DIR, fname)
    if os.path.exists(p):
        return FileResponse(p)
    return HTMLResponse("", status_code=404)


@app.get("/stickers/{fname}")
def st_file(fname: str):
    p = os.path.join(STICKER_DIR, fname)
    if os.path.exists(p):
        return FileResponse(p)
    return HTMLResponse("", status_code=404)


@app.get("/geo")
def geo(request: Request):
    try:
        ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
        if not ip or ip.startswith("127.") or ip.startswith("10."):
            ip = request.client.host
        r = rq.get(f"https://ipwho.is/{ip}", timeout=5)
        data = r.json()
        if data.get("success"):
            return {
                "country": data.get("country", ""),
                "code": data.get("country_code", ""),
                "timezone": (data.get("timezone") or {}).get("id", "UTC"),
                "offset": (data.get("timezone") or {}).get("utc", "+00:00"),
            }
    except Exception as e:
        print(f"[geo] {e}")
    return {"country": "Unknown", "code": "", "timezone": "UTC", "offset": "+00:00"}


# ─── WS ───
@app.websocket("/ws")
async def ws_ep(websocket: WebSocket):
    await websocket.accept()
    user_id = None
    p = await get_pool()
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except Exception:
                await websocket.send_text(json.dumps({"type": "error", "msg": "bad json"}))
                continue
            cmd = data.get("cmd")

            if cmd == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue

            # ─── RESUME ───
            if cmd == "resume":
                tok = data.get("token") or ""
                async with p.acquire() as con:
                    row = await con.fetchrow(
                        "SELECT user_id FROM sessions WHERE token=$1", tok
                    )
                if not row:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "session expired"}))
                    continue
                uid = row["user_id"]
                ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                     (websocket.client.host if websocket.client else "")
                async with p.acquire() as con:
                    await con.execute(
                        "UPDATE sessions SET last_ip=$1 WHERE token=$2", ip, tok
                    )
                user_id = uid
                clients[uid] = websocket
                await websocket.send_text(json.dumps({
                    "type": "logged_in", "user_id": uid, "token": tok, "resumed": True,
                }))
                for u in list(clients.keys()):
                    if u != uid:
                        await send_to(u, {"type": "user_online", "user": uid})
                continue

            # ─── REGISTER ───
            if cmd == "register":
                u = clean_u(data.get("username"))
                pw = data.get("password") or ""
                if not u or not pw or len(u) < 4:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "username 4+"}))
                    continue
                if not u.replace("_", "").isalnum():
                    await websocket.send_text(json.dumps({"type": "error", "msg": "только a-z 0-9 _"}))
                    continue
                try:
                    async with p.acquire() as con:
                        async with con.transaction():
                            await con.execute(
                                """INSERT INTO users
                                   (user_id,username,password,first_name,created_at,last_seen)
                                   VALUES ($1,$2,$3,$4,$5,$6)""",
                                u, u, hash_pw(pw), u, now_utc(), now_utc(),
                            )
                            await con.execute(
                                """INSERT INTO usernames
                                   (user_id,username,is_primary,created_at)
                                   VALUES ($1,$2,TRUE,$3)""",
                                u, u, now_utc(),
                            )
                            tok = uuid.uuid4().hex
                            ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                                 (websocket.client.host if websocket.client else "")
                            await con.execute(
                                """INSERT INTO sessions (token,user_id,created_at,last_ip)
                                   VALUES ($1,$2,$3,$4)""",
                                tok, u, now_utc(), ip,
                            )
                    user_id = u
                    clients[u] = websocket
                    await websocket.send_text(json.dumps({
                        "type": "registered", "user_id": u, "token": tok,
                    }))
                except asyncpg.UniqueViolationError:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "занято"}))
                continue

            # ─── LOGIN ───
            if cmd == "login":
                u = clean_u(data.get("username"))
                pw = data.get("password") or ""
                async with p.acquire() as con:
                    row = await con.fetchrow(
                        "SELECT password FROM users WHERE username=$1", u
                    )
                if not row or not check_pw(pw, row["password"]):
                    await websocket.send_text(json.dumps({"type": "error", "msg": "неверный"}))
                    continue
                tok = uuid.uuid4().hex
                ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                     (websocket.client.host if websocket.client else "")
                async with p.acquire() as con:
                    await con.execute(
                        "UPDATE users SET last_seen=$1 WHERE username=$2", now_utc(), u
                    )
                    await con.execute(
                        """INSERT INTO sessions (token,user_id,created_at,last_ip)
                           VALUES ($1,$2,$3,$4)""",
                        tok, u, now_utc(), ip,
                    )
                user_id = u
                clients[u] = websocket
                await websocket.send_text(json.dumps({
                    "type": "logged_in", "user_id": u, "token": tok,
                }))
                for uid in list(clients.keys()):
                    if uid != u:
                        await send_to(uid, {"type": "user_online", "user": u})
                continue

            if not user_id:
                await websocket.send_text(json.dumps({"type": "error", "msg": "сначала login"}))
                continue

            # ─── USERS ───
            if cmd == "users":
                async with p.acquire() as con:
                    rows = await con.fetch(
                        "SELECT username,first_name,avatar,last_seen FROM users ORDER BY username"
                    )
                lst = []
                for r in rows:
                    online = r["username"] in clients
                    lst.append({
                        "u": r["username"],
                        "n": r["first_name"] or r["username"],
                        "av": r["avatar"] or "",
                        "online": online,
                        "ls": r["last_seen"].isoformat() if r["last_seen"] else None,
                    })
                await websocket.send_text(json.dumps({"type": "users", "list": lst}))
                continue

            # ─── SEARCH ───
            if cmd == "search_users":
                q = clean_u(data.get("q"))
                async with p.acquire() as con:
                    rows = await con.fetch(
                        """SELECT username,first_name,avatar FROM users
                           WHERE (LOWER(username) LIKE $1 OR LOWER(first_name) LIKE $1)
                             AND username != $2
                           ORDER BY username LIMIT 30""",
                        f"%{q}%", user_id,
                    )
                await websocket.send_text(json.dumps({"type": "search_results", "list": [
                    {"u": r["username"], "n": r["first_name"] or r["username"], "av": r["avatar"] or ""}
                    for r in rows
                ]}))
                continue

            # ─── ADD CONTACT ───
            if cmd == "add_contact":
                target = clean_u(data.get("user"))
                if not target or target == user_id:
                    continue
                async with p.acquire() as con:
                    exists = await con.fetchval(
                        "SELECT 1 FROM users WHERE username=$1", target
                    )
                    if not exists:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "нет юзера"}))
                        continue
                    await con.execute(
                        """INSERT INTO contacts (owner,contact,ts) VALUES ($1,$2,$3)
                           ON CONFLICT (owner,contact) DO NOTHING""",
                        user_id, target, now_utc(),
                    )
                await websocket.send_text(json.dumps({"type": "contact_added", "user": target}))
                continue

            # ─── SEND ───
            if cmd == "send":
                to = clean_u(data.get("to"))
                text = data.get("text") or ""
                reply_to = data.get("reply_to")
                forwarded_from = data.get("forwarded_from")
                media_data = data.get("media_data")
                media_type = data.get("media_type")
                media_url = data.get("media_url")
                if not to:
                    continue
                if not text and not media_data and not media_url:
                    continue
                ts = now_utc()
                if media_data:
                    fname = save_file(UPLOAD_DIR, user_id, media_data)
                    if fname:
                        media_url = f"/uploads/{fname}"
                async with p.acquire() as con:
                    msg_id = await con.fetchval(
                        """INSERT INTO messages
                           (from_user,to_user,text,ts,reply_to,forwarded_from,media_url,media_type)
                           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                           RETURNING id""",
                        user_id, to, text, ts, reply_to, forwarded_from, media_url, media_type,
                    )
                payload = {
                    "type": "msg", "id": msg_id, "from": user_id, "text": text,
                    "ts": ts.isoformat(),
                    "reply_to": reply_to, "forwarded_from": forwarded_from,
                    "media_url": media_url, "media_type": media_type,
                }
                delivered = await send_to(to, payload)
                await websocket.send_text(json.dumps({
                    "type": "sent", "id": msg_id, "to": to, "text": text,
                    "ts": ts.isoformat(),
                    "reply_to": reply_to, "forwarded_from": forwarded_from,
                    "media_url": media_url, "media_type": media_type,
                    "delivered": delivered,
                }))
                continue

            # ─── MARK READ ───
            if cmd == "mark_read":
                w = clean_u(data.get("with"))
                if not w:
                    continue
                async with p.acquire() as con:
                    ids = [r["id"] for r in await con.fetch(
                        "SELECT id FROM messages WHERE from_user=$1 AND to_user=$2 AND read=FALSE",
                        w, user_id,
                    )]
                    if ids:
                        await con.execute(
                            "UPDATE messages SET read=TRUE WHERE from_user=$1 AND to_user=$2 AND read=FALSE",
                            w, user_id,
                        )
                if ids:
                    await send_to(w, {"type": "msg_read", "ids": ids, "by": user_id})
                continue

            # ─── EDIT ───
            if cmd == "edit_msg":
                mid = data.get("msg_id")
                new_text = (data.get("text") or "").strip()
                if not mid or not new_text:
                    continue
                async with p.acquire() as con:
                    row = await con.fetchrow(
                        "SELECT from_user,to_user FROM messages WHERE id=$1", mid
                    )
                    if not row or row["from_user"] != user_id:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "нельзя редактировать"}))
                        continue
                    await con.execute(
                        "UPDATE messages SET text=$1, edited=TRUE, edited_at=$2 WHERE id=$3",
                        new_text, now_utc(), mid,
                    )
                other = row["to_user"] if row["from_user"] == user_id else row["from_user"]
                await send_to(other, {"type": "msg_edited", "msg_id": mid, "text": new_text})
                await websocket.send_text(json.dumps({"type": "msg_edited", "msg_id": mid, "text": new_text}))
                continue

            # ─── DELETE ───
            if cmd == "delete_msg":
                mid = data.get("msg_id")
                if not mid:
                    continue
                async with p.acquire() as con:
                    row = await con.fetchrow(
                        "SELECT from_user,to_user FROM messages WHERE id=$1", mid
                    )
                    if not row:
                        continue
                    await con.execute(
                        "UPDATE messages SET deleted=TRUE, text='' WHERE id=$1", mid
                    )
                other = row["to_user"] if row["from_user"] == user_id else row["from_user"]
                await send_to(other, {"type": "msg_deleted", "msg_id": mid})
                await websocket.send_text(json.dumps({"type": "msg_deleted", "msg_id": mid}))
                continue

            # ─── REACTION ───
            if cmd == "react":
                mid = data.get("msg_id")
                emoji = data.get("emoji")
                chat_with = clean_u(data.get("chat_with"))
                if not mid or not emoji or not chat_with:
                    continue
                async with p.acquire() as con:
                    existing = await con.fetchval(
                        "SELECT id FROM reactions WHERE msg_id=$1 AND user_id=$2 AND emoji=$3",
                        mid, user_id, emoji,
                    )
                    if existing:
                        await con.execute("DELETE FROM reactions WHERE id=$1", existing)
                    else:
                        await con.execute(
                            """INSERT INTO reactions (msg_id,chat_id,user_id,emoji,ts)
                               VALUES ($1,$2,$3,$4,$5)""",
                            mid, chat_with, user_id, emoji, now_utc(),
                        )
                    rows = await con.fetch(
                        "SELECT emoji, COUNT(*) AS c FROM reactions WHERE msg_id=$1 GROUP BY emoji",
                        mid,
                    )
                payload = {
                    "type": "reactions_updated", "msg_id": mid,
                    "reactions": [{"emoji": r["emoji"], "count": r["c"]} for r in rows],
                }
                await send_to(chat_with, payload)
                await websocket.send_text(json.dumps(payload))
                continue

            # ─── PIN ───
            if cmd == "pin_msg":
                mid = data.get("msg_id")
                chat_with = clean_u(data.get("chat_with"))
                if not mid or not chat_with:
                    continue
                async with p.acquire() as con:
                    await con.execute(
                        """INSERT INTO pinned (chat_id, msg_id, pinned_by, pinned_at)
                           VALUES ($1,$2,$3,$4)
                           ON CONFLICT (chat_id) DO UPDATE
                           SET msg_id=EXCLUDED.msg_id,
                               pinned_by=EXCLUDED.pinned_by,
                               pinned_at=EXCLUDED.pinned_at""",
                        chat_with, mid, user_id, now_utc(),
                    )
                    row = await con.fetchrow(
                        "SELECT from_user,text FROM messages WHERE id=$1", mid
                    )
                if row:
                    payload = {"type": "msg_pinned", "msg_id": mid,
                               "from": row["from_user"], "text": row["text"]}
                    await send_to(chat_with, payload)
                    await websocket.send_text(json.dumps(payload))
                continue

            if cmd == "unpin_msg":
                chat_with = clean_u(data.get("chat_with"))
                async with p.acquire() as con:
                    await con.execute("DELETE FROM pinned WHERE chat_id=$1", chat_with)
                await send_to(chat_with, {"type": "msg_unpinned"})
                await websocket.send_text(json.dumps({"type": "msg_unpinned"}))
                continue

            if cmd == "get_pinned":
                chat_with = clean_u(data.get("chat_with"))
                async with p.acquire() as con:
                    row = await con.fetchrow(
                        "SELECT msg_id FROM pinned WHERE chat_id=$1", chat_with
                    )
                    if row:
                        m = await con.fetchrow(
                            "SELECT from_user,text FROM messages WHERE id=$1", row["msg_id"]
                        )
                        if m:
                            await websocket.send_text(json.dumps({
                                "type": "msg_pinned", "msg_id": row["msg_id"],
                                "from": m["from_user"], "text": m["text"],
                            }))
                continue

            # ─── HISTORY ───
            if cmd == "history":
                w = clean_u(data.get("with"))
                limit = int(data.get("limit") or 100)
                async with p.acquire() as con:
                    rows = await con.fetch(
                        """SELECT id,from_user,to_user,text,ts,reply_to,forwarded_from,
                                  media_url,media_type,edited,read
                           FROM messages
                           WHERE ((from_user=$1 AND to_user=$2) OR (from_user=$2 AND to_user=$1))
                             AND deleted=FALSE
                           ORDER BY ts DESC LIMIT $3""",
                        user_id, w, limit,
                    )
                    msgs = []
                    for r in reversed(rows):
                        reacts = await con.fetch(
                            "SELECT emoji, COUNT(*) AS c FROM reactions WHERE msg_id=$1 GROUP BY emoji",
                            r["id"],
                        )
                        msgs.append({
                            "id": r["id"], "from": r["from_user"], "to": r["to_user"],
                            "text": r["text"], "ts": r["ts"].isoformat() if r["ts"] else None,
                            "reply_to": r["reply_to"], "forwarded_from": r["forwarded_from"],
                            "media_url": r["media_url"], "media_type": r["media_type"],
                            "edited": r["edited"], "read": r["read"],
                            "reactions": [{"emoji": x["emoji"], "count": x["c"]} for x in reacts],
                        })
                await websocket.send_text(json.dumps({"type": "history", "with": w, "msgs": msgs}))
                continue

            # ─── PROFILE ───
            if cmd == "profile":
                target = clean_u(data.get("user")) or user_id
                async with p.acquire() as con:
                    row = await con.fetchrow(
                        """SELECT username,first_name,bio,avatar,created_at,last_seen
                           FROM users WHERE username=$1""",
                        target,
                    )
                    if not row:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "юзер не найден"}))
                        continue
                    gifts = await con.fetch(
                        """SELECT gift_name,gift_emoji,stars,from_user,ts
                           FROM gifts WHERE to_user=$1 ORDER BY ts DESC LIMIT 50""",
                        target,
                    )
                    unames = await con.fetch(
                        """SELECT username,is_primary FROM usernames
                           WHERE user_id=$1 ORDER BY is_primary DESC, id""",
                        row["username"],
                    )
                online = row["username"] in clients
                await websocket.send_text(json.dumps({
                    "type": "profile",
                    "user": row["username"],
                    "first_name": row["first_name"] or row["username"],
                    "bio": row["bio"] or "",
                    "avatar": row["avatar"] or "",
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "last_seen": row["last_seen"].isoformat() if row["last_seen"] else None,
                    "online": online,
                    "gifts": [{"name": g["gift_name"], "emoji": g["gift_emoji"],
                               "stars": g["stars"], "from": g["from_user"],
                               "ts": g["ts"].isoformat() if g["ts"] else None} for g in gifts],
                    "usernames": [{"u": x["username"], "primary": x["is_primary"]} for x in unames],
                }))
                continue

            # ─── UPDATE PROFILE ───
            if cmd == "update_profile":
                fn = (data.get("first_name") or "").strip().lstrip("@").strip() or user_id
                bio = (data.get("bio") or "").strip()
                av_data = data.get("avatar_data")
                av = data.get("avatar")
                if av_data:
                    new_name = save_file(AVATAR_DIR, user_id, av_data)
                    if new_name:
                        av = new_name
                async with p.acquire() as con:
                    if av is not None:
                        await con.execute(
                            "UPDATE users SET first_name=$1, bio=$2, avatar=$3 WHERE username=$4",
                            fn, bio, av, user_id,
                        )
                    else:
                        await con.execute(
                            "UPDATE users SET first_name=$1, bio=$2 WHERE username=$3",
                            fn, bio, user_id,
                        )
                await websocket.send_text(json.dumps({"type": "profile_updated"}))
                continue

            # ─── GET AVATAR ───
            if cmd == "get_avatar":
                fname = data.get("file") or ""
                path = os.path.join(AVATAR_DIR, fname)
                if fname and os.path.exists(path):
                    try:
                        with open(path, "rb") as f:
                            raw = f.read()
                        b64 = base64.b64encode(raw).decode()
                        ext = fname.rsplit(".", 1)[-1]
                        mime = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
                        await websocket.send_text(json.dumps({
                            "type": "avatar_data", "file": fname,
                            "data": f"data:{mime};base64,{b64}",
                        }))
                    except Exception as e:
                        print(f"[avatar] {e}")
                else:
                    await websocket.send_text(json.dumps({
                        "type": "avatar_data", "file": fname, "data": "",
                    }))
                continue

            # ─── SEND GIFT ───
            if cmd == "send_gift":
                to = clean_u(data.get("to"))
                if not to:
                    continue
                try:
                    stars = int(data.get("stars") or 0)
                except Exception:
                    stars = 0
                ts = now_utc()
                async with p.acquire() as con:
                    exists = await con.fetchval(
                        "SELECT 1 FROM users WHERE username=$1", to
                    )
                    if not exists:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "нет получателя"}))
                        continue
                    await con.execute(
                        """INSERT INTO gifts
                           (from_user,to_user,gift_name,gift_emoji,stars,ts)
                           VALUES ($1,$2,$3,$4,$5,$6)""",
                        user_id, to, data.get("name") or "Подарок",
                        data.get("emoji") or "🎁", stars, ts,
                    )
                await send_to(to, {"type": "gift_received", "from": user_id,
                                   "name": data.get("name"), "emoji": data.get("emoji"),
                                   "stars": stars, "ts": ts.isoformat()})
                await websocket.send_text(json.dumps({"type": "gift_sent", "to": to,
                                                      "name": data.get("name"),
                                                      "emoji": data.get("emoji"),
                                                      "stars": stars}))
                continue

            # ─── USERNAMES ───
            if cmd == "add_username":
                new_u = clean_u(data.get("username"))
                if not new_u or len(new_u) < 4 or len(new_u) > 32:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "4-32 символа"}))
                    continue
                if not new_u.replace("_", "").isalnum():
                    await websocket.send_text(json.dumps({"type": "error", "msg": "буквы/цифры/_"}))
                    continue
                try:
                    async with p.acquire() as con:
                        await con.execute(
                            """INSERT INTO usernames (user_id,username,is_primary,created_at)
                               VALUES ($1,$2,FALSE,$3)""",
                            user_id, new_u, now_utc(),
                        )
                    await websocket.send_text(json.dumps({"type": "username_added", "username": new_u}))
                except asyncpg.UniqueViolationError:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "username занят"}))
                continue

            if cmd == "delete_username":
                name = clean_u(data.get("username"))
                async with p.acquire() as con:
                    row = await con.fetchrow(
                        "SELECT is_primary FROM usernames WHERE user_id=$1 AND username=$2",
                        user_id, name,
                    )
                    if not row:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "не найдено"}))
                        continue
                    if row["is_primary"]:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "нельзя удалить основной"}))
                        continue
                    await con.execute(
                        "DELETE FROM usernames WHERE user_id=$1 AND username=$2",
                        user_id, name,
                    )
                await websocket.send_text(json.dumps({"type": "username_deleted", "username": name}))
                continue

            if cmd == "set_primary_username":
                name = clean_u(data.get("username"))
                async with p.acquire() as con:
                    exists = await con.fetchval(
                        "SELECT 1 FROM usernames WHERE user_id=$1 AND username=$2",
                        user_id, name,
                    )
                    if not exists:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "не найдено"}))
                        continue
                    async with con.transaction():
                        await con.execute(
                            "UPDATE usernames SET is_primary=FALSE WHERE user_id=$1", user_id
                        )
                        await con.execute(
                            "UPDATE usernames SET is_primary=TRUE WHERE user_id=$1 AND username=$2",
                            user_id, name,
                        )
                        await con.execute(
                            "UPDATE users SET username=$1 WHERE user_id=$2", name, user_id
                        )
                await websocket.send_text(json.dumps({"type": "primary_updated", "username": name}))
                continue

            if cmd == "delete_chat":
                w = clean_u(data.get("with"))
                async with p.acquire() as con:
                    await con.execute(
                        """UPDATE messages SET deleted=TRUE
                           WHERE (from_user=$1 AND to_user=$2) OR (from_user=$2 AND to_user=$1)""",
                        user_id, w,
                    )
                await websocket.send_text(json.dumps({"type": "chat_deleted", "with": w}))
                continue

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[ws] {e}")
        import traceback
        traceback.print_exc()
    finally:
        if user_id and clients.get(user_id) is websocket:
            del clients[user_id]
            try:
                async with p.acquire() as con:
                    await con.execute(
                        "UPDATE users SET last_seen=$1 WHERE username=$2",
                        now_utc(), user_id,
                    )
            except Exception:
                pass
            for uid in list(clients.keys()):
                await send_to(uid, {"type": "user_offline", "user": user_id})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
