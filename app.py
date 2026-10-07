#!/usr/bin/env python3
import asyncio
import base64
import hashlib
import json
import os
import random
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import requests as rq
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, FileResponse

DB = os.path.expanduser("~/messenger.db")
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


# ─── DB helpers ───
def db():
    c = sqlite3.connect(DB, timeout=10, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def init_db():
    con = db()
    con.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY, username TEXT UNIQUE,
            password TEXT, first_name TEXT, bio TEXT,
            avatar TEXT, created_at TEXT, last_seen TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user TEXT, to_user TEXT, text TEXT, ts TEXT,
            read INTEGER DEFAULT 0, deleted INTEGER DEFAULT 0,
            edited INTEGER DEFAULT 0, edited_at TEXT,
            reply_to INTEGER, forwarded_from TEXT,
            media_url TEXT, media_type TEXT
        );
        CREATE TABLE IF NOT EXISTS gifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user TEXT, to_user TEXT, gift_name TEXT,
            gift_emoji TEXT, stars INTEGER, ts TEXT
        );
        CREATE TABLE IF NOT EXISTS usernames (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT, username TEXT UNIQUE,
            is_primary INTEGER DEFAULT 0, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            msg_id INTEGER, chat_id TEXT, user_id TEXT,
            emoji TEXT, ts TEXT,
            UNIQUE(msg_id, user_id, emoji)
        );
        CREATE TABLE IF NOT EXISTS pinned (
            chat_id TEXT PRIMARY KEY, msg_id INTEGER,
            pinned_by TEXT, pinned_at TEXT
        );
        CREATE TABLE IF NOT EXISTS contacts (
            owner TEXT, contact TEXT, ts TEXT,
            UNIQUE(owner, contact)
        );
    """)
    con.commit()
    con.close()


def now_iso():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


# ─── PASSWORD ───
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


# ─── FILE ───
def save_file(folder, name_prefix, b64data):
    if not b64data or "," not in b64data:
        return None
    try:
        header, b64 = b64data.split(",", 1)
        # header вида: data:image/jpeg;base64
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


# ─── WS SEND ───
clients: dict[str, WebSocket] = {}


async def send_to(uid, payload):
    ws = clients.get(uid)
    if ws:
        try:
            await ws.send_text(json.dumps(payload))
            return True
        except Exception:
            pass
    return False


# ─── LIFESPAN ───
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    for d in (AVATAR_DIR, UPLOAD_DIR, STICKER_DIR):
        os.makedirs(d, exist_ok=True)
    print(f"[server] порт {PORT}")
    yield


app = FastAPI(lifespan=lifespan)


# ─── STATIC ───
@app.get("/")
def root():
    if os.path.exists(HTML_FILE):
        return FileResponse(HTML_FILE)
    return HTMLResponse("<h1>Messenger</h1>")


@app.get("/healthz")
def healthz():
    return {"ok": True}


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
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except Exception:
                await websocket.send_text(json.dumps({"type": "error", "msg": "bad json"}))
                continue
            cmd = data.get("cmd")

            # ─── PING ───
            if cmd == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue

            # ─── REGISTER ───
            if cmd == "register":
                u = (data.get("username") or "").strip().lower()
                p = data.get("password") or ""
                if not u or not p or len(u) < 4:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "username 4+"}))
                    continue
                con = db()
                try:
                    con.execute(
                        "INSERT INTO users (user_id,username,password,first_name,created_at,last_seen) VALUES (?,?,?,?,?,?)",
                        (u, u, hash_pw(p), u, now_iso(), now_iso()),
                    )
                    con.execute(
                        "INSERT INTO usernames (user_id,username,is_primary,created_at) VALUES (?,?,1,?)",
                        (u, u, now_iso()),
                    )
                    con.commit()
                    user_id = u
                    clients[u] = websocket
                    await websocket.send_text(json.dumps({"type": "registered", "user_id": u}))
                except sqlite3.IntegrityError:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "занято"}))
                finally:
                    con.close()
                continue

            # ─── LOGIN ───
            if cmd == "login":
                u = (data.get("username") or "").strip().lower()
                p = data.get("password") or ""
                con = db()
                row = con.execute("SELECT password FROM users WHERE username=?", (u,)).fetchone()
                if not row or not check_pw(p, row[0]):
                    con.close()
                    await websocket.send_text(json.dumps({"type": "error", "msg": "неверный"}))
                    continue
                con.execute("UPDATE users SET last_seen=? WHERE username=?", (now_iso(), u))
                con.commit()
                con.close()
                user_id = u
                clients[u] = websocket
                await websocket.send_text(json.dumps({"type": "logged_in", "user_id": u}))
                for uid in list(clients.keys()):
                    if uid != u:
                        await send_to(uid, {"type": "user_online", "user": u})
                continue

            if not user_id:
                await websocket.send_text(json.dumps({"type": "error", "msg": "сначала login"}))
                continue

            # ─── USERS ───
            if cmd == "users":
                con = db()
                rows = con.execute(
                    "SELECT username,first_name,avatar,last_seen FROM users ORDER BY username"
                ).fetchall()
                con.close()
                lst = []
                for r in rows:
                    online = r[0] in clients
                    lst.append({"u": r[0], "n": r[1] or r[0], "av": r[2] or "",
                                "online": online, "ls": r[3]})
                await websocket.send_text(json.dumps({"type": "users", "list": lst}))
                continue

            # ─── SEARCH ───
            if cmd == "search_users":
                q = (data.get("q") or "").strip().lower()
                con = db()
                rows = con.execute(
                    """SELECT username,first_name,avatar FROM users
                    WHERE (LOWER(username) LIKE ? OR LOWER(first_name) LIKE ?) AND username != ?
                    ORDER BY username LIMIT 30""",
                    (f"%{q}%", f"%{q}%", user_id),
                ).fetchall()
                con.close()
                await websocket.send_text(json.dumps({"type": "search_results", "list": [
                    {"u": r[0], "n": r[1] or r[0], "av": r[2] or ""} for r in rows
                ]}))
                continue

            # ─── ADD CONTACT ───
            if cmd == "add_contact":
                target = (data.get("user") or "").strip().lower()
                if not target or target == user_id:
                    continue
                con = db()
                if not con.execute("SELECT 1 FROM users WHERE username=?", (target,)).fetchone():
                    con.close()
                    await websocket.send_text(json.dumps({"type": "error", "msg": "нет юзера"}))
                    continue
                try:
                    con.execute("INSERT OR IGNORE INTO contacts (owner,contact,ts) VALUES (?,?,?)",
                                (user_id, target, now_iso()))
                    con.commit()
                except Exception:
                    pass
                con.close()
                await websocket.send_text(json.dumps({"type": "contact_added", "user": target}))
                continue

            # ─── SEND ───
            if cmd == "send":
                to = (data.get("to") or "").strip().lower()
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
                ts = now_iso()
                # если пришёл media_data — сохраняем файл
                if media_data:
                    fname = save_file(UPLOAD_DIR, user_id, media_data)
                    if fname:
                        media_url = f"/uploads/{fname}"
                con = db()
                cur = con.execute(
                    """INSERT INTO messages
                    (from_user,to_user,text,ts,reply_to,forwarded_from,media_url,media_type)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (user_id, to, text, ts, reply_to, forwarded_from, media_url, media_type),
                )
                msg_id = cur.lastrowid
                con.commit()
                con.close()
                payload = {
                    "type": "msg", "id": msg_id, "from": user_id, "text": text, "ts": ts,
                    "reply_to": reply_to, "forwarded_from": forwarded_from,
                    "media_url": media_url, "media_type": media_type,
                }
                delivered = await send_to(to, payload)
                await websocket.send_text(json.dumps({
                    "type": "sent", "id": msg_id, "to": to, "text": text, "ts": ts,
                    "reply_to": reply_to, "forwarded_from": forwarded_from,
                    "media_url": media_url, "media_type": media_type,
                    "delivered": delivered,
                }))
                continue

            # ─── EDIT ───
            if cmd == "edit_msg":
                mid = data.get("msg_id")
                new_text = (data.get("text") or "").strip()
                if not mid or not new_text:
                    continue
                con = db()
                row = con.execute("SELECT from_user,to_user FROM messages WHERE id=?", (mid,)).fetchone()
                if not row or row[0] != user_id:
                    con.close()
                    await websocket.send_text(json.dumps({"type": "error", "msg": "нельзя редактировать"}))
                    continue
                con.execute("UPDATE messages SET text=?, edited=1, edited_at=? WHERE id=?",
                            (new_text, now_iso(), mid))
                con.commit()
                con.close()
                other = row[1] if row[0] == user_id else row[0]
                await send_to(other, {"type": "msg_edited", "msg_id": mid, "text": new_text})
                await websocket.send_text(json.dumps({"type": "msg_edited", "msg_id": mid, "text": new_text}))
                continue

            # ─── DELETE ───
            if cmd == "delete_msg":
                mid = data.get("msg_id")
                if not mid:
                    continue
                con = db()
                row = con.execute("SELECT from_user,to_user FROM messages WHERE id=?", (mid,)).fetchone()
                if not row:
                    con.close()
                    continue
                con.execute("UPDATE messages SET deleted=1, text='' WHERE id=?", (mid,))
                con.commit()
                con.close()
                other = row[1] if row[0] == user_id else row[0]
                await send_to(other, {"type": "msg_deleted", "msg_id": mid})
                await websocket.send_text(json.dumps({"type": "msg_deleted", "msg_id": mid}))
                continue

            # ─── REACTION ───
            if cmd == "react":
                mid = data.get("msg_id")
                emoji = data.get("emoji")
                chat_with = data.get("chat_with")
                if not mid or not emoji or not chat_with:
                    continue
                con = db()
                existing = con.execute(
                    "SELECT id FROM reactions WHERE msg_id=? AND user_id=? AND emoji=?",
                    (mid, user_id, emoji),
                ).fetchone()
                if existing:
                    con.execute("DELETE FROM reactions WHERE id=?", (existing[0],))
                else:
                    con.execute(
                        "INSERT INTO reactions (msg_id,chat_id,user_id,emoji,ts) VALUES (?,?,?,?,?)",
                        (mid, chat_with, user_id, emoji, now_iso()),
                    )
                rows = con.execute(
                    "SELECT emoji, COUNT(*) FROM reactions WHERE msg_id=? GROUP BY emoji",
                    (mid,),
                ).fetchall()
                con.commit()
                con.close()
                payload = {"type": "reactions_updated", "msg_id": mid,
                           "reactions": [{"emoji": r[0], "count": r[1]} for r in rows]}
                await send_to(chat_with, payload)
                await websocket.send_text(json.dumps(payload))
                continue

            # ─── PIN ───
            if cmd == "pin_msg":
                mid = data.get("msg_id")
                chat_with = data.get("chat_with")
                if not mid or not chat_with:
                    continue
                con = db()
                con.execute(
                    "INSERT OR REPLACE INTO pinned (chat_id, msg_id, pinned_by, pinned_at) VALUES (?,?,?,?)",
                    (chat_with, mid, user_id, now_iso()),
                )
                row = con.execute("SELECT from_user,text FROM messages WHERE id=?", (mid,)).fetchone()
                con.commit()
                con.close()
                if row:
                    payload = {"type": "msg_pinned", "msg_id": mid, "from": row[0], "text": row[1]}
                    await send_to(chat_with, payload)
                    await websocket.send_text(json.dumps(payload))
                continue

            if cmd == "unpin_msg":
                chat_with = data.get("chat_with")
                con = db()
                con.execute("DELETE FROM pinned WHERE chat_id=?", (chat_with,))
                con.commit()
                con.close()
                await send_to(chat_with, {"type": "msg_unpinned"})
                await websocket.send_text(json.dumps({"type": "msg_unpinned"}))
                continue

            if cmd == "get_pinned":
                chat_with = data.get("chat_with")
                con = db()
                row = con.execute("SELECT msg_id FROM pinned WHERE chat_id=?", (chat_with,)).fetchone()
                if row:
                    m = con.execute("SELECT from_user,text FROM messages WHERE id=?", (row[0],)).fetchone()
                    if m:
                        await websocket.send_text(json.dumps({
                            "type": "msg_pinned", "msg_id": row[0], "from": m[0], "text": m[1],
                        }))
                con.close()
                continue

            # ─── HISTORY ───
            if cmd == "history":
                w = (data.get("with") or "").strip().lower()
                limit = int(data.get("limit") or 100)
                con = db()
                rows = con.execute(
                    """SELECT id,from_user,to_user,text,ts,reply_to,forwarded_from,
                              media_url,media_type,edited
                    FROM messages
                    WHERE ((from_user=? AND to_user=?) OR (from_user=? AND to_user=?)) AND deleted=0
                    ORDER BY ts DESC LIMIT ?""",
                    (user_id, w, w, user_id, limit),
                ).fetchall()
                msgs = []
                for r in reversed(rows):
                    mid = r[0]
                    reacts = con.execute(
                        "SELECT emoji,COUNT(*) FROM reactions WHERE msg_id=? GROUP BY emoji",
                        (mid,),
                    ).fetchall()
                    msgs.append({
                        "id": r[0], "from": r[1], "to": r[2], "text": r[3], "ts": r[4],
                        "reply_to": r[5], "forwarded_from": r[6],
                        "media_url": r[7], "media_type": r[8], "edited": bool(r[9]),
                        "reactions": [{"emoji": x[0], "count": x[1]} for x in reacts],
                    })
                con.close()
                await websocket.send_text(json.dumps({"type": "history", "with": w, "msgs": msgs}))
                continue

            # ─── PROFILE ───
            if cmd == "profile":
                target = (data.get("user") or user_id).lower()
                con = db()
                row = con.execute(
                    "SELECT username,first_name,bio,avatar,created_at,last_seen FROM users WHERE username=?",
                    (target,),
                ).fetchone()
                if not row:
                    con.close()
                    await websocket.send_text(json.dumps({"type": "error", "msg": "юзер не найден"}))
                    continue
                gifts = con.execute(
                    """SELECT gift_name,gift_emoji,stars,from_user,ts
                    FROM gifts WHERE to_user=? ORDER BY ts DESC LIMIT 50""",
                    (target,),
                ).fetchall()
                unames = con.execute(
                    "SELECT username,is_primary FROM usernames WHERE user_id=? ORDER BY is_primary DESC,id",
                    (row[0],),
                ).fetchall()
                con.close()
                online = row[0] in clients
                await websocket.send_text(json.dumps({
                    "type": "profile", "user": row[0], "first_name": row[1] or row[0],
                    "bio": row[2] or "", "avatar": row[3] or "", "created_at": row[4],
                    "last_seen": row[5], "online": online,
                    "gifts": [{"name": g[0], "emoji": g[1], "stars": g[2], "from": g[3], "ts": g[4]}
                              for g in gifts],
                    "usernames": [{"u": x[0], "primary": bool(x[1])} for x in unames],
                }))
                continue

            # ─── UPDATE PROFILE ───
            if cmd == "update_profile":
                fn = (data.get("first_name") or "").strip() or user_id
                bio = (data.get("bio") or "").strip()
                av_data = data.get("avatar_data")
                av = data.get("avatar")
                if av_data:
                    new_name = save_file(AVATAR_DIR, user_id, av_data)
                    if new_name:
                        av = new_name
                con = db()
                if av is not None:
                    con.execute("UPDATE users SET first_name=?,bio=?,avatar=? WHERE username=?",
                                (fn, bio, av, user_id))
                else:
                    con.execute("UPDATE users SET first_name=?,bio=? WHERE username=?",
                                (fn, bio, user_id))
                con.commit()
                con.close()
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
                to = (data.get("to") or "").strip().lower()
                if not to:
                    continue
                try:
                    stars = int(data.get("stars") or 0)
                except Exception:
                    stars = 0
                ts = now_iso()
                con = db()
                if not con.execute("SELECT 1 FROM users WHERE username=?", (to,)).fetchone():
                    con.close()
                    await websocket.send_text(json.dumps({"type": "error", "msg": "нет получателя"}))
                    continue
                con.execute(
                    "INSERT INTO gifts (from_user,to_user,gift_name,gift_emoji,stars,ts) VALUES (?,?,?,?,?,?)",
                    (user_id, to, data.get("name") or "Подарок",
                     data.get("emoji") or "🎁", stars, ts),
                )
                con.commit()
                con.close()
                await send_to(to, {"type": "gift_received", "from": user_id,
                                   "name": data.get("name"), "emoji": data.get("emoji"),
                                   "stars": stars, "ts": ts})
                await websocket.send_text(json.dumps({"type": "gift_sent", "to": to,
                                                      "name": data.get("name"),
                                                      "emoji": data.get("emoji"),
                                                      "stars": stars}))
                continue

            # ─── USERNAMES ───
            if cmd == "add_username":
                new_u = (data.get("username") or "").strip().lower().lstrip("@")
                if not new_u or len(new_u) < 4 or len(new_u) > 32:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "4-32 символа"}))
                    continue
                if not new_u.replace("_", "").isalnum():
                    await websocket.send_text(json.dumps({"type": "error", "msg": "буквы/цифры/_"}))
                    continue
                con = db()
                try:
                    con.execute(
                        "INSERT INTO usernames (user_id,username,is_primary,created_at) VALUES (?,?,0,?)",
                        (user_id, new_u, now_iso()),
                    )
                    con.commit()
                    await websocket.send_text(json.dumps({"type": "username_added", "username": new_u}))
                except sqlite3.IntegrityError:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "username занят"}))
                finally:
                    con.close()
                continue

            if cmd == "delete_username":
                name = (data.get("username") or "").strip().lower()
                con = db()
                row = con.execute(
                    "SELECT is_primary FROM usernames WHERE user_id=? AND username=?",
                    (user_id, name),
                ).fetchone()
                if not row:
                    con.close()
                    await websocket.send_text(json.dumps({"type": "error", "msg": "не найдено"}))
                    continue
                if row[0] == 1:
                    con.close()
                    await websocket.send_text(json.dumps({"type": "error", "msg": "нельзя удалить основной"}))
                    continue
                con.execute("DELETE FROM usernames WHERE user_id=? AND username=?", (user_id, name))
                con.commit()
                con.close()
                await websocket.send_text(json.dumps({"type": "username_deleted", "username": name}))
                continue

            if cmd == "set_primary_username":
                name = (data.get("username") or "").strip().lower()
                con = db()
                if not con.execute(
                    "SELECT 1 FROM usernames WHERE user_id=? AND username=?",
                    (user_id, name),
                ).fetchone():
                    con.close()
                    await websocket.send_text(json.dumps({"type": "error", "msg": "не найдено"}))
                    continue
                con.execute("UPDATE usernames SET is_primary=0 WHERE user_id=?", (user_id,))
                con.execute("UPDATE usernames SET is_primary=1 WHERE user_id=? AND username=?",
                            (user_id, name))
                con.execute("UPDATE users SET username=? WHERE user_id=?", (name, user_id))
                con.commit()
                con.close()
                await websocket.send_text(json.dumps({"type": "primary_updated", "username": name}))
                continue

            if cmd == "delete_chat":
                w = (data.get("with") or "").strip().lower()
                con = db()
                con.execute(
                    "UPDATE messages SET deleted=1 WHERE (from_user=? AND to_user=?) OR (from_user=? AND to_user=?)",
                    (user_id, w, w, user_id),
                )
                con.commit()
                con.close()
                await websocket.send_text(json.dumps({"type": "chat_deleted", "with": w}))
                continue

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[ws] {e}")
    finally:
        if user_id and clients.get(user_id) is websocket:
            del clients[user_id]
            try:
                con = db()
                con.execute("UPDATE users SET last_seen=? WHERE username=?", (now_iso(), user_id))
                con.commit()
                con.close()
            except Exception:
                pass
            for uid in list(clients.keys()):
                await send_to(uid, {"type": "user_offline", "user": user_id})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
