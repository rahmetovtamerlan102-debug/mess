#!/usr/bin/env python3
import base64
import hashlib
import json
import os
import random
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
import requests as rq
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, FileResponse

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

pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        pool = await asyncpg.create_pool(url, min_size=1, max_size=10)
    return pool

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL, first_name TEXT, bio TEXT,
    avatar TEXT, created_at TIMESTAMP, last_seen TIMESTAMP
);
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY, from_user TEXT, to_user TEXT, text TEXT,
    ts TIMESTAMP, read BOOLEAN DEFAULT FALSE, deleted BOOLEAN DEFAULT FALSE,
    edited BOOLEAN DEFAULT FALSE, edited_at TIMESTAMP,
    reply_to INTEGER, forwarded_from TEXT, media_url TEXT, media_type TEXT
);
CREATE TABLE IF NOT EXISTS gifts (
    id SERIAL PRIMARY KEY, from_user TEXT, to_user TEXT,
    gift_name TEXT, gift_emoji TEXT, stars INTEGER, ts TIMESTAMP
);
CREATE TABLE IF NOT EXISTS usernames (
    id SERIAL PRIMARY KEY, user_id TEXT, username TEXT UNIQUE NOT NULL,
    is_primary BOOLEAN DEFAULT FALSE, created_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS reactions (
    id SERIAL PRIMARY KEY, msg_id INTEGER, chat_id TEXT, user_id TEXT,
    emoji TEXT, ts TIMESTAMP, UNIQUE(msg_id, user_id, emoji)
);
CREATE TABLE IF NOT EXISTS pinned (
    chat_id TEXT PRIMARY KEY, msg_id INTEGER,
    pinned_by TEXT, pinned_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS contacts (
    owner TEXT, contact TEXT, ts TIMESTAMP, UNIQUE(owner, contact)
);
CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY, user_id TEXT, created_at TIMESTAMP, last_ip TEXT
);
CREATE TABLE IF NOT EXISTS privacy (
    user_id TEXT PRIMARY KEY,
    phone TEXT, seen TEXT DEFAULT 'all', photo TEXT DEFAULT 'all',
    fwd TEXT DEFAULT 'all', calls TEXT DEFAULT 'all',
    voice TEXT DEFAULT 'all', msgs TEXT DEFAULT 'all',
    bday TEXT DEFAULT 'all', gifts TEXT DEFAULT 'all',
    bio TEXT DEFAULT 'all', music TEXT DEFAULT 'all',
    inv TEXT DEFAULT 'none', autodel TEXT DEFAULT 'off',
    cloud_pw TEXT, passcode TEXT, passkey TEXT
);
CREATE TABLE IF NOT EXISTS devices (
    id SERIAL PRIMARY KEY, user_id TEXT, device_name TEXT,
    device_info TEXT, ip TEXT, location TEXT,
    last_seen TIMESTAMP, created_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS blacklist (
    id SERIAL PRIMARY KEY, owner TEXT, blocked TEXT,
    ts TIMESTAMP, UNIQUE(owner, blocked)
);
CREATE TABLE IF NOT EXISTS emails (
    id SERIAL PRIMARY KEY, user_id TEXT, email TEXT,
    verified BOOLEAN DEFAULT FALSE, ts TIMESTAMP
);
"""

async def init_db():
    p = await get_pool()
    async with p.acquire() as con:
        await con.execute(SCHEMA)

def now_utc(): return datetime.now(timezone.utc).replace(tzinfo=None)
def clean_u(s): return (s or "").strip().lower().lstrip("@").strip()

def hash_pw(pw: str, salt: bytes | None = None) -> str:
    salt = salt or os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, 100_000)
    return salt.hex() + ":" + h.hex()

def check_pw(pw: str, stored: str) -> bool:
    try:
        salt_hex, h_hex = stored.split(":")
        salt = bytes.fromhex(salt_hex)
        return hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, 100_000).hex() == h_hex
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
            if "/" in mime: mime = mime.split("/", 1)[1]
        mime = mime.lower().strip()
        ext = ALLOWED_MIME.get(mime)
        if not ext: return None
        raw = base64.b64decode(b64)
        if len(raw) > 15 * 1024 * 1024: return None
        fname = f"{name_prefix}_{uuid.uuid4().hex[:12]}.{ext}"
        with open(os.path.join(folder, fname), "wb") as f:
            f.write(raw)
        return fname
    except Exception as e:
        print(f"[file] {e}")
        return None

def geo_ip(ip):
    """Возвращает {country, city} по IP."""
    try:
        if not ip or ip.startswith("127.") or ip.startswith("10."):
            return {"country": "Unknown", "city": ""}
        r = rq.get(f"https://ipwho.is/{ip}", timeout=4)
        d = r.json()
        if d.get("success"):
            return {"country": d.get("country", ""), "city": d.get("city", "")}
    except Exception as e:
        print(f"[geo_ip] {e}")
    return {"country": "Unknown", "city": ""}

clients: dict[str, WebSocket] = {}

async def send_to(uid, payload):
    ws = clients.get(uid)
    if ws:
        try:
            await ws.send_text(json.dumps(payload, default=str))
            return True
        except Exception: pass
    return False

@asynccontextmanager
async def lifespan(app: FastAPI):
    for d in (AVATAR_DIR, UPLOAD_DIR, STICKER_DIR):
        os.makedirs(d, exist_ok=True)
    if DATABASE_URL:
        await init_db()
        print("[server] БД инициализирована")
    print(f"[server] порт {PORT}")
    yield
    if pool: await pool.close()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def root():
    if os.path.exists(HTML_FILE): return FileResponse(HTML_FILE)
    return HTMLResponse("<h1>Messenger</h1>")

@app.get("/healthz")
def healthz(): return {"ok": True, "db": bool(DATABASE_URL)}

@app.get("/avatars/{fname}")
def av_file(fname: str):
    p = os.path.join(AVATAR_DIR, fname)
    if os.path.exists(p): return FileResponse(p)
    return HTMLResponse("", status_code=404)

@app.get("/uploads/{fname}")
def up_file(fname: str):
    p = os.path.join(UPLOAD_DIR, fname)
    if os.path.exists(p): return FileResponse(p)
    return HTMLResponse("", status_code=404)

@app.get("/stickers/{fname}")
def st_file(fname: str):
    p = os.path.join(STICKER_DIR, fname)
    if os.path.exists(p): return FileResponse(p)
    return HTMLResponse("", status_code=404)

@app.get("/geo")
def geo(request: Request):
    try:
        ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
        if not ip or ip.startswith("127.") or ip.startswith("10."):
            ip = request.client.host
        g = geo_ip(ip)
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

def parse_device(ua: str) -> str:
    """Парсит User-Agent в человекочитаемое имя."""
    if not ua: return "Неизвестное устройство"
    ua_l = ua.lower()
    if "android" in ua_l:
        # Ищем модель
        import re
        m = re.search(r"android [\d.]+; ([^)]+)\)", ua, re.I)
        model = m.group(1).split(";")[0].strip() if m else "Android"
        return f"{model}"
    if "iphone" in ua_l: return "iPhone"
    if "ipad" in ua_l: return "iPad"
    if "macintosh" in ua_l or "mac os" in ua_l: return "macOS"
    if "windows" in ua_l: return "Windows"
    if "linux" in ua_l: return "Linux"
    return "Устройство"

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
                    row = await con.fetchrow("SELECT user_id FROM sessions WHERE token=$1", tok)
                if not row:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "session expired"}))
                    continue
                uid = row["user_id"]
                user_id = uid
                clients[uid] = websocket
                # Регистрируем устройство
                await register_device(con := None, uid, websocket) if False else None
                async with p.acquire() as con:
                    await _register_device(con, uid, websocket, tok)
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
                                "INSERT INTO users (user_id,username,password,first_name,created_at,last_seen) VALUES ($1,$2,$3,$4,$5,$6)",
                                u, u, hash_pw(pw), u, now_utc(), now_utc())
                            await con.execute(
                                "INSERT INTO usernames (user_id,username,is_primary,created_at) VALUES ($1,$2,TRUE,$3)",
                                u, u, now_utc())
                            await con.execute("INSERT INTO privacy (user_id) VALUES ($1) ON CONFLICT DO NOTHING", u)
                            tok = uuid.uuid4().hex
                            ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                                 (websocket.client.host if websocket.client else "")
                            await con.execute(
                                "INSERT INTO sessions (token,user_id,created_at,last_ip) VALUES ($1,$2,$3,$4)",
                                tok, u, now_utc(), ip)
                            await _register_device(con, u, websocket, tok)
                    user_id = u
                    clients[u] = websocket
                    await websocket.send_text(json.dumps({"type": "registered", "user_id": u, "token": tok}))
                except asyncpg.UniqueViolationError:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "занято"}))
                continue

            # ─── LOGIN ───
            if cmd == "login":
                u = clean_u(data.get("username"))
                pw = data.get("password") or ""
                async with p.acquire() as con:
                    row = await con.fetchrow("SELECT password FROM users WHERE username=$1", u)
                if not row or not check_pw(pw, row["password"]):
                    await websocket.send_text(json.dumps({"type": "error", "msg": "неверный"}))
                    continue
                tok = uuid.uuid4().hex
                ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                     (websocket.client.host if websocket.client else "")
                async with p.acquire() as con:
                    await con.execute("UPDATE users SET last_seen=$1 WHERE username=$2", now_utc(), u)
                    await con.execute(
                        "INSERT INTO sessions (token,user_id,created_at,last_ip) VALUES ($1,$2,$3,$4)",
                        tok, u, now_utc(), ip)
                    await con.execute("INSERT INTO privacy (user_id) VALUES ($1) ON CONFLICT DO NOTHING", u)
                    await _register_device(con, u, websocket, tok)
                user_id = u
                clients[u] = websocket
                await websocket.send_text(json.dumps({"type": "logged_in", "user_id": u, "token": tok}))
                for uid in list(clients.keys()):
                    if uid != u:
                        await send_to(uid, {"type": "user_online", "user": u})
                continue

            if not user_id:
                await websocket.send_text(json.dumps({"type": "error", "msg": "сначала login"}))
                continue

            # ─── PRIVACY ───
            if cmd == "get_privacy":
                async with p.acquire() as con:
                    row = await con.fetchrow("SELECT * FROM privacy WHERE user_id=$1", user_id)
                    if not row:
                        await con.execute("INSERT INTO privacy (user_id) VALUES ($1)", user_id)
                        row = await con.fetchrow("SELECT * FROM privacy WHERE user_id=$1", user_id)
                out = {k: (row[k] if row[k] is not None else "") for k in row.keys() if k != "user_id"}
                await websocket.send_text(json.dumps({"type": "privacy", "data": out}))
                continue

            if cmd == "set_privacy":
                k = data.get("key")
                v = data.get("value")
                allowed = ["seen","photo","fwd","calls","voice","msgs","bday","gifts","bio","music","inv","autodel"]
                if k not in allowed:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "unknown key"}))
                    continue
                async with p.acquire() as con:
                    await con.execute(f"UPDATE privacy SET {k}=$1 WHERE user_id=$2", v, user_id)
                await websocket.send_text(json.dumps({"type": "privacy_updated", "key": k, "value": v}))
                continue

            # ─── DEVICES ───
            if cmd == "get_devices":
                async with p.acquire() as con:
                    rows = await con.fetch(
                        "SELECT id,device_name,device_info,ip,location,last_seen,created_at FROM devices WHERE user_id=$1 ORDER BY last_seen DESC",
                        user_id)
                cur_ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                         (websocket.client.host if websocket.client else "")
                lst = []
                for r in rows:
                    lst.append({
                        "id": r["id"],
                        "name": r["device_name"] or "Устройство",
                        "info": r["device_info"] or "",
                        "ip": r["ip"] or "",
                        "location": r["location"] or "",
                        "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
                        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                        "is_current": r["ip"] == cur_ip,
                    })
                await websocket.send_text(json.dumps({"type": "devices", "list": lst}))
                continue

            if cmd == "terminate_device":
                did = data.get("id")
                async with p.acquire() as con:
                    await con.execute("DELETE FROM devices WHERE id=$1 AND user_id=$2", did, user_id)
                await websocket.send_text(json.dumps({"type": "device_terminated", "id": did}))
                continue

            # ─── BLACKLIST ───
            if cmd == "get_blacklist":
                async with p.acquire() as con:
                    rows = await con.fetch(
                        "SELECT blocked, ts FROM blacklist WHERE owner=$1 ORDER BY ts DESC", user_id)
                await websocket.send_text(json.dumps({"type": "blacklist", "list": [
                    {"u": r["blocked"], "ts": r["ts"].isoformat() if r["ts"] else None} for r in rows
                ]}))
                continue

            if cmd == "add_blacklist":
                target = clean_u(data.get("user"))
                if not target or target == user_id:
                    continue
                async with p.acquire() as con:
                    await con.execute(
                        "INSERT INTO blacklist (owner,blocked,ts) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                        user_id, target, now_utc())
                await websocket.send_text(json.dumps({"type": "blacklist_added", "user": target}))
                continue

            if cmd == "remove_blacklist":
                target = clean_u(data.get("user"))
                async with p.acquire() as con:
                    await con.execute("DELETE FROM blacklist WHERE owner=$1 AND blocked=$2", user_id, target)
                await websocket.send_text(json.dumps({"type": "blacklist_removed", "user": target}))
                continue

            # ─── EMAIL ───
            if cmd == "get_emails":
                async with p.acquire() as con:
                    rows = await con.fetch("SELECT id,email,verified FROM emails WHERE user_id=$1", user_id)
                await websocket.send_text(json.dumps({"type": "emails", "list": [
                    {"id": r["id"], "email": r["email"], "verified": r["verified"]} for r in rows
                ]}))
                continue

            if cmd == "add_email":
                em = (data.get("email") or "").strip().lower()
                if "@" not in em or "." not in em:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "неверный email"}))
                    continue
                async with p.acquire() as con:
                    await con.execute(
                        "INSERT INTO emails (user_id,email,verified,ts) VALUES ($1,$2,FALSE,$3)",
                        user_id, em, now_utc())
                await websocket.send_text(json.dumps({"type": "email_added", "email": em}))
                continue

            if cmd == "verify_email":
                eid = data.get("id")
                async with p.acquire() as con:
                    await con.execute("UPDATE emails SET verified=TRUE WHERE id=$1 AND user_id=$2", eid, user_id)
                await websocket.send_text(json.dumps({"type": "email_verified", "id": eid}))
                continue

            # ─── CLOUD PASSWORD / PASSCODE / PASSKEY ───
            if cmd == "set_cloud_pw":
                pw = data.get("password") or ""
                hashed = hash_pw(pw) if pw else None
                async with p.acquire() as con:
                    await con.execute("UPDATE privacy SET cloud_pw=$1 WHERE user_id=$2", hashed, user_id)
                await websocket.send_text(json.dumps({"type": "cloud_pw_set", "on": bool(pw)}))
                continue

            if cmd == "set_passcode":
                pw = data.get("password") or ""
                hashed = hash_pw(pw) if pw else None
                async with p.acquire() as con:
                    await con.execute("UPDATE privacy SET passcode=$1 WHERE user_id=$2", hashed, user_id)
                await websocket.send_text(json.dumps({"type": "passcode_set", "on": bool(pw)}))
                continue

            if cmd == "add_passkey":
                # Заглушка — просто метка
                async with p.acquire() as con:
                    await con.execute("UPDATE privacy SET passkey=$1 WHERE user_id=$2", "on", user_id)
                await websocket.send_text(json.dumps({"type": "passkey_added"}))
                continue

            if cmd == "remove_passkey":
                async with p.acquire() as con:
                    await con.execute("UPDATE privacy SET passkey=NULL WHERE user_id=$1", user_id)
                await websocket.send_text(json.dumps({"type": "passkey_removed"}))
                continue

            # ─── DELETE ACCOUNT ───
            if cmd == "delete_account":
                async with p.acquire() as con:
                    await con.execute("DELETE FROM users WHERE username=$1", user_id)
                    await con.execute("DELETE FROM messages WHERE from_user=$1 OR to_user=$1", user_id)
                    await con.execute("DELETE FROM sessions WHERE user_id=$1", user_id)
                    await con.execute("DELETE FROM devices WHERE user_id=$1", user_id)
                    await con.execute("DELETE FROM blacklist WHERE owner=$1 OR blocked=$1", user_id)
                    await con.execute("DELETE FROM privacy WHERE user_id=$1", user_id)
                    await con.execute("DELETE FROM emails WHERE user_id=$1", user_id)
                await websocket.send_text(json.dumps({"type": "account_deleted"}))
                continue

            # ... (остальные команды: users, search, send, history, profile, gifts, usernames — как раньше)
            # Я оставлю их как есть, чтобы не раздувать. Просто скопируйте из предыдущей версии.

            # ─── USERS, SEARCH, SEND, HISTORY, PROFILE, GIFTS, USERNAMES ───
            # (см. предыдущий app.py — без изменений)

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
                    await con.execute("UPDATE users SET last_seen=$1 WHERE username=$2", now_utc(), user_id)
            except Exception: pass
            for uid in list(clients.keys()):
                await send_to(uid, {"type": "user_offline", "user": user_id})

async def _register_device(con, uid, websocket, token):
    """Сохраняет/обновляет устройство по IP."""
    ua = websocket.headers.get("user-agent", "")
    ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
         (websocket.client.host if websocket.client else "")
    device_name = parse_device(ua)
    info = "Браузер"
    ua_l = ua.lower()
    if "telegram" in ua_l: info = "Telegram"
    elif "chrome" in ua_l: info = "Chrome"
    elif "firefox" in ua_l: info = "Firefox"
    elif "safari" in ua_l: info = "Safari"
    g = geo_ip(ip)
    location = ", ".join([x for x in [g.get("city"), g.get("country")] if x])
    # Если такое же устройство уже есть с тем же IP — обновим last_seen
    existing = await con.fetchrow(
        "SELECT id FROM devices WHERE user_id=$1 AND ip=$2 AND device_name=$3",
        uid, ip, device_name)
    if existing:
        await con.execute("UPDATE devices SET last_seen=$1, location=$2 WHERE id=$3",
                          now_utc(), location, existing["id"])
    else:
        await con.execute(
            "INSERT INTO devices (user_id,device_name,device_info,ip,location,last_seen,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            uid, device_name, info, ip, location, now_utc(), now_utc())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
