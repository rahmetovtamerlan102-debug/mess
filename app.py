#!/usr/bin/env python3
import asyncio
import base64
import hashlib
import json
import os
import random
import uuid
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

import asyncpg
import requests as rq
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, FileResponse

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
BREVO_KEY = os.environ.get("BREVO_API_KEY", "").strip()
BREVO_SENDER = os.environ.get("BREVO_SENDER", "").strip()

AVATAR_DIR = os.path.expanduser("~/avatars")
UPLOAD_DIR = os.path.expanduser("~/uploads")
STICKER_DIR = os.path.expanduser("~/stickers")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_FILE = os.path.join(BASE_DIR, "index.html")
MANIFEST_FILE = os.path.join(BASE_DIR, "manifest.json")
GIFT_DIR = os.path.join(BASE_DIR, "gifts")
PORT = int(os.environ.get("PORT", 10000))

SECRET_TTL = 86400
TIMER_VALUES = (0, 86400, 604800, 2592000)
CALL_TIMEOUT = 30

# === PREMIUM ===
PREMIUM_PRICES = {30: 250, 90: 600, 365: 1990}
MAX_FILE_FREE = 8 * 1024 * 1024
MAX_FILE_PREMIUM = 8 * 1024 * 1024
# === /PREMIUM ===

ALLOWED_MIME = {
    "jpeg": "jpg", "jpg": "jpg", "png": "png", "gif": "gif",
    "webp": "webp", "mp3": "mp3", "ogg": "ogg", "webm": "webm",
    "mp4": "mp4", "m4a": "m4a", "wav": "wav",
}

GIFTS = {
    "bear": {"name": "Мишка", "emoji": "🧸", "stars": 15, "lottie": "bear.json"},
    "rose": {"name": "Роза", "emoji": "🌹", "stars": 5, "lottie": "rose.json"},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("aitgram")

pool: asyncpg.Pool | None = None

active_calls: dict[str, dict] = {}
ringing: dict[str, dict] = {}


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
    owner TEXT, contact TEXT, ts TIMESTAMP,
    first_name TEXT, last_name TEXT, note TEXT,
    UNIQUE(owner, contact)
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
CREATE TABLE IF NOT EXISTS email_codes (
    id SERIAL PRIMARY KEY, user_id TEXT, email TEXT,
    code TEXT, created_at TIMESTAMP, used BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS chat_timer (
    a TEXT, b TEXT, seconds INTEGER DEFAULT 0, UNIQUE(a, b)
);
CREATE TABLE IF NOT EXISTS chat_hidden (
    owner TEXT, partner TEXT, ts TIMESTAMP, UNIQUE(owner, partner)
);
CREATE TABLE IF NOT EXISTS reports (
    id SERIAL PRIMARY KEY, from_user TEXT, target TEXT, reason TEXT, ts TIMESTAMP
);
CREATE TABLE IF NOT EXISTS publications (
    id TEXT PRIMARY KEY, user_id TEXT, media_url TEXT,
    media_type TEXT, ts TIMESTAMP
);
CREATE TABLE IF NOT EXISTS hidden_messages (
    user_id TEXT, msg_id INTEGER, UNIQUE(user_id, msg_id)
);
CREATE TABLE IF NOT EXISTS chats (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    avatar TEXT,
    owner TEXT NOT NULL,
    created_at TIMESTAMP,
    channel_type TEXT DEFAULT 'public',
    channel_link TEXT DEFAULT '',
    autodelete INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS chat_members (
    chat_id TEXT, user_id TEXT,
    is_admin BOOLEAN DEFAULT FALSE,
    joined_at TIMESTAMP,
    PRIMARY KEY (chat_id, user_id)
);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS secret BOOLEAN DEFAULT FALSE;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS expire_at TIMESTAMP;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS chat_id TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS first_name TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS last_name TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS note TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS phone_country TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS phone_country_flag TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS name_updated_at TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS photo_updated_at TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS prof_c INTEGER DEFAULT -1;
ALTER TABLE users ADD COLUMN IF NOT EXISTS prof_pat TEXT DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS prof_emo TEXT DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS prof_nc INTEGER DEFAULT -1;
ALTER TABLE users ADD COLUMN IF NOT EXISTS stars INTEGER DEFAULT 0;
-- === PREMIUM ===
ALTER TABLE users ADD COLUMN IF NOT EXISTS premium BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS premium_until TIMESTAMP;
-- === /PREMIUM ===
ALTER TABLE gifts ADD COLUMN IF NOT EXISTS gift_id TEXT;
ALTER TABLE gifts ADD COLUMN IF NOT EXISTS gift_file TEXT;
CREATE INDEX IF NOT EXISTS idx_messages_pair_ts ON messages (from_user, to_user, ts DESC);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages (ts DESC);
CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages (chat_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_chat_members_user ON chat_members (user_id);
"""


async def init_db():
    p = await get_pool()
    async with p.acquire() as con:
        await con.execute(SCHEMA)
        try:
            await con.execute("UPDATE users SET user_id = username WHERE user_id <> username")
            await con.execute("DELETE FROM usernames WHERE user_id NOT IN (SELECT user_id FROM users)")
            await con.execute(
                """INSERT INTO usernames (user_id, username, is_primary, created_at)
                   SELECT user_id, username, TRUE, COALESCE(created_at, NOW())
                   FROM users
                   WHERE user_id NOT IN (SELECT user_id FROM usernames)
                   ON CONFLICT (username) DO NOTHING""")
        except Exception as e:
            logger.warning(f"[init_db cleanup] {e}")


def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def clean_u(s):
    return (s or "").strip().lower().lstrip("@").strip()


def pair(a, b):
    return (a, b) if a < b else (b, a)


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


def save_file(folder, name_prefix, b64data, max_size=None):
    if not b64data or "," not in b64data:
        return None
    if max_size is None:
        max_size = MAX_FILE_FREE
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
            return None
        raw = base64.b64decode(b64)
        if len(raw) > max_size:
            return None
        fname = f"{name_prefix}_{uuid.uuid4().hex[:12]}.{ext}"
        with open(os.path.join(folder, fname), "wb") as f:
            f.write(raw)
        return fname
    except Exception as e:
        logger.error(f"[file] {e}")
        return None


# === PREMIUM ===
def is_premium(row) -> bool:
    try:
        if isinstance(row, dict):
            prem = row.get("premium")
            until = row.get("premium_until")
        else:
            prem = row["premium"]
            until = row["premium_until"]
        if not prem:
            return False
        if until and until < now_utc():
            return False
        return True
    except Exception:
        return False


def premium_fields(row) -> dict:
    try:
        if isinstance(row, dict):
            until = row.get("premium_until")
        else:
            until = row["premium_until"]
        active = is_premium(row)
        return {
            "premium": active,
            "premium_until": until.isoformat() + "Z" if until else None,
        }
    except Exception:
        return {"premium": False, "premium_until": None}


def max_upload_for(row) -> int:
    return MAX_FILE_FREE
# === /PREMIUM ===


async def build_reply_fields(con, reply_to):
    """Возвращает (reply_text, reply_from) для указанного id сообщения."""
    if not reply_to:
        return None, None
    try:
        parent = await con.fetchrow(
            "SELECT text, from_user FROM messages WHERE id=$1", int(reply_to))
    except Exception:
        return None, None
    if not parent:
        return None, None
    txt = (parent["text"] or "")[:200]
    return txt, parent["from_user"]


def geo_ip(ip):
    try:
        if not ip or ip.startswith("127.") or ip.startswith("10."):
            return {"country": "Unknown", "city": ""}
        r = rq.get(f"https://ipwho.is/{ip}", timeout=4)
        d = r.json()
        if d.get("success"):
            return {"country": d.get("country", ""), "city": d.get("city", "")}
    except Exception as e:
        logger.error(f"[geo_ip] {e}")
    return {"country": "Unknown", "city": ""}


def gen_code():
    return str(random.randint(100000, 999999))


async def send_email_code(to_email: str, code: str):
    logger.info(f"[email] Отправка кода {code} на {to_email}")
    if not BREVO_KEY or not BREVO_SENDER:
        logger.error("[email] BREVO_API_KEY или BREVO_SENDER не заданы")
        return False, "BREVO_API_KEY/BREVO_SENDER не заданы"

    def _send():
        return rq.post(
            "https://api.brevo.com/v3/smtp/email",
            headers={
                "accept": "application/json",
                "api-key": BREVO_KEY,
                "content-type": "application/json",
            },
            json={
                "sender": {"name": "Aitgram", "email": BREVO_SENDER},
                "to": [{"email": to_email}],
                "subject": "Код подтверждения Aitgram",
                "htmlContent": f"""<!DOCTYPE html>
<html><body style="font-family:system-ui,sans-serif;background:#0f1115;color:#fff;padding:30px;margin:0">
  <div style="max-width:480px;margin:0 auto;background:#1c1c1e;border-radius:16px;padding:32px;text-align:center">
    <div style="font-size:56px;margin-bottom:16px;line-height:1">📫</div>
    <h2 style="color:#4f7cff;margin:0 0 12px 0;font-weight:700">Код подтверждения Aitgram</h2>
    <p style="color:#8b909a;font-size:14px;margin:0 0 24px 0">Введите этот код в приложении Aitgram</p>
    <div style="font-size:42px;font-weight:700;letter-spacing:8px;color:#fff;padding:20px;background:#000;border-radius:14px;margin-bottom:20px;font-family:monospace">{code}</div>
    <p style="color:#8b909a;font-size:12px;margin:0">Код действует 10 минут</p>
  </div>
</body></html>""",
            },
            timeout=15,
        )

    try:
        r = await asyncio.to_thread(_send)
        if r.status_code in (200, 201, 202):
            logger.info(f"[brevo] OK → {to_email}")
            return True, None
        logger.error(f"[brevo] {r.status_code}: {r.text[:400]}")
        return False, f"HTTP {r.status_code}: {r.text[:150]}"
    except Exception as e:
        logger.error(f"[brevo] exception: {e}")
        return False, str(e)


clients: dict[str, WebSocket] = {}


def bind_client(uid: str, ws: WebSocket):
    for k, v in list(clients.items()):
        if v is ws and k != uid:
            del clients[k]
    clients[uid] = ws


async def send_to(uid, payload):
    ws = clients.get(uid)
    if ws:
        try:
            await ws.send_text(json.dumps(payload, default=str))
            return True
        except Exception:
            pass
    return False


async def blocked_by_me(con, owner):
    rows = await con.fetch("SELECT blocked FROM blacklist WHERE owner=$1", owner)
    return {r["blocked"] for r in rows}


async def who_blocked(con, uid):
    rows = await con.fetch("SELECT owner FROM blacklist WHERE blocked=$1", uid)
    return {r["owner"] for r in rows}


async def is_blocked(con, owner, who):
    return bool(await con.fetchval(
        "SELECT 1 FROM blacklist WHERE owner=$1 AND blocked=$2", owner, who))


async def get_timer(con, a, b):
    x, y = pair(a, b)
    return await con.fetchval("SELECT seconds FROM chat_timer WHERE a=$1 AND b=$2", x, y) or 0


# ===================== GROUPS & CHANNELS =====================
import re as _re


def is_group_id(s: str) -> bool:
    return isinstance(s, str) and (s.startswith("g_") or s.startswith("c_"))


def is_channel_id(s: str) -> bool:
    return isinstance(s, str) and s.startswith("c_")


def new_chat_id(kind: str) -> str:
    return ("c_" if kind == "channel" else "g_") + uuid.uuid4().hex[:10]


async def get_group_timer(con, chat_id: str) -> int:
    return await con.fetchval("SELECT autodelete FROM chats WHERE id=$1", chat_id) or 0


async def is_chat_member(con, chat_id: str, uid: str) -> bool:
    return bool(await con.fetchval(
        "SELECT 1 FROM chat_members WHERE chat_id=$1 AND user_id=$2", chat_id, uid))


async def is_chat_admin(con, chat_id: str, uid: str) -> bool:
    return bool(await con.fetchval(
        "SELECT 1 FROM chat_members WHERE chat_id=$1 AND user_id=$2 AND is_admin=TRUE",
        chat_id, uid))


async def chat_members_list(con, chat_id: str):
    rows = await con.fetch(
        """SELECT cm.user_id, cm.is_admin, u.first_name, u.avatar, u.last_seen,
                  u.premium
           FROM chat_members cm
           LEFT JOIN users u ON u.username = cm.user_id
           WHERE cm.chat_id=$1
           ORDER BY cm.is_admin DESC, cm.joined_at""", chat_id)
    out = []
    for r in rows:
        uid = r["user_id"]
        out.append({
            "u": uid,
            "n": r["first_name"] or uid,
            "av": r["avatar"] or "",
            "admin": bool(r["is_admin"]),
            "online": uid in clients,
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
            "premium": bool(r["premium"]),
        })
    return out


async def chat_summary(con, chat_id: str, uid: str):
    row = await con.fetchrow(
        """SELECT c.id, c.kind, c.name, c.description, c.avatar,
                  c.channel_type, c.channel_link, c.autodelete,
                  (SELECT text FROM messages m
                   WHERE m.chat_id=c.id AND m.deleted=FALSE
                   ORDER BY m.id DESC LIMIT 1) AS last_text,
                  (SELECT ts FROM messages m
                   WHERE m.chat_id=c.id AND m.deleted=FALSE
                   ORDER BY m.id DESC LIMIT 1) AS last_ts,
                  (SELECT from_user FROM messages m
                   WHERE m.chat_id=c.id AND m.deleted=FALSE
                   ORDER BY m.id DESC LIMIT 1) AS last_from
           FROM chats c WHERE c.id=$1""", chat_id)
    if not row:
        return None
    cnt = await con.fetchval(
        "SELECT COUNT(*) FROM chat_members WHERE chat_id=$1", chat_id) or 0
    last_ts = row["last_ts"]
    unread = 0
    if last_ts:
        unread = await con.fetchval(
            """SELECT COUNT(*) FROM messages
               WHERE chat_id=$1 AND deleted=FALSE AND read=FALSE
                 AND from_user<>$2""", chat_id, uid) or 0
    return {
        "user": row["id"],
        "kind": row["kind"],
        "name": row["name"],
        "description": row["description"] or "",
        "avatar": row["avatar"] or "",
        "channel_type": row["channel_type"] or "public",
        "channel_link": row["channel_link"] or "",
        "autodelete": row["autodelete"] or 0,
        "text": row["last_text"] or "",
        "ts": row["last_ts"].isoformat() if row["last_ts"] else None,
        "last_from": row["last_from"],
        "unread": unread,
        "mine": row["last_from"] == uid if row["last_from"] else False,
        "members_count": cnt,
    }


async def send_group_payload_to_members(con, chat_id: str, payload: dict, exclude: str = None):
    rows = await con.fetch("SELECT user_id FROM chat_members WHERE chat_id=$1", chat_id)
    for r in rows:
        uid = r["user_id"]
        if uid == exclude:
            continue
        await send_to(uid, payload)
# ===================== /GROUPS & CHANNELS =====================


async def broadcast_presence(uid, kind):
    try:
        p = await get_pool()
        async with p.acquire() as con:
            bl = await blocked_by_me(con, uid)
    except Exception:
        bl = set()
    for c in list(clients.keys()):
        if c != uid and c not in bl:
            await send_to(c, {"type": kind, "user": uid})


async def _save_missed_call(caller_id: str, callee_id: str, media: str = "audio",
                             rejected: bool = False, cancelled: bool = False):
    if not caller_id or not callee_id or caller_id == callee_id:
        return
    labels = {
        ("audio", False, False): "📞 Пропущенный аудиозвонок",
        ("video", False, False): "📹 Пропущенный видеозвонок",
        ("audio", True, False):  "📞 Отклонённый аудиозвонок",
        ("video", True, False):  "📹 Отклонённый видеозвонок",
        ("audio", False, True):  "📞 Отменённый аудиозвонок",
        ("video", False, True):  "📹 Отменённый видеозвонок",
    }
    label = labels.get((media, rejected, cancelled), "📞 Пропущенный звонок")
    ts = now_utc()
    try:
        p = await get_pool()
        async with p.acquire() as con:
            mid = await con.fetchval(
                """INSERT INTO messages
                   (from_user, to_user, text, ts, media_type)
                   VALUES ($1,$2,$3,$4,'call')
                   RETURNING id""",
                caller_id, callee_id, label, ts)
    except Exception as e:
        logger.error(f"[save_missed_call] {e}")
        mid = None

    payload = {
        "type": "msg",
        "id": mid,
        "from": caller_id,
        "to": callee_id,
        "text": label,
        "ts": ts.isoformat(),
        "media_type": "call",
        "media_url": None,
    }
    await send_to(callee_id, payload)


async def calls_watchdog():
    while True:
        await asyncio.sleep(2)
        now = time.time()
        for callee, info in list(ringing.items()):
            if now - info["ts"] > CALL_TIMEOUT:
                ringing.pop(callee, None)
                caller = info["from"]
                await send_to(caller, {"type": "call_no_answer", "to": callee})
                await send_to(callee, {"type": "call_missed", "from": caller, "media": info["media"]})
                await _save_missed_call(caller, callee, info["media"])


async def expire_loop():
    while True:
        await asyncio.sleep(30)
        try:
            p = await get_pool()
            async with p.acquire() as con:
                rows = await con.fetch(
                    """UPDATE messages SET deleted=TRUE, text=''
                       WHERE deleted=FALSE AND expire_at IS NOT NULL AND expire_at<=$1
                       RETURNING id, from_user, to_user""", now_utc())
            for r in rows:
                payload = {"type": "msg_deleted", "msg_id": r["id"]}
                await send_to(r["from_user"], payload)
                await send_to(r["to_user"], payload)
        except Exception as e:
            logger.error(f"[expire] {e}")


# === PREMIUM ===
async def premium_expiry_worker():
    while True:
        await asyncio.sleep(3600)
        try:
            p = await get_pool()
            async with p.acquire() as con:
                rows = await con.fetch(
                    """UPDATE users SET premium=FALSE
                       WHERE premium=TRUE AND premium_until IS NOT NULL
                         AND premium_until <= $1
                       RETURNING user_id""", now_utc())
            for r in rows:
                uid = r["user_id"]
                await send_to(uid, {"type": "premium_expired"})
                logger.info(f"[premium] expired for {uid}")
        except Exception as e:
            logger.error(f"[premium_expiry] {e}")
# === /PREMIUM ===


# === EMAIL CODE CLEANUP ===
async def email_codes_cleanup():
    while True:
        await asyncio.sleep(3600)
        try:
            p = await get_pool()
            async with p.acquire() as con:
                deleted = await con.fetchval(
                    """WITH del AS (
                           DELETE FROM email_codes
                           WHERE created_at < NOW() - INTERVAL '24 hours'
                           RETURNING 1
                       )
                       SELECT COUNT(*) FROM del""")
                if deleted:
                    logger.info(f"[email_codes] cleaned {deleted} old codes")
        except Exception as e:
            logger.error(f"[email_codes_cleanup] {e}")
# === /EMAIL CODE CLEANUP ===


@asynccontextmanager
async def lifespan(app: FastAPI):
    for d in (AVATAR_DIR, UPLOAD_DIR, STICKER_DIR, GIFT_DIR):
        os.makedirs(d, exist_ok=True)
    logger.info(f"[server] DATABASE_URL = {'✓' if DATABASE_URL else '✗'}")
    logger.info(f"[server] BREVO_API_KEY = {'✓' if BREVO_KEY else '✗'}")
    logger.info(f"[server] BREVO_SENDER = {BREVO_SENDER or '✗'}")
    logger.info(f"[server] GIFTS = {list(GIFTS.keys())}")
    task = None
    task_calls = None
    task_premium = None
    task_codes = None
    if DATABASE_URL and DATABASE_URL.startswith("postgres"):
        await init_db()
        task = asyncio.create_task(expire_loop())
        task_calls = asyncio.create_task(calls_watchdog())
        task_premium = asyncio.create_task(premium_expiry_worker())
        task_codes = asyncio.create_task(email_codes_cleanup())
    yield
    for t in (task, task_calls, task_premium, task_codes):
        if t:
            t.cancel()
    if pool:
        await pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def root():
    if os.path.exists(HTML_FILE):
        return FileResponse(HTML_FILE)
    return HTMLResponse("<h1>Aitgram</h1>")


@app.get("/manifest.json")
def manifest():
    if os.path.exists(MANIFEST_FILE):
        return FileResponse(MANIFEST_FILE, media_type="application/manifest+json")
    return HTMLResponse("", status_code=404)


@app.get("/sw.js")
def sw():
    js = ("self.addEventListener('install',e=>self.skipWaiting());"
          "self.addEventListener('activate',e=>self.clients.claim());"
          "self.addEventListener('fetch',e=>{});")
    return HTMLResponse(js, media_type="application/javascript")


@app.get("/lottie.js")
def lottie_js():
    p = os.path.join(BASE_DIR, "lottie.min.js")
    if os.path.exists(p):
        return FileResponse(p, media_type="application/javascript")
    return HTMLResponse("", status_code=404)


@app.get("/healthz")
def healthz():
    return {"ok": True, "db": bool(DATABASE_URL),
            "brevo": bool(BREVO_KEY), "sender": BREVO_SENDER or None,
            "gifts": list(GIFTS.keys())}


@app.get("/avatars/{fname}")
def av_file(fname: str):
    p = os.path.join(AVATAR_DIR, os.path.basename(fname))
    if os.path.exists(p):
        return FileResponse(p, headers={"Cache-Control": "public, max-age=31536000, immutable"})
    return HTMLResponse("", status_code=404)


@app.get("/uploads/{fname}")
def up_file(fname: str):
    p = os.path.join(UPLOAD_DIR, os.path.basename(fname))
    if os.path.exists(p):
        return FileResponse(p, headers={"Cache-Control": "public, max-age=31536000, immutable"})
    return HTMLResponse("", status_code=404)


@app.get("/stickers/{fname}")
def st_file(fname: str):
    p = os.path.join(STICKER_DIR, os.path.basename(fname))
    if os.path.exists(p):
        return FileResponse(p)
    return HTMLResponse("", status_code=404)


@app.get("/gifts/{fname}")
def gift_file(fname: str):
    safe = os.path.basename(fname)
    if not safe.endswith(".json"):
        return HTMLResponse("", status_code=404)
    p = os.path.join(GIFT_DIR, safe)
    if os.path.exists(p):
        return FileResponse(p, media_type="application/json",
                            headers={"Cache-Control": "public, max-age=31536000"})
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
        logger.error(f"[geo] {e}")
    return {"country": "Unknown", "code": "", "timezone": "UTC", "offset": "+00:00"}


def parse_device(ua: str) -> str:
    if not ua:
        return "Неизвестное устройство"
    ua_l = ua.lower()
    if "android" in ua_l:
        import re
        m = re.search(r"android [\d.]+; ([^)]+)\)", ua, re.I)
        return (m.group(1).split(";")[0].strip() if m else "Android")
    if "iphone" in ua_l:
        return "iPhone"
    if "ipad" in ua_l:
        return "iPad"
    if "macintosh" in ua_l or "mac os" in ua_l:
        return "macOS"
    if "windows" in ua_l:
        return "Windows"
    if "linux" in ua_l:
        return "Linux"
    return "Устройство"


async def _register_device(con, uid, websocket, token):
    ua = websocket.headers.get("user-agent", "")
    ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
         (websocket.client.host if websocket.client else "")
    device_name = parse_device(ua)
    info = "Браузер"
    ua_l = ua.lower()
    if "telegram" in ua_l:
        info = "Telegram"
    elif "chrome" in ua_l:
        info = "Chrome"
    elif "firefox" in ua_l:
        info = "Firefox"
    elif "safari" in ua_l:
        info = "Safari"
    g = geo_ip(ip)
    location = ", ".join([x for x in [g.get("city"), g.get("country")] if x])
    existing = await con.fetchrow(
        "SELECT id FROM devices WHERE user_id=$1 AND ip=$2 AND device_name=$3",
        uid, ip, device_name)
    if existing:
        await con.execute(
            "UPDATE devices SET last_seen=$1, location=$2 WHERE id=$3",
            now_utc(), location, existing["id"])
    else:
        await con.execute(
            """INSERT INTO devices
               (user_id,device_name,device_info,ip,location,last_seen,created_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7)""",
            uid, device_name, info, ip, location, now_utc(), now_utc())


async def rename_user_everywhere(con, old_id: str, new_id: str):
    if old_id == new_id:
        return
    await con.execute("UPDATE messages SET from_user=$1 WHERE from_user=$2", new_id, old_id)
    await con.execute("UPDATE messages SET to_user=$1 WHERE to_user=$2", new_id, old_id)
    await con.execute("UPDATE contacts SET owner=$1 WHERE owner=$2", new_id, old_id)
    await con.execute("UPDATE contacts SET contact=$1 WHERE contact=$2", new_id, old_id)
    await con.execute("UPDATE blacklist SET owner=$1 WHERE owner=$2", new_id, old_id)
    await con.execute("UPDATE blacklist SET blocked=$1 WHERE blocked=$2", new_id, old_id)
    await con.execute("UPDATE chat_timer SET a=$1 WHERE a=$2", new_id, old_id)
    await con.execute("UPDATE chat_timer SET b=$1 WHERE b=$2", new_id, old_id)
    await con.execute("UPDATE chat_hidden SET owner=$1 WHERE owner=$2", new_id, old_id)
    await con.execute("UPDATE chat_hidden SET partner=$1 WHERE partner=$2", new_id, old_id)
    await con.execute("UPDATE gifts SET from_user=$1 WHERE from_user=$2", new_id, old_id)
    await con.execute("UPDATE gifts SET to_user=$1 WHERE to_user=$2", new_id, old_id)
    await con.execute("UPDATE publications SET user_id=$1 WHERE user_id=$2", new_id, old_id)
    await con.execute("UPDATE sessions SET user_id=$1 WHERE user_id=$2", new_id, old_id)
    await con.execute("UPDATE devices SET user_id=$1 WHERE user_id=$2", new_id, old_id)
    await con.execute("UPDATE privacy SET user_id=$1 WHERE user_id=$2", new_id, old_id)
    await con.execute("UPDATE emails SET user_id=$1 WHERE user_id=$2", new_id, old_id)
    await con.execute("UPDATE reactions SET chat_id=$1 WHERE chat_id=$2", new_id, old_id)
    await con.execute("UPDATE reactions SET user_id=$1 WHERE user_id=$2", new_id, old_id)
    await con.execute("UPDATE pinned SET chat_id=$1 WHERE chat_id=$2", new_id, old_id)
    await con.execute("UPDATE pinned SET pinned_by=$1 WHERE pinned_by=$2", new_id, old_id)
    await con.execute("UPDATE reports SET from_user=$1 WHERE from_user=$2", new_id, old_id)
    await con.execute("UPDATE reports SET target=$1 WHERE target=$2", new_id, old_id)
    await con.execute("UPDATE chat_members SET user_id=$1 WHERE user_id=$2", new_id, old_id)
    await con.execute("UPDATE chats SET owner=$1 WHERE owner=$2", new_id, old_id)
    await con.execute(
        "UPDATE users SET user_id=$1, username=$1 WHERE user_id=$2",
        new_id, old_id)


@app.websocket("/ws")
async def ws_ep(websocket: WebSocket):
    await websocket.accept()
    user_id = None
    p = await get_pool()
    try:
        while True:
            try:
                raw = await websocket.receive_text()
            except WebSocketDisconnect:
                raise

            try:
                data = json.loads(raw)
            except Exception:
                try:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "bad json"}))
                except Exception:
                    raise WebSocketDisconnect()
                continue

            cmd = data.get("cmd")

            try:
                # ===================== КОМАНДЫ =====================

                if cmd == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
                    continue

                if cmd == "typing":
                    if not user_id:
                        continue
                    to = clean_u(data.get("to"))
                    if to and to != user_id:
                        async with p.acquire() as con:
                            if not await is_blocked(con, user_id, to):
                                await send_to(to, {"type": "typing", "from": user_id})
                    continue

                if cmd == "get_gifts":
                    async with p.acquire() as con:
                        bal = await con.fetchval(
                            "SELECT stars FROM users WHERE user_id=$1", user_id) or 0
                    await websocket.send_text(json.dumps({
                        "type": "gift_catalog",
                        "balance": bal,
                        "list": [
                            {"id": k, "name": v["name"], "emoji": v["emoji"],
                             "stars": v["stars"], "lottie": v["lottie"]}
                            for k, v in GIFTS.items()
                        ]
                    }))
                    continue

                if cmd == "send_gift":
                    to = clean_u(data.get("to"))
                    gid = (data.get("gift") or data.get("gift_id") or "").strip()
                    if not to or to == user_id or gid not in GIFTS:
                        await websocket.send_text(json.dumps(
                            {"type": "error", "msg": "неверный подарок"}))
                        continue
                    g = GIFTS[gid]
                    ts = now_utc()
                    async with p.acquire() as con:
                        if not await con.fetchval("SELECT 1 FROM users WHERE user_id=$1", to):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "получатель не найден"}))
                            continue
                        if await is_blocked(con, to, user_id):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "нельзя отправить"}))
                            continue
                        async with con.transaction():
                            bal = await con.fetchval(
                                """UPDATE users SET stars=stars-$1
                                   WHERE user_id=$2 AND stars >= $1
                                   RETURNING stars""",
                                g["stars"], user_id)
                            if bal is None:
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "Недостаточно звёзд"}))
                                continue
                            await con.execute(
                                """INSERT INTO gifts
                                   (from_user,to_user,gift_name,gift_emoji,stars,ts,gift_id,gift_file)
                                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
                                user_id, to, g["name"], g["emoji"], g["stars"], ts,
                                gid, g["lottie"])
                    await send_to(to, {"type": "gift_received", "from": user_id,
                                       "name": g["name"], "emoji": g["emoji"],
                                       "gift": gid, "stars": g["stars"],
                                       "ts": ts.isoformat()})
                    await websocket.send_text(json.dumps({
                        "type": "gift_sent", "to": to, "name": g["name"],
                        "emoji": g["emoji"], "gift": gid, "stars": g["stars"],
                        "balance": bal}))
                    continue

                # === PREMIUM ===
                if cmd == "buy_premium":
                    if not user_id:
                        continue
                    try:
                        days = int(data.get("days", 30))
                    except Exception:
                        days = 30
                    if days not in PREMIUM_PRICES:
                        await websocket.send_text(json.dumps(
                            {"type": "error", "msg": "неверный тариф"}))
                        continue
                    price = PREMIUM_PRICES[days]
                    async with p.acquire() as con:
                        async with con.transaction():
                            row = await con.fetchrow(
                                "SELECT stars, premium_until FROM users WHERE user_id=$1",
                                user_id)
                            if not row:
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "юзер не найден"}))
                                continue
                            bal = row["stars"] or 0
                            if bal < price:
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "Недостаточно звёзд"}))
                                continue
                            now = now_utc()
                            current_until = row["premium_until"]
                            if current_until and current_until > now:
                                new_until = current_until + timedelta(days=days)
                            else:
                                new_until = now + timedelta(days=days)
                            new_bal = await con.fetchval(
                                """UPDATE users SET stars=stars-$1, premium=TRUE,
                                                    premium_until=$2
                                   WHERE user_id=$3 AND stars >= $1
                                   RETURNING stars""",
                                price, new_until, user_id)
                            if new_bal is None:
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "Недостаточно звёзд"}))
                                continue
                    await websocket.send_text(json.dumps({
                        "type": "premium_bought",
                        "until": new_until.isoformat() + "Z",
                        "balance": new_bal,
                        "days": days,
                    }))
                    logger.info(f"[premium] {user_id} bought {days}d for {price}★")
                    continue
                # === /PREMIUM ===

                if cmd == "send_reg_code":
                    em = (data.get("email") or "").strip().lower()
                    if "@" not in em or "." not in em:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "неверный email"}))
                        continue
                    code = gen_code()
                    async with p.acquire() as con:
                        await con.execute("DELETE FROM email_codes WHERE email=$1 AND used=FALSE", em)
                        await con.execute(
                            """INSERT INTO email_codes (user_id,email,code,created_at,used)
                               VALUES ($1,$2,$3,$4,FALSE)""",
                            "reg:" + em, em, code, now_utc())
                    ok, err = await send_email_code(em, code)
                    await websocket.send_text(json.dumps({
                        "type": "reg_code_sent",
                        "email": em,
                        "dev_code": None,
                        "error": None if ok else (err or "Не удалось отправить код"),
                    }))
                    continue

                if cmd == "check_reg_code":
                    em = (data.get("email") or "").strip().lower()
                    code = (data.get("code") or "").strip()
                    async with p.acquire() as con:
                        row = await con.fetchrow(
                            """SELECT id FROM email_codes
                               WHERE email=$1 AND code=$2
                                 AND created_at > NOW() - INTERVAL '30 minutes'
                               ORDER BY id DESC LIMIT 1""", em, code)
                        if not row:
                            logger.warning(f"[check_reg_code] FAIL em={em!r} code={code!r}")
                            dbg = await con.fetch(
                                """SELECT id, code, used, created_at FROM email_codes
                                   WHERE email=$1 ORDER BY id DESC LIMIT 3""", em)
                            for r in dbg:
                                logger.warning(f"  dbg: id={r['id']} code={r['code']!r} used={r['used']} created={r['created_at']}")
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "Неверный или истёкший код"}))
                            continue

                        existing = await con.fetchrow(
                            """SELECT u.user_id, u.username, u.first_name, u.avatar
                               FROM emails e JOIN users u ON u.user_id = e.user_id
                               WHERE e.email=$1 AND e.verified=TRUE LIMIT 1""", em)

                        if existing:
                            await con.execute("UPDATE email_codes SET used=TRUE WHERE id=$1", row["id"])
                            uid = existing["user_id"]
                            tok = uuid.uuid4().hex
                            ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                                 (websocket.client.host if websocket.client else "")
                            await con.execute(
                                """INSERT INTO sessions (token,user_id,created_at,last_ip)
                                   VALUES ($1,$2,$3,$4)""", tok, uid, now_utc(), ip)
                            await con.execute(
                                "UPDATE users SET last_seen=$1 WHERE user_id=$2", now_utc(), uid)
                            await _register_device(con, uid, websocket, tok)
                            user_id = uid
                            bind_client(uid, websocket)
                            await websocket.send_text(json.dumps({
                                "type": "logged_in", "user_id": uid, "token": tok,
                                "first_name": existing["first_name"] or uid,
                                "avatar": existing["avatar"] or ""}))
                            await broadcast_presence(uid, "user_online")
                            continue

                    await websocket.send_text(json.dumps({"type": "reg_code_ok", "email": em}))
                    continue

                if cmd == "login_by_email":
                    em = (data.get("email") or "").strip().lower()
                    code = (data.get("code") or "").strip()
                    async with p.acquire() as con:
                        row = await con.fetchrow(
                            """SELECT id FROM email_codes WHERE email=$1 AND code=$2
                               AND created_at > NOW() - INTERVAL '30 minutes'
                               ORDER BY id DESC LIMIT 1""", em, code)
                        if not row:
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "Неверный код"}))
                            continue
                        await con.execute("UPDATE email_codes SET used=TRUE WHERE id=$1", row["id"])
                        u = await con.fetchrow(
                            """SELECT u.user_id, u.username, u.first_name, u.avatar
                               FROM emails e JOIN users u ON u.user_id = e.user_id
                               WHERE e.email=$1 LIMIT 1""", em)
                        if not u:
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "Аккаунт не найден"}))
                            continue
                        tok = uuid.uuid4().hex
                        ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                             (websocket.client.host if websocket.client else "")
                        await con.execute(
                            """INSERT INTO sessions (token,user_id,created_at,last_ip)
                               VALUES ($1,$2,$3,$4)""", tok, u["user_id"], now_utc(), ip)
                        await con.execute(
                            "UPDATE users SET last_seen=$1 WHERE user_id=$2",
                            now_utc(), u["user_id"])
                        await _register_device(con, u["user_id"], websocket, tok)
                    user_id = u["user_id"]
                    bind_client(user_id, websocket)
                    await websocket.send_text(json.dumps({
                        "type": "logged_in", "user_id": user_id, "token": tok,
                        "first_name": u["first_name"] or user_id,
                        "avatar": u["avatar"] or ""}))
                    await broadcast_presence(user_id, "user_online")
                    continue

                if cmd == "register_email":
                    em = (data.get("email") or "").strip().lower()
                    code = (data.get("code") or "").strip()
                    u = clean_u(data.get("username"))
                    pw = data.get("password") or ""
                    first_name = (data.get("first_name") or "").strip() or u
                    avatar_data = data.get("avatar_data")
                    if not u or len(u) < 4 or not pw or len(pw) < 4:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "username/пароль 4+"}))
                        continue
                    if not u.replace("_", "").isalnum():
                        await websocket.send_text(json.dumps({"type": "error", "msg": "a-z 0-9 _"}))
                        continue
                    async with p.acquire() as con:
                        row = await con.fetchrow(
                            """SELECT id FROM email_codes
                               WHERE email=$1 AND code=$2
                                 AND created_at > NOW() - INTERVAL '30 minutes'
                               ORDER BY id DESC LIMIT 1""", em, code)
                        if not row:
                            logger.warning(f"[register_email] FAIL em={em!r} code={code!r}")
                            dbg = await con.fetch(
                                """SELECT id, code, used, created_at FROM email_codes
                                   WHERE email=$1 ORDER BY id DESC LIMIT 3""", em)
                            for r in dbg:
                                logger.warning(f"  dbg: id={r['id']} code={r['code']!r} used={r['used']} created={r['created_at']}")
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "Код не подтверждён (введите заново или запросите новый)"}))
                            continue

                        await con.execute("UPDATE email_codes SET used=TRUE WHERE id=$1", row["id"])

                        if await con.fetchval("SELECT 1 FROM users WHERE username=$1", u):
                            await websocket.send_text(json.dumps({"type": "error", "msg": "username занят"}))
                            continue
                        av = None
                        if avatar_data:
                            av = save_file(AVATAR_DIR, u, avatar_data, max_size=5 * 1024 * 1024)
                        try:
                            async with con.transaction():
                                await con.execute(
                                    """INSERT INTO users
                                       (user_id,username,password,first_name,bio,
                                        avatar,created_at,last_seen)
                                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
                                    u, u, hash_pw(pw), first_name, "", av, now_utc(), now_utc())
                                await con.execute(
                                    """INSERT INTO usernames
                                       (user_id,username,is_primary,created_at)
                                       VALUES ($1,$2,TRUE,$3)""",
                                    u, u, now_utc())
                                await con.execute(
                                    "INSERT INTO privacy (user_id) VALUES ($1) ON CONFLICT DO NOTHING", u)
                                await con.execute(
                                    """INSERT INTO emails (user_id,email,verified,ts)
                                       VALUES ($1,$2,TRUE,$3)""",
                                    u, em, now_utc())
                                ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                                     (websocket.client.host if websocket.client else "")
                                tok = uuid.uuid4().hex
                                await con.execute(
                                    """INSERT INTO sessions (token,user_id,created_at,last_ip)
                                       VALUES ($1,$2,$3,$4)""",
                                    tok, u, now_utc(), ip)
                                await _register_device(con, u, websocket, tok)
                        except asyncpg.UniqueViolationError:
                            await websocket.send_text(json.dumps({"type": "error", "msg": "занято"}))
                            continue
                    user_id = u
                    bind_client(u, websocket)
                    await websocket.send_text(json.dumps({
                        "type": "registered", "user_id": u, "token": tok,
                        "first_name": first_name, "avatar": av or ""}))
                    await broadcast_presence(u, "user_online")
                    continue

                if cmd == "resume":
                    tok = data.get("token") or ""
                    async with p.acquire() as con:
                        row = await con.fetchrow(
                            """SELECT user_id FROM sessions
                               WHERE token=$1
                                 AND created_at > NOW() - INTERVAL '30 days'""", tok)
                    if not row:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "session expired"}))
                        continue
                    uid = row["user_id"]
                    user_id = uid
                    bind_client(uid, websocket)
                    async with p.acquire() as con:
                        await _register_device(con, uid, websocket, tok)
                        u = await con.fetchrow(
                            "SELECT first_name, avatar FROM users WHERE user_id=$1", uid)
                    await websocket.send_text(json.dumps({
                        "type": "logged_in", "user_id": uid, "token": tok,
                        "resumed": True,
                        "first_name": (u["first_name"] if u else uid) or uid,
                        "avatar": (u["avatar"] if u else "") or ""}))
                    await broadcast_presence(uid, "user_online")
                    continue

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
                            """INSERT INTO sessions (token,user_id,created_at,last_ip)
                               VALUES ($1,$2,$3,$4)""", tok, u, now_utc(), ip)
                        await con.execute(
                            "INSERT INTO privacy (user_id) VALUES ($1) ON CONFLICT DO NOTHING", u)
                        await _register_device(con, u, websocket, tok)
                        urow = await con.fetchrow(
                            "SELECT first_name, avatar FROM users WHERE username=$1", u)
                    user_id = u
                    bind_client(u, websocket)
                    await websocket.send_text(json.dumps({
                        "type": "logged_in", "user_id": u, "token": tok,
                        "first_name": (urow["first_name"] if urow else u) or u,
                        "avatar": (urow["avatar"] if urow else "") or ""}))
                    await broadcast_presence(u, "user_online")
                    continue

                if not user_id:
                    await websocket.send_text(json.dumps({"type": "error", "msg": "сначала login"}))
                    continue

                # ===================== ЗВОНКИ =====================

                if cmd == "call_invite":
                    to = clean_u(data.get("to"))
                    media = data.get("media") or "audio"
                    if media not in ("audio", "video"):
                        media = "audio"
                    if not to or to == user_id:
                        await send_to(user_id, {"type": "call_error", "msg": "invalid_to"})
                        continue
                    if to not in clients:
                        await send_to(user_id, {"type": "call_error", "msg": "offline"})
                        await _save_missed_call(user_id, to, media)
                        continue
                    async with p.acquire() as con:
                        if await is_blocked(con, user_id, to) or await is_blocked(con, to, user_id):
                            await send_to(user_id, {"type": "call_error", "msg": "blocked"})
                            continue
                    if to in active_calls:
                        await send_to(user_id, {"type": "call_busy", "to": to})
                        continue
                    if user_id in active_calls:
                        await send_to(user_id, {"type": "call_error", "msg": "you_busy"})
                        continue

                    ringing[to] = {"from": user_id, "media": media, "ts": time.time()}
                    await send_to(to, {
                        "type": "call_incoming",
                        "from": user_id,
                        "media": media,
                    })
                    await send_to(user_id, {"type": "call_ringing_self", "to": to, "media": media})
                    continue

                if cmd == "call_accept":
                    info = ringing.pop(user_id, None)
                    if not info:
                        await send_to(user_id, {"type": "call_error", "msg": "no_incoming"})
                        continue
                    caller = info["from"]
                    if caller not in clients:
                        await send_to(user_id, {"type": "call_error", "msg": "caller_gone"})
                        continue
                    media = info["media"]
                    active_calls[user_id] = {"peer": caller, "media": media, "started_at": time.time()}
                    active_calls[caller] = {"peer": user_id, "media": media, "started_at": time.time()}
                    await send_to(caller, {"type": "call_accepted", "by": user_id, "media": media, "initiator": True})
                    await send_to(user_id, {"type": "call_accepted", "by": user_id, "media": media, "initiator": False})
                    continue

                if cmd == "call_reject":
                    info = ringing.pop(user_id, None)
                    if not info:
                        await send_to(user_id, {"type": "call_error", "msg": "no_incoming"})
                        continue
                    caller = info["from"]
                    await send_to(caller, {"type": "call_rejected", "by": user_id})
                    await _save_missed_call(caller, user_id, info["media"], rejected=True)
                    continue

                if cmd == "call_cancel":
                    to = clean_u(data.get("to"))
                    if not to:
                        continue
                    if ringing.get(to, {}).get("from") == user_id:
                        ringing.pop(to, None)
                        await send_to(to, {"type": "call_cancelled", "by": user_id})
                        await _save_missed_call(user_id, to, data.get("media", "audio"), cancelled=True)
                    continue

                if cmd == "call_end":
                    info = active_calls.pop(user_id, None)
                    if not info:
                        continue
                    peer = info["peer"]
                    active_calls.pop(peer, None)
                    await send_to(peer, {"type": "call_ended", "by": user_id})
                    await send_to(user_id, {"type": "call_ended", "by": user_id})
                    continue

                if cmd in ("webrtc_offer", "webrtc_answer", "webrtc_ice"):
                    to = clean_u(data.get("to"))
                    if not to or to not in clients:
                        continue
                    payload = {"type": cmd, "from": user_id, "to": to}
                    if cmd == "webrtc_ice":
                        payload["candidate"] = data.get("candidate")
                    else:
                        payload["sdp"] = data.get("sdp")
                    await send_to(to, payload)
                    continue

                if cmd == "call_check_peer":
                    to = clean_u(data.get("to"))
                    if not to:
                        await send_to(user_id, {"type": "call_peer_status", "to": to, "online": False, "busy": False})
                        continue
                    online = to in clients
                    busy = (to in active_calls) or (to in ringing)
                    await send_to(user_id, {
                        "type": "call_peer_status",
                        "to": to,
                        "online": online,
                        "busy": busy,
                    })
                    continue

                # ===================== ОСТАЛЬНЫЕ КОМАНДЫ =====================

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
                    allowed = ["seen","photo","fwd","calls","voice","msgs",
                               "bday","gifts","bio","music","inv","autodel"]
                    if k not in allowed:
                        continue
                    async with p.acquire() as con:
                        await con.execute(f"UPDATE privacy SET {k}=$1 WHERE user_id=$2", v, user_id)
                    await websocket.send_text(json.dumps({"type": "privacy_updated", "key": k, "value": v}))
                    continue

                if cmd == "get_devices":
                    async with p.acquire() as con:
                        rows = await con.fetch(
                            """SELECT id,device_name,device_info,ip,location,last_seen,created_at
                               FROM devices WHERE user_id=$1 ORDER BY last_seen DESC""", user_id)
                    cur_ip = websocket.headers.get("x-forwarded-for", "").split(",")[0].strip() or \
                             (websocket.client.host if websocket.client else "")
                    lst = [{
                        "id": r["id"], "name": r["device_name"] or "Устройство",
                        "info": r["device_info"] or "", "ip": r["ip"] or "",
                        "location": r["location"] or "",
                        "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
                        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                        "is_current": r["ip"] == cur_ip,
                    } for r in rows]
                    await websocket.send_text(json.dumps({"type": "devices", "list": lst}))
                    continue

                if cmd == "terminate_device":
                    did = data.get("id")
                    async with p.acquire() as con:
                        await con.execute("DELETE FROM devices WHERE id=$1 AND user_id=$2", did, user_id)
                    await websocket.send_text(json.dumps({"type": "device_terminated", "id": did}))
                    continue

                if cmd == "get_blacklist":
                    async with p.acquire() as con:
                        rows = await con.fetch(
                            "SELECT blocked, ts FROM blacklist WHERE owner=$1 ORDER BY ts DESC", user_id)
                    await websocket.send_text(json.dumps({
                        "type": "blacklist",
                        "list": [{"u": r["blocked"],
                                  "ts": r["ts"].isoformat() if r["ts"] else None} for r in rows]}))
                    continue

                if cmd in ("add_blacklist", "block"):
                    target = clean_u(data.get("user"))
                    if not target or target == user_id:
                        continue
                    async with p.acquire() as con:
                        await con.execute(
                            """INSERT INTO blacklist (owner,blocked,ts)
                               VALUES ($1,$2,$3) ON CONFLICT DO NOTHING""",
                            user_id, target, now_utc())
                    await websocket.send_text(json.dumps({"type": "blacklist_added", "user": target}))
                    await websocket.send_text(json.dumps({"type": "blocked", "user": target}))
                    await send_to(target, {"type": "blocked_by", "user": user_id})
                    continue

                if cmd in ("remove_blacklist", "unblock"):
                    target = clean_u(data.get("user"))
                    async with p.acquire() as con:
                        await con.execute("DELETE FROM blacklist WHERE owner=$1 AND blocked=$2",
                                          user_id, target)
                    await websocket.send_text(json.dumps({"type": "blacklist_removed", "user": target}))
                    await websocket.send_text(json.dumps({"type": "unblocked", "user": target}))
                    await send_to(target, {"type": "unblocked_by", "user": user_id})
                    continue

                if cmd == "blocked_list":
                    async with p.acquire() as con:
                        bl = await blocked_by_me(con, user_id)
                        by = await who_blocked(con, user_id)
                    await websocket.send_text(json.dumps({
                        "type": "blocked_list", "list": sorted(bl), "list_by": sorted(by)}))
                    continue

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
                    async with p.acquire() as con:
                        await con.execute("UPDATE privacy SET passkey=$1 WHERE user_id=$2", "on", user_id)
                    await websocket.send_text(json.dumps({"type": "passkey_added"}))
                    continue

                if cmd == "remove_passkey":
                    async with p.acquire() as con:
                        await con.execute("UPDATE privacy SET passkey=NULL WHERE user_id=$1", user_id)
                    await websocket.send_text(json.dumps({"type": "passkey_removed"}))
                    continue

                if cmd == "users":
                    async with p.acquire() as con:
                        rows = await con.fetch(
                            """SELECT username,first_name,avatar,last_seen,
                                      prof_c,prof_pat,prof_emo,prof_nc,
                                      premium,premium_until
                               FROM users ORDER BY username""")
                        blockers = await who_blocked(con, user_id)
                        hidden = {r["partner"] for r in await con.fetch(
                            "SELECT partner FROM chat_hidden WHERE owner=$1", user_id)}
                        contacts = {r["contact"] for r in await con.fetch(
                            "SELECT contact FROM contacts WHERE owner=$1", user_id)}
                    lst = []
                    for r in rows:
                        un = r["username"]
                        if un in hidden:
                            continue
                        if un in blockers:
                            online, ls = False, "long"
                        else:
                            online = un in clients
                            ls = r["last_seen"].isoformat() if r["last_seen"] else None
                        lst.append({
                            "u": un, "n": r["first_name"] or un,
                            "av": r["avatar"] or "",
                            "online": online, "ls": ls,
                            "prof_c": r["prof_c"] if r["prof_c"] is not None else -1,
                            "prof_pat": r["prof_pat"] or "",
                            "prof_emo": r["prof_emo"] or "",
                            "prof_nc": r["prof_nc"] if r["prof_nc"] is not None else -1,
                            **premium_fields(r),
                        })
                    await websocket.send_text(json.dumps({
                        "type": "users", "list": lst, "contacts": sorted(contacts)}))
                    continue

                if cmd == "search_users":
                    q = clean_u(data.get("q"))
                    async with p.acquire() as con:
                        rows = await con.fetch(
                            """SELECT username,first_name,avatar,premium,premium_until
                               FROM users
                               WHERE (LOWER(username) LIKE $1 OR LOWER(first_name) LIKE $1)
                                 AND username != $2 ORDER BY username LIMIT 30""",
                            f"%{q}%", user_id)
                    await websocket.send_text(json.dumps({
                        "type": "search_results",
                        "list": [{**{"u": r["username"], "n": r["first_name"] or r["username"],
                                     "av": r["avatar"] or ""}, **premium_fields(r)} for r in rows]}))
                    continue

                if cmd == "add_contact":
                    target = clean_u(data.get("user"))
                    if not target or target == user_id:
                        continue
                    async with p.acquire() as con:
                        if not await con.fetchval("SELECT 1 FROM users WHERE username=$1", target):
                            continue
                        await con.execute(
                            """INSERT INTO contacts (owner,contact,ts)
                               VALUES ($1,$2,$3) ON CONFLICT DO NOTHING""",
                            user_id, target, now_utc())
                        await con.execute("DELETE FROM chat_hidden WHERE owner=$1 AND partner=$2",
                                          user_id, target)
                    await websocket.send_text(json.dumps({"type": "contact_added", "user": target}))
                    continue

                if cmd == "delete_contact":
                    target = clean_u(data.get("user"))
                    if not target or target == user_id:
                        continue
                    async with p.acquire() as con:
                        await con.execute("DELETE FROM contacts WHERE owner=$1 AND contact=$2",
                                          user_id, target)
                    await websocket.send_text(json.dumps({"type": "contact_deleted", "user": target}))
                    continue

                if cmd == "save_contact":
                    target = clean_u(data.get("user"))
                    fn = (data.get("first_name") or "").strip()[:50]
                    ln = (data.get("last_name") or "").strip()[:50]
                    note = (data.get("note") or "").strip()[:200]
                    if not target or target == user_id:
                        continue
                    async with p.acquire() as con:
                        await con.execute(
                            """INSERT INTO contacts (owner,contact,ts,first_name,last_name,note)
                               VALUES ($1,$2,$3,$4,$5,$6)
                               ON CONFLICT (owner,contact) DO UPDATE SET
                                 first_name=EXCLUDED.first_name,
                                 last_name=EXCLUDED.last_name,
                                 note=EXCLUDED.note""",
                            user_id, target, now_utc(), fn, ln, note)
                    await websocket.send_text(json.dumps({
                        "type": "contact_saved", "user": target,
                        "first_name": fn, "last_name": ln, "note": note}))
                    continue

                if cmd == "set_autodelete":
                    target = clean_u(data.get("user"))
                    try:
                        secs = int(data.get("seconds") or 0)
                    except Exception:
                        secs = 0
                    if not target or secs not in TIMER_VALUES:
                        continue
                    a, b = pair(user_id, target)
                    async with p.acquire() as con:
                        await con.execute(
                            """INSERT INTO chat_timer (a,b,seconds) VALUES ($1,$2,$3)
                               ON CONFLICT (a,b) DO UPDATE SET seconds=EXCLUDED.seconds""",
                            a, b, secs)
                    payload = {"type": "autodelete_set", "user": target, "seconds": secs}
                    await websocket.send_text(json.dumps(payload))
                    await send_to(target, {"type": "autodelete_set", "user": user_id, "seconds": secs})
                    continue

                if cmd == "report":
                    target = clean_u(data.get("user"))
                    reason = (data.get("reason") or "")[:200]
                    if not target:
                        continue
                    async with p.acquire() as con:
                        await con.execute(
                            """INSERT INTO reports (from_user,target,reason,ts)
                               VALUES ($1,$2,$3,$4)""",
                            user_id, target, reason, now_utc())
                    await websocket.send_text(json.dumps({"type": "report_sent"}))
                    continue

                if cmd == "send":
                    to = clean_u(data.get("to"))
                    text = data.get("text") or ""
                    reply_to = data.get("reply_to")
                    forwarded_from = data.get("forwarded_from")
                    media_data = data.get("media_data")
                    media_type = data.get("media_type")
                    media_url = data.get("media_url")
                    secret = bool(data.get("secret"))
                    client_id = data.get("client_id")

                    if not to or (not text and not media_data and not media_url):
                        await websocket.send_text(json.dumps({
                            "type": "error", "msg": "пустое сообщение",
                            "client_id": client_id}))
                        continue

                    if is_group_id(to):
                        async with p.acquire() as con:
                            if not await is_chat_member(con, to, user_id):
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "не участник",
                                     "client_id": client_id}))
                                continue
                            c = await con.fetchrow(
                                "SELECT kind, name, channel_type FROM chats WHERE id=$1", to)
                            if not c:
                                continue
                            if c["kind"] == "channel":
                                if not await is_chat_admin(con, to, user_id):
                                    await websocket.send_text(json.dumps(
                                        {"type": "error", "msg": "только админ канала может писать",
                                         "client_id": client_id}))
                                    continue
                            ts = now_utc()
                            if media_data:
                                fname = save_file(UPLOAD_DIR, user_id, media_data,
                                                  max_size=MAX_FILE_FREE)
                                if fname:
                                    media_url = f"/uploads/{fname}"
                                else:
                                    await websocket.send_text(json.dumps({
                                        "type": "error",
                                        "msg": "файл слишком большой",
                                        "client_id": client_id}))
                                    continue
                            secs = await get_group_timer(con, to)
                            expire = ts + timedelta(seconds=secs) if secs else None
                            msg_id = await con.fetchval(
                                """INSERT INTO messages
                                   (from_user,to_user,text,ts,reply_to,forwarded_from,
                                    media_url,media_type,secret,expire_at,chat_id)
                                   VALUES ($1,NULL,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                                   RETURNING id""",
                                user_id, text, ts, reply_to, forwarded_from,
                                media_url, media_type, secret, expire, to)
                            reply_text, reply_from = await build_reply_fields(con, reply_to)
                            members = await con.fetch(
                                "SELECT user_id FROM chat_members WHERE chat_id=$1", to)
                        payload = {
                            "type": "msg",
                            "id": msg_id,
                            "from": user_id,
                            "to": to,
                            "chat_id": to,
                            "text": text,
                            "ts": ts.isoformat(),
                            "reply_to": reply_to,
                            "reply_text": reply_text,
                            "reply_from": reply_from,
                            "forwarded_from": forwarded_from,
                            "media_url": media_url,
                            "media_type": media_type,
                            "secret": secret,
                            "client_id": client_id,
                        }
                        for m in members:
                            if m["user_id"] == user_id:
                                continue
                            await send_to(m["user_id"], payload)
                        await websocket.send_text(json.dumps({
                            "type": "sent",
                            "id": msg_id,
                            "to": to,
                            "chat_id": to,
                            "text": text,
                            "ts": ts.isoformat(),
                            "reply_to": reply_to,
                            "reply_text": reply_text,
                            "reply_from": reply_from,
                            "forwarded_from": forwarded_from,
                            "media_url": media_url,
                            "media_type": media_type,
                            "secret": secret,
                            "client_id": client_id,
                            "delivered": True,
                        }))
                        continue

                    ts = now_utc()
                    if media_data:
                        file_limit = MAX_FILE_FREE
                        fname = save_file(UPLOAD_DIR, user_id, media_data, max_size=file_limit)
                        if fname:
                            media_url = f"/uploads/{fname}"
                        else:
                            await websocket.send_text(json.dumps({
                                "type": "error",
                                "msg": f"Файл слишком большой (лимит {file_limit // (1024*1024)} МБ)",
                                "client_id": client_id}))
                            continue
                    shadow = False
                    async with p.acquire() as con:
                        if await is_blocked(con, user_id, to):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "Вы заблокировали пользователя",
                                 "client_id": client_id}))
                            continue
                        shadow = await is_blocked(con, to, user_id)
                        secs = await get_timer(con, user_id, to)
                        if secret:
                            secs = secs or SECRET_TTL
                        expire = ts + timedelta(seconds=secs) if secs else None
                        if shadow:
                            msg_id = -random.randint(1, 10**9)
                        else:
                            msg_id = await con.fetchval(
                                """INSERT INTO messages
                                   (from_user,to_user,text,ts,reply_to,forwarded_from,
                                    media_url,media_type,secret,expire_at)
                                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                                   RETURNING id""",
                                user_id, to, text, ts, reply_to, forwarded_from,
                                media_url, media_type, secret, expire)
                            await con.execute(
                                """DELETE FROM chat_hidden
                                   WHERE (owner=$1 AND partner=$2) OR (owner=$2 AND partner=$1)""",
                                user_id, to)
                        reply_text, reply_from = await build_reply_fields(con, reply_to)

                    payload = {"type": "msg", "id": msg_id, "from": user_id, "to": to, "text": text,
                               "ts": ts.isoformat(), "reply_to": reply_to,
                               "reply_text": reply_text, "reply_from": reply_from,
                               "forwarded_from": forwarded_from,
                               "media_url": media_url, "media_type": media_type,
                               "secret": secret, "client_id": client_id}
                    delivered = False if shadow else (
                        False if clients.get(to) is websocket else await send_to(to, payload))
                    await websocket.send_text(json.dumps({
                        "type": "sent", "id": msg_id, "to": to, "text": text,
                        "ts": ts.isoformat(), "reply_to": reply_to,
                        "reply_text": reply_text, "reply_from": reply_from,
                        "forwarded_from": forwarded_from,
                        "media_url": media_url, "media_type": media_type,
                        "secret": secret, "client_id": client_id, "delivered": delivered}))
                    continue

                if cmd == "mark_read":
                    w = clean_u(data.get("with"))
                    if not w:
                        continue
                    if is_group_id(w):
                        async with p.acquire() as con:
                            await con.execute(
                                """UPDATE messages SET read=TRUE
                                   WHERE chat_id=$1 AND from_user<>$2 AND read=FALSE""",
                                w, user_id)
                        continue
                    async with p.acquire() as con:
                        ids = [r["id"] for r in await con.fetch(
                            """SELECT id FROM messages
                               WHERE from_user=$1 AND to_user=$2 AND read=FALSE""", w, user_id)]
                        if ids:
                            await con.execute(
                                """UPDATE messages SET read=TRUE
                                   WHERE from_user=$1 AND to_user=$2 AND read=FALSE""", w, user_id)
                    if ids:
                        await send_to(w, {"type": "msg_read", "ids": ids, "by": user_id})
                    continue

                if cmd == "edit_msg":
                    try:
                        mid = int(data.get("msg_id"))
                    except Exception:
                        continue
                    new_text = (data.get("text") or "").strip()
                    if not new_text:
                        continue
                    async with p.acquire() as con:
                        row = await con.fetchrow(
                            "SELECT from_user,to_user FROM messages WHERE id=$1", mid)
                        if not row or row["from_user"] != user_id:
                            continue
                        await con.execute(
                            """UPDATE messages SET text=$1, edited=TRUE, edited_at=$2 WHERE id=$3""",
                            new_text, now_utc(), mid)
                    other = row["to_user"] if row["from_user"] == user_id else row["from_user"]
                    await send_to(other, {"type": "msg_edited", "msg_id": mid, "text": new_text})
                    await websocket.send_text(json.dumps(
                        {"type": "msg_edited", "msg_id": mid, "text": new_text}))
                    continue

                if cmd == "delete_msg":
                    try:
                        mid = int(data.get("msg_id"))
                    except Exception:
                        continue
                    for_all = bool(data.get("for_all", True))
                    async with p.acquire() as con:
                        row = await con.fetchrow(
                            "SELECT from_user,to_user FROM messages WHERE id=$1", mid)
                        if not row or user_id not in (row["from_user"], row["to_user"]):
                            continue
                        if for_all:
                            if row["from_user"] != user_id:
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "нельзя удалить у всех"}))
                                continue
                            await con.execute(
                                "UPDATE messages SET deleted=TRUE, text='' WHERE id=$1", mid)
                        else:
                            await con.execute(
                                """INSERT INTO hidden_messages (user_id,msg_id)
                                   VALUES ($1,$2) ON CONFLICT DO NOTHING""",
                                user_id, mid)
                    other = row["to_user"] if row["from_user"] == user_id else row["from_user"]
                    if for_all:
                        await send_to(other, {"type": "msg_deleted", "msg_id": mid})
                    await websocket.send_text(json.dumps({"type": "msg_deleted", "msg_id": mid}))
                    continue

                if cmd == "react":
                    try:
                        mid = int(data.get("msg_id"))
                    except Exception:
                        continue
                    emoji = data.get("emoji")
                    chat_with = clean_u(data.get("chat_with"))
                    if not emoji or not chat_with:
                        continue
                    if not isinstance(emoji, str) or len(emoji) > 8:
                        continue
                    if any(c in emoji for c in '<>"\'&'):
                        continue
                    async with p.acquire() as con:
                        row = await con.fetchrow(
                            "SELECT from_user,to_user FROM messages WHERE id=$1", mid)
                        if not row or user_id not in (row["from_user"], row["to_user"]):
                            continue
                        existing = await con.fetchval(
                            """SELECT id FROM reactions
                               WHERE msg_id=$1 AND user_id=$2 AND emoji=$3""", mid, user_id, emoji)
                        if existing:
                            await con.execute("DELETE FROM reactions WHERE id=$1", existing)
                        else:
                            await con.execute(
                                """INSERT INTO reactions (msg_id,chat_id,user_id,emoji,ts)
                                   VALUES ($1,$2,$3,$4,$5)""",
                                mid, chat_with, user_id, emoji, now_utc())
                        rows = await con.fetch(
                            """SELECT emoji, COUNT(*) AS c FROM reactions
                               WHERE msg_id=$1 GROUP BY emoji""", mid)
                    payload = {"type": "reactions_updated", "msg_id": mid,
                               "reactions": [{"emoji": r["emoji"], "count": r["c"]} for r in rows]}
                    await send_to(chat_with, payload)
                    await websocket.send_text(json.dumps(payload))
                    continue

                if cmd == "pin_msg":
                    try:
                        mid = int(data.get("msg_id"))
                    except Exception:
                        continue
                    chat_with = clean_u(data.get("chat_with"))
                    if not chat_with:
                        continue
                    async with p.acquire() as con:
                        m = await con.fetchrow(
                            "SELECT from_user,to_user FROM messages WHERE id=$1", mid)
                        if not m or user_id not in (m["from_user"], m["to_user"]):
                            continue
                        await con.execute(
                            """INSERT INTO pinned (chat_id, msg_id, pinned_by, pinned_at)
                               VALUES ($1,$2,$3,$4)
                               ON CONFLICT (chat_id) DO UPDATE SET
                                 msg_id=EXCLUDED.msg_id,
                                 pinned_by=EXCLUDED.pinned_by,
                                 pinned_at=EXCLUDED.pinned_at""",
                            chat_with, mid, user_id, now_utc())
                        row = await con.fetchrow(
                            "SELECT from_user,text FROM messages WHERE id=$1", mid)
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
                        row = await con.fetchrow("SELECT msg_id FROM pinned WHERE chat_id=$1", chat_with)
                        if row:
                            m = await con.fetchrow(
                                "SELECT from_user,to_user,text FROM messages WHERE id=$1", row["msg_id"])
                            if m and user_id in (m["from_user"], m["to_user"]):
                                await websocket.send_text(json.dumps({
                                    "type": "msg_pinned", "msg_id": row["msg_id"],
                                    "from": m["from_user"], "text": m["text"]}))
                    continue

                if cmd == "history":
                    w = clean_u(data.get("with"))
                    try:
                        limit = min(max(int(data.get("limit") or 100), 1), 500)
                    except Exception:
                        limit = 100
                    try:
                        before_id = int(data.get("before_id") or 0)
                    except Exception:
                        before_id = 0
                    secret = bool(data.get("secret"))
                    if is_group_id(w):
                        async with p.acquire() as con:
                            if not await is_chat_member(con, w, user_id):
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "не участник"}))
                                continue
                            c = await con.fetchrow(
                                "SELECT kind FROM chats WHERE id=$1", w)
                            is_chan = (c and c["kind"] == "channel")
                            rows = await con.fetch(
                                """SELECT id,from_user,to_user,text,ts,reply_to,
                                          forwarded_from,media_url,media_type,edited,read,secret
                                   FROM messages
                                   WHERE chat_id=$1 AND deleted=FALSE
                                     AND ($3::int = 0 OR id < $3::int)
                                   ORDER BY id DESC LIMIT $2""",
                                w, limit, before_id)
                            msgs = []
                            for r in reversed(rows):
                                reacts = await con.fetch(
                                    """SELECT emoji, COUNT(*) AS c FROM reactions
                                       WHERE msg_id=$1 GROUP BY emoji""", r["id"])
                                msgs.append({
                                    "id": r["id"],
                                    "from": r["from_user"],
                                    "to": r["to_user"],
                                    "text": r["text"],
                                    "ts": r["ts"].isoformat() if r["ts"] else None,
                                    "reply_to": r["reply_to"],
                                    "forwarded_from": r["forwarded_from"],
                                    "media_url": r["media_url"],
                                    "media_type": r["media_type"],
                                    "edited": r["edited"],
                                    "read": r["read"],
                                    "secret": bool(r["secret"]),
                                    "reactions": [{"emoji": x["emoji"], "count": x["c"]} for x in reacts],
                                })
                        await websocket.send_text(json.dumps({
                            "type": "history", "with": w,
                            "before_id": before_id, "msgs": msgs}))
                        continue
                    async with p.acquire() as con:
                        hidden_ts = await con.fetchval(
                            """SELECT ts FROM chat_hidden WHERE owner=$1 AND partner=$2""",
                            user_id, w)
                        rows = await con.fetch(
                            """SELECT id,from_user,to_user,text,ts,reply_to,
                                      forwarded_from,media_url,media_type,edited,read,secret
                               FROM messages
                               WHERE ((from_user=$1 AND to_user=$2)
                                   OR (from_user=$2 AND to_user=$1))
                                 AND deleted=FALSE
                                 AND COALESCE(secret, FALSE)=$4
                                 AND (expire_at IS NULL OR expire_at > $5)
                                 AND ($6::timestamp IS NULL OR ts > $6::timestamp)
                                 AND ($7::int = 0 OR id < $7::int)
                                 AND id NOT IN (
                                     SELECT msg_id FROM hidden_messages WHERE user_id=$1
                                 )
                               ORDER BY id DESC LIMIT $3""",
                            user_id, w, limit, secret, now_utc(), hidden_ts, before_id)

                        reply_ids = list({r["reply_to"] for r in rows if r["reply_to"]})
                        parents = {}
                        if reply_ids:
                            parent_rows = await con.fetch(
                                "SELECT id, text, from_user FROM messages WHERE id = ANY($1::int[])",
                                reply_ids)
                            for pr in parent_rows:
                                parents[pr["id"]] = pr

                        msgs = []
                        for r in reversed(rows):
                            reacts = await con.fetch(
                                """SELECT emoji, COUNT(*) AS c FROM reactions
                                   WHERE msg_id=$1 GROUP BY emoji""", r["id"])
                            rid = r["reply_to"]
                            parent = parents.get(rid) if rid else None
                            reply_text = (parent["text"] or "")[:200] if parent else None
                            reply_from = parent["from_user"] if parent else None
                            msgs.append({
                                "id": r["id"], "from": r["from_user"], "to": r["to_user"],
                                "text": r["text"], "ts": r["ts"].isoformat() if r["ts"] else None,
                                "reply_to": r["reply_to"],
                                "reply_text": reply_text,
                                "reply_from": reply_from,
                                "forwarded_from": r["forwarded_from"],
                                "media_url": r["media_url"], "media_type": r["media_type"],
                                "edited": r["edited"], "read": r["read"],
                                "secret": bool(r["secret"]),
                                "reactions": [{"emoji": x["emoji"], "count": x["c"]} for x in reacts]})
                    await websocket.send_text(json.dumps({
                        "type": "history", "with": w, "secret": secret,
                        "before_id": before_id, "msgs": msgs}))
                    continue

                if cmd == "forward":
                    to = clean_u(data.get("to"))
                    try:
                        mid = int(data.get("msg_id"))
                    except Exception:
                        mid = 0
                    if not to or to == user_id or mid <= 0:
                        continue
                    ts = now_utc()
                    async with p.acquire() as con:
                        src = await con.fetchrow(
                            """SELECT from_user,to_user,text,media_url,media_type FROM messages
                               WHERE id=$1 AND deleted=FALSE AND COALESCE(secret,FALSE)=FALSE
                                 AND (expire_at IS NULL OR expire_at>$2)""", mid, ts)
                        if (not src or user_id not in (src["from_user"], src["to_user"])
                                or not await con.fetchval("SELECT 1 FROM users WHERE user_id=$1", to)):
                            await websocket.send_text(json.dumps({"type": "error", "msg": "нельзя переслать"}))
                            continue
                        if await is_blocked(con, user_id, to):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "Вы заблокировали пользователя"}))
                            continue
                        shadow = await is_blocked(con, to, user_id)
                        secs = await get_timer(con, user_id, to)
                        expire = ts + timedelta(seconds=secs) if secs else None
                        if shadow:
                            new_id = -random.randint(1, 10**9)
                        else:
                            new_id = await con.fetchval(
                                """INSERT INTO messages
                                   (from_user,to_user,text,ts,forwarded_from,
                                    media_url,media_type,expire_at)
                                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id""",
                                user_id, to, src["text"], ts, src["from_user"],
                                src["media_url"], src["media_type"], expire)
                            await con.execute(
                                """DELETE FROM chat_hidden
                                   WHERE (owner=$1 AND partner=$2) OR (owner=$2 AND partner=$1)""",
                                user_id, to)
                    payload = {"type": "msg", "id": new_id, "from": user_id, "to": to,
                               "text": src["text"], "ts": ts.isoformat(),
                               "forwarded_from": src["from_user"],
                               "media_url": src["media_url"], "media_type": src["media_type"]}
                    delivered = False if shadow else await send_to(to, payload)
                    await websocket.send_text(json.dumps({
                        "type": "sent", "id": new_id, "to": to, "text": src["text"],
                        "ts": ts.isoformat(), "forwarded_from": src["from_user"],
                        "media_url": src["media_url"], "media_type": src["media_type"],
                        "delivered": delivered}))
                    continue

                if cmd == "profile":
                    target = clean_u(data.get("user")) or user_id
                    async with p.acquire() as con:
                        row = await con.fetchrow(
                            """SELECT username,first_name,bio,avatar,created_at,last_seen,
                                      phone_country,phone_country_flag,
                                      name_updated_at,photo_updated_at,
                                      prof_c,prof_pat,prof_emo,prof_nc,
                                      premium,premium_until
                               FROM users WHERE user_id=$1""", target)
                        if not row:
                            row = await con.fetchrow(
                                """SELECT username,first_name,bio,avatar,created_at,last_seen,
                                          phone_country,phone_country_flag,
                                          name_updated_at,photo_updated_at,
                                          prof_c,prof_pat,prof_emo,prof_nc,
                                          premium,premium_until
                                   FROM users WHERE username=$1""", target)
                        if not row:
                            await websocket.send_text(json.dumps({"type": "error", "msg": "юзер не найден"}))
                            continue
                        real_id = row["username"]
                        gifts = await con.fetch(
                            """SELECT g.gift_name, g.gift_emoji, g.stars, g.from_user, g.ts,
                                      g.gift_file,
                                      (SELECT avatar FROM users WHERE username=g.from_user) AS from_av
                               FROM gifts g WHERE g.to_user=$1 ORDER BY g.ts DESC LIMIT 50""", real_id)
                        unames = await con.fetch(
                            """SELECT username,is_primary FROM usernames
                               WHERE user_id=$1 ORDER BY is_primary DESC, id""", real_id)
                        i_blocked_him = await is_blocked(con, user_id, real_id)
                        he_blocked_me = await is_blocked(con, real_id, user_id)
                        hidden_from_me = real_id != user_id and (i_blocked_him or he_blocked_me)
                        timer = await get_timer(con, user_id, real_id) if real_id != user_id else 0
                    if hidden_from_me:
                        online, last_seen = False, "long"
                    else:
                        online = real_id in clients
                        last_seen = row["last_seen"].isoformat() if row["last_seen"] else None
                    await websocket.send_text(json.dumps({
                        "type": "profile",
                        "user": real_id,
                        "first_name": row["first_name"] or real_id,
                        "bio": row["bio"] or "",
                        "avatar": row["avatar"] or "",
                        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                        "registered_at": row["created_at"].isoformat() if row["created_at"] else None,
                        "last_seen": last_seen,
                        "online": online,
                        "phone_country": row["phone_country"],
                        "phone_country_flag": row["phone_country_flag"],
                        "updated_name_at": row["name_updated_at"].isoformat() if row["name_updated_at"] else None,
                        "updated_photo_at": row["photo_updated_at"].isoformat() if row["photo_updated_at"] else None,
                        "prof_c": row["prof_c"] if row["prof_c"] is not None else -1,
                        "prof_pat": row["prof_pat"] or "",
                        "prof_emo": row["prof_emo"] or "",
                        "prof_nc": row["prof_nc"] if row["prof_nc"] is not None else -1,
                        "autodelete": timer,
                        "i_blocked": bool(i_blocked_him),
                        "blocked_me": bool(he_blocked_me),
                        **premium_fields(row),
                        "gifts": [{
                            "name": g["gift_name"],
                            "emoji": g["gift_emoji"],
                            "stars": g["stars"],
                            "from": g["from_user"],
                            "from_av": g["from_av"] or "",
                            "ts": g["ts"].isoformat() if g["ts"] else None,
                            "lottie": g["gift_file"] or "",
                        } for g in gifts],
                        "usernames": [{"u": x["username"], "primary": x["is_primary"]} for x in unames]}))
                    continue

                if cmd == "update_profile":
                    fn = (data.get("first_name") or "").strip().lstrip("@").strip() or user_id
                    bio = (data.get("bio") or "").strip()
                    av_data = data.get("avatar_data")
                    av = data.get("avatar")
                    avatar_clear = bool(data.get("avatar_clear")) or (av_data == "")

                    has_prof = any(k in data for k in ("prof_c", "prof_pat", "prof_emo", "prof_nc"))
                    try:
                        prof_c = int(data.get("prof_c", -1))
                    except Exception:
                        prof_c = -1
                    prof_pat = (data.get("prof_pat") or "")[:16]
                    prof_emo = (data.get("prof_emo") or "")[:16]
                    try:
                        prof_nc = int(data.get("prof_nc", -1))
                    except Exception:
                        prof_nc = -1

                    new_av = None
                    if av_data:
                        new_av = save_file(AVATAR_DIR, user_id, av_data, max_size=5 * 1024 * 1024)
                        if new_av:
                            av = new_av
                    async with p.acquire() as con:
                        if avatar_clear:
                            await con.execute(
                                """UPDATE users SET first_name=$1, bio=$2, avatar=NULL,
                                                    name_updated_at=$3, photo_updated_at=$4
                                   WHERE user_id=$5""",
                                fn, bio, now_utc(), now_utc(), user_id)
                        elif av is not None:
                            await con.execute(
                                """UPDATE users SET first_name=$1, bio=$2, avatar=$3,
                                                    name_updated_at=$4, photo_updated_at=$5
                                   WHERE user_id=$6""",
                                fn, bio, av, now_utc(), now_utc(), user_id)
                        else:
                            await con.execute(
                                """UPDATE users SET first_name=$1, bio=$2, name_updated_at=$3
                                   WHERE user_id=$4""",
                                fn, bio, now_utc(), user_id)
                        if has_prof:
                            await con.execute(
                                """UPDATE users SET prof_c=$1, prof_pat=$2, prof_emo=$3, prof_nc=$4
                                   WHERE user_id=$5""",
                                prof_c, prof_pat, prof_emo, prof_nc, user_id)
                    await websocket.send_text(json.dumps({"type": "profile_updated"}))
                    continue

                if cmd == "get_avatar":
                    fname = data.get("file") or ""
                    path = os.path.join(AVATAR_DIR, os.path.basename(fname))
                    if fname and os.path.exists(path):
                        try:
                            with open(path, "rb") as f:
                                raw = f.read()
                            b64 = base64.b64encode(raw).decode()
                            ext = fname.rsplit(".", 1)[-1]
                            mime = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
                            await websocket.send_text(json.dumps({
                                "type": "avatar_data", "file": fname,
                                "data": f"data:{mime};base64,{b64}"}))
                        except Exception as e:
                            logger.error(f"[avatar] {e}")
                    else:
                        await websocket.send_text(json.dumps(
                            {"type": "avatar_data", "file": fname, "data": ""}))
                    continue

                if cmd == "publish":
                    media_data = data.get("media_data") or data.get("media_url")
                    media_type = data.get("media_type") or "image"
                    if not media_data:
                        continue
                    media_url = media_data
                    if media_data.startswith("data:"):
                        fname = save_file(UPLOAD_DIR, f"pub_{user_id}", media_data,
                                          max_size=MAX_FILE_FREE)
                        if fname:
                            media_url = f"/uploads/{fname}"
                    pid = f"p{uuid.uuid4().hex[:12]}"
                    ts = now_utc()
                    async with p.acquire() as con:
                        await con.execute(
                            """INSERT INTO publications (id,user_id,media_url,media_type,ts)
                               VALUES ($1,$2,$3,$4,$5)""",
                            pid, user_id, media_url, media_type, ts)
                    await websocket.send_text(json.dumps({
                        "type": "publication_added", "id": pid, "user": user_id,
                        "media_url": media_url, "media_type": media_type,
                        "ts": ts.isoformat()}))
                    continue

                if cmd == "delete_publication":
                    pid = data.get("id")
                    if not pid:
                        continue
                    async with p.acquire() as con:
                        await con.execute(
                            "DELETE FROM publications WHERE id=$1 AND user_id=$2", pid, user_id)
                    await websocket.send_text(json.dumps({"type": "publication_deleted", "id": pid}))
                    continue

                if cmd == "get_publications":
                    target = clean_u(data.get("user")) or user_id
                    async with p.acquire() as con:
                        real_id = await con.fetchval(
                            "SELECT user_id FROM users WHERE user_id=$1 OR username=$1", target)
                        if not real_id:
                            real_id = target
                        rows = await con.fetch(
                            """SELECT id,media_url,media_type,ts FROM publications
                               WHERE user_id=$1 ORDER BY ts DESC LIMIT 60""", real_id)
                    await websocket.send_text(json.dumps({
                        "type": "publications", "user": real_id,
                        "list": [{"id": r["id"], "media_url": r["media_url"],
                                  "media_type": r["media_type"],
                                  "ts": r["ts"].isoformat() if r["ts"] else None} for r in rows]}))
                    continue

                if cmd == "add_username":
                    new_u = clean_u(data.get("username"))
                    if not new_u or len(new_u) < 4 or len(new_u) > 32:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "4-32"}))
                        continue
                    if not new_u.replace("_", "").isalnum():
                        await websocket.send_text(json.dumps({"type": "error", "msg": "a-z 0-9 _"}))
                        continue
                    if new_u == user_id:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "это уже ваш username"}))
                        continue
                    try:
                        async with p.acquire() as con:
                            exists1 = await con.fetchval(
                                "SELECT user_id FROM usernames WHERE username=$1", new_u)
                            if exists1 and exists1 != user_id:
                                await websocket.send_text(json.dumps({"type": "error", "msg": "занят"}))
                                continue
                            exists2 = await con.fetchval(
                                "SELECT user_id FROM users WHERE username=$1", new_u)
                            if exists2 and exists2 != user_id:
                                await websocket.send_text(json.dumps({"type": "error", "msg": "занят"}))
                                continue

                            old_id = user_id
                            async with con.transaction():
                                await rename_user_everywhere(con, old_id, new_u)
                                await con.execute(
                                    "DELETE FROM usernames WHERE user_id=$1 AND username != $2",
                                    new_u, new_u)
                                await con.execute(
                                    """INSERT INTO usernames (user_id, username, is_primary, created_at)
                                       VALUES ($1, $2, TRUE, $3)
                                       ON CONFLICT (username) DO UPDATE SET
                                           user_id=EXCLUDED.user_id,
                                           is_primary=TRUE,
                                           created_at=EXCLUDED.created_at""",
                                    new_u, new_u, now_utc())
                                await con.execute(
                                    "UPDATE users SET name_updated_at=$1 WHERE user_id=$2",
                                    now_utc(), new_u)

                        user_id = new_u
                        if old_id in clients:
                            clients[new_u] = clients.pop(old_id)

                        await websocket.send_text(json.dumps(
                            {"type": "username_added", "username": new_u}))

                        async with p.acquire() as con:
                            row = await con.fetchrow(
                                """SELECT username, first_name, avatar, bio,
                                          prof_c, prof_pat, prof_emo, prof_nc,
                                          premium, premium_until
                                   FROM users WHERE user_id=$1""", new_u)
                            unames = await con.fetch(
                                """SELECT username, is_primary FROM usernames
                                   WHERE user_id=$1 ORDER BY is_primary DESC""", new_u)
                        await websocket.send_text(json.dumps({
                            "type": "profile", "user": new_u,
                            "first_name": (row["first_name"] if row else new_u) or new_u,
                            "avatar": (row["avatar"] if row else "") or "",
                            "bio": (row["bio"] if row else "") or "",
                            "online": True,
                            "last_seen": now_utc().isoformat(),
                            "prof_c": row["prof_c"] if row and row["prof_c"] is not None else -1,
                            "prof_pat": (row["prof_pat"] if row else "") or "",
                            "prof_emo": (row["prof_emo"] if row else "") or "",
                            "prof_nc": row["prof_nc"] if row and row["prof_nc"] is not None else -1,
                            **premium_fields(row),
                            "usernames": [{"u": x["username"], "primary": x["is_primary"]} for x in unames],
                        }))
                    except asyncpg.UniqueViolationError:
                        await websocket.send_text(json.dumps({"type": "error", "msg": "занят"}))
                    except Exception as e:
                        logger.error(f"[add_username] {e}")
                        import traceback; traceback.print_exc()
                        await websocket.send_text(json.dumps({"type": "error", "msg": str(e)}))
                    continue

                if cmd == "check_username":
                    name = clean_u(data.get("username"))
                    if not name or len(name) < 4:
                        await websocket.send_text(json.dumps(
                            {"type": "username_check", "available": False}))
                        continue
                    async with p.acquire() as con:
                        exists = await con.fetchval(
                            "SELECT 1 FROM usernames WHERE username=$1 AND user_id!=$2",
                            name, user_id)
                        if not exists:
                            exists = await con.fetchval(
                                "SELECT 1 FROM users WHERE username=$1 AND user_id!=$2",
                                name, user_id)
                    await websocket.send_text(json.dumps({
                        "type": "username_check", "available": not exists, "username": name}))
                    continue

                if cmd == "delete_username":
                    name = clean_u(data.get("username"))
                    async with p.acquire() as con:
                        row = await con.fetchrow(
                            """SELECT is_primary FROM usernames
                               WHERE user_id=$1 AND username=$2""", user_id, name)
                        if not row:
                            continue
                        if row["is_primary"]:
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "нельзя основной"}))
                            continue
                        await con.execute(
                            "DELETE FROM usernames WHERE user_id=$1 AND username=$2", user_id, name)
                    await websocket.send_text(json.dumps(
                        {"type": "username_deleted", "username": name}))
                    continue

                if cmd == "set_primary_username":
                    name = clean_u(data.get("username"))
                    async with p.acquire() as con:
                        if not await con.fetchval(
                                """SELECT 1 FROM usernames
                                   WHERE user_id=$1 AND username=$2""", user_id, name):
                            continue
                        async with con.transaction():
                            await con.execute(
                                "UPDATE usernames SET is_primary=FALSE WHERE user_id=$1", user_id)
                            await con.execute(
                                """UPDATE usernames SET is_primary=TRUE
                                   WHERE user_id=$1 AND username=$2""", user_id, name)
                            await con.execute(
                                "UPDATE users SET username=$1 WHERE user_id=$2", name, user_id)
                    await websocket.send_text(json.dumps(
                        {"type": "primary_updated", "username": name}))
                    continue

                if cmd == "list_chats":
                    async with p.acquire() as con:
                        rows = await con.fetch(
                            """SELECT
                                 CASE WHEN from_user=$1 THEN to_user ELSE from_user END AS peer,
                                 text, ts,
                                 (read=FALSE AND to_user=$1) AS unread,
                                 (from_user=$1) AS mine
                               FROM messages
                               WHERE (from_user=$1 OR to_user=$1)
                                 AND (chat_id IS NULL)
                                 AND deleted=FALSE
                                 AND (expire_at IS NULL OR expire_at > $2)
                               ORDER BY ts DESC""", user_id, now_utc())
                        seen = {}
                        for r in rows:
                            peer = r["peer"]
                            if peer in seen:
                                if r["unread"]:
                                    seen[peer]["unread"] += 1
                                continue
                            seen[peer] = {
                                "user": peer,
                                "text": r["text"] or "",
                                "ts": r["ts"].isoformat() if r["ts"] else None,
                                "unread": 1 if r["unread"] else 0,
                                "mine": bool(r["mine"]),
                                "kind": "private",
                            }
                        # Группы и каналы
                        g_rows = await con.fetch(
                            """SELECT c.id FROM chats c
                               JOIN chat_members cm ON cm.chat_id=c.id
                               WHERE cm.user_id=$1""", user_id)
                        for gr in g_rows:
                            summary = await chat_summary(con, gr["id"], user_id)
                            if summary:
                                seen[gr["id"]] = summary
                    await websocket.send_text(json.dumps({
                        "type": "chats", "list": list(seen.values())}))
                    continue

                # ===================== GROUPS & CHANNELS =====================

                if cmd == "create_chat":
                    kind = data.get("kind") or "group"
                    if kind not in ("group", "channel"):
                        await websocket.send_text(json.dumps(
                            {"type": "error", "msg": "неверный тип"}))
                        continue
                    name = (data.get("name") or "").strip()[:64]
                    if not name:
                        await websocket.send_text(json.dumps(
                            {"type": "error", "msg": "нет названия"}))
                        continue
                    members = [clean_u(x) for x in (data.get("members") or []) if x]
                    members = [x for x in members if x and x != user_id]
                    desc = (data.get("description") or "").strip()[:200]
                    av_data = data.get("avatar_data")
                    autodel = int(data.get("autodel") or data.get("autodelete") or 0)
                    if autodel not in TIMER_VALUES:
                        autodel = 0
                    chat_id = new_chat_id(kind)
                    av = None
                    if av_data:
                        av = save_file(AVATAR_DIR, chat_id, av_data, max_size=5 * 1024 * 1024)
                    ts = now_utc()
                    async with p.acquire() as con:
                        try:
                            async with con.transaction():
                                await con.execute(
                                    """INSERT INTO chats
                                       (id, kind, name, description, avatar, owner,
                                        created_at, channel_type, channel_link, autodelete)
                                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                                    chat_id, kind, name, desc, av, user_id, ts,
                                    "public", "", autodel)
                                await con.execute(
                                    """INSERT INTO chat_members
                                       (chat_id, user_id, is_admin, joined_at)
                                       VALUES ($1,$2,TRUE,$3)""",
                                    chat_id, user_id, ts)
                                for m in members:
                                    exists = await con.fetchval(
                                        "SELECT 1 FROM users WHERE user_id=$1", m)
                                    if exists:
                                        await con.execute(
                                            """INSERT INTO chat_members
                                               (chat_id, user_id, is_admin, joined_at)
                                               VALUES ($1,$2,FALSE,$3)
                                               ON CONFLICT DO NOTHING""",
                                            chat_id, m, ts)
                        except Exception as e:
                            logger.error(f"[create_chat] {e}")
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "не удалось создать"}))
                            continue
                        summary = await chat_summary(con, chat_id, user_id)
                        member_list = await chat_members_list(con, chat_id)

                    payload = {
                        "type": "chat_created",
                        "chat": chat_id,
                        "kind": kind,
                        "name": name,
                        "avatar": av or "",
                        "description": desc,
                        "members": [m["u"] for m in member_list],
                        "admins": [user_id],
                    }
                    await websocket.send_text(json.dumps(payload))
                    for m in member_list:
                        if m["u"] != user_id:
                            await send_to(m["u"], {
                                "type": "chat_created",
                                "chat": chat_id,
                                "kind": kind,
                                "name": name,
                                "avatar": av or "",
                                "description": desc,
                                "members": [x["u"] for x in member_list],
                                "admins": [user_id],
                            })
                    continue

                if cmd == "chat_info":
                    chat_id = (data.get("chat") or "").strip()
                    if not is_group_id(chat_id):
                        await websocket.send_text(json.dumps(
                            {"type": "error", "msg": "неверный chat"}))
                        continue
                    async with p.acquire() as con:
                        if not await is_chat_member(con, chat_id, user_id):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "не участник"}))
                            continue
                        c = await con.fetchrow(
                            """SELECT id, kind, name, description, avatar, owner,
                                      channel_type, channel_link, autodelete
                               FROM chats WHERE id=$1""", chat_id)
                        if not c:
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "чат не найден"}))
                            continue
                        members = await chat_members_list(con, chat_id)
                        admins = [m["u"] for m in members if m["admin"]]
                        cnt = len(members)
                    await websocket.send_text(json.dumps({
                        "type": "chat_info",
                        "chat": chat_id,
                        "kind": c["kind"],
                        "name": c["name"],
                        "description": c["description"] or "",
                        "avatar": c["avatar"] or "",
                        "owner": c["owner"],
                        "type": c["channel_type"] or "public",
                        "link": c["channel_link"] or "",
                        "autodelete": c["autodelete"] or 0,
                        "members": members,
                        "members_count": cnt,
                        "admins": admins,
                    }))
                    continue

                if cmd == "add_members":
                    chat_id = (data.get("chat") or "").strip()
                    users_to_add = [clean_u(x) for x in (data.get("users") or [])]
                    users_to_add = [x for x in users_to_add if x]
                    if not is_group_id(chat_id) or not users_to_add:
                        await websocket.send_text(json.dumps(
                            {"type": "error", "msg": "неверные данные"}))
                        continue
                    async with p.acquire() as con:
                        if not await is_chat_member(con, chat_id, user_id):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "не участник"}))
                            continue
                        c = await con.fetchrow(
                            "SELECT kind, owner FROM chats WHERE id=$1", chat_id)
                        if not c:
                            continue
                        if c["kind"] == "group":
                            if not await is_chat_admin(con, chat_id, user_id):
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "только админ"}))
                                continue
                        added = []
                        ts = now_utc()
                        for m in users_to_add:
                            exists = await con.fetchval(
                                "SELECT 1 FROM users WHERE user_id=$1", m)
                            if not exists:
                                continue
                            already = await con.fetchval(
                                "SELECT 1 FROM chat_members WHERE chat_id=$1 AND user_id=$2",
                                chat_id, m)
                            if already:
                                continue
                            await con.execute(
                                """INSERT INTO chat_members
                                   (chat_id, user_id, is_admin, joined_at)
                                   VALUES ($1,$2,FALSE,$3)""",
                                chat_id, m, ts)
                            added.append(m)
                        members = await chat_members_list(con, chat_id)
                        admins = [x["u"] for x in members if x["admin"]]
                    payload_new = {
                        "type": "chat_created",
                        "chat": chat_id,
                        "kind": c["kind"],
                        "name": None,
                        "members": [x["u"] for x in members],
                        "admins": admins,
                    }
                    for m in added:
                        await send_to(m, {
                            "type": "chat_created",
                            "chat": chat_id,
                            "kind": c["kind"],
                            "name": None,
                            "members": [x["u"] for x in members],
                            "admins": admins,
                        })
                    for m in members:
                        await send_to(m["u"], {
                            "type": "members_updated",
                            "chat": chat_id,
                            "added": True,
                            "members": [{"u": x["u"], "admin": x["admin"]} for x in members],
                            "admins": admins,
                        })
                    continue

                if cmd == "remove_member":
                    chat_id = (data.get("chat") or "").strip()
                    target = clean_u(data.get("user"))
                    if not is_group_id(chat_id) or not target:
                        continue
                    async with p.acquire() as con:
                        if not await is_chat_admin(con, chat_id, user_id):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "только админ"}))
                            continue
                        await con.execute(
                            "DELETE FROM chat_members WHERE chat_id=$1 AND user_id=$2",
                            chat_id, target)
                        members = await chat_members_list(con, chat_id)
                        admins = [x["u"] for x in members if x["admin"]]
                    await send_to(target, {"type": "chat_deleted", "chat": chat_id})
                    for m in members:
                        await send_to(m["u"], {
                            "type": "members_updated",
                            "chat": chat_id,
                            "added": False,
                            "members": [{"u": x["u"], "admin": x["admin"]} for x in members],
                            "admins": admins,
                        })
                    continue

                if cmd == "leave_chat":
                    chat_id = (data.get("chat") or "").strip()
                    if not is_group_id(chat_id):
                        continue
                    async with p.acquire() as con:
                        await con.execute(
                            "DELETE FROM chat_members WHERE chat_id=$1 AND user_id=$2",
                            chat_id, user_id)
                        c = await con.fetchrow(
                            "SELECT owner FROM chats WHERE id=$1", chat_id)
                        if c and c["owner"] == user_id:
                            new_admin = await con.fetchrow(
                                """SELECT user_id FROM chat_members
                                   WHERE chat_id=$1 ORDER BY joined_at LIMIT 1""", chat_id)
                            if new_admin:
                                await con.execute(
                                    "UPDATE chats SET owner=$1 WHERE id=$2",
                                    new_admin["user_id"], chat_id)
                                await con.execute(
                                    """UPDATE chat_members SET is_admin=TRUE
                                       WHERE chat_id=$1 AND user_id=$2""",
                                    chat_id, new_admin["user_id"])
                            else:
                                await con.execute("DELETE FROM chats WHERE id=$1", chat_id)
                                await con.execute("DELETE FROM chat_members WHERE chat_id=$1", chat_id)
                        members = await chat_members_list(con, chat_id)
                    await websocket.send_text(json.dumps(
                        {"type": "chat_deleted", "chat": chat_id}))
                    for m in members:
                        await send_to(m["u"], {
                            "type": "members_updated",
                            "chat": chat_id,
                            "added": False,
                        })
                    continue

                if cmd == "delete_chat":
                    chat_id = (data.get("chat") or "").strip()
                    if chat_id and is_group_id(chat_id):
                        async with p.acquire() as con:
                            c = await con.fetchrow(
                                "SELECT owner FROM chats WHERE id=$1", chat_id)
                            if not c:
                                continue
                            if c["owner"] != user_id and not await is_chat_admin(con, chat_id, user_id):
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "только владелец"}))
                                continue
                            members = await chat_members_list(con, chat_id)
                            await con.execute("DELETE FROM chat_members WHERE chat_id=$1", chat_id)
                            await con.execute("DELETE FROM chats WHERE id=$1", chat_id)
                            await con.execute("DELETE FROM messages WHERE chat_id=$1", chat_id)
                        await websocket.send_text(json.dumps(
                            {"type": "chat_deleted", "chat": chat_id}))
                        for m in members:
                            if m["u"] != user_id:
                                await send_to(m["u"], {
                                    "type": "chat_deleted", "chat": chat_id})
                        continue
                    w = clean_u(data.get("with"))
                    async with p.acquire() as con:
                        await con.execute(
                            """INSERT INTO chat_hidden (owner,partner,ts)
                               VALUES ($1,$2,$3)
                               ON CONFLICT (owner,partner) DO UPDATE SET ts=EXCLUDED.ts""",
                            user_id, w, now_utc())
                    await websocket.send_text(json.dumps({"type": "chat_deleted", "with": w}))
                    continue

                if cmd == "edit_chat":
                    chat_id = (data.get("chat") or "").strip()
                    if not is_group_id(chat_id):
                        continue
                    async with p.acquire() as con:
                        if not await is_chat_admin(con, chat_id, user_id):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "только админ"}))
                            continue
                        sets = []
                        params = []
                        if "name" in data and data.get("name"):
                            sets.append(f"name=${len(params)+1}")
                            params.append(str(data["name"])[:64])
                        if "description" in data:
                            sets.append(f"description=${len(params)+1}")
                            params.append(str(data.get("description") or "")[:200])
                        if "autodel" in data or "autodelete" in data:
                            v = int(data.get("autodel") or data.get("autodelete") or 0)
                            if v in TIMER_VALUES:
                                sets.append(f"autodelete=${len(params)+1}")
                                params.append(v)
                        av_data = data.get("avatar_data")
                        if av_data:
                            fname = save_file(AVATAR_DIR, chat_id, av_data,
                                              max_size=5 * 1024 * 1024)
                            if fname:
                                sets.append(f"avatar=${len(params)+1}")
                                params.append(fname)
                        if not sets:
                            continue
                        params.append(chat_id)
                        q = f"UPDATE chats SET {', '.join(sets)} WHERE id=${len(params)}"
                        await con.execute(q, *params)
                        c = await con.fetchrow(
                            """SELECT id,kind,name,description,avatar,
                                      channel_type,channel_link,autodelete
                               FROM chats WHERE id=$1""", chat_id)
                        members = await chat_members_list(con, chat_id)
                    payload = {
                        "type": "chat_updated",
                        "chat": chat_id,
                        "kind": c["kind"],
                        "name": c["name"],
                        "description": c["description"] or "",
                        "avatar": c["avatar"] or "",
                        "autodelete": c["autodelete"] or 0,
                    }
                    for m in members:
                        await send_to(m["u"], payload)
                    continue

                if cmd == "update_channel_settings":
                    chat_id = (data.get("chat") or "").strip()
                    if not is_channel_id(chat_id):
                        continue
                    ctype = (data.get("type") or "public").strip()
                    if ctype not in ("public", "private"):
                        ctype = "public"
                    link = (data.get("link") or "").strip().lower()
                    if ctype == "public":
                        if len(link) < 5 or not _re.match(r"^[a-z0-9_]+$", link):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "ссылка: 5+ символов a-z 0-9 _"}))
                            continue
                    else:
                        link = ""
                    async with p.acquire() as con:
                        if not await is_chat_admin(con, chat_id, user_id):
                            await websocket.send_text(json.dumps(
                                {"type": "error", "msg": "только админ"}))
                            continue
                        if ctype == "public" and link:
                            taken = await con.fetchval(
                                """SELECT id FROM chats
                                   WHERE channel_link=$1 AND id<>$2""",
                                link, chat_id)
                            if taken:
                                await websocket.send_text(json.dumps(
                                    {"type": "error", "msg": "ссылка занята"}))
                                continue
                        await con.execute(
                            """UPDATE chats SET channel_type=$1, channel_link=$2
                               WHERE id=$3""", ctype, link, chat_id)
                        members = await chat_members_list(con, chat_id)
                    payload = {
                        "type": "chat_updated",
                        "chat": chat_id,
                        "type": ctype,
                        "link": link,
                    }
                    for m in members:
                        await send_to(m["u"], payload)
                    continue
                # ===================== /GROUPS & CHANNELS =====================

                continue

            except WebSocketDisconnect:
                raise
            except Exception as cmd_err:
                logger.error(f"[cmd {cmd}] {cmd_err}")
                import traceback as _tb
                _tb.print_exc()
                try:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "msg": "внутренняя ошибка",
                        "client_id": data.get("client_id") if isinstance(data, dict) else None,
                    }))
                except Exception:
                    raise WebSocketDisconnect()
                continue

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"[ws] {e}")
        import traceback
        traceback.print_exc()
    finally:
        gone = [k for k, v in clients.items() if v is websocket]
        for k in gone:
            del clients[k]
        for k in gone:
            if k in active_calls:
                info = active_calls.pop(k)
                peer = info["peer"]
                active_calls.pop(peer, None)
                await send_to(peer, {"type": "call_ended", "by": k, "reason": "disconnect"})
            if k in ringing:
                info = ringing.pop(k)
                caller = info["from"]
                await send_to(caller, {"type": "call_no_answer", "to": k})
            for callee, info in list(ringing.items()):
                if info["from"] == k:
                    ringing.pop(callee, None)
                    await send_to(callee, {"type": "call_cancelled", "by": k})
                    await _save_missed_call(k, callee, info["media"], cancelled=True)
            try:
                async with p.acquire() as con:
                    await con.execute(
                        "UPDATE users SET last_seen=$1 WHERE user_id=$2",
                        now_utc(), k)
            except Exception:
                pass
            await broadcast_presence(k, "user_offline")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
