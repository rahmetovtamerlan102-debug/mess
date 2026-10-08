# app.py — Aitgram backend
# Зависимости: pip install aiohttp aiosqlite
# Запуск: python app.py  (по умолчанию порт 8080)

import asyncio
import json
import os
import random
import secrets
import string
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
import aiosqlite
from aiohttp import web, WSMsgType

# ================== CONFIG ==================
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
DB_PATH = os.environ.get("DB_PATH", "aitgram.db")
STATIC_DIR = Path(__file__).parent

# Resend
RESEND_KEY = os.environ.get("RESEND_KEY", "")
RESEND_FROM = os.environ.get("RESEND_FROM", "Aitgram <onboarding@resend.dev>")

CODE_TTL_MIN = 10

# ================== STATE ==================
online = {}          # user_id -> ws
codes = {}           # email -> (code, expires_ts, tries)
sessions = {}        # token -> user_id

# ================== DB ==================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username TEXT UNIQUE,
          email TEXT UNIQUE,
          password TEXT,
          first_name TEXT,
          last_name TEXT,
          bio TEXT,
          avatar TEXT,
          registered_at TEXT,
          phone_country TEXT,
          phone_country_flag TEXT,
          name_updated_at TEXT,
          photo_updated_at TEXT,
          last_seen TEXT,
          online INTEGER DEFAULT 0,
          primary_username TEXT
        );
        CREATE TABLE IF NOT EXISTS usernames (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER,
          username TEXT UNIQUE,
          is_primary INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS contacts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          owner_id INTEGER,
          contact_id INTEGER,
          first_name TEXT,
          last_name TEXT,
          note TEXT,
          UNIQUE(owner_id, contact_id)
        );
        CREATE TABLE IF NOT EXISTS blocks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          blocker_id INTEGER,
          blocked_id INTEGER,
          UNIQUE(blocker_id, blocked_id)
        );
        CREATE TABLE IF NOT EXISTS messages (
          id TEXT PRIMARY KEY,
          from_id INTEGER,
          to_id INTEGER,
          text TEXT,
          media TEXT,
          media_type TEXT,
          reply_to TEXT,
          forwarded_from TEXT,
          ts TEXT,
          read INTEGER DEFAULT 0,
          deleted INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS reactions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          msg_id TEXT,
          user_id INTEGER,
          emoji TEXT,
          UNIQUE(msg_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS autodelete (
          user_id INTEGER,
          peer_id INTEGER,
          seconds INTEGER,
          UNIQUE(user_id, peer_id)
        );
        CREATE TABLE IF NOT EXISTS pins (
          user_id INTEGER,
          peer_id INTEGER,
          msg_id TEXT
        );
        CREATE TABLE IF NOT EXISTS gifts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          from_id INTEGER,
          to_id INTEGER,
          name TEXT,
          emoji TEXT,
          stars INTEGER,
          ts TEXT
        );
        CREATE TABLE IF NOT EXISTS publications (
          id TEXT PRIMARY KEY,
          user_id INTEGER,
          media TEXT,
          media_type TEXT,
          ts TEXT
        );
        CREATE TABLE IF NOT EXISTS reports (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          from_id INTEGER,
          to_id INTEGER,
          reason TEXT,
          ts TEXT
        );
        """)
        await db.commit()

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

async def db_one(q, *a):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, a) as cur:
            return await cur.fetchone()

async def db_all(q, *a):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, a) as cur:
            return await cur.fetchall()

async def db_exec(q, *a):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(q, a)
        await db.commit()

# ================== UTIL ==================
def gen_code():
    return "".join(random.choices(string.digits, k=6))

def gen_token():
    return secrets.token_urlsafe(32)

def clean_u(s):
    return (s or "").lstrip("@").strip().lower()

async def send_to_user(uid, payload):
    ws = online.get(uid)
    if ws and not ws.closed:
        try:
            await ws.send_json(payload)
            return True
        except Exception:
            return False
    return False

async def broadcast_online(uid, online_flag):
    uname = await username_of(uid)
    payload = {"type": "user_online" if online_flag else "user_offline", "user": uname}
    rows = await db_all("SELECT owner_id FROM contacts WHERE contact_id=?", (uid,))
    targets = set([r["owner_id"] for r in rows] + list(online.keys()))
    for t in targets:
        if t != uid:
            await send_to_user(t, payload)

async def username_of(uid):
    row = await db_one("SELECT username FROM users WHERE id=?", (uid,))
    return row["username"] if row else None

# ================== RESEND EMAIL ==================
async def send_email(to: str, subject: str, code: str) -> bool:
    """Отправляет письмо через Resend. Возвращает True если отправлено."""
    if not RESEND_KEY:
        return False
    html = (
        "<div style='font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;"
        "max-width:480px;margin:0 auto;padding:24px;background:#0c0a12;color:#f4f1fa;border-radius:20px'>"
        "<div style='font-size:22px;font-weight:800;color:#ff6aa8;margin-bottom:16px'>Aitgram</div>"
        "<p style='font-size:16px;color:#f4f1fa;margin:0 0 20px'>Ваш код подтверждения:</p>"
        f"<div style='font-size:34px;font-weight:800;letter-spacing:8px;"
        f"color:#ff6aa8;text-align:center;padding:16px;background:#16131f;border-radius:16px'>{code}</div>"
        "<p style='color:#8f8aa3;font-size:13px;margin:20px 0 0'>Код действует 10 минут. "
        "Если вы не запрашивали код — просто проигнорируйте письмо.</p>"
        "</div>"
    )
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": RESEND_FROM,
                    "to": [to],
                    "subject": subject,
                    "html": html,
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                txt = await r.text()
                if r.status >= 300:
                    print("Resend error:", r.status, txt)
                    return False
                return True
    except Exception as e:
        print("Resend exception:", e)
        return False

# ================== HTTP ==================
async def index(request):
    return web.FileResponse(STATIC_DIR / "index.html")

async def manifest(request):
    p = STATIC_DIR / "manifest.json"
    if p.exists():
        return web.FileResponse(p)
    return web.json_response({
        "name": "Aitgram", "short_name": "Aitgram",
        "start_url": "/", "display": "standalone",
        "background_color": "#0c0a12", "theme_color": "#0c0a12",
        "icons": []
    })

async def sw(request):
    p = STATIC_DIR / "sw.js"
    if p.exists():
        return web.FileResponse(p)
    return web.Response(
        text="self.addEventListener('install',()=>self.skipWaiting());",
        content_type="application/javascript"
    )

# ================== WS ==================
async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=30, max_msg_size=32 * 1024 * 1024)
    await ws.prepare(request)

    my_uid = None
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except Exception:
                continue
            cmd = data.get("cmd")
            if not cmd:
                continue

            # ---- PING ----
            if cmd == "ping":
                continue

            # ---- RESUME ----
            if cmd == "resume":
                tok = data.get("token")
                uid = sessions.get(tok)
                if not uid:
                    await ws.send_json({"type": "error", "msg": "session expired"})
                    continue
                my_uid = uid
                online[uid] = ws
                await db_exec("UPDATE users SET online=1, last_seen=? WHERE id=?", (now_iso(), uid))
                u = await db_one("SELECT id, first_name, avatar FROM users WHERE id=?", (uid,))
                await ws.send_json({
                    "type": "logged_in", "user_id": uid, "token": tok,
                    "first_name": u["first_name"], "avatar": u["avatar"]
                })
                await broadcast_online(uid, True)
                continue

            # ---- SEND REG CODE ----
            if cmd == "send_reg_code":
                email = (data.get("email") or "").strip().lower()
                if not email or "@" not in email:
                    await ws.send_json({"type": "error", "msg": "Некорректный email"})
                    continue
                code = gen_code()
                codes[email] = (code, datetime.utcnow() + timedelta(minutes=CODE_TTL_MIN), 0)
                ok = await send_email(email, "Aitgram — код подтверждения", code)
                resp = {"type": "reg_code_sent", "email": email}
                if not ok:
                    resp["dev_code"] = code
                await ws.send_json(resp)
                continue

            # ---- CHECK REG CODE ----
            if cmd == "check_reg_code":
                email = (data.get("email") or "").strip().lower()
                code = (data.get("code") or "").strip()
                rec = codes.get(email)
                if not rec:
                    await ws.send_json({"type": "error", "msg": "Код не запрашивался"})
                    continue
                real_code, exp, tries = rec
                if datetime.utcnow() > exp:
                    await ws.send_json({"type": "error", "msg": "Код истёк"})
                    continue
                if code != real_code:
                    codes[email] = (real_code, exp, tries + 1)
                    await ws.send_json({"type": "error", "msg": "Неверный код"})
                    continue

                user = await db_one("SELECT id, first_name, avatar FROM users WHERE email=?", (email,))
                if user:
                    tok = gen_token()
                    sessions[tok] = user["id"]
                    my_uid = user["id"]
                    online[user["id"]] = ws
                    await db_exec("UPDATE users SET online=1, last_seen=? WHERE id=?",
                                  (now_iso(), user["id"]))
                    await ws.send_json({
                        "type": "logged_in", "user_id": user["id"], "token": tok,
                        "first_name": user["first_name"], "avatar": user["avatar"]
                    })
                    await broadcast_online(user["id"], True)
                else:
                    await ws.send_json({"type": "reg_code_ok", "email": email})
                continue

            # ---- LOGIN BY EMAIL (страховка) ----
            if cmd == "login_by_email":
                email = (data.get("email") or "").strip().lower()
                code = (data.get("code") or "").strip()
                rec = codes.get(email)
                if not rec or rec[0] != code:
                    await ws.send_json({"type": "error", "msg": "Неверный код"})
                    continue
                user = await db_one("SELECT id, first_name, avatar FROM users WHERE email=?", (email,))
                if not user:
                    await ws.send_json({"type": "error", "msg": "Аккаунт не найден"})
                    continue
                tok = gen_token()
                sessions[tok] = user["id"]
                my_uid = user["id"]
                online[user["id"]] = ws
                await db_exec("UPDATE users SET online=1, last_seen=? WHERE id=?",
                              (now_iso(), user["id"]))
                await ws.send_json({
                    "type": "logged_in", "user_id": user["id"], "token": tok,
                    "first_name": user["first_name"], "avatar": user["avatar"]
                })
                await broadcast_online(user["id"], True)
                continue

            # ---- REGISTER (создание аккаунта) ----
            if cmd == "register_email":
                email = (data.get("email") or "").strip().lower()
                code = (data.get("code") or "").strip()
                username = clean_u(data.get("username"))
                password = data.get("password") or ""
                first_name = (data.get("first_name") or "").strip() or username
                avatar_data = data.get("avatar_data")

                rec = codes.get(email)
                if not rec or rec[0] != code:
                    await ws.send_json({"type": "error", "msg": "Неверный код"})
                    continue

                if await db_one("SELECT id FROM users WHERE email=?", (email,)):
                    await ws.send_json({"type": "error", "msg": "Email уже зарегистрирован"})
                    continue

                if await db_one("SELECT id FROM users WHERE username=?", (username,)):
                    await ws.send_json({"type": "error", "msg": "Юзернейм занят"})
                    continue

                ts = now_iso()
                await db_exec("""INSERT INTO users
                    (username, email, password, first_name, avatar, registered_at,
                     primary_username, online, last_seen)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (username, email, password, first_name,
                     avatar_data if avatar_data else None,
                     ts, username, 1, ts))
                u = await db_one("SELECT id FROM users WHERE email=?", (email,))
                uid = u["id"]
                await db_exec("INSERT OR IGNORE INTO usernames(user_id,username,is_primary) VALUES (?,?,1)",
                              (uid, username))
                tok = gen_token()
                sessions[tok] = uid
                my_uid = uid
                online[uid] = ws
                await ws.send_json({
                    "type": "logged_in", "user_id": uid, "token": tok,
                    "first_name": first_name, "avatar": avatar_data
                })
                await broadcast_online(uid, True)
                continue

            # ================= AUTH REQUIRED =================
            if not my_uid:
                await ws.send_json({"type": "error", "msg": "session expired"})
                continue

            # ---- USERS ----
            if cmd == "users":
                rows = await db_all("""
                    SELECT u.id, u.username, u.first_name, u.avatar, u.online, u.last_seen
                    FROM users u WHERE u.id != ?
                """, (my_uid,))
                cont = await db_all("SELECT contact_id FROM contacts WHERE owner_id=?", (my_uid,))
                contacts = [r["contact_id"] for r in cont]
                lst = [{"u": r["username"], "n": r["first_name"] or r["username"],
                        "av": r["avatar"], "online": bool(r["online"]),
                        "ls": r["last_seen"], "id": r["id"]} for r in rows]
                await ws.send_json({"type": "users", "list": lst, "contacts": contacts})
                continue

            # ---- PROFILE ----
            if cmd == "profile":
                uname = clean_u(data.get("user"))
                u = await db_one("SELECT * FROM users WHERE username=?", (uname,))
                if not u:
                    await ws.send_json({"type": "error", "msg": "Профиль не найден"})
                    continue
                ulist = await db_all("SELECT username, is_primary FROM usernames WHERE user_id=?",
                                     (u["id"],))
                usernames = [{"u": r["username"], "primary": bool(r["is_primary"])} for r in ulist]
                blk = await db_one("SELECT 1 FROM blocks WHERE blocker_id=? AND blocked_id=?",
                                   (my_uid, u["id"]))
                blkby = await db_one("SELECT 1 FROM blocks WHERE blocker_id=? AND blocked_id=?",
                                     (u["id"], my_uid))
                ad = await db_one("SELECT seconds FROM autodelete WHERE user_id=? AND peer_id=?",
                                  (my_uid, u["id"]))
                gifts_rows = await db_all("""
                    SELECT g.name, g.emoji, g.stars, u.username as from_u, u.avatar as from_av
                    FROM gifts g JOIN users u ON u.id=g.from_id
                    WHERE g.to_id=? ORDER BY g.id DESC LIMIT 60
                """, (u["id"],))
                gifts = [{"name": r["name"], "emoji": r["emoji"], "stars": r["stars"],
                          "from": r["from_u"], "from_av": r["from_av"]} for r in gifts_rows]
                await ws.send_json({
                    "type": "profile",
                    "user": u["username"],
                    "first_name": u["first_name"],
                    "last_name": u["last_name"],
                    "bio": u["bio"],
                    "avatar": u["avatar"],
                    "usernames": usernames,
                    "online": bool(u["online"]),
                    "last_seen": u["last_seen"],
                    "registered_at": u["registered_at"],
                    "phone_country": u["phone_country"],
                    "phone_country_flag": u["phone_country_flag"],
                    "updated_name_at": u["name_updated_at"],
                    "updated_photo_at": u["photo_updated_at"],
                    "autodelete": ad["seconds"] if ad else 0,
                    "blocked_me": bool(blkby),
                    "i_blocked": bool(blk),
                    "gifts": gifts,
                })
                if u["avatar"]:
                    await ws.send_json({"type": "avatar_data",
                                        "file": u["avatar"], "data": u["avatar"]})
                continue

            # ---- GET AVATAR ----
            if cmd == "get_avatar":
                f = data.get("file")
                u = await db_one("SELECT avatar FROM users WHERE avatar=?", (f,))
                if u and u["avatar"]:
                    await ws.send_json({"type": "avatar_data", "file": f, "data": u["avatar"]})
                continue

            # ---- SEARCH ----
            if cmd == "search_users":
                q = clean_u(data.get("q"))
                rows = await db_all("""SELECT id,username,first_name,avatar,online,last_seen
                    FROM users WHERE username LIKE ? AND id != ? LIMIT 30""",
                    (f"%{q}%", my_uid))
                await ws.send_json({"type": "search_results",
                    "list": [{"u": r["username"], "n": r["first_name"] or r["username"],
                              "av": r["avatar"], "online": bool(r["online"])} for r in rows]})
                continue

            # ---- CONTACTS ----
            if cmd == "add_contact":
                uname = clean_u(data.get("user"))
                c = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not c:
                    continue
                await db_exec("INSERT OR IGNORE INTO contacts(owner_id,contact_id) VALUES (?,?)",
                              (my_uid, c["id"]))
                await ws.send_json({"type": "contact_added", "user": uname})
                continue

            if cmd == "delete_contact":
                uname = clean_u(data.get("user"))
                c = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not c:
                    continue
                await db_exec("DELETE FROM contacts WHERE owner_id=? AND contact_id=?",
                              (my_uid, c["id"]))
                await ws.send_json({"type": "contact_deleted", "user": uname})
                continue

            if cmd == "save_contact":
                uname = clean_u(data.get("user"))
                c = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not c:
                    continue
                await db_exec("""INSERT INTO contacts(owner_id,contact_id,first_name,last_name,note)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT(owner_id,contact_id) DO UPDATE SET
                    first_name=excluded.first_name,
                    last_name=excluded.last_name,
                    note=excluded.note""",
                    (my_uid, c["id"], data.get("first_name"), data.get("last_name"),
                     data.get("note")))
                await ws.send_json({"type": "contact_added", "user": uname})
                continue

            # ---- BLOCK ----
            if cmd == "block":
                uname = clean_u(data.get("user"))
                c = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not c:
                    continue
                await db_exec("INSERT OR IGNORE INTO blocks(blocker_id,blocked_id) VALUES (?,?)",
                              (my_uid, c["id"]))
                await ws.send_json({"type": "blocked", "user": uname})
                await send_to_user(c["id"], {"type": "blocked_by",
                                             "user": await username_of(my_uid)})
                continue

            if cmd == "unblock":
                uname = clean_u(data.get("user"))
                c = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not c:
                    continue
                await db_exec("DELETE FROM blocks WHERE blocker_id=? AND blocked_id=?",
                              (my_uid, c["id"]))
                await ws.send_json({"type": "unblocked", "user": uname})
                await send_to_user(c["id"], {"type": "unblocked_by",
                                             "user": await username_of(my_uid)})
                continue

            if cmd == "blocked_list":
                rows = await db_all("""SELECT u.username FROM blocks b
                    JOIN users u ON u.id=b.blocked_id WHERE b.blocker_id=?""", (my_uid,))
                by = await db_all("""SELECT u.username FROM blocks b
                    JOIN users u ON u.id=b.blocker_id WHERE b.blocked_id=?""", (my_uid,))
                await ws.send_json({
                    "type": "blocked_list",
                    "list": [r["username"] for r in rows],
                    "list_by": [r["username"] for r in by],
                })
                continue

            # ---- AUTODELETE ----
            if cmd == "set_autodelete":
                uname = clean_u(data.get("user"))
                sec = int(data.get("seconds") or 0)
                c = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not c:
                    continue
                if sec:
                    await db_exec("""INSERT INTO autodelete(user_id,peer_id,seconds) VALUES (?,?,?)
                        ON CONFLICT(user_id,peer_id) DO UPDATE SET seconds=excluded.seconds""",
                        (my_uid, c["id"], sec))
                else:
                    await db_exec("DELETE FROM autodelete WHERE user_id=? AND peer_id=?",
                                  (my_uid, c["id"]))
                await ws.send_json({"type": "autodelete_set", "user": uname, "seconds": sec})
                continue

            # ---- HISTORY ----
            if cmd == "history":
                uname = clean_u(data.get("with"))
                other = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not other:
                    await ws.send_json({"type": "history", "msgs": []})
                    continue
                rows = await db_all("""SELECT * FROM messages
                    WHERE deleted=0 AND ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?))
                    ORDER BY ts ASC LIMIT 200""",
                    (my_uid, other["id"], other["id"], my_uid))
                msgs = []
                for m in rows:
                    rx = await db_all("""SELECT emoji, COUNT(*) c FROM reactions
                        WHERE msg_id=? GROUP BY emoji""", (m["id"],))
                    msgs.append({
                        "id": m["id"],
                        "from": await username_of(m["from_id"]),
                        "text": m["text"],
                        "ts": m["ts"],
                        "media_url": m["media"],
                        "media_type": m["media_type"],
                        "reply_to": m["reply_to"],
                        "forwarded_from": m["forwarded_from"],
                        "reactions": [{"emoji": r["emoji"], "count": r["c"]} for r in rx],
                        "read": bool(m["read"]),
                    })
                await ws.send_json({"type": "history", "msgs": msgs})
                await db_exec("UPDATE messages SET read=1 WHERE from_id=? AND to_id=?",
                              (other["id"], my_uid))
                continue

            if cmd == "get_pinned":
                uname = clean_u(data.get("chat_with"))
                other = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not other:
                    continue
                p = await db_one("""SELECT msg_id FROM pins
                    WHERE user_id=? AND peer_id=? ORDER BY rowid DESC LIMIT 1""",
                    (my_uid, other["id"]))
                if p:
                    m = await db_one("SELECT * FROM messages WHERE id=?", (p["msg_id"],))
                    if m:
                        await ws.send_json({"type": "msg_pinned",
                            "from": await username_of(m["from_id"]), "text": m["text"]})
                continue

            if cmd == "pin_msg":
                mid = data.get("msg_id")
                uname = clean_u(data.get("chat_with"))
                other = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not other:
                    continue
                await db_exec("INSERT INTO pins(user_id,peer_id,msg_id) VALUES (?,?,?)",
                              (my_uid, other["id"], mid))
                m = await db_one("SELECT * FROM messages WHERE id=?", (mid,))
                if m:
                    await ws.send_json({"type": "msg_pinned",
                        "from": await username_of(m["from_id"]), "text": m["text"]})
                continue

            if cmd == "unpin_msg":
                uname = clean_u(data.get("chat_with"))
                other = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not other:
                    continue
                await db_exec("DELETE FROM pins WHERE user_id=? AND peer_id=?",
                              (my_uid, other["id"]))
                await ws.send_json({"type": "msg_unpinned"})
                continue

            if cmd == "mark_read":
                uname = clean_u(data.get("with"))
                other = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not other:
                    continue
                await db_exec("UPDATE messages SET read=1 WHERE from_id=? AND to_id=?",
                              (other["id"], my_uid))
                await send_to_user(other["id"], {"type": "msg_read", "ids": []})
                continue

            # ---- SEND MESSAGE ----
            if cmd == "send":
                uname = clean_u(data.get("to"))
                other = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not other:
                    continue
                blocked = await db_one("SELECT 1 FROM blocks WHERE blocker_id=? AND blocked_id=?",
                                       (other["id"], my_uid))
                mid = f"m{secrets.token_hex(8)}"
                ts = now_iso()
                await db_exec("""INSERT INTO messages(id,from_id,to_id,text,media,media_type,
                    reply_to,forwarded_from,ts) VALUES (?,?,?,?,?,?,?,?,?)""",
                    (mid, my_uid, other["id"], data.get("text"),
                     data.get("media_data"), data.get("media_type"),
                     data.get("reply_to"), data.get("forwarded_from"), ts))
                await ws.send_json({"type": "sent", "id": mid, "to": uname,
                                    "text": data.get("text"), "ts": ts,
                                    "delivered": not blocked})
                if not blocked:
                    await send_to_user(other["id"], {
                        "type": "msg", "id": mid,
                        "from": await username_of(my_uid),
                        "text": data.get("text"),
                        "media_url": data.get("media_data"),
                        "media_type": data.get("media_type"),
                        "reply_to": data.get("reply_to"),
                        "forwarded_from": data.get("forwarded_from"),
                        "ts": ts,
                    })
                continue

            # ---- EDIT / DELETE ----
            if cmd == "edit_msg":
                await db_exec("UPDATE messages SET text=? WHERE id=? AND from_id=?",
                              (data.get("text"), data.get("msg_id"), my_uid))
                m = await db_one("SELECT from_id,to_id FROM messages WHERE id=?", (data.get("msg_id"),))
                if m:
                    peer = m["to_id"] if m["from_id"] == my_uid else m["from_id"]
                    await send_to_user(peer, {"type": "msg_edited",
                        "msg_id": data.get("msg_id"), "text": data.get("text")})
                continue

            if cmd == "delete_msg":
                mid = data.get("msg_id")
                m = await db_one("SELECT from_id,to_id FROM messages WHERE id=?", (mid,))
                if not m or m["from_id"] != my_uid:
                    continue
                await db_exec("UPDATE messages SET deleted=1 WHERE id=?", (mid,))
                await ws.send_json({"type": "msg_deleted", "msg_id": mid})
                await send_to_user(m["to_id"], {"type": "msg_deleted", "msg_id": mid})
                continue

            if cmd == "delete_chat":
                uname = clean_u(data.get("with"))
                other = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not other:
                    continue
                await db_exec("""DELETE FROM messages
                    WHERE (from_id=? AND to_id=?) OR (from_id=? AND to_id=?)""",
                    (my_uid, other["id"], other["id"], my_uid))
                await ws.send_json({"type": "chat_deleted", "with": uname})
                continue

            # ---- REACTIONS ----
            if cmd == "react":
                mid = data.get("msg_id")
                emoji = data.get("emoji")
                existing = await db_one("SELECT emoji FROM reactions WHERE msg_id=? AND user_id=?",
                                        (mid, my_uid))
                if existing and existing["emoji"] == emoji:
                    await db_exec("DELETE FROM reactions WHERE msg_id=? AND user_id=?",
                                  (mid, my_uid))
                else:
                    await db_exec("""INSERT INTO reactions(msg_id,user_id,emoji) VALUES (?,?,?)
                        ON CONFLICT(msg_id,user_id) DO UPDATE SET emoji=excluded.emoji""",
                        (mid, my_uid, emoji))
                rx = await db_all("""SELECT emoji, COUNT(*) c FROM reactions
                    WHERE msg_id=? GROUP BY emoji""", (mid,))
                payload = {"type": "reactions_updated", "msg_id": mid,
                           "reactions": [{"emoji": r["emoji"], "count": r["c"]} for r in rx]}
                await ws.send_json(payload)
                m = await db_one("SELECT from_id,to_id FROM messages WHERE id=?", (mid,))
                if m:
                    peer = m["to_id"] if m["from_id"] == my_uid else m["from_id"]
                    await send_to_user(peer, payload)
                continue

            # ---- GIFTS ----
            if cmd == "send_gift":
                uname = clean_u(data.get("to"))
                other = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not other:
                    continue
                await db_exec("""INSERT INTO gifts(from_id,to_id,name,emoji,stars,ts)
                    VALUES (?,?,?,?,?,?)""",
                    (my_uid, other["id"], data.get("name"), data.get("emoji"),
                     int(data.get("stars") or 0), now_iso()))
                await ws.send_json({"type": "gift_sent", "to": uname})
                await send_to_user(other["id"], {"type": "gift_received",
                    "from": await username_of(my_uid),
                    "name": data.get("name"), "emoji": data.get("emoji"),
                    "stars": data.get("stars")})
                continue

            # ---- PUBLICATIONS ----
            if cmd == "publish":
                pid = f"p{secrets.token_hex(6)}"
                await db_exec("""INSERT INTO publications(id,user_id,media,media_type,ts)
                    VALUES (?,?,?,?,?)""",
                    (pid, my_uid, data.get("media_data"), data.get("media_type"), now_iso()))
                await ws.send_json({"type": "publication_added",
                    "id": pid, "user": await username_of(my_uid),
                    "media_url": data.get("media_data"),
                    "media_type": data.get("media_type"),
                    "ts": now_iso()})
                continue

            if cmd == "delete_publication":
                await db_exec("DELETE FROM publications WHERE id=? AND user_id=?",
                              (data.get("id"), my_uid))
                continue

            # ---- PROFILE UPDATE ----
            if cmd == "update_profile":
                updates = []
                params = []
                if "first_name" in data:
                    updates += ["first_name=?", "name_updated_at=?"]
                    params += [data["first_name"], now_iso()]
                if "bio" in data:
                    updates += ["bio=?"]
                    params += [data["bio"]]
                if data.get("avatar_clear") or data.get("avatar_data") == "":
                    updates += ["avatar=NULL", "photo_updated_at=?"]
                    params += [now_iso()]
                elif "avatar_data" in data and data["avatar_data"]:
                    updates += ["avatar=?", "photo_updated_at=?"]
                    params += [data["avatar_data"], now_iso()]
                if updates:
                    params.append(my_uid)
                    await db_exec(f"UPDATE users SET {', '.join(updates)} WHERE id=?", *params)
                await ws.send_json({"type": "profile_updated"})
                continue

            # ---- USERNAMES ----
            if cmd == "check_username":
                u = clean_u(data.get("username"))
                exists = await db_one("SELECT 1 FROM usernames WHERE username=?", (u,))
                await ws.send_json({"type": "username_check", "username": u,
                                    "available": not exists})
                continue

            if cmd == "add_username":
                u = clean_u(data.get("username"))
                if not u or len(u) < 4:
                    await ws.send_json({"type": "error", "msg": "Минимум 4"})
                    continue
                if await db_one("SELECT 1 FROM usernames WHERE username=?", (u,)):
                    await ws.send_json({"type": "error", "msg": "Занято"})
                    continue
                await db_exec("INSERT INTO usernames(user_id,username,is_primary) VALUES (?,?,0)",
                              (my_uid, u))
                await ws.send_json({"type": "username_added", "username": u})
                continue

            if cmd == "delete_username":
                u = clean_u(data.get("username"))
                row = await db_one("SELECT is_primary FROM usernames WHERE user_id=? AND username=?",
                                   (my_uid, u))
                if not row or row["is_primary"]:
                    continue
                await db_exec("DELETE FROM usernames WHERE user_id=? AND username=?",
                              (my_uid, u))
                await ws.send_json({"type": "username_deleted", "username": u})
                continue

            if cmd == "set_primary_username":
                u = clean_u(data.get("username"))
                await db_exec("UPDATE usernames SET is_primary=0 WHERE user_id=?", (my_uid,))
                await db_exec("UPDATE usernames SET is_primary=1 WHERE user_id=? AND username=?",
                              (my_uid, u))
                await db_exec("UPDATE users SET primary_username=?, username=? WHERE id=?",
                              (u, u, my_uid))
                await ws.send_json({"type": "primary_updated", "username": u})
                continue

            # ---- REPORT ----
            if cmd == "report":
                uname = clean_u(data.get("user"))
                other = await db_one("SELECT id FROM users WHERE username=?", (uname,))
                if not other:
                    continue
                await db_exec("""INSERT INTO reports(from_id,to_id,reason,ts)
                    VALUES (?,?,?,?)""",
                    (my_uid, other["id"], data.get("reason"), now_iso()))
                await ws.send_json({"type": "report_sent"})
                continue

    except Exception as e:
        print("WS error:", e)
    finally:
        if my_uid and online.get(my_uid) is ws:
            online.pop(my_uid, None)
            await db_exec("UPDATE users SET online=0, last_seen=? WHERE id=?",
                          (now_iso(), my_uid))
            await broadcast_online(my_uid, False)
    return ws

# ================== APP ==================
async def main():
    await init_db()
    app = web.Application(client_max_size=32 * 1024 * 1024)
    app.router.add_get("/", index)
    app.router.add_get("/manifest.json", manifest)
    app.router.add_get("/sw.js", sw)
    app.router.add_get("/ws", ws_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    print(f"Aitgram server started on http://{HOST}:{PORT}")
    if not RESEND_KEY:
        print("⚠ RESEND_KEY не задан — код будет показываться прямо на экране (dev_code)")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bye")
