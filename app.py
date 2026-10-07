#!/usr/bin/env python3
import asyncio
import base64
import json
import os
import sqlite3
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse

DB = os.path.expanduser("~/messenger.db")
AVATAR_DIR = os.path.expanduser("~/avatars")
HTML_FILE = os.path.join(os.path.dirname(__file__), "index.html")
PORT = int(os.environ.get("PORT", 10000))

app = FastAPI()
os.makedirs(AVATAR_DIR, exist_ok=True)
clients = {}


def init_db():
    con = sqlite3.connect(DB)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY, username TEXT UNIQUE,
            password TEXT, first_name TEXT, bio TEXT,
            avatar TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user TEXT, to_user TEXT, text TEXT, ts TEXT,
            read INTEGER DEFAULT 0, deleted INTEGER DEFAULT 0
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
    """)
    con.commit()
    con.close()


def now_iso():
    return datetime.utcnow().isoformat()


def save_avatar(username, b64data):
    if not b64data or "," not in b64data:
        return None
    try:
        header, b64 = b64data.split(",", 1)
        ext = "jpg"
        if "png" in header: ext = "png"
        elif "gif" in header: ext = "gif"
        elif "webp" in header: ext = "webp"
        raw = base64.b64decode(b64)
        if len(raw) > 5 * 1024 * 1024: return None
        fname = f"{username}_{int(datetime.utcnow().timestamp())}.{ext}"
        with open(os.path.join(AVATAR_DIR, fname), "wb") as f:
            f.write(raw)
        return fname
    except Exception as e:
        print(f"[avatar] {e}")
        return None


async def send_to(uid, payload):
    ws = clients.get(uid)
    if ws:
        try:
            await ws.send_text(json.dumps(payload)); return True
        except: pass
    return False


@app.get("/")
def root():
    if os.path.exists(HTML_FILE):
        return FileResponse(HTML_FILE)
    return HTMLResponse("<h1>Messenger</h1><p>index.html не найден</p>")


@app.get("/avatars/{fname}")
def get_avatar_file(fname: str):
    path = os.path.join(AVATAR_DIR, fname)
    if os.path.exists(path):
        return FileResponse(path)
    return HTMLResponse("", status_code=404)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    user_id = None
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except:
                await websocket.send_text(json.dumps({"type":"error","msg":"bad json"}))
                continue
            cmd = data.get("cmd")

            if cmd == "register":
                u = (data.get("username") or "").strip().lower()
                p = data.get("password") or ""
                if not u or not p or len(u) < 4:
                    await websocket.send_text(json.dumps({"type":"error","msg":"username 4+"}))
                    continue
                con = sqlite3.connect(DB)
                try:
                    con.execute("INSERT INTO users (user_id,username,password,first_name,created_at) VALUES (?,?,?,?,?)",
                                (u,u,p,u,now_iso()))
                    con.execute("INSERT INTO usernames (user_id,username,is_primary,created_at) VALUES (?,?,1,?)",
                                (u,u,now_iso()))
                    con.commit()
                    user_id = u; clients[u] = websocket
                    await websocket.send_text(json.dumps({"type":"registered","user_id":u}))
                except sqlite3.IntegrityError:
                    await websocket.send_text(json.dumps({"type":"error","msg":"занято"}))
                finally: con.close()
                continue

            if cmd == "login":
                u = (data.get("username") or "").strip().lower()
                p = data.get("password") or ""
                con = sqlite3.connect(DB)
                row = con.execute("SELECT password FROM users WHERE username=?", (u,)).fetchone()
                con.close()
                if not row or row[0] != p:
                    await websocket.send_text(json.dumps({"type":"error","msg":"неверный"}))
                    continue
                user_id = u; clients[u] = websocket
                await websocket.send_text(json.dumps({"type":"logged_in","user_id":u}))
                continue

            if not user_id:
                await websocket.send_text(json.dumps({"type":"error","msg":"сначала login"}))
                continue

            if cmd == "users":
                con = sqlite3.connect(DB)
                rows = con.execute("SELECT username,first_name,avatar FROM users ORDER BY username").fetchall()
                con.close()
                await websocket.send_text(json.dumps({"type":"users","list":[
                    {"u":r[0],"n":r[1] or r[0],"av":r[2] or ""} for r in rows]}))
                continue

            if cmd == "search_users":
                q = (data.get("q") or "").strip().lower()
                con = sqlite3.connect(DB)
                rows = con.execute("""SELECT username,first_name,avatar FROM users
                    WHERE (LOWER(username) LIKE ? OR LOWER(first_name) LIKE ?) AND username != ?
                    ORDER BY username LIMIT 30""", (f"%{q}%",f"%{q}%",user_id)).fetchall()
                con.close()
                await websocket.send_text(json.dumps({"type":"search_results","list":[
                    {"u":r[0],"n":r[1] or r[0],"av":r[2] or ""} for r in rows]}))
                continue

            if cmd == "send":
                to = (data.get("to") or "").strip().lower()
                text = data.get("text") or ""
                if not to or not text: continue
                ts = now_iso()
                con = sqlite3.connect(DB)
                con.execute("INSERT INTO messages (from_user,to_user,text,ts) VALUES (?,?,?,?)",
                            (user_id,to,text,ts))
                con.commit(); con.close()
                delivered = await send_to(to, {"type":"msg","from":user_id,"text":text,"ts":ts})
                await websocket.send_text(json.dumps({"type":"sent","to":to,"text":text,"ts":ts,"delivered":delivered}))
                continue

            if cmd == "history":
                w = (data.get("with") or "").strip().lower()
                limit = int(data.get("limit") or 100)
                con = sqlite3.connect(DB)
                rows = con.execute("""SELECT from_user,to_user,text,ts FROM messages
                    WHERE ((from_user=? AND to_user=?) OR (from_user=? AND to_user=?)) AND deleted=0
                    ORDER BY ts DESC LIMIT ?""", (user_id,w,w,user_id,limit)).fetchall()
                con.close()
                msgs = [{"from":r[0],"to":r[1],"text":r[2],"ts":r[3]} for r in reversed(rows)]
                await websocket.send_text(json.dumps({"type":"history","with":w,"msgs":msgs}))
                continue

            if cmd == "profile":
                target = (data.get("user") or user_id).lower()
                con = sqlite3.connect(DB)
                row = con.execute("SELECT username,first_name,bio,avatar,created_at FROM users WHERE username=?",
                                  (target,)).fetchone()
                if not row:
                    con.close()
                    await websocket.send_text(json.dumps({"type":"error","msg":"юзер не найден"}))
                    continue
                gifts = con.execute("""SELECT gift_name,gift_emoji,stars,from_user,ts
                    FROM gifts WHERE to_user=? ORDER BY ts DESC LIMIT 50""",(target,)).fetchall()
                unames = con.execute("SELECT username,is_primary FROM usernames WHERE user_id=? ORDER BY is_primary DESC,id",
                                    (row[0],)).fetchall()
                con.close()
                await websocket.send_text(json.dumps({
                    "type":"profile","user":row[0],"first_name":row[1] or row[0],
                    "bio":row[2] or "","avatar":row[3] or "","created_at":row[4],
                    "gifts":[{"name":g[0],"emoji":g[1],"stars":g[2],"from":g[3],"ts":g[4]} for g in gifts],
                    "usernames":[{"u":x[0],"primary":bool(x[1])} for x in unames]
                }))
                continue

            if cmd == "update_profile":
                fn = (data.get("first_name") or "").strip() or user_id
                bio = (data.get("bio") or "").strip()
                av_data = data.get("avatar_data")
                av = data.get("avatar")
                if av_data:
                    new_name = save_avatar(user_id, av_data)
                    if new_name: av = new_name
                con = sqlite3.connect(DB)
                if av is not None:
                    con.execute("UPDATE users SET first_name=?,bio=?,avatar=? WHERE username=?",
                                (fn,bio,av,user_id))
                else:
                    con.execute("UPDATE users SET first_name=?,bio=? WHERE username=?",
                                (fn,bio,user_id))
                con.commit(); con.close()
                await websocket.send_text(json.dumps({"type":"profile_updated"}))
                continue

            if cmd == "get_avatar":
                fname = data.get("file") or ""
                path = os.path.join(AVATAR_DIR, fname)
                if fname and os.path.exists(path):
                    try:
                        with open(path,"rb") as f: raw = f.read()
                        b64 = base64.b64encode(raw).decode()
                        ext = fname.rsplit(".",1)[-1]
                        mime = "image/jpeg" if ext in ("jpg","jpeg") else f"image/{ext}"
                        await websocket.send_text(json.dumps({"type":"avatar_data","file":fname,
                            "data":f"data:{mime};base64,{b64}"}))
                    except Exception as e:
                        print(f"[avatar] {e}")
                else:
                    await websocket.send_text(json.dumps({"type":"avatar_data","file":fname,"data":""}))
                continue

            if cmd == "send_gift":
                to = (data.get("to") or "").strip().lower()
                if not to: continue
                ts = now_iso()
                con = sqlite3.connect(DB)
                if not con.execute("SELECT 1 FROM users WHERE username=?",(to,)).fetchone():
                    con.close()
                    await websocket.send_text(json.dumps({"type":"error","msg":"нет получателя"}))
                    continue
                con.execute("INSERT INTO gifts (from_user,to_user,gift_name,gift_emoji,stars,ts) VALUES (?,?,?,?,?,?)",
                            (user_id,to,data.get("name") or "Подарок",
                             data.get("emoji") or "🎁", int(data.get("stars") or 0),ts))
                con.commit(); con.close()
                await send_to(to, {"type":"gift_received","from":user_id,
                    "name":data.get("name"),"emoji":data.get("emoji"),"stars":data.get("stars"),"ts":ts})
                await websocket.send_text(json.dumps({"type":"gift_sent","to":to,
                    "name":data.get("name"),"emoji":data.get("emoji"),"stars":data.get("stars")}))
                continue

            if cmd == "add_username":
                new_u = (data.get("username") or "").strip().lower().lstrip("@")
                if not new_u or len(new_u) < 4 or len(new_u) > 32:
                    await websocket.send_text(json.dumps({"type":"error","msg":"4-32 символа"}))
                    continue
                if not new_u.replace("_","").isalnum():
                    await websocket.send_text(json.dumps({"type":"error","msg":"буквы/цифры/_"}))
                    continue
                con = sqlite3.connect(DB)
                try:
                    con.execute("INSERT INTO usernames (user_id,username,is_primary,created_at) VALUES (?,?,0,?)",
                                (user_id,new_u,now_iso()))
                    con.commit()
                    await websocket.send_text(json.dumps({"type":"username_added","username":new_u}))
                except sqlite3.IntegrityError:
                    await websocket.send_text(json.dumps({"type":"error","msg":"username занят"}))
                finally: con.close()
                continue

            if cmd == "delete_username":
                name = (data.get("username") or "").strip().lower()
                con = sqlite3.connect(DB)
                row = con.execute("SELECT is_primary FROM usernames WHERE user_id=? AND username=?",
                                  (user_id,name)).fetchone()
                if not row:
                    con.close()
                    await websocket.send_text(json.dumps({"type":"error","msg":"не найдено"}))
                    continue
                if row[0] == 1:
                    con.close()
                    await websocket.send_text(json.dumps({"type":"error","msg":"нельзя удалить основной"}))
                    continue
                con.execute("DELETE FROM usernames WHERE user_id=? AND username=?",(user_id,name))
                con.commit(); con.close()
                await websocket.send_text(json.dumps({"type":"username_deleted","username":name}))
                continue

            if cmd == "set_primary_username":
                name = (data.get("username") or "").strip().lower()
                con = sqlite3.connect(DB)
                if not con.execute("SELECT 1 FROM usernames WHERE user_id=? AND username=?",
                                   (user_id,name)).fetchone():
                    con.close()
                    await websocket.send_text(json.dumps({"type":"error","msg":"не найдено"}))
                    continue
                if con.execute("SELECT 1 FROM users WHERE username=? AND user_id!=?",
                               (name,user_id)).fetchone():
                    con.close()
                    await websocket.send_text(json.dumps({"type":"error","msg":"занят"}))
                    continue
                con.execute("UPDATE usernames SET is_primary=0 WHERE user_id=?", (user_id,))
                con.execute("UPDATE usernames SET is_primary=1 WHERE user_id=? AND username=?",
                            (user_id,name))
                con.execute("UPDATE users SET username=? WHERE user_id=?", (name,user_id))
                con.commit(); con.close()
                await websocket.send_text(json.dumps({"type":"primary_updated","username":name}))
                continue

            if cmd == "delete_chat":
                w = (data.get("with") or "").strip().lower()
                con = sqlite3.connect(DB)
                con.execute("UPDATE messages SET deleted=1 WHERE (from_user=? AND to_user=?) OR (from_user=? AND to_user=?)",
                            (user_id,w,w,user_id))
                con.commit(); con.close()
                await websocket.send_text(json.dumps({"type":"chat_deleted","with":w}))
                continue

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[ws] {e}")
    finally:
        if user_id and clients.get(user_id) is websocket:
            del clients[user_id]


@app.on_event("startup")
async def startup():
    init_db()
    print(f"[server] старт на порту {PORT}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
