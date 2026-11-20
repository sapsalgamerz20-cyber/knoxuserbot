#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, time, random, asyncio, threading, requests, hashlib, inspect
import uuid, base64
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
import psutil

try:
    from igrapiweb import make_ig_web_socket
except ImportError:
    print("❌ igrapiweb not installed. Run: pip install igrapiweb", flush=True)
    sys.exit(1)

_MWS_PARAMS = set()
try:
    _MWS_PARAMS = set(inspect.signature(make_ig_web_socket).parameters.keys())
except Exception:
    pass


# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════
ADMIN_KEY = os.environ.get("ADMIN_KEY", "knox123")
OWNER_ID = int(os.environ.get("OWNER_ID", "7741173292"))

GITHUB_PAT = os.environ.get("GH_PAT", "")
GH_REPO = os.environ.get("GH_REPO", "sapsalgamerz20-cyber/knoxuserbot")
GH_FILE = "config.json"
GH_BRANCH = "main"

IST = timezone(timedelta(hours=5, minutes=30))

EMOJI_POOL = ["🪾", "🪺", "🪹", "🍃", "🍂", "🍁", "🍀", "☘️", "🌿", "🌾", "🌵", "🌴", "🌳", "🌲", "🪴", "🌱", "⚘️", "🪻", "🌷", "🌼", "🌻", "🌺", "🥀", "🌹", "🏵", "🪷", "🌸", "💮"]
GC_EMOJI = "🪼"
FOOTER = "\n\n\n\n𝐤ɴᴏ𝐱 𝐒ᴘɪʀɪᴛᴜᴀʟ 𝐏ᴀʏʟᴏᴀᴅ v2🏞️"

UA_POOL = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; V2148A) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; CPH2451) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15"
]

AUTHORIZED_USERS = {OWNER_ID}
SESSIONS = {}
CONFIG_LOCK = threading.Lock()
SESSIONS_LOCK = threading.Lock()
AUTH_LOCK = threading.Lock()
GH_SHA = None

WS_ROTATE_MIN = 1800
WS_ROTATE_MAX = 5400
HEALTH_CHECK_MIN = 300
HEALTH_CHECK_MAX = 600

try:
    SOCKET_POOL = max(1, min(10, int(os.environ.get("SOCKET_POOL", "1"))))
except Exception:
    SOCKET_POOL = 1

HTTP_TIMEOUT = (5, 10)


# ═══════════════════════════════════════════════════════════
# SYSTEM STATS
# ═══════════════════════════════════════════════════════════
def get_sys_stats():
    try:
        cpu = psutil.cpu_percent(interval=None)
        cores = psutil.cpu_count(logical=True)
        ram = psutil.virtual_memory()
        proc = psutil.Process(os.getpid())
        mem_info = proc.memory_info()
        return {
            "cpu_pct": cpu,
            "cores": cores,
            "ram_total_gb": round(ram.total / (1024**3), 2),
            "ram_used_gb": round((ram.total - ram.available) / (1024**3), 2),
            "ram_percent": ram.percent,
            "proc_ram_mb": round(mem_info.rss / (1024 * 1024), 1),
        }
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════
# GITHUB SYNC
# ═══════════════════════════════════════════════════════════
def gh_fetch_config():
    global GH_SHA, AUTHORIZED_USERS
    if not GITHUB_PAT:
        return {}
    url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE}"
    headers = {"Authorization": f"token {GITHUB_PAT}"}
    try:
        r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            GH_SHA = data["sha"]
            content = base64.b64decode(data["content"]).decode("utf-8")
            cfg = json.loads(content)
            if "users" in cfg:
                with AUTH_LOCK:
                    AUTHORIZED_USERS = set(cfg["users"])
                    AUTHORIZED_USERS.add(OWNER_ID)
            return cfg.get("sessions", {})
    except Exception as e:
        print(f"⚠️ GH fetch: {e}", flush=True)
    return {}


def gh_push_config(silent=True, retry=0):
    global GH_SHA
    if not GITHUB_PAT or retry > 3:
        return

    with CONFIG_LOCK:
        with SESSIONS_LOCK:
            sessions_snapshot = {k: v.data for k, v in SESSIONS.items()}
        with AUTH_LOCK:
            users_snapshot = list(AUTHORIZED_USERS)
        cfg = {"users": users_snapshot, "sessions": sessions_snapshot}
        content_b64 = base64.b64encode(json.dumps(cfg, indent=2).encode()).decode()
        sha_snapshot = GH_SHA

    url = f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE}"
    headers = {"Authorization": f"token {GITHUB_PAT}"}
    payload = {
        "message": f"sync {datetime.now(IST).strftime('%H:%M:%S')}",
        "content": content_b64,
        "branch": GH_BRANCH,
    }
    if sha_snapshot:
        payload["sha"] = sha_snapshot

    try:
        r = requests.put(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT)
        if r.status_code in (200, 201):
            with CONFIG_LOCK:
                GH_SHA = r.json()["content"]["sha"]
        elif r.status_code == 409:
            old_sha = GH_SHA
            gh_fetch_config()
            if GH_SHA != old_sha and GH_SHA is not None:
                gh_push_config(silent, retry + 1)
    except Exception as e:
        print(f"❌ Push err: {e}", flush=True)


def gh_sync_loop():
    while True:
        time.sleep(30)
        gh_push_config()


# ═══════════════════════════════════════════════════════════
# SESSION WORKER
# ═══════════════════════════════════════════════════════════
class SessionWorker:
    def __init__(self, data):
        self.data = data
        self.stop_event = threading.Event()
        self.msg_thread = None
        self.rename_thread = None
        self.rotate_thread = None
        self.health_thread = None
        self.listen_threads = []
        self._executor = None
        self.sockets = []
        self.socket_locks = []
        self.pool_size = SOCKET_POOL
        self._rr_idx = 0
        self._rr_lock = threading.Lock()
        self._inflight = 0
        self._inflight_lock = threading.Lock()

        if "device_id" not in self.data:
            seed = self.data.get("session_name", self.data["session_id"][:16])
            self.data["device_id"] = hashlib.md5(seed.encode(), usedforsecurity=False).hexdigest()
        self._device_id = self.data["device_id"]

        self.stats = {
            "sent": 0, "failed": 0, "renames": 0,
            "start_time": None, "status": "stopped",
            "last_error": None,
            "session_status": None,
            "session_error": None
        }
        self._stats_lock = threading.Lock()

    def build_message(self):
        num = random.randint(1000000000, 9999999999)
        emoji = random.choice(EMOJI_POOL)
        line = f"{self.data['msg_text']} !! {emoji} !!"
        body = ("\n" * 5).join([line] * random.randint(12, 18))
        return f"{num}\n\n{body}{FOOTER}"

    def build_gc_name(self):
        return f"{self.data['gc_name_text']} !! {GC_EMOJI} !!"

    def _make_factory_kwargs(self):
        base = {"sessionid": self.data["session_id"], "user_agent": self.data["ua"]}
        if "device_id" in _MWS_PARAMS:
            base["device_id"] = self._device_id
        elif "device" in _MWS_PARAMS:
            base["device"] = self._device_id
        elif "device_dict" in _MWS_PARAMS:
            base["device_dict"] = self._device_id
        return base

    def _connect(self, label, max_retries=3, retry_delay=2):
        err = None
        base_kwargs = self._make_factory_kwargs()
        for attempt in range(max_retries):
            for mode in ("dgw", "iris"):
                try:
                    try:
                        s = make_ig_web_socket(**base_kwargs)
                    except TypeError:
                        s = make_ig_web_socket(sessionid=self.data["session_id"], user_agent=self.data["ua"])
                    s.connect(dm_receive=mode)
                    lt = threading.Thread(target=self._listen_loop, args=(s,), daemon=True)
                    lt.start()
                    self.listen_threads.append(lt)
                    return s
                except Exception as e:
                    err = e
                    if "connack" in str(e).lower():
                        time.sleep(retry_delay)
                        continue
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        raise Exception(f"{label}: {err}")

    def _listen_loop(self, sock):
        try:
            sock.listen()
        except Exception:
            pass

    def _seed_pool(self):
        n = self.pool_size
        self.sockets = [None] * n
        self.socket_locks = [threading.Lock() for _ in range(n)]
        ok = 0
        last_err = None
        for i in range(n):
            try:
                s = self._connect(f"pool{i}", max_retries=2, retry_delay=2)
                with self.socket_locks[i]:
                    self.sockets[i] = s
                ok += 1
            except Exception as e:
                last_err = str(e)[:100]
            if i < n - 1:
                time.sleep(1.0)
        if ok == 0:
            raise Exception(last_err or "no sockets connected")
        return ok

    def start(self):
        with self._stats_lock:
            if self.stats["status"] in ("live", "starting", "stopping"):
                return False, "Already running/stopping"

        for old_t in (self.msg_thread, self.rename_thread, self.rotate_thread, self.health_thread):
            if old_t and old_t.is_alive():
                old_t.join(timeout=5)
                if old_t.is_alive():
                    return False, "Previous threads still running."
        for lt in list(self.listen_threads):
            if lt.is_alive():
                lt.join(timeout=5)
                if lt.is_alive():
                    return False, "Previous listener still running."

        with self._inflight_lock:
            if self._inflight > 0:
                return False, f"{self._inflight} task(s) still running."

        self.stop_event = threading.Event()
        self.listen_threads = []
        self._rr_idx = 0

        try:
            ok = self._seed_pool()
        except Exception as e:
            with self._stats_lock:
                self.stats["status"] = "error"
                self.stats["last_error"] = str(e)[:100]
            return False, str(e)[:100]

        with self._inflight_lock:
            self._inflight = 0

        with self._stats_lock:
            self.stats.update({
                "status": "live", "start_time": time.time(),
                "sent": 0, "failed": 0, "renames": 0,
                "last_error": None if ok == self.pool_size else f"only {ok}/{self.pool_size} sockets",
                "session_status": None, "session_error": None
            })

        self.msg_thread = threading.Thread(target=self._msg_loop, daemon=True)
        self.rename_thread = threading.Thread(target=self._rename_loop, daemon=True)
        self.rotate_thread = threading.Thread(target=self._ws_rotate_loop, daemon=True)
        self.health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.msg_thread.start()
        self.rename_thread.start()
        self.rotate_thread.start()
        self.health_thread.start()
        return True, None

    def _msg_loop(self):
        self._executor = ThreadPoolExecutor(max_workers=16)
        next_fire = time.monotonic()
        try:
            while not self.stop_event.is_set():
                d_min = max(0.01, float(self.data.get("delay_min", 0.1)))
                d_max = max(d_min, float(self.data.get("delay_max", 0.2)))
                interval = random.uniform(d_min, d_max)

                live = sum(1 for s in self.sockets if s is not None)
                max_inflight = max(2, live * 4) if live > 0 else 1

                now = time.monotonic()
                max_debt = max(1.5, interval * 20)
                if next_fire < now - max_debt:
                    next_fire = now

                wait = next_fire - now
                if wait > 0 and self.stop_event.wait(wait):
                    break

                with self._inflight_lock:
                    if self._inflight >= max_inflight:
                        next_fire += interval
                        if self.stop_event.wait(min(0.05, interval)):
                            break
                        continue
                    self._inflight += 1

                try:
                    self._executor.submit(self._fire_message_rr)
                except RuntimeError:
                    with self._inflight_lock:
                        self._inflight -= 1
                    break

                next_fire += interval
        finally:
            try:
                self._executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self._executor = None

    def _is_ok(self, res):
        if isinstance(res, dict):
            if res.get("ok") is True:
                return True
            if "response" in res and isinstance(res["response"], dict):
                d = res["response"].get("data", {})
                if d and any("message_id" in str(v) for v in d.values()):
                    return True
        return False

    def _fire_message_rr(self):
        try:
            if self.stop_event.is_set():
                return
            with self._rr_lock:
                n = len(self.sockets)
                if n == 0 or len(self.socket_locks) < n:
                    return
                idx = self._rr_idx % n
                self._rr_idx = (self._rr_idx + 1) % n

            if not any(self.sockets[i] is not None for i in range(n)):
                with self._stats_lock:
                    self.stats["failed"] += 1
                return

            msg = self.build_message()
            for offset in range(n):
                i = (idx + offset) % n
                try:
                    with self.socket_locks[i]:
                        sock = self.sockets[i]
                        if sock is None:
                            continue
                        res = sock.send_message(self.data["thread_id"], {"text": msg})

                    if self._is_ok(res):
                        with self._stats_lock:
                            self.stats["sent"] += 1
                    else:
                        with self._stats_lock:
                            self.stats["failed"] += 1
                            self.stats["last_error"] = str(res)[:100]
                    return
                except Exception as e:
                    err_str = str(e).lower()
                    with self._stats_lock:
                        self.stats["last_error"] = f"send@{i}: {err_str[:80]}"
                    if "timeout" in err_str or "timed out" in err_str:
                        with self._stats_lock:
                            self.stats["failed"] += 1
                        return
                    continue

            with self._stats_lock:
                self.stats["failed"] += 1
        finally:
            with self._inflight_lock:
                self._inflight -= 1

    def _rotate_socket_at(self, idx):
        with self.socket_locks[idx]:
            old = self.sockets[idx]
            self.sockets[idx] = None
        if old:
            try:
                old.disconnect()
            except Exception:
                pass
        backoff = 5
        while not self.stop_event.is_set():
            try:
                new = self._connect(f"rot{idx}", max_retries=2, retry_delay=2)
                with self.socket_locks[idx]:
                    self.sockets[idx] = new
                return
            except Exception as e:
                with self._stats_lock:
                    self.stats["last_error"] = f"rot{idx}: {str(e)[:80]}"
                if self.stop_event.wait(backoff):
                    return
                backoff = min(backoff * 2, 60)

    def _ws_rotate_loop(self):
        n = len(self.sockets)
        if n == 0:
            return
        base = time.monotonic() + random.uniform(WS_ROTATE_MIN, WS_ROTATE_MAX)
        next_rotate = [base + i * random.uniform(300, 900) + random.uniform(0, 120) for i in range(n)]
        while not self.stop_event.is_set():
            now = time.monotonic()
            for i in range(n):
                if self.stop_event.is_set():
                    break
                if now >= next_rotate[i]:
                    self._rotate_socket_at(i)
                    next_rotate[i] = time.monotonic() + random.uniform(WS_ROTATE_MIN, WS_ROTATE_MAX)
            if self.stop_event.wait(5):
                break

    def _health_check_loop(self):
        while not self.stop_event.is_set():
            if self.stop_event.wait(random.uniform(HEALTH_CHECK_MIN, HEALTH_CHECK_MAX)):
                break
            sock = next((s for s in self.sockets if s is not None), None)
            if sock is None:
                with self._stats_lock:
                    self.stats["session_status"] = "error"
                    self.stats["session_error"] = "no live socket"
                continue
            try:
                health = sock.session_health()
                web = health.get("web")
                logged_out = health.get("logged_out", False)
                with self._stats_lock:
                    if logged_out:
                        self.stats["session_status"] = "logged_out"
                        self.stats["session_error"] = "logged_out=True"
                    elif web == "walled":
                        self.stats["session_status"] = "walled"
                        self.stats["session_error"] = "web=walled"
                    elif web == "ok":
                        self.stats["session_status"] = "ok"
                        self.stats["session_error"] = None
                    else:
                        self.stats["session_status"] = "error"
                        self.stats["session_error"] = f"unknown health: {health}"
            except Exception as e:
                with self._stats_lock:
                    self.stats["session_status"] = "error"
                    self.stats["session_error"] = str(e)[:100]

    def _rename_loop(self):
        while not self.stop_event.is_set():
            r_min = max(1.0, float(self.data.get("rename_min", 5)))
            r_max = max(r_min, float(self.data.get("rename_max", 15)))
            if self.stop_event.wait(random.uniform(r_min, r_max)):
                break
            idx = next((i for i, s in enumerate(self.sockets) if s is not None), None)
            if idx is None:
                continue
            try:
                with self.socket_locks[idx]:
                    sock = self.sockets[idx]
                    if sock is None:
                        continue
                    sock.rename_group(self.data["thread_id"], self.build_gc_name())
                with self._stats_lock:
                    self.stats["renames"] += 1
            except Exception as e:
                with self._stats_lock:
                    self.stats["last_error"] = f"rename: {str(e)[:80]}"

    def stop(self):
        if self.stats.get("status") == "stopped":
            return
        self.stop_event.set()
        with self._stats_lock:
            self.stats["status"] = "stopping"
        for i in range(len(self.sockets)):
            s = self.sockets[i]
            self.sockets[i] = None
            if s:
                try:
                    s.disconnect()
                except Exception:
                    pass
        for t in (self.msg_thread, self.rename_thread, self.rotate_thread, self.health_thread):
            if t and t.is_alive():
                t.join(timeout=3)
        for lt in self.listen_threads:
            if lt.is_alive():
                lt.join(timeout=3)
        self.listen_threads = []
        for _ in range(50):
            with self._inflight_lock:
                if self._inflight == 0:
                    break
            time.sleep(0.1)
        with self._stats_lock:
            self.stats["status"] = "stopped"

    def get_stats(self):
        with self._stats_lock:
            s = dict(self.stats)
        s["uptime"] = int(time.time() - s["start_time"]) if (s["start_time"] and s["status"] == "live") else 0
        s["sockets_live"] = sum(1 for x in self.sockets if x is not None) if self.sockets else 0
        s["sockets_total"] = len(self.sockets)
        return s


# ═══════════════════════════════════════════════════════════
# FASTAPI LIFESPAN & AUTH
# ═══════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    saved = await asyncio.to_thread(gh_fetch_config)
    with SESSIONS_LOCK:
        for _, sdata in saved.items():
            if "_uid" not in sdata:
                sdata["_uid"] = uuid.uuid4().hex[:12]
            if "device_id" not in sdata:
                seed = sdata.get("session_name", sdata["session_id"][:16])
                sdata["device_id"] = hashlib.md5(seed.encode(), usedforsecurity=False).hexdigest()
            SESSIONS[sdata["_uid"]] = SessionWorker(sdata)

    try:
        psutil.cpu_percent(interval=0.1)
    except Exception:
        pass

    threading.Thread(target=gh_sync_loop, daemon=True).start()

    with SESSIONS_LOCK:
        to_start = [w for w in SESSIONS.values() if w.data.get("auto_start", False)]
    for w in to_start:
        threading.Thread(target=w.start, daemon=True).start()
        await asyncio.sleep(2)

    yield

    with SESSIONS_LOCK:
        workers = list(SESSIONS.values())
    for w in workers:
        await asyncio.to_thread(w.stop)


app = FastAPI(title="IG Multi-Sender Web", lifespan=lifespan)


def verify_key(x_admin_key: str = Header(default="")):
    if ADMIN_KEY and x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Invalid Admin Key")


class SessionCreate(BaseModel):
    session_name: str
    session_id: str
    gc_name_text: str
    msg_text: str
    thread_id: str
    delay_min: float = Field(default=0.1, ge=0.01)
    delay_max: float = Field(default=0.2, ge=0.01)
    rename_min: float = Field(default=5.0, ge=1.0)
    rename_max: float = Field(default=15.0, ge=1.0)
    validate_first: bool = True


class DelayAdjust(BaseModel):
    delay_min: float | None = None
    delay_max: float | None = None
    rename_min: float | None = None
    rename_max: float | None = None


def _validate_ig_session(session_id: str, ua: str):
    err_msg = ""
    for attempt in range(2):
        for mode in ("dgw", "iris"):
            try:
                s = make_ig_web_socket(sessionid=session_id, user_agent=ua)
                s.connect(dm_receive=mode)
                try:
                    s.disconnect()
                except Exception:
                    pass
                return True, ""
            except Exception as e:
                err_msg = str(e)[:120]
        if attempt == 0:
            time.sleep(2)
    return False, err_msg


# ═══════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.get("/health")
async def health_check():
    return {"ok": True}


@app.get("/api/state", dependencies=[Depends(verify_key)])
async def get_state():
    with SESSIONS_LOCK:
        items = []
        for uid, w in SESSIONS.items():
            d = dict(w.data)
            d.pop("session_id", None)
            items.append({"uid": uid, "config": d, "stats": w.get_stats()})
    return {"sys": get_sys_stats(), "sessions": items}


@app.post("/api/sessions", dependencies=[Depends(verify_key)])
async def create_session(body: SessionCreate):
    if not body.thread_id.isdigit():
        raise HTTPException(status_code=400, detail="thread_id must be numeric")
    if body.delay_max < body.delay_min or body.rename_max < body.rename_min:
        raise HTTPException(status_code=400, detail="max delay must be >= min delay")

    with SESSIONS_LOCK:
        dup = next((w for w in SESSIONS.values()
                    if w.data.get("session_id") == body.session_id and w.data.get("thread_id") == body.thread_id), None)
        if dup:
            raise HTTPException(status_code=409, detail="Duplicate session_id + thread_id")
        existing = next((w for w in SESSIONS.values() if w.data.get("session_id") == body.session_id), None)

    if existing:
        ua = existing.data.get("ua") or random.choice(UA_POOL)
        device_id = existing.data.get("device_id", uuid.uuid4().hex)
    else:
        ua = random.choice(UA_POOL)
        device_id = uuid.uuid4().hex
        if body.validate_first:
            ok, err = await asyncio.to_thread(_validate_ig_session, body.session_id, ua)
            if not ok:
                raise HTTPException(status_code=400, detail=f"Session validation failed: {err}")

    uid = uuid.uuid4().hex[:12]
    data = {
        "_uid": uid,
        "owner_id": OWNER_ID,
        "session_name": body.session_name,
        "session_id": body.session_id,
        "gc_name_text": body.gc_name_text,
        "msg_text": body.msg_text,
        "thread_id": body.thread_id,
        "delay_min": body.delay_min,
        "delay_max": body.delay_max,
        "rename_min": body.rename_min,
        "rename_max": body.rename_max,
        "ua": ua,
        "device_id": device_id,
        "auto_start": False,
    }
    w = SessionWorker(data)
    with CONFIG_LOCK:
        with SESSIONS_LOCK:
            SESSIONS[uid] = w

    ok, err = await asyncio.to_thread(w.start)
    if ok:
        data["auto_start"] = True
    await asyncio.to_thread(gh_push_config)
    return {"uid": uid, "started": ok, "error": err}


@app.post("/api/sessions/{uid}/start", dependencies=[Depends(verify_key)])
async def start_session(uid: str):
    with SESSIONS_LOCK:
        w = SESSIONS.get(uid)
    if not w:
        raise HTTPException(status_code=404, detail="Session not found")
    ok, err = await asyncio.to_thread(w.start)
    if ok:
        w.data["auto_start"] = True
        await asyncio.to_thread(gh_push_config)
    return {"started": ok, "error": err}


@app.post("/api/sessions/{uid}/stop", dependencies=[Depends(verify_key)])
async def stop_session(uid: str):
    with SESSIONS_LOCK:
        w = SESSIONS.get(uid)
    if not w:
        raise HTTPException(status_code=404, detail="Session not found")
    await asyncio.to_thread(w.stop)
    w.data["auto_start"] = False
    await asyncio.to_thread(gh_push_config)
    return {"stopped": True}


@app.patch("/api/sessions/{uid}", dependencies=[Depends(verify_key)])
async def adjust_session(uid: str, body: DelayAdjust):
    with SESSIONS_LOCK:
        w = SESSIONS.get(uid)
    if not w:
        raise HTTPException(status_code=404, detail="Session not found")
    if body.delay_min is not None:
        w.data["delay_min"] = max(0.01, round(body.delay_min, 2))
    if body.delay_max is not None:
        w.data["delay_max"] = max(w.data["delay_min"], round(body.delay_max, 2))
    if body.rename_min is not None:
        w.data["rename_min"] = max(1.0, round(body.rename_min, 1))
    if body.rename_max is not None:
        w.data["rename_max"] = max(w.data["rename_min"], round(body.rename_max, 1))
    await asyncio.to_thread(gh_push_config)
    return {"ok": True}


@app.delete("/api/sessions/{uid}", dependencies=[Depends(verify_key)])
async def delete_session(uid: str):
    with SESSIONS_LOCK:
        w = SESSIONS.get(uid)
    if not w:
        raise HTTPException(status_code=404, detail="Session not found")
    await asyncio.to_thread(w.stop)
    with CONFIG_LOCK:
        with SESSIONS_LOCK:
            SESSIONS.pop(uid, None)
    await asyncio.to_thread(gh_push_config)
    return {"deleted": True}


# ═══════════════════════════════════════════════════════════
# WEB DASHBOARD UI
# ═══════════════════════════════════════════════════════════
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>IG Multi-Sender • Web Panel</title>
<style>
  :root { --bg:#0d1117; --card:#161b22; --border:#30363d; --text:#e6edf3; --muted:#8b949e; --accent:#2f81f7; --green:#3fb950; --red:#f85149; --yellow:#d29922; }
  * { box-sizing:border-box; font-family:system-ui,-apple-system,sans-serif; }
  body { margin:0; background:var(--bg); color:var(--text); padding:16px; }
  .wrap { max-width:960px; margin:0 auto; }
  header { display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px; margin-bottom:16px; }
  h1 { font-size:1.3rem; margin:0; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:14px; margin-bottom:14px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:10px; }
  input, button { padding:8px 10px; border-radius:6px; border:1px solid var(--border); background:#0d1117; color:var(--text); font-size:0.9rem; }
  button { cursor:pointer; font-weight:600; background:var(--accent); border:none; }
  button.sec { background:#21262d; border:1px solid var(--border); }
  button.danger { background:var(--red); }
  button.ok { background:var(--green); color:#000; }
  button:disabled { opacity:0.5; cursor:not-allowed; }
  .badge { padding:2px 8px; border-radius:999px; font-size:0.75rem; font-weight:700; text-transform:uppercase; }
  .live { background:#1b4721; color:var(--green); }
  .stopped { background:#491b1b; color:var(--red); }
  .error, .stopping { background:#4d3800; color:var(--yellow); }
  .row { display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px; }
  .muted { color:var(--muted); font-size:0.85rem; }
  .err { color:var(--red); font-size:0.8rem; margin-top:6px; word-break:break-all; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>🤖 IG Multi-Sender <span class="muted">By 𝐤ɴᴏ𝐱</span></h1>
      <div id="sysbar" class="muted">Loading system stats...</div>
    </div>
    <div style="display:flex;gap:8px;">
      <input id="adminKey" type="password" placeholder="Admin Key" />
      <button class="sec" onclick="saveKey()">Unlock</button>
      <button class="sec" onclick="refresh()">🔄</button>
    </div>
  </header>

  <div class="card">
    <h3 style="margin-top:0">➕ Add Session</h3>
    <form id="addForm" class="grid" onsubmit="addSession(event)">
      <input name="session_name" placeholder="Session Name" required />
      <input name="session_id" placeholder="Instagram Session ID" required />
      <input name="gc_name_text" placeholder="GC Name Text" required />
      <input name="msg_text" placeholder="Message Text" required />
      <input name="thread_id" placeholder="Thread ID (numbers)" required />
      <input name="delay_min" type="number" step="0.01" value="0.1" placeholder="Min Delay (s)" required />
      <input name="delay_max" type="number" step="0.01" value="0.2" placeholder="Max Delay (s)" required />
      <input name="rename_min" type="number" step="0.5" value="5" placeholder="Rename Min (s)" required />
      <input name="rename_max" type="number" step="0.5" value="15" placeholder="Rename Max (s)" required />
      <button type="submit" id="addBtn">Create & Start</button>
    </form>
  </div>

  <div id="sessions"></div>
</div>

<script>
const keyInput = document.getElementById('adminKey');
keyInput.value = localStorage.getItem('knox_key') || '';
function saveKey() { localStorage.setItem('knox_key', keyInput.value); refresh(); }

async function api(path, opts={}) {
  opts.headers = Object.assign({'Content-Type':'application/json', 'X-Admin-Key': keyInput.value}, opts.headers || {});
  const res = await fetch(path, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || ('HTTP ' + res.status));
  return data;
}

function fmtUp(s) {
  if (s < 60) return s + 's';
  if (s < 3600) return Math.floor(s/60) + 'm ' + (s%60) + 's';
  return Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm';
}

async function refresh() {
  try {
    const data = await api('/api/state');
    const sys = data.sys;
    document.getElementById('sysbar').textContent = sys
      ? `CPU: ${sys.cpu_pct}% (${sys.cores}c) • RAM: ${sys.ram_used_gb}/${sys.ram_total_gb}GB (${sys.ram_percent}%) • Process: ${sys.proc_ram_mb}MB`
      : 'System stats N/A';
    const box = document.getElementById('sessions');
    if (!data.sessions.length) {
      box.innerHTML = '<div class="card muted">No sessions yet.</div>';
      return;
    }
    box.innerHTML = data.sessions.map(item => {
      const c = item.config, st = item.stats;
      const isLive = st.status === 'live';
      return `<div class="card">
        <div class="row">
          <div>
            <strong>${esc(c.session_name)}</strong>
            <span class="badge ${st.status}">${st.status}</span>
            <span class="muted">• Thread: ${esc(c.thread_id)}</span>
          </div>
          <div style="display:flex;gap:6px;flex-wrap:wrap;">
            ${isLive
              ? `<button class="danger" onclick="act('${item.uid}','stop')">🛑 Stop</button>`
              : `<button class="ok" onclick="act('${item.uid}','start')">▶️ Start</button>`}
            <button class="sec" onclick="adj('${item.uid}', ${c.delay_min}, ${c.delay_max}, -0.1)">Msg -0.1</button>
            <button class="sec" onclick="adj('${item.uid}', ${c.delay_min}, ${c.delay_max}, 0.1)">Msg +0.1</button>
            <button class="danger" onclick="delSess('${item.uid}')">🗑</button>
          </div>
        </div>
        <div class="muted" style="margin-top:8px;">
          Sent: <b>${st.sent}</b> | Failed: <b>${st.failed}</b> | Renames: <b>${st.renames}</b> |
          Sockets: <b>${st.sockets_live}/${st.sockets_total}</b> | Uptime: <b>${fmtUp(st.uptime)}</b> |
          Delay: ${c.delay_min}-${c.delay_max}s | Health: ${st.session_status || 'pending'}
        </div>
        ${st.last_error ? `<div class="err">⚠️ ${esc(st.last_error)}</div>` : ''}
      </div>`;
    }).join('');
  } catch (e) {
    document.getElementById('sysbar').textContent = '⚠️ ' + e.message;
  }
}

function esc(s) { return String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

async function addSession(e) {
  e.preventDefault();
  const btn = document.getElementById('addBtn');
  btn.disabled = true; btn.textContent = 'Validating...';
  const fd = new FormData(e.target);
  const body = Object.fromEntries(fd.entries());
  ['delay_min','delay_max','rename_min','rename_max'].forEach(k => body[k] = parseFloat(body[k]));
  try {
    const r = await api('/api/sessions', {method:'POST', body:JSON.stringify(body)});
    if (!r.started && r.error) alert('Saved as stopped: ' + r.error);
    e.target.reset();
    refresh();
  } catch (err) { alert(err.message); }
  finally { btn.disabled = false; btn.textContent = 'Create & Start'; }
}

async function act(uid, op) {
  try { await api(`/api/sessions/${uid}/${op}`, {method:'POST'}); refresh(); }
  catch (e) { alert(e.message); }
}

async function adj(uid, dmin, dmax, delta) {
  const nmin = Math.max(0.01, +(dmin + delta).toFixed(2));
  const nmax = Math.max(nmin, +(dmax + delta).toFixed(2));
  try { await api(`/api/sessions/${uid}`, {method:'PATCH', body:JSON.stringify({delay_min:nmin, delay_max:nmax})}); refresh(); }
  catch (e) { alert(e.message); }
}

async function delSess(uid) {
  if (!confirm('Delete this session?')) return;
  try { await api(`/api/sessions/${uid}`, {method:'DELETE'}); refresh(); }
  catch (e) { alert(e.message); }
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return DASHBOARD_HTML
