#!/usr/bin/env python3
"""
PortPing VI — free, account-less Discord alerts for GTA VI PC port progress.

Data source: https://portwatch-vi.xyz/data/tracker.json (unofficial fan tracker).
Zero dependencies (Python 3.10+ stdlib). One process does everything:
  • HTTP server: landing page + JSON API (subscribe / unsubscribe / status)
  • Poller thread: fetches the feed, diffs it, broadcasts changes to every webhook

Env vars (all optional):
  PORT=8080                 HTTP port (Railway sets it)
  DATA_DIR=./data           where portping.db lives (mount a volume here in prod)
  SITE_URL=http://localhost:8080   public URL, shown in Discord footers
  CHECK_INTERVAL_MIN=10     how often to poll the feed
  MIN_SCORE_DELTA=0.1       minimum % change that counts as a change
  BROADCAST_WORKERS=5       parallel webhook senders
  AVATAR_URL=               optional png/jpg avatar for the webhook messages
"""
import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

FEED_URL = "https://portwatch-vi.xyz/data/tracker.json"
SOURCE_URL = "https://portwatch-vi.xyz"
HERE = Path(__file__).resolve().parent
STATIC = HERE / "static"

PORT = int(os.environ.get("PORT", "8080"))
DATA_DIR = Path(os.environ.get("DATA_DIR", HERE / "data"))
SITE_URL = os.environ.get("SITE_URL", f"http://localhost:{PORT}").rstrip("/")
INTERVAL = max(1, int(os.environ.get("CHECK_INTERVAL_MIN", "10"))) * 60
MIN_DELTA = float(os.environ.get("MIN_SCORE_DELTA", "0.1"))
WORKERS = max(1, int(os.environ.get("BROADCAST_WORKERS", "5")))
AVATAR_URL = os.environ.get("AVATAR_URL") or None
UA = "PortPingVI/1.0 (+%s)" % SITE_URL
EMBED_COLOR = 0xFF3E8A
MAX_FAILS = 20


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ═════════════════════════ database ═════════════════════════
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "portping.db"
_db_lock = threading.Lock()


def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def db_init():
    with _db_lock, db() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS subscribers(
            hook_id TEXT PRIMARY KEY, url TEXT NOT NULL, mode TEXT NOT NULL DEFAULT 'all',
            name TEXT, created_at TEXT, last_ok TEXT, fails INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, text TEXT, major INTEGER);
        """)


def kv_get(k, default=None):
    with db() as c:
        r = c.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return json.loads(r["v"]) if r else default


def kv_set(k, v):
    with _db_lock, db() as c:
        c.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                  (k, json.dumps(v, ensure_ascii=False)))


# ═════════════════════════ scoring (mirrors PortWatch scoring.js) ═════════════════════════
GW_WEIGHTS = {"loader": 0.20, "shaders": 0.28, "graphics": 0.32, "proven": 0.20}
LADDER = [("loads", "Exe runs", 10), ("boots", "First frame", 20), ("menu", "Main menu", 30),
          ("world", "In the world", 40), ("playable", "Playable", 50)]
STABILITY = {"unknown": 0, "constant": 0, "often": 0.25, "sometimes": 0.55, "rarely": 0.8, "stable": 1}


def num(v):
    if v is True:
        return 1.0
    if v is None or v is False:
        return 0.0
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return f if math.isfinite(f) else 0.0


def c01(x):
    return max(0.0, min(1.0, x))


def score_effort(e):
    g, gw = e.get("gta") or {}, e.get("groundwork") or {}
    s = 10 * sum(w * c01(num(gw.get(k))) for k, w in GW_WEIGHTS.items())
    milestone, prev, playable = ("Groundwork" if s > 0 else "—"), 0, False
    for key, label, th in LADDER:
        v = c01(num(g.get(key)))
        if v >= 1:
            s, prev, milestone, playable = max(s, th), th, label, key == "playable"
            continue
        if v > 0:
            s = max(s, prev + (th - prev) * v)
            milestone = f"{label} (partial)"
        break
    if playable:
        st = g.get("stability")
        stab = STABILITY.get(st.lower(), 0) if isinstance(st, str) else c01(num(st))
        q = (0.35 * c01(num(g.get("story"))) + 0.25 * c01(num(g.get("render")))
             + 0.20 * c01(num(g.get("fps")) / 30) + 0.20 * stab)
        s = 50 + q * 50
        if q >= 1:
            milestone = "Complete"
    ms = e.get("manualScore")
    if isinstance(ms, (int, float)) and not isinstance(ms, bool) and math.isfinite(ms):
        s = max(0.0, min(100.0, float(ms)))
    return round(s, 1), milestone


# ═════════════════════════ snapshot / diff ═════════════════════════
def _h(o):
    return hashlib.sha1(json.dumps(o, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:12]


def snapshot(data):
    efforts = {}
    for e in data.get("efforts") or []:
        eid = e.get("id") or e.get("name")
        if not eid:
            continue
        sc, ms = score_effort(e)
        g, nk = e.get("gta") or {}, e.get("nuked")
        efforts[eid] = {
            "name": e.get("name") or eid, "score": sc, "milestone": ms, "tier": e.get("tier"),
            "nuked": bool(nk), "nuked_note": nk.get("note") if isinstance(nk, dict) else None,
            "fps": g.get("fps"), "stability": g.get("stability"),
            "verification": e.get("verification"), "stars": (e.get("stats") or {}).get("stars"),
            "repo": e.get("repo"), "summary": e.get("summary"),
            "text_hash": _h([e.get("summary"), e.get("highlights"), e.get("blockers")]),
        }
    exe = data.get("exe") or {}
    news = {n["id"]: {"title": n.get("title"), "category": n.get("category"), "date": n.get("date")}
            for n in data.get("news") or [] if n.get("id")}
    return {"updated": data.get("updated"),
            "exe": {k: exe.get(k) for k in ("status", "expected", "releasedAt", "by", "note")},
            "efforts": efforts, "news": news}


def ranking(snap, limit=None):
    rows = [dict(e, id=k) for k, e in snap["efforts"].items() if not e["nuked"] and e["tier"] != "watchlist"]
    rows.sort(key=lambda e: (e["score"], e["stars"] or 0), reverse=True)
    return rows[:limit] if limit else rows


def headline(snap):
    r = ranking(snap)
    if not r:
        return 0.0, 0.0
    return r[0]["score"], round(sum(e["score"] for e in r[:3]) / len(r[:3]), 1)


def diff(old, new):
    """Returns list of (priority, text). Priority 0-2 = major, 3-4 = details."""
    out, oe, ne = [], old["efforts"], new["efforts"]
    if old["exe"] != new["exe"]:
        x = new["exe"]
        out.append((0, f"🚨 **Executable status:** `{old['exe'].get('status')}` → `{x.get('status')}`"
                       + (f" (expected {x['expected']})" if x.get("expected") else "")
                       + (f" — {x['note']}" if x.get("note") else "")))
    lo, ao = headline(old)
    ln, an = headline(new)
    if abs(ln - lo) >= MIN_DELTA:
        out.append((1, f"🏆 **Overall progress:** {lo}% → **{ln}%** · top-3 avg {ao}% → {an}%"))
    for eid, e in ne.items():
        o = oe.get(eid)
        if o is None:
            out.append((2, f"🆕 **New effort:** {e['name']} — {e['score']}% ({e['milestone']})"))
            continue
        if e["nuked"] and not o["nuked"]:
            out.append((1, f"💥 **{e['name']} got nuked**" + (f": {e['nuked_note']}" if e["nuked_note"] else "")))
        d = round(e["score"] - o["score"], 1)
        if abs(d) >= MIN_DELTA:
            out.append((2, f"{'📈' if d > 0 else '📉'} **{e['name']}:** {o['score']}% → **{e['score']}%** "
                           f"({'+' if d > 0 else ''}{d})"))
        if e["milestone"] != o["milestone"]:
            out.append((2, f"🏁 **{e['name']}** reached **{e['milestone']}** (was {o['milestone']})"))
        if e["fps"] != o["fps"]:
            out.append((3, f"🎞️ {e['name']}: FPS {o['fps']} → {e['fps']}"))
        if e["stability"] != o["stability"]:
            out.append((3, f"🧱 {e['name']}: stability {o['stability']} → {e['stability']}"))
        if e["tier"] != o["tier"]:
            out.append((3, f"🔀 {e['name']}: moved {o['tier']} → {e['tier']}"))
        if e["verification"] != o["verification"]:
            out.append((3, f"✅ {e['name']}: verification {o['verification']} → {e['verification']}"))
        if e["text_hash"] != o["text_hash"]:
            out.append((4, f"✏️ {e['name']}: notes updated — {(e['summary'] or '')[:180]}"))
    for eid, o in oe.items():
        if eid not in ne:
            out.append((1, f"❌ **{o['name']}** was removed from the tracker"))
    for nid, n in new["news"].items():
        if nid not in old["news"]:
            out.append((2, f"📰 **[{n['category']}]** {n['title']}"))
    out.sort(key=lambda t: t[0])
    return out


# ═════════════════════════ discord ═════════════════════════
HOOK_RE = re.compile(
    r"^https://(?:(?:ptb|canary)\.)?discord(?:app)?\.com/api(?:/v\d+)?/webhooks/(\d{15,22})/([A-Za-z0-9_-]{30,100})/?$")


def parse_hook(url):
    m = HOOK_RE.match((url or "").strip())
    if not m:
        return None
    return m.group(1), f"https://discord.com/api/webhooks/{m.group(1)}/{m.group(2)}"


def http_json(method, url, payload=None, timeout=20):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", "User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return r.status, (json.loads(body) if body else None), r.headers
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            j = json.loads(body) if body else None
        except ValueError:
            j = None
        return e.code, j, e.headers


def post_webhook(url, payload):
    """Returns HTTP status (handles 429 with up to 3 retries)."""
    for _ in range(4):
        status, body, headers = http_json("POST", url, payload)
        if status != 429:
            return status
        retry = (body or {}).get("retry_after") or float(headers.get("Retry-After") or 2)
        time.sleep(min(float(retry), 30) + 0.25)
    return 429


def base_payload(embeds):
    p = {"username": "PortPing VI", "embeds": embeds, "allowed_mentions": {"parse": []}}
    if AVATAR_URL:
        p["avatar_url"] = AVATAR_URL
    return p


def leaderboard_field(snap, n=5):
    rows = ranking(snap, n)
    if not rows:
        return None
    lines = [f"{i}. {e['name'][:18]:<18} {e['score']:>5}%" for i, e in enumerate(rows, 1)]
    return {"name": "Leaderboard", "value": "```\n" + "\n".join(lines) + "\n```", "inline": False}


def footer():
    return {"text": f"Unofficial · data from portwatch-vi.xyz · manage alerts at {SITE_URL}"}


def build_update_payloads(lines, snap):
    """Split lines into embeds ≤3800 chars; one embed per message keeps it simple."""
    chunks, cur = [], ""
    for ln in lines:
        if len(cur) + len(ln) + 1 > 3800:
            chunks.append(cur)
            cur = ""
        cur += ln + "\n"
    if cur:
        chunks.append(cur)
    urgent = lines and lines[0].startswith("🚨")
    payloads = []
    for i, ch in enumerate(chunks):
        emb = {"title": ("🚨 GTA VI on PC — executable update" if urgent else "GTA VI PC port update")
               + (f" ({i + 1}/{len(chunks)})" if len(chunks) > 1 else ""),
               "url": SOURCE_URL, "description": ch.strip(), "color": EMBED_COLOR,
               "timestamp": now_iso(), "footer": footer()}
        if i == len(chunks) - 1:
            f = leaderboard_field(snap)
            if f:
                emb["fields"] = [f]
        payloads.append(base_payload([emb]))
    return payloads


def mark(hook_id, status):
    with _db_lock, db() as c:
        if status in (200, 204):
            c.execute("UPDATE subscribers SET last_ok=?, fails=0 WHERE hook_id=?", (now_iso(), hook_id))
        elif status in (401, 403, 404):
            c.execute("DELETE FROM subscribers WHERE hook_id=?", (hook_id,))
            log(f"Removed dead webhook {hook_id} (HTTP {status})")
        else:
            c.execute("UPDATE subscribers SET fails=fails+1 WHERE hook_id=?", (hook_id,))
            c.execute("DELETE FROM subscribers WHERE hook_id=? AND fails>=?", (hook_id, MAX_FAILS))


def broadcast(changes, snap):
    major_lines = [t for p, t in changes if p <= 2]
    all_lines = [t for _, t in changes]
    with db() as c:
        subs = [dict(r) for r in c.execute("SELECT hook_id, url, mode FROM subscribers")]
    pay_all = build_update_payloads(all_lines, snap) if all_lines else []
    pay_major = build_update_payloads(major_lines, snap) if major_lines else []

    def send(sub):
        payloads = pay_major if sub["mode"] == "major" else pay_all
        status = 204
        for p in payloads:
            try:
                status = post_webhook(sub["url"], p)
            except Exception as e:  # noqa: BLE001
                log(f"send error {sub['hook_id']}: {e}")
                status = 0
            if status not in (200, 204):
                break
        if payloads:
            mark(sub["hook_id"], status)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(send, subs))
    log(f"Broadcast {len(all_lines)} change(s) to {len(subs)} webhook(s)")


# ═════════════════════════ poller ═════════════════════════
def fetch_feed(etag=None):
    req = urllib.request.Request(FEED_URL, headers={"User-Agent": UA, "Accept": "application/json"})
    if etag:
        req.add_header("If-None-Match", etag)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8")), r.headers.get("ETag")
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return None, etag
        raise


def check_once():
    data, etag = fetch_feed(kv_get("etag"))
    kv_set("last_check", now_iso())
    if data is None:
        return
    new = snapshot(data)
    old = kv_get("snapshot")
    if old is not None:
        changes = diff(old, new)
        if changes:
            with _db_lock, db() as c:
                ts = now_iso()
                c.executemany("INSERT INTO events(ts,text,major) VALUES(?,?,?)",
                              [(ts, t, 1 if p <= 2 else 0) for p, t in changes])
                c.execute("DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT 200)")
            kv_set("last_change", now_iso())
            kv_set("snapshot", new)
            kv_set("etag", etag)
            broadcast(changes, new)
            return
    else:
        log("Baseline snapshot saved (no broadcast on first run).")
    kv_set("snapshot", new)
    kv_set("etag", etag)


def poller():
    while True:
        try:
            check_once()
        except Exception as e:  # noqa: BLE001
            log(f"poll error: {e}")
        time.sleep(INTERVAL)


# ═════════════════════════ API ═════════════════════════
_rl = defaultdict(deque)
_rl_lock = threading.Lock()


def rate_ok(ip, limit=8, window=600):
    t = time.time()
    with _rl_lock:
        q = _rl[ip]
        while q and q[0] < t - window:
            q.popleft()
        if len(q) >= limit:
            return False
        q.append(t)
        return True


def api_subscribe(body):
    parsed = parse_hook(body.get("url"))
    if not parsed:
        return 400, {"ok": False, "error": "That doesn't look like a Discord webhook URL."}
    hook_id, url = parsed
    mode = "major" if body.get("mode") == "major" else "all"
    status, info, _ = http_json("GET", url)
    if status != 200 or not isinstance(info, dict):
        return 400, {"ok": False, "error": "Discord says this webhook doesn't exist (or was deleted)."}
    name = info.get("name") or "webhook"
    with db() as c:
        existed = c.execute("SELECT 1 FROM subscribers WHERE hook_id=?", (hook_id,)).fetchone() is not None
    snap = kv_get("snapshot")
    lead = headline(snap)[0] if snap else None
    desc = ("Settings updated." if existed else "You're subscribed. This channel will get a message whenever "
            "a GTA VI PC port moves: progress jumps, new milestones, takedowns, news.")
    desc += f"\n\n**Mode:** {'Big moves only' if mode == 'major' else 'Everything'}"
    if lead is not None:
        desc += f"\n**Current leader:** {lead}%"
    emb = {"title": "✅ PortPing VI is connected", "url": SITE_URL, "description": desc,
           "color": EMBED_COLOR, "footer": footer()}
    if snap and (f := leaderboard_field(snap)):
        emb["fields"] = [f]
    st = post_webhook(url, base_payload([emb]))
    if st not in (200, 204):
        return 400, {"ok": False, "error": f"Couldn't post to that webhook (HTTP {st})."}
    with _db_lock, db() as c:
        c.execute("""INSERT INTO subscribers(hook_id,url,mode,name,created_at,last_ok,fails)
                     VALUES(?,?,?,?,?,?,0) ON CONFLICT(hook_id) DO UPDATE
                     SET url=excluded.url, mode=excluded.mode, name=excluded.name, fails=0""",
                  (hook_id, url, mode, name, now_iso(), now_iso()))
    return 200, {"ok": True, "updated": existed, "name": name, "mode": mode}


def api_unsubscribe(body):
    parsed = parse_hook(body.get("url"))
    if not parsed:
        return 400, {"ok": False, "error": "That doesn't look like a Discord webhook URL."}
    hook_id, url = parsed
    with _db_lock, db() as c:
        removed = c.execute("DELETE FROM subscribers WHERE hook_id=?", (hook_id,)).rowcount > 0
    if removed:
        try:
            post_webhook(url, base_payload([{"title": "👋 Unsubscribed from PortPing VI",
                                             "description": "No more alerts will be sent here.",
                                             "color": 0x888888, "footer": footer()}]))
        except Exception:  # noqa: BLE001
            pass
    return 200, {"ok": True, "removed": removed}


def api_status():
    snap = kv_get("snapshot")
    with db() as c:
        subs = c.execute("SELECT COUNT(*) n FROM subscribers").fetchone()["n"]
        events = [dict(r) for r in c.execute("SELECT ts,text,major FROM events ORDER BY id DESC LIMIT 15")]
    out = {"subscribers": subs, "last_check": kv_get("last_check"), "last_change": kv_get("last_change"),
           "interval_min": INTERVAL // 60, "events": events, "source": SOURCE_URL}
    if snap:
        lead, avg = headline(snap)
        rows = ranking(snap)
        out.update({
            "feed_updated": snap.get("updated"), "leader": lead, "top3": avg,
            "tracked": len(rows), "exe": snap.get("exe"),
            "furthest": rows[0]["milestone"] if rows else None,
            "board": [{k: e[k] for k in ("name", "score", "milestone", "fps", "stars", "verification", "repo")}
                      for e in rows[:10]],
        })
    return 200, out


class Handler(BaseHTTPRequestHandler):
    server_version = "PortPing"
    sys_version = ""

    def log_message(self, fmt, *args):
        pass

    def _ip(self):
        return (self.headers.get("X-Forwarded-For") or self.client_address[0]).split(",")[0].strip()

    def _send(self, code, body, ctype="application/json; charset=utf-8", cache="no-store"):
        raw = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", cache)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            return self._send(200, (STATIC / "index.html").read_bytes(), "text/html; charset=utf-8", "no-cache")
        if path == "/favicon.svg":
            return self._send(200, (STATIC / "favicon.svg").read_bytes(), "image/svg+xml", "max-age=86400")
        if path == "/api/status":
            return self._send(*api_status())
        if path == "/healthz":
            return self._send(200, {"ok": True})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path not in ("/api/subscribe", "/api/unsubscribe"):
            return self._send(404, {"error": "not found"})
        n = int(self.headers.get("Content-Length") or 0)
        if n > 4096:
            return self._send(413, {"ok": False, "error": "Too large."})
        if not rate_ok(self._ip()):
            return self._send(429, {"ok": False, "error": "Slow down a bit and try again in a few minutes."})
        try:
            body = json.loads(self.rfile.read(n) or b"{}")
            assert isinstance(body, dict)
        except Exception:  # noqa: BLE001
            return self._send(400, {"ok": False, "error": "Bad request."})
        try:
            fn = api_subscribe if path == "/api/subscribe" else api_unsubscribe
            self._send(*fn(body))
        except Exception as e:  # noqa: BLE001
            log(f"api error: {e}")
            self._send(500, {"ok": False, "error": "Something broke on our side. Try again."})


def main():
    db_init()
    threading.Thread(target=poller, daemon=True).start()
    log(f"PortPing VI on :{PORT}  (site {SITE_URL}, poll every {INTERVAL // 60} min, db {DB_PATH})")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
