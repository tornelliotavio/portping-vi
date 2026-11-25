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
  AVATAR_URL=               png/jpg avatar for the webhook messages (default: SITE_URL/avatar.png)
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

from i18n import LANGS, STRINGS, norm_lang

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
AVATAR_URL = os.environ.get("AVATAR_URL") or (f"{SITE_URL}/avatar.png" if SITE_URL.startswith("https://") else None)
UA = "PortPingVI/1.0 (+%s)" % SITE_URL
EMBED_COLOR = 0xFF3E8A
URGENT_COLOR = 0xFF8A3D
BYE_COLOR = 0x4E5058
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
            name TEXT, created_at TEXT, last_ok TEXT, fails INTEGER NOT NULL DEFAULT 0,
            lang TEXT NOT NULL DEFAULT 'en');
        CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, text TEXT, major INTEGER);
        """)
        if "lang" not in {r[1] for r in c.execute("PRAGMA table_info(subscribers)")}:
            c.execute("ALTER TABLE subscribers ADD COLUMN lang TEXT NOT NULL DEFAULT 'en'")


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


class Pct(float):
    """A percentage; formatted per language (78.7% / 78,7%)."""


class Delta(float):
    """A signed change; formatted per language (+0.8 / +0,8)."""


class Ms(str):
    """A milestone label from score_effort(); translated per language."""


class Name(str):
    """Free text from the feed (names, titles); markdown-escaped for Discord."""


def _name(v):
    return Name(v) if v else None


def diff(old, new):
    """Returns list of (priority, key, params). Priority 0-2 = major, 3-4 = details.
    Rendered per language by render_event(); 'extra' is untranslated feed text appended after a dash."""
    out, oe, ne = [], old["efforts"], new["efforts"]
    if old["exe"] != new["exe"]:
        x = new["exe"]
        out.append((0, "ev_exe", {"old": old["exe"].get("status"), "new": x.get("status"),
                                  "date": x.get("expected"), "extra": x.get("note")}))
    lo, ao = headline(old)
    ln, an = headline(new)
    if abs(ln - lo) >= MIN_DELTA:
        out.append((1, "ev_overall", {"old": Pct(lo), "new": Pct(ln), "aold": Pct(ao), "anew": Pct(an)}))
    for eid, e in ne.items():
        o, name = oe.get(eid), Name(e["name"])
        if o is None:
            out.append((2, "ev_new", {"name": name, "score": Pct(e["score"]), "ms": Ms(e["milestone"])}))
            continue
        if e["nuked"] and not o["nuked"]:
            out.append((1, "ev_nuked", {"name": name, "extra": e["nuked_note"]}))
        d = round(e["score"] - o["score"], 1)
        if abs(d) >= MIN_DELTA:
            out.append((2, "ev_score", {"icon": "📈" if d > 0 else "📉", "name": name,
                                        "old": Pct(o["score"]), "new": Pct(e["score"]), "delta": Delta(d)}))
        if e["milestone"] != o["milestone"]:
            out.append((2, "ev_milestone", {"name": name, "ms_new": Ms(e["milestone"]), "ms_old": Ms(o["milestone"])}))
        for field, key, prio in (("fps", "ev_fps", 3), ("stability", "ev_stability", 3),
                                 ("tier", "ev_tier", 3), ("verification", "ev_verification", 3)):
            if e[field] != o[field]:
                out.append((prio, key, {"name": name, "old": o[field], "new": e[field]}))
        if e["text_hash"] != o["text_hash"]:
            out.append((4, "ev_notes", {"name": name, "extra": e["summary"]}))
    for eid, o in oe.items():
        if eid not in ne:
            out.append((1, "ev_removed", {"name": Name(o["name"])}))
    for nid, n in new["news"].items():
        if nid not in old["news"]:
            out.append((2, "ev_news", {"cat": _name(n["category"]), "title": _name(n["title"])}))
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


_MD = re.compile(r"([\\*_~`|>\[\]])")


def md_escape(text):
    return _MD.sub(r"\\\1", str(text))


def T(lang, key, **kw):
    s = STRINGS.get(lang, {}).get(key) or STRINGS["en"].get(key, key)
    return s.format(**kw) if kw else s


def tr_ms(ms, lang):
    """Translate a milestone label like 'Main menu' or 'Main menu (partial)'."""
    ms = str(ms or "—")
    partial = ms.endswith(" (partial)")
    base = ms[:-10] if partial else ms
    key = "ms_" + base
    out = STRINGS.get(lang, {}).get(key) or STRINGS["en"].get(key) or base
    return f"{out} ({T(lang, 'partial')})" if partial else out


def fmt(v, lang, discord=True):
    if v is None or v == "":
        return "—"
    if isinstance(v, (Pct, Delta)):
        s = f"{v:+.1f}" if isinstance(v, Delta) else f"{v:.1f}%"
        return s.replace(".", ",") if LANGS.get(lang, ("", False))[1] else s
    if isinstance(v, Ms):
        return tr_ms(v, lang)
    if isinstance(v, Name) and discord:
        return md_escape(v)
    return str(v)


def render_event(ev, lang, discord=True):
    _, key, params = ev
    kw = {k: fmt(v, lang, discord) for k, v in params.items() if k not in ("extra", "date")}
    text = T(lang, key, **kw)
    if key == "ev_exe" and params.get("date"):
        text += T(lang, "ev_exe_expected", date=params["date"])
    extra = params.get("extra")
    if extra:
        extra = str(extra).strip()
        extra = extra[:180] + ("…" if len(extra) > 180 else "")
        text += " — " + (md_escape(extra) if discord else extra)
    return text


def base_payload(embeds):
    p = {"username": "PortPing VI", "embeds": embeds, "allowed_mentions": {"parse": []}}
    if AVATAR_URL:
        p["avatar_url"] = AVATAR_URL
    return p


def footer(lang):
    f = {"text": T(lang, "footer")}
    if AVATAR_URL:
        f["icon_url"] = AVATAR_URL
    return f


MEDALS = ("🥇", "🥈", "🥉")


def top3_field(snap, lang):
    rows = ranking(snap, 3) if snap else []
    if not rows:
        return None
    lines = [f"{MEDALS[i]} **{md_escape(e['name'][:32])}** · `{fmt(Pct(e['score']), lang)}` · {tr_ms(e['milestone'], lang)}"
             for i, e in enumerate(rows)]
    return {"name": T(lang, "top3"), "value": "\n".join(lines), "inline": False}


def build_update_payloads(events, snap, lang):
    """One embed per message; long updates are split into ≤3800-char chunks."""
    lines = [render_event(ev, lang) for ev in events]
    chunks, cur = [], ""
    for ln in lines:
        if cur and len(cur) + len(ln) + 1 > 3800:
            chunks.append(cur)
            cur = ""
        cur += ln + "\n"
    if cur:
        chunks.append(cur)
    urgent = bool(events) and events[0][1] == "ev_exe"
    links = T(lang, "links", src=SOURCE_URL, site=SITE_URL)
    payloads = []
    for i, ch in enumerate(chunks):
        last = i == len(chunks) - 1
        emb = {"title": T(lang, "title_exe" if urgent else "title_update")
               + (f" ({i + 1}/{len(chunks)})" if len(chunks) > 1 else ""),
               "description": ch.strip() + ("\n\n" + links if last else ""),
               "color": URGENT_COLOR if urgent else EMBED_COLOR,
               "footer": footer(lang), "timestamp": now_iso()}
        if last and (f := top3_field(snap, lang)):
            emb["fields"] = [f]
        payloads.append(base_payload([emb]))
    return payloads


def hello_payload(lang, mode, existed, snap):
    emb = {"title": T(lang, "again_title" if existed else "hello_title"),
           "description": T(lang, "again_desc" if existed else "hello_desc") + "\n\n" + T(lang, "manage", site=SITE_URL),
           "color": EMBED_COLOR, "footer": footer(lang), "timestamp": now_iso(),
           "fields": [{"name": T(lang, "alerts"), "value": T(lang, "mode_major" if mode == "major" else "mode_all"),
                       "inline": True},
                      {"name": T(lang, "language"), "value": LANGS[lang][0], "inline": True}]}
    if f := top3_field(snap, lang):
        emb["fields"].append(f)
    return base_payload([emb])


def bye_payload(lang):
    return base_payload([{"title": T(lang, "bye_title"), "description": T(lang, "bye_desc", site=SITE_URL),
                          "color": BYE_COLOR, "footer": footer(lang), "timestamp": now_iso()}])


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


def broadcast(events, snap):
    with db() as c:
        subs = [dict(r) for r in c.execute("SELECT hook_id, url, mode, lang FROM subscribers")]
    major = [ev for ev in events if ev[0] <= 2]
    payloads = {}
    for sub in subs:
        key = (sub["lang"], sub["mode"])
        if key not in payloads:
            evs = major if sub["mode"] == "major" else events
            payloads[key] = build_update_payloads(evs, snap, sub["lang"]) if evs else []

    def send(sub):
        todo = payloads[(sub["lang"], sub["mode"])]
        status = 204
        for p in todo:
            try:
                status = post_webhook(sub["url"], p)
            except Exception as e:  # noqa: BLE001
                log(f"send error {sub['hook_id']}: {e}")
                status = 0
            if status not in (200, 204):
                break
        if todo:
            mark(sub["hook_id"], status)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(send, subs))
    log(f"Broadcast {len(events)} change(s) to {len(subs)} webhook(s)")


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
                              [(ts, render_event(ev, "en", discord=False), 1 if ev[0] <= 2 else 0) for ev in changes])
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
    lang = norm_lang(body.get("lang"))
    status, info, _ = http_json("GET", url)
    if status != 200 or not isinstance(info, dict):
        return 400, {"ok": False, "error": "Discord says this webhook doesn't exist (or was deleted)."}
    name = info.get("name") or "webhook"
    with db() as c:
        existed = c.execute("SELECT 1 FROM subscribers WHERE hook_id=?", (hook_id,)).fetchone() is not None
    st = post_webhook(url, hello_payload(lang, mode, existed, kv_get("snapshot")))
    if st not in (200, 204):
        return 400, {"ok": False, "error": f"Couldn't post to that webhook (HTTP {st})."}
    with _db_lock, db() as c:
        c.execute("""INSERT INTO subscribers(hook_id,url,mode,name,created_at,last_ok,fails,lang)
                     VALUES(?,?,?,?,?,?,0,?) ON CONFLICT(hook_id) DO UPDATE
                     SET url=excluded.url, mode=excluded.mode, name=excluded.name, fails=0, lang=excluded.lang""",
                  (hook_id, url, mode, name, now_iso(), now_iso(), lang))
    return 200, {"ok": True, "updated": existed, "name": name, "mode": mode, "lang": lang}


def api_unsubscribe(body):
    parsed = parse_hook(body.get("url"))
    if not parsed:
        return 400, {"ok": False, "error": "That doesn't look like a Discord webhook URL."}
    hook_id, url = parsed
    with _db_lock, db() as c:
        row = c.execute("SELECT lang FROM subscribers WHERE hook_id=?", (hook_id,)).fetchone()
        removed = c.execute("DELETE FROM subscribers WHERE hook_id=?", (hook_id,)).rowcount > 0
    if removed:
        try:
            post_webhook(url, bye_payload(row["lang"] if row else "en"))
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
        if path == "/avatar.png":
            return self._send(200, (STATIC / "avatar.png").read_bytes(), "image/png", "max-age=86400")
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
