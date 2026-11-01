'''
Design Requirement: Leaderboard

1. Requirement Analysis
1A Functional Requirements
- User should be able to upload data.
- User can query about top K list.
- Time period selection.
- Real-time updates.
 1B Non-Functional Requirements
- Scale: 1M req/sec
- CAP Theorem: Available >>>>> Consistent

2. Identifying Entities
- User
- Scores
- Time frame

3. API Design
3A. Upload Data : POST /v1/scores → Upload data
3B . Query : WS/GET /v1/leaderboard/{leaderboardID}/top?k={limit} → Pagination
3C. Real-Time : GET /v1/leaderboard/{leaderboardID}/rank/{userID}?window=k → Pagination

4. High level design 
user -> api gateway -> sever ( score and ranking) -> database ( customerDB , scoreDB )

5. low level design 
uploaded image

'''
import bisect
import hashlib
import logging
import queue
import random
import re
import sqlite3
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone

from flask import Flask, Response, g, jsonify, request
from werkzeug.serving import make_server

PORT = 8050
WINDOWS = ["hour", "day", "week", "month", "all"]
WINDOW_TTL = {"hour": 7200, "day": 172800, "week": 1209600, "month": 5356800, "all": None}
PARTITIONS = 3

BOARD_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,31}$")
USER_RE = re.compile(r"^[A-Za-z0-9_-]{3,32}$")

if "_PREV" in globals():
    try:
        _PREV["stop"].set()
        _PREV["server"].shutdown()
        _PREV["server"].server_close()
    except Exception:
        pass
STOP = threading.Event()


def bucket_for(window, ts):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    if window == "hour":
        return dt.strftime("%Y%m%d%H")
    if window == "day":
        return dt.strftime("%Y%m%d")
    if window == "week":
        year, week, _ = dt.isocalendar()
        return f"{year}W{week:02d}"
    if window == "month":
        return dt.strftime("%Y%m")
    return "all"


def key_for(board, window, ts):
    return f"lb:{board}:{window}:{bucket_for(window, ts)}"


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.c = {
            "requests": 0, "throttled": 0, "scores_accepted": 0,
            "duplicates": 0, "rank_queries": 0, "top_queries": 0, "fallbacks": 0,
        }
        self.version = 0
        self.recent = deque()

    def bump(self, name, n=1):
        with self.lock:
            self.c[name] = self.c.get(name, 0) + n

    def bump_version(self):
        with self.lock:
            self.version += 1
            return self.version

    def hit(self):
        now = time.time()
        with self.lock:
            self.c["requests"] += 1
            self.recent.append(now)
            while self.recent and self.recent[0] < now - 5:
                self.recent.popleft()

    def rps(self):
        now = time.time()
        with self.lock:
            while self.recent and self.recent[0] < now - 5:
                self.recent.popleft()
            return round(len(self.recent) / 5.0, 1)


M = Metrics()


class RedisSim:
    def __init__(self):
        self.lock = threading.RLock()
        self.z = {}
        self.kv = {}
        self.exp = {}
        self.ops = 0

    def _purge(self, key):
        e = self.exp.get(key)
        if e and e < time.time():
            self.z.pop(key, None)
            self.kv.pop(key, None)
            self.exp.pop(key, None)

    def zadd_max(self, key, member, score, ts, ttl=None):
        with self.lock:
            self.ops += 1
            self._purge(key)
            zset = self.z.setdefault(key, {})
            if ttl and key not in self.exp:
                self.exp[key] = time.time() + ttl
            old = zset.get(member)
            if old is None or score > old[0]:
                zset[member] = (score, ts)
                return True
            return False

    def _ordered(self, key):
        zset = self.z.get(key, {})
        return sorted(zset.items(), key=lambda kv: (-kv[1][0], kv[1][1], kv[0]))

    def zrank(self, key, member):
        with self.lock:
            self.ops += 1
            self._purge(key)
            zset = self.z.get(key, {})
            if member not in zset:
                return None
            s, t = zset[member]
            return 1 + sum(1 for m, (s2, t2) in zset.items()
                           if s2 > s or (s2 == s and (t2 < t or (t2 == t and m < member))))

    def zscore(self, key, member):
        with self.lock:
            v = self.z.get(key, {}).get(member)
            return v[0] if v else None

    def zcard(self, key):
        with self.lock:
            self._purge(key)
            return len(self.z.get(key, {}))

    def zslice(self, key, start, count):
        with self.lock:
            self.ops += 1
            self._purge(key)
            rows = self._ordered(key)[start:start + count]
            return [(m, v[0], v[1]) for m, v in rows]

    def setnx(self, key, ttl):
        with self.lock:
            self.ops += 1
            self._purge(key)
            if key in self.kv:
                return False
            self.kv[key] = 1
            self.exp[key] = time.time() + ttl
            return True

    def hset(self, key, value):
        with self.lock:
            self.kv[key] = value

    def hget(self, key):
        with self.lock:
            return self.kv.get(key)

    def stats(self):
        with self.lock:
            return {"keys": len(self.z), "members": sum(len(v) for v in self.z.values()), "ops": self.ops}


class KafkaSim:
    def __init__(self, partitions):
        self.n = partitions
        self.parts = [queue.Queue() for _ in range(partitions)]
        self.produced = [0] * partitions
        self.consumed = [0] * partitions
        self.paused = False
        self.lock = threading.Lock()

    def publish(self, key, value):
        p = int(hashlib.md5(key.encode()).hexdigest(), 16) % self.n
        with self.lock:
            offset = self.produced[p]
            self.produced[p] += 1
        self.parts[p].put(value)
        return p, offset

    def lag(self):
        return [self.produced[i] - self.consumed[i] for i in range(self.n)]

    def stats(self):
        lag = self.lag()
        return {
            "produced": sum(self.produced), "consumed": sum(self.consumed),
            "lag": sum(lag), "partitions": lag, "paused": self.paused,
        }


class CustomerDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.lock = threading.Lock()
        with self.lock:
            self.conn.executescript("""
                CREATE TABLE players (
                    user_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    country TEXT,
                    updated_at REAL
                );
                CREATE TABLE scores (
                    event_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    board_id TEXT NOT NULL,
                    score INTEGER NOT NULL,
                    ts REAL NOT NULL
                );
                CREATE INDEX idx_scores_board_ts ON scores(board_id, ts);
            """)

    def persist(self, ev):
        with self.lock:
            self.conn.execute(
                """INSERT INTO players(user_id, name, country, updated_at) VALUES (?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET name=excluded.name,
                   country=excluded.country, updated_at=excluded.updated_at""",
                (ev["user"], ev["name"], ev["country"], time.time()))
            self.conn.execute(
                "INSERT OR IGNORE INTO scores(event_id, user_id, board_id, score, ts) VALUES (?,?,?,?,?)",
                (ev["event_id"], ev["user"], ev["board"], ev["score"], ev["ts"]))
            self.conn.commit()

    def players(self, ids):
        if not ids:
            return {}
        marks = ",".join("?" * len(ids))
        with self.lock:
            rows = self.conn.execute(
                f"SELECT user_id, name, country FROM players WHERE user_id IN ({marks})", list(ids)).fetchall()
        return {r[0]: {"name": r[1], "country": r[2]} for r in rows}

    def stats(self):
        with self.lock:
            p = self.conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
            s = self.conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0]
        return {"players": p, "scores": s}


class SortedDB:
    def __init__(self):
        self.lock = threading.Lock()
        self.boards = {}

    def upsert(self, key, user, score, ts):
        with self.lock:
            b = self.boards.setdefault(key, {"best": {}, "list": []})
            old = b["best"].get(user)
            if old and old[0] >= score:
                return False
            if old:
                idx = bisect.bisect_left(b["list"], (-old[0], old[1], user))
                if idx < len(b["list"]) and b["list"][idx] == (-old[0], old[1], user):
                    b["list"].pop(idx)
            b["best"][user] = (score, ts)
            bisect.insort(b["list"], (-score, ts, user))
            return True

    def size(self, key):
        with self.lock:
            return len(self.boards.get(key, {"list": []})["list"])

    def top(self, key, offset, k):
        with self.lock:
            rows = self.boards.get(key, {"list": []})["list"][offset:offset + k]
            return [(u, -s, t) for s, t, u in rows]

    def rank(self, key, user):
        with self.lock:
            b = self.boards.get(key)
            if not b or user not in b["best"]:
                return None
            score, ts = b["best"][user]
            idx = bisect.bisect_left(b["list"], (-score, ts, user))
            return idx + 1, len(b["list"]), score

    def stats(self):
        with self.lock:
            return {"keys": len(self.boards), "entries": sum(len(b["list"]) for b in self.boards.values())}


redis = RedisSim()
kafka = KafkaSim(PARTITIONS)
customer_db = CustomerDB()
sorted_db = SortedDB()
BOARDS = {"space-raiders": "Space Raiders", "puzzle-quest": "Puzzle Quest", "speed-racer": "Speed Racer"}


def handle_event(ev):
    customer_db.persist(ev)
    for w in WINDOWS:
        sorted_db.upsert(key_for(ev["board"], w, ev["ts"]), ev["user"], ev["score"], ev["ts"])


def consumer_loop(p):
    while not STOP.is_set():
        if kafka.paused:
            time.sleep(0.1)
            continue
        try:
            ev = kafka.parts[p].get(timeout=0.1)
        except queue.Empty:
            continue
        try:
            handle_event(ev)
        except Exception as exc:
            print("consumer error:", exc)
        kafka.consumed[p] += 1


for _p in range(PARTITIONS):
    threading.Thread(target=consumer_loop, args=(_p,), daemon=True).start()


class ScoreService:
    def submit(self, board, user, name, country, score, ts=None, event_id=None):
        ts = ts or time.time()
        event_id = event_id or uuid.uuid4().hex
        if not redis.setnx(f"idem:{event_id}", 3600):
            M.bump("duplicates")
            return {"duplicate": True, "event_id": event_id}
        for w in WINDOWS:
            redis.zadd_max(key_for(board, w, ts), user, score, ts, WINDOW_TTL[w])
        redis.hset(f"profile:{user}", {"name": name, "country": country})
        ev = {"event_id": event_id, "board": board, "user": user, "name": name,
              "country": country, "score": score, "ts": ts}
        partition, offset = kafka.publish(user, ev)
        BOARDS.setdefault(board, board.replace("-", " ").title())
        M.bump("scores_accepted")
        M.bump_version()
        return {
            "duplicate": False, "event_id": event_id,
            "kafka": {"topic": "score-events", "partition": partition, "offset": offset},
            "rank": {
                "day": redis.zrank(key_for(board, "day", ts), user),
                "all": redis.zrank(key_for(board, "all", ts), user),
            },
        }


class RankingService:
    def _profiles(self, ids):
        found = customer_db.players(ids)
        for u in ids:
            if u not in found:
                cached = redis.hget(f"profile:{u}") or {"name": u, "country": ""}
                found[u] = cached
        return found

    def top(self, board, window, k, offset):
        key = key_for(board, window, time.time())
        source = "sorted_db"
        total = sorted_db.size(key)
        rows = sorted_db.top(key, offset, k)
        if redis.zcard(key) > total:
            rows = redis.zslice(key, offset, k)
            total = redis.zcard(key)
            source = "redis-fallback"
            M.bump("fallbacks")
        prof = self._profiles([r[0] for r in rows])
        entries = [{
            "rank": offset + i + 1, "user_id": u, "name": prof[u]["name"],
            "country": prof[u].get("country", ""), "score": s, "achieved_at": t,
        } for i, (u, s, t) in enumerate(rows)]
        M.bump("top_queries")
        return {
            "leaderboard_id": board, "window": window, "k": k, "offset": offset,
            "total": total, "next_offset": offset + k if offset + k < total else None,
            "source": source, "version": M.version, "entries": entries,
        }

    def rank(self, board, user, window, around=2):
        key = key_for(board, window, time.time())
        source = "sorted_db"
        info = sorted_db.rank(key, user)
        if info is None and redis.zscore(key, user) is not None:
            r = redis.zrank(key, user)
            info = (r, redis.zcard(key), redis.zscore(key, user))
            source = "redis-fallback"
            M.bump("fallbacks")
        if info is None:
            return None
        rank, total, score = info
        start = max(0, rank - 1 - around)
        if source == "sorted_db":
            rows = sorted_db.top(key, start, around * 2 + 1)
        else:
            rows = redis.zslice(key, start, around * 2 + 1)
        prof = self._profiles([r[0] for r in rows] + [user])
        neighbours = [{
            "rank": start + i + 1, "user_id": u, "name": prof[u]["name"], "score": s,
        } for i, (u, s, t) in enumerate(rows)]
        M.bump("rank_queries")
        return {
            "leaderboard_id": board, "user_id": user, "name": prof[user]["name"],
            "window": window, "rank": rank, "total": total, "score": score,
            "top_percent": round(rank / total * 100, 1), "source": source,
            "neighbours": neighbours,
        }


score_service = ScoreService()
ranking_service = RankingService()


app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.ERROR)
BUCKETS = {}
BUCKET_LOCK = threading.Lock()
RATE, BURST = 150.0, 300.0


def allow(ip):
    now = time.time()
    with BUCKET_LOCK:
        tokens, last = BUCKETS.get(ip, (BURST, now))
        tokens = min(BURST, tokens + (now - last) * RATE)
        if tokens < 1:
            BUCKETS[ip] = (tokens, now)
            return False
        BUCKETS[ip] = (tokens - 1, now)
        return True


@app.before_request
def gateway_in():
    g.t0 = time.time()
    g.rid = uuid.uuid4().hex[:10]
    if request.path.startswith("/v1"):
        M.hit()
        if not allow(request.remote_addr or "local"):
            M.bump("throttled")
            resp = jsonify({"error": "rate_limited", "message": "Too many requests"})
            resp.status_code = 429
            resp.headers["Retry-After"] = "1"
            return resp


@app.after_request
def gateway_out(resp):
    resp.headers["X-Request-Id"] = getattr(g, "rid", "-")
    resp.headers["X-Gateway"] = "api-gateway/1.0"
    resp.headers["Cache-Control"] = "no-store"
    return resp


def bad(msg, code=400):
    resp = jsonify({"error": "bad_request", "message": msg})
    resp.status_code = code
    return resp


@app.route("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.route("/v1/scores", methods=["POST"])
def post_score():
    body = request.get_json(silent=True) or {}
    board = str(body.get("leaderboard_id", "")).strip().lower()
    user = str(body.get("user_id", "")).strip()
    name = str(body.get("name", "")).strip()[:24] or user
    country = str(body.get("country", "")).strip().upper()[:2]
    if not BOARD_RE.match(board):
        return bad("leaderboard_id must be 2-32 chars: a-z, 0-9, _ or -")
    if not USER_RE.match(user):
        return bad("user_id must be 3-32 chars: letters, digits, _ or -")
    try:
        score = int(body.get("score"))
    except (TypeError, ValueError):
        return bad("score must be an integer")
    if not 0 <= score <= 1_000_000_000:
        return bad("score must be between 0 and 1,000,000,000")
    result = score_service.submit(board, user, name, country, score, event_id=body.get("event_id"))
    result["latency_ms"] = round((time.time() - g.t0) * 1000, 2)
    return jsonify(result), 202


@app.route("/v1/leaderboard/<board>/top")
def get_top(board):
    window = request.args.get("window", "day")
    if window not in WINDOWS:
        return bad(f"window must be one of {WINDOWS}")
    try:
        k = max(1, min(100, int(request.args.get("k", 10))))
        offset = max(0, int(request.args.get("offset", 0)))
    except ValueError:
        return bad("k and offset must be integers")
    data = ranking_service.top(board.lower(), window, k, offset)
    data["latency_ms"] = round((time.time() - g.t0) * 1000, 2)
    return jsonify(data)


@app.route("/v1/leaderboard/<board>/rank/<user>")
def get_rank(board, user):
    window = request.args.get("window", "day")
    if window not in WINDOWS:
        return bad(f"window must be one of {WINDOWS}")
    data = ranking_service.rank(board.lower(), user, window)
    if data is None:
        return bad("user has no score in this window", 404)
    data["latency_ms"] = round((time.time() - g.t0) * 1000, 2)
    return jsonify(data)


@app.route("/v1/leaderboards")
def list_boards():
    return jsonify([{"id": k, "title": v} for k, v in BOARDS.items()])


@app.route("/v1/system/stats")
def stats():
    return jsonify({
        "gateway": {"rps": M.rps(), "total": M.c["requests"], "throttled": M.c["throttled"]},
        "score": {"accepted": M.c["scores_accepted"], "duplicates": M.c["duplicates"]},
        "ranking": {"queries": M.c["top_queries"] + M.c["rank_queries"], "fallbacks": M.c["fallbacks"]},
        "kafka": kafka.stats(),
        "redis": redis.stats(),
        "sorted_db": sorted_db.stats(),
        "customer_db": customer_db.stats(),
        "version": M.version,
    })


FIRST = ["Nova", "Pixel", "Blaze", "Echo", "Rogue", "Viper", "Zen", "Hawk", "Frost", "Ninja",
         "Titan", "Ghost", "Comet", "Raven", "Storm", "Cobra", "Lunar", "Ember", "Drift", "Apex"]
LAST = ["Slayer", "Runner", "Master", "Wolf", "Fox", "Knight", "Rider", "Sniper", "Wizard", "Pilot"]
COUNTRIES = ["NP", "IN", "US", "JP", "BR", "DE", "GB", "KR", "FR", "AU"]


@app.route("/v1/system/simulate", methods=["POST"])
def simulate():
    body = request.get_json(silent=True) or {}
    board = str(body.get("leaderboard_id", "space-raiders")).lower()
    if not BOARD_RE.match(board):
        return bad("invalid leaderboard_id")
    players = max(1, min(2000, int(body.get("players", 50))))
    events = max(1, min(5000, int(body.get("events", 200))))
    rnd = random.Random()
    for _ in range(events):
        i = rnd.randrange(players)
        rnd2 = random.Random(i)
        uid = f"bot_{i:04d}"
        name = f"{rnd2.choice(FIRST)}{rnd2.choice(LAST)}{i % 100}"
        score_service.submit(board, uid, name, rnd2.choice(COUNTRIES), rnd.randint(100, 50000))
    return jsonify({"submitted": events, "players": players})


@app.route("/v1/system/chaos", methods=["POST"])
def chaos():
    body = request.get_json(silent=True) or {}
    kafka.paused = bool(body.get("kafka_paused", False))
    return jsonify({"kafka_paused": kafka.paused})


def seed():
    rnd = random.Random(7)
    now = time.time()
    for board in BOARDS:
        for i in range(30):
            uid = f"{board[:3]}_{i:03d}"
            name = f"{rnd.choice(FIRST)}{rnd.choice(LAST)}"
            country = rnd.choice(COUNTRIES)
            for _ in range(rnd.randint(1, 4)):
                age = rnd.choice([0, 600, 7200, 86400 * 2, 86400 * 10, 86400 * 40]) + rnd.randint(0, 3000)
                score_service.submit(board, uid, name, country, rnd.randint(500, 40000), ts=now - age)
    for _ in range(100):
        if sum(kafka.lag()) == 0:
            break
        time.sleep(0.05)


seed()

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Leaderboards</title>
    <style>
        :root {
            --bg: #f6f5f1;
            --ink: #1c1c1a;
            --muted: #6b6a63;
            --line: #d9d6cc;
            --accent: #1f5f4a;
            --mine: #fbf3c9;
            --up: #1f7a4a;
            --down: #a6342c;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
            background: var(--bg);
            color: var(--ink);
            line-height: 1.5;
            border-top: 4px solid var(--ink);
        }

        .page {
            max-width: 1040px;
            margin: 0 auto;
            padding: 28px 20px 48px;
        }

        header {
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 8px;
            padding-bottom: 14px;
            border-bottom: 1px solid var(--ink);
        }

        h1 {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 28px;
            font-weight: 700;
        }

        .updated {
            font-size: 13px;
            color: var(--muted);
            font-variant-numeric: tabular-nums;
        }

        .columns {
            display: grid;
            grid-template-columns: minmax(0, 1fr) 280px;
            gap: 40px;
            margin-top: 20px;
        }

        @media (max-width: 820px) {
            .columns {
                grid-template-columns: 1fr;
                gap: 28px;
            }
        }

        .toolbar {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 8px 18px;
            margin-bottom: 8px;
        }

        select,
        input {
            font: inherit;
            font-size: 14px;
            color: var(--ink);
            background: #fff;
            border: 1px solid var(--line);
            border-radius: 2px;
            padding: 7px 9px;
        }

        select:focus,
        input:focus,
        button:focus-visible {
            outline: 2px solid var(--accent);
            outline-offset: 1px;
        }

        .windows {
            display: flex;
            gap: 16px;
        }

        .windows button {
            font: inherit;
            font-size: 14px;
            background: none;
            border: 0;
            border-bottom: 2px solid transparent;
            color: var(--muted);
            padding: 4px 0;
            cursor: pointer;
        }

        .windows button.active {
            color: var(--ink);
            font-weight: 600;
            border-bottom-color: var(--ink);
        }

        .link {
            font: inherit;
            font-size: 13px;
            background: none;
            border: 0;
            color: var(--accent);
            text-decoration: underline;
            cursor: pointer;
            padding: 0;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th {
            text-align: left;
            font-size: 12px;
            font-weight: 600;
            color: var(--muted);
            padding: 8px 8px 8px 0;
            border-bottom: 1px solid var(--ink);
        }

        td {
            padding: 9px 8px 9px 0;
            border-bottom: 1px solid var(--line);
            font-size: 15px;
        }

        th.num,
        td.num {
            text-align: right;
            padding-right: 0;
            font-variant-numeric: tabular-nums;
        }

        td.rank {
            width: 64px;
            font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
            font-size: 14px;
        }

        td.country {
            width: 72px;
            color: var(--muted);
        }

        tr.me td {
            background: var(--mine);
            font-weight: 600;
        }

        tr.moved td {
            animation: moved 1.6s ease-out;
        }

        @keyframes moved {
            from { background: var(--mine); }
            to   { background: transparent; }
        }

        .move {
            font-size: 11px;
            margin-left: 4px;
        }

        .move.up {
            color: var(--up);
        }

        .move.down {
            color: var(--down);
        }

        .empty td {
            color: var(--muted);
            padding: 28px 0;
        }

        .pager {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-top: 12px;
            font-size: 13px;
            color: var(--muted);
        }

        .pager button {
            font: inherit;
            font-size: 13px;
            background: none;
            border: 0;
            color: var(--accent);
            cursor: pointer;
            padding: 2px 0;
        }

        .pager button:disabled {
            color: var(--line);
            cursor: default;
        }

        aside section + section {
            margin-top: 32px;
        }

        h2 {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 18px;
            margin-bottom: 4px;
        }

        .hint {
            font-size: 13px;
            color: var(--muted);
            margin-bottom: 12px;
        }

        label {
            display: block;
            font-size: 13px;
            font-weight: 600;
            margin: 10px 0 3px;
        }

        aside input {
            width: 100%;
        }

        .pair {
            display: grid;
            grid-template-columns: 1fr 72px;
            gap: 10px;
        }

        .submit-row {
            display: flex;
            align-items: center;
            gap: 14px;
            margin-top: 14px;
        }

        button.primary {
            font: inherit;
            font-size: 14px;
            font-weight: 600;
            color: #fff;
            background: var(--accent);
            border: 0;
            border-radius: 2px;
            padding: 9px 16px;
            cursor: pointer;
        }

        button.primary:hover {
            background: #174a39;
        }

        .status {
            margin-top: 10px;
            font-size: 13px;
            min-height: 20px;
        }

        .status.error {
            color: var(--down);
        }

        .result {
            margin-top: 12px;
            font-size: 14px;
        }

        .result .where {
            font-size: 15px;
            font-weight: 600;
        }

        .result ol {
            list-style: none;
            margin-top: 8px;
            border-top: 1px solid var(--line);
        }

        .result li {
            display: flex;
            justify-content: space-between;
            gap: 10px;
            padding: 5px 0;
            border-bottom: 1px solid var(--line);
            font-size: 13px;
            font-variant-numeric: tabular-nums;
        }

        .result li.self {
            font-weight: 700;
        }
    </style>
</head>
<body>

    <div class="page">

        <header>
            <h1>Leaderboards</h1>
            <span class="updated" id="updated">Loading...</span>
        </header>

        <div class="columns">

            <main>
                <div class="toolbar">
                    <select id="boardSel" aria-label="Leaderboard"></select>
                    <button class="link" id="newBoardBtn">New leaderboard</button>
                    <div class="windows" id="windows">
                        <button data-w="hour">Hour</button>
                        <button data-w="day" class="active">Day</button>
                        <button data-w="week">Week</button>
                        <button data-w="month">Month</button>
                        <button data-w="all">All time</button>
                    </div>
                    <select id="kSel" aria-label="Rows per page">
                        <option value="5">5 rows</option>
                        <option value="10" selected>10 rows</option>
                        <option value="25">25 rows</option>
                        <option value="50">50 rows</option>
                    </select>
                </div>

                <table>
                    <thead>
                        <tr>
                            <th>Rank</th>
                            <th>Player</th>
                            <th>Country</th>
                            <th class="num">Best score</th>
                        </tr>
                    </thead>
                    <tbody id="rows"></tbody>
                </table>

                <div class="pager">
                    <button id="prevBtn">Previous</button>
                    <span id="pageInfo"></span>
                    <button id="nextBtn">Next</button>
                </div>
            </main>

            <aside>
                <section>
                    <h2>Submit a score</h2>
                    <p class="hint">No account needed. Your best score in each period is the one that counts.</p>

                    <label for="uid">Player ID</label>
                    <input id="uid" maxlength="32" autocomplete="off">

                    <div class="pair">
                        <div>
                            <label for="uname">Name</label>
                            <input id="uname" maxlength="24" autocomplete="off">
                        </div>
                        <div>
                            <label for="ucountry">Country</label>
                            <input id="ucountry" maxlength="2" placeholder="NP" autocomplete="off">
                        </div>
                    </div>

                    <label for="uscore">Score</label>
                    <input id="uscore" type="number" min="0" inputmode="numeric">

                    <div class="submit-row">
                        <button class="primary" id="submitBtn">Submit score</button>
                        <button class="link" id="randBtn">Use a random score</button>
                    </div>
                    <div class="status" id="status" role="status"></div>
                </section>

                <section>
                    <h2>Look up a player</h2>
                    <p class="hint">Shows rank in the selected period and the players around it.</p>

                    <label for="rankUid">Player ID</label>
                    <input id="rankUid" autocomplete="off">

                    <div class="submit-row">
                        <button class="primary" id="rankBtn">Find rank</button>
                    </div>
                    <div class="result" id="rankResult"></div>
                </section>
            </aside>

        </div>
    </div>

    <script>
        const state = {
            board: "space-raiders",
            window: "day",
            k: 10,
            offset: 0,
            prevRanks: {},
            lastVersion: -1,
            me: { id: "", name: "" }
        };

        const WINDOW_LABEL = { hour: "this hour", day: "today", week: "this week", month: "this month", all: "all time" };

        const $ = (id) => document.getElementById(id);

        function esc(s) {
            return String(s).replace(/[&<>"']/g, (c) => ({
                "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
            }[c]));
        }

        function fmt(n) {
            return Number(n).toLocaleString();
        }

        function setStatus(msg, isError) {
            const el = $("status");
            el.textContent = msg;
            el.className = "status" + (isError ? " error" : "");
        }

        async function api(path, opts) {
            const res = await fetch(path, opts);
            const data = await res.json().catch(() => ({}));
            if (!res.ok) {
                throw new Error(data.message || ("Request failed (" + res.status + ")"));
            }
            return data;
        }

        function loadGuest() {
            try {
                const raw = localStorage.getItem("leaderboard_guest");
                if (raw) { state.me = JSON.parse(raw); }
            } catch (e) {
                state.me = { id: "", name: "" };
            }
            if (!state.me.id) {
                state.me.id = "player_" + Math.floor(1000 + Math.random() * 9000);
                state.me.name = "Guest " + Math.floor(100 + Math.random() * 900);
            }
            $("uid").value = state.me.id;
            $("uname").value = state.me.name;
            $("rankUid").value = state.me.id;
        }

        function saveGuest() {
            try {
                localStorage.setItem("leaderboard_guest", JSON.stringify(state.me));
            } catch (e) {
                return;
            }
        }

        async function loadBoards() {
            const boards = await api("/v1/leaderboards");
            const sel = $("boardSel");
            sel.innerHTML = boards.map((b) => `<option value="${esc(b.id)}">${esc(b.title)}</option>`).join("");
            if (!boards.find((b) => b.id === state.board) && boards.length) {
                state.board = boards[0].id;
            }
            sel.value = state.board;
        }

        function renderRows(data) {
            const body = $("rows");
            if (!data.entries.length) {
                body.innerHTML = `<tr class="empty"><td colspan="4">Nobody has a score ${WINDOW_LABEL[state.window]} yet. Submit one to start this board.</td></tr>`;
                return;
            }
            const changed = data.version !== state.lastVersion;
            const hadRanks = Object.keys(state.prevRanks).length > 0;
            body.innerHTML = data.entries.map((e) => {
                const prev = state.prevRanks[e.user_id];
                let move = "";
                if (prev && prev !== e.rank) {
                    move = prev > e.rank
                        ? `<span class="move up">&#9650; ${prev - e.rank}</span>`
                        : `<span class="move down">&#9660; ${e.rank - prev}</span>`;
                } else if (!prev && hadRanks) {
                    move = `<span class="move up">new</span>`;
                }
                const classes = [];
                if (e.user_id === state.me.id) { classes.push("me"); }
                if (changed && move) { classes.push("moved"); }
                return `
                    <tr class="${classes.join(" ")}">
                        <td class="rank">${e.rank}${move}</td>
                        <td>${esc(e.name)}</td>
                        <td class="country">${esc(e.country || "")}</td>
                        <td class="num">${fmt(e.score)}</td>
                    </tr>
                `;
            }).join("");
        }

        async function refreshTop() {
            try {
                const q = `window=${state.window}&k=${state.k}&offset=${state.offset}`;
                const data = await api(`/v1/leaderboard/${state.board}/top?${q}`);
                renderRows(data);

                state.prevRanks = {};
                data.entries.forEach((e) => { state.prevRanks[e.user_id] = e.rank; });
                state.lastVersion = data.version;

                const from = data.total ? data.offset + 1 : 0;
                const to = Math.min(data.offset + data.k, data.total);
                $("pageInfo").textContent = data.total ? `${from}\u2013${to} of ${fmt(data.total)}` : "";
                $("prevBtn").disabled = state.offset === 0;
                $("nextBtn").disabled = data.next_offset === null;
                $("updated").textContent = "Updated " + new Date().toLocaleTimeString();
            } catch (err) {
                $("updated").textContent = "Connection lost, retrying...";
            }
        }

        async function lookupRank(quiet) {
            const uid = $("rankUid").value.trim();
            const box = $("rankResult");
            if (!uid) {
                if (!quiet) { box.textContent = "Enter a player ID first."; }
                return;
            }
            try {
                const r = await api(`/v1/leaderboard/${state.board}/rank/${encodeURIComponent(uid)}?window=${state.window}`);
                box.innerHTML = `
                    <div class="where">${esc(r.name)}: rank ${fmt(r.rank)} of ${fmt(r.total)}</div>
                    <div>${fmt(r.score)} points, top ${r.top_percent}% ${WINDOW_LABEL[state.window]}</div>
                    <ol>
                        ${r.neighbours.map((n) => `
                            <li class="${n.user_id === uid ? "self" : ""}">
                                <span>${n.rank}. ${esc(n.name)}</span>
                                <span>${fmt(n.score)}</span>
                            </li>`).join("")}
                    </ol>
                `;
            } catch (err) {
                if (!quiet) {
                    box.textContent = `${uid} has no score ${WINDOW_LABEL[state.window]} on this board.`;
                }
            }
        }

        async function submitScore() {
            const body = {
                leaderboard_id: state.board,
                user_id: $("uid").value.trim(),
                name: $("uname").value.trim(),
                country: $("ucountry").value.trim(),
                score: $("uscore").value
            };
            try {
                const r = await api("/v1/scores", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(body)
                });
                state.me = { id: body.user_id, name: body.name };
                saveGuest();
                $("rankUid").value = body.user_id;
                setStatus(`Saved. You are #${r.rank.day} today and #${r.rank.all} all time.`, false);
                await refreshTop();
                await lookupRank(true);
            } catch (err) {
                setStatus(err.message, true);
            }
        }

        $("boardSel").addEventListener("change", (e) => {
            state.board = e.target.value;
            state.offset = 0;
            state.prevRanks = {};
            refreshTop();
            lookupRank(true);
        });

        $("windows").addEventListener("click", (e) => {
            const btn = e.target.closest("button");
            if (!btn) { return; }
            document.querySelectorAll("#windows button").forEach((b) => b.classList.remove("active"));
            btn.classList.add("active");
            state.window = btn.dataset.w;
            state.offset = 0;
            state.prevRanks = {};
            refreshTop();
            lookupRank(true);
        });

        $("kSel").addEventListener("change", (e) => {
            state.k = Number(e.target.value);
            state.offset = 0;
            refreshTop();
        });

        $("prevBtn").addEventListener("click", () => {
            state.offset = Math.max(0, state.offset - state.k);
            refreshTop();
        });

        $("nextBtn").addEventListener("click", () => {
            state.offset += state.k;
            refreshTop();
        });

        $("newBoardBtn").addEventListener("click", () => {
            const id = (prompt("Name for the new leaderboard (letters, numbers, - or _):") || "").trim().toLowerCase();
            if (!id) { return; }
            if (!/^[a-z0-9][a-z0-9_-]{1,31}$/.test(id)) {
                setStatus("Leaderboard names need 2 to 32 characters: a-z, 0-9, - or _.", true);
                return;
            }
            const opt = document.createElement("option");
            opt.value = id;
            opt.textContent = id.replace(/[-_]/g, " ");
            $("boardSel").appendChild(opt);
            $("boardSel").value = id;
            state.board = id;
            state.offset = 0;
            state.prevRanks = {};
            setStatus("The board is created when its first score is submitted.", false);
            refreshTop();
        });

        $("submitBtn").addEventListener("click", submitScore);
        $("randBtn").addEventListener("click", () => {
            $("uscore").value = Math.floor(Math.random() * 45000) + 500;
        });
        $("rankBtn").addEventListener("click", () => lookupRank(false));

        (async function boot() {
            loadGuest();
            await loadBoards();
            await refreshTop();
            setInterval(() => {
                refreshTop();
                lookupRank(true);
            }, 2000);
        })();
    </script>
</body>
</html>
"""

_server = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=_server.serve_forever, daemon=True).start()
_PREV = {"server": _server, "stop": STOP}
time.sleep(0.5)

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height=1500)
    print(f"Game Leaderboard is running on port {PORT}")
except ImportError:
    print(f"Open http://127.0.0.1:{PORT} in your browser")