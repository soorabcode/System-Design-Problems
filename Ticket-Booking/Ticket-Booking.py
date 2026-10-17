"""
Cinema ticket booking - runs in one Google Colab cell (or locally).

1. Requirement analysis 
A. functional requrement 
- search a event 
- view list of events
- book the seat of event
B. non functional requrement 
- scale : 10m dau
- availability 

2. Identifying Entities 
- user
- event 
- ticket

3. API 
A. search 
GET /v1/search?query={search}&location={location}&date={date} -> List<EVENTID> 
B. View 
GET /v1/event={eventID} -> event details 
C. Book 
POST /v1/booking/reserve{list<seat> seats ; }
POST /v1/booking/confirm { bookingID,paymentID }
4. Hih level design 
user -> api gateway -> sever ( search , view & book ) -> database

5. low level design 
uploaded image
"""
import json
import os
import re
import sqlite3
import sys
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

HOLD_SECONDS = 300
MAX_SEATS_PER_BOOKING = 6
SEAT_ROWS, SEAT_COLS = "ABCDEFGH", 10
SEAT_RE = re.compile(r"^[A-H](10|[1-9])$")
GATEWAY_LOG = deque(maxlen=300)  


class ApiError(Exception):
    def __init__(self, status, message, **extra):
        super().__init__(message)
        self.status, self.message, self.extra = status, message, extra

class Cache:
    def __init__(self):
        self._d, self._lock = {}, threading.Lock()
        self.hits = self.misses = 0

    def get(self, key):
        with self._lock:
            item = self._d.get(key)
            if item and item[0] > time.time():
                self.hits += 1
                return item[1]
            self._d.pop(key, None)
            self.misses += 1
            return None

    def set(self, key, value, ttl):
        with self._lock:
            self._d[key] = (time.time() + ttl, value)

    def invalidate(self, prefix):
        with self._lock:
            for k in [k for k in self._d if k.startswith(prefix)]:
                del self._d[k]


# Database
class Database:
   
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.conn.executescript("""

        CREATE TABLE events(
        id INTEGER PRIMARY KEY, 
        title TEXT, 
        genre TEXT, 
        language TEXT,
        location TEXT,
        venue TEXT, 
        date TEXT, 
        time TEXT, 
        price INTEGER, 
        duration TEXT, 
        rating TEXT);


        CREATE TABLE seats(
        event_id INTEGER, 
        seat_id TEXT, 
        status TEXT DEFAULT 'AVAILABLE',
        booking_id TEXT, 
        hold_expires REAL, 
        PRIMARY KEY(event_id, seat_id));

    
        CREATE TABLE bookings(
        id TEXT PRIMARY KEY, 
        event_id INTEGER, 
        user TEXT, 
        seats TEXT,
        status TEXT, 
        amount INTEGER, 
        payment_id TEXT, 
        created_at REAL, 
        expires_at REAL);
        CREATE INDEX idx_events_search ON events(location, date);
        """)
        self._seed()

    def _seed(self):
        d = lambda n: (date.today() + timedelta(days=n)).isoformat()
        shows = [
            ("Galactic Frontier", "Sci-fi", "English", "Kathmandu", "QFX Labim Mall", d(0), "18:30", 450, "2h 15m", "PG-13"),
            ("Galactic Frontier", "Sci-fi", "English", "Kathmandu", "Big Movies, City Centre", d(1), "20:00", 500, "2h 15m", "PG-13"),
            ("Pashupati Express", "Drama", "Nepali", "Kathmandu", "FCube Cinemas, Chabahil", d(0), "16:00", 350, "2h 05m", "U"),
            ("Pashupati Express", "Drama", "Nepali", "Pokhara", "Cinema Hall, Lakeside", d(1), "17:30", 300, "2h 05m", "U"),
            ("Laughing Gas", "Comedy", "Hindi", "Kathmandu", "QFX Civil Mall", d(2), "19:15", 400, "1h 55m", "U/A"),
            ("Midnight Detective", "Thriller", "English", "Pokhara", "Fishtail Cineplex", d(0), "21:00", 450, "2h 20m", "R"),
            ("Midnight Detective", "Thriller", "English", "Lalitpur", "QFX Labim Mall", d(2), "21:30", 450, "2h 20m", "R"),
            ("Himalayan Heart", "Romance", "Nepali", "Lalitpur", "Jai Nepal Cinemas", d(1), "14:00", 250, "2h 10m", "U"),
        ]
        with self.lock:
            self.conn.execute("BEGIN")
            for s in shows:
                cur = self.conn.execute(
                    "INSERT INTO events(title,genre,language,location,venue,date,time,price,duration,rating)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)", s)
                self.conn.executemany("INSERT INTO seats(event_id, seat_id) VALUES (?,?)",
                    [(cur.lastrowid, f"{r}{c}") for r in SEAT_ROWS for c in range(1, SEAT_COLS + 1)])
            self.conn.execute("COMMIT")

    def sweep_expired(self, now):
        """Free seats whose hold ran out. Call inside a transaction."""
        self.conn.execute("UPDATE seats SET status='AVAILABLE', booking_id=NULL, hold_expires=NULL "
                          "WHERE status='RESERVED' AND hold_expires < ?", (now,))
        self.conn.execute("UPDATE bookings SET status='EXPIRED' WHERE status='PENDING' AND expires_at < ?", (now,))


# Services
class SearchService:
    def __init__(self, db, cache):
        self.db, self.cache = db, cache

    def search(self, query="", location="", day=""):
        key = f"search:{query.lower()}|{location.lower()}|{day}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        sql, args = "SELECT id FROM events WHERE 1=1", []
        if query:
            sql += " AND (title LIKE ? OR genre LIKE ? OR language LIKE ?)"
            args += [f"%{query}%"] * 3
        if location:
            sql += " AND location = ?"
            args.append(location)
        if day:
            sql += " AND date = ?"
            args.append(day)
        with self.db.lock:
            ids = [r["id"] for r in self.db.conn.execute(sql + " ORDER BY date, time", args)]
        self.cache.set(key, ids, ttl=30)
        return ids


class ViewTicketService:
    TTL = 5 
    def __init__(self, db, cache):
        self.db, self.cache = db, cache

    def event(self, event_id):
        key = f"event:{event_id}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        with self.db.lock:
            self.db.conn.execute("BEGIN IMMEDIATE")
            try:
                self.db.sweep_expired(time.time())
                ev = self.db.conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
                if not ev:
                    raise ApiError(404, "That showing does not exist")
                seats = {r["seat_id"]: r["status"] for r in self.db.conn.execute(
                    "SELECT seat_id, status FROM seats WHERE event_id=?", (event_id,))}
            finally:
                self.db.conn.execute("COMMIT")
        data = dict(ev, seats=seats, available=sum(s == "AVAILABLE" for s in seats.values()))
        self.cache.set(key, data, self.TTL)
        return data


class BookTicketService:
    def __init__(self, db, cache):
        self.db, self.cache = db, cache

    def reserve(self, body):
        try:
            event_id = int(body.get("eventId"))
        except (TypeError, ValueError):
            raise ApiError(400, "No showing was specified")
        seats, user = body.get("seats"), str(body.get("user") or "").strip()[:60]
        if not user:
            raise ApiError(400, "Enter the name for the booking before holding seats")
        if not isinstance(seats, list) or not seats:
            raise ApiError(400, "Select at least one seat")
        if len(set(seats)) != len(seats) or len(seats) > MAX_SEATS_PER_BOOKING:
            raise ApiError(400, f"A booking can hold 1 to {MAX_SEATS_PER_BOOKING} different seats")
        if not all(isinstance(s, str) and SEAT_RE.match(s) for s in seats):
            raise ApiError(400, "One of the seat ids is not valid")
        with self.db.lock:
            c = self.db.conn
            c.execute("BEGIN IMMEDIATE")   
            try:
                now = time.time()
                self.db.sweep_expired(now)
                ev = c.execute("SELECT price FROM events WHERE id=?", (event_id,)).fetchone()
                if not ev:
                    raise ApiError(404, "That showing does not exist")
                marks = ",".join("?" * len(seats))
                rows = c.execute(f"SELECT seat_id, status FROM seats WHERE event_id=? AND seat_id IN ({marks})",
                                 [event_id, *seats]).fetchall()
                if len(rows) != len(seats):
                    raise ApiError(400, "One of the seats is not part of this showing")
                taken = sorted(r["seat_id"] for r in rows if r["status"] != "AVAILABLE")
                if taken:
                    raise ApiError(409, "Seat " + ", ".join(taken) + " was taken while you were choosing.", taken=taken)
                booking_id = "BK-" + uuid.uuid4().hex[:8].upper()
                expires, amount = now + HOLD_SECONDS, ev["price"] * len(seats)
                c.execute(f"UPDATE seats SET status='RESERVED', booking_id=?, hold_expires=? "
                          f"WHERE event_id=? AND seat_id IN ({marks})", [booking_id, expires, event_id, *seats])
                c.execute("INSERT INTO bookings VALUES (?,?,?,?,?,?,?,?,?)",
                          (booking_id, event_id, user, json.dumps(sorted(seats)), "PENDING", amount, None, now, expires))
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
        self.cache.invalidate(f"event:{event_id}")
        return {"bookingId": booking_id, "status": "PENDING", "seats": sorted(seats), "user": user,
                "amount": amount, "holdSeconds": HOLD_SECONDS}

    def confirm(self, body):
        booking_id, payment_id = str(body.get("bookingId") or ""), str(body.get("paymentId") or "").strip()
        if not booking_id or not payment_id:
            raise ApiError(400, "A booking id and a payment id are both required")
        with self.db.lock:
            c = self.db.conn
            c.execute("BEGIN IMMEDIATE")
            try:
                now = time.time()
                b = c.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
                if not b:
                    raise ApiError(404, "No booking with that id")
                if b["status"] != "CONFIRMED":
                    if b["status"] != "PENDING" or b["expires_at"] < now:
                        self.db.sweep_expired(now)
                        c.execute("COMMIT")
                        self.cache.invalidate(f"event:{b['event_id']}")
                        raise ApiError(410, "The 5-minute hold ran out and the seats were released. Choose seats again.")
                    c.execute("UPDATE seats SET status='BOOKED', hold_expires=NULL WHERE booking_id=?", (booking_id,))
                    c.execute("UPDATE bookings SET status='CONFIRMED', payment_id=? WHERE id=?", (payment_id, booking_id))
                    b = c.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
                result = self._view(b)
                c.execute("COMMIT")
            except ApiError:
                raise
            except BaseException:
                c.execute("ROLLBACK")
                raise
        self.cache.invalidate(f"event:{b['event_id']}")
        return result

    def cancel(self, body):
        booking_id = str(body.get("bookingId") or "")
        with self.db.lock:
            c = self.db.conn
            c.execute("BEGIN IMMEDIATE")
            b = c.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()
            if b and b["status"] == "PENDING":
                c.execute("UPDATE seats SET status='AVAILABLE', booking_id=NULL, hold_expires=NULL WHERE booking_id=?", (booking_id,))
                c.execute("UPDATE bookings SET status='CANCELLED' WHERE id=?", (booking_id,))
            c.execute("COMMIT")
        if b:
            self.cache.invalidate(f"event:{b['event_id']}")
        return {"bookingId": booking_id, "status": "CANCELLED" if b else "NOT_FOUND"}

    def _view(self, b):
        ev = self.db.conn.execute("SELECT * FROM events WHERE id=?", (b["event_id"],)).fetchone()
        return {"bookingId": b["id"], "status": b["status"], "user": b["user"], "seats": json.loads(b["seats"]),
                "amount": b["amount"], "paymentId": b["payment_id"],
                "event": {k: ev[k] for k in ("id", "title", "venue", "location", "date", "time")}}


# Booking Server
class BookingServer:
    def __init__(self, db, cache):
        self.search, self.view, self.book, self.cache = (
            SearchService(db, cache), ViewTicketService(db, cache), BookTicketService(db, cache), cache)

    def handle(self, method, path, query, body):
        if method == "GET" and path == "/v1/search":
            q = lambda k: (query.get(k) or [""])[0].strip()
            return {"events": self.search.search(q("query"), q("location"), q("date"))}
        m = re.fullmatch(r"/v1/event/(\d+)", path)
        if method == "GET" and m:
            return self.view.event(int(m.group(1)))
        if method == "POST" and path == "/v1/booking/reserve":
            return self.book.reserve(body)
        if method == "POST" and path == "/v1/booking/confirm":
            return self.book.confirm(body)
        if method == "POST" and path == "/v1/booking/cancel":
            return self.book.cancel(body)
        if method == "GET" and path == "/v1/health":
            return {"ok": True, "cacheHits": self.cache.hits, "cacheMisses": self.cache.misses}
        raise ApiError(404, "Unknown endpoint")


# API Gateway
class APIGateway:
    RATE_LIMIT, WINDOW = 120, 10     

    def __init__(self, server):
        self.server, self._hits, self._lock = server, defaultdict(deque), threading.Lock()

    def _allowed(self, ip):
        now = time.time()
        with self._lock:
            q = self._hits[ip]
            while q and q[0] < now - self.WINDOW:
                q.popleft()
            if len(q) >= self.RATE_LIMIT:
                return False
            q.append(now)
            return True

    def route(self, method, path, query, body, ip):
        t0 = time.time()
        try:
            if not self._allowed(ip):
                raise ApiError(429, "Too many requests from this browser. Wait a few seconds and try again.")
            status, data = 200, self.server.handle(method, path, query, body)
        except ApiError as e:
            status, data = e.status, {"error": e.message, **e.extra}
        except Exception as e:
            GATEWAY_LOG.append(f"unhandled error: {e!r}")
            status, data = 500, {"error": "The server hit an unexpected error. Try again; if it repeats, restart the cell."}
        GATEWAY_LOG.append(f"{time.strftime('%H:%M:%S')} {method:4} {path} -> {status} ({(time.time()-t0)*1000:.1f} ms)")
        return status, data


def gateway_log(n=30):
    print("\n".join(list(GATEWAY_LOG)[-n:]) or "(no requests yet)")


# HTTP
class Handler(BaseHTTPRequestHandler):
    gateway = None

    def log_message(self, *a):       
        pass

    def _send(self, status, payload, ctype="application/json"):
        raw = payload.encode() if isinstance(payload, str) else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def _dispatch(self, method):
        url = urlparse(self.path)
        if method == "GET" and url.path == "/":
            return self._send(200, PAGE, "text/html")
        body = {}
        if method == "POST":
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
                if not isinstance(body, dict):
                    raise ValueError
            except ValueError:
                return self._send(400, {"error": "The request body was not valid JSON"})
        status, data = self.gateway.route(method, url.path, parse_qs(url.query), body, self.client_address[0])
        self._send(status, data)

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")


#  UI
PAGE = r"""<!DOCTYPE html>

<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Showtimes - cinema booking demo</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' fill='%231c1b19'/%3E%3Crect x='6' y='8' width='20' height='4' fill='%23f5f2eb'/%3E%3Crect x='6' y='16' width='5' height='5' fill='%23f5f2eb'/%3E%3Crect x='13.5' y='16' width='5' height='5' fill='%23f5f2eb'/%3E%3Crect x='21' y='16' width='5' height='5' fill='%23f5f2eb'/%3E%3C/svg%3E">
<style>
  :root{--paper:#f5f2eb;--field:#fffdf8;--ink:#1c1b19;--muted:#67635a;--line:#d6cfbf;--edge:#8a8475;--accent:#9e2a1d}
  *{box-sizing:border-box}
  [hidden]{display:none !important}
  body{margin:0;background:var(--paper);color:var(--ink);font:16px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
  .wrap{max-width:70rem;margin:0 auto;padding:1.25rem 1.25rem 3rem}
  h1,h2,h3{font-family:Georgia,"Times New Roman",serif;line-height:1.2;margin:0}
  h1{font-size:1.6rem} h2{font-size:1.3rem} h3{font-size:1.05rem}
  p{margin:0}
  .mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.92em}
  .muted{color:var(--muted)} .low{color:var(--accent);font-weight:600} .fine{font-size:.85rem;color:var(--muted)}
  header{border-bottom:2px solid var(--ink);padding-bottom:.75rem;margin-bottom:1rem}
  header p{margin-top:.25rem;font-size:.9rem;color:var(--muted);max-width:42rem}

  label{display:block;font-size:.8rem;color:var(--muted);margin-bottom:.2rem}
  input,select{font:inherit;color:var(--ink);background:var(--field);border:1px solid var(--edge);border-radius:3px;padding:.45rem .6rem;min-height:2.5rem;width:100%}
  button{font:inherit;border-radius:3px;border:1px solid var(--ink);padding:.45rem .9rem;min-height:2.5rem;cursor:pointer}
  button.primary{background:var(--ink);color:var(--paper)} button.primary:hover:not(:disabled){background:#3a3834}
  button.secondary{background:transparent;color:var(--ink)} button.secondary:hover:not(:disabled){background:#e9e4d6}
  button:disabled{opacity:.4;cursor:not-allowed}
  a.back,button.back{background:none;border:none;padding:0;min-height:0;color:var(--ink);text-decoration:underline;text-underline-offset:3px}
  :focus-visible{outline:3px solid var(--accent);outline-offset:2px}

  #msg{margin:0 0 1rem;padding:.6rem .8rem;border-left:4px solid var(--ink);background:#ebe6d9}
  #msg.error{border-color:var(--accent);background:#f3dfda}

  .filters{display:grid;grid-template-columns:minmax(12rem,2fr) minmax(9rem,1fr) minmax(9rem,1fr) auto;gap:.75rem;align-items:end;margin-bottom:1.5rem}
  .film{display:grid;grid-template-columns:14rem 1fr;gap:.75rem 2rem;padding:1.25rem 0;border-top:1px solid var(--edge)}
  .film-info p{margin-top:.35rem;color:var(--muted);font-size:.92rem}
  .shows{list-style:none;margin:0;padding:0}
  .show{display:grid;grid-template-columns:9.5rem 1fr 5rem 8.5rem auto;gap:.25rem 1rem;align-items:center;padding:.6rem 0}
  .show+.show{border-top:1px solid var(--line)}
  .empty{padding:1.5rem 0;max-width:40rem}

  .seat-layout{display:grid;grid-template-columns:minmax(0,1fr) 20rem;gap:2rem;align-items:start;margin-top:1rem}
  .showing-meta{margin:.3rem 0 1rem;color:var(--muted)}
  .screen{border-top:3px solid var(--ink);text-align:center;font-size:.75rem;letter-spacing:.25em;text-transform:uppercase;color:var(--muted);padding-top:.25rem;margin:0 2rem 1rem}
  .seatmap{overflow-x:auto;padding-bottom:.5rem}
  .seatrow{display:flex;gap:5px;align-items:center;margin-bottom:5px;justify-content:center;min-width:max-content}
  .rowlabel{width:1.2rem;font-size:.8rem;color:var(--muted);text-align:center}
  .aisle{width:1.1rem}
  .seat{width:clamp(26px,7vw,34px);height:clamp(26px,7vw,34px);padding:0;min-height:0;font-size:.7rem;background:var(--field);border:1px solid var(--ink);border-radius:2px;color:var(--ink)}
  .seat:hover:not(:disabled){background:#e9e4d6}
  .seat.is-selected{background:var(--ink);color:var(--paper)}
  .seat.is-held{border:1px dashed var(--edge);color:var(--edge);background:transparent;opacity:1}
  .seat.is-booked{background:#d9d3c4;border-color:#d9d3c4;color:#8a8475;opacity:1}
  .seat:disabled{cursor:not-allowed;opacity:1}
  .legend{display:flex;flex-wrap:wrap;gap:.4rem 1.25rem;margin-top:1rem;font-size:.82rem;color:var(--muted)}
  .legend span::before{content:"";display:inline-block;width:.85rem;height:.85rem;margin-right:.4rem;vertical-align:-2px;border:1px solid var(--ink);background:var(--field)}
  .legend .l-sel::before{background:var(--ink)} .legend .l-held::before{border:1px dashed var(--edge);background:transparent}
  .legend .l-book::before{background:#d9d3c4;border-color:#d9d3c4}

  aside.summary{border:1px solid var(--ink);padding:1rem;position:sticky;top:1rem;background:var(--field)}
  aside.summary h3{margin-bottom:.6rem}
  aside.summary>*+*{margin-top:.8rem}
  dl{margin:0;display:grid;grid-template-columns:auto 1fr;gap:.25rem 1rem;font-size:.92rem}
  dt{color:var(--muted)} dd{margin:0}
  .actions{display:flex;flex-wrap:wrap;gap:.5rem}
  .hold{font-size:1.1rem}

  .ticket{max-width:34rem;border:1px solid var(--ink);padding:1.25rem;background:var(--field);margin:1rem 0}
  .ticket h2{margin-bottom:.8rem}

  @media (max-width:780px){
    .filters{grid-template-columns:1fr 1fr} .filters .f-q{grid-column:1 / 3} .filters button{grid-column:1 / 3}
    .film{grid-template-columns:1fr}
    .show{grid-template-columns:1fr auto} .show>*{grid-column:1} .show>button{grid-column:2;grid-row:1 / span 4;align-self:center}
    .seat-layout{grid-template-columns:1fr} aside.summary{position:static}
    .screen{margin:0 0 1rem}
  }
  @media print{ header,#msg,.noprint{display:none !important} body{background:#fff} }
</style>
</head>
<body>
<div class="wrap">
  <header class="noprint">
    <h1>Showtimes</h1>
    <p>Demo schedule for Kathmandu, Lalitpur and Pokhara: today and the next two days. Payment is simulated and nothing is charged.</p>
  </header>
  <div id="msg" role="status" hidden></div>

  <section id="list-view" aria-label="Showtimes">
    <form class="filters" id="filters">
      <div class="f-q"><label for="q">Film, genre or language</label><input id="q" type="search" placeholder="e.g. Nepali, comedy, Midnight"></div>
      <div><label for="loc">Location</label>
        <select id="loc"><option value="">Any</option><option>Kathmandu</option><option>Lalitpur</option><option>Pokhara</option></select></div>
      <div><label for="day">Date</label><input id="day" type="date"></div>
      <button class="primary" type="submit">Filter showings</button>
    </form>
    <div id="results" aria-live="polite"></div>
  </section>

  <section id="seat-view" hidden>
    <button class="back noprint" type="button" id="backBtn">&larr; Back to showtimes</button>
    <div class="seat-layout">
      <div>
        <h2 id="s-title"></h2>
        <p class="showing-meta" id="s-meta"></p>
        <div class="screen">Screen</div>
        <div class="seatmap" id="map"></div>
        <div class="legend"><span>Available</span><span class="l-sel">Yours</span><span class="l-held">Held by someone else</span><span class="l-book">Booked</span></div>
        <p class="fine" style="margin-top:.8rem"><button class="back" type="button" id="refreshBtn">Refresh seat availability</button></p>
      </div>
      <aside class="summary" id="summary" aria-label="Your booking"></aside>
    </div>
  </section>

  <section id="ticket-view" hidden></section>
</div>

<script>
"use strict";
const ROWS = "ABCDEFGH", COLS = 10, MAX = 6, TOTAL = ROWS.length * COLS;
const S = {ev: null, picked: new Set(), stage: "pick", booking: null, deadline: 0, timer: null, name: ""};
const $ = id => document.getElementById(id);
const esc = s => String(s).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const fmtDate = d => new Date(d + "T00:00:00").toLocaleDateString("en-GB", {weekday:"short", day:"numeric", month:"short"});

/* Relative URLs (no leading slash) so the page also works behind Colab's port proxy. */
async function api(path, opts) {
  const r = await fetch(path, opts);
  const data = await r.json().catch(() => ({}));
  if (!r.ok) { const e = new Error(data.error || ("Request failed with status " + r.status)); e.status = r.status; e.data = data; throw e; }
  return data;
}
const post = (p, b) => api(p, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(b)});

function setMsg(text, kind) {
  const m = $("msg");
  m.hidden = !text; m.textContent = text || ""; m.className = kind || "";
}
function show(view) {
  for (const v of ["list-view", "seat-view", "ticket-view"]) $(v).hidden = v !== view;
  window.scrollTo(0, 0);
}

/* ---------- Search and list ---------- */
async function search(e) {
  if (e) e.preventDefault();
  const p = new URLSearchParams({query: $("q").value.trim(), location: $("loc").value, date: $("day").value});
  try {
    const {events} = await api("v1/search?" + p);
    renderResults(await Promise.all(events.map(id => api("v1/event/" + id))));
  } catch (err) {
    $("results").innerHTML = `<p class="empty">Showtimes could not be loaded (${esc(err.message)}). Make sure the notebook cell that started the server is still running, then filter again.</p>`;
  }
}
function renderResults(list) {
  if (!list.length) {
    $("results").innerHTML = `<p class="empty">No showings match those filters. Try removing the date or location; the demo only has shows for today and the next two days.</p>`;
    return;
  }
  const films = new Map();
  for (const e of list) { if (!films.has(e.title)) films.set(e.title, []); films.get(e.title).push(e); }
  $("results").innerHTML = [...films].map(([title, shows]) => {
    const f = shows[0];
    return `<section class="film">
      <div class="film-info"><h2>${esc(title)}</h2><p>${esc(f.genre)}, ${esc(f.language)}<br>${esc(f.duration)}, rated ${esc(f.rating)}</p></div>
      <ul class="shows">${shows.map(e => `
        <li class="show">
          <span><b>${fmtDate(e.date)}</b> <span class="mono">${esc(e.time)}</span></span>
          <span>${esc(e.venue)}<br><span class="muted">${esc(e.location)}</span></span>
          <span class="mono">NPR ${e.price}</span>
          <span class="${e.available && e.available < 15 ? "low" : "muted"}">${e.available ? e.available + " of " + TOTAL + " seats left" : "Sold out"}</span>
          <button class="secondary" type="button" ${e.available ? "" : "disabled"} onclick="openShow(${e.id})">Choose seats</button>
        </li>`).join("")}</ul></section>`;
  }).join("");
}

/* ---------- Seat selection ---------- */
async function openShow(id) {
  try { S.ev = await api("v1/event/" + id); } catch (e) { return setMsg(e.message, "error"); }
  Object.assign(S, {picked: new Set(), stage: "pick", booking: null});
  setMsg(null);
  $("s-title").textContent = S.ev.title;
  $("s-meta").textContent = `${fmtDate(S.ev.date)}, ${S.ev.time} · ${S.ev.venue}, ${S.ev.location} · NPR ${S.ev.price} per seat`;
  buildMap(); renderSummary(); show("seat-view");
}
function buildMap() {
  let h = "";
  for (const r of ROWS) {
    h += `<div class="seatrow"><span class="rowlabel" aria-hidden="true">${r}</span>`;
    for (let c = 1; c <= COLS; c++) { h += `<button type="button" class="seat" data-seat="${r}${c}">${c}</button>`; if (c === 5) h += `<span class="aisle"></span>`; }
    h += `</div>`;
  }
  $("map").innerHTML = h;
  refreshSeats();
}
function refreshSeats() {
  const held = S.booking ? S.booking.seats : [];
  document.querySelectorAll("#map .seat").forEach(b => {
    const id = b.dataset.seat, st = S.ev.seats[id];
    const mine = S.picked.has(id) || (S.stage === "pay" && held.includes(id));
    b.classList.toggle("is-selected", mine);
    b.classList.toggle("is-held", !mine && st === "RESERVED");
    b.classList.toggle("is-booked", !mine && st === "BOOKED");
    b.disabled = S.stage !== "pick" || (!mine && st !== "AVAILABLE");
    b.setAttribute("aria-pressed", mine ? "true" : "false");
    b.setAttribute("aria-label", `Seat ${id}, ` + (mine ? "yours" : st === "AVAILABLE" ? "available" : st === "RESERVED" ? "held by someone else" : "booked"));
  });
}
$("map").addEventListener("click", e => {
  const b = e.target.closest("button.seat"); if (!b || S.stage !== "pick") return;
  const id = b.dataset.seat;
  if (S.picked.has(id)) S.picked.delete(id);
  else if (S.picked.size >= MAX) return setMsg(`A booking holds at most ${MAX} seats.`, "error");
  else S.picked.add(id);
  setMsg(null); refreshSeats(); renderSummary();
});

function renderSummary() {
  const el = $("summary"), ev = S.ev;
  if (S.stage === "pick") {
    const seats = [...S.picked].sort(), n = seats.length;
    el.innerHTML = `
      <h3>Your seats</h3>
      <p>${n ? `<span class="mono">${seats.join(", ")}</span>` : `<span class="muted">None selected yet. You can pick up to ${MAX}.</span>`}</p>
      <p>${n ? `NPR ${ev.price} × ${n} = <b>NPR ${ev.price * n}</b>` : ""}</p>
      <div><label for="name">Name for the booking</label><input id="name" maxlength="60" autocomplete="name" value="${esc(S.name)}"></div>
      <button class="primary" type="button" id="holdBtn" ${n ? "" : "disabled"}>${n ? `Hold ${n} seat${n > 1 ? "s" : ""} for 5 minutes` : "Hold seats"}</button>
      <p class="fine">Holding does not charge you. Seats go back on sale if payment is not completed within 5 minutes.</p>`;
    $("name").addEventListener("input", e => S.name = e.target.value);
    $("holdBtn").addEventListener("click", hold);
  } else if (S.stage === "pay") {
    const b = S.booking;
    el.innerHTML = `
      <h3>Pay to keep these seats</h3>
      <dl><dt>Seats</dt><dd class="mono">${b.seats.join(", ")}</dd><dt>Name</dt><dd>${esc(b.user)}</dd>
          <dt>Reference</dt><dd class="mono">${b.bookingId}</dd><dt>Amount</dt><dd><b>NPR ${b.amount}</b></dd></dl>
      <p class="hold">Time left to pay: <b class="mono" id="hold"></b></p>
      <div><label for="pm">Payment method (simulated)</label>
        <select id="pm"><option>Mobile wallet</option><option>Bank card</option><option>Bank transfer</option></select></div>
      <div class="actions"><button class="primary" type="button" id="payBtn">Pay NPR ${b.amount} (simulated)</button>
        <button class="secondary" type="button" id="relBtn">Release seats</button></div>`;
    $("payBtn").addEventListener("click", pay);
    $("relBtn").addEventListener("click", () => release("The seats were released."));
  }
}

/* ---------- Hold -> pay ---------- */
async function hold() {
  S.name = $("name").value;
  try {
    S.booking = await post("v1/booking/reserve", {eventId: S.ev.id, seats: [...S.picked], user: S.name});
    S.stage = "pay"; S.deadline = Date.now() + S.booking.holdSeconds * 1000;
    setMsg(null); renderSummary(); refreshSeats(); startTimer();
  } catch (e) {
    if (e.status === 409) {
      (e.data.taken || []).forEach(s => S.picked.delete(s));
      S.ev = await api("v1/event/" + S.ev.id).catch(() => S.ev);
      refreshSeats(); renderSummary();
      setMsg(e.message + " Your other seats are still selected; pick a replacement and hold again.", "error");
    } else setMsg(e.message, "error");
  }
}
function startTimer() {
  clearInterval(S.timer);
  const tick = () => {
    const left = Math.max(0, Math.ceil((S.deadline - Date.now()) / 1000)), el = $("hold");
    if (el) el.textContent = Math.floor(left / 60) + ":" + String(left % 60).padStart(2, "0");
    if (left === 0) { clearInterval(S.timer); resetToPick("The 5-minute hold ran out and the seats were released. Choose seats again.", "error"); }
  };
  tick(); S.timer = setInterval(tick, 1000);
}
async function resetToPick(text, kind) {
  clearInterval(S.timer);
  Object.assign(S, {stage: "pick", booking: null, picked: new Set()});
  S.ev = await api("v1/event/" + S.ev.id).catch(() => S.ev);
  refreshSeats(); renderSummary(); setMsg(text, kind);
}
async function release(text) {
  if (S.booking) await post("v1/booking/cancel", {bookingId: S.booking.bookingId}).catch(() => {});
  resetToPick(text);
}
async function pay() {
  const btn = $("payBtn"); btn.disabled = true; btn.textContent = "Processing simulated payment…";
  await new Promise(r => setTimeout(r, 700));
  const paymentId = "SIM-" + Math.random().toString(36).slice(2, 10).toUpperCase();
  try {
    const t = await post("v1/booking/confirm", {bookingId: S.booking.bookingId, paymentId});
    clearInterval(S.timer); S.booking = null; setMsg(null); renderTicket(t); show("ticket-view");
  } catch (e) {
    if (e.status === 410) resetToPick(e.message, "error");
    else { setMsg(e.message, "error"); btn.disabled = false; btn.textContent = "Retry payment"; }
  }
}
function renderTicket(t) {
  const e = t.event;
  $("ticket-view").innerHTML = `
    <div class="ticket">
      <h2>Booking ${esc(t.bookingId)} confirmed</h2>
      <dl><dt>Film</dt><dd>${esc(e.title)}</dd><dt>When</dt><dd>${fmtDate(e.date)}, ${esc(e.time)}</dd>
          <dt>Where</dt><dd>${esc(e.venue)}, ${esc(e.location)}</dd><dt>Seats</dt><dd class="mono">${t.seats.join(", ")}</dd>
          <dt>Name</dt><dd>${esc(t.user)}</dd><dt>Paid</dt><dd>NPR ${t.amount} (simulated, ref <span class="mono">${esc(t.paymentId)}</span>)</dd></dl>
    </div>
    <div class="actions noprint"><button class="secondary" type="button" onclick="window.print()">Print this ticket</button>
      <button class="primary" type="button" id="againBtn">Book another showing</button></div>`;
  $("againBtn").addEventListener("click", () => { show("list-view"); search(); });
}

$("filters").addEventListener("submit", search);
$("refreshBtn").addEventListener("click", async () => {
  S.ev = await api("v1/event/" + S.ev.id).catch(() => S.ev);
  [...S.picked].forEach(s => { if (S.ev.seats[s] !== "AVAILABLE") S.picked.delete(s); });
  refreshSeats(); renderSummary();
});
$("backBtn").addEventListener("click", async () => {
  if (S.booking) await post("v1/booking/cancel", {bookingId: S.booking.bookingId}).catch(() => {});
  clearInterval(S.timer); S.booking = null; setMsg(null); show("list-view"); search();
});
search();
</script>
</body>
</html>
"""


# launch
def _in_notebook():
    return "ipykernel" in sys.modules


def _display(port, height=880):
    try:                                           # Google Colab
        from google.colab import output
        output.serve_kernel_port_as_iframe(port, height=str(height))
    except ImportError:                            # Jupyter / VS Code
        from IPython.display import IFrame, display
        display(IFrame(f"http://127.0.0.1:{port}/", width="100%", height=height))


def launch(port=0):
    """Start (or restart) the app. In a notebook it returns immediately and shows the UI."""
    previous = globals().get("_SERVER")
    if previous is not None:                       # re-running the cell replaces the old server
        previous.shutdown()
        previous.server_close()
    Handler.gateway = APIGateway(BookingServer(Database(), Cache()))
    server = ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("PORT", port))), Handler)
    globals()["_SERVER"] = server
    actual = server.server_address[1]
    if _in_notebook():
        threading.Thread(target=server.serve_forever, daemon=True).start()
        _display(actual)
    else:
        print(f"Open http://127.0.0.1:{actual}/   (Ctrl+C to stop)")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__" or _in_notebook():
    launch()
