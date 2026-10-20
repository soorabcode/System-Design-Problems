#  Hotel Booking Platform (no-login)
''' 

1. Requirement analysis 
A. functional requrement 
- search 
- view
- book 
- payment 
B. non functional requrement 
- scale : 100M MAU 
- Cap theroem - availabe for search and consistrent for book

2. Identifying Entities 
- user
- hotels

3. API 
A. search 
GET /v1/search?query={search}-> List<hotelID> :pagenation 
B. View 
GET /v1/hotel={hotelD} -> hotel details 
C. Book 
POST /v1/book{book body : hotelID}-> bookID
D. Payment 
GET /v1/payment={bookID}

4. Hih level design 
user -> api gateway -> sever ( search , view , book , payment ) -> database

5. low level design 
uploaded image
''' 
import collections, datetime as dt, itertools, json, queue, random, re
import sqlite3, threading, time, uuid
from flask import Flask, request, jsonify, Response, g

HOLD_SECONDS = 600        
PORT = 5057
TAX_RATE = 0.12


_seq = itertools.count(1)
SYSLOG = collections.deque(maxlen=300)


def log(component, msg):
    SYSLOG.append({"id": next(_seq), "t": time.strftime("%H:%M:%S"), "c": component, "m": msg})


# Tiny thread-safe sqlite wrapper (stand-in for Postgres or MySQL)
class Store:
    def __init__(self, name):
        self.name = name
        self.c = sqlite3.connect(":memory:", check_same_thread=False)
        self.c.row_factory = sqlite3.Row
        self.l = threading.RLock()

    def script(self, sql):
        with self.l:
            self.c.executescript(sql)

    def q(self, sql, a=()):
        with self.l:
            return [dict(r) for r in self.c.execute(sql, a).fetchall()]

    def one(self, sql, a=()):
        r = self.q(sql, a)
        return r[0] if r else None

    def x(self, sql, a=()):
        with self.l:
            cur = self.c.execute(sql, a)
            self.c.commit()
            return cur.rowcount


PG = Store("postgres")   
BK = Store("mysql")     

PG.script("""
CREATE TABLE hotel(id TEXT PRIMARY KEY, name TEXT, city TEXT, country TEXT, stars INT,
  rating REAL, reviews INT, description TEXT, amenities TEXT);
CREATE TABLE room(id TEXT PRIMARY KEY, hotel_id TEXT, room_type TEXT, room_no TEXT);
CREATE TABLE price(hotel_id TEXT, room_type TEXT, nightly_price REAL, capacity INT, beds TEXT,
  PRIMARY KEY(hotel_id, room_type));
""")
BK.script("""
CREATE TABLE bookings(id TEXT PRIMARY KEY, hotel_id TEXT, hotel_name TEXT, room_id TEXT, room_type TEXT,
  room_no TEXT, checkin TEXT, checkout TEXT, nights INT, guests INT, guest_name TEXT, email TEXT,
  phone TEXT, subtotal REAL, taxes REAL, amount REAL, status TEXT, created REAL, hold_expires REAL,
  paid_at REAL, payment_ref TEXT, card_last4 TEXT);
CREATE INDEX idx_bk_status ON bookings(status);
CREATE TABLE archive(seq INTEGER PRIMARY KEY AUTOINCREMENT, booking_id TEXT, event TEXT, payload TEXT, ts REAL);
""")

# Hotel DB
# name, city, country, stars, rating, reviews, base nightly price, tagline
HOTELS = [
    ("Himalayan Horizon Hotel", "Kathmandu", "Nepal", 5, 4.7, 2310, 120, "Luxury at the foot of the Himalayas with rooftop sunrise views."),
    ("Thamel Heritage Inn", "Kathmandu", "Nepal", 3, 4.3, 980, 45, "Newari-style courtyard stay in the heart of Thamel."),
    ("Lakeside Serenity Resort", "Pokhara", "Nepal", 4, 4.6, 1720, 95, "Wake up to Phewa Lake and the Annapurna range."),
    ("Fewa View Boutique", "Pokhara", "Nepal", 3, 4.4, 640, 55, "Cosy boutique rooms a short walk from the lakeside."),
    ("Chitwan Jungle Lodge", "Chitwan", "Nepal", 4, 4.5, 870, 80, "Safari-style lodge on the edge of the national park."),
]
AMENITIES = ["WiFi", "Pool", "Spa", "Gym", "Breakfast included", "Airport shuttle", "Parking", "Restaurant",
             "Bar", "Pet friendly", "Rooftop", "24h reception", "Garden", "Laundry"]
ROOM_TYPES = [  
    ("Standard", 1.0, 2, "1 Queen bed", 4),
    ("Deluxe", 1.45, 3, "1 King bed + sofa", 3),
    ("Family Suite", 2.2, 5, "2 Queen beds + lounge", 2),
]

for i, (name, city, country, stars, rating, reviews, base, tag) in enumerate(HOTELS, 1):
    hid = f"H{i:03d}"
    rnd = random.Random(i)
    ams = ["WiFi"] + rnd.sample(AMENITIES[1:], rnd.randint(4, 7))
    PG.x("INSERT INTO hotel VALUES(?,?,?,?,?,?,?,?,?)",
         (hid, name, city, country, stars, rating, reviews, tag, json.dumps(ams)))
    for rt, mult, cap, beds, count in ROOM_TYPES:
        PG.x("INSERT INTO price VALUES(?,?,?,?,?)", (hid, rt, round(base * mult), cap, beds))
        for n in range(1, count + 1):
            base_no = {"Standard": 100, "Deluxe": 200, "Family Suite": 300}[rt]
            PG.x("INSERT INTO room VALUES(?,?,?,?)", (f"{hid}-{rt[:3].upper()}{n}", hid, rt, str(base_no + n)))


# Elastic stand-in
def tokenize(text):
    return re.findall(r"[a-z0-9]+", str(text).lower())


class Elastic:
    def __init__(self):
        self.inv = collections.defaultdict(set)
        self.ids = set()

    def index(self, hid, text):
        self.ids.add(hid)
        for t in set(tokenize(text)):
            self.inv[t].add(hid)

    def search(self, query):
       
        toks = tokenize(query)
        if not toks:
            return [(h, 0) for h in sorted(self.ids)]
        scores, result = collections.Counter(), None
        for t in toks:
            hits = set()
            for term, ids in self.inv.items():
                if term.startswith(t):
                    for h in ids:
                        scores[h] += 3 if term == t else 1
                    hits |= ids
            result = hits if result is None else result & hits
        return [(h, scores[h]) for h in (result or [])]


ES = Elastic()
for h in PG.q("SELECT * FROM hotel"):
    ES.index(h["id"], " ".join([h["name"], h["city"], h["country"], h["description"], str(h["stars"]), "star stars",
                                " ".join(json.loads(h["amenities"]))]))
log("Elastic", f"Indexed {len(ES.ids)} hotels from Hotel DB (Postgres)")


#  Redis Lock stand-in
class RedisLock:
    def __init__(self):
        self.d, self.m = {}, threading.Lock()

    def _purge(self, now):
        for k in [k for k, (_, e) in self.d.items() if e is not None and e <= now]:
            del self.d[k]

    def acquire_many(self, keys, owner, ttl):
        with self.m:
            now = time.time()
            self._purge(now)
            if any(k in self.d for k in keys):
                return False
            for k in keys:
                self.d[k] = (owner, now + ttl)
            return True

    def snapshot(self):
        with self.m:
            self._purge(time.time())
            return set(self.d)

    def release_owner(self, owner):
        with self.m:
            for k in [k for k, (o, _) in self.d.items() if o == owner]:
                del self.d[k]

    def persist_owner(self, owner):        
        with self.m:
            for k, (o, _) in list(self.d.items()):
                if o == owner:
                    self.d[k] = (o, None)

    def stats(self):
        with self.m:
            self._purge(time.time())
            return {"total": len(self.d), "holding": sum(1 for _, e in self.d.values() if e is not None)}


LOCKS = RedisLock()


# Kafka stand-in: topics + consumer threads
class Kafka:
    def __init__(self):
        self.topics, self.count = {}, collections.defaultdict(lambda: {"produced": 0, "consumed": 0})

    def subscribe(self, topic, group, handler):
        q = queue.Queue()
        self.topics[topic] = q

        def loop():
            while True:
                msg = q.get()
                try:
                    handler(msg)
                    self.count[topic]["consumed"] += 1
                except Exception as e:  
                    log("Kafka", f"[{group}] handler error on {topic}: {e}")

        threading.Thread(target=loop, daemon=True).start()

    def publish(self, topic, msg):
        self.count[topic]["produced"] += 1
        log("Kafka", f"produce → {topic} :: {msg.get('type')}")
        self.topics[topic].put(msg)


KAFKA = Kafka()
ACKS = {}   


def booking_writer(msg):
    b = msg["booking"]
    ack = ACKS.get(b["id"])
    try:
        BK.x("""INSERT INTO bookings VALUES(:id,:hotel_id,:hotel_name,:room_id,:room_type,:room_no,:checkin,
            :checkout,:nights,:guests,:guest_name,:email,:phone,:subtotal,:taxes,:amount,:status,:created,
            :hold_expires,NULL,NULL,NULL)""", b)
        log("BookingDB", f"INSERT booking {b['id']} (PENDING) room {b['room_no']}")
        KAFKA.publish("booking.events", {"type": "BOOKING_CREATED", "booking_id": b["id"]})
        if ack:
            ack[1] = True
    except Exception as e:
        LOCKS.release_owner(b["id"])
        log("BookingDB", f"INSERT failed for {b['id']}: {e}")
    finally:
        if ack:
            ack[0].set()


def archival_consumer(msg):
   
    BK.x("INSERT INTO archive(booking_id,event,payload,ts) VALUES(?,?,?,?)",
         (msg.get("booking_id"), msg["type"], json.dumps(msg, default=str), time.time()))
    log("Archival", f"archived {msg['type']} for {msg.get('booking_id')}")


KAFKA.subscribe("booking.requests", "booking-writer", booking_writer)
KAFKA.subscribe("booking.events", "archival", archival_consumer)


def expire_booking(bid):
    n = BK.x("UPDATE bookings SET status='EXPIRED' WHERE id=? AND status='PENDING'", (bid,))
    if n:
        LOCKS.release_owner(bid)
        log("RedisLock", f"TTL expired → room hold released for {bid}")
        KAFKA.publish("booking.events", {"type": "BOOKING_EXPIRED", "booking_id": bid})
    return bool(n)


def reaper():
    while True:
        time.sleep(2)
        for r in BK.q("SELECT id FROM bookings WHERE status='PENDING' AND hold_expires<=?", (time.time(),)):
            expire_booking(r["id"])


threading.Thread(target=reaper, daemon=True).start()


# Payment gateway stand-in (Stripe test-mode behaviour)
def luhn(num):
    s, alt = 0, False
    for ch in reversed(num):
        d = int(ch)
        if alt:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        s += d
        alt = not alt
    return s % 10 == 0


def stripe_charge(amount, number, name, exp, cvc):
    time.sleep(0.8) 
    n = re.sub(r"\D", "", number or "")
    if len(n) < 13 or len(n) > 19 or not luhn(n):
        return {"ok": False, "message": "Your card number is invalid."}
    m = re.fullmatch(r"(\d{1,2})\s*/\s*(\d{2}|\d{4})", (exp or "").strip())
    if not m or not 1 <= int(m.group(1)) <= 12:
        return {"ok": False, "message": "Your card's expiry date is invalid."}
    yy = int(m.group(2)) + (2000 if len(m.group(2)) == 2 else 0)
    today = dt.date.today()
    if (yy, int(m.group(1))) < (today.year, today.month):
        return {"ok": False, "message": "Your card has expired."}
    if not re.fullmatch(r"\d{3,4}", (cvc or "").strip()):
        return {"ok": False, "message": "Your card's security code is invalid."}
    if n == "4000000000000002":
        return {"ok": False, "message": "Your card was declined."}
    if n == "4000000000009995":
        return {"ok": False, "message": "Your card has insufficient funds."}
    brand = "Visa" if n[0] == "4" else "Mastercard" if n[0] == "5" else "Amex" if n[0] == "3" else "Card"
    return {"ok": True, "ref": "ch_" + uuid.uuid4().hex[:18], "last4": n[-4:], "brand": brand}


def parse_range(ci, co):
    try:
        a, b = dt.date.fromisoformat(ci), dt.date.fromisoformat(co)
    except Exception:
        raise ValueError("Please choose valid check-in and check-out dates.")
    if a < dt.date.today() - dt.timedelta(days=1):
        raise ValueError("Check-in cannot be in the past.")
    if b <= a:
        raise ValueError("Check-out must be after check-in.")
    if (b - a).days > 30:
        raise ValueError("Stays are limited to 30 nights.")
    return [(a + dt.timedelta(i)).isoformat() for i in range((b - a).days)]


def room_keys(room_id, nights):
    return [f"room:{room_id}:{d}" for d in nights]


def free_rooms(hid, rtype, nights, snap=None):
    snap = LOCKS.snapshot() if snap is None else snap
    rooms = PG.q("SELECT id, room_no FROM room WHERE hotel_id=? AND room_type=?", (hid, rtype))
    return [r for r in rooms if not any(k in snap for k in room_keys(r["id"], nights))]


def hotel_summary(h, nights=None, snap=None):
    prices = PG.q("SELECT * FROM price WHERE hotel_id=? ORDER BY nightly_price", (h["id"],))
    out = {k: h[k] for k in ("id", "name", "city", "country", "stars", "rating", "reviews")}
    out["amenities"] = json.loads(h["amenities"])
    out["from_price"] = prices[0]["nightly_price"]
    if nights:
        out["available_rooms"] = sum(len(free_rooms(h["id"], p["room_type"], nights, snap)) for p in prices)
    return out


def public_booking(b, pii=False):
    keys = ["id", "hotel_id", "hotel_name", "room_type", "room_no", "checkin", "checkout", "nights", "guests",
            "subtotal", "taxes", "amount", "status", "created", "paid_at", "payment_ref", "card_last4", "guest_name"]
    out = {k: b[k] for k in keys}
    out["bookID"] = b["id"]
    if pii:
        out["email"], out["phone"] = b["email"], b["phone"]
    if b["status"] == "PENDING":
        out["seconds_left"] = max(0, int(b["hold_expires"] - time.time()))
    return out


def get_booking(bid):
    b = BK.one("SELECT * FROM bookings WHERE id=?", (bid,))
    if b and b["status"] == "PENDING" and b["hold_expires"] <= time.time():
        expire_booking(bid)
        b = BK.one("SELECT * FROM bookings WHERE id=?", (bid,))
    return b


def err(msg, code=400):
    return jsonify(error=msg), code


# Flask app = Load balancer / API gateway + services
app = Flask(__name__)
SERVICES = {"search": 3, "view": 2, "book": 3, "payment": 2, "booking-mgmt": 2}   # instances per service
_rr = {k: itertools.count() for k in SERVICES}
PAYING, PAY_MUTEX = set(), threading.Lock()


def service_of(path):
    if path.startswith("/v1/booking") or path.startswith("/v1/manager"):
        return "booking-mgmt"
    for prefix, svc in (("/v1/search", "search"), ("/v1/hotel", "view"), ("/v1/book", "book"),
                        ("/v1/payment", "payment")):
        if path.startswith(prefix):
            return svc
    return None


@app.before_request
def lb_in():
    svc = service_of(request.path)
    g.served = None
    if svc:
        g.served = f"{svc}-svc-{next(_rr[svc]) % SERVICES[svc] + 1}"
        g.t0 = time.time()


@app.after_request
def lb_out(resp):
    if g.get("served"):
        resp.headers["X-Served-By"] = g.served
        log("LoadBalancer", f"{request.method} {request.path} → {g.served} [{resp.status_code}] "
                            f"{int((time.time() - g.t0) * 1000)}ms")
    resp.headers["Cache-Control"] = "no-store"
    return resp


# A. SEARCH  GET /v1/search?query=...&page=1 -> List<hotelID> (paginated)
@app.get("/v1/search")
def search():
    query = request.args.get("query", "")
    ci, co = request.args.get("checkin"), request.args.get("checkout")
    nights = None
    if ci and co:
        try:
            nights = parse_range(ci, co)
        except ValueError as e:
            return err(str(e))
    try:
        page = max(1, int(request.args.get("page", 1)))
        size = min(20, max(1, int(request.args.get("page_size", 6))))
        min_stars = int(request.args.get("min_stars", 0))
    except ValueError:
        return err("Invalid pagination parameters.")
    sort = request.args.get("sort", "relevance")

    hits = ES.search(query)                                  # Elastic -> hotel ids
    snap = LOCKS.snapshot()
    hotels = []
    for hid, score in hits:                                  # hydrate from Hotel DB (Postgres)
        h = PG.one("SELECT * FROM hotel WHERE id=?", (hid,))
        if h and h["stars"] >= min_stars:
            s = hotel_summary(h, nights, snap)
            s["_score"] = score
            hotels.append(s)
    keyf = {"price_asc": lambda s: (s["from_price"], -s["rating"]),
            "price_desc": lambda s: (-s["from_price"], -s["rating"]),
            "rating": lambda s: (-s["rating"], s["from_price"])}.get(sort, lambda s: (-s["_score"], -s["rating"]))
    hotels.sort(key=keyf)
    total = len(hotels)
    chunk = hotels[(page - 1) * size: page * size]
    for s in chunk:
        s.pop("_score", None)
    return jsonify(query=query, page=page, page_size=size, total=total,
                   total_pages=max(1, -(-total // size)), hotel_ids=[s["id"] for s in chunk], results=chunk)


# B. VIEW  GET /v1/hotel/<hotelID> -> hotel details 
@app.get("/v1/hotel/<hid>")
def view_hotel(hid):
    h = PG.one("SELECT * FROM hotel WHERE id=?", (hid,))
    if not h:
        return err("Hotel not found.", 404)
    ci, co = request.args.get("checkin"), request.args.get("checkout")
    nights = None
    if ci and co:
        try:
            nights = parse_range(ci, co)
        except ValueError as e:
            return err(str(e))
    out = hotel_summary(h)
    out["description"] = h["description"]
    out["hold_minutes"] = HOLD_SECONDS // 60
    out["nights"] = len(nights) if nights else 0
    snap = LOCKS.snapshot()
    rooms = PG.q("SELECT * FROM price WHERE hotel_id=? ORDER BY nightly_price", (hid,))
    for r in rooms:
        r.pop("hotel_id")
        if nights:
            r["available"] = len(free_rooms(hid, r["room_type"], nights, snap))
    out["rooms"] = rooms
    return jsonify(out)


# C. BOOK  POST /v1/book {hotelID,...} -> bookID  
@app.post("/v1/book")
def book():
    d = request.get_json(force=True, silent=True) or {}
    hid, rtype = d.get("hotelID"), d.get("roomType")
    name, email, phone = (d.get("name") or "").strip(), (d.get("email") or "").strip(), (d.get("phone") or "").strip()
    try:
        nights = parse_range(d.get("checkin"), d.get("checkout"))
        guests = int(d.get("guests", 1))
    except ValueError as e:
        return err(str(e))
    h = PG.one("SELECT * FROM hotel WHERE id=?", (hid,))
    p = PG.one("SELECT * FROM price WHERE hotel_id=? AND room_type=?", (hid, rtype)) if h else None
    if not h or not p:
        return err("Unknown hotel or room type.", 404)
    if len(name) < 2:
        return err("Please enter your full name.")
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        return err("Please enter a valid email address.")
    if not 1 <= guests <= p["capacity"]:
        return err(f"This room sleeps up to {p['capacity']} guests.")

    bid = "BK-" + uuid.uuid4().hex[:8].upper()
    rooms = PG.q("SELECT * FROM room WHERE hotel_id=? AND room_type=?", (hid, rtype))
    random.shuffle(rooms)                                    # spread contention over rooms
    chosen = None
    for r in rooms:                                          # Redis: atomic lock of every room-night
        if LOCKS.acquire_many(room_keys(r["id"], nights), bid, HOLD_SECONDS):
            chosen = r
            log("RedisLock", f"SET NX room {r['room_no']} × {len(nights)} nights TTL {HOLD_SECONDS}s owner={bid}")
            break
    if not chosen:
        return err("Sorry, those rooms were just taken or are sold out for your dates.", 409)

    subtotal = round(p["nightly_price"] * len(nights), 2)
    taxes = round(subtotal * TAX_RATE, 2)
    now = time.time()
    booking = dict(id=bid, hotel_id=hid, hotel_name=h["name"], room_id=chosen["id"], room_type=rtype,
                   room_no=chosen["room_no"], checkin=nights[0],
                   checkout=(dt.date.fromisoformat(nights[-1]) + dt.timedelta(1)).isoformat(),
                   nights=len(nights), guests=guests, guest_name=name, email=email, phone=phone,
                   subtotal=subtotal, taxes=taxes, amount=round(subtotal + taxes, 2), status="PENDING",
                   created=now, hold_expires=now + HOLD_SECONDS)
    ack = [threading.Event(), False]
    ACKS[bid] = ack
    KAFKA.publish("booking.requests", {"type": "BOOKING_REQUESTED", "booking": booking})
    ack[0].wait(5)                                           # wait for the Booking DB write (consistency)
    ACKS.pop(bid, None)
    if not ack[1]:
        LOCKS.release_owner(bid)
        return err("Booking service is busy, please try again.", 503)
    return jsonify(bookID=bid, status="PENDING", amount=booking["amount"], hold_seconds=HOLD_SECONDS,
                   payment_url=f"/v1/payment/{bid}"), 201


# D. PAYMENT  GET /v1/payment/<bookID>  (+ POST to pay)
@app.get("/v1/payment/<bid>")
def payment_info(bid):
    b = get_booking(bid)
    return jsonify(public_booking(b)) if b else err("Booking not found.", 404)


@app.post("/v1/payment/<bid>")
def payment_pay(bid):
    b = get_booking(bid)
    if not b:
        return err("Booking not found.", 404)
    if b["status"] == "CONFIRMED":
        return jsonify(public_booking(b))                    # idempotent
    if b["status"] != "PENDING":
        return err("This reservation hold has expired or was cancelled. Please search again.", 410)
    with PAY_MUTEX:
        if bid in PAYING:
            return err("A payment for this booking is already in progress.", 409)
        PAYING.add(bid)
    try:
        d = request.get_json(force=True, silent=True) or {}
        res = stripe_charge(b["amount"], d.get("number"), d.get("name"), d.get("exp"), d.get("cvc"))
        log("Stripe", f"charge {b['amount']:.2f} for {bid} → {'succeeded' if res['ok'] else 'FAILED'}")
        if not res["ok"]:
            return err(res["message"], 402)
        n = BK.x("""UPDATE bookings SET status='CONFIRMED', paid_at=?, payment_ref=?, card_last4=?
                    WHERE id=? AND status='PENDING'""", (time.time(), res["ref"], res["last4"], bid))
        if not n:
            log("Stripe", f"hold expired mid-payment → refund {res['ref']}")
            return err("Your hold expired during payment; the charge was refunded.", 410)
        LOCKS.persist_owner(bid)
        log("RedisLock", f"hold for {bid} made permanent (booking confirmed)")
        KAFKA.publish("booking.events", {"type": "PAYMENT_SUCCEEDED", "booking_id": bid,
                                         "amount": b["amount"], "ref": res["ref"]})
        return jsonify(public_booking(BK.one("SELECT * FROM bookings WHERE id=?", (bid,))))
    finally:
        with PAY_MUTEX:
            PAYING.discard(bid)


# Hotel Booking Management Service: View Booking (customer / manager) 
@app.get("/v1/booking/<bid>")
def customer_booking(bid):
    b = get_booking(bid)
    email = (request.args.get("email") or "").strip().lower()
    if not b or b["email"].lower() != email:                 # no login: booking ID + email is the credential
        return err("We couldn't find a booking with that ID and email.", 404)
    return jsonify(public_booking(b, pii=True))


@app.post("/v1/booking/<bid>/cancel")
def cancel_booking(bid):
    b = get_booking(bid)
    email = ((request.get_json(force=True, silent=True) or {}).get("email") or "").strip().lower()
    if not b or b["email"].lower() != email:
        return err("We couldn't find a booking with that ID and email.", 404)
    if b["status"] not in ("PENDING", "CONFIRMED"):
        return err(f"A {b['status'].lower()} booking can't be cancelled.", 409)
    BK.x("UPDATE bookings SET status='CANCELLED' WHERE id=?", (bid,))
    LOCKS.release_owner(bid)
    log("RedisLock", f"released rooms for cancelled booking {bid}")
    KAFKA.publish("booking.events", {"type": "BOOKING_CANCELLED", "booking_id": bid})
    return jsonify(public_booking(BK.one("SELECT * FROM bookings WHERE id=?", (bid,)), pii=True))


@app.get("/v1/manager/bookings")
def manager_bookings():
    status = request.args.get("status")
    rows = BK.q("SELECT * FROM bookings " + ("WHERE status=? " if status else "") + "ORDER BY created DESC LIMIT 200",
                (status,) if status else ())
    for r in rows:                                           # lazily expire stale holds before display
        if r["status"] == "PENDING" and r["hold_expires"] <= time.time():
            expire_booking(r["id"])
    rows = BK.q("SELECT * FROM bookings " + ("WHERE status=? " if status else "") + "ORDER BY created DESC LIMIT 200",
                (status,) if status else ())
    stats = {r["status"]: r["n"] for r in BK.q("SELECT status, COUNT(*) n FROM bookings GROUP BY status")}
    revenue = BK.one("SELECT COALESCE(SUM(amount),0) v FROM bookings WHERE status='CONFIRMED'")["v"]
    return jsonify(bookings=[public_booking(r, pii=True) for r in rows], stats=stats, revenue=revenue)


@app.get("/v1/system/events")
def sys_events():
    since = int(request.args.get("since", 0))
    stats = {"locks": LOCKS.stats(), "kafka": {k: dict(v) for k, v in KAFKA.count.items()},
             "archive_rows": BK.one("SELECT COUNT(*) n FROM archive")["n"],
             "hotels": len(ES.ids), "hold_seconds": HOLD_SECONDS, "instances": SERVICES}
    return jsonify(events=[e for e in list(SYSLOG) if e["id"] > since][-120:], stats=stats)


@app.get("/")
def home():
    return Response(PAGE, mimetype="text/html")


# Front end
PAGE = r"""<!doctype html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="Search hotels, reserve a room and pay. No account needed; manage your stay with a booking ID and email.">
  <title>StayNest</title>
  <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' fill='%230f5c4d'/%3E%3Ctext x='16' y='23' font-size='20' font-family='sans-serif' font-weight='700' text-anchor='middle' fill='white'%3ES%3C/text%3E%3C/svg%3E">

  <style>
    :root {
      --ink: #1c1917;
      --muted: #57534e;
      --line: #d6d3d1;
      --bg: #fafaf9;
      --panel: #ffffff;
      --accent: #0f5c4d;
      --accent-ink: #ffffff;
      --ok: #166534;
      --warn: #92400e;
      --bad: #991b1b;

      --s1: 4px;
      --s2: 8px;
      --s3: 16px;
      --s4: 24px;
      --s5: 40px;

      --radius: 4px;
      --font: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
      --mono: ui-monospace, "SFMono-Regular", Consolas, monospace;
    }

   
    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      font: 16px/1.5 var(--font);
      color: var(--ink);
      background: var(--bg);
    }

    a {
      color: var(--accent);
    }

    h1 {
      margin: 0 0 var(--s2);
      font-size: 1.75rem;
      line-height: 1.2;
    }

    h2 {
      margin: var(--s4) 0 var(--s2);
      font-size: 1.2rem;
    }

    h3 {
      margin: 0;
      font-size: 1.05rem;
    }

    p {
      margin: 0 0 var(--s2);
    }

    .muted {
      color: var(--muted);
    }

    .wrap {
      max-width: 960px;
      margin: 0 auto;
      padding: 0 var(--s3);
    }

    :focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 2px;
    }

    
    .site-header {
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }

    .site-header .wrap {
      display: flex;
      flex-wrap: wrap;
      align-items: baseline;
      justify-content: space-between;
      gap: var(--s2) var(--s4);
      padding-top: var(--s3);
      padding-bottom: var(--s3);
    }

    .brand {
      font-weight: 700;
      font-size: 1.15rem;
      color: var(--ink);
      text-decoration: none;
    }

    .site-nav {
      display: flex;
      flex-wrap: wrap;
      gap: var(--s3);
    }

    .site-nav a {
      color: var(--muted);
      text-decoration: none;
      padding: var(--s1) 0;
      border-bottom: 2px solid transparent;
    }

    .site-nav a[aria-current="page"] {
      color: var(--ink);
      border-bottom-color: var(--accent);
    }

    main {
      padding-top: var(--s4);
      padding-bottom: var(--s5);
    }

   
    label {
      display: block;
      font-size: 0.875rem;
      font-weight: 600;
    }

    input,
    select {
      display: block;
      width: 100%;
      margin-top: var(--s1);
      padding: var(--s2) 10px;
      font: inherit;
      color: var(--ink);
      background: var(--panel);
      border: 1px solid var(--muted);
      border-radius: var(--radius);
    }

    select.inline {
      display: inline-block;
      width: auto;
      margin: 0 0 0 var(--s2);
    }

    .field {
      margin-bottom: var(--s3);
    }

    .row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: var(--s3);
    }

    .btn {
      padding: var(--s2) var(--s3);
      font: inherit;
      font-weight: 600;
      color: var(--accent-ink);
      background: var(--accent);
      border: 1px solid var(--accent);
      border-radius: var(--radius);
      cursor: pointer;
      text-decoration: none;
      display: inline-block;
    }

    .btn:disabled {
      color: var(--muted);
      background: #e7e5e4;
      border-color: #e7e5e4;
      cursor: not-allowed;
    }

    .btn.secondary {
      color: var(--accent);
      background: var(--panel);
    }

    .btn.danger {
      color: var(--bad);
      background: var(--panel);
      border-color: var(--bad);
    }

    .msg {
      margin: var(--s3) 0;
      padding: var(--s2) var(--s3);
      border-left: 4px solid var(--muted);
      background: var(--panel);
    }

    .msg.error {
      border-left-color: var(--bad);
      color: var(--bad);
    }

    .search {
      display: grid;
      grid-template-columns: 2fr 1fr 1fr 80px auto;
      gap: var(--s3);
      align-items: end;
      padding-bottom: var(--s4);
      border-bottom: 1px solid var(--line);
    }

    .toolbar {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: var(--s2) var(--s3);
      margin: var(--s3) 0 var(--s2);
    }

    .results {
      margin: 0;
      padding: 0;
      list-style: none;
    }

    .result {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: var(--s3);
      padding: var(--s3) 0;
      border-bottom: 1px solid var(--line);
    }

    .result:hover {
      background: #f5f5f4;
    }

    .result .price {
      text-align: right;
    }

    .price strong {
      font-size: 1.3rem;
    }

    .avail {
      margin: var(--s1) 0 0;
      font-size: 0.875rem;
      font-weight: 600;
    }

    .ok {
      color: var(--ok);
    }

    .warn {
      color: var(--warn);
    }

    .bad {
      color: var(--bad);
    }

    .pager {
      display: flex;
      align-items: center;
      gap: var(--s3);
      margin-top: var(--s3);
    }

    .dates {
      display: flex;
      flex-wrap: wrap;
      gap: var(--s3);
      align-items: end;
      margin: var(--s3) 0;
    }

    .dates label {
      min-width: 160px;
    }

    .table-wrap {
      overflow-x: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9375rem;
    }

    th,
    td {
      padding: 10px var(--s2);
      text-align: left;
      vertical-align: top;
      border-bottom: 1px solid var(--line);
    }

    th {
      font-size: 0.8125rem;
      color: var(--muted);
      font-weight: 600;
    }

    td.num,
    th.num {
      text-align: right;
    }

    dialog {
      width: min(460px, calc(100vw - 32px));
      padding: var(--s4);
      border: 1px solid var(--muted);
      border-radius: var(--radius);
    }

    dialog::backdrop {
      background: rgba(28, 25, 23, 0.5);
    }

    .actions {
      display: flex;
      gap: var(--s2);
      margin-top: var(--s3);
    }

    .split {
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: var(--s5);
      align-items: start;
    }

    .summary {
      margin: 0;
    }

    .summary div {
      display: flex;
      justify-content: space-between;
      gap: var(--s3);
      padding: var(--s2) 0;
      border-bottom: 1px solid var(--line);
    }

    .summary dt {
      color: var(--muted);
    }

    .summary dd {
      margin: 0;
      font-weight: 600;
      text-align: right;
    }

    .summary .total {
      font-size: 1.15rem;
    }

    .countdown {
      font-size: 2rem;
      font-weight: 700;
      font-variant-numeric: tabular-nums;
      margin: 0 0 var(--s3);
    }

    .booking-id {
      font: 700 1.75rem var(--mono);
      letter-spacing: 0.04em;
      margin: var(--s2) 0 var(--s3);
    }

    details {
      margin-top: var(--s3);
      font-size: 0.875rem;
      color: var(--muted);
    }

    code {
      font-family: var(--mono);
    }

    .status {
      font-weight: 700;
    }

    .status-confirmed {
      color: var(--ok);
    }

    .status-pending {
      color: var(--warn);
    }

    .status-cancelled {
      color: var(--bad);
    }

    .status-expired {
      color: var(--muted);
    }

    .log {
      height: 360px;
      overflow: auto;
      padding: var(--s2) var(--s3);
      font: 0.8125rem/1.7 var(--mono);
      background: var(--panel);
      border: 1px solid var(--line);
    }

    .log b {
      display: inline-block;
      min-width: 100px;
    }

    .counts {
      display: flex;
      flex-wrap: wrap;
      gap: var(--s2) var(--s4);
      margin: 0 0 var(--s3);
    }

    .counts dt {
      font-size: 0.8125rem;
      color: var(--muted);
    }

    .counts dd {
      margin: 0;
      font-size: 1.2rem;
      font-weight: 700;
    }

    @media (max-width: 760px) {
      .search {
        grid-template-columns: 1fr 1fr;
      }

      .search .wide {
        grid-column: 1 / -1;
      }

      .split {
        grid-template-columns: 1fr;
        gap: var(--s4);
      }

      .result {
        grid-template-columns: 1fr;
        gap: var(--s2);
      }

      .result .price {
        text-align: left;
      }

      .btn {
        padding: 12px var(--s3);
      }
    }
  </style>
</head>

<body>
  <header class="site-header">
    <div class="wrap">
      <a class="brand" href="#/">StayNest</a>
      <nav class="site-nav" aria-label="Main">
        <a href="#/" data-route="search">Search hotels</a>
        <a href="#/booking" data-route="booking">Find my booking</a>
        <a href="#/manager" data-route="manager">All bookings</a>
        <a href="#/system" data-route="system">System log</a>
      </nav>
    </div>
  </header>

  <main class="wrap" id="app"></main>
  <dialog id="reserve"></dialog>

  <script>
    const app = document.querySelector('#app');
    const dlg = document.querySelector('#reserve');

    const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));

    const money = n => '$' + Number(n).toLocaleString(undefined, {
      minimumFractionDigits: Number.isInteger(+n) ? 0 : 2,
      maximumFractionDigits: 2
    });

    const iso = d => {
      const z = new Date(d.getTime() - d.getTimezoneOffset() * 60000);
      return z.toISOString().slice(0, 10);
    };
    const addDays = (s, n) => {
      const d = new Date(s + 'T00:00:00');
      d.setDate(d.getDate() + n);
      return iso(d);
    };
    const fmtDate = s => new Date(s + 'T00:00:00').toLocaleDateString(undefined, {
      day: 'numeric', month: 'short', year: 'numeric'
    });
    const fmtTime = t => t ? new Date(t * 1000).toLocaleString() : '-';
    const mmss = s => String(Math.floor(s / 60)).padStart(2, '0') + ':' + String(s % 60).padStart(2, '0');
    const setTitle = t => { document.title = t + ' - StayNest'; };
    const statusLabel = s => `<span class="status status-${s.toLowerCase()}">${s.charAt(0) + s.slice(1).toLowerCase()}</span>`;
    const errorBox = m => `<p class="msg error" role="alert">${esc(m)}</p>`;

    const today = iso(new Date());
    const S = { q: '', ci: addDays(today, 1), co: addDays(today, 3), guests: 2, sort: 'relevance', stars: 0, page: 1 };

    let cleanups = [];
    const later = fn => cleanups.push(fn);

    async function api(path, opts) {
      const res = await fetch(path, opts);
      let body = {};
      try { body = await res.json(); } catch (e) { /* non-JSON error body */ }
      if (!res.ok) {
        throw Object.assign(new Error(body.error || `Request failed (${res.status})`), { status: res.status });
      }
      return body;
    }

    const postJSON = (path, data) => api(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });

    function route() {
      cleanups.forEach(fn => fn());
      cleanups = [];
      if (dlg.open) dlg.close();

      const parts = (location.hash.slice(1) || '/').split('?')[0].split('/').filter(Boolean);
      const section = { hotel: 'search', pay: 'search', done: 'search' }[parts[0]] || parts[0] || 'search';
      document.querySelectorAll('.site-nav a').forEach(a => {
        if (a.dataset.route === section) a.setAttribute('aria-current', 'page');
        else a.removeAttribute('aria-current');
      });
      window.scrollTo(0, 0);

      const pages = {
        hotel: () => pageHotel(parts[1]),
        pay: () => pagePay(parts[1]),
        done: () => pageDone(parts[1]),
        booking: pageLookup,
        manager: pageManager,
        system: pageSystem
      };
      (pages[parts[0]] || pageSearch)();
    }
    addEventListener('hashchange', route);

    function availability(h) {
      if (h.available_rooms === undefined) return '';
      if (h.available_rooms === 0) return '<p class="avail bad">Sold out for these dates</p>';
      if (h.available_rooms <= 3) return `<p class="avail warn">${h.available_rooms} room${h.available_rooms > 1 ? 's' : ''} left for these dates</p>`;
      return `<p class="avail ok">${h.available_rooms} rooms available</p>`;
    }

    const resultRow = h => `
      <li class="result">
        <div>
          <h3><a href="#/hotel/${h.id}">${esc(h.name)}</a></h3>
          <p class="muted">${esc(h.city)}, ${esc(h.country)} · ${h.stars}-star · rated ${h.rating} (${h.reviews.toLocaleString()} reviews)</p>
          <p class="muted">${h.amenities.slice(0, 5).map(esc).join(', ')}</p>
        </div>
        <div class="price">
          <strong>${money(h.from_price)}</strong> per night
          ${availability(h)}
        </div>
      </li>`;

    function pageSearch() {
      setTitle('Search hotels');
      app.innerHTML = `
        <h1>Search hotels</h1>
        <p class="muted">No account required. You manage a booking with its ID and your email.</p>

        <form id="search-form" class="search">
          <label class="wide">Destination, hotel or amenity
            <input id="q" placeholder="Pokhara, Tokyo, pool, airport shuttle" value="${esc(S.q)}">
          </label>
          <label>Check-in
            <input type="date" id="ci" min="${today}" value="${S.ci}">
          </label>
          <label>Check-out
            <input type="date" id="co" min="${S.ci}" value="${S.co}">
          </label>
          <label>Guests
            <select id="guests">
              ${[1, 2, 3, 4, 5].map(n => `<option ${n === S.guests ? 'selected' : ''}>${n}</option>`).join('')}
            </select>
          </label>
          <button class="btn">Search</button>
        </form>

        <div class="toolbar">
          <p id="count" class="muted" role="status"></p>
          <div>
            <label style="display:inline">Minimum rating
              <select id="stars" class="inline">
                ${[[0, 'Any'], [3, '3 stars'], [4, '4 stars'], [5, '5 stars']]
                  .map(([v, t]) => `<option value="${v}" ${v === S.stars ? 'selected' : ''}>${t}</option>`).join('')}
              </select>
            </label>
            <label style="display:inline; margin-left:16px">Sort by
              <select id="sort" class="inline">
                ${[['relevance', 'Best match'], ['price_asc', 'Price, low to high'], ['price_desc', 'Price, high to low'], ['rating', 'Guest rating']]
                  .map(([v, t]) => `<option value="${v}" ${v === S.sort ? 'selected' : ''}>${t}</option>`).join('')}
              </select>
            </label>
          </div>
        </div>

        <ol id="results" class="results"></ol>
        <div id="pager" class="pager"></div>`;

      const ci = document.querySelector('#ci');
      const co = document.querySelector('#co');
      ci.onchange = () => {
        S.ci = ci.value;
        co.min = S.ci;
        if (co.value <= S.ci) { S.co = addDays(S.ci, 1); co.value = S.co; }
      };
      co.onchange = () => { S.co = co.value; };

      document.querySelector('#search-form').onsubmit = e => {
        e.preventDefault();
        S.q = document.querySelector('#q').value.trim();
        S.ci = ci.value;
        S.co = co.value;
        S.guests = +document.querySelector('#guests').value;
        S.page = 1;
        runSearch();
      };
      document.querySelector('#stars').onchange = e => { S.stars = +e.target.value; S.page = 1; runSearch(); };
      document.querySelector('#sort').onchange = e => { S.sort = e.target.value; S.page = 1; runSearch(); };
      runSearch();
    }

    async function runSearch() {
      const list = document.querySelector('#results');
      const count = document.querySelector('#count');
      count.textContent = 'Searching…';
      try {
        const params = new URLSearchParams({
          query: S.q, checkin: S.ci, checkout: S.co,
          page: S.page, page_size: 6, sort: S.sort, min_stars: S.stars
        });
        const r = await api('v1/search?' + params);
        count.textContent = `${r.total} hotel${r.total === 1 ? '' : 's'} · ${fmtDate(S.ci)} to ${fmtDate(S.co)}`;

        list.innerHTML = r.results.length
          ? r.results.map(resultRow).join('')
          : `<li class="result"><div>
               <p><strong>No hotels match "${esc(S.q)}"${S.stars ? ' at ' + S.stars + '+ stars' : ''}.</strong></p>
               <p class="muted">Search by city (Kathmandu, Tokyo, Dubai), hotel name, or an amenity such as pool or spa. Lower the minimum rating to see more.</p>
             </div></li>`;

        document.querySelector('#pager').innerHTML = r.total_pages > 1 ? `
          <button class="btn secondary" id="prev" ${r.page <= 1 ? 'disabled' : ''}>Previous</button>
          <span class="muted">Page ${r.page} of ${r.total_pages}</span>
          <button class="btn secondary" id="next" ${r.page >= r.total_pages ? 'disabled' : ''}>Next</button>` : '';
        const prev = document.querySelector('#prev');
        const next = document.querySelector('#next');
        if (prev) prev.onclick = () => { S.page--; runSearch(); };
        if (next) next.onclick = () => { S.page++; runSearch(); };
      } catch (e) {
        count.textContent = '';
        list.innerHTML = `<li>${errorBox(e.message)}</li>`;
      }
    }

    async function pageHotel(id) {
      app.innerHTML = '<p class="muted">Loading hotel…</p>';
      try {
        const h = await api(`v1/hotel/${id}?checkin=${S.ci}&checkout=${S.co}`);
        setTitle(h.name);
        app.innerHTML = `
          <p><a href="#/">&larr; Back to search</a></p>
          <h1>${esc(h.name)}</h1>
          <p class="muted">${esc(h.city)}, ${esc(h.country)} · ${h.stars}-star · rated ${h.rating} (${h.reviews.toLocaleString()} reviews)</p>
          <p>${esc(h.description)}</p>
          <p><strong>Amenities:</strong> ${h.amenities.map(esc).join(', ')}</p>

          <h2>Rooms</h2>
          <div class="dates">
            <label>Check-in <input type="date" id="hci" min="${today}" value="${S.ci}"></label>
            <label>Check-out <input type="date" id="hco" min="${S.ci}" value="${S.co}"></label>
            <p class="muted">${h.nights} night${h.nights === 1 ? '' : 's'}</p>
          </div>

          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Room</th>
                  <th>Sleeps</th>
                  <th class="num">Per night</th>
                  <th class="num">${h.nights} night${h.nights === 1 ? '' : 's'}, before tax</th>
                  <th>Availability</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                ${h.rooms.map(r => `
                  <tr>
                    <td><strong>${esc(r.room_type)}</strong><br><span class="muted">${esc(r.beds)}</span></td>
                    <td>${r.capacity}</td>
                    <td class="num">${money(r.nightly_price)}</td>
                    <td class="num">${money(r.nightly_price * h.nights)}</td>
                    <td>${r.available === 0 ? '<span class="bad">Sold out</span>'
                         : `<span class="${r.available <= 2 ? 'warn' : 'ok'}">${r.available} left</span>`}</td>
                    <td class="num">
                      <button class="btn" data-room="${esc(r.room_type)}" ${r.available === 0 ? 'disabled' : ''}>Reserve</button>
                    </td>
                  </tr>`).join('')}
              </tbody>
            </table>
          </div>`;

        const reload = () => pageHotel(id);
        document.querySelector('#hci').onchange = e => {
          S.ci = e.target.value;
          if (S.co <= S.ci) S.co = addDays(S.ci, 1);
          reload();
        };
        document.querySelector('#hco').onchange = e => { S.co = e.target.value; reload(); };
        app.querySelectorAll('button[data-room]').forEach(b => {
          b.onclick = () => reserveDialog(h, h.rooms.find(r => r.room_type === b.dataset.room));
        });
      } catch (e) {
        app.innerHTML = `<p><a href="#/">&larr; Back to search</a></p>${errorBox(e.message)}`;
      }
    }

    function reserveDialog(h, r) {
      dlg.innerHTML = `
        <h2 style="margin-top:0">Reserve ${esc(r.room_type)}</h2>
        <p class="muted">${esc(h.name)} · ${fmtDate(S.ci)} to ${fmtDate(S.co)} · ${money(r.nightly_price * h.nights)} before tax</p>

        <form id="reserve-form">
          <div class="field">
            <label>Full name <input id="gname" required minlength="2" autocomplete="name"></label>
          </div>
          <div class="field">
            <label>Email <input id="gemail" type="email" required autocomplete="email"></label>
          </div>
          <div class="row">
            <div class="field">
              <label>Phone (optional) <input id="gphone" type="tel" autocomplete="tel"></label>
            </div>
            <div class="field">
              <label>Guests
                <select id="gguests">
                  ${Array.from({ length: r.capacity }, (_, i) =>
                    `<option ${i + 1 === Math.min(S.guests, r.capacity) ? 'selected' : ''}>${i + 1}</option>`).join('')}
                </select>
              </label>
            </div>
          </div>
          <p class="muted">The room is held for ${h.hold_minutes} minutes while you pay. If the hold runs out, it is released to other guests.</p>
          <div id="reserve-error"></div>
          <div class="actions">
            <button class="btn" id="reserve-submit">Continue to payment</button>
            <button type="button" class="btn secondary" id="reserve-cancel">Cancel</button>
          </div>
        </form>`;
      dlg.showModal();
      document.querySelector('#reserve-cancel').onclick = () => dlg.close();

      document.querySelector('#reserve-form').onsubmit = async e => {
        e.preventDefault();
        const submit = document.querySelector('#reserve-submit');
        submit.disabled = true;
        submit.textContent = 'Reserving…';
        const email = document.querySelector('#gemail').value;
        try {
          const res = await postJSON('v1/book', {
            hotelID: h.id,
            roomType: r.room_type,
            checkin: S.ci,
            checkout: S.co,
            guests: +document.querySelector('#gguests').value,
            name: document.querySelector('#gname').value,
            email,
            phone: document.querySelector('#gphone').value
          });
          try { sessionStorage.setItem('lookup', JSON.stringify({ id: res.bookID, email })); } catch (err) { /* storage unavailable */ }
          dlg.close();
          location.hash = '#/pay/' + res.bookID;
        } catch (err) {
          submit.disabled = false;
          submit.textContent = 'Continue to payment';
          document.querySelector('#reserve-error').innerHTML = errorBox(err.message);
        }
      };
    }

    const summaryRows = b => `
      <div><dt>Hotel</dt><dd>${esc(b.hotel_name)}</dd></div>
      <div><dt>Room</dt><dd>${esc(b.room_type)}</dd></div>
      <div><dt>Dates</dt><dd>${fmtDate(b.checkin)} to ${fmtDate(b.checkout)}</dd></div>
      <div><dt>Guests</dt><dd>${b.guests}</dd></div>
      <div><dt>${b.nights} night${b.nights > 1 ? 's' : ''}</dt><dd>${money(b.subtotal)}</dd></div>
      <div><dt>Taxes and fees</dt><dd>${money(b.taxes)}</dd></div>
      <div class="total"><dt>Total</dt><dd>${money(b.amount)}</dd></div>`;

    async function pagePay(bid) {
      app.innerHTML = '<p class="muted">Loading booking…</p>';
      try {
        const b = await api('v1/payment/' + bid);
        if (b.status === 'CONFIRMED') { location.hash = '#/done/' + bid; return; }
        if (b.status !== 'PENDING') {
          setTitle('Reservation ' + b.status.toLowerCase());
          app.innerHTML = `
            <h1>This reservation ${b.status === 'EXPIRED' ? 'expired' : 'was cancelled'}</h1>
            <p>The room is no longer held for you. Availability may have changed, so search again to pick a room.</p>
            <a class="btn" href="#/">Search hotels</a>`;
          return;
        }

        setTitle('Pay for booking ' + b.bookID);
        let left = b.seconds_left;
        app.innerHTML = `
          <h1>Payment</h1>
          <div class="split">
            <section>
              <p class="muted" style="margin-bottom:0">Room held for</p>
              <p class="countdown" id="timer" role="timer">${mmss(left)}</p>

              <form id="pay-form">
                <div class="field">
                  <label>Name on card <input id="cname" required autocomplete="cc-name" value="${esc(b.guest_name)}"></label>
                </div>
                <div class="field">
                  <label>Card number <input id="cnum" required inputmode="numeric" autocomplete="cc-number" placeholder="4242 4242 4242 4242" maxlength="23"></label>
                </div>
                <div class="row">
                  <div class="field">
                    <label>Expiry (MM/YY) <input id="cexp" required autocomplete="cc-exp" placeholder="12/30" maxlength="7"></label>
                  </div>
                  <div class="field">
                    <label>Security code <input id="ccvc" required inputmode="numeric" autocomplete="cc-csc" maxlength="4"></label>
                  </div>
                </div>
                <div id="pay-error"></div>
                <button class="btn" id="pay-submit">Pay ${money(b.amount)}</button>
              </form>

              <details>
                <summary>Test card numbers</summary>
                <p>This is a demo gateway in test mode. Use any future expiry date and any security code. Card numbers are never stored.</p>
                <p><code>4242 4242 4242 4242</code> succeeds<br>
                   <code>4000 0000 0000 0002</code> is declined<br>
                   <code>4000 0000 0000 9995</code> has insufficient funds</p>
              </details>
            </section>

            <section>
              <h2 style="margin-top:0">Booking ${b.bookID}</h2>
              <dl class="summary">${summaryRows(b)}</dl>
            </section>
          </div>`;

        const timer = setInterval(() => {
          left--;
          const el = document.querySelector('#timer');
          if (!el) return;
          el.textContent = mmss(Math.max(0, left));
          if (left <= 0) { clearInterval(timer); pagePay(bid); }
        }, 1000);
        later(() => clearInterval(timer));

        document.querySelector('#cnum').oninput = e => {
          e.target.value = e.target.value.replace(/\D/g, '').slice(0, 19).replace(/(.{4})/g, '$1 ').trim();
        };
        document.querySelector('#cexp').oninput = e => {
          let v = e.target.value.replace(/\D/g, '').slice(0, 4);
          if (v.length > 2) v = v.slice(0, 2) + '/' + v.slice(2);
          e.target.value = v;
        };

        document.querySelector('#pay-form').onsubmit = async e => {
          e.preventDefault();
          const submit = document.querySelector('#pay-submit');
          submit.disabled = true;
          submit.textContent = 'Contacting card issuer…';
          document.querySelector('#pay-error').innerHTML = '';
          try {
            await postJSON('v1/payment/' + bid, {
              name: document.querySelector('#cname').value,
              number: document.querySelector('#cnum').value,
              exp: document.querySelector('#cexp').value,
              cvc: document.querySelector('#ccvc').value
            });
            location.hash = '#/done/' + bid;
          } catch (err) {
            if (err.status === 410 || err.status === 404) { pagePay(bid); return; }
            submit.disabled = false;
            submit.textContent = 'Pay ' + money(b.amount);
            document.querySelector('#pay-error').innerHTML = errorBox(err.message + ' Your room is still held; try another card.');
          }
        };
      } catch (e) {
        app.innerHTML = `${errorBox(e.message)}<a class="btn" href="#/">Back to search</a>`;
      }
    }

    async function pageDone(bid) {
      try {
        const b = await api('v1/payment/' + bid);
        if (b.status !== 'CONFIRMED') { location.hash = '#/pay/' + bid; return; }
        setTitle('Booking ' + b.bookID + ' confirmed');
        app.innerHTML = `
          <h1>Booking confirmed</h1>
          <p>We received ${money(b.amount)} on the card ending ${b.card_last4}. Keep this booking ID: with your email, it is the only way to view or cancel the stay.</p>
          <p class="booking-id">${b.bookID}</p>

          <dl class="summary" style="max-width:520px">
            ${summaryRows(b).replace(/<div class="total">[\s\S]*$/, '')}
            <div><dt>Room number</dt><dd>${b.room_no}</dd></div>
            <div class="total"><dt>Total paid</dt><dd>${money(b.amount)}</dd></div>
          </dl>

          <div class="actions">
            <a class="btn" href="#/booking">View this booking</a>
            <a class="btn secondary" href="#/">Search again</a>
          </div>`;
      } catch (e) {
        app.innerHTML = errorBox(e.message);
      }
    }

    function pageLookup() {
      setTitle('Find my booking');
      let pre = {};
      try { pre = JSON.parse(sessionStorage.getItem('lookup') || '{}'); } catch (e) { /* ignore */ }

      app.innerHTML = `
        <h1>Find my booking</h1>
        <p class="muted">Enter the booking ID from your confirmation and the email you booked with.</p>
        <form id="lookup-form" style="max-width:420px">
          <div class="field">
            <label>Booking ID <input id="lid" required placeholder="BK-1A2B3C4D" value="${esc(pre.id || '')}"></label>
          </div>
          <div class="field">
            <label>Email <input id="lemail" type="email" required value="${esc(pre.email || '')}"></label>
          </div>
          <button class="btn">Find booking</button>
        </form>
        <div id="lookup-result" style="margin-top:24px"></div>`;

      const find = async () => {
        const out = document.querySelector('#lookup-result');
        const id = document.querySelector('#lid').value.trim().toUpperCase();
        const email = document.querySelector('#lemail').value.trim();
        try {
          showBooking(await api(`v1/booking/${encodeURIComponent(id)}?email=${encodeURIComponent(email)}`));
        } catch (e) {
          out.innerHTML = errorBox(e.message);
        }
      };
      document.querySelector('#lookup-form').onsubmit = e => { e.preventDefault(); find(); };
      if (pre.id && pre.email) find();
    }

    function showBooking(b) {
      const out = document.querySelector('#lookup-result');
      out.innerHTML = `
        <h2 style="margin-top:0">${b.bookID} · ${statusLabel(b.status)}</h2>
        <dl class="summary" style="max-width:520px">
          <div><dt>Hotel</dt><dd>${esc(b.hotel_name)}</dd></div>
          <div><dt>Room</dt><dd>${esc(b.room_type)}, room ${b.room_no}</dd></div>
          <div><dt>Guest</dt><dd>${esc(b.guest_name)} (${b.guests})</dd></div>
          <div><dt>Dates</dt><dd>${fmtDate(b.checkin)} to ${fmtDate(b.checkout)}</dd></div>
          <div><dt>Total</dt><dd>${money(b.amount)}</dd></div>
          ${b.status === 'PENDING' ? `<div><dt>Hold expires in</dt><dd>${mmss(b.seconds_left)}</dd></div>` : ''}
          ${b.card_last4 ? `<div><dt>Paid with card ending</dt><dd>${b.card_last4}</dd></div>` : ''}
        </dl>
        <div class="actions">
          ${b.status === 'PENDING' ? `<a class="btn" href="#/pay/${b.bookID}">Complete payment</a>` : ''}
          ${['PENDING', 'CONFIRMED'].includes(b.status) ? '<button class="btn danger" id="cancel-booking">Cancel booking</button>' : ''}
        </div>
        <div id="cancel-msg"></div>`;

      const cancel = document.querySelector('#cancel-booking');
      if (cancel) cancel.onclick = async () => {
        if (!confirm(`Cancel booking ${b.bookID}? The room will be released immediately.`)) return;
        try {
          const updated = await postJSON(`v1/booking/${b.bookID}/cancel`, { email: document.querySelector('#lemail').value });
          showBooking(updated);
          document.querySelector('#cancel-msg').innerHTML = '<p class="msg">Booking cancelled and the room released.</p>';
        } catch (e) {
          document.querySelector('#cancel-msg').innerHTML = errorBox(e.message);
        }
      };
    }

    async function pageManager() {
      setTitle('All bookings');
      app.innerHTML = `
        <h1>All bookings</h1>
        <div class="toolbar">
          <p id="manager-summary" class="muted" role="status"></p>
          <div>
            <label style="display:inline">Status
              <select id="status-filter" class="inline">
                <option value="">All</option>
                <option value="CONFIRMED">Confirmed</option>
                <option value="PENDING">Pending</option>
                <option value="EXPIRED">Expired</option>
                <option value="CANCELLED">Cancelled</option>
              </select>
            </label>
          </div>
        </div>
        <div id="manager-table" class="table-wrap"></div>`;

      const load = async () => {
        const filter = document.querySelector('#status-filter');
        if (!filter) return;
        try {
          const r = await api('v1/manager/bookings' + (filter.value ? '?status=' + filter.value : ''));
          const st = r.stats;
          document.querySelector('#manager-summary').textContent =
            `${st.CONFIRMED || 0} confirmed, ${st.PENDING || 0} pending, ${st.EXPIRED || 0} expired, ${st.CANCELLED || 0} cancelled · revenue ${money(r.revenue)}`;

          document.querySelector('#manager-table').innerHTML = r.bookings.length ? `
            <table>
              <thead>
                <tr>
                  <th>Booking</th><th>Hotel</th><th>Room</th><th>Stay</th><th>Guest</th>
                  <th class="num">Total</th><th>Status</th><th>Created</th>
                </tr>
              </thead>
              <tbody>
                ${r.bookings.map(b => `
                  <tr>
                    <td><code>${b.bookID}</code></td>
                    <td>${esc(b.hotel_name)}</td>
                    <td>${esc(b.room_type)} #${b.room_no}</td>
                    <td>${b.checkin} to ${b.checkout}</td>
                    <td>${esc(b.guest_name)}<br><span class="muted">${esc(b.email)}</span></td>
                    <td class="num">${money(b.amount)}</td>
                    <td>${statusLabel(b.status)}</td>
                    <td>${fmtTime(b.created)}</td>
                  </tr>`).join('')}
              </tbody>
            </table>`
            : `<p><strong>No ${filter.value ? filter.value.toLowerCase() + ' ' : ''}bookings yet.</strong></p>
               <p class="muted">Bookings appear here as soon as a guest reserves a room from the search page.</p>`;
        } catch (e) {
          document.querySelector('#manager-table').innerHTML = errorBox(e.message);
        }
      };

      document.querySelector('#status-filter').onchange = load;
      load();
      const timer = setInterval(load, 5000);
      later(() => clearInterval(timer));
    }

    function pageSystem() {
      setTitle('System log');
      app.innerHTML = `
        <h1>System log</h1>
        <p class="muted">Search and hotel pages read from the index and never take locks, so they stay available. Booking and payment take a 10-minute lock on each room-night before anything is written, so two guests cannot hold the same room.</p>
        <dl id="counts" class="counts"></dl>
        <div class="log" id="log" aria-label="Live system events"></div>`;

      let since = 0;
      const poll = async () => {
        const log = document.querySelector('#log');
        if (!log) return;
        try {
          const r = await api('v1/system/events?since=' + since);
          const nearBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 30;
          r.events.forEach(e => {
            since = Math.max(since, e.id);
            log.insertAdjacentHTML('beforeend', `<div>${e.t} <b>${esc(e.c)}</b> ${esc(e.m)}</div>`);
          });
          if (nearBottom) log.scrollTop = log.scrollHeight;

          const s = r.stats, k = s.kafka;
          const pair = t => `${k[t]?.consumed || 0} / ${k[t]?.produced || 0}`;
          document.querySelector('#counts').innerHTML = [
            ['Hotels indexed', s.hotels],
            ['Room-night locks', s.locks.total],
            ['Locks on 10-minute hold', s.locks.holding],
            ['booking.requests consumed / produced', pair('booking.requests')],
            ['booking.events consumed / produced', pair('booking.events')],
            ['Archived events', s.archive_rows]
          ].map(([label, value]) => `<div><dt>${label}</dt><dd>${value}</dd></div>`).join('');
        } catch (e) { /* retry on next tick */ }
      };
      poll();
      const timer = setInterval(poll, 1500);
      later(() => clearInterval(timer));
    }

    route();
  </script>
</body>

</html>"""

_srv = globals().get("_srv")
try:
    if _srv:
        _srv.shutdown()          # allow re-running the cell
except Exception:
    pass
from werkzeug.serving import make_server
_srv = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=_srv.serve_forever, daemon=True).start()
log("System", f"StayNest started on port {PORT}")

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height=950)
except ImportError:
    print(f"Not running in Colab. Open http://localhost:{PORT}")