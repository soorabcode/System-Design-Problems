#!/usr/bin/env python3
"""
E-COMMERCE SYSTE]M 

1. Requirement analysis 
A. functional requrement 
- user must be able to login using id and password
- search and view products 
- place order
- payment and checkout 
- status check of order 
B. non functional requrement 
- scale : 10m dau and 10 order/sec
- availability 
- latency of search < 200ms 

2. Identifying Entities 
- user
- products 
- cart
- order
- checkout 

3. API 
A. search 
GET /v1/search?query={search}-> List<productID> :pagenation 
B. View 
GET /v1/product={productD} -> product details 
C. Cart
POST /v1//cart/product={add all productID}
D. Checkout 
POST /v1/checkout {post body : propdouctId } -> order id
Ppost /v1/paymentp{ppst bodu : orderid }-> conformation 
E. Status 
GET /v1/status/{orderID}


4. Hih level design 
user -> api gateway -> sever ( login , search , product m cart , checout , status ) -> database

5. low level design 
uploaded image

"""
import hashlib
import hmac
import itertools
import json
import queue
import re
import secrets
import sqlite3
import threading
import time
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PORT = 8000


# Generic SQLite wrapper (thread-safe) : used for User DB, Cart DB, Order DB

class DB:
    def __init__(self, ddl):
        self.c = sqlite3.connect(":memory:", check_same_thread=False)
        self.c.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.c.executescript(ddl)

    def q(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.c.execute(sql, args).fetchall()]

    def x(self, sql, args=()):
        with self.lock:
            cur = self.c.execute(sql, args)
            self.c.commit()
            return cur.lastrowid


# USER DB (MySQL role): name, email, password (salted hash)

USER_DB = DB("""
CREATE TABLE users(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT,
                   email TEXT UNIQUE, password_hash TEXT, salt TEXT);
""")


def hash_pw(pw, salt):
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 50_000).hex()


def seed_user(name, email, pw):
    salt = secrets.token_hex(8)
    USER_DB.x("INSERT INTO users(name,email,password_hash,salt) VALUES(?,?,?,?)",
              (name, email, hash_pw(pw, salt), salt))


seed_user("Demo User", "user@sg.com", "1234")

SESSIONS = {} 

# PRODUCT DB (MongoDB role): document store with name, price, image ...
# Every write is emitted to the oplog -> CDC pipeline -> Elasticsearch
CDC_QUEUE = queue.Queue()
PRODUCT_LOCK = threading.RLock()
PRODUCTS = {}

CATALOG = [
    ("Wireless Headphones", 89.99, "🎧", "Electronics", "Noise cancelling over-ear bluetooth headphones with 30h battery", 40),
    ("Smart Watch", 149.00, "⌚", "Electronics", "Fitness tracking smartwatch with heart rate and GPS", 25),
    ("Gaming Laptop", 1199.00, "💻", "Electronics", "15 inch laptop with RTX graphics and 16GB RAM", 8),
    ("Mechanical Keyboard", 74.50, "⌨️", "Electronics", "RGB mechanical keyboard with hot swappable switches", 60),
    ("Wireless Mouse", 29.99, "🖱️", "Electronics", "Ergonomic wireless mouse with silent clicks", 120),
    ("4K Monitor", 329.00, "🖥️", "Electronics", "27 inch 4K IPS monitor with HDR support", 15),
    ("Smartphone", 699.00, "📱", "Electronics", "Flagship smartphone with OLED display and triple camera", 30),
    ("Digital Camera", 549.00, "📷", "Electronics", "Mirrorless camera with 24MP sensor and 4K video", 12),
    ("Running Shoes", 64.99, "👟", "Fashion", "Lightweight breathable running shoes for daily training", 80),
    ("Leather Backpack", 79.00, "🎒", "Fashion", "Durable leather backpack with laptop compartment", 35),
    ("Sunglasses", 24.99, "🕶️", "Fashion", "Polarized UV400 sunglasses unisex design", 150),
    ("Denim Jacket", 54.00, "🧥", "Fashion", "Classic blue denim jacket slim fit", 45),
    ("Coffee Maker", 59.99, "☕", "Home", "Programmable drip coffee maker with thermal carafe", 50),
    ("Table Lamp", 22.50, "💡", "Home", "Dimmable LED desk lamp with USB charging port", 90),
    ("Ceramic Plant Pot", 14.99, "🪴", "Home", "Minimalist ceramic pot for indoor plants", 100),
    ("Cookware Set", 119.00, "🍳", "Home", "10 piece non stick cookware set induction ready", 20),
    ("Yoga Mat", 19.99, "🧘", "Sports", "Non slip eco friendly yoga mat 6mm thick", 110),
    ("Football", 17.50, "⚽", "Sports", "Official size 5 match football", 70),
    ("Mountain Bike", 459.00, "🚲", "Sports", "21 speed aluminium frame mountain bike", 10),
    ("Dumbbell Set", 49.00, "🏋️", "Sports", "Adjustable dumbbell set 2 to 20 kg", 33),
    ("Programming Book", 39.90, "📘", "Books", "Clean code and software design principles handbook", 75),
    ("System Design Book", 44.90, "📗", "Books", "Designing scalable distributed systems guide", 65),
    ("Notebook Pack", 9.99, "📓", "Books", "Pack of 5 ruled notebooks for students", 200),
    ("Electric Guitar", 279.00, "🎸", "Music", "Solid body electric guitar with amplifier bundle", 9),
]


def product_upsert(doc):
    with PRODUCT_LOCK:
        PRODUCTS[doc["id"]] = doc
    CDC_QUEUE.put(("upsert", dict(doc)))   


for i, (n, p, img, cat, desc, stock) in enumerate(CATALOG, start=1):
    product_upsert({"id": i, "name": n, "price": p, "image": img,
                    "category": cat, "description": desc, "stock": stock})



# ELASTICSEARCH (inverted index) + CDC pipeline + CACHE

def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())


class SearchIndex:
    def __init__(self):
        self.terms = {}        # term -> set(productId)
        self.doc_terms = {}    # productId -> set(term)
        self.lock = threading.RLock()
        self.synced = 0

    def upsert(self, doc):
        with self.lock:
            self.remove(doc["id"])
            ts = set(tokenize(doc["name"] + " " + doc["category"] + " " + doc["description"]))
            self.doc_terms[doc["id"]] = ts
            for t in ts:
                self.terms.setdefault(t, set()).add(doc["id"])
            self.synced += 1

    def remove(self, pid):
        for t in self.doc_terms.pop(pid, set()):
            self.terms.get(t, set()).discard(pid)

    def search(self, query):
        toks = tokenize(query)
        with self.lock:
            if not toks:
                return sorted(self.doc_terms.keys())
            scores = None
            for tok in toks:   # every token must prefix-match (AND semantics)
                hit = {}
                for term, ids in self.terms.items():
                    if term.startswith(tok):
                        w = 3 if term == tok else 1
                        for pid in ids:
                            hit[pid] = hit.get(pid, 0) + w
                scores = hit if scores is None else {k: scores[k] + hit[k] for k in scores if k in hit}
            ranked = sorted((scores or {}).items(), key=lambda kv: (-kv[1], kv[0]))
            return [pid for pid, _ in ranked]


ES = SearchIndex()


class LRUCache:
    def __init__(self, cap=256, ttl=30):
        self.d, self.cap, self.ttl, self.lock = OrderedDict(), cap, ttl, threading.Lock()

    def get(self, k):
        with self.lock:
            v = self.d.get(k)
            if v and v[0] > time.time():
                self.d.move_to_end(k)
                return v[1]
            self.d.pop(k, None)
            return None

    def set(self, k, val):
        with self.lock:
            self.d[k] = (time.time() + self.ttl, val)
            self.d.move_to_end(k)
            while len(self.d) > self.cap:
                self.d.popitem(last=False)

    def clear(self):
        with self.lock:
            self.d.clear()


CACHE = LRUCache()


def cdc_worker():
    """CDC pipeline: Product DB (MongoDB) -> Elasticsearch."""
    while True:
        op, doc = CDC_QUEUE.get()
        time.sleep(0.02)  # simulated pipeline lag
        if op == "upsert":
            ES.upsert(doc)
        CACHE.clear()     # invalidate stale search results


threading.Thread(target=cdc_worker, daemon=True).start()
for _ in range(100):                      # let the initial catalog finish indexing
    if ES.synced >= len(CATALOG):
        break
    time.sleep(0.05)


# CART DB (PostgreSQL role) and ORDER DB (PostgreSQL role)

CART_DB = DB("""
CREATE TABLE carts(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER UNIQUE, items TEXT);
""")
ORDER_DB = DB("""
CREATE TABLE orders(id TEXT PRIMARY KEY, user_id INTEGER, items TEXT, total REAL,
                    status TEXT, created REAL, paid_at REAL, payment_ref TEXT);
""")


def get_cart(uid):
    rows = CART_DB.q("SELECT * FROM carts WHERE user_id=?", (uid,))
    if not rows:
        CART_DB.x("INSERT INTO carts(user_id,items) VALUES(?,?)", (uid, "[]"))
        rows = CART_DB.q("SELECT * FROM carts WHERE user_id=?", (uid,))
    return rows[0]["id"], json.loads(rows[0]["items"])


def save_cart(uid, items):
    CART_DB.x("UPDATE carts SET items=? WHERE user_id=?", (json.dumps(items), uid))


# SERVICES 
def svc_login(inst, c):
    b = c["body"]
    email = str(b.get("email", "")).strip().lower()
    pw = str(b.get("password", ""))
    rows = USER_DB.q("SELECT * FROM users WHERE email=?", (email,))
    if not rows or not hmac.compare_digest(rows[0]["password_hash"], hash_pw(pw, rows[0]["salt"])):
        return 401, {"error": "Invalid email or password"}
    tok = secrets.token_hex(16)
    SESSIONS[tok] = (rows[0]["id"], time.time() + 3600)
    return 200, {"token": tok, "user": {"name": rows[0]["name"], "email": rows[0]["email"]}}


def svc_search(inst, c):
    q = c["query"].get("query", [""])[0].strip().lower()
    page = max(1, int(c["query"].get("page", ["1"])[0] or 1))
    size = min(50, max(1, int(c["query"].get("size", ["8"])[0] or 8)))
    key = f"{q}|{page}|{size}"
    hit = CACHE.get(key)
    if hit:
        c["hdr"]["X-Cache"] = "HIT"
        return 200, hit
    c["hdr"]["X-Cache"] = "MISS"
    time.sleep(0.015)  # simulated Elasticsearch network hop
    ids = ES.search(q)
    total = len(ids)
    out = {"productIds": ids[(page - 1) * size: page * size], "page": page,
           "size": size, "total": total, "pages": max(1, -(-total // size))}
    CACHE.set(key, out)
    return 200, out


def svc_product(inst, c):
    p = PRODUCTS.get(int(c["params"][0]))
    return (200, p) if p else (404, {"error": "Product not found"})


def svc_admin_price(inst, c):
    """Demo: change a price in Product DB; CDC pipeline will sync Elasticsearch."""
    pid = int(c["body"].get("productId", 0))
    p = PRODUCTS.get(pid)
    if not p:
        return 404, {"error": "Product not found"}
    new = dict(p, price=round(float(c["body"].get("price", p["price"])), 2))
    product_upsert(new)
    return 200, {"updated": new, "note": "CDC event queued -> Elasticsearch"}


def cart_view(uid):
    _, items = get_cart(uid)
    lines, total = [], 0.0
    for it in items:
        p = PRODUCTS.get(it["productId"])
        if p:
            lines.append({"productId": p["id"], "name": p["name"], "price": p["price"],
                          "image": p["image"], "qty": it["qty"]})
            total += p["price"] * it["qty"]
    return {"items": lines, "total": round(total, 2), "count": sum(l["qty"] for l in lines)}


def svc_cart_get(inst, c):
    return 200, cart_view(c["user"])


def svc_cart_add(inst, c):
    b = c["body"]
    ids = b.get("productIds") or ([b["productId"]] if "productId" in b else [])
    if not ids:
        return 400, {"error": "productIds required"}
    uid = c["user"]
    _, items = get_cart(uid)
    for pid in ids:
        pid = int(pid)
        if pid not in PRODUCTS:
            return 404, {"error": f"Product {pid} not found"}
        for it in items:
            if it["productId"] == pid:
                it["qty"] += 1
                break
        else:
            items.append({"productId": pid, "qty": 1})
    save_cart(uid, items)
    return 200, cart_view(uid)


def svc_cart_remove(inst, c):
    uid, pid = c["user"], int(c["params"][0])
    _, items = get_cart(uid)
    save_cart(uid, [i for i in items if i["productId"] != pid])
    return 200, cart_view(uid)


def svc_checkout(inst, c):
    uid = c["user"]
    _, items = get_cart(uid)
    sel = c["body"].get("productIds")
    if sel:
        sel = {int(x) for x in sel}
        items_sel = [i for i in items if i["productId"] in sel]
    else:
        items_sel = items
    if not items_sel:
        return 400, {"error": "Cart is empty"}
    lines, total = [], 0.0
    for it in items_sel:
        p = PRODUCTS[it["productId"]]
        if p["stock"] < it["qty"]:
            return 409, {"error": f"Only {p['stock']} left of {p['name']}"}
        lines.append({"productId": p["id"], "name": p["name"], "price": p["price"],
                      "image": p["image"], "qty": it["qty"]})
        total += p["price"] * it["qty"]
    oid = "ORD-" + secrets.token_hex(4).upper()
    ORDER_DB.x("INSERT INTO orders(id,user_id,items,total,status,created) VALUES(?,?,?,?,?,?)",
               (oid, uid, json.dumps(lines), round(total, 2), "PENDING_PAYMENT", time.time()))
    taken = {l["productId"] for l in lines}
    save_cart(uid, [i for i in items if i["productId"] not in taken])
    return 201, {"orderId": oid, "total": round(total, 2), "status": "PENDING_PAYMENT"}


def luhn_ok(num):
    s, alt = 0, False
    for ch in reversed(num):
        d = int(ch)
        if alt:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        s += d
        alt = not alt
    return s % 10 == 0


def payment_gateway(card, amount):
    """Mock external Payment Gateway."""
    time.sleep(0.25)
    num = re.sub(r"\D", "", str(card.get("number", "")))
    if not (13 <= len(num) <= 19) or not luhn_ok(num):
        return False, None, "Invalid card number"
    if not re.fullmatch(r"\d{2}/\d{2}", str(card.get("expiry", ""))):
        return False, None, "Invalid expiry (MM/YY)"
    if not re.fullmatch(r"\d{3,4}", str(card.get("cvv", ""))):
        return False, None, "Invalid CVV"
    if num.endswith("0002"):
        return False, None, "Card declined by issuer"
    return True, "PAY-" + secrets.token_hex(5).upper(), "approved"


def svc_payment(inst, c):
    """Checkout -> Payment API -> Payment Gateway -> Order DB"""
    b = c["body"]
    rows = ORDER_DB.q("SELECT * FROM orders WHERE id=? AND user_id=?", (b.get("orderId"), c["user"]))
    if not rows:
        return 404, {"error": "Order not found"}
    o = rows[0]
    c["hdr"]["X-Path"] = f"checkout-{inst.split('-')[1]} > payment-api > payment-gateway > order-db"
    if o["status"] != "PENDING_PAYMENT":   # idempotent
        return 200, {"confirmation": o["payment_ref"], "orderId": o["id"],
                     "status": "PAID", "amount": o["total"], "note": "already paid"}
    ok, ref, msg = payment_gateway(b.get("card", {}), o["total"])
    if not ok:
        return 402, {"error": msg}
    ORDER_DB.x("UPDATE orders SET status='PAID', paid_at=?, payment_ref=? WHERE id=?",
               (time.time(), ref, o["id"]))
    for l in json.loads(o["items"]):        # decrement stock -> CDC keeps search in sync
        with PRODUCT_LOCK:
            p = dict(PRODUCTS[l["productId"]])
            p["stock"] = max(0, p["stock"] - l["qty"])
        product_upsert(p)
    return 200, {"confirmation": ref, "orderId": o["id"], "status": "PAID", "amount": o["total"]}


def derive_status(o):
    if o["status"] == "PENDING_PAYMENT":
        return "PENDING_PAYMENT"
    age = time.time() - (o["paid_at"] or time.time())
    for limit, name in ((8, "PAID"), (20, "PROCESSING"), (40, "SHIPPED")):
        if age < limit:
            return name
    return "DELIVERED"


def order_out(o):
    return {"orderId": o["id"], "status": derive_status(o), "total": o["total"],
            "items": json.loads(o["items"]), "created": o["created"], "confirmation": o["payment_ref"]}


def svc_status(inst, c):
    rows = ORDER_DB.q("SELECT * FROM orders WHERE id=? AND user_id=?", (c["params"][0].upper(), c["user"]))
    return (200, order_out(rows[0])) if rows else (404, {"error": "Order not found"})


def svc_orders(inst, c):
    rows = ORDER_DB.q("SELECT * FROM orders WHERE user_id=? ORDER BY created DESC", (c["user"],))
    return 200, {"orders": [order_out(o) for o in rows]}


# API GATEWAY (auth + round-robin load balancing + routing)
INSTANCES = {"user": 2, "search": 3, "product": 2, "cart": 2, "checkout": 2, "status": 2}
ROUTES = [  # method, regex, service, handler, auth_required
    ("POST", r"^/v1/login$", "user", svc_login, False),
    ("GET", r"^/v1/search$", "search", svc_search, True),
    ("GET", r"^/v1/product/(\d+)$", "product", svc_product, True),
    ("POST", r"^/v1/admin/price$", "product", svc_admin_price, True),
    ("GET", r"^/v1/cart$", "cart", svc_cart_get, True),
    ("POST", r"^/v1/cart$", "cart", svc_cart_add, True),
    ("DELETE", r"^/v1/cart/(\d+)$", "cart", svc_cart_remove, True),
    ("POST", r"^/v1/checkout$", "checkout", svc_checkout, True),
    ("POST", r"^/v1/payment$", "checkout", svc_payment, True),
    ("GET", r"^/v1/status/([A-Za-z0-9\-]+)$", "status", svc_status, True),
    ("GET", r"^/v1/orders$", "status", svc_orders, True),
]


class Gateway:
    def __init__(self):
        self.rr = {n: itertools.cycle(range(1, k + 1)) for n, k in INSTANCES.items()}
        self.lock = threading.Lock()

    def pick(self, service):   # ROUND ROBIN
        with self.lock:
            return f"{service}-{next(self.rr[service])}"

    def dispatch(self, method, path, query, body, auth):
        t0 = time.perf_counter()
        hdr = {}
        for m, rx, service, fn, need_auth in ROUTES:
            if m != method:
                continue
            mt = re.match(rx, path)
            if not mt:
                continue
            ctx = {"body": body, "query": query, "params": mt.groups(), "user": None, "hdr": hdr}
            if need_auth:
                tok = auth.replace("Bearer ", "").strip()
                sess = SESSIONS.get(tok)
                if not sess or sess[1] < time.time():
                    return 401, {"error": "Unauthorized - please log in"}, {}
                ctx["user"] = sess[0]
            inst = self.pick(service)
            hdr["X-Service"] = inst
            try:
                code, data = fn(inst, ctx)
            except Exception as e:  # noqa
                code, data = 500, {"error": f"Server error: {e}"}
            hdr["X-Latency-Ms"] = f"{(time.perf_counter() - t0) * 1000:.1f}"
            return code, data, hdr
        return 404, {"error": "No such route"}, {}


GATEWAY = Gateway()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json", headers=None):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Expose-Headers", "*")
        for k, v in (headers or {}).items():
            self.send_header(k, str(v))
        self.end_headers()
        self.wfile.write(data)

    def _handle(self, method):
        u = urlparse(self.path)
        if method == "GET" and u.path in ("/", "/index.html"):
            return self._send(200, HTML.encode(), "text/html; charset=utf-8")
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b""
        try:
            body = json.loads(raw) if raw else {}
        except ValueError:
            return self._send(400, {"error": "Bad JSON"})
        code, data, hdr = GATEWAY.dispatch(method, u.path, parse_qs(u.query), body,
                                           self.headers.get("Authorization", ""))
        self._send(code, data, headers=hdr)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_DELETE(self):
        self._handle("DELETE")


# FRONTEND (HTML + CSS + JS, served inline)
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>ShopGrid</title>
    <style>
        :root {
            --bg: #fafaf9;
            --surface: #ffffff;
            --ink: #1a1a1a;
            --mut: #6b6b68;
            --line: #e4e4e1;
            --accent: #1f4d3a;
            --ok: #2f6b3a;
            --bad: #a13a2b;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Helvetica Neue", Arial, sans-serif;
            background: var(--bg);
            color: var(--ink);
        }

        button,
        input {
            font: inherit;
        }

        /* ---------- Header ---------- */
        header {
            position: sticky;
            top: 0;
            z-index: 20;
            background: var(--surface);
            border-bottom: 1px solid var(--line);
            display: flex;
            gap: 16px;
            align-items: center;
            padding: 12px 24px;
            flex-wrap: wrap;
        }

        .logo {
            font-weight: 700;
            font-size: 17px;
            letter-spacing: -0.01em;
            white-space: nowrap;
        }

        .search {
            flex: 1;
            display: flex;
            max-width: 560px;
            min-width: 200px;
        }

        .search input {
            flex: 1;
            min-width: 0;
            border: 1px solid var(--line);
            border-right: 0;
            border-radius: 4px 0 0 4px;
            padding: 9px 12px;
            outline: none;
            background: #fff;
        }

        .search input:focus {
            border-color: var(--ink);
        }

        .search button {
            border: 1px solid var(--ink);
            background: var(--ink);
            color: #fff;
            padding: 0 16px;
            border-radius: 0 4px 4px 0;
            cursor: pointer;
            font-size: 14px;
        }

        .actions {
            display: flex;
            gap: 8px;
            align-items: center;
            margin-left: auto;
        }

        .ghost {
            background: var(--surface);
            color: var(--ink);
            border: 1px solid var(--line);
            padding: 8px 12px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            white-space: nowrap;
        }

        .ghost:hover {
            border-color: var(--ink);
        }

        .count {
            display: inline-block;
            min-width: 18px;
            margin-left: 6px;
            padding: 0 5px;
            border-radius: 3px;
            background: var(--bg);
            font-size: 12px;
            text-align: center;
        }

        /* ---------- Layout ---------- */
        main {
            max-width: 1100px;
            margin: 28px auto;
            padding: 0 24px;
        }

        .meta {
            display: flex;
            justify-content: space-between;
            color: var(--mut);
            font-size: 13px;
            margin-bottom: 16px;
            flex-wrap: wrap;
            gap: 6px;
        }

        /* ---------- Product grid ---------- */
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
            gap: 1px;
            background: var(--line);
            border: 1px solid var(--line);
        }

        .prod {
            background: var(--surface);
            cursor: pointer;
            display: flex;
            flex-direction: column;
        }

        .prod:hover .pn {
            text-decoration: underline;
        }

        .img {
            height: 120px;
            background: var(--bg);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 48px;
        }

        .pb {
            padding: 12px 14px 14px;
            display: flex;
            flex-direction: column;
            gap: 2px;
            flex: 1;
        }

        .pn {
            font-weight: 600;
        }

        .pc {
            font-size: 12px;
            color: var(--mut);
        }

        .oos {
            color: var(--bad);
        }

        .pr {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: auto;
            padding-top: 10px;
        }

        .price {
            font-weight: 700;
            font-variant-numeric: tabular-nums;
        }

        /* ---------- Buttons ---------- */
        .btn {
            background: var(--accent);
            color: #fff;
            border: 1px solid var(--accent);
            padding: 8px 14px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }

        .btn:hover {
            opacity: 0.9;
        }

        .btn.ghost {
            background: var(--surface);
            color: var(--ink);
            border-color: var(--line);
        }

        .btn:disabled {
            opacity: 0.4;
            cursor: not-allowed;
        }

        .btn.block {
            width: 100%;
            padding: 11px;
        }

        /* ---------- Pagination ---------- */
        .pager {
            display: flex;
            gap: 4px;
            justify-content: center;
            margin: 24px 0;
            flex-wrap: wrap;
        }

        .pager button {
            border: 1px solid var(--line);
            background: var(--surface);
            padding: 6px 11px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            color: var(--ink);
        }

        .pager button.on {
            background: var(--ink);
            color: #fff;
            border-color: var(--ink);
        }

        .empty {
            color: var(--mut);
            padding: 48px 0;
            text-align: center;
        }

        /* ---------- Login ---------- */
        .login {
            max-width: 360px;
            margin: 12vh auto;
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 6px;
            padding: 28px;
        }

        .login h1 {
            font-size: 20px;
            margin: 0 0 4px;
        }

        .login p {
            color: var(--mut);
            margin: 0 0 18px;
            font-size: 14px;
        }

        label {
            display: block;
            font-size: 13px;
            margin: 12px 0 4px;
            color: var(--mut);
        }

        input.f {
            width: 100%;
            padding: 10px 12px;
            border: 1px solid var(--line);
            border-radius: 4px;
            background: #fff;
        }

        input.f:focus {
            outline: none;
            border-color: var(--ink);
        }

        .note {
            font-size: 13px;
            color: var(--mut);
            margin-top: 14px;
        }

        .err {
            color: var(--bad);
            font-size: 13px;
            margin-top: 10px;
            min-height: 18px;
        }

        /* ---------- Modal ---------- */
        #ov {
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.4);
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 50;
            padding: 16px;
        }

        #ov.on {
            display: flex;
        }

        .modal {
            background: var(--surface);
            border-radius: 6px;
            width: 100%;
            max-width: 520px;
            max-height: 85vh;
            overflow: auto;
            padding: 24px;
            position: relative;
        }

        .modal h2 {
            font-size: 18px;
            margin: 0 0 14px;
        }

        .x {
            position: absolute;
            right: 14px;
            top: 10px;
            border: 0;
            background: none;
            font-size: 22px;
            cursor: pointer;
            color: var(--mut);
            line-height: 1;
        }

        .row {
            display: flex;
            gap: 12px;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid var(--line);
        }

        .row .em {
            font-size: 26px;
            width: 36px;
            text-align: center;
        }

        .row .gr {
            flex: 1;
        }

        .row small {
            color: var(--mut);
        }

        .tot {
            display: flex;
            justify-content: space-between;
            font-weight: 700;
            padding: 14px 0;
            border-top: 1px solid var(--line);
            margin-top: 6px;
        }

        .two {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
        }

        .summary {
            background: var(--bg);
            padding: 10px 12px;
            border-radius: 4px;
            font-size: 14px;
            margin-bottom: 6px;
        }

        .summary small {
            color: var(--mut);
            font-size: 12px;
        }

        .hint {
            font-size: 12px;
            color: var(--mut);
            margin-top: 10px;
        }

        .center {
            text-align: center;
            padding: 8px 0;
        }

        /* ---------- Order status ---------- */
        .steps {
            display: flex;
            gap: 4px;
            margin: 12px 0;
            font-size: 12px;
            color: var(--mut);
        }

        .step {
            flex: 1;
            border-top: 2px solid var(--line);
            padding-top: 6px;
        }

        .step.done {
            border-color: var(--ok);
            color: var(--ink);
        }

        .step.cur {
            border-color: var(--ink);
            color: var(--ink);
            font-weight: 600;
        }

        .ordcard {
            border: 1px solid var(--line);
            border-radius: 4px;
            padding: 12px;
            margin: 10px 0;
        }

        .ordhead {
            display: flex;
            justify-content: space-between;
            gap: 8px;
        }

        .ordcard small {
            color: var(--mut);
        }

        /* ---------- Toast ---------- */
        .toast {
            position: fixed;
            bottom: 24px;
            left: 50%;
            transform: translateX(-50%);
            background: var(--ink);
            color: #fff;
            padding: 9px 16px;
            border-radius: 4px;
            z-index: 90;
            opacity: 0;
            transition: opacity 0.2s;
            pointer-events: none;
            font-size: 14px;
        }

        .toast.on {
            opacity: 1;
        }
    </style>
</head>
<body>

    <div id="app"></div>
    <div id="ov"><div class="modal" id="modal"></div></div>
    <div class="toast" id="toast"></div>

    <script>
        /* storage that survives blocked sessionStorage (Colab iframes) */
        const mem = {};
        const store = {
            get(k) {
                try { return sessionStorage.getItem(k); }
                catch (e) { return mem[k] ?? null; }
            },
            set(k, v) {
                try { sessionStorage.setItem(k, v); }
                catch (e) { mem[k] = v; }
            },
            clear() {
                try { sessionStorage.clear(); } catch (e) {}
                for (const k in mem) delete mem[k];
            }
        };

        let token = store.get('tok');
        let me = JSON.parse(store.get('me') || 'null');
        let state = { q: '', page: 1, cart: { items: [], total: 0, count: 0 } };
        let poll = null;

        const $ = id => document.getElementById(id);
        const esc = s => String(s).replace(/[&<>"']/g, c => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[c]));
        const money = n => '$' + Number(n).toFixed(2);

        function toast(m) {
            const t = $('toast');
            t.textContent = m;
            t.classList.add('on');
            setTimeout(() => t.classList.remove('on'), 2200);
        }

        async function api(method, path, body) {
            const t0 = performance.now();
            const r = await fetch(path, {
                method,
                headers: {
                    'Content-Type': 'application/json',
                    ...(token ? { Authorization: 'Bearer ' + token } : {})
                },
                body: body ? JSON.stringify(body) : undefined
            });
            const data = await r.json().catch(() => ({}));
            if (r.status === 401 && token) { logout(); throw new Error('Session expired'); }
            if (!r.ok) throw new Error(data.error || 'Request failed');
            data._ms = performance.now() - t0;
            data._cache = r.headers.get('X-Cache');
            return data;
        }

        /* ---------- LOGIN ---------- */
        function renderLogin() {
            $('app').innerHTML = `
                <div class="login">
                    <h1>ShopGrid</h1>
                    <p>Sign in to continue</p>
                    <label for="em">Email</label>
                    <input class="f" id="em" value="user@sg.com" autocomplete="username">
                    <label for="pw">Password</label>
                    <input class="f" id="pw" type="password" value="1234" autocomplete="current-password">
                    <div class="err" id="err"></div>
                    <button class="btn block" onclick="doLogin()">Sign in</button>
                    <div class="note">Demo account: user@sg.com / 1234</div>
                </div>`;
            $('pw').addEventListener('keydown', e => { if (e.key === 'Enter') doLogin(); });
        }

        async function doLogin() {
            try {
                const d = await api('POST', '/v1/login', { email: $('em').value, password: $('pw').value });
                token = d.token;
                me = d.user;
                store.set('tok', token);
                store.set('me', JSON.stringify(me));
                renderShell();
                await loadCart();
                search();
            } catch (e) {
                $('err').textContent = e.message;
            }
        }

        function logout() {
            token = null;
            me = null;
            store.clear();
            closeModal();
            renderLogin();
        }

        /* ---------- SHELL ---------- */
        function renderShell() {
            $('app').innerHTML = `
                <header>
                    <div class="logo">ShopGrid</div>
                    <div class="search">
                        <input id="q" placeholder="Search products" value="${esc(state.q)}">
                        <button onclick="state.page=1;state.q=$('q').value;search()">Search</button>
                    </div>
                    <div class="actions">
                        <button class="ghost" onclick="openOrders()">Orders</button>
                        <button class="ghost" onclick="openCart()">Cart<span class="count" id="cc">0</span></button>
                        <button class="ghost" onclick="logout()" title="${esc(me ? me.email : '')}">Sign out</button>
                    </div>
                </header>
                <main>
                    <div class="meta">
                        <span id="info"></span>
                        <span id="lat"></span>
                    </div>
                    <div class="grid" id="grid"></div>
                    <div class="pager" id="pager"></div>
                </main>`;
            $('q').addEventListener('keydown', e => {
                if (e.key === 'Enter') { state.page = 1; state.q = $('q').value; search(); }
            });
            updCart();
        }

        /* ---------- SEARCH + VIEW ---------- */
        async function search() {
            try {
                const s = await api('GET', `/v1/search?query=${encodeURIComponent(state.q)}&page=${state.page}&size=8`);
                const prods = await Promise.all(s.productIds.map(id => api('GET', '/v1/product/' + id)));

                $('info').textContent = `${s.total} result${s.total === 1 ? '' : 's'}` +
                    (state.q ? ` for "${state.q}"` : '');
                $('lat').textContent = `Search ${s._ms.toFixed(0)} ms · cache ${s._cache || 'n/a'} · target under 200 ms`;
                $('grid').innerHTML = prods.length
                    ? prods.map(card).join('')
                    : '<div class="empty" style="grid-column:1/-1;background:var(--surface)">No products match your search.</div>';

                let pg = '';
                for (let i = 1; i <= s.pages; i++) {
                    pg += `<button class="${i == s.page ? 'on' : ''}" onclick="state.page=${i};search()">${i}</button>`;
                }
                $('pager').innerHTML = s.pages > 1 ? pg : '';
            } catch (e) {
                toast(e.message);
            }
        }

        function card(p) {
            return `
                <div class="prod" onclick="openProduct(${p.id})">
                    <div class="img">${p.image}</div>
                    <div class="pb">
                        <div class="pn">${esc(p.name)}</div>
                        <div class="pc">${esc(p.category)} · ${p.stock > 0 ? p.stock + ' in stock' : '<span class="oos">Out of stock</span>'}</div>
                        <div class="pr">
                            <span class="price">${money(p.price)}</span>
                            <button class="btn" ${p.stock ? '' : 'disabled'} onclick="event.stopPropagation();addCart(${p.id})">Add</button>
                        </div>
                    </div>
                </div>`;
        }

        async function openProduct(id) {
            try {
                const p = await api('GET', '/v1/product/' + id);
                showModal(`
                    <button class="x" onclick="closeModal()" aria-label="Close">&times;</button>
                    <div class="img" style="height:160px;font-size:80px;border:1px solid var(--line);border-radius:4px">${p.image}</div>
                    <h2 style="margin-top:16px">${esc(p.name)}</h2>
                    <div class="pc" style="margin-bottom:8px">${esc(p.category)}</div>
                    <p style="color:var(--mut);margin:0 0 12px">${esc(p.description)}</p>
                    <div class="tot" style="border-top:0">
                        <span>${money(p.price)}</span>
                        <span style="font-weight:400;color:var(--mut);font-size:14px">${p.stock} in stock</span>
                    </div>
                    <button class="btn block" ${p.stock ? '' : 'disabled'} onclick="addCart(${p.id});closeModal()">Add to cart</button>`);
            } catch (e) {
                toast(e.message);
            }
        }

        /* ---------- CART ---------- */
        async function loadCart() {
            try { state.cart = await api('GET', '/v1/cart'); updCart(); } catch (e) {}
        }

        function updCart() {
            const c = $('cc');
            if (c) c.textContent = state.cart.count;
        }

        async function addCart(id) {
            try {
                state.cart = await api('POST', '/v1/cart', { productIds: [id] });
                updCart();
                toast('Added to cart');
            } catch (e) {
                toast(e.message);
            }
        }

        async function rmCart(id) {
            try {
                state.cart = await api('DELETE', '/v1/cart/' + id);
                updCart();
                openCart();
            } catch (e) {
                toast(e.message);
            }
        }

        async function openCart() {
            await loadCart();
            const c = state.cart;
            const lines = c.items.map(i => `
                <div class="row">
                    <span class="em">${i.image}</span>
                    <div class="gr"><b>${esc(i.name)}</b><br><small>${money(i.price)} × ${i.qty}</small></div>
                    <b>${money(i.price * i.qty)}</b>
                    <button class="btn ghost" onclick="rmCart(${i.productId})">Remove</button>
                </div>`).join('');

            showModal(`
                <button class="x" onclick="closeModal()" aria-label="Close">&times;</button>
                <h2>Cart</h2>
                ${c.items.length
                    ? lines + `
                        <div class="tot"><span>Total</span><span>${money(c.total)}</span></div>
                        <button class="btn block" onclick="doCheckout()">Checkout</button>`
                    : '<div class="empty">Your cart is empty.</div>'}`);
        }

        /* ---------- CHECKOUT + PAYMENT ---------- */
        async function doCheckout() {
            try {
                const o = await api('POST', '/v1/checkout', { productIds: state.cart.items.map(i => i.productId) });
                await loadCart();
                openPayment(o);
            } catch (e) {
                toast(e.message);
            }
        }

        function openPayment(o) {
            showModal(`
                <button class="x" onclick="closeModal()" aria-label="Close">&times;</button>
                <h2>Payment</h2>
                <div class="summary">
                    Order <b>${o.orderId}</b><br>
                    <small>Amount due ${money(o.total)}</small>
                </div>
                <label for="cn">Card number</label>
                <input class="f" id="cn" value="4242 4242 4242 4242">
                <div class="two">
                    <div>
                        <label for="ce">Expiry (MM/YY)</label>
                        <input class="f" id="ce" value="12/30">
                    </div>
                    <div>
                        <label for="cv">CVV</label>
                        <input class="f" id="cv" value="123">
                    </div>
                </div>
                <div class="err" id="perr"></div>
                <button class="btn block" id="paybtn" style="margin-top:10px" onclick="pay('${o.orderId}')">Pay ${money(o.total)}</button>
                <p class="hint">Test cards: 4242 4242 4242 4242 succeeds. 4000 0000 0000 0002 is declined.</p>`);
        }

        async function pay(oid) {
            const b = $('paybtn');
            b.disabled = true;
            b.textContent = 'Processing…';
            try {
                const r = await api('POST', '/v1/payment', {
                    orderId: oid,
                    card: { number: $('cn').value, expiry: $('ce').value, cvv: $('cv').value }
                });
                showModal(`
                    <button class="x" onclick="closeModal()" aria-label="Close">&times;</button>
                    <div class="center">
                        <h2>Payment confirmed</h2>
                        <p style="color:var(--mut)">
                            Order <b style="color:var(--ink)">${r.orderId}</b><br>
                            Confirmation <b style="color:var(--ink)">${r.confirmation}</b><br>
                            Charged ${money(r.amount)}
                        </p>
                        <button class="btn" onclick="openOrders('${r.orderId}')">View order status</button>
                    </div>`);
                search();
            } catch (e) {
                $('perr').textContent = e.message;
                b.disabled = false;
                b.textContent = 'Try again';
            }
        }

        /* ---------- ORDER STATUS ---------- */
        const FLOW = ['PENDING_PAYMENT', 'PAID', 'PROCESSING', 'SHIPPED', 'DELIVERED'];

        function stepsHtml(st) {
            const ix = FLOW.indexOf(st);
            return '<div class="steps">' + FLOW.map((s, i) => `
                <div class="step ${i < ix ? 'done' : i == ix ? 'cur' : ''}">${s.replace('_', ' ')}</div>`).join('') +
                '</div>';
        }

        async function renderOrders(focus) {
            const d = await api('GET', '/v1/orders');
            let one = '';
            if (focus) {
                try {
                    const s = await api('GET', '/v1/status/' + encodeURIComponent(focus));
                    one = `
                        <div class="ordcard" style="border-color:var(--ink)">
                            <div class="ordhead"><b>${s.orderId}</b><span>${s.status}</span></div>
                            ${stepsHtml(s.status)}
                        </div>`;
                } catch (e) {
                    one = `<div class="err">${esc(e.message)}</div>`;
                }
            }
            const m = $('ordbody');
            if (!m) return;
            m.innerHTML = one + (d.orders.length
                ? d.orders.map(o => `
                    <div class="ordcard">
                        <div class="ordhead"><b>${o.orderId}</b><b>${money(o.total)}</b></div>
                        <small>${o.items.map(i => esc(i.name) + ' × ' + i.qty).join(', ')}</small>
                        ${stepsHtml(o.status)}
                        ${o.status == 'PENDING_PAYMENT'
                            ? `<button class="btn" onclick="openPayment({orderId:'${o.orderId}',total:${o.total}})">Pay now</button>`
                            : ''}
                    </div>`).join('')
                : '<div class="empty">No orders yet.</div>');
        }

        function openOrders(focus) {
            showModal(`
                <button class="x" onclick="closeModal()" aria-label="Close">&times;</button>
                <h2>Orders</h2>
                <div style="display:flex;gap:8px">
                    <input class="f" id="oid" placeholder="Order ID, e.g. ORD-1A2B3C4D" value="${focus || ''}">
                    <button class="btn" onclick="renderOrders($('oid').value.trim())">Check</button>
                </div>
                <div id="ordbody" style="margin-top:8px"></div>`);
            renderOrders(focus);
            clearInterval(poll);
            poll = setInterval(() => {
                if (!$('ordbody')) return clearInterval(poll);
                const v = $('oid') && $('oid').value.trim();
                renderOrders(v || null);
            }, 3000);
        }

        /* ---------- MODAL ---------- */
        function showModal(h) {
            $('modal').innerHTML = h;
            $('ov').classList.add('on');
        }

        function closeModal() {
            clearInterval(poll);
            $('ov').classList.remove('on');
            $('modal').innerHTML = '';
        }

        $('ov').addEventListener('click', e => { if (e.target.id === 'ov') closeModal(); });

        /* ---------- BOOT ---------- */
        if (token && me) {
            renderShell();
            loadCart();
            search();
        } else {
            renderLogin();
        }
    </script>
</body>
</html>
"""

# LAUNCHER - works in Google Colab (inline interface) and in plain Python

_SERVER = globals().get("_SERVER")   # survives re-running the cell


def start(port=PORT, height=850):
    global _SERVER
    if _SERVER is not None:          # re-run safe: stop the previous server first
        try:
            _SERVER.shutdown()
            _SERVER.server_close()
        except Exception:
            pass
    _SERVER = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=_SERVER.serve_forever, daemon=True).start()
    print("")

    try:
        from google.colab import output          # inside Colab
        from IPython.display import display, HTML
        output.serve_kernel_port_as_iframe(port, height=height)
        try:
            url = output.eval_js(f"google.colab.kernel.proxyPort({port})")
        #    display(HTML(f'<p><a href="{url}" target="_blank">Open in a new tab</a> '
             #            f'(use this if the embedded view is blank)</p>'))
        except Exception:
            pass
    except ImportError:                          # plain Python
        import webbrowser
        print(f"Open http://localhost:{port}")
        webbrowser.open(f"http://localhost:{port}")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            print("\nShutting down.")
            _SERVER.shutdown()


if __name__ == "__main__":
    start()