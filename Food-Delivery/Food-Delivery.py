''' 
TiffinGo - food delivery platform 

1. Requirement analysis 
A. functional requrement 
- login/logout 
- show nearby restaurants 
- search 
- view menu
- cart 
- payment 
- status 
- delivery
B. non functional requrement 
- scale : 100M MAU 
- Cap theroem - availabe for search and consistent for payment 

2. Identifying Entities 
- user
- restaurants 
- delivery agents

3. API 
A. login 
POST /v1/user/register={userid,metadetails}->login/logout
B. Show 
GET /v1/results={restaurant ={location}}->  List<restaurantID> :pagenation 
C. Search
GET /v1/search?query={search}-> List<restaurantID> :pagenation 
D. View Menu
GET /v1/restaurant={restaurantID} -> food items details 
D. Order Book 
POST /v1/book{book body :restaurantID}-> bookID
E. Cart 
POST /v1/cart/items-> cartID
F. Payment 
POST /v1/payment={bookID} -> paymentID 
G. Status 
GET /v1/delivery/{orderID}/tracking

4. Hih level design 
user -> api gateway -> sever ( search , view , book , payment ) -> database

5. low level design 
uploaded image

'''
import re
import math
import time
import hmac
import secrets
import hashlib
import datetime
import threading
from collections import deque, defaultdict
from functools import wraps

from flask import Flask, request, jsonify, Response, g
from werkzeug.serving import make_server

PORT = 5000
app = Flask(__name__)
DB_LOCK = threading.RLock()   

def now():
    return time.time()


def err(message, code=400, **extra):
    return jsonify(error=message, **extra), code


def body():
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


def num(value, lo, hi):
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if lo <= f <= hi else None


def km(a_lat, a_lng, b_lat, b_lng):
    p = math.pi / 180
    d = (math.sin((b_lat - a_lat) * p / 2) ** 2
         + math.cos(a_lat * p) * math.cos(b_lat * p) * math.sin((b_lng - a_lng) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(d))


def fee_for(distance):
    return int(round((30 + distance * 15) / 5) * 5)


def eta_for(restaurant, distance):
    return int(restaurant["prep"] + round(distance * 3.5) + 5)


def short_id(prefix):
    return prefix + "-" + secrets.token_hex(3).upper()


#  Seed data: areas, restaurants, delivery agents

AREAS = {
    "Pulchowk": (27.6790, 85.3185),
    "Thamel": (27.7153, 85.3123),
    "Lazimpat": (27.7230, 85.3235),
    "Jhamsikhel": (27.6748, 85.3078),
    "New Baneshwor": (27.6915, 85.3425),
    "Koteshwor": (27.6781, 85.3490),
    "Boudha": (27.7215, 85.3620),
    "Kalanki": (27.6935, 85.2810),
}

RESTAURANTS = {}


def add_restaurant(rid, name, cuisine, area, lat, lng, rating, prep, items):
    RESTAURANTS[rid] = {
        "id": rid, "name": name, "cuisine": cuisine, "area": area,
        "lat": lat, "lng": lng, "rating": rating, "prep": prep,
        "items": [
            {"id": f"{rid}-{i + 1}", "name": n, "price": p, "veg": v, "desc": d}
            for i, (n, p, v, d) in enumerate(items)
        ],
    }


add_restaurant("r1", "Himal Momo House", "Momo and Tibetan", "Thamel", 27.7154, 85.3123, 4.5, 15, [
    ("Steamed Buff Momo (10 pcs)", 180, False, "Hand-folded, served with tomato achar"),
    ("Chicken Jhol Momo", 220, False, "Dipped in warm sesame-tomato broth"),
    ("Veg Fried Momo", 160, True, "Crisp-fried cabbage and carrot momo"),
    ("Paneer Kothey Momo", 190, True, "Pan-seared on one side, soft on the other"),
    ("Chicken Thukpa", 240, False, "Hearty noodle soup with vegetables"),
    ("Masala Chiya", 40, True, "Milk tea with ginger and cardamom"),
])
add_restaurant("r2", "Thakali Kitchen", "Thakali and Dal Bhat", "Lazimpat", 27.7227, 85.3240, 4.6, 25, [
    ("Veg Thakali Set", 420, True, "Rice, dal, gundruk, saag, pickle and ghee"),
    ("Chicken Thakali Set", 520, False, "The full set with a chicken curry"),
    ("Dhindo with Gundruk", 360, True, "Buckwheat dhindo with fermented greens"),
    ("Buff Sukuti", 340, False, "Dried, spiced and pan-fried buffalo"),
    ("Timur Achar", 60, True, "Sichuan pepper pickle on the side"),
])
add_restaurant("r3", "Newa Bhoj Ghar", "Newari", "Pulchowk", 27.6770, 85.3205, 4.4, 20, [
    ("Newari Khaja Set", 380, False, "Beaten rice, choila, bara and potato achar"),
    ("Buff Choila", 290, False, "Grilled buffalo with mustard oil and spices"),
    ("Black Lentil Bara", 110, True, "Savoury lentil pancake"),
    ("Yomari (2 pcs)", 90, True, "Steamed rice-flour dumpling with molasses"),
    ("Juju Dhau", 80, True, "Thick set curd from Bhaktapur style recipe"),
])
add_restaurant("r4", "Pulchowk Chowmein Corner", "Street food and Chinese", "Pulchowk", 27.6802, 85.3165, 4.2, 12, [
    ("Veg Chowmein", 140, True, "Wok-tossed noodles with vegetables"),
    ("Buff Chowmein", 170, False, "Smoky wok noodles with buffalo"),
    ("Chicken Fried Rice", 210, False, "Egg-fried rice with chicken"),
    ("Chilli Potato", 150, True, "Crisp potato in a sticky chilli glaze"),
    ("Lemon Soda", 70, True, "Fresh and fizzy"),
])
add_restaurant("r5", "Baneshwor Pizza Co.", "Pizza and Pasta", "New Baneshwor", 27.6915, 85.3420, 4.3, 22, [
    ("Margherita Pizza", 450, True, "Tomato, mozzarella and basil"),
    ("Chicken Supreme Pizza", 620, False, "Chicken, peppers, olives and onion"),
    ("Garlic Bread", 180, True, "Toasted with garlic butter"),
    ("Penne Alfredo", 360, True, "Creamy white sauce with mushrooms"),
    ("Chocolate Brownie", 160, True, "Warm, with a fudgy centre"),
])
add_restaurant("r6", "Sekuwa Corner", "Grill and Barbecue", "Koteshwor", 27.6780, 85.3490, 4.5, 25, [
    ("Chicken Sekuwa", 380, False, "Charcoal-grilled with Nepali spices"),
    ("Buff Sekuwa", 340, False, "Smoky grilled buffalo skewers"),
    ("Paneer Tikka", 320, True, "Marinated in yoghurt and grilled"),
    ("Tandoori Roti (2 pcs)", 40, True, "Fresh from the oven"),
    ("Bhatmas Sadeko", 140, True, "Roasted soybean salad with lemon and chilli"),
])
add_restaurant("r7", "Boudha Bakery and Cafe", "Cafe and Bakery", "Boudha", 27.7215, 85.3620, 4.4, 10, [
    ("Cinnamon Roll", 150, True, "Baked each morning"),
    ("Chicken Sandwich", 260, False, "Grilled chicken, lettuce and mustard mayo"),
    ("Veg Club Sandwich", 240, True, "Three layers with cheese and vegetables"),
    ("Cappuccino", 180, True, "Double shot, steamed milk"),
    ("Apple Pie Slice", 170, True, "Served warm"),
])
add_restaurant("r8", "Jhamsikhel Green Bowl", "Healthy and Vegetarian", "Jhamsikhel", 27.6748, 85.3075, 4.6, 18, [
    ("Quinoa Veg Bowl", 380, True, "Roasted vegetables, quinoa and tahini"),
    ("Lentil Soup", 200, True, "Red lentils, cumin and lemon"),
    ("Tofu Stir Fry", 330, True, "Seasonal greens with garlic and ginger"),
    ("Fruit Smoothie", 210, True, "Banana, mango and yoghurt"),
    ("Millet Roti Wrap", 290, True, "Millet roti with hummus and salad"),
])
add_restaurant("r9", "Dal Bhat Power 24", "Dal Bhat and Nepali", "Kalanki", 27.6935, 85.2810, 4.3, 18, [
    ("Dal Bhat Tarkari", 250, True, "Unlimited dal, rice, curry and pickle"),
    ("Chicken Dal Bhat", 340, False, "With a chicken curry on the side"),
    ("Aloo Tama Bodi", 160, True, "Potato and bamboo shoot curry"),
    ("Masu Ko Jhol", 280, False, "Home-style meat curry"),
])

# Inverted index: token -> {restaurantId: weight}
WORD = re.compile(r"[a-z0-9]+")
INDEX = defaultdict(dict)


def build_index():
    for r in RESTAURANTS.values():
        parts = [(r["name"], 5), (r["cuisine"], 3), (r["area"], 1)]
        parts += [(item["name"], 2) for item in r["items"]]
        for text, weight in parts:
            for token in WORD.findall(text.lower()):
                INDEX[token][r["id"]] = INDEX[token].get(r["id"], 0) + weight


build_index()

AGENTS = [
    {"id": "A1", "name": "Sujan Tamang", "vehicle": "Scooter", "lat": 27.6860, "lng": 85.3150, "orderId": None},
    {"id": "A2", "name": "Mina Gurung", "vehicle": "Bicycle", "lat": 27.7100, "lng": 85.3200, "orderId": None},
    {"id": "A3", "name": "Prakash Shrestha", "vehicle": "Motorbike", "lat": 27.6800, "lng": 85.3400, "orderId": None},
    {"id": "A4", "name": "Rina Maharjan", "vehicle": "Scooter", "lat": 27.6700, "lng": 85.3100, "orderId": None},
    {"id": "A5", "name": "Dawa Sherpa", "vehicle": "Motorbike", "lat": 27.7200, "lng": 85.3550, "orderId": None},
    {"id": "A6", "name": "Anil Karki", "vehicle": "Scooter", "lat": 27.6930, "lng": 85.2900, "orderId": None},
]


# In-memory stores

USERS = {}            # userId -> user
EMAILS = {}           # email  -> userId
SESSIONS = {}         # token  -> {uid, exp}
CARTS = {}            # userId -> {id, restaurantId, items{itemId: qty}}
ORDERS = {}           # orderId -> order
IDEMPOTENCY = {}      # (userId, key) -> response
LOGIN_FAILS = {}      # email -> {count, until}
SEARCH_CACHE = {}     # normalized query -> (timestamp, ranked results)

SESSION_TTL = 2 * 60 * 60
SEARCH_TTL = 30
HOLD_SECONDS = 10 * 60
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def hash_password(password, salt=None):
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 120_000)
    return salt.hex() + "$" + digest.hex()


def check_password(password, stored):
    salt_hex, digest_hex = stored.split("$")
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt_hex), 120_000)
    return hmac.compare_digest(digest.hex(), digest_hex)


def public_user(user):
    return {k: user[k] for k in ("id", "name", "email", "phone", "wallet")}


def create_user(name, email, phone, password, wallet):
    uid = "U" + secrets.token_hex(4).upper()
    USERS[uid] = {"id": uid, "name": name, "email": email, "phone": phone,
                  "pw": hash_password(password), "wallet": wallet}
    EMAILS[email] = uid
    return USERS[uid]


def open_session(user):
    token = secrets.token_urlsafe(32)
    SESSIONS[token] = {"uid": user["id"], "exp": now() + SESSION_TTL}
    return token


create_user("Demo Customer", "demo@tiffingo.test", "9800000000", "demo12345", 1500)


def auth_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        header = request.headers.get("Authorization", "")
        token = header[7:] if header.startswith("Bearer ") else ""
        session = SESSIONS.get(token)
        if not session or session["exp"] < now():
            SESSIONS.pop(token, None)
            return err("Please log in to continue.", 401)
        g.user = USERS[session["uid"]]
        g.token = token
        return fn(*args, **kwargs)
    return wrapper


# API gateway
HITS = defaultdict(deque)
REQUEST_LOG = deque(maxlen=60)
SERVICE_BY_PREFIX = [
    ("/v1/user", "auth"), ("/v1/search", "search"), ("/v1/results", "search"),
    ("/v1/restaurant", "view"), ("/v1/cart", "cart"), ("/v1/book", "book"),
    ("/v1/orders", "book"), ("/v1/payment", "payment"), ("/v1/delivery", "delivery"),
]


@app.before_request
def gateway_in():
    g.t0 = time.perf_counter()
    g.cache = None
    if request.path.startswith("/v1/") and not request.path.startswith("/v1/_gateway"):
        window = HITS[request.remote_addr]
        t = now()
        while window and t - window[0] > 60:
            window.popleft()
        if len(window) >= 300:
            return err("Too many requests. Please wait a moment.", 429)
        window.append(t)


@app.after_request
def gateway_out(resp):
    resp.headers["X-Content-Type-Options"] = "nosniff"
    if request.path.startswith("/v1/"):
        resp.headers["Cache-Control"] = "no-store"
        if g.get("cache"):
            resp.headers["X-Cache"] = g.cache
        if not request.path.startswith("/v1/_gateway") and not request.headers.get("X-Poll"):
            service = next((s for p, s in SERVICE_BY_PREFIX if request.path.startswith(p)), "gateway")
            REQUEST_LOG.appendleft({
                "time": datetime.datetime.now().strftime("%H:%M:%S"),
                "method": request.method,
                "path": request.full_path.rstrip("?")[:70],
                "status": resp.status_code,
                "ms": round((time.perf_counter() - g.t0) * 1000, 1),
                "service": service,
                "cache": g.get("cache") or "",
            })
    return resp


# Search + browse service
def card(r, lat, lng, matches=None):
    d = km(lat, lng, r["lat"], r["lng"])
    out = {k: r[k] for k in ("id", "name", "cuisine", "area", "rating", "prep")}
    out.update(distanceKm=round(d, 1), etaMin=eta_for(r, d), deliveryFee=fee_for(d))
    if matches:
        out["matches"] = matches
    return out


def paginate(rows, page, size):
    total = len(rows)
    start = (page - 1) * size
    chunk = rows[start:start + size]
    return {"items": chunk, "ids": [c["id"] for c in chunk], "page": page,
            "hasMore": start + size < total, "total": total}


def page_args():
    try:
        page = max(1, int(request.args.get("page", 1)))
        size = min(20, max(1, int(request.args.get("size", 4))))
    except ValueError:
        page, size = 1, 4
    return page, size


def location_args():
    lat = num(request.args.get("lat"), -90, 90)
    lng = num(request.args.get("lng"), -180, 180)
    return (lat, lng) if lat is not None and lng is not None else AREAS["Pulchowk"]


def ranked_search(query):
    key = " ".join(WORD.findall(query.lower()))
    cached = SEARCH_CACHE.get(key)
    if cached and now() - cached[0] < SEARCH_TTL:
        g.cache = "HIT"
        return cached[1]
    g.cache = "MISS"
    tokens = key.split()
    per_token = []
    for token in tokens:
        found = {}
        for word, posting in INDEX.items():
            if word.startswith(token):
                for rid, weight in posting.items():
                    found[rid] = found.get(rid, 0) + weight * (2 if word == token else 1)
        per_token.append(found)
    result = []
    if per_token:
        common = set.intersection(*[set(f) for f in per_token])
        for rid in common:
            score = sum(f[rid] for f in per_token)
            matches = [i["name"] for i in RESTAURANTS[rid]["items"]
                       if any(w.startswith(t) for w in WORD.findall(i["name"].lower()) for t in tokens)][:2]
            result.append((rid, score, matches))
    SEARCH_CACHE[key] = (now(), result)
    return result


@app.get("/v1/areas")
def areas():
    return jsonify({name: {"lat": lat, "lng": lng} for name, (lat, lng) in AREAS.items()})


@app.get("/v1/results")
def results():
    lat, lng = location_args()
    page, size = page_args()
    rows = [card(r, lat, lng) for r in RESTAURANTS.values()]
    rows = sorted([c for c in rows if c["distanceKm"] <= 12], key=lambda c: c["distanceKm"])
    return jsonify(paginate(rows, page, size))


@app.get("/v1/search")
def search():
    query = (request.args.get("query") or "").strip()[:60]
    if len(query) < 2:
        return err("Type at least 2 characters to search.", 400)
    lat, lng = location_args()
    page, size = page_args()
    ranked = ranked_search(query)
    rows = [(score, card(RESTAURANTS[rid], lat, lng, matches)) for rid, score, matches in ranked]
    rows.sort(key=lambda row: (-row[0], row[1]["distanceKm"]))
    return jsonify(paginate([c for _, c in rows], page, size))


@app.get("/v1/restaurant/<rid>")
def view_restaurant(rid):
    r = RESTAURANTS.get(rid)
    if not r:
        return err("Restaurant not found.", 404)
    lat, lng = location_args()
    return jsonify(restaurant=card(r, lat, lng), items=r["items"])


# Auth service
@app.post("/v1/user/register")
def register():
    d = body()
    name = str(d.get("name", "")).strip()
    email = str(d.get("email", "")).strip().lower()
    phone = re.sub(r"[^0-9+]", "", str(d.get("phone", "")))[:15]
    password = str(d.get("password", ""))
    if not 2 <= len(name) <= 60:
        return err("Enter your name (2 to 60 characters).")
    if not EMAIL_RE.match(email) or len(email) > 120:
        return err("Enter a valid email address.")
    if len(password) < 8 or len(password) > 100:
        return err("Use a password with at least 8 characters.")
    with DB_LOCK:
        if email in EMAILS:
            return err("An account with this email already exists. Log in instead.", 409)
        user = create_user(name, email, phone, password, 1000)
    return jsonify(token=open_session(user), user=public_user(user)), 201


@app.post("/v1/user/login")
def login():
    d = body()
    email = str(d.get("email", "")).strip().lower()
    password = str(d.get("password", ""))
    fails = LOGIN_FAILS.get(email, {"count": 0, "until": 0})
    if fails["until"] > now():
        wait = int(fails["until"] - now()) + 1
        return err(f"Too many attempts. Try again in {wait} seconds.", 429)
    uid = EMAILS.get(email)
    if not uid or not check_password(password, USERS[uid]["pw"]):
        fails["count"] += 1
        if fails["count"] >= 5:
            fails = {"count": 0, "until": now() + 60}
        LOGIN_FAILS[email] = fails
        return err("Email or password is incorrect.", 401)
    LOGIN_FAILS.pop(email, None)
    user = USERS[uid]
    return jsonify(token=open_session(user), user=public_user(user))


@app.post("/v1/user/logout")
@auth_required
def logout():
    SESSIONS.pop(g.token, None)
    return jsonify(ok=True)


@app.get("/v1/user/me")
@auth_required
def me():
    return jsonify(user=public_user(g.user))


# Cart service
def money(subtotal, fee):
    vat = int(round(subtotal * 0.13))
    return {"subtotal": subtotal, "deliveryFee": fee, "vat": vat, "total": subtotal + fee + vat}


def cart_view(uid, lat, lng):
    cart = CARTS.get(uid)
    if not cart or not cart["items"]:
        return {"cartID": cart["id"] if cart else None, "restaurantId": None, "lines": [], "totals": money(0, 0)}
    r = RESTAURANTS[cart["restaurantId"]]
    catalog = {i["id"]: i for i in r["items"]}
    lines = [{"itemId": iid, "name": catalog[iid]["name"], "price": catalog[iid]["price"],
              "veg": catalog[iid]["veg"], "qty": qty} for iid, qty in cart["items"].items()]
    subtotal = sum(l["price"] * l["qty"] for l in lines)
    fee = fee_for(km(lat, lng, r["lat"], r["lng"]))
    return {"cartID": cart["id"], "restaurantId": r["id"], "restaurantName": r["name"],
            "lines": lines, "totals": money(subtotal, fee)}


@app.get("/v1/cart")
@auth_required
def get_cart():
    lat, lng = location_args()
    return jsonify(cart_view(g.user["id"], lat, lng))


@app.post("/v1/cart/items")
@auth_required
def cart_items():
    d = body()
    r = RESTAURANTS.get(d.get("restaurantId"))
    item = next((i for i in r["items"] if i["id"] == d.get("itemId")), None) if r else None
    qty = d.get("qty")
    if not r or not item:
        return err("That item is not available.", 404)
    if not isinstance(qty, int) or isinstance(qty, bool) or not 0 <= qty <= 20:
        return err("Quantity must be between 0 and 20.")
    lat = num(d.get("lat"), -90, 90)
    lng = num(d.get("lng"), -180, 180)
    lat, lng = (lat, lng) if lat is not None and lng is not None else AREAS["Pulchowk"]
    uid = g.user["id"]
    with DB_LOCK:
        cart = CARTS.get(uid)
        if not cart:
            cart = CARTS[uid] = {"id": short_id("CART"), "restaurantId": r["id"], "items": {}}
        if cart["items"] and cart["restaurantId"] != r["id"]:
            if not d.get("replace"):
                return err("Your cart has items from another restaurant.", 409,
                           reason="DIFFERENT_RESTAURANT",
                           current=RESTAURANTS[cart["restaurantId"]]["name"])
            cart["items"] = {}
        cart["restaurantId"] = r["id"]
        if qty == 0:
            cart["items"].pop(item["id"], None)
        else:
            cart["items"][item["id"]] = qty
        return jsonify(cart_view(uid, lat, lng))


@app.delete("/v1/cart")
@auth_required
def clear_cart():
    CARTS.pop(g.user["id"], None)
    return jsonify(cart_view(g.user["id"], *AREAS["Pulchowk"]))


# Order / booking service  
def order_summary(o):
    return {"id": o["id"], "restaurantName": o["restaurantName"], "status": o["status"],
            "total": o["total"], "itemCount": sum(i["qty"] for i in o["items"]),
            "createdAt": o["createdAt"]}


@app.post("/v1/book")
@auth_required
def book():
    d = body()
    uid = g.user["id"]
    address = str(d.get("address", "")).strip()
    note = str(d.get("note", "")).strip()[:140]
    if not 5 <= len(address) <= 200:
        return err("Enter a delivery address (5 to 200 characters).")
    lat = num(d.get("lat"), -90, 90)
    lng = num(d.get("lng"), -180, 180)
    if lat is None or lng is None:
        return err("Choose a delivery area.")
    with DB_LOCK:
        cart = CARTS.get(uid)
        if not cart or not cart["items"]:
            return err("Your cart is empty.", 409)
        view = cart_view(uid, lat, lng)         # prices always come from the server
        r = RESTAURANTS[cart["restaurantId"]]
        dist = km(lat, lng, r["lat"], r["lng"])
        if dist > 12:
            return err("This restaurant does not deliver to your area.", 422)
        order = {
            "id": short_id("ORD"), "userId": uid, "restaurantId": r["id"], "restaurantName": r["name"],
            "items": [{"id": l["itemId"], "name": l["name"], "price": l["price"], "qty": l["qty"]}
                      for l in view["lines"]],
            **view["totals"],
            "address": address, "area": str(d.get("area", ""))[:40], "note": note, "lat": lat, "lng": lng,
            "status": "PAYMENT_PENDING", "paymentStatus": "UNPAID", "method": None, "paymentId": None,
            "createdAt": now(), "expiresAt": now() + HOLD_SECONDS, "paidAt": None,
            "agentId": None, "riderStart": None, "etaTotal": eta_for(r, dist),
        }
        ORDERS[order["id"]] = order
        CARTS.pop(uid, None)
    return jsonify(bookID=order["id"], order=order_public(order)), 201


def order_public(o):
    refresh(o)
    keys = ("id", "restaurantId", "restaurantName", "items", "subtotal", "deliveryFee", "vat", "total",
            "address", "area", "note", "status", "paymentStatus", "method", "paymentId",
            "createdAt", "expiresAt", "etaTotal")
    return {k: o[k] for k in keys}


def own_order(order_id):
    o = ORDERS.get(order_id)
    return o if o and o["userId"] == g.user["id"] else None


@app.get("/v1/orders")
@auth_required
def list_orders():
    mine = [o for o in ORDERS.values() if o["userId"] == g.user["id"]]
    for o in mine:
        refresh(o)
    mine.sort(key=lambda o: o["createdAt"], reverse=True)
    return jsonify(orders=[order_summary(o) for o in mine])


@app.get("/v1/orders/<order_id>")
@auth_required
def get_order(order_id):
    o = own_order(order_id)
    return jsonify(order=order_public(o)) if o else err("Order not found.", 404)


# Payment service  
def valid_card_number(number):
    return number.isdigit() and len(number) == 16


@app.post("/v1/payment")
@auth_required
def payment():
    d = body()
    uid = g.user["id"]
    idem = str(d.get("idempotencyKey", ""))[:64]
    method = d.get("method")
    with DB_LOCK:
        if idem and (uid, idem) in IDEMPOTENCY:
            return jsonify(IDEMPOTENCY[(uid, idem)]), 200
        o = own_order(str(d.get("bookID", "")))
        if not o:
            return err("Order not found.", 404)
        refresh(o)
        if o["status"] == "EXPIRED":
            return err("This order expired. Add your items to the cart again.", 410)
        if o["status"] != "PAYMENT_PENDING":
            return err("This order is already paid.", 409)
        if method not in ("wallet", "card", "cod"):
            return err("Choose a payment method.")

        status = "PAID"
        if method == "wallet":
            if g.user["wallet"] < o["total"]:
                return err(f"Wallet balance is Rs {g.user['wallet']}. You need Rs {o['total']}.", 402)
            g.user["wallet"] -= o["total"]
        elif method == "card":
            card_info = d.get("card") if isinstance(d.get("card"), dict) else {}
            number = re.sub(r"\s", "", str(card_info.get("number", "")))
            expiry = str(card_info.get("expiry", ""))
            cvv = str(card_info.get("cvv", ""))
            if not valid_card_number(number):
                return err("Enter a 16-digit card number.")
            m = re.match(r"^(0[1-9]|1[0-2])/(\d{2})$", expiry)
            if not m or (2000 + int(m.group(2)), int(m.group(1))) < (datetime.date.today().year, datetime.date.today().month):
                return err("Enter a valid expiry date (MM/YY).")
            if not (cvv.isdigit() and len(cvv) == 3):
                return err("Enter the 3-digit security code.")
            if number.endswith("0000"):
                return err("The card was declined. Try another card or payment method.", 402)
        else:
            status = "PAY_ON_DELIVERY"

        o["paymentId"] = short_id("PAY")
        o["method"] = method
        o["paymentStatus"] = status
        o["paidAt"] = now()
        o["status"] = "CONFIRMED"
        assign_agent(o)
        response = {"paymentID": o["paymentId"], "bookID": o["id"], "status": o["status"],
                    "paymentStatus": status, "wallet": g.user["wallet"]}
        if idem:
            IDEMPOTENCY[(uid, idem)] = response
        return jsonify(response), 200


# Delivery service  

STAGES = [
    ("CONFIRMED", 0, "Order confirmed"),
    ("PREPARING", 6, "Kitchen is preparing"),
    ("PICKED_UP", 30, "Rider picked up"),
    ("OUT_FOR_DELIVERY", 36, "On the way"),
    ("DELIVERED", 90, "Delivered"),
]
DEMO_TOTAL = 90


def assign_agent(order):
    free = [a for a in AGENTS if a["orderId"] is None]
    if not free:
        return
    r = RESTAURANTS[order["restaurantId"]]
    agent = min(free, key=lambda a: km(a["lat"], a["lng"], r["lat"], r["lng"]))
    agent["orderId"] = order["id"]
    order["agentId"] = agent["id"]
    order["riderStart"] = (agent["lat"], agent["lng"])


def refresh(order):
    with DB_LOCK:
        if order["status"] == "PAYMENT_PENDING" and now() > order["expiresAt"]:
            order["status"] = "EXPIRED"
        if order["paidAt"] and order["status"] != "DELIVERED":
            elapsed = now() - order["paidAt"]
            current = "CONFIRMED"
            for key, start, _ in STAGES:
                if elapsed >= start:
                    current = key
            order["status"] = current
            if not order["agentId"]:
                assign_agent(order)
            if current == "DELIVERED":
                agent = next((a for a in AGENTS if a["id"] == order["agentId"]), None)
                if agent:
                    agent["orderId"] = None
                    agent["lat"], agent["lng"] = order["lat"], order["lng"]
                if order["paymentStatus"] == "PAY_ON_DELIVERY":
                    order["paymentStatus"] = "PAID"
    return order


def lerp(a, b, f):
    return (a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f)


def rider_position(order):
    r = RESTAURANTS[order["restaurantId"]]
    start = order["riderStart"] or (r["lat"], r["lng"])
    shop, home = (r["lat"], r["lng"]), (order["lat"], order["lng"])
    elapsed = now() - order["paidAt"]
    if elapsed < 30:
        return lerp(start, shop, elapsed / 30)
    if elapsed < 36:
        return shop
    if elapsed < DEMO_TOTAL:
        return lerp(shop, home, (elapsed - 36) / (DEMO_TOTAL - 36))
    return home


@app.get("/v1/delivery/<order_id>/tracking")
@auth_required
def tracking(order_id):
    o = own_order(order_id)
    if not o:
        return err("Order not found.", 404)
    refresh(o)
    base = {"orderId": o["id"], "status": o["status"], "restaurantName": o["restaurantName"],
            "items": o["items"], "total": o["total"], "address": o["address"],
            "paymentStatus": o["paymentStatus"], "method": o["method"]}
    if not o["paidAt"]:
        base["expiresIn"] = max(0, int(o["expiresAt"] - now()))
        return jsonify(base)
    r = RESTAURANTS[o["restaurantId"]]
    elapsed = now() - o["paidAt"]
    done = [key for key, start, _ in STAGES if elapsed >= start]
    base.update(
        stages=[{"key": k, "label": label, "done": k in done, "current": k == o["status"]}
                for k, _, label in STAGES],
        etaMinutes=0 if o["status"] == "DELIVERED" else
        max(1, math.ceil(o["etaTotal"] * (1 - min(elapsed, DEMO_TOTAL) / DEMO_TOTAL))),
        restaurant={"name": r["name"], "lat": r["lat"], "lng": r["lng"]},
        destination={"lat": o["lat"], "lng": o["lng"]},
        simulated=True,
    )
    agent = next((a for a in AGENTS if a["id"] == o["agentId"]), None)
    if agent:
        lat, lng = rider_position(o)
        base["rider"] = {"name": agent["name"], "vehicle": agent["vehicle"], "lat": lat, "lng": lng}
    return jsonify(base)


@app.get("/v1/_gateway/log")
def gateway_log():
    return jsonify(entries=list(REQUEST_LOG)[:14])


@app.errorhandler(404)
def not_found(_):
    return err("Not found.", 404) if request.path.startswith("/v1/") else ("Not found", 404)


@app.errorhandler(500)
def server_error(_):
    return err("Something went wrong on our side. Please try again.", 500)


# Front end (HTML + CSS + JS served from the same process)

PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>TiffinGo - food delivery</title>
    <meta name="description" content="Order from restaurants near you in Kathmandu. A demo food delivery platform running on sample data.">
    <meta property="og:title" content="TiffinGo - food delivery near you">
    <meta property="og:description" content="Search nearby restaurants, build a cart, pay, and track the rider. Demo running on sample data.">
    <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%230e3b35'/%3E%3Ctext x='16' y='23' font-size='20' font-family='Georgia,serif' font-weight='700' text-anchor='middle' fill='%23f4b400'%3ET%3C/text%3E%3C/svg%3E">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:wght@500;700&family=Public+Sans:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --forest: #0e3b35;
            --forest-soft: #1b6b5e;
            --saffron: #f4b400;
            --saffron-dark: #8a6400;
            --paper: #f6f7f5;
            --card: #ffffff;
            --line: #dde3df;
            --ink: #18211f;
            --muted: #5c6b66;
            --danger: #b3261e;
            --danger-bg: #fbe9e7;
            --ok: #1b7a46;
            --radius: 8px;
            --display: "Bricolage Grotesque", "Trebuchet MS", system-ui, sans-serif;
            --body: "Public Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
        }

        * { box-sizing: border-box; }

        html { scroll-behavior: smooth; }

        body {
            margin: 0;
            background: var(--paper);
            color: var(--ink);
            font-family: var(--body);
            font-size: 16px;
            line-height: 1.5;
        }

        button, input, select, textarea { font: inherit; color: inherit; }

        button { cursor: pointer; }

        :focus-visible {
            outline: 3px solid var(--saffron);
            outline-offset: 2px;
        }

        .sr {
            position: absolute;
            width: 1px;
            height: 1px;
            overflow: hidden;
            clip: rect(0 0 0 0);
            white-space: nowrap;
        }

        /* ---------- top bar ---------- */
        .topbar {
            position: sticky;
            top: 0;
            z-index: 20;
            display: grid;
            grid-template-columns: auto auto minmax(180px, 1fr) auto;
            gap: 12px 16px;
            align-items: center;
            padding: 12px 24px;
            background: var(--forest);
            color: #fff;
        }

        .brand {
            font-family: var(--display);
            font-weight: 700;
            font-size: 1.5rem;
            letter-spacing: -0.02em;
            color: #fff;
            text-decoration: none;
            border: 0;
            background: none;
            padding: 0;
        }

        .brand span { color: var(--saffron); }

        .area-select {
            background: rgba(255, 255, 255, 0.12);
            border: 1px solid rgba(255, 255, 255, 0.25);
            border-radius: 8px;
            padding: 8px 10px;
            color: #fff;
        }

        .area-select option { color: var(--ink); }

        .search {
            display: flex;
            min-width: 0;
        }

        .search input {
            flex: 1;
            min-width: 0;
            padding: 10px 14px;
            border: 0;
            border-radius: 8px 0 0 8px;
            background: #fff;
            color: var(--ink);
        }

        .search button {
            padding: 10px 16px;
            border: 0;
            border-radius: 0 8px 8px 0;
            background: var(--saffron);
            color: var(--forest);
            font-weight: 600;
        }

        .actions {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .chip-btn {
            padding: 8px 12px;
            border: 1px solid rgba(255, 255, 255, 0.35);
            border-radius: 8px;
            background: transparent;
            color: #fff;
        }

        .chip-btn:hover { background: rgba(255, 255, 255, 0.12); }

        .chip-btn.primary {
            background: var(--saffron);
            border-color: var(--saffron);
            color: var(--forest);
            font-weight: 600;
        }

        .badge {
            display: inline-block;
            min-width: 22px;
            margin-left: 4px;
            padding: 0 6px;
            border-radius: 4px;
            background: var(--forest);
            color: #fff;
            font-size: 0.8rem;
            text-align: center;
        }

        .who {
            font-size: 0.85rem;
            color: rgba(255, 255, 255, 0.85);
            white-space: nowrap;
        }

        main {
            max-width: 860px;
            margin: 0 auto;
            padding: 28px 20px 80px;
        }

        h1 {
            margin: 0 0 4px;
            font-family: var(--display);
            font-size: clamp(1.8rem, 4vw, 2.5rem);
            line-height: 1.15;
            letter-spacing: -0.02em;
        }

        h2 {
            margin: 0 0 8px;
            font-family: var(--display);
            font-size: 1.3rem;
        }

        .sub { margin: 0 0 20px; color: var(--muted); }

        .link-btn {
            padding: 0;
            border: 0;
            background: none;
            color: var(--forest-soft);
            text-decoration: underline;
        }

        .rest-list {
            margin: 0;
            padding: 0;
            list-style: none;
            border-top: 1px solid var(--line);
        }

        .rest-list li { border-bottom: 1px solid var(--line); }

        .rest {
            display: grid;
            grid-template-columns: 56px 1fr auto;
            gap: 16px;
            align-items: center;
            width: 100%;
            padding: 16px 8px;
            border: 0;
            background: none;
            text-align: left;
        }

        .rest:hover { background: #eef2ef; }

        .rest-mark {
            display: grid;
            place-items: center;
            width: 56px;
            height: 56px;
            border-radius: 6px;
            font-family: var(--display);
            font-size: 1.6rem;
            font-weight: 700;
        }

        .rest-name {
            display: block;
            font-family: var(--display);
            font-size: 1.2rem;
            font-weight: 700;
        }

        .rest-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 2px 14px;
            color: var(--muted);
            font-size: 0.92rem;
        }

        .rest-match {
            display: block;
            margin-top: 2px;
            font-size: 0.9rem;
            color: var(--saffron-dark);
        }

        .rest-dist { text-align: right; font-size: 0.85rem; color: var(--muted); }

        .rest-dist strong {
            display: block;
            font-family: var(--display);
            font-size: 1.35rem;
            color: var(--ink);
        }

        .more {
            display: block;
            margin: 20px auto 0;
            padding: 10px 22px;
            border: 1px solid var(--forest-soft);
            border-radius: 8px;
            background: transparent;
            color: var(--forest-soft);
            font-weight: 600;
        }

        .more:hover { background: var(--forest-soft); color: #fff; }

        .menu-head {
            padding-bottom: 16px;
            border-bottom: 1px solid var(--line);
        }

        .facts {
            display: flex;
            flex-wrap: wrap;
            gap: 4px 20px;
            margin: 8px 0 0;
            color: var(--muted);
        }

        .toggle {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            margin: 16px 0 4px;
        }

        .item {
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 4px 16px;
            align-items: center;
            padding: 16px 0;
            border-bottom: 1px solid var(--line);
        }

        .item-name { font-weight: 600; }

        .item-desc { margin: 0; color: var(--muted); font-size: 0.92rem; }

        .item-price { font-family: var(--display); font-weight: 700; }

        .diet {
            display: inline-block;
            width: 14px;
            height: 14px;
            margin-right: 8px;
            border: 2px solid var(--ok);
            border-radius: 3px;
            vertical-align: -1px;
            position: relative;
        }

        .diet::after {
            content: "";
            position: absolute;
            inset: 2px;
            border-radius: 50%;
            background: var(--ok);
        }

        .diet.non { border-color: var(--danger); }

        .diet.non::after { background: var(--danger); }

        .add-btn {
            min-width: 84px;
            padding: 8px 16px;
            border: 1px solid var(--forest-soft);
            border-radius: 8px;
            background: #fff;
            color: var(--forest-soft);
            font-weight: 600;
        }

        .add-btn:hover { background: var(--forest-soft); color: #fff; }

        .stepper {
            display: inline-flex;
            align-items: center;
            border: 1px solid var(--forest-soft);
            border-radius: 8px;
            overflow: hidden;
        }

        .stepper button {
            width: 34px;
            height: 34px;
            border: 0;
            background: var(--forest-soft);
            color: #fff;
            font-size: 1.1rem;
        }

        .stepper span {
            min-width: 32px;
            text-align: center;
            font-weight: 600;
        }

        .btn {
            padding: 12px 20px;
            border: 0;
            border-radius: 8px;
            background: var(--forest);
            color: #fff;
            font-weight: 600;
        }

        .btn:hover { background: var(--forest-soft); }

        .btn:disabled { opacity: 0.55; cursor: not-allowed; }

        .btn.block { display: block; width: 100%; }

        .btn.ghost {
            border: 1px solid var(--line);
            background: #fff;
            color: var(--ink);
        }

        .field { display: block; margin-bottom: 14px; }

        .field > span { display: block; margin-bottom: 4px; font-weight: 600; font-size: 0.92rem; }

        .field input[type="text"],
        .field input[type="email"],
        .field input[type="password"],
        .field input[type="tel"],
        .field textarea {
            width: 100%;
            padding: 10px 12px;
            border: 1px solid #b9c4bf;
            border-radius: 8px;
            background: #fff;
        }

        .field textarea { resize: vertical; min-height: 64px; }

        .row2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }

        .form-error {
            margin: 0 0 14px;
            padding: 10px 12px;
            border-radius: 8px;
            background: var(--danger-bg);
            color: var(--danger);
            font-size: 0.92rem;
        }

        .hint { color: var(--muted); font-size: 0.88rem; }

        .scrim {
            position: fixed;
            inset: 0;
            z-index: 30;
            background: rgba(14, 30, 27, 0.45);
            display: none;
        }

        .scrim.open { display: block; }

        .drawer {
            position: fixed;
            top: 0;
            right: 0;
            bottom: 0;
            z-index: 40;
            display: flex;
            flex-direction: column;
            width: min(420px, 100%);
            background: var(--card);
            transform: translateX(100%);
            visibility: hidden;
            transition: transform 0.2s ease, visibility 0s linear 0.2s;
            border-left: 1px solid var(--line);
        }

        .drawer.open {
            transform: none;
            visibility: visible;
            transition: transform 0.2s ease;
        }

        .drawer-head,
        .drawer-foot {
            padding: 16px 20px;
            border-bottom: 1px solid var(--line);
        }

        .drawer-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .drawer-foot { border-top: 1px solid var(--line); border-bottom: 0; }

        .drawer-body { flex: 1; overflow-y: auto; padding: 8px 20px; }

        .line {
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 4px 12px;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid var(--line);
        }

        .totals { margin: 0; }

        .totals div {
            display: flex;
            justify-content: space-between;
            padding: 2px 0;
            color: var(--muted);
        }

        .totals .grand {
            margin-top: 6px;
            padding-top: 8px;
            border-top: 1px solid var(--line);
            color: var(--ink);
            font-weight: 700;
            font-size: 1.1rem;
        }

        .close-btn {
            width: 36px;
            height: 36px;
            border: 0;
            border-radius: 8px;
            background: transparent;
            font-size: 1.4rem;
        }

        .close-btn:hover { background: var(--paper); }

        .empty {
            padding: 28px 0;
            color: var(--muted);
        }

        .modal-wrap {
            position: fixed;
            inset: 0;
            z-index: 50;
            display: none;
            place-items: center;
            padding: 16px;
            background: rgba(14, 30, 27, 0.55);
            overflow-y: auto;
        }

        .modal-wrap.open { display: grid; }

        .modal {
            width: min(460px, 100%);
            padding: 24px;
            border-radius: 8px;
            background: var(--card);
        }

        .tabs {
            display: flex;
            gap: 4px;
            margin-bottom: 18px;
            padding: 4px;
            border-radius: 8px;
            background: var(--paper);
        }

        .tabs button {
            flex: 1;
            padding: 8px;
            border: 0;
            border-radius: 6px;
            background: transparent;
            font-weight: 600;
            color: var(--muted);
        }

        .tabs button[aria-selected="true"] {
            background: #fff;
            color: var(--ink);
            box-shadow: inset 0 0 0 1px var(--line);
        }

        .pay-option {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 8px;
            padding: 12px;
            border: 1px solid var(--line);
            border-radius: 8px;
            cursor: pointer;
        }

        .pay-option:has(input:checked) {
            border-color: var(--forest-soft);
            background: #eef6f3;
        }

        .pay-option small { display: block; color: var(--muted); }

        .order-row {
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 2px 16px;
            width: 100%;
            padding: 16px 8px;
            border: 0;
            border-bottom: 1px solid var(--line);
            background: none;
            text-align: left;
        }

        .order-row:hover { background: #eef2ef; }

        .status {
            align-self: center;
            padding: 3px 8px;
            border-radius: 4px;
            background: #e4efe9;
            color: var(--forest);
            font-size: 0.82rem;
            font-weight: 600;
        }

        .status.warn { background: #fff3cf; color: var(--saffron-dark); }

        .status.bad { background: var(--danger-bg); color: var(--danger); }

        .eta {
            font-family: var(--display);
            font-size: 3rem;
            font-weight: 700;
            line-height: 1;
        }

        .track-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 28px;
            margin-top: 20px;
        }

        .steps { margin: 0; padding: 0; list-style: none; }

        .steps li {
            position: relative;
            padding: 0 0 18px 30px;
            color: var(--muted);
        }

        .steps li::before {
            content: "";
            position: absolute;
            left: 0;
            top: 3px;
            width: 16px;
            height: 16px;
            border: 2px solid var(--line);
            border-radius: 50%;
            background: #fff;
        }

        .steps li::after {
            content: "";
            position: absolute;
            left: 7px;
            top: 21px;
            bottom: 1px;
            width: 2px;
            background: var(--line);
        }

        .steps li:last-child::after { display: none; }

        .steps li.done { color: var(--ink); }

        .steps li.done::before { border-color: var(--forest-soft); background: var(--forest-soft); }

        .steps li.current { font-weight: 600; }

        .steps li.current::before { border-color: var(--saffron); background: var(--saffron); }

        .map {
            width: 100%;
            border: 1px solid var(--line);
            border-radius: var(--radius);
            background: #eaf0ec;
        }

        .map .route { stroke: #8fa59d; stroke-width: 2; stroke-dasharray: 5 5; fill: none; }

        .map .shop { fill: var(--forest); }

        .map .home { fill: var(--danger); }

        .map .rider { fill: var(--saffron); stroke: var(--forest); stroke-width: 2; }

        .map text { font-size: 10px; fill: var(--muted); font-family: var(--body); }

        .panel {
            margin-top: 16px;
            padding: 12px 14px;
            border: 1px solid var(--line);
            border-radius: var(--radius);
            background: #fff;
        }

        .toasts {
            position: fixed;
            left: 50%;
            bottom: 20px;
            z-index: 60;
            display: grid;
            gap: 8px;
            transform: translateX(-50%);
        }

        .toast {
            padding: 10px 16px;
            border-radius: 8px;
            background: var(--forest);
            color: #fff;
        }

        .toast.error { background: var(--danger); }

        .dev {
            max-width: 860px;
            margin: 0 auto;
            padding: 0 20px 60px;
        }

        .dev-toggle { margin-bottom: 12px; }

        .dev-body {
            padding: 16px;
            border: 1px solid var(--line);
            border-radius: var(--radius);
            background: #fff;
            font-size: 0.9rem;
        }

        .dev-body table { width: 100%; border-collapse: collapse; }

        .dev-body th,
        .dev-body td {
            padding: 4px 8px;
            border-bottom: 1px solid var(--line);
            text-align: left;
            white-space: nowrap;
        }

        .scroll-x { overflow-x: auto; }

        @media (max-width: 760px) {
            .topbar { position: static; grid-template-columns: 1fr auto; padding: 10px 14px; }
            .stepper button { width: 40px; height: 40px; }
            .add-btn, .btn, .chip-btn, .more { min-height: 44px; }
            .search { grid-column: 1 / -1; order: 5; }
            .actions { grid-column: 1 / -1; order: 4; flex-wrap: wrap; }
            .rest { grid-template-columns: 48px 1fr; }
            .rest-mark { width: 48px; height: 48px; }
            .rest-dist { grid-column: 2; text-align: left; }
            .rest-dist strong { display: inline; margin-right: 8px; }
            .track-grid { grid-template-columns: 1fr; }
            .row2 { grid-template-columns: 1fr; }
        }

        @media (prefers-reduced-motion: reduce) {
            html { scroll-behavior: auto; }
            .drawer { transition: none; }
        }
    </style>
</head>
<body>
    <header class="topbar">
        <button class="brand" data-act="home" aria-label="TiffinGo home">Tiffin<span>Go</span></button>

        <label class="sr" for="area">Deliver to</label>
        <select id="area" class="area-select" aria-label="Deliver to"></select>

        <form id="searchForm" class="search" role="search">
            <label class="sr" for="q">Search restaurants or dishes</label>
            <input id="q" type="search" placeholder="Search momo, thakali, pizza" autocomplete="off" maxlength="60">
            <button type="submit">Search</button>
        </form>

        <nav class="actions" aria-label="Account">
            <span id="who" class="who"></span>
            <button class="chip-btn" id="ordersBtn" data-act="orders" hidden>My orders</button>
            <button class="chip-btn primary" id="authBtn" data-act="auth">Log in</button>
            <button class="chip-btn" data-act="cart">Cart<span class="badge" id="cartCount">0</span></button>
        </nav>
    </header>

    <main id="view" tabindex="-1"></main>

    <section class="dev">
        <button class="link-btn dev-toggle" data-act="dev" aria-expanded="false" aria-controls="devBody">How this is built</button>
        <div id="devBody" class="dev-body" hidden></div>
    </section>

    <div class="scrim" id="scrim" data-act="closeAll"></div>

    <aside class="drawer" id="drawer" aria-label="Your cart" aria-hidden="true"></aside>

    <div class="modal-wrap" id="modalWrap">
        <div class="modal" id="modal" role="dialog" aria-modal="true"></div>
    </div>

    <div class="toasts" id="toasts" aria-live="polite"></div>

    <script>
        "use strict";

        const $ = (sel, root = document) => root.querySelector(sel);
        const TINTS = [["#d8ece5", "#0e3b35"], ["#fbe6a8", "#5e4300"], ["#f6d5cf", "#7a1f16"],
                       ["#d9e3f5", "#1d3a6b"], ["#e6dcf0", "#44285f"], ["#e3ecd0", "#3b4c15"]];

        const state = {
            token: null, user: null, areas: {}, area: "Pulchowk", cart: null,
            list: null, rest: null, vegOnly: false, mode: "home",
            timer: null, pending: null, order: null, idem: {}
        };

        try {
            state.token = sessionStorage.getItem("tg_token");
            state.area = sessionStorage.getItem("tg_area") || "Pulchowk";
        } catch (e) { /* storage can be blocked inside iframes */ }

        const esc = s => String(s ?? "").replace(/[&<>"']/g, c =>
            ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
        const rs = n => "Rs " + Number(n).toLocaleString("en-IN");
        const hash = s => [...s].reduce((a, c) => (a * 31 + c.charCodeAt(0)) >>> 0, 7);
        const loc = () => state.areas[state.area] || { lat: 27.679, lng: 85.3185 };

        async function api(method, path, payload, opts = {}) {
            const headers = { "Content-Type": "application/json" };
            if (state.token) headers["Authorization"] = "Bearer " + state.token;
            if (opts.poll) headers["X-Poll"] = "1";
            let res;
            try {
                res = await fetch(path, { method, headers, body: payload ? JSON.stringify(payload) : undefined });
            } catch (networkError) {
                const offline = new Error("Can't reach TiffinGo. Check that the Colab cell is still running, then try again.");
                offline.status = 0;
                offline.data = {};
                throw offline;
            }
            let data = {};
            try { data = await res.json(); } catch (e) { /* empty body */ }
            if (!res.ok) {
                if (res.status === 401 && state.token) { clearSession(); updateAuthUI(); }
                const error = new Error(data.error || "The server could not finish that request. Try again, and re-run the Colab cell if it keeps failing.");
                error.status = res.status;
                error.data = data;
                throw error;
            }
            return data;
        }

        function toast(message, kind = "") {
            const el = document.createElement("div");
            el.className = "toast " + kind;
            el.textContent = message;
            $("#toasts").appendChild(el);
            setTimeout(() => el.remove(), 3500);
        }

        function stopPolling() {
            if (state.timer) clearInterval(state.timer);
            state.timer = null;
        }

        function clearSession() {
            state.token = null; state.user = null; state.cart = null;
            try { sessionStorage.removeItem("tg_token"); } catch (e) { /* ignore */ }
        }

        const ACTIONS = {};
        document.addEventListener("click", e => {
            const el = e.target.closest("[data-act]");
            if (!el) return;
            const fn = ACTIONS[el.dataset.act];
            if (fn) { e.preventDefault(); fn(el); }
        });
        document.addEventListener("keydown", e => { if (e.key === "Escape") closeAll(); });

        function updateAuthUI() {
            const user = state.user;
            $("#authBtn").textContent = user ? "Log out" : "Log in";
            $("#authBtn").classList.toggle("primary", !user);
            $("#ordersBtn").hidden = !user;
            $("#who").textContent = user ? user.name.split(" ")[0] + " / Wallet " + rs(user.wallet) : "";
            const count = state.cart ? state.cart.lines.reduce((n, l) => n + l.qty, 0) : 0;
            $("#cartCount").textContent = count;
        }

        function fillAreas() {
            $("#area").innerHTML = Object.keys(state.areas)
                .map(a => `<option ${a === state.area ? "selected" : ""}>${esc(a)}</option>`).join("");
        }

        $("#area").addEventListener("change", async e => {
            state.area = e.target.value;
            try { sessionStorage.setItem("tg_area", state.area); } catch (err) { /* ignore */ }
            await loadCart();
            if (state.mode === "home") openList("home");
            else if (state.mode === "search") openList("search", state.list.query);
            else if (state.mode === "menu") openMenu(state.rest.restaurant.id);
        });

        $("#searchForm").addEventListener("submit", e => {
            e.preventDefault();
            const q = $("#q").value.trim();
            if (q.length < 2) { toast("Type at least 2 characters to search.", "error"); return; }
            openList("search", q);
        });

        async function openList(kind, query = "") {
            stopPolling();
            state.mode = kind;
            state.list = { kind, query, items: [], page: 0, hasMore: false, total: 0, loading: true, error: "" };
            renderList();
            await fetchPage();
        }

        async function fetchPage() {
            const L = state.list;
            const { lat, lng } = loc();
            const base = L.kind === "search" ? `/v1/search?query=${encodeURIComponent(L.query)}&` : "/v1/results?";
            try {
                const d = await api("GET", `${base}lat=${lat}&lng=${lng}&page=${L.page + 1}&size=4`);
                if (state.list !== L) return;
                L.items.push(...d.items);
                L.page = d.page; L.hasMore = d.hasMore; L.total = d.total;
            } catch (e) { L.error = e.message; }
            L.loading = false;
            renderList();
        }

        function restaurantRow(r) {
            const tint = TINTS[hash(r.id) % TINTS.length];
            return `
                <li>
                    <button class="rest" data-act="menu" data-id="${esc(r.id)}">
                        <span class="rest-mark" style="background:${tint[0]};color:${tint[1]}">${esc(r.name[0])}</span>
                        <span>
                            <span class="rest-name">${esc(r.name)}</span>
                            <span class="rest-meta">
                                <span>${esc(r.cuisine)}</span>
                                <span>Rated ${r.rating}</span>
                                <span>${esc(r.area)}</span>
                            </span>
                            ${r.matches ? `<span class="rest-match">Serves ${r.matches.map(esc).join(" and ")}</span>` : ""}
                        </span>
                        <span class="rest-dist">
                            <strong>${r.distanceKm} km</strong>
                            <span>About ${r.etaMin} min, delivery ${rs(r.deliveryFee)}</span>
                        </span>
                    </button>
                </li>`;
        }

        function renderList() {
            const L = state.list;
            let head;
            if (L.kind === "search") {
                head = `<h1>Results for "${esc(L.query)}"</h1>
                        <p class="sub">${L.loading ? "Searching" : L.total + " found near " + esc(state.area)}.
                        <button class="link-btn" data-act="home">Back to nearby</button></p>`;
            } else {
                head = `<h1>Near ${esc(state.area)}</h1>
                        <p class="sub">${L.loading ? "Finding restaurants" : L.total + " restaurants deliver to you"}. Sorted by distance. Restaurants, menus, ratings and riders are sample data.</p>`;
            }
            let content = "";
            if (L.error) content = `<p class="form-error" role="alert">${esc(L.error)}</p>`;
            else if (!L.loading && !L.items.length) {
                content = `<div class="empty"><p>Nothing matches "${esc(L.query)}" here yet.</p>
                           <p class="hint">Try a dish such as momo, sekuwa or pizza.</p>
                           <button class="btn" data-act="home">See nearby restaurants</button></div>`;
            } else {
                content = `<ul class="rest-list">${L.items.map(restaurantRow).join("")}</ul>`;
                if (L.hasMore) content += `<button class="more" data-act="more">Show more restaurants</button>`;
            }
            $("#view").innerHTML = head + content;
        }

        ACTIONS.home = () => { $("#q").value = ""; openList("home"); window.scrollTo(0, 0); };
        ACTIONS.more = async el => { el.disabled = true; await fetchPage(); };
        ACTIONS.menu = el => openMenu(el.dataset.id);

        async function openMenu(id) {
            stopPolling();
            state.mode = "menu";
            const { lat, lng } = loc();
            try {
                state.rest = await api("GET", `/v1/restaurant/${id}?lat=${lat}&lng=${lng}`);
            } catch (e) { toast(e.message, "error"); return; }
            state.vegOnly = false;
            const r = state.rest.restaurant;
            $("#view").innerHTML = `
                <button class="link-btn" data-act="home">Back to restaurants</button>
                <div class="menu-head">
                    <h1>${esc(r.name)}</h1>
                    <div class="facts">
                        <span>${esc(r.cuisine)}</span>
                        <span>Rated ${r.rating}</span>
                        <span>${r.distanceKm} km away</span>
                        <span>About ${r.etaMin} min</span>
                        <span>Delivery ${rs(r.deliveryFee)}</span>
                    </div>
                    <label class="toggle"><input type="checkbox" id="vegOnly"> Vegetarian only</label>
                </div>
                <div id="menuItems"></div>`;
            $("#vegOnly").addEventListener("change", e => { state.vegOnly = e.target.checked; renderItems(); });
            renderItems();
            window.scrollTo(0, 0);
        }

        function cartQty(itemId) {
            if (!state.cart) return 0;
            const line = state.cart.lines.find(l => l.itemId === itemId);
            return line ? line.qty : 0;
        }

        function renderItems() {
            if (state.mode !== "menu" || !state.rest) return;
            const rid = state.rest.restaurant.id;
            const items = state.rest.items.filter(i => !state.vegOnly || i.veg);
            $("#menuItems").innerHTML = items.map(i => {
                const q = cartQty(i.id);
                const control = q
                    ? `<span class="stepper">
                           <button data-act="qty" data-r="${rid}" data-i="${i.id}" data-q="${q - 1}" aria-label="Remove one ${esc(i.name)}">-</button>
                           <span>${q}</span>
                           <button data-act="qty" data-r="${rid}" data-i="${i.id}" data-q="${q + 1}" aria-label="Add one ${esc(i.name)}">+</button>
                       </span>`
                    : `<button class="add-btn" data-act="qty" data-r="${rid}" data-i="${i.id}" data-q="1">Add</button>`;
                return `
                    <div class="item">
                        <div>
                            <div class="item-name"><span class="diet ${i.veg ? "" : "non"}" title="${i.veg ? "Vegetarian" : "Non-vegetarian"}"></span>${esc(i.name)}</div>
                            <p class="item-desc">${esc(i.desc)}</p>
                            <span class="item-price">${rs(i.price)}</span>
                        </div>
                        ${control}
                    </div>`;
            }).join("");
        }

        async function loadCart() {
            if (!state.user) { state.cart = null; updateAuthUI(); return; }
            const { lat, lng } = loc();
            try { state.cart = await api("GET", `/v1/cart?lat=${lat}&lng=${lng}`); } catch (e) { /* ignore */ }
            updateAuthUI();
            renderItems();
            if ($("#drawer").classList.contains("open")) renderDrawer();
        }

        async function setQty(restId, itemId, qty, replace = false) {
            if (!state.user) {
                state.pending = { restId, itemId, qty };
                openAuth("login", "Log in to start your cart. Browsing stays open to everyone.");
                return;
            }
            const { lat, lng } = loc();
            try {
                state.cart = await api("POST", "/v1/cart/items", { restaurantId: restId, itemId, qty, replace, lat, lng });
                updateAuthUI();
                renderItems();
                if ($("#drawer").classList.contains("open")) renderDrawer();
            } catch (e) {
                if (e.status === 409 && e.data.reason === "DIFFERENT_RESTAURANT") {
                    state.pending = { restId, itemId, qty };
                    openModal(`
                        <h2>Start a new cart?</h2>
                        <p>Your cart has items from ${esc(e.data.current)}. A cart can hold one restaurant at a time.</p>
                        <div class="row2">
                            <button class="btn ghost" data-act="closeAll">Keep current cart</button>
                            <button class="btn" data-act="replaceCart">Start new cart</button>
                        </div>`);
                } else toast(e.message, "error");
            }
        }

        ACTIONS.qty = el => setQty(el.dataset.r, el.dataset.i, Number(el.dataset.q));
        ACTIONS.replaceCart = () => {
            const p = state.pending; closeAll();
            if (p) setQty(p.restId, p.itemId, p.qty, true);
        };

        function renderDrawer() {
            const c = state.cart;
            const empty = !c || !c.lines.length;
            let body;
            if (!state.user) {
                body = `<div class="empty"><p>Log in to build a cart and order.</p>
                        <button class="btn" data-act="auth">Log in</button></div>`;
            } else if (empty) {
                body = `<div class="empty"><p>Your cart is empty.</p><p class="hint">Open a restaurant and tap Add on a dish.</p></div>`;
            } else {
                body = c.lines.map(l => `
                    <div class="line">
                        <div><span class="diet ${l.veg ? "" : "non"}"></span>${esc(l.name)}<br><span class="hint">${rs(l.price)} each</span></div>
                        <span class="stepper">
                            <button data-act="qty" data-r="${c.restaurantId}" data-i="${l.itemId}" data-q="${l.qty - 1}" aria-label="Remove one ${esc(l.name)}">-</button>
                            <span>${l.qty}</span>
                            <button data-act="qty" data-r="${c.restaurantId}" data-i="${l.itemId}" data-q="${l.qty + 1}" aria-label="Add one ${esc(l.name)}">+</button>
                        </span>
                    </div>`).join("");
            }
            const t = c && c.totals;
            const foot = (!empty && state.user) ? `
                <div class="drawer-foot">
                    <div class="totals">
                        <div><span>Subtotal</span><span>${rs(t.subtotal)}</span></div>
                        <div><span>Delivery to ${esc(state.area)}</span><span>${rs(t.deliveryFee)}</span></div>
                        <div><span>VAT 13%</span><span>${rs(t.vat)}</span></div>
                        <div class="grand"><span>Total</span><span>${rs(t.total)}</span></div>
                    </div>
                    <button class="btn block" style="margin-top:12px" data-act="checkout">Checkout</button>
                </div>` : "";
            $("#drawer").innerHTML = `
                <div class="drawer-head">
                    <div><h2 style="margin:0">Your cart</h2>
                    <span class="hint">${c && c.restaurantName ? esc(c.restaurantName) : ""}</span></div>
                    <button class="close-btn" data-act="closeAll" aria-label="Close cart">&times;</button>
                </div>
                <div class="drawer-body">${body}</div>${foot}`;
        }

        ACTIONS.cart = async () => {
            await loadCart();
            renderDrawer();
            $("#drawer").classList.add("open");
            $("#drawer").setAttribute("aria-hidden", "false");
            $("#scrim").classList.add("open");
        };

        function closeDrawer() {
            $("#drawer").classList.remove("open");
            $("#drawer").setAttribute("aria-hidden", "true");
            $("#scrim").classList.remove("open");
        }

        let lastFocus = null;

        function openModal(html) {
            lastFocus = document.activeElement;
            $("#modal").innerHTML = html;
            $("#modalWrap").classList.add("open");
            const first = $("#modal input, #modal button, #modal select");
            if (first) first.focus();
        }

        function closeModal() {
            const wrap = $("#modalWrap");
            if (!wrap.classList.contains("open")) return;
            wrap.classList.remove("open");
            if (lastFocus && lastFocus.focus) lastFocus.focus();
        }

        document.addEventListener("keydown", e => {
            if (e.key !== "Tab" || !$("#modalWrap").classList.contains("open")) return;
            const focusable = [...$("#modal").querySelectorAll("button, input, select, textarea")]
                .filter(el => !el.disabled && el.offsetParent !== null);
            if (!focusable.length) return;
            const first = focusable[0], last = focusable[focusable.length - 1];
            if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
            else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
        });

        function closeAll() { closeDrawer(); closeModal(); }
        $("#modalWrap").addEventListener("mousedown", e => { if (e.target.id === "modalWrap") closeModal(); });
        ACTIONS.closeAll = closeAll;

        function openAuth(tab = "login", message = "") {
            closeDrawer();
            const isLogin = tab === "login";
            openModal(`
                <div class="tabs" role="tablist">
                    <button role="tab" aria-selected="${isLogin}" data-act="tab" data-tab="login">Log in</button>
                    <button role="tab" aria-selected="${!isLogin}" data-act="tab" data-tab="register">Create account</button>
                </div>
                ${message ? `<p class="hint">${esc(message)}</p>` : ""}
                <div id="authError"></div>
                <form id="authForm" data-tab="${tab}" novalidate>
                    ${isLogin ? "" : `
                        <label class="field"><span>Full name</span><input type="text" name="name" autocomplete="name" required></label>
                        <label class="field"><span>Phone (optional)</span><input type="tel" name="phone" autocomplete="tel"></label>`}
                    <label class="field"><span>Email</span><input type="email" name="email" autocomplete="email" required></label>
                    <label class="field"><span>Password</span><input type="password" name="password" autocomplete="${isLogin ? "current-password" : "new-password"}" required>
                        ${isLogin ? "" : `<small class="hint">At least 8 characters.</small>`}</label>
                    <button class="btn block" type="submit">${isLogin ? "Log in" : "Create account"}</button>
                </form>
                ${isLogin ? `<p class="hint" style="margin:14px 0 0">Just exploring? <button class="link-btn" data-act="demo">Fill in the demo account</button></p>` : ""}`);
        }

        ACTIONS.tab = el => openAuth(el.dataset.tab);
        ACTIONS.demo = () => {
            const f = $("#authForm");
            f.email.value = "demo@tiffingo.test";
            f.password.value = "demo12345";
        };
        ACTIONS.auth = async () => {
            if (state.user) {
                try { await api("POST", "/v1/user/logout"); } catch (e) { /* session may be gone already */ }
                clearSession(); updateAuthUI(); closeAll();
                toast("Logged out. Your orders stay on your account.");
                openList("home");
            } else openAuth("login");
        };

        document.addEventListener("submit", async e => {
            if (e.target.id !== "authForm") return;
            e.preventDefault();
            const f = e.target, tab = f.dataset.tab;
            const payload = Object.fromEntries(new FormData(f).entries());
            const btn = f.querySelector("button[type=submit]");
            btn.disabled = true;
            try {
                const d = await api("POST", tab === "login" ? "/v1/user/login" : "/v1/user/register", payload);
                state.token = d.token; state.user = d.user;
                try { sessionStorage.setItem("tg_token", d.token); } catch (err) { /* ignore */ }
                closeModal();
                toast(`Logged in as ${d.user.email}.`);
                await loadCart();
                const p = state.pending; state.pending = null;
                if (p) await setQty(p.restId, p.itemId, p.qty);
            } catch (err) {
                $("#authError").innerHTML = `<p class="form-error" role="alert">${esc(err.message)}</p>`;
                btn.disabled = false;
            }
        });

        ACTIONS.checkout = () => {
            if (!state.user) { openAuth("login"); return; }
            closeDrawer();
            const t = state.cart.totals;
            openModal(`
                <h2>Delivery details</h2>
                <div id="bookError"></div>
                <form id="bookForm" novalidate>
                    <p class="hint" style="margin-top:0">Delivering to ${esc(state.area)}. Change the area from the header if needed.</p>
                    <label class="field"><span>Street, house number or landmark</span>
                        <textarea name="address" required maxlength="200" placeholder="Near the main gate, house 12"></textarea></label>
                    <label class="field"><span>Note for the rider (optional)</span>
                        <input type="text" name="note" maxlength="140"></label>
                    <div class="totals"><div class="grand"><span>Total</span><span>${rs(t.total)}</span></div></div>
                    <button class="btn block" style="margin-top:14px" type="submit">Continue to payment</button>
                </form>`);
        };

        document.addEventListener("submit", async e => {
            if (e.target.id !== "bookForm") return;
            e.preventDefault();
            const f = e.target;
            const btn = f.querySelector("button[type=submit]");
            btn.disabled = true;
            const { lat, lng } = loc();
            try {
                const d = await api("POST", "/v1/book", {
                    address: f.address.value, note: f.note.value, area: state.area, lat, lng
                });
                state.cart = null; updateAuthUI(); renderItems();
                showPayment(d.order);
            } catch (err) {
                $("#bookError").innerHTML = `<p class="form-error" role="alert">${esc(err.message)}</p>`;
                btn.disabled = false;
            }
        });

        function showPayment(order) {
            state.order = order;
            if (!state.idem[order.id]) state.idem[order.id] = "idem-" + Math.random().toString(36).slice(2) + Date.now();
            const lines = order.items.map(i => `<div><span>${i.qty} x ${esc(i.name)}</span><span>${rs(i.price * i.qty)}</span></div>`).join("");
            openModal(`
                <h2>Pay for order ${esc(order.id)}</h2>
                <p class="hint" style="margin-top:0">${esc(order.restaurantName)}. Your order is held for 10 minutes.</p>
                <div class="totals" style="margin-bottom:16px">
                    ${lines}
                    <div><span>Delivery</span><span>${rs(order.deliveryFee)}</span></div>
                    <div><span>VAT 13%</span><span>${rs(order.vat)}</span></div>
                    <div class="grand"><span>Total</span><span>${rs(order.total)}</span></div>
                </div>
                <div id="payError"></div>
                <form id="payForm" novalidate>
                    <label class="pay-option"><input type="radio" name="method" value="wallet" checked>
                        <span>Wallet<small>Demo balance ${rs(state.user.wallet)}</small></span></label>
                    <label class="pay-option"><input type="radio" name="method" value="card">
                        <span>Card<small>Any 16 digits works. A number ending in 0000 is declined.</small></span></label>
                    <label class="pay-option"><input type="radio" name="method" value="cod">
                        <span>Cash on delivery<small>Pay the rider when food arrives</small></span></label>
                    <div id="cardFields" hidden>
                        <label class="field"><span>Card number</span><input type="text" name="number" inputmode="numeric" autocomplete="cc-number" placeholder="4242 4242 4242 4242"></label>
                        <div class="row2">
                            <label class="field"><span>Expiry (MM/YY)</span><input type="text" name="expiry" autocomplete="cc-exp" placeholder="09/29"></label>
                            <label class="field"><span>Security code</span><input type="password" name="cvv" inputmode="numeric" autocomplete="cc-csc" maxlength="3"></label>
                        </div>
                    </div>
                    <button class="btn block" type="submit" id="payBtn">Pay ${rs(order.total)}</button>
                </form>`);
        }

        document.addEventListener("change", e => {
            if (e.target.name !== "method") return;
            $("#cardFields").hidden = e.target.value !== "card";
            $("#payBtn").textContent = e.target.value === "cod" ? "Place order" : "Pay " + rs(state.order.total);
        });

        document.addEventListener("submit", async e => {
            if (e.target.id !== "payForm") return;
            e.preventDefault();
            const f = e.target, order = state.order;
            const method = f.method.value;
            const btn = $("#payBtn");
            btn.disabled = true;
            const payload = { bookID: order.id, method, idempotencyKey: state.idem[order.id] };
            if (method === "card") payload.card = { number: f.number.value, expiry: f.expiry.value, cvv: f.cvv.value };
            try {
                const d = await api("POST", "/v1/payment", payload);
                state.user.wallet = d.wallet;
                updateAuthUI(); closeModal();
                toast(method === "cod" ? `Order placed. Pay ${rs(order.total)} in cash when the rider arrives.` : `Paid ${rs(order.total)} from your ${method === "wallet" ? "wallet" : "card"}. The kitchen has your order.`);
                openTracking(order.id);
            } catch (err) {
                $("#payError").innerHTML = `<p class="form-error" role="alert">${esc(err.message)}</p>`;
                btn.disabled = false;
                if (err.status === 410) setTimeout(closeModal, 1500);
            }
        });

        const STATUS_TEXT = {
            PAYMENT_PENDING: ["Awaiting payment", "warn"], EXPIRED: ["Expired", "bad"],
            CONFIRMED: ["Confirmed", ""], PREPARING: ["Preparing", ""], PICKED_UP: ["Picked up", ""],
            OUT_FOR_DELIVERY: ["On the way", ""], DELIVERED: ["Delivered", ""]
        };

        ACTIONS.orders = async () => {
            stopPolling(); state.mode = "orders"; closeAll();
            try {
                const d = await api("GET", "/v1/orders");
                const rows = d.orders.map(o => {
                    const [label, cls] = STATUS_TEXT[o.status] || [o.status, ""];
                    return `<button class="order-row" data-act="track" data-id="${esc(o.id)}">
                                <span><strong>${esc(o.restaurantName)}</strong><br>
                                <span class="hint">${esc(o.id)}, ${o.itemCount} items, ${rs(o.total)}</span></span>
                                <span class="status ${cls}">${label}</span>
                            </button>`;
                }).join("");
                $("#view").innerHTML = `<h1>My orders</h1><p class="sub">Newest first.</p>` +
                    (rows || `<div class="empty"><p>No orders yet.</p><button class="btn" data-act="home">Find something to eat</button></div>`);
            } catch (e) { toast(e.message, "error"); }
        };

        ACTIONS.track = el => openTracking(el.dataset.id);

        async function openTracking(id) {
            stopPolling();
            state.mode = "track"; state.trackId = id;
            closeAll();
            await pollTrack(id, false);
            state.timer = setInterval(() => pollTrack(id, true), 3000);
        }

        async function pollTrack(id, poll) {
            try {
                const d = await api("GET", `/v1/delivery/${id}/tracking`, null, { poll });
                if (state.mode !== "track" || state.trackId !== id) return;
                renderTrack(d);
                if (d.status === "DELIVERED" || d.status === "EXPIRED") stopPolling();
            } catch (e) {
                if (e.status === 401 || e.status === 404) stopPolling();
                toast(e.message, "error");
            }
        }

        function mapSvg(d) {
            const pts = [d.restaurant, d.destination, d.rider].filter(Boolean);
            const lats = pts.map(p => p.lat), lngs = pts.map(p => p.lng);
            const minLat = Math.min(...lats), minLng = Math.min(...lngs);
            const spanLat = Math.max(Math.max(...lats) - minLat, 0.004);
            const spanLng = Math.max(Math.max(...lngs) - minLng, 0.004);
            const X = p => 30 + ((p.lng - minLng) / spanLng) * 260;
            const Y = p => 170 - ((p.lat - minLat) / spanLat) * 140;
            const r = d.rider;
            return `<svg class="map" viewBox="0 0 320 200" role="img" aria-label="Map showing the rider between the restaurant and your address">
                        <line class="route" x1="${X(d.restaurant)}" y1="${Y(d.restaurant)}" x2="${X(d.destination)}" y2="${Y(d.destination)}"></line>
                        <rect class="shop" x="${X(d.restaurant) - 7}" y="${Y(d.restaurant) - 7}" width="14" height="14" rx="3"></rect>
                        <text x="${X(d.restaurant) + 11}" y="${Y(d.restaurant) + 4}">Restaurant</text>
                        <circle class="home" cx="${X(d.destination)}" cy="${Y(d.destination)}" r="7"></circle>
                        <text x="${X(d.destination) + 11}" y="${Y(d.destination) + 4}">You</text>
                        ${r ? `<circle class="rider" cx="${X(r)}" cy="${Y(r)}" r="8"></circle>` : ""}
                    </svg>`;
        }

        function renderTrack(d) {
            const [label, cls] = STATUS_TEXT[d.status] || [d.status, ""];
            const items = d.items.map(i => `${i.qty} x ${esc(i.name)}`).join(", ");
            if (!d.stages) {
                const expired = d.status === "EXPIRED";
                $("#view").innerHTML = `
                    <button class="link-btn" data-act="orders">All orders</button>
                    <h1>${esc(d.restaurantName)}</h1>
                    <p class="sub">Order ${esc(d.orderId)}, ${rs(d.total)}</p>
                    <p><span class="status ${cls}">${label}</span></p>
                    <p>${expired ? "This order was not paid in time. Add the dishes to your cart again to reorder."
                                 : `Pay within ${Math.ceil(d.expiresIn / 60)} minutes to confirm it.`}</p>
                    ${expired ? `<button class="btn" data-act="home">Browse restaurants</button>`
                              : `<button class="btn" data-act="payPending" data-id="${esc(d.orderId)}">Pay now</button>`}`;
                return;
            }
            const steps = d.stages.map(s =>
                `<li class="${s.done ? "done" : ""} ${s.current ? "current" : ""}">${esc(s.label)}</li>`).join("");
            const delivered = d.status === "DELIVERED";
            const cod = d.method === "cod" && d.paymentStatus !== "PAID";
            const current = d.stages.find(s => s.current);
            const heading = delivered ? "Delivered" : (current ? current.label : "Order confirmed");
            $("#view").innerHTML = `
                <button class="link-btn" data-act="orders">All orders</button>
                <h1>${esc(heading)}</h1>
                <p class="sub">${esc(d.restaurantName)}, order ${esc(d.orderId)}</p>
                <div class="eta">${delivered ? "Delivered" : d.etaMinutes + " min"}</div>
                <p class="hint">${delivered ? "Dropped at " + esc(d.address) + "." : "Estimated arrival. This demo runs about 20 times faster than real life."}</p>
                <div class="track-grid">
                    <div><ol class="steps">${steps}</ol></div>
                    <div>${mapSvg(d)}
                        ${d.rider ? `<div class="panel"><strong>${esc(d.rider.name)}</strong><br><span class="hint">${esc(d.rider.vehicle)} rider</span></div>` : `<div class="panel hint">Finding a rider near the restaurant.</div>`}
                    </div>
                </div>
                <div class="panel"><strong>${items}</strong><br>
                    <span class="hint">${rs(d.total)}, ${cod ? "pay cash to the rider" : "paid"}. Delivering to ${esc(d.address)}.</span></div>`;
        }

        ACTIONS.payPending = async el => {
            try {
                const d = await api("GET", `/v1/orders/${el.dataset.id}`);
                showPayment(d.order);
            } catch (e) { toast(e.message, "error"); }
        };

        ACTIONS.dev = async el => {
            const box = $("#devBody");
            const open = box.hidden;
            box.hidden = !open;
            el.setAttribute("aria-expanded", String(open));
            if (!open) return;
            let rows = "";
            try {
                const d = await api("GET", "/v1/_gateway/log");
                rows = d.entries.map(r => `<tr><td>${r.time}</td><td>${r.method}</td><td>${esc(r.path)}</td>
                    <td>${r.status}</td><td>${r.ms} ms</td><td>${r.service}</td><td>${r.cache}</td></tr>`).join("");
            } catch (e) { /* ignore */ }
            box.innerHTML = `
                <h2>How this is built</h2>
                <p><strong>Flow:</strong> browser, API gateway (rate limit and logging), then the search, view, cart, book, payment and delivery services, then the data store.</p>
                <p><strong>Search and browse favour availability.</strong> They read an in-memory index with a 30 second cache and never wait on orders or payments.</p>
                <p><strong>Orders and payment favour consistency.</strong> Prices are recomputed on the server, each payment runs as one locked transaction, repeated taps reuse an idempotency key, and an order moves through fixed states.</p>
                <p><strong>Sample data:</strong> the restaurants, ratings, riders and wallet balance are made up for this demo. Nothing here is a real business or a real payment.</p>
                <p><strong>Login policy:</strong> browsing is public. Cart, booking, payment, orders and tracking need a session.</p>
                <h2 style="margin-top:16px">Recent gateway requests</h2>
                <div class="scroll-x"><table><thead><tr><th>Time</th><th>Method</th><th>Path</th><th>Status</th><th>Time taken</th><th>Service</th><th>Cache</th></tr></thead>
                <tbody>${rows || `<tr><td colspan="7">No requests yet.</td></tr>`}</tbody></table></div>
                <p><button class="link-btn" data-act="devRefresh">Refresh log</button></p>`;
        };
        ACTIONS.devRefresh = () => { $("#devBody").hidden = true; ACTIONS.dev($('[data-act="dev"]')); };

        (async function init() {
            try { state.areas = await api("GET", "/v1/areas"); } catch (e) { /* offline */ }
            if (!state.areas[state.area]) state.area = "Pulchowk";
            fillAreas();
            if (state.token) {
                try { state.user = (await api("GET", "/v1/user/me")).user; } catch (e) { clearSession(); }
            }
            updateAuthUI();
            await loadCart();
            openList("home");
        })();
    </script>
</body>
</html>
"""


@app.get("/")
def index():
    return Response(PAGE, mimetype="text/html")

previous = globals().get("_server")
if previous is not None:
    previous.shutdown()                      # lets you re-run the cell safely

_server = make_server("127.0.0.1", PORT, app, threaded=True)
threading.Thread(target=_server.serve_forever, daemon=True).start()

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height=900)
    print("TiffinGo is running. Demo login: demo@tiffingo.test / demo12345")
except ImportError:
    print(f"TiffinGo is running at http://127.0.0.1:{PORT}  (demo login: demo@tiffingo.test / demo12345)")