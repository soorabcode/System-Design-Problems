'''

Dating-APP

1. Requirement Analysis

Functional Requirements

* Create a profile.
* Update profile.
* Set preferences (age, sex, location).
* Able to swipe left and right.
* Chatting.
* Upload images.

Non-Functional Requirements

* CAP Theorem: Availability >>>> Consistency.
* Minimum latency.

2. Identifying Entities

* User
* Profiles / Swipe
* Chat

3. API Design

Create Profile:

POST /v1/user/{ID}
→ {login + register}

Update Profile:

POST /v1/user/{ID}/update

Set Preference:

POST /v1/{ID}/set
{
  "age": "...",
  "sex": "...",
  "location": "..."
}

Able to Swipe:

GET /v1/users/recommend

Upload Image:

POST /v1/{ID}/upload

Chatting:

WS /v1/{ID}/chat

4. High Level Design
user -> api gateway -> sever ( ) -> database ( )

5. low level design 
![Dating-APP Low-Level Design](Dating-APP.jpg)
'''

import html
import random
import re
import threading
import time
import uuid
from collections import defaultdict, deque
from functools import lru_cache
from urllib.parse import quote

from flask import Flask, Response, jsonify, request
from werkzeug.serving import make_server


def now_ms():
    return int(time.time() * 1000)


def pair_key(a, b):
    return ":".join(sorted([a, b]))


class RedisSim:
    def __init__(self):
        self._lock = threading.Lock()
        self._sets = defaultdict(set)
        self._cache = {}

    def sadd(self, key, *values):
        with self._lock:
            self._sets[key].update(values)

    def sismember(self, key, value):
        with self._lock:
            return value in self._sets.get(key, ())

    def smembers(self, key):
        with self._lock:
            return set(self._sets.get(key, ()))

    def delete(self, *keys):
        with self._lock:
            for key in keys:
                self._sets.pop(key, None)
                self._cache.pop(key, None)

    def setex(self, key, ttl_seconds, value):
        with self._lock:
            self._cache[key] = (time.time() + ttl_seconds, value)

    def get(self, key):
        with self._lock:
            item = self._cache.get(key)
            if not item:
                return None
            if item[0] < time.time():
                del self._cache[key]
                return None
            return item[1]

    def key_count(self):
        with self._lock:
            return len(self._sets) + len(self._cache)


class MongoSim:
    def __init__(self):
        self._lock = threading.Lock()
        self.matches = {}
        self.relations = defaultdict(set)
        self.metadata = defaultdict(lambda: {"likes_sent": 0, "passes": 0, "matches": 0})

    def insert_match(self, a, b):
        key = pair_key(a, b)
        with self._lock:
            if key in self.matches:
                return self.matches[key], False
            doc = {"_id": key, "users": [a, b], "created_at": now_ms(), "status": "active"}
            self.matches[key] = doc
            self.relations[a].add(b)
            self.relations[b].add(a)
            self.metadata[a]["matches"] += 1
            self.metadata[b]["matches"] += 1
            return doc, True

    def bump(self, uid, field):
        with self._lock:
            self.metadata[uid][field] += 1


class ChatDB:
    def __init__(self):
        self._lock = threading.Lock()
        self.messages = defaultdict(list)
        self.media = {}

    def add_message(self, sender, receiver, text="", image=None):
        msg = {
            "id": uuid.uuid4().hex[:10],
            "from": sender,
            "to": receiver,
            "text": text,
            "image": image,
            "ts": now_ms(),
            "read": False,
        }
        with self._lock:
            self.messages[pair_key(sender, receiver)].append(msg)
        return msg

    def total_messages(self):
        with self._lock:
            return sum(len(v) for v in self.messages.values())

user_db = {}
redis = RedisSim()
mongo = MongoSim()
chat_db = ChatDB()


@lru_cache(maxsize=256)
def make_avatar(name, hue, variant=0):
    initial = html.escape((name or "?")[:1].upper())
    hue2 = (hue + 55 + variant * 40) % 360
    shapes = [
        "<circle cx='300' cy='170' r='170' fill='rgba(255,255,255,.18)'/>",
        "<rect x='50' y='400' width='500' height='260' rx='130' fill='rgba(255,255,255,.18)'/>",
        "<circle cx='110' cy='540' r='150' fill='rgba(255,255,255,.18)'/>",
    ][variant % 3]
    svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 600 760'>"
        "<defs><linearGradient id='g' x1='0' y1='0' x2='1' y2='1'>"
        f"<stop offset='0' stop-color='hsl({hue},75%,58%)'/>"
        f"<stop offset='1' stop-color='hsl({hue2},70%,42%)'/>"
        "</linearGradient></defs>"
        "<rect width='600' height='760' fill='url(#g)'/>"
        f"{shapes}"
        "<text x='300' y='430' font-size='260' font-family='Arial,sans-serif' "
        f"font-weight='700' fill='white' text-anchor='middle'>{initial}</text>"
        "</svg>"
    )
    return "data:image/svg+xml;utf8," + quote(svg)

SEEDS = [
    ("Aria", 26, "female", "London", "Coffee snob and weekend hiker. Looking for someone to explore new trails with.", ["Hiking", "Coffee", "Photography"], 340, 3),
    ("Kenji", 29, "male", "Tokyo", "Ramen researcher by night, product designer by day. I will beat you at Mario Kart.", ["Design", "Ramen", "Gaming"], 200, 2),
    ("Maya", 27, "female", "New York", "Bookstore regular, bad at karaoke, great at brunch plans.", ["Books", "Brunch", "Karaoke"], 285, 2),
    ("Leo", 31, "male", "Paris", "Amateur chef. Ask me about my sourdough starter, it has a name.", ["Cooking", "Cycling", "Jazz"], 25, 3),
    ("Zara", 24, "female", "Sydney", "Surf at sunrise, tacos at sunset. Dog person.", ["Surfing", "Dogs", "Travel"], 175, 2),
    ("Noah", 28, "male", "Berlin", "Techno, bouldering and way too many houseplants.", ["Music", "Climbing", "Plants"], 150, 2),
    ("Priya", 25, "female", "Toronto", "Med student with a love for stand-up comedy and long walks.", ["Comedy", "Walking", "Baking"], 315, 3),
    ("Lucas", 30, "male", "London", "Runs a tiny bike shop. Looking for a co-pilot for tandem rides.", ["Cycling", "Coffee", "Travel"], 45, 2),
    ("Sofia", 28, "female", "Paris", "Illustrator who sketches strangers on trains. Yes, maybe you.", ["Art", "Museums", "Wine"], 5, 3),
    ("Ethan", 27, "male", "New York", "Climbing gyms, board games, and pretending to like running.", ["Climbing", "Board games", "Running"], 225, 2),
    ("Yuki", 26, "female", "Tokyo", "Vinyl collector. Will trade you a playlist for a good recipe.", ["Vinyl", "Cooking", "Film"], 255, 2),
    ("Omar", 32, "male", "Sydney", "Marine biologist. I know a lot about octopuses, you have been warned.", ["Diving", "Science", "Beach"], 190, 3),
    ("Elena", 29, "female", "Berlin", "Architect, amateur pianist and a very serious pancake critic.", ["Architecture", "Piano", "Brunch"], 95, 2),
    ("Sam", 25, "other", "Toronto", "Indie game dev. Fuelled by bubble tea and late-night sprints.", ["Gaming", "Coding", "Bubble tea"], 265, 2),
]


def seed_users():
    for name, age, sex, city, bio, interests, hue, n_photos in SEEDS:
        sid = "seed_" + name.lower()
        user_db[sid] = {
            "id": sid,
            "name": name,
            "age": age,
            "sex": sex,
            "city": city,
            "bio": bio,
            "interests": interests,
            "uploads": [],
            "n_photos": n_photos,
            "hue": hue,
            "is_seed": True,
            "prefs": {"age_min": 18, "age_max": 99, "interested_in": "everyone", "location": city, "scope": "any"},
            "created_at": now_ms(),
        }

seed_users()

GREETINGS = [
    "Hey! Looks like we matched 😊",
    "Hi there! Glad we matched. How is your week going?",
    "Well hello! Your profile made me smile.",
    "Hey you 👋 What are you up to this weekend?",
]
REPLIES = [
    "Haha, that is great!",
    "Tell me more about that 😄",
    "I love that. What got you into it?",
    "Ha, same here honestly.",
    "Sounds fun. We should plan something!",
    "Interesting! What is your favourite part?",
    "Ooh, I like your style.",
]


def seed_say(seed_id, to_id, text, delay):
    def _send():
        chat_db.add_message(seed_id, to_id, text=text)
    timer = threading.Timer(delay, _send)
    timer.daemon = True
    timer.start()

VALID_ID = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
SEXES = ("male", "female", "other")
IMAGE_TYPES = ("image/jpeg", "image/png", "image/webp", "image/gif")
MAX_PHOTOS = 6


def photos_of(user):
    if user["uploads"]:
        return list(user["uploads"])
    return [make_avatar(user["name"], user["hue"], v) for v in range(user.get("n_photos", 1))]


def public_view(user):
    return {
        "id": user["id"],
        "name": user["name"],
        "age": user["age"],
        "sex": user["sex"],
        "city": user["city"],
        "bio": user["bio"],
        "interests": user["interests"],
        "photos": photos_of(user),
    }


def own_view(user):
    view = public_view(user)
    view["uploads"] = list(user["uploads"])
    view["prefs"] = dict(user["prefs"])
    view["stats"] = dict(mongo.metadata[user["id"]])
    return view


def clean_profile(data, partial=False):
    out, errors = {}, []
    if "name" in data or not partial:
        name = str(data.get("name", "")).strip()
        if not 1 <= len(name) <= 30:
            errors.append("Name must be 1 to 30 characters")
        out["name"] = name
    if "age" in data or not partial:
        try:
            age = int(data.get("age"))
        except (TypeError, ValueError):
            age = 0
        if not 18 <= age <= 99:
            errors.append("Age must be between 18 and 99")
        out["age"] = age
    if "sex" in data or not partial:
        sex = str(data.get("sex", "")).lower()
        if sex not in SEXES:
            errors.append("Choose male, female or other")
        out["sex"] = sex
    if "city" in data or not partial:
        city = str(data.get("city", "")).strip()[:40]
        if not city:
            errors.append("Add your city")
        out["city"] = city
    if "bio" in data or not partial:
        out["bio"] = str(data.get("bio", "")).strip()[:300]
    if "interests" in data or not partial:
        raw = data.get("interests", [])
        if isinstance(raw, str):
            raw = raw.split(",")
        items = [str(i).strip()[:20] for i in raw if str(i).strip()]
        out["interests"] = items[:8]
    return out, errors


def clean_prefs(data):
    errors = []
    try:
        age_min = int(data.get("age_min"))
        age_max = int(data.get("age_max"))
    except (TypeError, ValueError):
        return None, ["Age range must be numbers"]
    if not (18 <= age_min <= 99 and 18 <= age_max <= 99) or age_min > age_max:
        errors.append("Age range must be between 18 and 99, lowest first")
    interested_in = str(data.get("interested_in", "everyone")).lower()
    if interested_in not in ("male", "female", "other", "everyone"):
        errors.append("Choose who you want to see")
    scope = str(data.get("scope", "any")).lower()
    if scope not in ("city", "any"):
        errors.append("Choose city only or anywhere")
    prefs = {
        "age_min": age_min,
        "age_max": age_max,
        "interested_in": interested_in,
        "location": str(data.get("location", "")).strip()[:40],
        "scope": scope,
    }
    return prefs, errors


def compute_candidates(me):
    prefs = user_db[me]["prefs"]
    swiped = redis.smembers(f"swiped:{me}")
    pool = []
    for cid, cand in list(user_db.items()):
        if cid == me or cid in swiped:
            continue
        if not prefs["age_min"] <= cand["age"] <= prefs["age_max"]:
            continue
        if prefs["interested_in"] != "everyone" and cand["sex"] != prefs["interested_in"]:
            continue
        same_city = bool(prefs["location"]) and cand["city"].lower() == prefs["location"].lower()
        if prefs["scope"] == "city" and prefs["location"] and not same_city:
            continue
        pool.append((0 if same_city else 1, random.random(), cid))
    pool.sort()
    return [cid for _, _, cid in pool]


def recommend(me, limit, exclude):
    swiped = redis.smembers(f"swiped:{me}")
    cache_key = f"reco:{me}"
    cached = redis.get(cache_key)
    source = "cache"
    if cached is None:
        cached = compute_candidates(me)
        redis.setex(cache_key, 30, cached)
        source = "db"
    ids = [i for i in cached if i not in swiped and i not in exclude][:limit]
    if not ids and source == "cache":
        cached = compute_candidates(me)
        redis.setex(cache_key, 30, cached)
        source = "db"
        ids = [i for i in cached if i not in exclude][:limit]
    return ids, source

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 6 * 1024 * 1024

gateway_log = deque(maxlen=40)
latencies = deque(maxlen=200)
rate_windows = defaultdict(deque)
RATE_LIMIT = 300
RATE_WINDOW = 10


def json_body():
    return request.get_json(silent=True) or {}


def fail(message, code=400):
    return jsonify(error=message), code


def service_of(path):
    if "/chat" in path:
        return "Chat"
    if any(part in path for part in ("/swipe", "/recommend", "/matches", "/set")):
        return "Match"
    return "User"


def is_quiet(path):
    if path in ("/", "/v1/system") or path.startswith("/v1/media/"):
        return True
    if request.method == "GET" and ("/chat/" in path or path.endswith("/matches")):
        return True
    return False


@app.before_request
def gateway_in():
    request.environ["hl_t0"] = time.perf_counter()
    if request.path.startswith("/v1/"):
        window = rate_windows[request.remote_addr]
        now = time.time()
        while window and window[0] < now - RATE_WINDOW:
            window.popleft()
        if len(window) >= RATE_LIMIT:
            return fail("Too many requests. Slow down a little.", 429)
        window.append(now)


@app.after_request
def gateway_out(response):
    t0 = request.environ.get("hl_t0")
    if t0 is not None and request.path.startswith("/v1/"):
        ms = (time.perf_counter() - t0) * 1000
        response.headers["X-Latency-ms"] = f"{ms:.1f}"
        latencies.append(ms)
        if not is_quiet(request.path):
            gateway_log.appendleft({
                "t": now_ms(),
                "method": request.method,
                "path": request.path,
                "service": service_of(request.path),
                "status": response.status_code,
                "ms": round(ms, 1),
            })
    return response


@app.errorhandler(413)
def too_large(_):
    return fail("File is too large. Maximum size is 5 MB.", 413)


@app.get("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


@app.post("/v1/user/<uid>")
def login_or_register(uid):
    if not VALID_ID.match(uid):
        return fail("Invalid user id")
    existing = user_db.get(uid)
    data = json_body()
    if existing:
        return jsonify(created=False, profile=own_view(existing))
    if not data:
        return jsonify(error="Profile not found", needs_profile=True), 404
    fields, errors = clean_profile(data)
    if errors:
        return fail("; ".join(errors))
    if fields["sex"] == "male":
        interested_in = "female"
    elif fields["sex"] == "female":
        interested_in = "male"
    else:
        interested_in = "everyone"
    user = {
        "id": uid,
        **fields,
        "uploads": [],
        "n_photos": 1,
        "hue": (sum(map(ord, fields["name"])) * 37) % 360,
        "is_seed": False,
        "prefs": {
            "age_min": 18,
            "age_max": 60,
            "interested_in": interested_in,
            "location": fields["city"],
            "scope": "any",
        },
        "created_at": now_ms(),
    }
    user_db[uid] = user
    for sid, seed in user_db.items():
        if seed.get("is_seed") and random.random() < 0.65:
            redis.sadd(f"likedby:{uid}", sid)
    return jsonify(created=True, profile=own_view(user)), 201


@app.get("/v1/user/<uid>")
def view_profile(uid):
    user = user_db.get(uid)
    if not user:
        return fail("Profile not found", 404)
    return jsonify(profile=own_view(user))


@app.post("/v1/user/<uid>/update")
def update_profile(uid):
    user = user_db.get(uid)
    if not user:
        return fail("Profile not found", 404)
    fields, errors = clean_profile(json_body(), partial=True)
    if errors:
        return fail("; ".join(errors))
    user.update(fields)
    redis.delete(f"reco:{uid}")
    return jsonify(profile=own_view(user))


@app.post("/v1/<uid>/set")
def set_preferences(uid):
    user = user_db.get(uid)
    if not user:
        return fail("Profile not found", 404)
    prefs, errors = clean_prefs(json_body())
    if errors:
        return fail("; ".join(errors))
    user["prefs"] = prefs
    redis.delete(f"reco:{uid}")
    return jsonify(profile=own_view(user))


@app.post("/v1/<uid>/upload")
def upload_image(uid):
    user = user_db.get(uid)
    if not user:
        return fail("Profile not found", 404)
    file = request.files.get("file")
    if not file:
        return fail("Choose an image to upload")
    if file.mimetype not in IMAGE_TYPES:
        return fail("Only JPG, PNG, WEBP or GIF images are allowed", 415)
    data = file.read()
    if len(data) > 5 * 1024 * 1024:
        return fail("File is too large. Maximum size is 5 MB.", 413)
    purpose = request.args.get("purpose", "profile")
    if purpose == "profile" and len(user["uploads"]) >= MAX_PHOTOS:
        return fail(f"You can keep up to {MAX_PHOTOS} photos")
    media_id = uuid.uuid4().hex
    chat_db.media[media_id] = (file.mimetype, data)
    url = f"/v1/media/{media_id}"
    if purpose == "profile":
        user["uploads"].append(url)
        redis.delete(f"reco:{uid}")
    return jsonify(url=url, profile=own_view(user))


@app.post("/v1/<uid>/photo/delete")
def delete_photo(uid):
    user = user_db.get(uid)
    if not user:
        return fail("Profile not found", 404)
    url = json_body().get("url")
    if url in user["uploads"]:
        user["uploads"].remove(url)
    return jsonify(profile=own_view(user))


@app.get("/v1/media/<media_id>")
def get_media(media_id):
    item = chat_db.media.get(media_id)
    if not item:
        return fail("File not found", 404)
    return Response(item[1], mimetype=item[0], headers={"Cache-Control": "public, max-age=3600"})


@app.get("/v1/users/recommend")
def get_recommendations():
    uid = request.args.get("id", "")
    if uid not in user_db:
        return fail("Profile not found", 404)
    try:
        limit = max(1, min(int(request.args.get("limit", 8)), 20))
    except ValueError:
        limit = 8
    exclude = set(filter(None, request.args.get("exclude", "").split(",")))
    ids, source = recommend(uid, limit, exclude)
    return jsonify(profiles=[public_view(user_db[i]) for i in ids], source=source)


@app.post("/v1/<uid>/swipe")
def swipe(uid):
    if uid not in user_db:
        return fail("Profile not found", 404)
    data = json_body()
    target, direction = data.get("target"), data.get("direction")
    if target not in user_db or target == uid or direction not in ("left", "right"):
        return fail("Invalid swipe")
    redis.sadd(f"swiped:{uid}", target)
    if direction == "left":
        mongo.bump(uid, "passes")
        return jsonify(ok=True, matched=False)

    mongo.bump(uid, "likes_sent")
    redis.sadd(f"right:{uid}", target)
    mutual = redis.sismember(f"right:{target}", uid) or redis.sismember(f"likedby:{uid}", target)
    if not mutual:
        return jsonify(ok=True, matched=False)

    _, is_new = mongo.insert_match(uid, target)
    redis.sadd(f"matches:{uid}", target)
    redis.sadd(f"matches:{target}", uid)
    if is_new and user_db[target].get("is_seed"):
        seed_say(target, uid, random.choice(GREETINGS), random.uniform(1.0, 2.0))
    return jsonify(ok=True, matched=True, match=public_view(user_db[target]))


@app.post("/v1/<uid>/swipes/reset")
def reset_swipes(uid):
    if uid not in user_db:
        return fail("Profile not found", 404)
    redis.delete(f"swiped:{uid}", f"reco:{uid}")
    for match_id in redis.smembers(f"matches:{uid}"):
        redis.sadd(f"swiped:{uid}", match_id)
    return jsonify(ok=True)


@app.get("/v1/<uid>/matches")
def list_matches(uid):
    if uid not in user_db:
        return fail("Profile not found", 404)
    rows = []
    for peer in redis.smembers(f"matches:{uid}"):
        history = chat_db.messages.get(pair_key(uid, peer), [])
        last = history[-1] if history else None
        unread = sum(1 for m in history if m["to"] == uid and not m["read"])
        doc = mongo.matches.get(pair_key(uid, peer), {})
        rows.append({
            "profile": public_view(user_db[peer]),
            "matched_at": doc.get("created_at", 0),
            "last": {"text": last["text"] or "📷 Photo", "ts": last["ts"], "mine": last["from"] == uid} if last else None,
            "unread": unread,
        })
    rows.sort(key=lambda r: (r["last"]["ts"] if r["last"] else r["matched_at"]), reverse=True)
    return jsonify(matches=rows)


def chat_guard(uid, peer):
    if uid not in user_db or peer not in user_db:
        return fail("Profile not found", 404)
    if not redis.sismember(f"matches:{uid}", peer):
        return fail("You can only chat with your matches", 403)
    return None


@app.get("/v1/<uid>/chat/<peer>")
def chat_history(uid, peer):
    blocked = chat_guard(uid, peer)
    if blocked:
        return blocked
    try:
        after = max(0, int(request.args.get("after", 0)))
    except ValueError:
        after = 0
    history = chat_db.messages.get(pair_key(uid, peer), [])
    for msg in history:
        if msg["to"] == uid:
            msg["read"] = True
    fresh = history[after:]
    return jsonify(
        messages=[{"id": m["id"], "mine": m["from"] == uid, "text": m["text"], "image": m["image"], "ts": m["ts"]} for m in fresh],
        next=len(history),
    )


@app.post("/v1/<uid>/chat/<peer>")
def chat_send(uid, peer):
    blocked = chat_guard(uid, peer)
    if blocked:
        return blocked
    data = json_body()
    text = str(data.get("text", "")).strip()[:1000]
    image = data.get("image")
    if image and not (str(image).startswith("/v1/media/") and str(image).split("/")[-1] in chat_db.media):
        return fail("Image not found")
    if not text and not image:
        return fail("Write a message or attach a photo")
    chat_db.add_message(uid, peer, text=text, image=image or None)
    if user_db[peer].get("is_seed"):
        reply = "Nice photo! 📸" if image and not text else random.choice(REPLIES)
        seed_say(peer, uid, reply, random.uniform(1.2, 2.8))
    return jsonify(ok=True)


@app.get("/v1/system")
def system_status():
    avg = sum(latencies) / len(latencies) if latencies else 0
    return jsonify(
        counts={
            "users": sum(1 for u in user_db.values() if not u.get("is_seed")),
            "demo_profiles": sum(1 for u in user_db.values() if u.get("is_seed")),
            "redis_keys": redis.key_count(),
            "matches": len(mongo.matches),
            "messages": chat_db.total_messages(),
            "media_files": len(chat_db.media),
        },
        avg_latency_ms=round(avg, 1),
        log=list(gateway_log)[:14],
    )

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>HeartLink</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Fraunces:opsz,wght@9..144,600;9..144,700&display=swap" rel="stylesheet">

    <style>
        :root {
            --ink: #1f1330;
            --plum: #3b1f5c;
            --rose: #f0476b;
            --rose-dark: #d12f55;
            --peach: #ffd9c7;
            --blush: #fbf4f7;
            --line: #ecdfe6;
            --muted: #7d6b88;
            --green: #2fbf71;
            --radius-lg: 24px;
            --radius-md: 14px;
            --font-display: "Fraunces", Georgia, serif;
            --font-body: "DM Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        html,
        body {
            height: 100%;
        }

        body {
            font-family: var(--font-body);
            color: var(--ink);
            background: linear-gradient(160deg, #3b1f5c 0%, #7a2a63 55%, #f0476b 130%);
            display: flex;
            justify-content: center;
            align-items: center;
        }

        button,
        input,
        select,
        textarea {
            font: inherit;
            color: inherit;
        }

        button {
            cursor: pointer;
            border: none;
            background: none;
        }

        :focus-visible {
            outline: 3px solid var(--rose);
            outline-offset: 2px;
        }

        .app {
            width: 100%;
            max-width: 440px;
            height: min(100vh, 820px);
            background: var(--blush);
            display: flex;
            flex-direction: column;
            position: relative;
            overflow: hidden;
        }

        @media (min-width: 520px) {
            .app {
                border-radius: 28px;
                box-shadow: 0 30px 80px rgba(20, 5, 40, 0.5);
            }
        }

        .topbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 14px 20px 10px;
        }

        .brand {
            font-family: var(--font-display);
            font-weight: 700;
            font-size: 26px;
            color: var(--plum);
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .brand-mark {
            width: 28px;
            height: 28px;
            border-radius: 9px;
            background: var(--rose);
            color: #fff;
            font-size: 16px;
            display: grid;
            place-items: center;
        }

        .latency {
            font-size: 12px;
            color: var(--muted);
            background: #fff;
            border: 1px solid var(--line);
            border-radius: 999px;
            padding: 4px 10px;
        }

        .views {
            flex: 1;
            min-height: 0;
            position: relative;
        }

        .view {
            display: none;
            height: 100%;
            flex-direction: column;
        }

        .view.active {
            display: flex;
        }

        .scroll {
            flex: 1;
            min-height: 0;
            overflow-y: auto;
            padding: 8px 20px 24px;
        }

        .view-title {
            font-family: var(--font-display);
            font-size: 26px;
            color: var(--plum);
            margin-bottom: 4px;
        }

        .view-sub {
            color: var(--muted);
            font-size: 14px;
            margin-bottom: 16px;
            line-height: 1.45;
        }

        .tabbar {
            display: flex;
            background: #fff;
            border-top: 1px solid var(--line);
            padding: 6px 8px 10px;
        }

        .tabbar.hidden {
            display: none;
        }

        .tab {
            flex: 1;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 2px;
            padding: 6px 0;
            font-size: 12px;
            color: var(--muted);
            border-radius: 12px;
            position: relative;
        }

        .tab .tab-icon {
            font-size: 20px;
            filter: grayscale(1);
            opacity: 0.7;
        }

        .tab.active {
            color: var(--rose-dark);
            font-weight: 700;
        }

        .tab.active .tab-icon {
            filter: none;
            opacity: 1;
        }

        .badge {
            position: absolute;
            top: 2px;
            left: calc(50% + 8px);
            min-width: 18px;
            height: 18px;
            padding: 0 5px;
            border-radius: 999px;
            background: var(--rose);
            color: #fff;
            font-size: 11px;
            font-weight: 700;
            display: grid;
            place-items: center;
        }

        .badge.hidden {
            display: none;
        }

        .field {
            display: flex;
            flex-direction: column;
            gap: 6px;
            margin-bottom: 14px;
        }

        .field label {
            font-size: 13px;
            font-weight: 700;
            color: var(--plum);
        }

        .field input,
        .field select,
        .field textarea {
            background: #fff;
            border: 1.5px solid var(--line);
            border-radius: var(--radius-md);
            padding: 11px 13px;
            font-size: 15px;
            width: 100%;
        }

        .field textarea {
            resize: vertical;
            min-height: 72px;
        }

        .field input:focus,
        .field select:focus,
        .field textarea:focus {
            outline: none;
            border-color: var(--rose);
        }

        .row {
            display: flex;
            gap: 12px;
        }

        .row .field {
            flex: 1;
        }

        .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            padding: 13px 20px;
            border-radius: 999px;
            font-weight: 700;
            font-size: 15px;
            transition: transform 0.12s ease, background 0.12s ease;
        }

        .btn:active {
            transform: scale(0.97);
        }

        .btn-primary {
            background: var(--rose);
            color: #fff;
        }

        .btn-primary:hover {
            background: var(--rose-dark);
        }

        .btn-ghost {
            background: #fff;
            border: 1.5px solid var(--line);
            color: var(--plum);
        }

        .btn-block {
            width: 100%;
        }

        .btn-danger {
            color: #b3243f;
            background: #fff;
            border: 1.5px solid #f2c4cd;
        }

        .section-title {
            font-family: var(--font-display);
            font-size: 19px;
            color: var(--plum);
            margin: 22px 0 10px;
        }

        .hint {
            color: var(--muted);
            font-size: 13px;
            line-height: 1.45;
            margin-bottom: 12px;
        }

        .deck-wrap {
            flex: 1;
            min-height: 0;
            padding: 6px 16px 0;
            position: relative;
        }

        .deck {
            position: relative;
            width: 100%;
            height: 100%;
        }

        .card {
            position: absolute;
            inset: 0;
            border-radius: var(--radius-lg);
            overflow: hidden;
            background: var(--plum);
            box-shadow: 0 10px 30px rgba(59, 31, 92, 0.25);
            touch-action: none;
            user-select: none;
            transition: transform 0.25s ease;
        }

        .card:not(.top) {
            pointer-events: none;
            transform: scale(0.95) translateY(12px);
        }

        .card-photo {
            position: absolute;
            inset: 0;
            background-size: cover;
            background-position: center;
        }

        .photo-dots {
            position: absolute;
            top: 10px;
            left: 14px;
            right: 14px;
            display: flex;
            gap: 5px;
        }

        .photo-dots span {
            flex: 1;
            height: 4px;
            border-radius: 4px;
            background: rgba(255, 255, 255, 0.4);
        }

        .photo-dots span.on {
            background: #fff;
        }

        .card-info {
            position: absolute;
            left: 0;
            right: 0;
            bottom: 0;
            padding: 60px 20px 20px;
            color: #fff;
            background: linear-gradient(to top, rgba(31, 19, 48, 0.92) 25%, rgba(31, 19, 48, 0));
        }

        .card-info h2 {
            font-family: var(--font-display);
            font-size: 30px;
            line-height: 1.1;
        }

        .card-info h2 span {
            font-weight: 600;
            opacity: 0.85;
        }

        .card-city {
            font-size: 14px;
            margin: 6px 0 8px;
            opacity: 0.9;
        }

        .card-bio {
            font-size: 14px;
            line-height: 1.45;
            margin-bottom: 10px;
        }

        .chips {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }

        .chip {
            font-size: 12px;
            padding: 4px 10px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.2);
            border: 1px solid rgba(255, 255, 255, 0.35);
        }

        .chip.dark {
            background: #fff;
            border: 1px solid var(--line);
            color: var(--plum);
        }

        .stamp {
            position: absolute;
            top: 36px;
            padding: 4px 14px;
            border: 4px solid;
            border-radius: 10px;
            font-family: var(--font-display);
            font-size: 30px;
            opacity: 0;
            pointer-events: none;
        }

        .stamp-like {
            left: 22px;
            color: #5ff0a0;
            border-color: #5ff0a0;
            transform: rotate(-14deg);
        }

        .stamp-nope {
            right: 22px;
            color: #ff7a93;
            border-color: #ff7a93;
            transform: rotate(14deg);
        }

        .actions {
            display: flex;
            justify-content: center;
            gap: 28px;
            padding: 14px 0 16px;
        }

        .round {
            width: 62px;
            height: 62px;
            border-radius: 50%;
            background: #fff;
            font-size: 26px;
            box-shadow: 0 6px 18px rgba(59, 31, 92, 0.18);
            transition: transform 0.12s ease;
        }

        .round:active {
            transform: scale(0.92);
        }

        .round.nope {
            color: #e0415f;
        }

        .round.like {
            color: var(--green);
        }

        .empty {
            text-align: center;
            padding: 60px 28px;
        }

        .empty .big {
            font-size: 44px;
            margin-bottom: 10px;
        }

        .empty h3 {
            font-family: var(--font-display);
            font-size: 22px;
            color: var(--plum);
            margin-bottom: 8px;
        }

        .empty p {
            color: var(--muted);
            font-size: 14px;
            line-height: 1.5;
            margin-bottom: 18px;
        }

        .empty .btn + .btn {
            margin-top: 10px;
        }

        .new-row {
            display: flex;
            gap: 14px;
            overflow-x: auto;
            padding-bottom: 8px;
        }

        .new-item {
            flex: 0 0 auto;
            width: 66px;
            text-align: center;
            font-size: 12px;
            font-weight: 500;
        }

        .avatar {
            border-radius: 50%;
            background-size: cover;
            background-position: center;
            background-color: var(--peach);
        }

        .new-item .avatar {
            width: 66px;
            height: 66px;
            border: 3px solid var(--rose);
            margin-bottom: 4px;
        }

        .convo {
            display: flex;
            align-items: center;
            gap: 12px;
            width: 100%;
            text-align: left;
            padding: 10px 6px;
            border-radius: var(--radius-md);
        }

        .convo:hover {
            background: #fff;
        }

        .convo .avatar {
            width: 52px;
            height: 52px;
            flex: 0 0 auto;
        }

        .convo-body {
            flex: 1;
            min-width: 0;
        }

        .convo-name {
            font-weight: 700;
        }

        .convo-last {
            color: var(--muted);
            font-size: 14px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .convo-last.unread {
            color: var(--ink);
            font-weight: 700;
        }

        .convo-dot {
            width: 10px;
            height: 10px;
            background: var(--rose);
            border-radius: 50%;
        }

        .chat-head {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 6px 16px 12px;
            border-bottom: 1px solid var(--line);
        }

        .chat-head .avatar {
            width: 42px;
            height: 42px;
        }

        .chat-head strong {
            display: block;
        }

        .chat-head small {
            color: var(--muted);
        }

        .icon-btn {
            width: 38px;
            height: 38px;
            border-radius: 50%;
            background: #fff;
            border: 1.5px solid var(--line);
            font-size: 18px;
        }

        .messages {
            flex: 1;
            min-height: 0;
            overflow-y: auto;
            padding: 16px;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .msg {
            max-width: 78%;
            padding: 9px 13px;
            border-radius: 18px;
            font-size: 15px;
            line-height: 1.4;
            background: #fff;
            border: 1px solid var(--line);
            align-self: flex-start;
            border-bottom-left-radius: 6px;
            word-wrap: break-word;
        }

        .msg.mine {
            align-self: flex-end;
            background: var(--rose);
            border-color: var(--rose);
            color: #fff;
            border-bottom-left-radius: 18px;
            border-bottom-right-radius: 6px;
        }

        .msg img {
            display: block;
            max-width: 100%;
            max-height: 220px;
            border-radius: 12px;
            margin-bottom: 4px;
        }

        .msg time {
            display: block;
            font-size: 11px;
            opacity: 0.7;
            margin-top: 3px;
        }

        .composer {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 10px 12px;
            background: #fff;
            border-top: 1px solid var(--line);
        }

        .composer input[type="text"] {
            flex: 1;
            border: 1.5px solid var(--line);
            border-radius: 999px;
            padding: 10px 16px;
            background: var(--blush);
        }

        .composer input[type="text"]:focus {
            outline: none;
            border-color: var(--rose);
        }

        .photo-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-bottom: 8px;
        }

        .photo-slot {
            aspect-ratio: 3 / 4;
            border-radius: var(--radius-md);
            background-size: cover;
            background-position: center;
            position: relative;
            border: 1.5px dashed var(--line);
            background-color: #fff;
        }

        .photo-slot.add {
            display: grid;
            place-items: center;
            font-size: 28px;
            color: var(--rose);
        }

        .photo-slot .remove {
            position: absolute;
            top: 6px;
            right: 6px;
            width: 26px;
            height: 26px;
            border-radius: 50%;
            background: rgba(31, 19, 48, 0.75);
            color: #fff;
            font-size: 14px;
        }

        .stats {
            display: flex;
            gap: 10px;
            margin-bottom: 6px;
        }

        .stat {
            flex: 1;
            background: #fff;
            border: 1px solid var(--line);
            border-radius: var(--radius-md);
            padding: 10px;
            text-align: center;
        }

        .stat b {
            display: block;
            font-family: var(--font-display);
            font-size: 22px;
            color: var(--plum);
        }

        .stat span {
            font-size: 12px;
            color: var(--muted);
        }

        .flow {
            background: #fff;
            border: 1px solid var(--line);
            border-radius: var(--radius-md);
            padding: 14px;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 8px;
            font-size: 13px;
            margin-bottom: 14px;
        }

        .flow-node {
            padding: 8px 16px;
            border-radius: 10px;
            background: var(--blush);
            border: 1px solid var(--line);
            font-weight: 700;
            color: var(--plum);
        }

        .flow-node.gateway {
            background: var(--plum);
            color: #fff;
            border-color: var(--plum);
        }

        .flow-arrow {
            color: var(--muted);
        }

        .flow-cols {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 8px;
            width: 100%;
        }

        .flow-col {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 6px;
            text-align: center;
        }

        .flow-col small {
            color: var(--muted);
            line-height: 1.3;
        }

        .tiles {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-bottom: 14px;
        }

        .log {
            background: #fff;
            border: 1px solid var(--line);
            border-radius: var(--radius-md);
            overflow: hidden;
        }

        .log-row {
            display: grid;
            grid-template-columns: 48px 1fr 50px;
            gap: 8px;
            padding: 8px 12px;
            font-size: 12px;
            border-bottom: 1px solid var(--line);
            align-items: center;
        }

        .log-row:last-child {
            border-bottom: none;
        }

        .log-row .path {
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .log-row .svc {
            font-weight: 700;
            color: var(--rose-dark);
        }

        .log-row .ms {
            text-align: right;
            color: var(--muted);
        }

        .log-empty {
            padding: 16px;
            color: var(--muted);
            font-size: 13px;
        }

        .modal {
            position: absolute;
            inset: 0;
            z-index: 20;
            display: none;
            align-items: center;
            justify-content: center;
            padding: 24px;
            background: rgba(31, 19, 48, 0.82);
        }

        .modal.show {
            display: flex;
        }

        .modal-card {
            width: 100%;
            background: #fff;
            border-radius: var(--radius-lg);
            padding: 28px 22px 22px;
            text-align: center;
            animation: pop 0.3s ease;
        }

        @keyframes pop {
            from {
                transform: scale(0.85);
                opacity: 0;
            }

            to {
                transform: scale(1);
                opacity: 1;
            }
        }

        .modal-card h2 {
            font-family: var(--font-display);
            font-size: 32px;
            color: var(--rose-dark);
            margin-bottom: 6px;
        }

        .modal-card p {
            color: var(--muted);
            margin-bottom: 18px;
        }

        .modal-photos {
            display: flex;
            justify-content: center;
            margin-bottom: 18px;
        }

        .modal-photos .avatar {
            width: 92px;
            height: 92px;
            border: 4px solid #fff;
            box-shadow: 0 6px 18px rgba(59, 31, 92, 0.3);
        }

        .modal-photos .avatar + .avatar {
            margin-left: -18px;
        }

        .modal-card .btn + .btn {
            margin-top: 10px;
        }

        .toast {
            position: absolute;
            left: 50%;
            bottom: 86px;
            transform: translate(-50%, 20px);
            background: var(--ink);
            color: #fff;
            padding: 10px 18px;
            border-radius: 999px;
            font-size: 14px;
            opacity: 0;
            pointer-events: none;
            transition: opacity 0.2s ease, transform 0.2s ease;
            z-index: 30;
            max-width: 90%;
            text-align: center;
        }

        .toast.show {
            opacity: 1;
            transform: translate(-50%, 0);
        }

        @media (prefers-reduced-motion: reduce) {
            * {
                animation: none !important;
                transition: none !important;
            }
        }
    </style>
</head>

<body>
    <div class="app" id="app">

        <header class="topbar">
            <div class="brand">
                <span class="brand-mark">&hearts;</span>
                HeartLink
            </div>
            <div class="latency" id="latency" title="Time the API gateway took for the last request">&#9889; -- ms</div>
        </header>

        <main class="views">

                        <section class="view" id="view-onboarding">
                <div class="scroll">
                    <h1 class="view-title">Create your profile</h1>
                    <p class="view-sub">No account needed. This browser is your identity, so your profile stays here.</p>

                    <div class="field">
                        <label for="ob-name">Name</label>
                        <input id="ob-name" type="text" maxlength="30" placeholder="What should people call you?">
                    </div>
                    <div class="row">
                        <div class="field">
                            <label for="ob-age">Age</label>
                            <input id="ob-age" type="number" min="18" max="99" placeholder="18+">
                        </div>
                        <div class="field">
                            <label for="ob-sex">Sex</label>
                            <select id="ob-sex">
                                <option value="female">Female</option>
                                <option value="male">Male</option>
                                <option value="other">Other</option>
                            </select>
                        </div>
                    </div>
                    <div class="field">
                        <label for="ob-city">City</label>
                        <input id="ob-city" type="text" maxlength="40" placeholder="e.g. London">
                    </div>
                    <div class="field">
                        <label for="ob-bio">About you</label>
                        <textarea id="ob-bio" maxlength="300" placeholder="A line or two that sounds like you"></textarea>
                    </div>
                    <div class="field">
                        <label for="ob-interests">Interests</label>
                        <input id="ob-interests" type="text" placeholder="Separate with commas: hiking, jazz, cooking">
                    </div>
                    <button class="btn btn-primary btn-block" id="ob-submit">Start swiping</button>
                </div>
            </section>

                        <section class="view" id="view-discover">
                <div class="deck-wrap">
                    <div class="deck" id="deck"></div>
                </div>
                <div class="actions" id="actions">
                    <button class="round nope" id="btn-nope" aria-label="Pass">&#10005;</button>
                    <button class="round like" id="btn-like" aria-label="Like">&hearts;</button>
                </div>
            </section>

                        <section class="view" id="view-matches">
                <div class="scroll">
                    <h1 class="view-title">Matches</h1>
                    <p class="view-sub">You both swiped right. Say hello first.</p>
                    <div class="new-row" id="new-row"></div>
                    <h2 class="section-title">Messages</h2>
                    <div id="convos"></div>
                </div>
            </section>

                        <section class="view" id="view-chat">
                <div class="chat-head">
                    <button class="icon-btn" id="chat-back" aria-label="Back to matches">&larr;</button>
                    <div class="avatar" id="chat-avatar" style="width:42px;height:42px"></div>
                    <div>
                        <strong id="chat-name"></strong>
                        <small id="chat-city"></small>
                    </div>
                </div>
                <div class="messages" id="messages"></div>
                <div class="composer">
                    <button class="icon-btn" id="chat-attach" aria-label="Attach a photo">&#128247;</button>
                    <input type="file" id="chat-file" accept="image
        const $ = (id) => document.getElementById(id);
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

        function esc(value) {
            return String(value ?? '').replace(/[&<>"']/g, (c) => ({
                '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
            }[c]));
        }

        function fmtTime(ts) {
            return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }

        function setBg(el, url) {
            el.style.backgroundImage = `url("${url}")`;
        }

        let toastTimer = null;
        function toast(message) {
            const el = $('toast');
            el.textContent = message;
            el.classList.add('show');
            clearTimeout(toastTimer);
            toastTimer = setTimeout(() => el.classList.remove('show'), 2600);
        }

        async function api(path, opts = {}) {
            const init = { method: opts.method || 'GET', headers: {} };
            if (opts.form) {
                init.body = opts.form;
            } else if (opts.body !== undefined) {
                init.headers['Content-Type'] = 'application/json';
                init.body = JSON.stringify(opts.body);
            }
            const res = await fetch(path, init);
            const ms = res.headers.get('X-Latency-ms');
            if (ms) $('latency').innerHTML = `&#9889; ${Math.round(parseFloat(ms))} ms`;
            let data = {};
            try { data = await res.json(); } catch (e) {}
            if (!res.ok) {
                const err = new Error(data.error || `Request failed (${res.status})`);
                err.status = res.status;
                throw err;
            }
            return data;
        }

        function newId() {
            const bytes = new Uint8Array(12);
            crypto.getRandomValues(bytes);
            return 'u' + Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
        }

        let uid = null;
        try { uid = localStorage.getItem('heartlink_uid'); } catch (e) {}
        if (!uid) {
            uid = newId();
            try { localStorage.setItem('heartlink_uid', uid); } catch (e) {}
        }

        let me = null;
        let currentView = null;
        let pollTimer = null;

        function show(view) {
            clearInterval(pollTimer);
            currentView = view;
            document.querySelectorAll('.view').forEach((v) => v.classList.toggle('active', v.id === 'view-' + view));
            const tabView = view === 'chat' ? 'matches' : view;
            document.querySelectorAll('.tab').forEach((t) => t.classList.toggle('active', t.dataset.view === tabView));
            $('tabbar').classList.toggle('hidden', view === 'onboarding' || view === 'chat');

            if (view === 'discover') {
                if (!deck.length) loadDeck();
            } else if (view === 'matches') {
                loadMatches();
                pollTimer = setInterval(loadMatches, 4000);
            } else if (view === 'profile') {
                fillProfile();
            } else if (view === 'system') {
                loadSystem();
                pollTimer = setInterval(loadSystem, 2000);
            } else if (view === 'chat') {
                pollTimer = setInterval(pollChat, 1500);
            }
        }

        document.querySelectorAll('.tab').forEach((tab) => {
            tab.addEventListener('click', () => show(tab.dataset.view));
        });

        async function boot() {
            try {
                const res = await api(`/v1/user/${uid}`, { method: 'POST' });
                me = res.profile;
                show('discover');
            } catch (e) {
                if (e.status === 404) {
                    show('onboarding');
                } else {
                    toast(e.message);
                }
            }
            refreshBadge();
            setInterval(refreshBadge, 6000);
        }

        $('ob-submit').addEventListener('click', async () => {
            const body = {
                name: $('ob-name').value,
                age: $('ob-age').value,
                sex: $('ob-sex').value,
                city: $('ob-city').value,
                bio: $('ob-bio').value,
                interests: $('ob-interests').value
            };
            try {
                const res = await api(`/v1/user/${uid}`, { method: 'POST', body });
                me = res.profile;
                toast('Profile created');
                show('discover');
            } catch (e) {
                toast(e.message);
            }
        });

        let deck = [];
        let loading = false;
        let busy = false;

        async function loadDeck() {
            if (loading) return;
            loading = true;
            try {
                const exclude = deck.map((p) => p.id).join(',');
                const res = await api(`/v1/users/recommend?id=${uid}&limit=8&exclude=${exclude}`);
                deck = deck.concat(res.profiles);
            } catch (e) {
                toast(e.message);
            }
            loading = false;
            renderDeck();
        }

        function renderDeck() {
            const el = $('deck');
            el.innerHTML = '';
            $('actions').style.display = deck.length ? 'flex' : 'none';

            if (!deck.length) {
                el.innerHTML = `
                    <div class="empty">
                        <div class="big">&#127775;</div>
                        <h3>You're all caught up</h3>
                        <p>No new profiles match your preferences right now. Widen your age range or location, or take another look at the people you passed.</p>
                        <button class="btn btn-primary btn-block" id="empty-prefs">Change preferences</button>
                        <button class="btn btn-ghost btn-block" id="empty-reset">See passed profiles again</button>
                    </div>`;
                $('empty-prefs').addEventListener('click', () => show('profile'));
                $('empty-reset').addEventListener('click', resetSwipes);
                return;
            }

            const visible = deck.slice(0, 2);
            for (let i = visible.length - 1; i >= 0; i--) {
                const card = buildCard(visible[i]);
                if (i === 0) {
                    card.classList.add('top');
                    attachDrag(card);
                }
                el.appendChild(card);
            }
        }

        function buildCard(p) {
            const card = document.createElement('div');
            card.className = 'card';
            card.dataset.photo = '0';
            card.innerHTML = `
                <div class="card-photo"></div>
                <div class="photo-dots">${p.photos.map((_, i) => `<span class="${i === 0 ? 'on' : ''}"></span>`).join('')}</div>
                <div class="stamp stamp-like">LIKE</div>
                <div class="stamp stamp-nope">NOPE</div>
                <div class="card-info">
                    <h2>${esc(p.name)} <span>${p.age}</span></h2>
                    <p class="card-city">&#128205; ${esc(p.city)}</p>
                    <p class="card-bio">${esc(p.bio)}</p>
                    <div class="chips">${p.interests.map((i) => `<span class="chip">${esc(i)}</span>`).join('')}</div>
                </div>`;
            setBg(card.querySelector('.card-photo'), p.photos[0]);
            card._photos = p.photos;
            return card;
        }

        function cyclePhoto(card, clientX) {
            const photos = card._photos || [];
            if (photos.length < 2) return;
            const rect = card.getBoundingClientRect();
            const forward = clientX > rect.left + rect.width / 2;
            let idx = parseInt(card.dataset.photo, 10);
            idx = forward ? Math.min(idx + 1, photos.length - 1) : Math.max(idx - 1, 0);
            card.dataset.photo = idx;
            setBg(card.querySelector('.card-photo'), photos[idx]);
            card.querySelectorAll('.photo-dots span').forEach((d, i) => d.classList.toggle('on', i === idx));
        }

        function attachDrag(card) {
            let startX = 0, startY = 0, dx = 0, dragging = false, moved = false;
            const likeStamp = card.querySelector('.stamp-like');
            const nopeStamp = card.querySelector('.stamp-nope');

            card.addEventListener('pointerdown', (e) => {
                if (busy) return;
                dragging = true;
                moved = false;
                startX = e.clientX;
                startY = e.clientY;
                card.setPointerCapture(e.pointerId);
                card.style.transition = 'none';
            });

            card.addEventListener('pointermove', (e) => {
                if (!dragging) return;
                dx = e.clientX - startX;
                const dy = e.clientY - startY;
                if (Math.abs(dx) > 6) moved = true;
                card.style.transform = `translate(${dx}px, ${dy * 0.3}px) rotate(${dx / 18}deg)`;
                likeStamp.style.opacity = Math.max(0, Math.min(dx / 100, 1));
                nopeStamp.style.opacity = Math.max(0, Math.min(-dx / 100, 1));
            });

            const release = (e) => {
                if (!dragging) return;
                dragging = false;
                card.style.transition = '';
                if (dx > 110) {
                    fling('right');
                } else if (dx < -110) {
                    fling('left');
                } else {
                    card.style.transform = '';
                    likeStamp.style.opacity = 0;
                    nopeStamp.style.opacity = 0;
                    if (!moved && e.type === 'pointerup') cyclePhoto(card, e.clientX);
                }
                dx = 0;
            };
            card.addEventListener('pointerup', release);
            card.addEventListener('pointercancel', release);
        }

        async function fling(direction) {
            if (busy || !deck.length) return;
            busy = true;
            const person = deck[0];
            const card = document.querySelector('#deck .card.top');
            if (card) {
                const sign = direction === 'right' ? 1 : -1;
                card.style.transition = 'transform 0.35s ease, opacity 0.35s ease';
                card.style.transform = `translate(${sign * 700}px, -40px) rotate(${sign * 30}deg)`;
                card.style.opacity = 0;
            }
            const request = api(`/v1/${uid}/swipe`, { method: 'POST', body: { target: person.id, direction } }).catch((e) => {
                toast(e.message);
                return null;
            });
            await sleep(300);
            deck.shift();
            renderDeck();
            const result = await request;
            busy = false;
            if (result && result.matched) showMatch(result.match);
            if (deck.length < 3) loadDeck();
            refreshBadge();
        }

        $('btn-like').addEventListener('click', () => fling('right'));
        $('btn-nope').addEventListener('click', () => fling('left'));
        document.addEventListener('keydown', (e) => {
            if (currentView !== 'discover') return;
            if (e.key === 'ArrowRight') fling('right');
            if (e.key === 'ArrowLeft') fling('left');
        });

        async function resetSwipes() {
            try {
                await api(`/v1/${uid}/swipes/reset`, { method: 'POST' });
                deck = [];
                toast('Passed profiles are back');
                show('discover');
                loadDeck();
            } catch (e) {
                toast(e.message);
            }
        }

        let matchPeer = null;

        function showMatch(person) {
            matchPeer = person;
            $('match-text').textContent = `You and ${person.name} liked each other.`;
            setBg($('match-me'), me.photos[0]);
            setBg($('match-them'), person.photos[0]);
            $('match-modal').classList.add('show');
        }

        $('match-close').addEventListener('click', () => $('match-modal').classList.remove('show'));
        $('match-chat').addEventListener('click', () => {
            $('match-modal').classList.remove('show');
            openChat(matchPeer);
        });

        async function loadMatches() {
            try {
                const res = await api(`/v1/${uid}/matches`);
                renderMatches(res.matches);
            } catch (e) {
                toast(e.message);
            }
        }

        function renderMatches(rows) {
            const fresh = rows.filter((r) => !r.last);
            $('new-row').innerHTML = fresh.length
                ? fresh.map((r, i) => `
                    <button class="new-item" data-i="${i}">
                        <div class="avatar" data-photo="${esc(r.profile.photos[0])}"></div>
                        ${esc(r.profile.name)}
                    </button>`).join('')
                : '<p class="hint">New matches show up here.</p>';
            $('new-row').querySelectorAll('.new-item').forEach((btn) => {
                btn.addEventListener('click', () => openChat(fresh[btn.dataset.i].profile));
            });

            const talking = rows.filter((r) => r.last);
            $('convos').innerHTML = talking.length
                ? talking.map((r, i) => `
                    <button class="convo" data-i="${i}">
                        <div class="avatar" data-photo="${esc(r.profile.photos[0])}"></div>
                        <div class="convo-body">
                            <div class="convo-name">${esc(r.profile.name)}</div>
                            <div class="convo-last ${r.unread ? 'unread' : ''}">${r.last.mine ? 'You: ' : ''}${esc(r.last.text)}</div>
                        </div>
                        ${r.unread ? '<span class="convo-dot"></span>' : ''}
                    </button>`).join('')
                : '<p class="hint">No conversations yet. Tap a new match to say hi.</p>';
            $('convos').querySelectorAll('.convo').forEach((btn) => {
                btn.addEventListener('click', () => openChat(talking[btn.dataset.i].profile));
            });

            document.querySelectorAll('#view-matches .avatar[data-photo]').forEach((a) => setBg(a, a.dataset.photo));
            const unread = rows.reduce((n, r) => n + r.unread, 0);
            setBadge(unread);
        }

        function setBadge(count) {
            const badge = $('badge');
            badge.textContent = count;
            badge.classList.toggle('hidden', count === 0);
        }

        async function refreshBadge() {
            if (!me || currentView === 'matches' || currentView === 'chat') return;
            try {
                const res = await api(`/v1/${uid}/matches`);
                setBadge(res.matches.reduce((n, r) => n + r.unread, 0));
            } catch (e) {}
        }

        let peer = null;
        let nextIndex = 0;

        function openChat(person) {
            peer = person;
            nextIndex = 0;
            $('messages').innerHTML = '';
            $('chat-name').textContent = person.name;
            $('chat-city').textContent = person.city;
            setBg($('chat-avatar'), person.photos[0]);
            $('chat-avatar').className = 'avatar';
            show('chat');
            pollChat();
        }

        async function pollChat() {
            if (!peer) return;
            try {
                const res = await api(`/v1/${uid}/chat/${peer.id}?after=${nextIndex}`);
                if (res.messages.length) {
                    const box = $('messages');
                    const stick = box.scrollHeight - box.scrollTop - box.clientHeight < 80 || nextIndex === 0;
                    res.messages.forEach(appendMessage);
                    if (stick) box.scrollTop = box.scrollHeight;
                }
                nextIndex = res.next;
            } catch (e) {}
        }

        function appendMessage(m) {
            const div = document.createElement('div');
            div.className = 'msg' + (m.mine ? ' mine' : '');
            if (m.image) {
                const img = document.createElement('img');
                img.src = m.image;
                img.alt = 'Shared photo';
                div.appendChild(img);
            }
            if (m.text) {
                const span = document.createElement('span');
                span.textContent = m.text;
                div.appendChild(span);
            }
            const time = document.createElement('time');
            time.textContent = fmtTime(m.ts);
            div.appendChild(time);
            $('messages').appendChild(div);
        }

        async function sendMessage(image) {
            const input = $('chat-input');
            const text = input.value.trim();
            if (!text && !image) return;
            input.value = '';
            try {
                await api(`/v1/${uid}/chat/${peer.id}`, { method: 'POST', body: { text, image } });
                await pollChat();
            } catch (e) {
                input.value = text;
                toast(e.message);
            }
        }

        $('chat-send').addEventListener('click', () => sendMessage(null));
        $('chat-input').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') sendMessage(null);
        });
        $('chat-back').addEventListener('click', () => {
            peer = null;
            show('matches');
        });
        $('chat-attach').addEventListener('click', () => $('chat-file').click());
        $('chat-file').addEventListener('change', async (e) => {
            const file = e.target.files[0];
            e.target.value = '';
            if (!file) return;
            const form = new FormData();
            form.append('file', file);
            try {
                const res = await api(`/v1/${uid}/upload?purpose=chat`, { method: 'POST', form });
                await sendMessage(res.url);
            } catch (err) {
                toast(err.message);
            }
        });

        function fillProfile() {
            $('pf-name').value = me.name;
            $('pf-age').value = me.age;
            $('pf-sex').value = me.sex;
            $('pf-city').value = me.city;
            $('pf-bio').value = me.bio;
            $('pf-interests').value = me.interests.join(', ');
            $('pr-min').value = me.prefs.age_min;
            $('pr-max').value = me.prefs.age_max;
            $('pr-sex').value = me.prefs.interested_in;
            $('pr-loc').value = me.prefs.location;
            $('pr-scope').value = me.prefs.scope;

            $('stats').innerHTML = `
                <div class="stat"><b>${me.stats.likes_sent}</b><span>Likes sent</span></div>
                <div class="stat"><b>${me.stats.passes}</b><span>Passes</span></div>
                <div class="stat"><b>${me.stats.matches}</b><span>Matches</span></div>`;
            renderPhotos();
        }

        function renderPhotos() {
            const grid = $('photo-grid');
            grid.innerHTML = '';
            me.uploads.forEach((url) => {
                const slot = document.createElement('div');
                slot.className = 'photo-slot';
                setBg(slot, url);
                const remove = document.createElement('button');
                remove.className = 'remove';
                remove.setAttribute('aria-label', 'Remove photo');
                remove.innerHTML = '&#10005;';
                remove.addEventListener('click', async () => {
                    try {
                        const res = await api(`/v1/${uid}/photo/delete`, { method: 'POST', body: { url } });
                        me = res.profile;
                        renderPhotos();
                    } catch (e) {
                        toast(e.message);
                    }
                });
                slot.appendChild(remove);
                grid.appendChild(slot);
            });
            if (me.uploads.length < 6) {
                const add = document.createElement('button');
                add.className = 'photo-slot add';
                add.setAttribute('aria-label', 'Add a photo');
                add.innerHTML = '+';
                add.addEventListener('click', () => $('photo-file').click());
                grid.appendChild(add);
            }
        }

        $('photo-file').addEventListener('change', async (e) => {
            const file = e.target.files[0];
            e.target.value = '';
            if (!file) return;
            const form = new FormData();
            form.append('file', file);
            try {
                const res = await api(`/v1/${uid}/upload?purpose=profile`, { method: 'POST', form });
                me = res.profile;
                renderPhotos();
                toast('Photo added');
            } catch (err) {
                toast(err.message);
            }
        });

        $('pf-save').addEventListener('click', async () => {
            const body = {
                name: $('pf-name').value,
                age: $('pf-age').value,
                sex: $('pf-sex').value,
                city: $('pf-city').value,
                bio: $('pf-bio').value,
                interests: $('pf-interests').value
            };
            try {
                const res = await api(`/v1/user/${uid}/update`, { method: 'POST', body });
                me = res.profile;
                deck = [];
                toast('Profile saved');
            } catch (e) {
                toast(e.message);
            }
        });

        $('pr-save').addEventListener('click', async () => {
            const body = {
                age_min: $('pr-min').value,
                age_max: $('pr-max').value,
                interested_in: $('pr-sex').value,
                location: $('pr-loc').value,
                scope: $('pr-scope').value
            };
            try {
                const res = await api(`/v1/${uid}/set`, { method: 'POST', body });
                me = res.profile;
                deck = [];
                toast('Preferences saved');
            } catch (e) {
                toast(e.message);
            }
        });

        $('btn-reset-swipes').addEventListener('click', resetSwipes);
        $('btn-new-identity').addEventListener('click', () => {
            if (!confirm('Start over with a new anonymous profile on this browser?')) return;
            try { localStorage.removeItem('heartlink_uid'); } catch (e) {}
            location.reload();
        });

        async function loadSystem() {
            try {
                const res = await api('/v1/system');
                const c = res.counts;
                const tiles = [
                    [c.users, 'Users (User DB)'],
                    [c.redis_keys, 'Redis keys'],
                    [c.matches, 'Matches (Mongo)'],
                    [c.messages, 'Messages (Chat DB)'],
                    [c.media_files, 'Media files'],
                    [res.avg_latency_ms + ' ms', 'Avg gateway latency']
                ];
                $('tiles').innerHTML = tiles.map((t) => `<div class="stat"><b>${t[0]}</b><span>${t[1]}</span></div>`).join('');
                $('log').innerHTML = res.log.length
                    ? res.log.map((r) => `
                        <div class="log-row">
                            <span class="svc">${esc(r.service)}</span>
                            <span class="path">${esc(r.method)} ${esc(r.path)}</span>
                            <span class="ms">${r.ms} ms</span>
                        </div>`).join('')
                    : '<div class="log-empty">Requests will appear here as you use the app.</div>';
            } catch (e) {}
        }

        boot();
    </script>
</body>

</html>
"""

import logging
logging.getLogger("werkzeug").setLevel(logging.ERROR)

PORT = 5057

if globals().get("_hl_server") is not None:
    try:
        _hl_server.shutdown()
        time.sleep(0.5)
    except Exception:
        pass

_hl_server = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=_hl_server.serve_forever, daemon=True).start()
time.sleep(1)

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height=840)
    print("HeartLink is running. Use the app above (open the System tab to watch the gateway).")
except ImportError:
    print(f"HeartLink is running at http://127.0.0.1:{PORT}")