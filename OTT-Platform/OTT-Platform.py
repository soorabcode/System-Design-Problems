'''
OTT PLATFORM  

1. Requirement analysis 
A. functional requrement 
- create account
- subscripiton of different plans
- search and view tv shows
- watch tv shows
B. non functional requrement 
- scale : 100M MAU & 10k Videos
- availability : Cap Theoream
- latency of search < 2ms 

2. Identifying Entities 
- user
- videos

3. API 
A. Account Creation 
POST /v1/user/regoster + login/logout -> confirmation 
B. Subscrption
GET /v1/subscrption/plan-> detaiils
C. search videos
GET /v1/search?query={search}-> List<videoID> :pagenation 
D. View Videos
GET /v1/video={videoD} -> video details 
E. Watch Videos
GET /v1/videos/play={videoID}-> videop play


4. Hih level design 
user -> api gateway -> sever ( account , subscription , search , watch ) -> database

5. low level design 
uploaded image

''' 

import hashlib, io, math, os, re, secrets, threading, time, uuid
from collections import OrderedDict
from flask import Flask, jsonify, request, send_file, Response
from werkzeug.serving import make_server


# 1. DATA STORES  
USER_DB = {}                 # email -> {id,name,email,pw_hash,salt,subscription}
PAYMENT_DB = []              # [{user_id, plan, amount, txn, ts}]
VIDEO_DB = OrderedDict()     # video_id -> metadata
BLOB_S3 = {}                 # blob key -> bytes  (uploaded video files)
SESSIONS = {}                # token -> email

PLANS = {
    "basic":    {"id": "basic",    "name": "Basic",    "price": 4.99,  "tier": 1, "quality": "480p SD",  "screens": 1},
    "standard": {"id": "standard", "name": "Standard", "price": 9.99,  "tier": 2, "quality": "1080p HD", "screens": 2},
    "premium":  {"id": "premium",  "name": "Premium",  "price": 14.99, "tier": 3, "quality": "4K UHD",   "screens": 4},
}

def hash_pw(pw, salt):
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, 50_000).hex()

# 2. ACCOUNT SERVICE  (-> User DB)

class AccountService:
    @staticmethod
    def register(name, email, password):
        email = (email or "").strip().lower()
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
            raise ValueError("Invalid email address")
        if not password or len(password) < 4:
            raise ValueError("Password must be at least 4 characters")
        if email in USER_DB:
            raise ValueError("Account already exists")
        salt = os.urandom(16)
        USER_DB[email] = {"id": str(uuid.uuid4())[:8], "name": name or email.split("@")[0],
                          "email": email, "salt": salt, "pw_hash": hash_pw(password, salt),
                          "subscription": None}
        return USER_DB[email]

    @staticmethod
    def login(email, password):
        u = USER_DB.get((email or "").strip().lower())
        if not u or not secrets.compare_digest(u["pw_hash"], hash_pw(password or "", u["salt"])):
            raise PermissionError("Invalid email or password")
        token = secrets.token_hex(16)
        SESSIONS[token] = u["email"]
        return token, u

    @staticmethod
    def public(u):
        return {"id": u["id"], "name": u["name"], "email": u["email"], "subscription": u["subscription"]}

# 3. PAYMENT SERVICE  (-> Payment DB: user ID + subscription)

class PaymentService:
    @staticmethod
    def charge(user, plan, card):
        digits = re.sub(r"\D", "", card or "")
        if len(digits) != 16:
            raise ValueError("Payment failed: card number must have 16 digits")
        txn = "TXN-" + secrets.token_hex(4).upper()
        PAYMENT_DB.append({"user_id": user["id"], "subscription": plan["id"],
                           "amount": plan["price"], "txn": txn, "ts": time.time()})
        return txn

# 4. SUBSCRIPTION SERVICE  (-> User DB + Payment service)

class SubscriptionService:
    @staticmethod
    def subscribe(user, plan_id, card):
        plan = PLANS.get(plan_id)
        if not plan:
            raise ValueError("Unknown plan")
        txn = PaymentService.charge(user, plan, card)
        user["subscription"] = plan["id"]        
        return txn

    @staticmethod
    def cancel(user):
        user["subscription"] = None

# 5. SEARCH SERVICE  (-> inverted index "Elastic Search" -> LRU Cache)

def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())

class SearchService:
    index = {}                       
    cache = OrderedDict()            
    CACHE_SIZE = 256
    lock = threading.Lock()

    @classmethod
    def index_video(cls, v):
        with cls.lock:
            for t in set(tokenize(" ".join([v["title"], v["genre"], v["description"], v["cast"]]))):
                cls.index.setdefault(t, set()).add(v["id"])
            cls.cache.clear()         

    @classmethod
    def search(cls, query, page=1, size=4):
        t0 = time.perf_counter()
        q = " ".join(tokenize(query))
        hit = q in cls.cache
        if hit:
            cls.cache.move_to_end(q)
            ids = cls.cache[q]
        else:
            toks = q.split()
            if not toks:
                ids = list(VIDEO_DB.keys())
            else:
                sets = []
                for t in toks:       
                    s = set()
                    for k, v in cls.index.items():
                        if k.startswith(t):
                            s |= v
                    sets.append(s)
                found = set.intersection(*sets) if sets else set()
                ids = [i for i in VIDEO_DB if i in found]
            cls.cache[q] = ids
            if len(cls.cache) > cls.CACHE_SIZE:
                cls.cache.popitem(last=False)
        total = len(ids)
        pages = max(1, math.ceil(total / size))
        page = min(max(1, page), pages)
        page_ids = ids[(page - 1) * size: page * size]
        ms = (time.perf_counter() - t0) * 1000
        return {"query": query, "videoIds": page_ids, "page": page, "pages": pages, "total": total,
                "cacheHit": hit, "latencyMs": round(ms, 3)}

# 6. UPLOADER -> BLOB S3 -> CHUNKER -> VIDEO DB

class Chunker:
    CHUNK = 512 * 1024
    @staticmethod
    def split(data):
        n = max(1, math.ceil(len(data) / Chunker.CHUNK))
        return [{"index": i, "bytes": min(Chunker.CHUNK, len(data) - i * Chunker.CHUNK)} for i in range(n)]

class Uploader:
    @staticmethod
    def upload(title, genre, description, tier, file_bytes=None, url=None):
        vid = "v" + secrets.token_hex(3)
        meta = {"id": vid, "title": title, "genre": genre or "General", "description": description or "",
                "cast": "", "year": time.localtime().tm_year, "duration": "—", "tier": int(tier or 1),
                "source": "url", "url": url, "blob": None, "chunks": []}
        if file_bytes:
            key = f"videos/{vid}.mp4"
            BLOB_S3[key] = file_bytes                    # Blob S3
            meta.update(source="blob", blob=key, chunks=Chunker.split(file_bytes))  # Chunker
        VIDEO_DB[vid] = meta                              # Video DB
        SearchService.index_video(meta)
        return meta

SAMPLE = "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/"
SEED = [
    ("Big Buck Bunny", "Animation", "A giant rabbit takes revenge on three bullying rodents.", "Blender Foundation", 2008, "9:56", 1, "BigBuckBunny.mp4"),
    ("Elephants Dream", "Sci-Fi", "Two strange characters explore a surreal mechanical world.", "Orange Studio", 2006, "10:53", 1, "ElephantsDream.mp4"),
    ("Sintel", "Fantasy", "A young woman searches for her baby dragon across the world.", "Blender Foundation", 2010, "14:48", 2, "Sintel.mp4"),
    ("Tears of Steel", "Sci-Fi", "Warriors and scientists fight robots to save the future.", "Blender Foundation", 2012, "12:14", 2, "TearsOfSteel.mp4"),
    ("For Bigger Blazes", "Action", "Fiery action packed short with spectacular visuals.", "Google", 2013, "0:15", 1, "ForBiggerBlazes.mp4"),
    ("For Bigger Escapes", "Adventure", "A quick getaway adventure short.", "Google", 2013, "0:15", 1, "ForBiggerEscapes.mp4"),
    ("For Bigger Fun", "Comedy", "Light-hearted fun in fifteen seconds.", "Google", 2013, "1:00", 2, "ForBiggerFun.mp4"),
    ("For Bigger Joyrides", "Adventure", "Take a joyride through spectacular roads.", "Google", 2013, "0:15", 2, "ForBiggerJoyrides.mp4"),
    ("For Bigger Meltdowns", "Drama", "A dramatic meltdown in short form.", "Google", 2013, "0:15", 3, "ForBiggerMeltdowns.mp4"),
    ("Subaru Outback On Street And Dirt", "Documentary", "A car review across tarmac and trails.", "Garage419", 2012, "9:19", 3, "SubaruOutbackOnStreetAndDirt.mp4"),
]
for i, (t, g, d, c, y, dur, tier, f) in enumerate(SEED, 1):
    v = {"id": f"v{i:03d}", "title": t, "genre": g, "description": d, "cast": c, "year": y,
         "duration": dur, "tier": tier, "source": "url", "url": SAMPLE + f, "blob": None, "chunks": []}
    VIDEO_DB[v["id"]] = v
    SearchService.index_video(v)

# dummy user
AccountService.register("Demo User", "user@sg.com", "1234")

# 7. API GATEWAY  (auth, routing) + REST API
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024

def card(v):
    return {k: v[k] for k in ("id", "title", "genre", "description", "cast", "year", "duration", "tier")}

def current_user():
    h = request.headers.get("Authorization", "")
    token = h[7:] if h.startswith("Bearer ") else request.args.get("token", "")
    email = SESSIONS.get(token)
    return USER_DB.get(email) if email else None

def auth(fn):
    from functools import wraps
    @wraps(fn)
    def w(*a, **k):
        u = current_user()
        if not u:
            return jsonify(error="Please log in"), 401
        return fn(u, *a, **k)
    return w

# Account 
@app.post("/v1/user/register")
def register():
    d = request.get_json(force=True)
    try:
        AccountService.register(d.get("name"), d.get("email"), d.get("password"))
        token, u = AccountService.login(d.get("email"), d.get("password"))
    except ValueError as e:
        return jsonify(error=str(e)), 400
    return jsonify(message="Account created", token=token, user=AccountService.public(u)), 201

@app.post("/v1/user/login")
def login():
    d = request.get_json(force=True)
    try:
        token, u = AccountService.login(d.get("email"), d.get("password"))
    except PermissionError as e:
        return jsonify(error=str(e)), 401
    return jsonify(message="Login successful", token=token, user=AccountService.public(u))

@app.post("/v1/user/logout")
@auth
def logout(u):
    h = request.headers.get("Authorization", "")[7:]
    SESSIONS.pop(h, None)
    return jsonify(message="Logged out")

@app.get("/v1/user/me")
@auth
def me(u):
    return jsonify(user=AccountService.public(u))

# Subscription
@app.get("/v1/subscription/plan")
def plans():
    return jsonify(plans=list(PLANS.values()))

@app.post("/v1/subscription/subscribe")
@auth
def subscribe(u):
    d = request.get_json(force=True)
    try:
        txn = SubscriptionService.subscribe(u, d.get("plan"), d.get("card"))
    except ValueError as e:
        return jsonify(error=str(e)), 402
    return jsonify(message="Subscribed", txn=txn, user=AccountService.public(u))

@app.post("/v1/subscription/cancel")
@auth
def cancel(u):
    SubscriptionService.cancel(u)
    return jsonify(message="Subscription cancelled", user=AccountService.public(u))

# Search / View / Watch 
@app.get("/v1/search")
@auth
def search(u):
    r = SearchService.search(request.args.get("query", ""), int(request.args.get("page", 1)),
                             int(request.args.get("size", 4)))
    r["videos"] = [card(VIDEO_DB[i]) for i in r["videoIds"]]
    return jsonify(r)

@app.get("/v1/video/<vid>")
@auth
def video(u, vid):
    v = VIDEO_DB.get(vid)
    if not v:
        return jsonify(error="Video not found"), 404
    return jsonify(video=card(v), chunks=len(v["chunks"]))

def can_watch(u, v):
    p = PLANS.get(u["subscription"] or "")
    return bool(p and p["tier"] >= v["tier"])

@app.get("/v1/videos/play/<vid>")
@auth
def play(u, vid):
    v = VIDEO_DB.get(vid)
    if not v:
        return jsonify(error="Video not found"), 404
    if not u["subscription"]:
        return jsonify(error="Subscription required. Please choose a plan."), 403
    if not can_watch(u, v):
        need = next(p["name"] for p in PLANS.values() if p["tier"] == v["tier"])
        return jsonify(error=f"This title needs the {need} plan or higher."), 403
    token = request.headers.get("Authorization", "")[7:]
    url = v["url"] if v["source"] == "url" else f"/v1/stream/{vid}?token={token}"
    return jsonify(videoId=vid, title=v["title"], streamUrl=url,
                   quality=PLANS[u["subscription"]]["quality"])

@app.get("/v1/stream/<vid>")
@auth
def stream(u, vid):
    v = VIDEO_DB.get(vid)
    if not v or v["source"] != "blob" or not can_watch(u, v):
        return jsonify(error="Not available"), 403
    return send_file(io.BytesIO(BLOB_S3[v["blob"]]), mimetype="video/mp4", conditional=True)

# Uploader 
@app.post("/v1/videos/upload")
@auth
def upload(u):
    title = request.form.get("title", "").strip()
    if not title:
        return jsonify(error="Title is required"), 400
    f = request.files.get("file")
    url = request.form.get("url", "").strip() or None
    if not f and not url:
        return jsonify(error="Provide a video file or a video URL"), 400
    meta = Uploader.upload(title, request.form.get("genre"), request.form.get("description"),
                           request.form.get("tier", 1), f.read() if f else None, url)
    return jsonify(message="Uploaded, chunked and indexed", video=card(meta), chunks=len(meta["chunks"])), 201

# 8. FRONT-END  (inline HTML + CSS + JS)
PAGE = r"""<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>StreamGate OTT</title>
    <meta name="color-scheme" content="dark">
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='6' fill='%23e50914'/><path d='M12 9l12 7-12 7z' fill='white'/></svg>">

    <style>
        :root {
            --bg: #0e1016;
            --panel: #161924;
            --panel2: #1d2130;
            --txt: #eceef6;
            --mut: #98a0b8;
            --acc: #e50914;
            --ok: #2ecc71;
            --bd: #2a2f45;
            --r: 6px;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: system-ui, "Segoe UI", Roboto, sans-serif;
            background: var(--bg);
            color: var(--txt);
        }

        button,
        input,
        select,
        textarea {
            font: inherit;
        }

        :focus-visible {
            outline: 2px solid #fff;
            outline-offset: 2px;
        }

        .hdr {
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px 20px;
            padding: 12px 20px;
            background: var(--panel);
            border-bottom: 1px solid var(--bd);
            position: sticky;
            top: 0;
            z-index: 5;
        }

        .logo {
            font-weight: 800;
            font-size: 20px;
            color: var(--acc);
        }

        .logo span {
            color: var(--txt);
        }

        .logo-lg {
            font-size: 26px;
            margin-bottom: 16px;
        }

        .nav {
            display: flex;
            gap: 18px;
        }

        .nav button {
            padding: 6px 0;
            background: none;
            border: 0;
            border-bottom: 2px solid transparent;
            color: var(--mut);
            cursor: pointer;
        }

        .nav button.on {
            color: var(--txt);
            border-bottom-color: var(--acc);
        }

        .search {
            flex: 1;
            min-width: 200px;
            display: flex;
        }

        .search input {
            flex: 1;
            padding: 9px 12px;
            border: 1px solid var(--bd);
            border-right: 0;
            border-radius: var(--r) 0 0 var(--r);
            background: var(--bg);
            color: var(--txt);
        }

        .search button {
            padding: 0 16px;
            border: 0;
            border-radius: 0 var(--r) var(--r) 0;
            background: var(--panel2);
            border: 1px solid var(--bd);
            color: var(--txt);
            cursor: pointer;
        }

        .user {
            display: flex;
            align-items: center;
            gap: 12px;
            font-size: 13px;
            color: var(--mut);
        }

        .tag {
            padding: 2px 6px;
            border: 1px solid var(--bd);
            border-radius: 3px;
            font-size: 11px;
            color: var(--txt);
        }

        .tag.sub {
            border-color: var(--ok);
            color: var(--ok);
        }

        .btn {
            min-height: 38px;
            padding: 8px 16px;
            border: 1px solid var(--acc);
            border-radius: var(--r);
            background: var(--acc);
            color: #fff;
            font-weight: 600;
            cursor: pointer;
        }

        .btn.alt {
            background: transparent;
            border-color: var(--bd);
            color: var(--txt);
        }

        .btn:disabled {
            opacity: 0.45;
            cursor: default;
        }

        main {
            max-width: 1100px;
            margin: 0 auto;
            padding: 24px 20px 40px;
        }

        h2 {
            margin: 0 0 6px;
        }

        .meta {
            margin: 0 0 16px;
            color: var(--mut);
            font-size: 13px;
        }

        .err {
            min-height: 18px;
            color: #ff7a7a;
            font-size: 13px;
        }

        .hide {
            display: none !important;
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(230px, 1fr));
            gap: 16px;
        }

        .vc {
            display: block;
            width: 100%;
            padding: 0;
            text-align: left;
            background: var(--panel);
            border: 1px solid var(--bd);
            border-radius: var(--r);
            color: inherit;
            cursor: pointer;
            overflow: hidden;
        }

        .vc:hover {
            border-color: var(--mut);
        }

        .vc .bd {
            padding: 10px 12px 12px;
        }

        .vc small {
            color: var(--mut);
        }

        .th {
            display: flex;
            align-items: flex-end;
            justify-content: space-between;
            gap: 8px;
            height: 120px;
            padding: 10px 12px;
            font-weight: 700;
            font-size: 17px;
        }

        .th .tier {
            font-size: 11px;
            font-weight: 500;
            opacity: 0.8;
            white-space: nowrap;
        }

        .pager {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 12px;
            margin-top: 24px;
            color: var(--mut);
        }

        .plans {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
            gap: 16px;
        }

        .plan {
            padding: 18px;
            background: var(--panel);
            border: 1px solid var(--bd);
            border-radius: var(--r);
        }

        .plan.cur {
            border-color: var(--ok);
        }

        .plan h3 {
            margin: 0;
        }

        .plan .pr {
            margin: 6px 0 10px;
            font-size: 28px;
            font-weight: 700;
        }

        .plan ul {
            margin: 0 0 16px;
            padding-left: 18px;
            color: var(--mut);
            line-height: 1.7;
        }

        .box {
            max-width: 400px;
            margin: 56px auto;
            padding: 24px;
            background: var(--panel);
            border: 1px solid var(--bd);
            border-radius: var(--r);
        }

        label {
            display: block;
            font-size: 13px;
            color: var(--mut);
        }

        .box input,
        .form input,
        .form select,
        .form textarea {
            width: 100%;
            margin: 4px 0 14px;
            padding: 9px 12px;
            border: 1px solid var(--bd);
            border-radius: var(--r);
            background: var(--bg);
            color: var(--txt);
        }

        .form {
            max-width: 540px;
        }

        .tabs {
            display: flex;
            gap: 8px;
            margin-bottom: 16px;
        }

        .tabs button {
            flex: 1;
        }

        .hint {
            margin-top: 14px;
            padding-top: 12px;
            border-top: 1px solid var(--bd);
            color: var(--mut);
            font-size: 12px;
            line-height: 1.6;
        }

        .modal {
            position: fixed;
            inset: 0;
            display: none;
            align-items: center;
            justify-content: center;
            padding: 16px;
            background: rgba(0, 0, 0, 0.75);
            z-index: 20;
        }

        .modal.on {
            display: flex;
        }

        .mc {
            width: min(800px, 100%);
            max-height: 94vh;
            overflow: auto;
            padding: 20px;
            background: var(--panel);
            border: 1px solid var(--bd);
            border-radius: var(--r);
        }

        .mc-head {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
        }

        video {
            width: 100%;
            margin: 12px 0 4px;
            background: #000;
        }

        .toast {
            position: fixed;
            bottom: 24px;
            left: 50%;
            transform: translateX(-50%);
            display: none;
            max-width: 90vw;
            padding: 10px 16px;
            background: var(--panel2);
            border: 1px solid var(--bd);
            border-radius: var(--r);
            z-index: 30;
        }

        @media (max-width: 640px) {
            .hdr {
                padding: 10px 14px;
            }

            .search {
                order: 3;
                flex-basis: 100%;
            }

            .user {
                margin-left: auto;
            }

            main {
                padding: 16px 14px 40px;
            }

            .grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>

<body>

    <div id="auth">
        <div class="box">
            <div class="logo logo-lg">
                Stream<span>Gate</span>
            </div>

            <div class="tabs">
                <button class="btn" id="tl">Login</button>
                <button class="btn alt" id="tr">Create account</button>
            </div>

            <div id="nameRow" class="hide">
                <label for="aname">Name</label>
                <input id="aname" placeholder="Your name">
            </div>

            <label for="aemail">Email</label>
            <input id="aemail" value="user@sg.com">

            <label for="apass">Password</label>
            <input id="apass" type="password" value="1234">

            <div class="err" id="aerr"></div>
            <button class="btn" style="width: 100%;" id="abtn">Login</button>

            <div class="hint">
                Demo account &rarr; <b>user@sg.com</b> / <b>1234</b><br>
                POST /v1/user/register &middot; /login &middot; /logout
            </div>
        </div>
    </div>

    <div id="app" class="hide">

        <header class="hdr">
            <div class="logo">Stream<span>Gate</span></div>

            <nav class="nav">
                <button data-v="browse" class="on">Browse</button>
                <button data-v="plans">Plans</button>
                <button data-v="upload">Upload</button>
            </nav>

            <div class="search">
                <input id="q" aria-label="Search the catalog" placeholder="Title, genre or cast">
                <button id="sb">Search</button>
            </div>

            <div class="user">
                <span id="uname"></span>
                <span class="tag" id="usub"></span>
                <button class="btn alt" id="lo">Logout</button>
            </div>
        </header>

        <main>
            <section id="v-browse">
                <h2 id="bt">Catalog</h2>
                <p class="meta" id="sm"></p>
                <div class="grid" id="grid"></div>
                <div class="pager" id="pager"></div>
            </section>

            <section id="v-plans" class="hide">
                <h2>Subscription plans</h2>
                <p class="meta">
                    GET /v1/subscription/plan &middot; payments are simulated (any 16-digit card works)
                </p>

                <div class="form" style="margin-bottom: 16px;">
                    <label for="card">Card number (demo)</label>
                    <input id="card" value="4242 4242 4242 4242">
                </div>

                <div class="plans" id="plans"></div>
            </section>

            <section id="v-upload" class="hide">
                <h2>Upload a video</h2>
                <p class="meta">Uploader &rarr; Blob S3 &rarr; Chunker &rarr; Video DB &rarr; Search index</p>

                <div class="form">
                    <label for="ut">Title *</label>
                    <input id="ut">

                    <label for="ug">Genre</label>
                    <input id="ug" value="General">

                    <label for="ud">Description</label>
                    <textarea id="ud" rows="2"></textarea>

                    <label for="utier">Minimum plan</label>
                    <select id="utier">
                        <option value="1">Basic</option>
                        <option value="2">Standard</option>
                        <option value="3">Premium</option>
                    </select>

                    <label for="uf">Video file (mp4, keep it small)</label>
                    <input type="file" id="uf" accept="video/*">
                    <label for="uu">or a direct video URL</label>
                    <input id="uu" placeholder="https://example.com/clip.mp4">

                    <button class="btn" id="ub">Upload video</button>
                    <div class="err" id="uerr"></div>
                </div>
            </section>
        </main>
    </div>

    <div class="modal" id="vm" role="dialog" aria-modal="true" aria-labelledby="vt">
        <div class="mc">
            <div class="mc-head">
                <h2 id="vt"></h2>
                <button class="btn alt" id="vx" aria-label="Close">&times;</button>
            </div>

            <p class="meta" id="vmeta"></p>
            <p id="vd"></p>

            <div id="pl"></div>

            <div style="display: flex; gap: 10px;">
                <button class="btn" id="vp">&#9654; Play</button>
            </div>
        </div>
    </div>

    <div class="toast" id="toast"></div>

    <script>
        const $ = s => document.querySelector(s);
        const $$ = s => [...document.querySelectorAll(s)];

        const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;"
        }[c]));

        let token = null;
        let user = null;
        let query = "";
        let page = 1;
        let curVid = null;
        const TIER = ["", "Basic", "Standard", "Premium"];

        async function api(path, opts = {}) {
            opts.headers = Object.assign(
                opts.body instanceof FormData ? {} : { "Content-Type": "application/json" },
                opts.headers || {}
            );
            if (token) {
                opts.headers.Authorization = "Bearer " + token;
            }

            const r = await fetch(path, opts);
            const j = await r.json().catch(() => ({}));

            if (!r.ok) {
                throw Object.assign(new Error(j.error || r.statusText), { status: r.status });
            }
            return j;
        }

        function toast(msg) {
            const t = $("#toast");
            t.textContent = msg;
            t.style.display = "block";
            setTimeout(() => (t.style.display = "none"), 2600);
        }

        let mode = "login";

        function setMode(m) {
            mode = m;
            $("#nameRow").classList.toggle("hide", m !== "register");
            $("#abtn").textContent = m === "login" ? "Login" : "Create account";
            $("#tl").className = "btn" + (m === "login" ? "" : " alt");
            $("#tr").className = "btn" + (m === "register" ? "" : " alt");
            $("#aerr").textContent = "";
        }

        $("#tl").onclick = () => setMode("login");
        $("#tr").onclick = () => setMode("register");

        $("#abtn").onclick = async () => {
            try {
                const j = await api("/v1/user/" + mode, {
                    method: "POST",
                    body: JSON.stringify({
                        name: $("#aname").value,
                        email: $("#aemail").value,
                        password: $("#apass").value
                    })
                });
                token = j.token;
                user = j.user;
                enter();
                toast(j.message);
            } catch (e) {
                $("#aerr").textContent = e.message;
            }
        };

        $("#lo").onclick = async () => {
            try {
                await api("/v1/user/logout", { method: "POST" });
            } catch (e) { }
            token = null;
            user = null;
            $("#app").classList.add("hide");
            $("#auth").classList.remove("hide");
        };

        function enter() {
            $("#auth").classList.add("hide");
            $("#app").classList.remove("hide");
            paintUser();
            go("browse");
            search("", 1);
        }

        function paintUser() {
            const tiers = { basic: 1, standard: 2, premium: 3 };
            const badge = $("#usub");
            $("#uname").textContent = user.name;
            badge.textContent = user.subscription ? TIER[tiers[user.subscription]] + " plan" : "No plan";
            badge.className = "tag" + (user.subscription ? " sub" : "");
        }

        function go(view) {
            $$(".nav button").forEach(b => b.classList.toggle("on", b.dataset.v === view));
            ["browse", "plans", "upload"].forEach(x => {
                $("#v-" + x).classList.toggle("hide", x !== view);
            });
            if (view === "plans") {
                loadPlans();
            }
        }

        $$(".nav button").forEach(b => (b.onclick = () => go(b.dataset.v)));

        function hue(id) {
            let h = 0;
            for (const c of id) {
                h = (h * 31 + c.charCodeAt(0)) % 360;
            }
            return h;
        }

        function videoCard(v) {
            const h = hue(v.id);
            return `
                <button class="vc" data-id="${esc(v.id)}">
                    <div class="th" style="background: hsl(${h} 40% 26%);">
                        <span>${esc(v.title)}</span>
                        <span class="tier">${TIER[v.tier]}+</span>
                    </div>
                    <div class="bd">
                        <b>${esc(v.genre)}</b> &middot; <small>${v.year} &middot; ${esc(v.duration)}</small><br>
                        <small>${esc(v.description)}</small>
                    </div>
                </button>`;
        }

        async function search(q, p) {
            query = q;
            page = p;
            try {
                const j = await api("/v1/search?query=" + encodeURIComponent(q) + "&page=" + p + "&size=8");

                $("#bt").textContent = q ? 'Results for "' + q + '"' : "Catalog";
                $("#sm").textContent = j.total + " result(s)";

                $("#grid").innerHTML = j.videos.map(videoCard).join("") ||
                    `<p class="meta">No titles match "${esc(q)}". Try a single word, a genre or a cast name.</p>`;
                $$(".vc").forEach(c => (c.onclick = () => openVideo(c.dataset.id)));

                $("#pager").innerHTML = `
                    <button class="btn alt" id="pp" ${j.page <= 1 ? "disabled" : ""}>&larr; Prev</button>
                    <span>Page ${j.page} / ${j.pages}</span>
                    <button class="btn alt" id="pn" ${j.page >= j.pages ? "disabled" : ""}>Next &rarr;</button>`;
                $("#pp").onclick = () => search(query, page - 1);
                $("#pn").onclick = () => search(query, page + 1);
            } catch (e) {
                toast(e.message);
            }
        }

        $("#sb").onclick = () => {
            go("browse");
            search($("#q").value, 1);
        };

        $("#q").onkeydown = e => {
            if (e.key === "Enter") {
                $("#sb").click();
            }
        };

        async function openVideo(id) {
            try {
                const j = await api("/v1/video/" + id);
                const v = j.video;
                curVid = id;

                $("#vt").textContent = v.title;
                $("#vmeta").textContent =
                    `${v.genre} \u00b7 ${v.year} \u00b7 ${v.duration} \u00b7 Cast: ${v.cast || "\u2014"} \u00b7 Needs ${TIER[v.tier]}+ plan` +
                    (j.chunks ? ` \u00b7 ${j.chunks} chunks` : "");
                $("#vd").textContent = v.description;
                $("#pl").innerHTML = "";
                $("#vm").classList.add("on");
                $("#vx").focus();
            } catch (e) {
                toast(e.message);
            }
        }

        $("#vx").onclick = () => {
            $("#pl").innerHTML = "";
            $("#vm").classList.remove("on");
        };

        $("#vp").onclick = async () => {
            try {
                const j = await api("/v1/videos/play/" + curVid);
                $("#pl").innerHTML = `
                    <video controls autoplay src="${esc(j.streamUrl)}"></video>
                    <p class="meta">Streaming at ${esc(j.quality)}</p>`;
            } catch (e) {
                $("#pl").innerHTML = `<p class="err">${esc(e.message)}</p>`;
                if (e.status === 403) {
                    $("#pl").innerHTML += '<button class="btn" id="gp">Choose a plan</button>';
                    $("#gp").onclick = () => {
                        $("#vx").click();
                        go("plans");
                    };
                }
            }
        };

        function planCard(p) {
            const current = user.subscription === p.id;
            return `
                <div class="plan ${current ? "cur" : ""}">
                    <h3>${p.name}</h3>
                    <div class="pr">$${p.price}<small style="font-size: 13px; color: var(--mut);">/mo</small></div>
                    <ul>
                        <li>${p.quality}</li>
                        <li>${p.screens} screen(s)</li>
                        <li>Titles up to ${TIER[p.tier]} tier</li>
                    </ul>
                    <button class="btn" data-p="${p.id}" ${current ? "disabled" : ""}>
                        ${current ? "Current plan" : "Subscribe, $" + p.price + "/mo"}
                    </button>
                </div>`;
        }

        async function loadPlans() {
            const j = await api("/v1/subscription/plan");

            $("#plans").innerHTML =
                j.plans.map(planCard).join("") +
                (user.subscription
                    ? `<div class="plan">
                           <h3>Cancel</h3>
                           <p class="meta">Stop your subscription</p>
                           <button class="btn alt" id="cn">Cancel plan</button>
                       </div>`
                    : "");

            $$("[data-p]").forEach(b => {
                b.onclick = async () => {
                    try {
                        const r = await api("/v1/subscription/subscribe", {
                            method: "POST",
                            body: JSON.stringify({ plan: b.dataset.p, card: $("#card").value })
                        });
                        user = r.user;
                        paintUser();
                        loadPlans();
                        toast("Plan activated. Payment ref " + r.txn);
                    } catch (e) {
                        toast(e.message);
                    }
                };
            });

            const cancel = $("#cn");
            if (cancel) {
                cancel.onclick = async () => {
                    const r = await api("/v1/subscription/cancel", { method: "POST" });
                    user = r.user;
                    paintUser();
                    loadPlans();
                    toast(r.message);
                };
            }
        }

        $("#ub").onclick = async () => {
            const fd = new FormData();
            fd.append("title", $("#ut").value);
            fd.append("genre", $("#ug").value);
            fd.append("description", $("#ud").value);
            fd.append("tier", $("#utier").value);
            fd.append("url", $("#uu").value);
            if ($("#uf").files[0]) {
                fd.append("file", $("#uf").files[0]);
            }

            $("#uerr").textContent = "";
            try {
                const j = await api("/v1/videos/upload", { method: "POST", body: fd });
                toast(j.message + (j.chunks ? ` (${j.chunks} chunks)` : ""));
                $("#ut").value = "";
                go("browse");
                search("", 1);
            } catch (e) {
                $("#uerr").textContent = e.message;
            }
        };

        document.addEventListener("keydown", e => {
            if (e.key === "Escape" && $("#vm").classList.contains("on")) {
                $("#vx").click();
            }
        });
    </script>
</body>

</html>"""

@app.get("/")
def index():
    return Response(PAGE, mimetype="text/html")

#  9. RUN INSIDE COLAB
PORT = 5050
try:
    _server.shutdown()          # allows re-running the cell
except NameError:
    pass
_server = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=_server.serve_forever, daemon=True).start()

try:
    from google.colab import output
  #  print("StreamGate is live  ->  login: user@sg.com / 1234")
    output.serve_kernel_port_as_iframe(PORT, height=900)
except ImportError:
    print(f"Running locally: open http://localhost:{PORT}   (login: user@sg.com / 1234)")