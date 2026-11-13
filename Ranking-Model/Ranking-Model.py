'''
Ranking Model for Instagram

1. Requirement Analysis

Functional Requirements

* Suggest posts from content creators.
* ML model is aligned with business.
* Viewing content, engaging, liking and commenting.

Non-Functional Requirements

* Scale: 10M DAU.
* CAP Theorem: Available >>> Consistent.

2. Identifying Entities

* User
* Platform
* Content

3. Pipeline Design

Candidate Generation → Ranking → Post Processing

* User
* Feed View
* Application Feed
* Database
* Feature Store
* Offline Pipeline
* Model Train

4. High-Level Design

       User
         |
         v
      Feed View
         |
         v
       Content
         |
         v
     Content DB


5. low level design 
![Ranking-Model Low-Level Design](Ranking-Model.jpg)

'''
import math
import random
import time
import uuid
import threading

from flask import Flask, jsonify, request, Response
from werkzeug.serving import make_server

PORT = 5055
LOCK = threading.RLock()
random.seed(7)

CATEGORIES = ["Travel", "Food", "Fitness", "Tech", "Fashion", "Music", "Art", "Pets"]
CREATOR_NAMES = {
    "Travel": ["wander.maya", "trail.tales"],
    "Food": ["momo.diaries", "spice.route"],
    "Fitness": ["lift.with.arun", "run.club.ktm"],
    "Tech": ["circuit.sutra", "code.and.chai"],
    "Fashion": ["thread.theory", "street.style.np"],
    "Music": ["strings.sessions", "beat.lab"],
    "Art": ["ink.and.hue", "clay.corner"],
    "Pets": ["paws.daily", "tiny.tails"],
}
CAPTIONS = {
    "Travel": [
        "Sunrise above the clouds after a 4am start.",
        "Back streets of an old town, mapped on foot.",
        "Three weeks, one carry-on bag.",
    ],
    "Food": [
        "Steamed momos on a rainy evening.",
        "How a dal bhat plate is put together.",
        "Five street food stops in one night.",
    ],
    "Fitness": [
        "Leg day, full session.",
        "10 km before sunrise.",
        "The mobility routine that fixed my posture.",
    ],
    "Tech": [
        "A small radio receiver built from scratch.",
        "Two hours into a bug that was one wrong index.",
        "My desk setup for long work sessions.",
    ],
    "Fashion": [
        "One thrifted jacket, three outfits.",
        "Monochrome looks for the festive season.",
        "Handloom textiles up close.",
    ],
    "Music": [
        "Unplugged session by the lake.",
        "A beat built from kitchen sounds.",
        "First time playing this riff live.",
    ],
    "Art": [
        "Watercolor study of morning light on the hills.",
        "The first clay bowl that survived the kiln.",
        "Ink sketches from the morning commute.",
    ],
    "Pets": [
        "Our puppy sees snow for the first time.",
        "Cat against a cardboard box.",
        "Rescue dog, six months later.",
    ],
}

CREATORS = {}
CONTENT = {}


def seed_content():
    k = 0
    for cat in CATEGORIES:
        for name in CREATOR_NAMES[cat]:
            cid = "c%d" % k
            k += 1
            quality = round(random.uniform(0.5, 0.95), 2)
            CREATORS[cid] = {"id": cid, "name": name, "category": cat, "quality": quality}
            for j in range(3):
                pid = "p%d" % len(CONTENT)
                impressions = int(random.uniform(300, 3000) * (0.5 + quality))
                likes = int(impressions * random.uniform(0.03, 0.12) * (0.6 + quality))
                comments = int(likes * random.uniform(0.03, 0.12))
                CONTENT[pid] = {
                    "id": pid,
                    "creator": cid,
                    "category": cat,
                    "caption": CAPTIONS[cat][j],
                    "created_at": time.time() - random.uniform(1, 120) * 3600,
                    "impressions": impressions,
                    "likes": likes,
                    "comments": comments,
                    "comments_list": [],
                }


seed_content()

FEATURES = [
    "bias", "category_affinity", "creator_affinity", "follows_creator",
    "popularity", "like_rate", "comment_rate", "recency",
    "creator_quality", "already_seen",
]
FEATURE_STORE = {"content": {}, "last_run": None, "runs": 0, "last_source": "-"}


def compute_content_features(p, now):
    age_h = (now - p["created_at"]) / 3600.0
    return {
        "like_rate": round((p["likes"] + 2) / (p["impressions"] + 20), 4),
        "comment_rate": round((p["comments"] + 1) / (p["impressions"] + 50), 4),
        "popularity": round(min(1.0, math.log1p(p["likes"]) / math.log1p(1000)), 4),
        "recency": round(math.exp(-age_h / 48.0), 4),
        "age_hours": round(age_h, 1),
        "creator_quality": CREATORS[p["creator"]]["quality"],
        "snapshot_impressions": p["impressions"],
    }


def run_feature_pipeline(source="manual"):
    with LOCK:
        now = time.time()
        for pid, p in CONTENT.items():
            FEATURE_STORE["content"][pid] = compute_content_features(p, now)
        FEATURE_STORE["last_run"] = now
        FEATURE_STORE["runs"] += 1
        FEATURE_STORE["last_source"] = source


run_feature_pipeline("startup")

USERS = {}


def get_user(uid):
    u = USERS.get(uid)
    if u is None:
        u = {
            "cat_aff": {}, "creator_aff": {}, "follows": set(), "seen": set(),
            "liked": set(), "interests": [], "created": time.time(), "synthetic": False,
        }
        USERS[uid] = u
    return u


def bump(u, p, delta):
    u["cat_aff"][p["category"]] = u["cat_aff"].get(p["category"], 0.0) + delta
    u["creator_aff"][p["creator"]] = u["creator_aff"].get(p["creator"], 0.0) + delta


MODELS = {}
ACTIVE = {"version": "v1"}
IMPRESSIONS = []
IMP_INDEX = {}
CONFIG = {"w_like": 1.0, "w_comment": 3.0, "w_explore": 0.15, "max_per_creator": 2, "feed_size": 10}
EVENTS = {"feeds": 0, "impressions": 0, "likes": 0, "comments": 0, "latency_sum": 0.0, "fallbacks": 0}


def register_model(weights, note, metrics=None):
    ver = "v%d" % (len(MODELS) + 1)
    MODELS[ver] = {
        "version": ver, "weights": weights, "note": note,
        "metrics": metrics or {}, "created": time.time(),
    }
    return ver


register_model(
    {
        "like": [-3.0, 1.6, 1.0, 0.8, 0.6, 0.8, 0.2, 0.6, 0.5, -1.2],
        "comment": [-4.2, 1.2, 0.8, 0.6, 0.4, 0.4, 0.8, 0.4, 0.4, -1.0],
    },
    "Initial weights set by hand",
)


def sigmoid(z):
    z = max(-30.0, min(30.0, z))
    return 1.0 / (1.0 + math.exp(-z))


def dot(w, x):
    return sum(a * b for a, b in zip(w, x))


def logloss(w, rows, key):
    if not rows:
        return None
    total = 0.0
    for r in rows:
        p = min(1 - 1e-6, max(1e-6, sigmoid(dot(w, r["x"]))))
        total += -(r[key] * math.log(p) + (1 - r[key]) * math.log(1 - p))
    return round(total / len(rows), 4)


def auc(w, rows, key):
    scored = sorted([(sigmoid(dot(w, r["x"])), r[key]) for r in rows], key=lambda t: t[0])
    npos = sum(1 for s in scored if s[1] == 1)
    nneg = len(scored) - npos
    if npos == 0 or nneg == 0:
        return None
    rank_sum = sum(i + 1 for i, s in enumerate(scored) if s[1] == 1)
    return round((rank_sum - npos * (npos + 1) / 2.0) / (npos * nneg), 4)


def train_model(epochs=25, lr=0.1):
    with LOCK:
        rows = list(IMPRESSIONS[-8000:])
        base = MODELS[ACTIVE["version"]]
        base_weights = {k: list(v) for k, v in base["weights"].items()}
    if len(rows) < 60:
        return None, None, (
            "Only %d impressions are logged and training needs at least 60. "
            "Open the feed a few times or generate synthetic sessions on this tab." % len(rows)
        )
    random.shuffle(rows)
    split = int(len(rows) * 0.8)
    train, test = rows[:split], rows[split:]
    new_weights = {}
    metrics = {"rows": len(rows), "base": base["version"]}
    for head, key in (("like", "y_like"), ("comment", "y_comment")):
        w = list(base_weights[head])
        for ep in range(epochs):
            random.shuffle(train)
            step = lr / (1 + 0.1 * ep)
            for r in train:
                g = sigmoid(dot(w, r["x"])) - r[key]
                for i in range(len(w)):
                    reg = 0.0 if i == 0 else 1e-4 * w[i]
                    w[i] -= step * (g * r["x"][i] + reg)
        new_weights[head] = [round(v, 4) for v in w]
        metrics[head + "_logloss_old"] = logloss(base_weights[head], test, key)
        metrics[head + "_logloss_new"] = logloss(w, test, key)
        metrics[head + "_auc_old"] = auc(base_weights[head], test, key)
        metrics[head + "_auc_new"] = auc(w, test, key)
    with LOCK:
        ver = register_model(
            new_weights,
            "Trained from %s on %d logged impressions" % (base["version"], len(rows)),
            metrics,
        )
    return ver, metrics, None


def build_x(u, p):
    cf = FEATURE_STORE["content"].get(p["id"]) or compute_content_features(p, time.time())
    return [
        1.0,
        math.tanh(u["cat_aff"].get(p["category"], 0.0)),
        math.tanh(u["creator_aff"].get(p["creator"], 0.0)),
        1.0 if p["creator"] in u["follows"] else 0.0,
        cf["popularity"],
        min(1.0, cf["like_rate"] * 5),
        min(1.0, cf["comment_rate"] * 25),
        cf["recency"],
        cf["creator_quality"],
        1.0 if p["id"] in u["seen"] else 0.0,
    ]


def candidate_generation(u):
    cands = {}

    def add(pid, src):
        cands.setdefault(pid, set()).add(src)

    posts = list(CONTENT.values())
    for p in posts:
        if p["creator"] in u["follows"]:
            add(p["id"], "following")
    top_cats = sorted([c for c, v in u["cat_aff"].items() if v > 0], key=lambda c: -u["cat_aff"][c])[:3]
    interest = sorted([p for p in posts if p["category"] in top_cats], key=lambda p: -p["likes"])
    for p in interest[:30]:
        add(p["id"], "interest")
    now = time.time()
    trending = sorted(posts, key=lambda p: -(p["likes"] * math.exp(-(now - p["created_at"]) / 3600.0 / 72.0)))
    for p in trending[:15]:
        add(p["id"], "trending")
    for p in sorted(posts, key=lambda p: p["impressions"])[:8]:
        add(p["id"], "low-exposure")
    return cands


def rank(u, cands, model):
    out = []
    for pid, sources in cands.items():
        p = CONTENT[pid]
        x = build_x(u, p)
        p_like = sigmoid(dot(model["weights"]["like"], x))
        p_comment = sigmoid(dot(model["weights"]["comment"], x))
        explore = 1.0 / (1.0 + p["impressions"] / 300.0)
        score = CONFIG["w_like"] * p_like + CONFIG["w_comment"] * p_comment + CONFIG["w_explore"] * explore
        out.append({
            "post": p, "x": x, "sources": sorted(sources), "p_like": p_like,
            "p_comment": p_comment, "explore": explore, "score": score, "demoted": False,
        })
    out.sort(key=lambda r: -r["score"])
    return out


def post_process(ranked, n):
    final, counts, remaining = [], {}, list(ranked)
    while remaining and len(final) < n:
        pick = None
        for r in remaining:
            cid = r["post"]["creator"]
            if counts.get(cid, 0) >= CONFIG["max_per_creator"]:
                continue
            if final and final[-1]["post"]["creator"] == cid:
                continue
            pick = r
            break
        if pick is None:
            break
        for r in remaining:
            if r is pick:
                break
            r["demoted"] = True
        final.append(pick)
        remaining.remove(pick)
        counts[pick["post"]["creator"]] = counts.get(pick["post"]["creator"], 0) + 1
    return final


def serialize_item(r, u, model):
    p = r["post"]
    w = model["weights"]["like"]
    return {
        "id": p["id"],
        "creator_id": p["creator"],
        "creator": CREATORS[p["creator"]]["name"],
        "category": p["category"],
        "caption": p["caption"],
        "likes": p["likes"],
        "comments": p["comments"],
        "age_hours": round((time.time() - p["created_at"]) / 3600.0, 1),
        "liked": p["id"] in u["liked"],
        "following": p["creator"] in u["follows"],
        "sources": r["sources"],
        "p_like": round(r["p_like"], 3),
        "p_comment": round(r["p_comment"], 3),
        "explore": round(r["explore"], 3),
        "score": round(r["score"], 3),
        "demoted": r["demoted"],
        "contrib": [
            {"f": FEATURES[i], "x": round(r["x"][i], 3), "w": round(w[i], 2), "c": round(r["x"][i] * w[i], 3)}
            for i in range(len(FEATURES))
        ],
    }


def serve_feed(uid, n=None):
    t0 = time.time()
    n = n or CONFIG["feed_size"]
    with LOCK:
        u = get_user(uid)
        model = MODELS[ACTIVE["version"]]
        fallback = False
        stats = {"candidates": 0, "by_source": {}, "ranked": 0, "final": 0, "demoted": 0}
        try:
            cands = candidate_generation(u)
            ranked = rank(u, cands, model)
            final = post_process(ranked, n)
            stats["candidates"] = len(cands)
            stats["ranked"] = len(ranked)
            for srcs in cands.values():
                for s in srcs:
                    stats["by_source"][s] = stats["by_source"].get(s, 0) + 1
        except Exception:
            fallback = True
            EVENTS["fallbacks"] += 1
            top = sorted(CONTENT.values(), key=lambda p: -p["likes"])[:n]
            final = [{
                "post": p, "x": build_x(u, p), "sources": ["fallback"], "p_like": 0.0,
                "p_comment": 0.0, "explore": 0.0, "score": 0.0, "demoted": False,
            } for p in top]
        items = [serialize_item(r, u, model) for r in final]
        for r in final:
            p = r["post"]
            p["impressions"] += 1
            u["seen"].add(p["id"])
            imp = {"uid": uid, "pid": p["id"], "x": r["x"], "y_like": 0, "y_comment": 0}
            IMPRESSIONS.append(imp)
            IMP_INDEX[(uid, p["id"])] = imp
        if len(IMPRESSIONS) > 20000:
            del IMPRESSIONS[:2000]
        stats["final"] = len(items)
        stats["demoted"] = sum(1 for i in items if i["demoted"])
        latency = round((time.time() - t0) * 1000, 1)
        EVENTS["feeds"] += 1
        EVENTS["impressions"] += len(items)
        EVENTS["latency_sum"] += latency
        return {
            "items": items,
            "pipeline": dict(
                stats,
                fallback=fallback,
                latency_ms=latency,
                model=model["version"],
                feature_age_s=round(time.time() - FEATURE_STORE["last_run"], 1),
            ),
        }


def do_like(uid, u, p):
    if p["id"] in u["liked"]:
        return
    u["liked"].add(p["id"])
    p["likes"] += 1
    bump(u, p, 0.6)
    imp = IMP_INDEX.get((uid, p["id"]))
    if imp:
        imp["y_like"] = 1
    EVENTS["likes"] += 1


def do_unlike(uid, u, p):
    if p["id"] not in u["liked"]:
        return
    u["liked"].discard(p["id"])
    p["likes"] = max(0, p["likes"] - 1)
    bump(u, p, -0.6)
    imp = IMP_INDEX.get((uid, p["id"]))
    if imp:
        imp["y_like"] = 0


def do_comment(uid, u, p, text):
    p["comments_list"].append({"user": uid[-4:], "text": text, "ts": time.time()})
    p["comments"] += 1
    bump(u, p, 1.0)
    imp = IMP_INDEX.get((uid, p["id"]))
    if imp:
        imp["y_comment"] = 1
    EVENTS["comments"] += 1


def simulate(n_users):
    for _ in range(n_users):
        uid = "sim_" + uuid.uuid4().hex[:8]
        with LOCK:
            get_user(uid)["synthetic"] = True
        favourites = random.sample(CATEGORIES, 2)
        for _round in range(3):
            feed = serve_feed(uid, 8)
            with LOCK:
                u = USERS[uid]
                for it in feed["items"]:
                    p = CONTENT[it["id"]]
                    if random.random() < 0.04 + (0.5 if p["category"] in favourites else 0.0):
                        do_like(uid, u, p)
                        if random.random() < 0.25:
                            do_comment(uid, u, p, "Synthetic comment")
                        if random.random() < 0.10:
                            u["follows"].add(p["creator"])


app = Flask(__name__)


@app.route("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


@app.route("/api/meta")
def api_meta():
    return jsonify(
        categories=CATEGORIES,
        creators=list(CREATORS.values()),
        features=FEATURES,
        config=CONFIG,
    )


@app.route("/api/feed")
def api_feed():
    uid = request.args.get("uid", "anon")
    n = int(request.args.get("n", CONFIG["feed_size"]))
    return jsonify(serve_feed(uid, max(1, min(n, 30))))


@app.route("/api/profile", methods=["GET", "POST"])
def api_profile():
    d = request.get_json(silent=True) or {}
    uid = d.get("uid") or request.args.get("uid", "anon")
    with LOCK:
        u = get_user(uid)
        if request.method == "POST":
            chosen = [c for c in d.get("interests", []) if c in CATEGORIES]
            for c in CATEGORIES:
                prev = 1.0 if c in u["interests"] else 0.0
                now = 1.0 if c in chosen else 0.0
                u["cat_aff"][c] = u["cat_aff"].get(c, 0.0) + (now - prev)
            u["interests"] = chosen
        return jsonify(
            interests=u["interests"],
            follows=sorted(u["follows"]),
            cat_aff={k: round(v, 2) for k, v in u["cat_aff"].items()},
            creator_aff={k: round(v, 2) for k, v in u["creator_aff"].items()},
            seen=len(u["seen"]),
            liked=len(u["liked"]),
        )


@app.route("/api/profile/reset", methods=["POST"])
def api_profile_reset():
    d = request.get_json(force=True)
    with LOCK:
        u = get_user(d.get("uid", "anon"))
        u["seen"].clear()
    return jsonify(ok=True)


@app.route("/api/events", methods=["POST"])
def api_events():
    d = request.get_json(force=True)
    uid, kind = d.get("uid", "anon"), d.get("type")
    with LOCK:
        u = get_user(uid)
        if kind in ("follow", "unfollow"):
            cid = d.get("creator_id")
            if cid not in CREATORS:
                return jsonify(error="Unknown creator id."), 404
            if kind == "follow":
                u["follows"].add(cid)
                u["creator_aff"][cid] = u["creator_aff"].get(cid, 0.0) + 0.5
            else:
                u["follows"].discard(cid)
            return jsonify(ok=True, following=cid in u["follows"])
        p = CONTENT.get(d.get("post_id"))
        if p is None:
            return jsonify(error="Unknown post id."), 404
        if kind == "like":
            do_like(uid, u, p)
        elif kind == "unlike":
            do_unlike(uid, u, p)
        elif kind == "comment":
            text = (d.get("text") or "").strip()[:200]
            if not text:
                return jsonify(error="The comment is empty. Type some text before posting."), 400
            do_comment(uid, u, p, text)
        else:
            return jsonify(error="Unknown event type."), 400
        return jsonify(ok=True, likes=p["likes"], comments=p["comments"], liked=p["id"] in u["liked"])


@app.route("/api/posts/<pid>/comments")
def api_comments(pid):
    p = CONTENT.get(pid)
    if p is None:
        return jsonify(error="Unknown post id."), 404
    return jsonify(comments=[{"user": c["user"], "text": c["text"]} for c in p["comments_list"][-20:]])


@app.route("/api/posts", methods=["POST"])
def api_publish():
    d = request.get_json(force=True)
    cid, caption = d.get("creator_id"), (d.get("caption") or "").strip()[:200]
    if cid not in CREATORS:
        return jsonify(error="Choose a creator."), 400
    if not caption:
        return jsonify(error="The caption is empty. Write a caption before publishing."), 400
    with LOCK:
        pid = "p%d" % len(CONTENT)
        CONTENT[pid] = {
            "id": pid,
            "creator": cid,
            "category": CREATORS[cid]["category"],
            "caption": caption,
            "created_at": time.time(),
            "impressions": 0,
            "likes": 0,
            "comments": 0,
            "comments_list": [],
        }
    return jsonify(ok=True, id=pid)


@app.route("/api/features")
def api_features():
    with LOCK:
        rows = []
        for pid, cf in FEATURE_STORE["content"].items():
            p = CONTENT[pid]
            rows.append(dict(
                cf,
                id=pid,
                creator=CREATORS[p["creator"]]["name"],
                category=p["category"],
                live_impressions=p["impressions"],
            ))
        rows.sort(key=lambda r: -r["popularity"])
        last = FEATURE_STORE["last_run"]
        return jsonify(
            content=rows[:25],
            total=len(rows),
            runs=FEATURE_STORE["runs"],
            last_run_age=round(time.time() - last, 1) if last else None,
            last_source=FEATURE_STORE["last_source"],
        )


@app.route("/api/pipeline/run", methods=["POST"])
def api_pipeline_run():
    run_feature_pipeline("manual")
    return jsonify(ok=True)


@app.route("/api/models")
def api_models():
    with LOCK:
        out = []
        for ver, m in MODELS.items():
            out.append({
                "version": ver,
                "note": m["note"],
                "metrics": m["metrics"],
                "active": ver == ACTIVE["version"],
            })
        return jsonify(models=out[::-1])


@app.route("/api/models/train", methods=["POST"])
def api_train():
    ver, metrics, err = train_model()
    if err:
        return jsonify(error=err), 400
    return jsonify(ok=True, version=ver, metrics=metrics)


@app.route("/api/models/activate", methods=["POST"])
def api_activate():
    ver = request.get_json(force=True).get("version")
    with LOCK:
        if ver not in MODELS:
            return jsonify(error="Unknown model version."), 404
        ACTIVE["version"] = ver
    return jsonify(ok=True, active=ver)


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "POST":
        d = request.get_json(force=True)
        with LOCK:
            for key, lo, hi in (("w_like", 0, 10), ("w_comment", 0, 10), ("w_explore", 0, 2)):
                if key in d:
                    CONFIG[key] = max(lo, min(hi, float(d[key])))
            if "max_per_creator" in d:
                CONFIG["max_per_creator"] = max(1, min(10, int(d["max_per_creator"])))
    return jsonify(CONFIG)


@app.route("/api/simulate", methods=["POST"])
def api_simulate():
    n = max(1, min(int(request.get_json(force=True).get("users", 50)), 400))
    simulate(n)
    return jsonify(ok=True, users=n, logged=len(IMPRESSIONS))


@app.route("/api/stats")
def api_stats():
    with LOCK:
        per_cat = {c: {"impressions": 0, "likes": 0, "comments": 0} for c in CATEGORIES}
        for p in CONTENT.values():
            per_cat[p["category"]]["impressions"] += p["impressions"]
            per_cat[p["category"]]["likes"] += p["likes"]
            per_cat[p["category"]]["comments"] += p["comments"]
        feeds = max(1, EVENTS["feeds"])
        return jsonify(
            users=len(USERS),
            synthetic=sum(1 for u in USERS.values() if u["synthetic"]),
            posts=len(CONTENT),
            feeds=EVENTS["feeds"],
            impressions=EVENTS["impressions"],
            likes=EVENTS["likes"],
            comments=EVENTS["comments"],
            fallbacks=EVENTS["fallbacks"],
            like_rate=round(EVENTS["likes"] / max(1, EVENTS["impressions"]), 4),
            avg_latency_ms=round(EVENTS["latency_sum"] / feeds, 1),
            logged_impressions=len(IMPRESSIONS),
            active_model=ACTIVE["version"],
            per_category=per_cat,
        )


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Feed ranking console</title>
    <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Crect width='16' height='16' fill='%231f4e79'/%3E%3Ctext x='8' y='12' font-size='11' font-family='sans-serif' text-anchor='middle' fill='white'%3ER%3C/text%3E%3C/svg%3E">
    <style>
        :root {
            --text: #1a1a1a;
            --muted: #5f5f5f;
            --line: #d4d4d4;
            --accent: #1f4e79;
            --error: #a32020;
            --surface: #f6f6f4;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
            font-size: 14px;
            line-height: 1.5;
            color: var(--text);
            background: #ffffff;
        }

        .wrap {
            max-width: 1000px;
            margin: 0 auto;
            padding: 0 16px;
        }

        header {
            border-bottom: 1px solid var(--line);
            padding: 14px 0 0;
        }

        header h1 {
            margin: 0 0 8px;
            font-size: 17px;
            font-weight: 600;
        }

        nav {
            display: flex;
            flex-wrap: wrap;
            gap: 4px 18px;
        }

        nav button {
            border: 0;
            border-bottom: 2px solid transparent;
            background: none;
            padding: 6px 0;
            font: inherit;
            color: var(--muted);
            cursor: pointer;
        }

        nav button[aria-selected="true"] {
            color: var(--text);
            border-bottom-color: var(--accent);
            font-weight: 600;
        }

        #status {
            min-height: 22px;
            margin: 10px 0 0;
            color: var(--muted);
        }

        #status.error {
            color: var(--error);
        }

        main {
            padding: 6px 0 40px;
        }

        h2 {
            margin: 18px 0 8px;
            font-size: 15px;
            font-weight: 600;
        }

        p {
            margin: 6px 0;
        }

        .muted {
            color: var(--muted);
        }

        .columns {
            display: grid;
            grid-template-columns: minmax(0, 1fr) 260px;
            gap: 32px;
        }

        @media (max-width: 760px) {
            .columns {
                grid-template-columns: 1fr;
                gap: 8px;
            }
        }

        button,
        input,
        select {
            font: inherit;
        }

        .btn {
            border: 1px solid var(--line);
            background: #ffffff;
            color: var(--text);
            padding: 5px 10px;
            border-radius: 3px;
            cursor: pointer;
        }

        .btn:hover {
            background: var(--surface);
        }

        .btn.primary {
            background: var(--accent);
            border-color: var(--accent);
            color: #ffffff;
        }

        .btn[aria-pressed="true"] {
            background: var(--surface);
            font-weight: 600;
        }

        button:focus-visible,
        input:focus-visible,
        select:focus-visible {
            outline: 2px solid var(--accent);
            outline-offset: 1px;
        }

        input[type="text"],
        input[type="number"],
        select {
            border: 1px solid var(--line);
            border-radius: 3px;
            padding: 5px 8px;
            background: #ffffff;
            color: var(--text);
        }

        input[type="text"] {
            width: 100%;
        }

        .toolbar {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            align-items: center;
            margin: 10px 0;
        }

        .post {
            border-top: 1px solid var(--line);
            padding: 12px 0;
        }

        .post-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 4px 10px;
            align-items: baseline;
            color: var(--muted);
        }

        .post-meta strong {
            color: var(--text);
        }

        .caption {
            margin: 4px 0 8px;
            font-size: 15px;
        }

        .actions {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }

        .detail,
        .comments {
            margin-top: 10px;
            padding: 8px 0 0;
        }

        .comment {
            padding: 4px 0;
            border-bottom: 1px solid var(--line);
        }

        .comment-form {
            display: flex;
            gap: 6px;
            margin-top: 8px;
        }

        .table-wrap {
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th,
        td {
            text-align: left;
            padding: 6px 10px 6px 0;
            border-bottom: 1px solid var(--line);
            vertical-align: top;
            white-space: nowrap;
        }

        td.wrap-text {
            white-space: normal;
        }

        th {
            font-weight: 600;
            color: var(--muted);
        }

        td.num,
        th.num {
            text-align: right;
            font-variant-numeric: tabular-nums;
            padding-right: 14px;
        }

        fieldset {
            border: 1px solid var(--line);
            border-radius: 3px;
            margin: 0 0 12px;
            padding: 8px 12px;
        }

        legend {
            padding: 0 4px;
            font-weight: 600;
        }

        label.check {
            display: block;
            padding: 2px 0;
        }

        .field {
            display: grid;
            grid-template-columns: 190px 90px;
            gap: 8px;
            align-items: center;
            margin: 6px 0;
        }

        .field-stack {
            margin: 8px 0;
        }

        .field-stack label {
            display: block;
            margin-bottom: 3px;
        }

        .tab-panel[hidden] {
            display: none;
        }
    </style>
</head>
<body>
    <header>
        <div class="wrap">
            <h1>Feed ranking console</h1>
            <nav role="tablist" id="tabs">
                <button role="tab" data-tab="feed" aria-selected="true">Feed</button>
                <button role="tab" data-tab="pipeline" aria-selected="false">Pipeline</button>
                <button role="tab" data-tab="features" aria-selected="false">Feature store</button>
                <button role="tab" data-tab="models" aria-selected="false">Models</button>
                <button role="tab" data-tab="metrics" aria-selected="false">Metrics</button>
                <button role="tab" data-tab="publish" aria-selected="false">Publish</button>
            </nav>
        </div>
    </header>

    <div class="wrap">
        <p id="status" role="status"></p>

        <main>
            <section class="tab-panel" id="tab-feed">
                <div class="columns">
                    <div>
                        <div class="toolbar">
                            <button class="btn primary" id="btnFeed">Request a new feed</button>
                            <button class="btn" id="btnSeen">Clear seen posts</button>
                        </div>
                        <p class="muted" id="feedMeta"></p>
                        <div id="feedList"></div>
                    </div>
                    <div>
                        <h2>Interests</h2>
                        <fieldset id="interests">
                            <legend>Starting topics</legend>
                        </fieldset>
                        <h2>Session</h2>
                        <p class="muted" id="sessionInfo"></p>
                        <button class="btn" id="btnNewSession">Start a new anonymous session</button>
                    </div>
                </div>
            </section>

            <section class="tab-panel" id="tab-pipeline" hidden>
                <h2>Last feed request</h2>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr><th>Stage</th><th>Component</th><th>Detail</th></tr>
                        </thead>
                        <tbody id="pipeStages"></tbody>
                    </table>
                </div>
                <h2>Returned posts</h2>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th class="num">#</th>
                                <th>Post</th>
                                <th>Candidate sources</th>
                                <th class="num">P(like)</th>
                                <th class="num">P(comment)</th>
                                <th class="num">Score</th>
                                <th>Re-ordered</th>
                            </tr>
                        </thead>
                        <tbody id="pipeTable"></tbody>
                    </table>
                </div>
            </section>

            <section class="tab-panel" id="tab-features" hidden>
                <h2>Content features</h2>
                <p class="muted" id="fsMeta"></p>
                <div class="toolbar">
                    <button class="btn primary" id="btnPipeline">Run feature pipeline</button>
                </div>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Post</th>
                                <th>Creator</th>
                                <th class="num">Popularity</th>
                                <th class="num">Like rate</th>
                                <th class="num">Comment rate</th>
                                <th class="num">Recency</th>
                                <th class="num">Age (h)</th>
                                <th class="num">Impressions in store</th>
                                <th class="num">Impressions now</th>
                            </tr>
                        </thead>
                        <tbody id="fsTable"></tbody>
                    </table>
                </div>
                <h2>This session's user features</h2>
                <div id="userFeatures"></div>
            </section>

            <section class="tab-panel" id="tab-models" hidden>
                <div class="columns">
                    <div>
                        <h2>Model store</h2>
                        <div class="toolbar">
                            <button class="btn primary" id="btnTrain">Train from logged impressions</button>
                        </div>
                        <div class="table-wrap">
                            <table>
                                <thead>
                                    <tr>
                                        <th>Version</th>
                                        <th>Origin</th>
                                        <th class="num">Like AUC</th>
                                        <th class="num">Comment AUC</th>
                                        <th></th>
                                    </tr>
                                </thead>
                                <tbody id="modelTable"></tbody>
                            </table>
                        </div>
                        <p class="muted">AUC is measured on held-out rows and shown as previous model, then new model.</p>
                    </div>
                    <div>
                        <h2>Scoring weights</h2>
                        <div class="field">
                            <label for="w_like">Like weight</label>
                            <input type="number" id="w_like" min="0" max="10" step="0.1">
                        </div>
                        <div class="field">
                            <label for="w_comment">Comment weight</label>
                            <input type="number" id="w_comment" min="0" max="10" step="0.1">
                        </div>
                        <div class="field">
                            <label for="w_explore">Low-exposure bonus</label>
                            <input type="number" id="w_explore" min="0" max="2" step="0.05">
                        </div>
                        <div class="field">
                            <label for="max_per_creator">Max posts per creator</label>
                            <input type="number" id="max_per_creator" min="1" max="10" step="1">
                        </div>
                        <button class="btn" id="btnConfig">Save weights</button>

                        <h2>Synthetic sessions</h2>
                        <p class="muted">Each session has a hidden taste for two categories and acts on the feed three times.</p>
                        <div class="field">
                            <label for="simCount">Number of sessions</label>
                            <input type="number" id="simCount" min="1" max="400" step="1" value="100">
                        </div>
                        <button class="btn" id="btnSim">Generate synthetic sessions</button>
                    </div>
                </div>
            </section>

            <section class="tab-panel" id="tab-metrics" hidden>
                <h2>Totals since the server started</h2>
                <div class="table-wrap">
                    <table>
                        <tbody id="statTable"></tbody>
                    </table>
                </div>
                <h2>By category</h2>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>Category</th>
                                <th class="num">Impressions</th>
                                <th class="num">Likes</th>
                                <th class="num">Comments</th>
                            </tr>
                        </thead>
                        <tbody id="catTable"></tbody>
                    </table>
                </div>
            </section>

            <section class="tab-panel" id="tab-publish" hidden>
                <h2>Publish a post</h2>
                <div style="max-width: 460px;">
                    <div class="field-stack">
                        <label for="pubCreator">Creator</label>
                        <select id="pubCreator"></select>
                    </div>
                    <div class="field-stack">
                        <label for="pubCaption">Caption</label>
                        <input type="text" id="pubCaption" maxlength="200">
                    </div>
                    <button class="btn primary" id="btnPublish">Publish post</button>
                </div>
            </section>
        </main>
    </div>

    <script>
        var state = { uid: null, meta: null, feed: [], pipeline: null, interests: [] };

        function esc(s) {
            var d = document.createElement("div");
            d.textContent = s;
            return d.innerHTML;
        }

        function $(id) {
            return document.getElementById(id);
        }

        function setStatus(msg, isError) {
            var el = $("status");
            el.textContent = msg;
            el.className = isError ? "error" : "";
        }

        async function api(path, body) {
            var opts = body === undefined ? {} : {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body)
            };
            var res;
            try {
                res = await fetch(path, opts);
            } catch (e) {
                setStatus("Could not reach the server. Check that the Colab cell is still running, then try again.", true);
                throw e;
            }
            var data = await res.json();
            if (!res.ok) {
                setStatus(data.error || "The server rejected the request.", true);
                throw new Error(data.error);
            }
            return data;
        }

        function initUid() {
            var uid = null;
            try {
                uid = localStorage.getItem("ranking_uid");
            } catch (e) {
                uid = null;
            }
            if (!uid) {
                uid = "u_" + Math.random().toString(36).slice(2, 10);
                try {
                    localStorage.setItem("ranking_uid", uid);
                } catch (e) {}
            }
            state.uid = uid;
        }

        function showTab(name) {
            document.querySelectorAll("#tabs button").forEach(function (b) {
                b.setAttribute("aria-selected", b.dataset.tab === name ? "true" : "false");
            });
            document.querySelectorAll(".tab-panel").forEach(function (p) {
                p.hidden = p.id !== "tab-" + name;
            });
            if (name === "features") loadFeatures();
            if (name === "models") loadModels();
            if (name === "metrics") loadStats();
            if (name === "pipeline") renderPipeline();
        }

        $("tabs").addEventListener("click", function (e) {
            var b = e.target.closest("button");
            if (b) showTab(b.dataset.tab);
        });

        async function loadProfile() {
            var p = await api("/api/profile?uid=" + state.uid);
            state.interests = p.interests;
            renderInterests();
            $("sessionInfo").textContent =
                p.seen + " posts seen, " + p.liked + " liked, " + p.follows.length + " creators followed.";
            return p;
        }

        function renderInterests() {
            var box = $("interests");
            box.innerHTML = "<legend>Starting topics</legend>";
            state.meta.categories.forEach(function (c) {
                var label = document.createElement("label");
                label.className = "check";
                var input = document.createElement("input");
                input.type = "checkbox";
                input.value = c;
                input.checked = state.interests.indexOf(c) >= 0;
                label.appendChild(input);
                label.appendChild(document.createTextNode(" " + c));
                box.appendChild(label);
            });
        }

        $("interests").addEventListener("change", async function () {
            var chosen = Array.prototype.map.call(
                document.querySelectorAll("#interests input:checked"),
                function (i) { return i.value; }
            );
            await api("/api/profile", { uid: state.uid, interests: chosen });
            state.interests = chosen;
            await loadFeed();
            setStatus("Interests saved: " + (chosen.length ? chosen.join(", ") : "none") + ". Feed rebuilt with the new topics.");
        });

        async function loadFeed() {
            var data = await api("/api/feed?uid=" + state.uid);
            state.feed = data.items;
            state.pipeline = data.pipeline;
            var p = data.pipeline;
            $("feedMeta").textContent =
                "Model " + p.model + ": " + p.final + " posts chosen from " + p.candidates + " candidates in " +
                p.latency_ms + " ms. Feature store data is " + p.feature_age_s + " s old." +
                (p.fallback ? " Ranking failed, so these are the most-liked posts instead." : "");
            renderFeed();
            loadProfile();
        }

        function renderFeed() {
            var list = $("feedList");
            if (!state.feed.length) {
                list.innerHTML = '<p class="muted">The feed is empty. Clear seen posts and request a new feed.</p>';
                return;
            }
            list.innerHTML = state.feed.map(function (it) {
                var rows = it.contrib.map(function (c) {
                    return "<tr><td>" + c.f + '</td><td class="num">' + c.x + '</td><td class="num">' + c.w +
                        '</td><td class="num">' + c.c + "</td></tr>";
                }).join("");
                return (
                    '<article class="post" data-id="' + it.id + '">' +
                        '<div class="post-meta">' +
                            "<strong>" + esc(it.creator) + "</strong>" +
                            "<span>" + esc(it.category) + "</span>" +
                            "<span>" + it.age_hours + " h old</span>" +
                            '<button class="btn" data-action="follow" data-creator="' + it.creator_id + '" aria-pressed="' + it.following + '">' +
                                (it.following ? "Following" : "Follow creator") + "</button>" +
                        "</div>" +
                        '<p class="caption">' + esc(it.caption) + "</p>" +
                        '<div class="actions">' +
                            '<button class="btn" data-action="like" aria-pressed="' + it.liked + '">' +
                                (it.liked ? "Liked" : "Like") + " (" + it.likes + ")</button>" +
                            '<button class="btn" data-action="comments">Comments (' + it.comments + ")</button>" +
                            '<button class="btn" data-action="detail">Ranking detail</button>' +
                        "</div>" +
                        '<div class="detail" hidden>' +
                            "<p>P(like) " + it.p_like + ", P(comment) " + it.p_comment + ", low-exposure term " + it.explore +
                            ", score " + it.score + ". Candidate sources: " + it.sources.join(", ") + "." +
                            (it.demoted ? " Moved down to avoid repeating a creator." : "") + "</p>" +
                            '<div class="table-wrap"><table><thead><tr><th>Feature</th><th class="num">Value</th>' +
                            '<th class="num">Weight</th><th class="num">Contribution</th></tr></thead><tbody>' +
                            rows + "</tbody></table></div>" +
                        "</div>" +
                        '<div class="comments" hidden></div>' +
                    "</article>"
                );
            }).join("");
        }

        $("feedList").addEventListener("click", async function (e) {
            var btn = e.target.closest("button");
            if (!btn) return;
            var post = btn.closest(".post");
            var pid = post.dataset.id;
            var item = state.feed.filter(function (x) { return x.id === pid; })[0];
            var action = btn.dataset.action;
            if (action === "like") {
                var r = await api("/api/events", { uid: state.uid, type: item.liked ? "unlike" : "like", post_id: pid });
                item.liked = r.liked;
                item.likes = r.likes;
                btn.setAttribute("aria-pressed", String(item.liked));
                btn.textContent = (item.liked ? "Liked" : "Like") + " (" + item.likes + ")";
            } else if (action === "follow") {
                var cid = btn.dataset.creator;
                var was = item.following;
                await api("/api/events", { uid: state.uid, type: was ? "unfollow" : "follow", creator_id: cid });
                state.feed.forEach(function (x) {
                    if (x.creator_id === cid) x.following = !was;
                });
                document.querySelectorAll('[data-action="follow"][data-creator="' + cid + '"]').forEach(function (b) {
                    b.setAttribute("aria-pressed", String(!was));
                    b.textContent = was ? "Follow creator" : "Following";
                });
                setStatus(was
                    ? "Unfollowed " + item.creator + "."
                    : "Following " + item.creator + ". Their posts are now a candidate source for your next feed.");
            } else if (action === "detail") {
                var d = post.querySelector(".detail");
                d.hidden = !d.hidden;
            } else if (action === "comments") {
                var box = post.querySelector(".comments");
                box.hidden = !box.hidden;
                if (!box.hidden) await loadComments(pid, box);
            } else if (action === "post-comment") {
                var input = post.querySelector(".comments input");
                var res = await api("/api/events", { uid: state.uid, type: "comment", post_id: pid, text: input.value });
                item.comments = res.comments;
                post.querySelector('[data-action="comments"]').textContent = "Comments (" + item.comments + ")";
                await loadComments(pid, post.querySelector(".comments"));
                setStatus("Comment posted on " + item.creator + "'s post.");
            }
        });

        async function loadComments(pid, box) {
            var r = await api("/api/posts/" + pid + "/comments");
            var list = r.comments.length
                ? r.comments.map(function (c) {
                    return '<div class="comment"><strong>user_' + esc(c.user) + "</strong> " + esc(c.text) + "</div>";
                }).join("")
                : '<p class="muted">No comments on this post yet.</p>';
            box.innerHTML = list +
                '<div class="comment-form"><input type="text" maxlength="200" aria-label="Comment text">' +
                '<button class="btn" data-action="post-comment">Post comment</button></div>';
        }

        $("btnFeed").onclick = async function () {
            await loadFeed();
            setStatus("Feed rebuilt with model " + state.pipeline.model + ".");
        };

        $("btnSeen").onclick = async function () {
            await api("/api/profile/reset", { uid: state.uid });
            await loadFeed();
            setStatus("Seen posts cleared. Posts you have already viewed can rank high again.");
        };

        $("btnNewSession").onclick = async function () {
            try {
                localStorage.removeItem("ranking_uid");
            } catch (e) {}
            initUid();
            await loadProfile();
            await loadFeed();
            setStatus("New anonymous session started. Interests, follows and likes start empty.");
        };

        function renderPipeline() {
            var p = state.pipeline;
            if (!p) {
                $("pipeStages").innerHTML =
                    '<tr><td colspan="3" class="wrap-text muted">No feed has been requested in this session. Open the Feed tab first.</td></tr>';
                $("pipeTable").innerHTML = "";
                return;
            }
            var src = Object.keys(p.by_source).map(function (k) { return k + " " + p.by_source[k]; }).join(", ");
            var stages = [
                ["1", "Application feed", "GET /api/feed for an anonymous id, " + p.latency_ms + " ms end to end"],
                ["2", "Candidate generation", p.candidates + " distinct posts (" + src + ")"],
                ["3", "Ranking", p.ranked + " posts scored by model " + p.model + ", reading the feature store (" + p.feature_age_s + " s old)"],
                ["4", "Post processing", p.final + " posts returned, " + p.demoted + " re-ordered for creator diversity"],
                ["5", "Fallback", p.fallback ? "Used: ranking failed and most-liked posts were returned" : "Not used"]
            ];
            $("pipeStages").innerHTML = stages.map(function (s) {
                return "<tr><td>" + s[0] + "</td><td>" + s[1] + '</td><td class="wrap-text">' + s[2] + "</td></tr>";
            }).join("");
            $("pipeTable").innerHTML = state.feed.map(function (it, i) {
                return '<tr><td class="num">' + (i + 1) + "</td><td>" + esc(it.creator) + ": " + esc(it.caption.slice(0, 40)) +
                    "</td><td>" + it.sources.join(", ") + '</td><td class="num">' + it.p_like +
                    '</td><td class="num">' + it.p_comment + '</td><td class="num">' + it.score +
                    "</td><td>" + (it.demoted ? "yes" : "no") + "</td></tr>";
            }).join("");
        }

        async function loadFeatures() {
            var f = await api("/api/features");
            $("fsMeta").textContent =
                "Pipeline has run " + f.runs + " times; the last run was " + f.last_run_age + " s ago (" + f.last_source +
                "). It runs automatically every 20 seconds, so ranking can read values slightly behind live counts. Showing 25 of " +
                f.total + " posts, most popular first.";
            $("fsTable").innerHTML = f.content.map(function (r) {
                return "<tr><td>" + r.id + "</td><td>" + esc(r.creator) + '</td><td class="num">' + r.popularity +
                    '</td><td class="num">' + r.like_rate + '</td><td class="num">' + r.comment_rate +
                    '</td><td class="num">' + r.recency + '</td><td class="num">' + r.age_hours +
                    '</td><td class="num">' + r.snapshot_impressions + '</td><td class="num">' + r.live_impressions + "</td></tr>";
            }).join("");
            var p = await loadProfile();
            var aff = Object.keys(p.cat_aff).map(function (k) { return k + " " + p.cat_aff[k]; });
            $("userFeatures").innerHTML =
                "<p>Category affinity: " + (aff.length ? aff.join(", ") : "none yet, since this session has no likes, comments or interests") + ".</p>" +
                "<p>Followed creators: " + (p.follows.length ? p.follows.join(", ") : "none") + ".</p>";
        }

        $("btnPipeline").onclick = async function () {
            await api("/api/pipeline/run", {});
            await loadFeatures();
            setStatus("Feature pipeline finished. Stored features now match current post counts.");
        };

        function fmt(v) {
            return v === null || v === undefined ? "n/a" : v;
        }

        async function loadModels() {
            var r = await api("/api/models");
            $("modelTable").innerHTML = r.models.map(function (m) {
                var mt = m.metrics || {};
                var hasMetrics = mt.like_auc_new !== undefined;
                return "<tr><td><strong>" + m.version + "</strong>" + (m.active ? " (serving)" : "") +
                    '</td><td class="wrap-text">' + esc(m.note) + "</td>" +
                    '<td class="num">' + (hasMetrics ? fmt(mt.like_auc_old) + " to " + fmt(mt.like_auc_new) : "n/a") + "</td>" +
                    '<td class="num">' + (hasMetrics ? fmt(mt.comment_auc_old) + " to " + fmt(mt.comment_auc_new) : "n/a") + "</td>" +
                    "<td>" + (m.active ? "" : '<button class="btn" data-version="' + m.version + '">Serve ' + m.version + "</button>") + "</td></tr>";
            }).join("");
            var c = await api("/api/config");
            ["w_like", "w_comment", "w_explore", "max_per_creator"].forEach(function (k) {
                $(k).value = c[k];
            });
        }

        $("modelTable").addEventListener("click", async function (e) {
            var b = e.target.closest("button[data-version]");
            if (!b) return;
            await api("/api/models/activate", { version: b.dataset.version });
            await loadModels();
            setStatus("Now serving " + b.dataset.version + ". Request a new feed to see its ordering.");
        });

        $("btnTrain").onclick = async function () {
            setStatus("Training on logged impressions. This takes a few seconds.");
            var r = await api("/api/models/train", {});
            var m = r.metrics;
            await loadModels();
            setStatus(
                "Registered " + r.version + " from " + m.rows + " impressions. Like AUC " + fmt(m.like_auc_old) + " to " +
                fmt(m.like_auc_new) + ", comment AUC " + fmt(m.comment_auc_old) + " to " + fmt(m.comment_auc_new) +
                " on held-out rows. It is not serving yet; use the Serve button to switch."
            );
        };

        $("btnConfig").onclick = async function () {
            await api("/api/config", {
                w_like: $("w_like").value,
                w_comment: $("w_comment").value,
                w_explore: $("w_explore").value,
                max_per_creator: $("max_per_creator").value
            });
            setStatus("Weights saved. They apply to the next feed request.");
        };

        $("btnSim").onclick = async function () {
            var n = parseInt($("simCount").value, 10) || 1;
            setStatus("Generating " + n + " synthetic sessions.");
            var r = await api("/api/simulate", { users: n });
            setStatus(r.users + " synthetic sessions added. " + r.logged + " impressions are now logged for training.");
        };

        async function loadStats() {
            var s = await api("/api/stats");
            var rows = [
                ["Sessions", s.users + " (" + s.synthetic + " synthetic)"],
                ["Posts", s.posts],
                ["Feeds served", s.feeds],
                ["Impressions", s.impressions],
                ["Likes", s.likes],
                ["Comments", s.comments],
                ["Likes per impression", (s.like_rate * 100).toFixed(1) + "%"],
                ["Average feed latency", s.avg_latency_ms + " ms"],
                ["Fallback responses", s.fallbacks],
                ["Impressions logged for training", s.logged_impressions],
                ["Serving model", s.active_model]
            ];
            $("statTable").innerHTML = rows.map(function (r) {
                return "<tr><th>" + r[0] + '</th><td class="num">' + r[1] + "</td></tr>";
            }).join("");
            $("catTable").innerHTML = Object.keys(s.per_category).map(function (k) {
                var v = s.per_category[k];
                return "<tr><td>" + k + '</td><td class="num">' + v.impressions + '</td><td class="num">' + v.likes +
                    '</td><td class="num">' + v.comments + "</td></tr>";
            }).join("");
        }

        $("btnPublish").onclick = async function () {
            var cid = $("pubCreator").value;
            var cap = $("pubCaption").value;
            await api("/api/posts", { creator_id: cid, caption: cap });
            $("pubCaption").value = "";
            var name = state.meta.creators.filter(function (c) { return c.id === cid; })[0].name;
            setStatus("Post published for " + name + ". It has no impressions yet, so the low-exposure source can pick it up.");
        };

        (async function boot() {
            initUid();
            state.meta = await api("/api/meta");
            $("pubCreator").innerHTML = state.meta.creators.map(function (c) {
                return '<option value="' + c.id + '">' + esc(c.name) + " (" + c.category + ")</option>";
            }).join("");
            await loadProfile();
            await loadFeed();
        })();
    </script>
</body>
</html>
"""


if "PIPELINE_STOP" in globals():
    PIPELINE_STOP.set()
PIPELINE_STOP = threading.Event()


def pipeline_loop(stop_event):
    while not stop_event.wait(20):
        run_feature_pipeline("scheduled")


threading.Thread(target=pipeline_loop, args=(PIPELINE_STOP,), daemon=True).start()

if "SERVER" in globals():
    try:
        SERVER.shutdown()
    except Exception:
        pass

SERVER = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=SERVER.serve_forever, daemon=True).start()
time.sleep(1.0)

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height=950)
except ImportError:
    print("Not running in Colab. Open http://localhost:%d in your browser." % PORT)