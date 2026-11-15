'''
Predict Netflix Watch Time 

Predict Netflix Watch Time

1. Requirement Analysis

Functional Requirements

* Predict next watch time for a movie / TV show.
* Customizable system for recommendations.
* Acknowledge behavioural change.

Non-Functional Requirements

* Scale of users: 10M DAU.
* Scale of data: 10 million data.
* CAP Theorem: Availability >>> Consistency.

2. Identifying Entities

* User
* Platform
* Model

3. Pipeline Design

- Unsupervised Learning

4. High-Level Design

4a. Data Set: Watch Time DB

* Content – Impression
* Play: Start, Pause, Resume, Stop
* Content: Skip, Played, Left, Ignored

4b. Feature Engineering:

* User
* Content

4c. Target:

\hat{T} = f(U,C,X)\hat{T} = \sum (\text{watched})

Where:

* = User
* = Content
* = Features

5. Low-Level Design

Pipeline diagram:

Data Ingest

Pre Processing

Feature Store

Online

Offline

Model Train

Real Time Interface



5. low level design 
![Predict Netflix Watch Time 
 Low-Level Design](Predict Netflix Watch Time .jpg)

https://github.com/soorabcode/System-Design-Problems/tree/main/Predict%20Netflix%20Watch%20Time%20
'''
import math
import random
import threading
import time
from collections import defaultdict
from itertools import product

import numpy as np
from flask import Flask, Response, jsonify, request
from werkzeug.serving import make_server

PORT = 5000
LOCK = threading.RLock()

GENRES = ["Action", "Comedy", "Drama", "Sci-Fi", "Thriller",
          "Documentary", "Romance", "Animation", "Horror", "Crime"]
DEVICES = ["TV", "Mobile", "Laptop", "Tablet"]
DEVICE_BONUS = {"TV": 0.08, "Laptop": 0.02, "Tablet": -0.02, "Mobile": -0.08}
ALPHA = 0.3

FEATURES = [
    ("u_avg",    "User",       "User avg completion",            "offline"),
    ("u_skip",   "User",       "User skip rate",                 "offline"),
    ("u_genre",  "User",       "User affinity for this genre",   "offline"),
    ("u_dev",    "User",       "User completion on this device", "offline"),
    ("c_avg",    "Content",    "Content avg completion",         "offline"),
    ("c_skip",   "Content",    "Content skip rate",              "offline"),
    ("c_pop",    "Content",    "Content popularity",             "offline"),
    ("c_dur",    "Content",    "Duration (normalised)",          "offline"),
    ("c_movie",  "Content",    "Is movie",                       "offline"),
    ("h_sin",    "Context",    "Hour of day (sin)",              "request"),
    ("h_cos",    "Context",    "Hour of day (cos)",              "request"),
    ("weekend",  "Context",    "Weekend",                        "request"),
    ("d_TV",     "Context",    "Device: TV",                     "request"),
    ("d_Mobile", "Context",    "Device: Mobile",                 "request"),
    ("d_Laptop", "Context",    "Device: Laptop",                 "request"),
    ("d_Tablet", "Context",    "Device: Tablet",                 "request"),
    ("o_ema",    "Behavioral", "Recent completion",              "online"),
    ("o_skip",   "Behavioral", "Recent skip rate",               "online"),
    ("o_trend",  "Behavioral", "Recent vs long-term completion", "online"),
    ("o_gema",   "Behavioral", "Recent genre completion",        "online"),
    ("o_gtrend", "Behavioral", "Recent vs long-term genre",      "online"),
]

USERS, CONTENT = {}, {}
RAW, CLEAN = [], []
OFFLINE = {}
ONLINE = {}
SEEN = defaultdict(set)
SEGMENTS = {}
SEGMENT_SUMMARY = []
MODEL = None
PIPE = {}
COUNTERS = {"predict_calls": 0, "dropped": 0}


def clip(value, low, high):
    return max(low, min(high, value))


def outcome_of(ratio):
    if ratio < 0.06:
        return "Skip"
    if ratio >= 0.9:
        return "Played"
    return "Left"


def likely_outcome(ratio):
    if ratio < 0.10:
        return "Likely to be skipped"
    if ratio >= 0.85:
        return "Likely to be watched to the end"
    return "Likely to be left part-way"


def mark(stage, rows, started, note=""):
    PIPE[stage] = {
        "status": "ok",
        "rows": rows,
        "note": note,
        "ms": int((time.time() - started) * 1000),
        "at": time.strftime("%H:%M:%S"),
    }


def simulate_event(rnd, user, item, ts):
    affinity = user["aff"][item["genre"]]
    if user["night"]:
        hour = rnd.choice([20, 21, 22, 23, 0, 1])
    else:
        hour = rnd.choice([17, 18, 19, 20, 21])
    device = user["device"] if rnd.random() < 0.7 else rnd.choice(DEVICES)
    weekend = int(rnd.random() < 0.3)
    base = dict(user=user["id"], content=item["id"], hour=hour,
                device=device, weekend=weekend, ts=ts)

    play_probability = 0.25 + 0.6 * affinity + 0.15 * item["quality"]
    if rnd.random() > play_probability:
        return dict(base, outcome="Ignored", watched=0.0, pauses=0)

    if rnd.random() < 0.12 * (1 - affinity):
        ratio = rnd.uniform(0.01, 0.05)
    else:
        ratio = 0.12 + 0.55 * affinity + 0.25 * item["quality"]
        ratio += 0.2 * (user["patience"] - 0.5)
        ratio += DEVICE_BONUS[device]
        if item["type"] == "Movie" and weekend:
            ratio += 0.05
        if hour >= 23 or hour <= 1:
            ratio += 0.05 if user["night"] else -0.07
        ratio += rnd.gauss(0, 0.1)
    ratio = clip(ratio, 0.01, 1.0)
    watched = round(ratio * item["duration"], 1)
    pauses = int(rnd.random() * ratio * item["duration"] / 25)
    return dict(base, outcome=outcome_of(ratio), watched=watched, pauses=pauses)


def generate_world(n_users=300, n_content=200, n_events=12000, seed=7):
    global USERS, CONTENT, RAW
    rnd = random.Random(seed)
    adjectives = ["Silent", "Crimson", "Hidden", "Last", "Broken", "Golden",
                  "Midnight", "Electric", "Lost", "Wild", "Frozen", "Neon",
                  "Secret", "Burning", "Distant", "Royal"]
    nouns = ["Empire", "Harbor", "Signal", "Garden", "Protocol", "Kingdom",
             "Horizon", "Witness", "Circuit", "Voyage", "Legacy", "Frontier",
             "Echo", "Mirage", "Archive", "Summit"]
    titles = [f"{a} {n}" for a, n in product(adjectives, nouns)]
    rnd.shuffle(titles)

    CONTENT = {}
    for i in range(n_content):
        is_movie = rnd.random() < 0.55
        content_id = f"C{i + 1:03d}"
        CONTENT[content_id] = dict(
            id=content_id,
            title=titles[i],
            genre=rnd.choice(GENRES),
            type="Movie" if is_movie else "Series",
            duration=rnd.randint(80, 170) if is_movie else rnd.choice([24, 30, 42, 50, 58]),
            year=rnd.randint(2005, 2026),
            quality=round(rnd.random(), 3),
            weight=rnd.paretovariate(2.0),
        )

    USERS = {}
    for i in range(n_users):
        favourites = rnd.sample(GENRES, 2)
        affinity = {
            g: rnd.uniform(0.75, 0.95) if g in favourites else rnd.uniform(0.15, 0.5)
            for g in GENRES
        }
        user_id = f"U{i + 1:03d}"
        USERS[user_id] = dict(
            id=user_id,
            aff=affinity,
            patience=rnd.uniform(0.2, 0.95),
            device=rnd.choice(DEVICES),
            night=rnd.random() < 0.4,
        )

    user_ids = list(USERS)
    content_ids = list(CONTENT)
    weights = [CONTENT[c]["weight"] for c in content_ids]
    RAW = []
    for ts in range(n_events):
        user = USERS[rnd.choice(user_ids)]
        item = CONTENT[rnd.choices(content_ids, weights)[0]]
        RAW.append(simulate_event(rnd, user, item, ts))


def ingest_event(event):
    user_id, content_id = event.get("user"), event.get("content")
    if user_id not in USERS or content_id not in CONTENT:
        raise ValueError("Unknown user or content id")
    item = CONTENT[content_id]
    watched = clip(float(event.get("watched", 0)), 0.0, float(item["duration"]))
    outcome = "Ignored" if watched <= 0 else outcome_of(watched / item["duration"])
    device = event.get("device", "TV")
    row = dict(
        user=user_id,
        content=content_id,
        hour=int(event.get("hour", 20)) % 24,
        device=device if device in DEVICES else "TV",
        weekend=int(event.get("weekend", 0)),
        outcome=outcome,
        watched=round(watched, 1),
        pauses=int(event.get("pauses", 0)),
        ts=len(RAW),
    )
    RAW.append(row)
    if watched > 0:
        ratio = watched / item["duration"]
        skip = 1.0 if outcome == "Skip" else 0.0
        genre = item["genre"]
        base_avg = smean(("u", user_id), OFFLINE["gmean"])
        base_skip = smean(("us", user_id), OFFLINE["gskip"])
        base_genre = smean(("ug", user_id, genre), base_avg)
        online_update(ONLINE, user_id, genre, ratio, skip, base_avg, base_skip, base_genre)
        SEEN[user_id].add(content_id)
    return row


def preprocess(raw):
    clean, dropped = [], 0
    for event in raw:
        item = CONTENT.get(event.get("content"))
        if item is None or event.get("user") not in USERS:
            dropped += 1
            continue
        if event.get("watched") is None or event["watched"] < 0:
            dropped += 1
            continue
        duration = item["duration"]
        watched = min(float(event["watched"]), duration)
        row = dict(event)
        row["watched"] = watched
        row["ratio"] = watched / duration if watched > 0 else 0.0
        row["hour"] = int(event["hour"]) % 24
        row["device"] = event["device"] if event["device"] in DEVICES else "TV"
        clean.append(row)
    return clean, dropped


def build_offline(clean):
    stats = defaultdict(lambda: [0.0, 0])
    content_plays = defaultdict(int)
    seen = defaultdict(set)
    ratios, skips = [], []
    for event in clean:
        if event["watched"] <= 0:
            continue
        user_id, content_id = event["user"], event["content"]
        genre = CONTENT[content_id]["genre"]
        ratio = event["ratio"]
        skip = 1.0 if event["outcome"] == "Skip" else 0.0
        updates = (
            (("u", user_id), ratio),
            (("ug", user_id, genre), ratio),
            (("ud", user_id, event["device"]), ratio),
            (("c", content_id), ratio),
            (("us", user_id), skip),
            (("cs", content_id), skip),
        )
        for key, value in updates:
            stats[key][0] += value
            stats[key][1] += 1
        content_plays[content_id] += 1
        seen[user_id].add(content_id)
        ratios.append(ratio)
        skips.append(skip)

    OFFLINE.clear()
    OFFLINE.update(
        stats=dict(stats),
        cplays=dict(content_plays),
        gmean=float(np.mean(ratios)),
        gskip=float(np.mean(skips)),
        maxplays=max(content_plays.values()),
        rows=len(ratios),
        raw_count=len(RAW),
    )
    SEEN.clear()
    SEEN.update(seen)
    build_segments()


def smean(key, prior, exclude=None, strength=3.0):
    total, count = OFFLINE["stats"].get(key, (0.0, 0))
    if exclude is not None:
        total -= exclude
        count -= 1
    return (total + prior * strength) / (count + strength)


def make_features(user_id, content_id, hour, device, weekend, online, exclude=None):
    item = CONTENT[content_id]
    genre = item["genre"]
    ratio_ex = exclude[0] if exclude else None
    skip_ex = exclude[1] if exclude else None
    offline = OFFLINE

    u_avg = smean(("u", user_id), offline["gmean"], ratio_ex)
    u_skip = smean(("us", user_id), offline["gskip"], skip_ex)
    u_genre = smean(("ug", user_id, genre), u_avg, ratio_ex)
    u_dev = smean(("ud", user_id, device), u_avg, ratio_ex)
    c_avg = smean(("c", content_id), offline["gmean"], ratio_ex)
    c_skip = smean(("cs", content_id), offline["gskip"], skip_ex)
    plays = offline["cplays"].get(content_id, 0) - (1 if exclude else 0)
    c_pop = math.log1p(max(plays, 0)) / math.log1p(offline["maxplays"])

    state = online.get(user_id)
    o_ema = state["ema"] if state else u_avg
    o_skip = state["skip"] if state else u_skip
    o_gema = state["g"].get(genre, u_genre) if state else u_genre

    features = {
        "u_avg": u_avg,
        "u_skip": u_skip,
        "u_genre": u_genre,
        "u_dev": u_dev,
        "c_avg": c_avg,
        "c_skip": c_skip,
        "c_pop": c_pop,
        "c_dur": item["duration"] / 180.0,
        "c_movie": 1.0 if item["type"] == "Movie" else 0.0,
        "h_sin": math.sin(2 * math.pi * hour / 24),
        "h_cos": math.cos(2 * math.pi * hour / 24),
        "weekend": float(weekend),
        "o_ema": o_ema,
        "o_skip": o_skip,
        "o_trend": o_ema - u_avg,
        "o_gema": o_gema,
        "o_gtrend": o_gema - u_genre,
    }
    for name in DEVICES:
        features["d_" + name] = 1.0 if device == name else 0.0
    return features


def online_update(online, user_id, genre, ratio, skip, base_avg, base_skip, base_genre):
    state = online.setdefault(user_id, {"ema": base_avg, "skip": base_skip, "n": 0, "g": {}})
    state["ema"] = (1 - ALPHA) * state["ema"] + ALPHA * ratio
    state["skip"] = (1 - ALPHA) * state["skip"] + ALPHA * skip
    state["g"][genre] = (1 - ALPHA) * state["g"].get(genre, base_genre) + ALPHA * ratio
    state["n"] += 1


def warm_online(clean, last_n=5):
    ONLINE.clear()
    recent = defaultdict(list)
    for event in sorted(clean, key=lambda r: r["ts"]):
        if event["watched"] > 0:
            recent[event["user"]].append(event)
    for user_id, events in recent.items():
        for event in events[-last_n:]:
            genre = CONTENT[event["content"]]["genre"]
            base_avg = smean(("u", user_id), OFFLINE["gmean"])
            base_skip = smean(("us", user_id), OFFLINE["gskip"])
            base_genre = smean(("ug", user_id, genre), base_avg)
            skip = 1.0 if event["outcome"] == "Skip" else 0.0
            online_update(ONLINE, user_id, genre, event["ratio"], skip,
                          base_avg, base_skip, base_genre)


def kmeans(data, k=4, iterations=25, seed=3):
    rng = np.random.RandomState(seed)
    centres = data[rng.choice(len(data), k, replace=False)].copy()
    labels = np.zeros(len(data), dtype=int)
    for _ in range(iterations):
        distances = ((data[:, None, :] - centres[None, :, :]) ** 2).sum(2)
        labels = distances.argmin(1)
        for j in range(k):
            if (labels == j).any():
                centres[j] = data[labels == j].mean(0)
    return labels, centres


def build_segments():
    global SEGMENT_SUMMARY
    ids = list(USERS)
    rows = []
    for user_id in ids:
        u_avg = smean(("u", user_id), OFFLINE["gmean"])
        u_skip = smean(("us", user_id), OFFLINE["gskip"])
        affinity = [smean(("ug", user_id, g), u_avg) for g in GENRES]
        rows.append([u_avg, u_skip * 2] + affinity)
    labels, centres = kmeans(np.array(rows))
    order = np.argsort(centres[:, 0])
    names = ["Casual browsers", "Selective viewers", "Engaged viewers", "Binge-watchers"]
    name_of = {int(cluster): names[i] for i, cluster in enumerate(order)}

    SEGMENTS.clear()
    SEGMENT_SUMMARY = []
    for cluster in order:
        members = [ids[i] for i in range(len(ids)) if labels[i] == cluster]
        SEGMENT_SUMMARY.append({
            "name": name_of[int(cluster)],
            "count": len(members),
            "avg_completion": round(float(centres[cluster][0]) * 100, 1),
        })
        for user_id in members:
            SEGMENTS[user_id] = name_of[int(cluster)]


def build_training(clean):
    online = {}
    X, y, durations = [], [], []
    for event in sorted(clean, key=lambda r: r["ts"]):
        if event["watched"] <= 0:
            continue
        user_id, content_id = event["user"], event["content"]
        genre = CONTENT[content_id]["genre"]
        ratio = event["ratio"]
        skip = 1.0 if event["outcome"] == "Skip" else 0.0
        features = make_features(user_id, content_id, event["hour"], event["device"],
                                 event["weekend"], online, exclude=(ratio, skip))
        X.append([features[name] for name, _, _, _ in FEATURES])
        y.append(ratio)
        durations.append(CONTENT[content_id]["duration"])
        online_update(online, user_id, genre, ratio, skip,
                      features["u_avg"], features["u_skip"], features["u_genre"])
    return np.array(X), np.array(y), np.array(durations)


def train_model():
    global MODEL
    started = time.time()
    X, y, durations = build_training(CLEAN)
    n = len(y)
    order = np.random.RandomState(42).permutation(n)
    a, b = int(0.7 * n), int(0.85 * n)
    train, valid, test = order[:a], order[a:b], order[b:]

    mean = X[train].mean(0)
    std = X[train].std(0) + 1e-9
    Z = (X - mean) / std
    y_mean = float(y[train].mean())

    best, grid = None, []
    for lam in [0.1, 1, 10, 100, 1000]:
        A = Z[train]
        w = np.linalg.solve(A.T @ A + lam * np.eye(A.shape[1]), A.T @ (y[train] - y_mean))
        rmse = float(np.sqrt(np.mean((Z[valid] @ w + y_mean - y[valid]) ** 2)))
        grid.append({"lambda": lam, "val_rmse": round(rmse, 4)})
        if best is None or rmse < best[0]:
            best = (rmse, lam, w)
    _, lam, w = best

    valid_residual = Z[valid] @ w + y_mean - y[valid]
    pred = np.clip(Z[test] @ w + y_mean, 0.01, 1.0)
    mae_minutes = float(np.mean(np.abs(pred - y[test]) * durations[test]))
    baseline_minutes = float(np.mean(np.abs(X[test, 0] - y[test]) * durations[test]))
    ss_res = float(np.sum((pred - y[test]) ** 2))
    ss_tot = float(np.sum((y[test] - y[test].mean()) ** 2))

    MODEL = dict(
        mu=mean, sd=std, w=w, ym=y_mean, lam=lam, grid=grid,
        sigma=float(valid_residual.std()),
        n_train=len(train), n_test=len(test),
        mae_min=round(mae_minutes, 2),
        base_mae_min=round(baseline_minutes, 2),
        rmse=round(float(np.sqrt(ss_res / len(test))), 4),
        r2=round(1 - ss_res / ss_tot, 3),
        trained_at=time.strftime("%H:%M:%S"),
    )
    mark("model_train", n, started, f"ridge regression, lambda {lam}")


def predict_one(user_id, content_id, hour, device, weekend, detail=True):
    model = MODEL
    features = make_features(user_id, content_id, hour, device, weekend, ONLINE)
    x = np.array([features[name] for name, _, _, _ in FEATURES])
    contribution = ((x - model["mu"]) / model["sd"]) * model["w"]
    ratio = float(clip(contribution.sum() + model["ym"], 0.02, 1.0))
    item = CONTENT[content_id]
    minutes = ratio * item["duration"]
    result = {"ratio": ratio, "minutes": minutes, "duration": item["duration"]}
    if not detail:
        return result

    half_width = 1.28 * model["sigma"] * item["duration"]
    groups = defaultdict(float)
    listing = []
    for (name, group, label, store), value in zip(FEATURES, contribution):
        groups[group] += float(value) * 100
        listing.append({"name": name, "group": group, "label": label,
                        "store": store, "value": round(features[name], 3)})
    drift = features["o_ema"] - features["u_avg"]
    result.update(
        title=item["title"],
        genre=item["genre"],
        type=item["type"],
        outcome=likely_outcome(ratio),
        low=max(0.0, minutes - half_width),
        high=min(item["duration"], minutes + half_width),
        baseline=model["ym"] * 100,
        groups={k: round(v, 1) for k, v in groups.items()},
        features=listing,
        segment=SEGMENTS.get(user_id, "n/a"),
        drift=round(drift * 100, 1),
        drift_flag=abs(drift) > 0.12,
        seen=content_id in SEEN[user_id],
    )
    return result


def rebuild_offline():
    global CLEAN
    started = time.time()
    CLEAN, dropped = preprocess(RAW)
    COUNTERS["dropped"] = dropped
    mark("preprocess", len(CLEAN), started, f"{dropped} invalid rows dropped")
    started = time.time()
    build_offline(CLEAN)
    mark("feature_store", OFFLINE["rows"], started, "offline batch rebuilt")


def run_pipeline(regenerate=True):
    if regenerate:
        started = time.time()
        generate_world()
        mark("ingest", len(RAW), started, "synthetic view logs generated")
    rebuild_offline()
    if regenerate:
        warm_online(CLEAN)
    train_model()
    PIPE["predict"] = {
        "status": "ready",
        "rows": COUNTERS["predict_calls"],
        "note": "serving predictions",
        "ms": 0,
        "at": time.strftime("%H:%M:%S"),
    }


app = Flask(__name__)


@app.errorhandler(Exception)
def on_error(error):
    code = getattr(error, "code", None)
    if not isinstance(code, int):
        code = 400 if isinstance(error, (ValueError, KeyError)) else 500
    return jsonify(error=str(error)), code


@app.get("/")
def home():
    return Response(PAGE, mimetype="text/html")


@app.get("/api/meta")
def api_meta():
    users = []
    for user in USERS.values():
        suffix = ", night viewer" if user["night"] else ""
        users.append({"id": user["id"], "label": f"{user['id']} ({user['device']}{suffix})"})
    content = [{k: c[k] for k in ("id", "title", "genre", "type", "duration", "year")}
               for c in CONTENT.values()]
    return jsonify(users=users, content=content, genres=GENRES, devices=DEVICES)


@app.get("/api/state")
def api_state():
    with LOCK:
        plays = [e for e in CLEAN if e["watched"] > 0]
        recent = plays[-300:]
        shifted = 0
        for user_id, state in ONLINE.items():
            if abs(state["ema"] - smean(("u", user_id), OFFLINE["gmean"])) > 0.12:
                shifted += 1
        PIPE["predict"]["rows"] = COUNTERS["predict_calls"]
        model = MODEL
        return jsonify(
            users=len(USERS),
            content=len(CONTENT),
            raw_events=len(RAW),
            plays=len(plays),
            pending=len(RAW) - OFFLINE["raw_count"],
            pipeline=PIPE,
            segments=SEGMENT_SUMMARY,
            drift={
                "recent": round(float(np.mean([e["ratio"] for e in recent])) * 100, 1),
                "overall": round(OFFLINE["gmean"] * 100, 1),
                "users_shifted": shifted,
            },
            model={k: model[k] for k in ("mae_min", "base_mae_min", "rmse", "r2", "lam",
                                         "grid", "n_train", "n_test", "trained_at")},
        )


@app.post("/api/predict")
def api_predict():
    body = request.get_json(force=True)
    with LOCK:
        COUNTERS["predict_calls"] += 1
        return jsonify(predict_one(body["user"], body["content"], int(body.get("hour", 21)),
                                   body.get("device", "TV"), int(body.get("weekend", 0))))


@app.post("/api/recommend")
def api_recommend():
    body = request.get_json(force=True)
    user_id = body["user"]
    w_watch = float(body.get("w_watch", 1.0))
    w_pop = float(body.get("w_pop", 0.3))
    w_fresh = float(body.get("w_fresh", 0.1))
    rows = []
    with LOCK:
        COUNTERS["predict_calls"] += 1
        for content_id, item in CONTENT.items():
            if body.get("genre") and item["genre"] != body["genre"]:
                continue
            if body.get("hide_seen") and content_id in SEEN[user_id]:
                continue
            prediction = predict_one(user_id, content_id, int(body.get("hour", 21)),
                                     body.get("device", "TV"), int(body.get("weekend", 0)),
                                     detail=False)
            popularity = (math.log1p(OFFLINE["cplays"].get(content_id, 0))
                          / math.log1p(OFFLINE["maxplays"]))
            rows.append(dict(
                id=content_id, title=item["title"], genre=item["genre"],
                type=item["type"], duration=item["duration"], year=item["year"],
                minutes=prediction["minutes"], ratio=prediction["ratio"],
                pop=popularity, fresh=(item["year"] - 2005) / 21.0,
            ))
    top_minutes = max([r["minutes"] for r in rows] or [1.0])
    for row in rows:
        row["score"] = (w_watch * row["minutes"] / top_minutes
                        + w_pop * row["pop"] + w_fresh * row["fresh"])
    rows.sort(key=lambda r: r["score"], reverse=True)
    return jsonify(items=rows[:int(body.get("limit", 12))], total=len(rows))


@app.post("/api/ingest/event")
def api_ingest():
    with LOCK:
        row = ingest_event(request.get_json(force=True))
        PIPE["ingest"] = {
            "status": "ok",
            "rows": len(RAW),
            "ms": 0,
            "note": "live events streaming in",
            "at": time.strftime("%H:%M:%S"),
        }
        return jsonify(event=row, pending=len(RAW) - OFFLINE["raw_count"])


@app.post("/api/simulate/shift")
def api_shift():
    body = request.get_json(force=True)
    user_id, genre, mode = body["user"], body["genre"], body.get("mode", "binge")
    rnd = random.Random()
    with LOCK:
        pool = [c for c in CONTENT.values() if c["genre"] == genre]
        sample = pool[0]
        for item in pool:
            if item["id"] not in SEEN[user_id]:
                sample = item
                break
        args = (user_id, sample["id"], 21, "TV", 0)
        before = predict_one(*args, detail=False)
        for _ in range(8):
            item = rnd.choice(pool)
            ratio = rnd.uniform(0.92, 1.0) if mode == "binge" else rnd.uniform(0.01, 0.08)
            ingest_event(dict(user=user_id, content=item["id"], hour=21, device="TV",
                              watched=ratio * item["duration"]))
        after = predict_one(*args, detail=False)
        return jsonify(content=sample, before=before, after=after,
                       pending=len(RAW) - OFFLINE["raw_count"])


@app.post("/api/offline/rebuild")
def api_rebuild():
    with LOCK:
        rebuild_offline()
        return jsonify(ok=True)


@app.post("/api/model/train")
def api_train():
    with LOCK:
        rebuild_offline()
        train_model()
        return jsonify(ok=True)


@app.post("/api/pipeline/run")
def api_pipeline():
    with LOCK:
        run_pipeline(regenerate=True)
        return jsonify(ok=True)


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Watch Time Prediction</title>
    <style>
        :root {
            --bg: #f6f7f9;
            --panel: #ffffff;
            --line: #d9dde3;
            --text: #1d2430;
            --muted: #667085;
            --accent: #2f5fd0;
            --warn: #9a6700;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            background: var(--bg);
            color: var(--text);
            font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
            font-size: 14px;
            line-height: 1.45;
        }

        header {
            padding: 16px 24px;
            background: var(--panel);
            border-bottom: 1px solid var(--line);
        }

        header h1 {
            margin: 0;
            font-size: 18px;
            font-weight: 600;
        }

        header p {
            margin: 2px 0 0;
            color: var(--muted);
        }

        nav {
            display: flex;
            gap: 2px;
            padding: 0 24px;
            background: var(--panel);
            border-bottom: 1px solid var(--line);
            flex-wrap: wrap;
        }

        nav button {
            background: none;
            border: none;
            border-bottom: 2px solid transparent;
            padding: 10px 14px;
            color: var(--muted);
            font: inherit;
            cursor: pointer;
        }

        nav button.active {
            color: var(--text);
            border-bottom-color: var(--accent);
        }

        main {
            max-width: 1100px;
            margin: 0 auto;
            padding: 20px 24px 40px;
        }

        .tab {
            display: none;
        }

        .tab.active {
            display: block;
        }

        .grid {
            display: grid;
            gap: 12px;
        }

        .cols-2 {
            grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
        }

        .cols-4 {
            grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        }

        .card {
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 6px;
            padding: 16px;
        }

        .card h2 {
            margin: 0 0 10px;
            font-size: 14px;
            font-weight: 600;
        }

        .stat .value {
            font-size: 22px;
            font-weight: 600;
        }

        .stat .label {
            color: var(--muted);
            font-size: 12px;
        }

        .flow {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }

        .stage {
            flex: 1 1 150px;
            border: 1px solid var(--line);
            border-radius: 6px;
            padding: 10px 12px;
        }

        .stage b {
            display: block;
        }

        .stage small {
            display: block;
            color: var(--muted);
        }

        .tag {
            display: inline-block;
            padding: 1px 7px;
            border: 1px solid var(--line);
            border-radius: 4px;
            font-size: 11px;
            color: var(--muted);
        }

        label {
            display: block;
            margin: 10px 0 4px;
            color: var(--muted);
            font-size: 12px;
        }

        select,
        input[type="text"],
        input[type="number"] {
            width: 100%;
            padding: 7px 8px;
            border: 1px solid var(--line);
            border-radius: 4px;
            background: #fff;
            color: var(--text);
            font: inherit;
        }

        input[type="range"] {
            width: 100%;
        }

        button.primary {
            margin-top: 14px;
            padding: 8px 16px;
            border: 1px solid var(--accent);
            border-radius: 4px;
            background: var(--accent);
            color: #fff;
            font: inherit;
            cursor: pointer;
        }

        button.secondary {
            margin: 4px 6px 0 0;
            padding: 7px 12px;
            border: 1px solid var(--line);
            border-radius: 4px;
            background: #fff;
            color: var(--text);
            font: inherit;
            cursor: pointer;
        }

        .result-value {
            font-size: 32px;
            font-weight: 600;
            line-height: 1.1;
        }

        .muted {
            color: var(--muted);
        }

        .meter {
            height: 8px;
            margin: 8px 0;
            border-radius: 4px;
            background: #e8ebf0;
            overflow: hidden;
        }

        .meter > div {
            height: 100%;
            background: var(--accent);
        }

        .contrib {
            display: flex;
            align-items: center;
            gap: 10px;
            margin: 6px 0;
        }

        .contrib .name {
            width: 90px;
            color: var(--muted);
        }

        .contrib .track {
            position: relative;
            flex: 1;
            height: 8px;
            border-radius: 4px;
            background: #e8ebf0;
        }

        .contrib .fill {
            position: absolute;
            top: 0;
            height: 100%;
            border-radius: 4px;
        }

        .contrib .amount {
            width: 70px;
            text-align: right;
        }

        .notice {
            margin-bottom: 12px;
            padding: 8px 10px;
            border: 1px solid var(--warn);
            border-radius: 4px;
            color: var(--warn);
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        td,
        th {
            padding: 6px 4px;
            border-bottom: 1px solid var(--line);
            text-align: left;
        }

        th {
            color: var(--muted);
            font-weight: 500;
        }

        .results {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(230px, 1fr));
            gap: 10px;
            margin-top: 14px;
        }

        .results .item {
            border: 1px solid var(--line);
            border-radius: 6px;
            padding: 12px;
        }

        .results .item h3 {
            margin: 0 0 2px;
            font-size: 14px;
        }

        .comparison {
            display: flex;
            gap: 24px;
            align-items: center;
            flex-wrap: wrap;
            margin-top: 12px;
        }

        ul.plain {
            margin: 6px 0 0;
            padding-left: 18px;
        }

        code {
            font-family: ui-monospace, Menlo, Consolas, monospace;
            font-size: 12px;
        }

        #status {
            min-height: 20px;
            padding: 6px 24px;
            color: var(--muted);
            font-size: 12px;
        }
    </style>
</head>
<body>
    <header>
        <h1>Watch Time Prediction</h1>
        <p>Predicted watch time for a user and a title, from user, content and context features</p>
    </header>

    <nav>
        <button class="active" data-tab="dashboard">Dashboard</button>
        <button data-tab="predict">Predict</button>
        <button data-tab="recommend">Recommend</button>
        <button data-tab="behaviour">Behaviour change</button>
        <button data-tab="pipeline">Pipeline</button>
    </nav>

    <div id="status"></div>

    <main>
        <section id="dashboard" class="tab active">
            <div class="grid cols-4" id="stats"></div>

            <div class="card" style="margin-top:12px">
                <h2>Pipeline</h2>
                <div class="flow" id="flow"></div>
            </div>

            <div class="grid cols-2" style="margin-top:12px">
                <div class="card">
                    <h2>User segments (k-means)</h2>
                    <table id="segments"></table>
                </div>
                <div class="card">
                    <h2>Behaviour drift</h2>
                    <div id="drift"></div>
                </div>
            </div>

            <div class="grid cols-2" style="margin-top:12px">
                <div class="card">
                    <h2>Functional requirements</h2>
                    <ul class="plain">
                        <li>Predict the next watch time for a movie or TV show</li>
                        <li>Customisable recommendations</li>
                        <li>Account for changes in viewing behaviour</li>
                    </ul>
                </div>
                <div class="card">
                    <h2>Non-functional requirements</h2>
                    <ul class="plain">
                        <li>Target scale: 10M daily active users, 10 million data points</li>
                        <li>Availability is preferred over consistency. Predictions do not wait
                            for the batch store; recent events are served from the online store
                            until the next batch rebuild.</li>
                    </ul>
                </div>
            </div>
        </section>

        <section id="predict" class="tab">
            <div class="grid cols-2">
                <div class="card">
                    <h2>Prediction request</h2>
                    <label for="p-user">User</label>
                    <select id="p-user"></select>
                    <label for="p-filter">Filter titles</label>
                    <input type="text" id="p-filter" placeholder="Title or genre">
                    <label for="p-content">Title</label>
                    <select id="p-content"></select>
                    <label for="p-device">Device</label>
                    <select id="p-device"></select>
                    <label for="p-hour">Hour of day: <span id="p-hour-v">21</span>:00</label>
                    <input type="range" id="p-hour" min="0" max="23" value="21">
                    <label><input type="checkbox" id="p-weekend"> Weekend</label>
                    <button class="primary" id="p-go">Predict</button>
                </div>
                <div class="card" id="p-out">
                    <h2>Result</h2>
                    <p class="muted">Select a user and a title, then run the prediction.</p>
                </div>
            </div>
            <div class="grid cols-4" id="p-features" style="margin-top:12px"></div>
        </section>

        <section id="recommend" class="tab">
            <div class="card">
                <h2>Recommendations ranked by predicted watch time</h2>
                <div class="grid cols-4">
                    <div>
                        <label for="r-user">User</label>
                        <select id="r-user"></select>
                    </div>
                    <div>
                        <label for="r-device">Device</label>
                        <select id="r-device"></select>
                    </div>
                    <div>
                        <label for="r-genre">Genre</label>
                        <select id="r-genre"></select>
                    </div>
                    <div>
                        <label for="r-hour">Hour: <span id="r-hour-v">21</span>:00</label>
                        <input type="range" id="r-hour" min="0" max="23" value="21">
                    </div>
                </div>
                <div class="grid cols-4">
                    <div>
                        <label for="w-watch">Predicted watch time weight: <span id="w1-v">100</span></label>
                        <input type="range" id="w-watch" min="0" max="100" value="100">
                    </div>
                    <div>
                        <label for="w-pop">Popularity weight: <span id="w2-v">30</span></label>
                        <input type="range" id="w-pop" min="0" max="100" value="30">
                    </div>
                    <div>
                        <label for="w-fresh">Release year weight: <span id="w3-v">10</span></label>
                        <input type="range" id="w-fresh" min="0" max="100" value="10">
                    </div>
                    <div>
                        <label><input type="checkbox" id="r-hide" checked> Hide watched titles</label>
                        <label><input type="checkbox" id="r-weekend"> Weekend</label>
                    </div>
                </div>
                <button class="primary" id="r-go">Rank titles</button>
                <div class="results" id="r-out"></div>
            </div>
        </section>

        <section id="behaviour" class="tab">
            <div class="card">
                <h2>Behaviour change</h2>
                <p class="muted">
                    Sends 8 new viewing events for one user. The online feature store updates
                    immediately, so the prediction changes without a batch rebuild or retraining.
                </p>
                <div class="grid cols-4">
                    <div>
                        <label for="b-user">User</label>
                        <select id="b-user"></select>
                    </div>
                    <div>
                        <label for="b-genre">Genre</label>
                        <select id="b-genre"></select>
                    </div>
                    <div>
                        <label for="b-mode">Behaviour</label>
                        <select id="b-mode">
                            <option value="binge">Watches titles to the end</option>
                            <option value="disengage">Skips titles early</option>
                        </select>
                    </div>
                </div>
                <button class="primary" id="b-go">Send events</button>
                <div id="b-out"></div>
            </div>
        </section>

        <section id="pipeline" class="tab">
            <div class="grid cols-2">
                <div class="card">
                    <h2>Batch jobs</h2>
                    <button class="secondary" id="a-rebuild">Rebuild offline features</button>
                    <button class="secondary" id="a-train">Retrain model</button>
                    <button class="secondary" id="a-regen">Regenerate data and run all</button>
                    <h2 style="margin-top:18px">Ingest a view event</h2>
                    <div class="grid cols-2">
                        <div>
                            <label for="i-user">User</label>
                            <select id="i-user"></select>
                        </div>
                        <div>
                            <label for="i-content">Title</label>
                            <select id="i-content"></select>
                        </div>
                        <div>
                            <label for="i-min">Minutes watched (0 for ignored)</label>
                            <input type="number" id="i-min" value="30" min="0">
                        </div>
                        <div>
                            <label for="i-device">Device</label>
                            <select id="i-device"></select>
                        </div>
                    </div>
                    <button class="primary" id="i-go">Ingest event</button>
                </div>
                <div class="card">
                    <h2>Model evaluation</h2>
                    <div id="m-out"></div>
                </div>
            </div>

            <div class="grid cols-2" style="margin-top:12px">
                <div class="card">
                    <h2>Design summary</h2>
                    <ul class="plain">
                        <li>Entities: user, platform, model</li>
                        <li>Watch time data: impressions; play, pause, resume, stop; outcome of
                            skip, played, left or ignored</li>
                        <li>Features: user, content, context, behavioural</li>
                        <li>Target: predicted completion ratio, converted to minutes</li>
                        <li>Offline store: long-term user and content statistics, rebuilt in batch</li>
                        <li>Online store: moving averages updated on every event</li>
                        <li>Models: ridge regression for prediction, k-means for user segments</li>
                    </ul>
                </div>
                <div class="card">
                    <h2>API</h2>
                    <table>
                        <tr><td><code>GET /api/meta</code></td><td>users and catalogue</td></tr>
                        <tr><td><code>GET /api/state</code></td><td>pipeline and model status</td></tr>
                        <tr><td><code>POST /api/predict</code></td><td>watch time for a user and title</td></tr>
                        <tr><td><code>POST /api/recommend</code></td><td>ranked titles</td></tr>
                        <tr><td><code>POST /api/ingest/event</code></td><td>store one view event</td></tr>
                        <tr><td><code>POST /api/simulate/shift</code></td><td>behaviour change burst</td></tr>
                        <tr><td><code>POST /api/offline/rebuild</code></td><td>rebuild offline features</td></tr>
                        <tr><td><code>POST /api/model/train</code></td><td>retrain model</td></tr>
                    </table>
                </div>
            </div>
        </section>
    </main>

    <script>
        const $ = (id) => document.getElementById(id);
        let META = null;
        let statusTimer = null;

        async function api(path, body) {
            const options = body === undefined ? {} : {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify(body)
            };
            const response = await fetch(path, options);
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || "Request failed");
            }
            return data;
        }

        function setStatus(message) {
            $("status").textContent = message;
            clearTimeout(statusTimer);
            statusTimer = setTimeout(() => { $("status").textContent = ""; }, 4000);
        }

        function guard(fn) {
            return async () => {
                try {
                    await fn();
                } catch (error) {
                    setStatus("Error: " + error.message);
                }
            };
        }

        function fillSelect(id, items, valueKey, labelKey) {
            $(id).innerHTML = items.map((item) => {
                const value = typeof item === "object" ? item[valueKey] : item;
                const label = typeof item === "object" ? item[labelKey] : item;
                return `<option value="${value}">${label}</option>`;
            }).join("");
        }

        function fillContent(id, filter) {
            const query = (filter || "").toLowerCase();
            const items = META.content
                .filter((c) => (c.title + " " + c.genre + " " + c.type).toLowerCase().includes(query))
                .map((c) => ({id: c.id, label: `${c.title} (${c.genre} ${c.type}, ${c.duration} min)`}));
            fillSelect(id, items, "id", "label");
        }

        function minutes(value) {
            return value.toFixed(0) + " min";
        }

        document.querySelectorAll("nav button").forEach((button) => {
            button.onclick = () => {
                document.querySelectorAll("nav button").forEach((b) => b.classList.remove("active"));
                document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
                button.classList.add("active");
                $(button.dataset.tab).classList.add("active");
                if (button.dataset.tab === "dashboard" || button.dataset.tab === "pipeline") {
                    loadState();
                }
            };
        });

        const STAGES = [
            ["ingest", "Data ingest", "View logs, activity, metadata"],
            ["preprocess", "Preprocessing", "Clean, transform"],
            ["feature_store", "Feature store", "Online and offline"],
            ["model_train", "Model training", "Train, evaluate, tune"],
            ["predict", "Prediction service", "Real-time interface"]
        ];

        async function loadState() {
            const state = await api("/api/state");
            const model = state.model;
            const stats = [
                [state.users, "Users"],
                [state.content, "Titles"],
                [state.raw_events.toLocaleString(), "Logged events"],
                [state.plays.toLocaleString(), "Play sessions"],
                [state.pending, "Events pending batch"],
                [model.mae_min + " min", "MAE (baseline " + model.base_mae_min + " min)"],
                [model.r2, "R-squared, held out"],
                [state.pipeline.predict.rows, "Predictions served"]
            ];
            $("stats").innerHTML = stats.map((s) =>
                `<div class="card stat"><div class="value">${s[0]}</div><div class="label">${s[1]}</div></div>`
            ).join("");

            $("flow").innerHTML = STAGES.map((stage) => {
                const info = state.pipeline[stage[0]] || {status: "idle", rows: 0, note: "", at: "-"};
                return `<div class="stage">
                            <b>${stage[1]}</b>
                            <small>${stage[2]}</small>
                            <small>${info.status} &middot; ${info.rows} rows &middot; ${info.at}</small>
                            <small>${info.note || ""}</small>
                        </div>`;
            }).join("");

            $("segments").innerHTML =
                "<tr><th>Segment</th><th>Users</th><th>Avg completion</th></tr>" +
                state.segments.map((s) =>
                    `<tr><td>${s.name}</td><td>${s.count}</td><td>${s.avg_completion}%</td></tr>`
                ).join("");

            $("drift").innerHTML = `
                <p>Completion over the last 300 plays: <b>${state.drift.recent}%</b>
                   (overall ${state.drift.overall}%)</p>
                <p>Users whose recent behaviour differs from their history:
                   <b>${state.drift.users_shifted}</b></p>`;

            $("m-out").innerHTML = `
                <table>
                    <tr><td>MAE (minutes)</td><td>${model.mae_min} (user-average baseline ${model.base_mae_min})</td></tr>
                    <tr><td>RMSE (completion ratio)</td><td>${model.rmse}</td></tr>
                    <tr><td>R-squared</td><td>${model.r2}</td></tr>
                    <tr><td>Selected lambda</td><td>${model.lam}</td></tr>
                    <tr><td>Train / test rows</td><td>${model.n_train} / ${model.n_test}</td></tr>
                    <tr><td>Trained at</td><td>${model.trained_at}</td></tr>
                </table>
                <h2 style="margin-top:14px">Lambda search</h2>
                <table>
                    <tr><th>Lambda</th><th>Validation RMSE</th></tr>
                    ${model.grid.map((g) => `<tr><td>${g.lambda}</td><td>${g.val_rmse}</td></tr>`).join("")}
                </table>`;
        }

        async function runPrediction() {
            const r = await api("/api/predict", {
                user: $("p-user").value,
                content: $("p-content").value,
                device: $("p-device").value,
                hour: +$("p-hour").value,
                weekend: $("p-weekend").checked ? 1 : 0
            });
            const percent = (r.ratio * 100).toFixed(0);
            const groupRows = Object.entries(r.groups).map(([name, value]) => {
                const width = Math.min(50, Math.abs(value) * 2.5);
                const style = value >= 0
                    ? `left:50%;width:${width}%;background:var(--accent)`
                    : `left:${50 - width}%;width:${width}%;background:var(--warn)`;
                return `<div class="contrib">
                            <div class="name">${name}</div>
                            <div class="track"><div class="fill" style="${style}"></div></div>
                            <div class="amount">${value > 0 ? "+" : ""}${value} pp</div>
                        </div>`;
            }).join("");

            $("p-out").innerHTML = `
                ${r.drift_flag ? `<div class="notice">Behaviour change: recent completion is
                    ${r.drift > 0 ? "up" : "down"} ${Math.abs(r.drift)} points against history.</div>` : ""}
                <h2>${r.title} <span class="tag">${r.genre} ${r.type}</span></h2>
                <div class="result-value">${minutes(r.minutes)}</div>
                <p class="muted">of ${r.duration} min &middot; 80% range ${minutes(r.low)} to ${minutes(r.high)}
                    &middot; ${r.outcome}${r.seen ? " &middot; already watched" : ""}</p>
                <div class="meter"><div style="width:${percent}%"></div></div>
                <p>Predicted completion ${percent}% &middot; segment: ${r.segment}</p>
                <h2 style="margin-top:14px">Contribution by feature group (baseline ${r.baseline.toFixed(0)}%)</h2>
                ${groupRows}`;

            const order = ["User", "Content", "Context", "Behavioral"];
            $("p-features").innerHTML = order.map((group) => {
                const rows = r.features.filter((f) => f.group === group);
                return `<div class="card"><h2>${group} features</h2>
                    <table>${rows.map((f) =>
                        `<tr><td>${f.label}</td><td>${f.value}</td><td><span class="tag">${f.store}</span></td></tr>`
                    ).join("")}</table></div>`;
            }).join("");
        }

        async function runRecommend() {
            const r = await api("/api/recommend", {
                user: $("r-user").value,
                device: $("r-device").value,
                genre: $("r-genre").value,
                hour: +$("r-hour").value,
                hide_seen: $("r-hide").checked,
                weekend: $("r-weekend").checked ? 1 : 0,
                w_watch: $("w-watch").value / 100,
                w_pop: $("w-pop").value / 100,
                w_fresh: $("w-fresh").value / 100
            });
            $("r-out").innerHTML = r.items.map((x, i) => `
                <div class="item">
                    <h3>${i + 1}. ${x.title}</h3>
                    <div class="muted">${x.genre} &middot; ${x.type} &middot; ${x.year}</div>
                    <p>${minutes(x.minutes)} predicted of ${x.duration} min (${(x.ratio * 100).toFixed(0)}%)</p>
                    <div class="muted">Score ${x.score.toFixed(2)}</div>
                </div>`).join("") || '<p class="muted">No titles match.</p>';
        }

        async function runShift() {
            const r = await api("/api/simulate/shift", {
                user: $("b-user").value,
                genre: $("b-genre").value,
                mode: $("b-mode").value
            });
            const delta = r.after.minutes - r.before.minutes;
            $("b-out").innerHTML = `
                <div class="comparison">
                    <div><div class="muted">Before</div><div class="result-value">${minutes(r.before.minutes)}</div></div>
                    <div class="muted">to</div>
                    <div><div class="muted">After 8 events</div><div class="result-value">${minutes(r.after.minutes)}</div></div>
                </div>
                <p>Title: ${r.content.title} (${r.content.genre}, ${r.content.duration} min).
                   Change: ${delta >= 0 ? "+" : ""}${delta.toFixed(1)} min, without retraining.</p>
                <p class="muted">${r.pending} events are waiting for the next batch rebuild.</p>`;
        }

        async function ingest() {
            const r = await api("/api/ingest/event", {
                user: $("i-user").value,
                content: $("i-content").value,
                watched: +$("i-min").value,
                device: $("i-device").value,
                hour: 21
            });
            setStatus(`Event stored as ${r.event.outcome}. ${r.pending} events pending batch.`);
        }

        async function runJob(path, message) {
            setStatus("Running...");
            await api(path, {});
            setStatus(message);
            loadState();
        }

        async function init() {
            META = await api("/api/meta");
            ["p-user", "r-user", "b-user", "i-user"].forEach((id) =>
                fillSelect(id, META.users, "id", "label"));
            ["p-device", "r-device", "i-device"].forEach((id) => fillSelect(id, META.devices));
            fillSelect("b-genre", META.genres);
            $("r-genre").innerHTML = '<option value="">All genres</option>' +
                META.genres.map((g) => `<option>${g}</option>`).join("");
            fillContent("p-content");
            fillContent("i-content");

            $("p-filter").oninput = (e) => fillContent("p-content", e.target.value);
            $("p-hour").oninput = (e) => { $("p-hour-v").textContent = e.target.value; };
            $("r-hour").oninput = (e) => { $("r-hour-v").textContent = e.target.value; };
            [["w-watch", "w1-v"], ["w-pop", "w2-v"], ["w-fresh", "w3-v"]].forEach(([source, target]) => {
                $(source).oninput = (e) => { $(target).textContent = e.target.value; };
            });

            $("p-go").onclick = guard(runPrediction);
            $("r-go").onclick = guard(runRecommend);
            $("b-go").onclick = guard(runShift);
            $("i-go").onclick = guard(ingest);
            $("a-rebuild").onclick = guard(() => runJob("/api/offline/rebuild", "Offline features rebuilt"));
            $("a-train").onclick = guard(() => runJob("/api/model/train", "Model retrained"));
            $("a-regen").onclick = guard(async () => {
                await runJob("/api/pipeline/run", "Data regenerated and pipeline re-run");
                META = await api("/api/meta");
            });
            loadState();
        }

        init().catch((error) => setStatus("Startup error: " + error.message));
    </script>
</body>
</html>
"""

print("Running pipeline: ingest, preprocess, feature store, train")
run_pipeline(regenerate=True)
print(f"Model ready: MAE {MODEL['mae_min']} min "
      f"(baseline {MODEL['base_mae_min']} min), R2 {MODEL['r2']}")

if "_SERVER" in globals():
    try:
        _SERVER.shutdown()
        _SERVER.server_close()
    except Exception:
        pass

_SERVER = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=_SERVER.serve_forever, daemon=True).start()

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height="1000")
except ImportError:
    print(f"Open http://localhost:{PORT} in a browser")