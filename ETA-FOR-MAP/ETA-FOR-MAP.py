'''
Estimated Time of Arrival (ETA) for Maps



1. Requirement Analysis

Functional Requirements

- System that gives ETA for Maps.

Non-Functional Requirements

* Scale: 10M DAU.
* CAP Theorem: Available >>> Consistent.

2. Identifying Entities

* User
* Map
* ETA

3. Pipeline Design

Interface:

def f(segment_id, interval_within_week) -> eta

Parametrization:

eta = f(segment_id, interval, within_week)

4. High-Level Design

a. Data – Make sure map data and travel data are usable for training.

b. Model – Parametrization.

c. Training – Features.

d. Validation – Train/test split.

e. Deployment – Use it in product.

Data

Map:

* segment_id
* distance
* speed_limit
* free_flow_speed

Travel:

* year
* num_car
* avg_speed
* interval – within_year



5. low level design 
![ETA-FOR-MAP Low-Level Design](ETA-FOR-MAP.jpg)

'''
import sys
import subprocess
import importlib

for _pkg, _mod in [("flask", "flask"), ("numpy", "numpy"), ("scikit-learn", "sklearn")]:
    try:
        importlib.import_module(_mod)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", _pkg])

import heapq
import logging
import math
import random
import socket
import threading
import time
from collections import Counter

import numpy as np
from flask import Flask, Response, jsonify, request
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
INTERVALS_PER_DAY = 48
INTERVALS_PER_WEEK = 7 * INTERVALS_PER_DAY
INTERVAL_SECONDS = 1800
WEEK_SECONDS = 7 * 86400
KM_PER_UNIT = 0.012
YEARS = [2024, 2025, 2026]
DEPLOY_YEAR = YEARS[-1]
MAP_W, MAP_H = 1000, 640

STATS = Counter()


def interval_label(i):
    d = (i // INTERVALS_PER_DAY) % 7
    m = (i % INTERVALS_PER_DAY) * 30
    return f"{DAYS[d][:3]} {m // 60:02d}:{m % 60:02d}"


def classify(kind, r, c):
    if kind == "H":
        return "highway" if r == 2 else "arterial" if r in (0, 4) else "local"
    if kind == "V":
        return "highway" if c == 3 else "arterial" if c in (1, 5) else "local"
    return "local"


def is_connected(n_nodes, edges):
    adj = {i: [] for i in range(n_nodes)}
    for a, b, *_ in edges:
        adj[a].append(b)
        adj[b].append(a)
    seen, stack = {0}, [0]
    while stack:
        n = stack.pop()
        for m in adj[n]:
            if m not in seen:
                seen.add(m)
                stack.append(m)
    return len(seen) == n_nodes


def build_map(seed=7):
    rnd = random.Random(seed)
    cols, rows = 7, 5
    nodes = []
    for r in range(rows):
        for c in range(cols):
            nodes.append({
                "id": r * cols + c,
                "x": round(90 + c * 135 + rnd.uniform(-20, 20), 1),
                "y": round(75 + r * 120 + rnd.uniform(-18, 18), 1),
            })
    edges = []
    for r in range(rows):
        for c in range(cols):
            i = r * cols + c
            if c + 1 < cols:
                edges.append((i, i + 1, "H", r, c))
            if r + 1 < rows:
                edges.append((i, i + cols, "V", r, c))
    for (r, c) in [(0, 0), (1, 4), (2, 1), (3, 5)]:
        i = r * cols + c
        edges.append((i, i + cols + 1, "D", r, c))

    removable = [e for e in edges if e[2] != "D" and classify(*e[2:]) == "local"]
    rnd.shuffle(removable)
    removed = 0
    for e in removable:
        if removed >= 7:
            break
        trial = [x for x in edges if x != e]
        if is_connected(len(nodes), trial):
            edges = trial
            removed += 1

    counters = Counter()
    prefix = {"highway": "Expressway", "arterial": "Avenue", "local": "Street"}
    limits = {"highway": 80, "arterial": 60, "local": 40}
    segments = []
    for sid, (a, b, kind, r, c) in enumerate(edges):
        rtype = classify(kind, r, c)
        counters[rtype] += 1
        na, nb = nodes[a], nodes[b]
        length = math.hypot(na["x"] - nb["x"], na["y"] - nb["y"])
        segments.append({
            "segment_id": sid,
            "u": a,
            "v": b,
            "road_type": rtype,
            "name": f"{prefix[rtype]} {counters[rtype]}",
            "distance_km": round(max(length * KM_PER_UNIT, 0.2), 3),
            "speed_limit": limits[rtype],
            "free_flow_speed": round(limits[rtype] * rnd.uniform(0.85, 0.95), 1),
            "mx": (na["x"] + nb["x"]) / 2,
            "my": (na["y"] + nb["y"]) / 2,
        })
    return nodes, segments


NODES, SEGMENTS = build_map()
N_SEG = len(SEGMENTS)
NODE_XY = {n["id"]: (n["x"], n["y"]) for n in NODES}
ADJ = {n["id"]: [] for n in NODES}
for _s in SEGMENTS:
    ADJ[_s["u"]].append((_s["v"], _s["segment_id"]))
    ADJ[_s["v"]].append((_s["u"], _s["segment_id"]))

SEG_DIST = np.array([s["distance_km"] for s in SEGMENTS])
SEG_LIMIT = np.array([s["speed_limit"] for s in SEGMENTS], dtype=float)
SEG_FF = np.array([s["free_flow_speed"] for s in SEGMENTS])
FREE_FLOW_ETA = SEG_DIST / SEG_FF * 3600.0


def demand_profile(dow, hour):
    if dow < 5:
        return (0.12
                + 0.95 * math.exp(-((hour - 8.3) / 1.4) ** 2)
                + 1.05 * math.exp(-((hour - 17.6) / 1.7) ** 2)
                + 0.38 * math.exp(-((hour - 13.0) / 3.2) ** 2))
    return (0.10
            + 0.55 * math.exp(-((hour - 13.5) / 3.4) ** 2)
            + 0.35 * math.exp(-((hour - 19.0) / 2.2) ** 2))


TYPE_FACTOR = {"highway": 1.0, "arterial": 0.9, "local": 0.65}
TYPE_CAPACITY = {"highway": 3600, "arterial": 1800, "local": 700}


def generate_travel_data(seed=11):
    rng = np.random.default_rng(seed)
    rows = []
    for yi, year in enumerate(YEARS):
        growth = 1.0 + 0.05 * yi
        for s in SEGMENTS:
            d = math.hypot(s["mx"] - 450, s["my"] - 315)
            centrality = math.exp(-(d / 380.0) ** 2)
            for interval in range(INTERVALS_PER_WEEK):
                dow = interval // INTERVALS_PER_DAY
                hour = (interval % INTERVALS_PER_DAY) / 2 + 0.25
                load = (demand_profile(dow, hour)
                        * (0.55 + 0.9 * centrality)
                        * TYPE_FACTOR[s["road_type"]]
                        * growth
                        * rng.normal(1.0, 0.04))
                load = max(load, 0.02)
                speed = s["free_flow_speed"] / (1 + 0.6 * load ** 3) * rng.normal(1.0, 0.03)
                speed = float(min(max(speed, 0.12 * s["free_flow_speed"]), s["free_flow_speed"]))
                cars = int(TYPE_CAPACITY[s["road_type"]] * load / 2)
                rows.append((s["segment_id"], year, interval, cars, speed))
    return np.array(rows)


def make_features(seg_ids, intervals, years):
    seg_ids = np.asarray(seg_ids).astype(int)
    intervals = np.asarray(intervals).astype(int)
    dow = intervals // INTERVALS_PER_DAY
    hour = (intervals % INTERVALS_PER_DAY) / 2.0
    ang = 2 * math.pi * hour / 24.0
    return np.column_stack([
        seg_ids,
        SEG_DIST[seg_ids],
        SEG_LIMIT[seg_ids],
        SEG_FF[seg_ids],
        intervals,
        dow,
        hour,
        np.sin(ang),
        np.cos(ang),
        (dow >= 5).astype(float),
        np.asarray(years, dtype=float),
    ])


FEATURE_NAMES = ["segment_id", "distance", "speed_limit", "free_flow_speed",
                 "interval_within_week", "day_of_week", "hour", "sin_hour",
                 "cos_hour", "is_weekend", "year"]

TRAVEL = generate_travel_data()

X = make_features(TRAVEL[:, 0], TRAVEL[:, 2], TRAVEL[:, 1])
y = TRAVEL[:, 4]
dist_all = SEG_DIST[TRAVEL[:, 0].astype(int)]
X_tr, X_te, y_tr, y_te, d_tr, d_te = train_test_split(
    X, y, dist_all, test_size=0.2, random_state=42)

MODEL = HistGradientBoostingRegressor(
    max_iter=300, learning_rate=0.08, max_leaf_nodes=48,
    categorical_features=[0], random_state=42)
MODEL.fit(X_tr, y_tr)

_pred_te = np.clip(MODEL.predict(X_te), 3.0, None)
_eta_true = d_te / y_te * 3600
_eta_pred = d_te / _pred_te * 3600
METRICS = {
    "train_rows": int(len(X_tr)),
    "test_rows": int(len(X_te)),
    "mae_speed": float(mean_absolute_error(y_te, _pred_te)),
    "rmse_speed": float(math.sqrt(mean_squared_error(y_te, _pred_te))),
    "r2": float(r2_score(y_te, _pred_te)),
    "mae_eta_s": float(mean_absolute_error(_eta_true, _eta_pred)),
    "mape_eta": float(np.mean(np.abs(_eta_true - _eta_pred) / _eta_true) * 100),
}

_seg_grid = np.repeat(np.arange(N_SEG), INTERVALS_PER_WEEK)
_int_grid = np.tile(np.arange(INTERVALS_PER_WEEK), N_SEG)
_pred = MODEL.predict(make_features(_seg_grid, _int_grid, np.full(len(_seg_grid), DEPLOY_YEAR)))
SPEED_TABLE = np.clip(_pred.reshape(N_SEG, INTERVALS_PER_WEEK),
                      0.12 * SEG_FF[:, None], SEG_FF[:, None])
ETA_TABLE = SEG_DIST[:, None] / SPEED_TABLE * 3600.0


def segment_eta(seg_id, interval, degraded=False):
    STATS["lookups"] += 1
    try:
        if degraded:
            raise RuntimeError("simulated model outage")
        eta = float(ETA_TABLE[seg_id, interval % INTERVALS_PER_WEEK])
        if not math.isfinite(eta):
            raise ValueError("bad value")
        return eta, False
    except Exception:
        STATS["fallbacks"] += 1
        return float(FREE_FLOW_ETA[seg_id]), True


def interval_of(clock_sec):
    return int(clock_sec // INTERVAL_SECONDS) % INTERVALS_PER_WEEK


def map_match(x, y):
    best = None
    for s in SEGMENTS:
        ax, ay = NODE_XY[s["u"]]
        bx, by = NODE_XY[s["v"]]
        dx, dy = bx - ax, by - ay
        l2 = dx * dx + dy * dy
        t = 0.0 if l2 == 0 else max(0.0, min(1.0, ((x - ax) * dx + (y - ay) * dy) / l2))
        px, py = ax + t * dx, ay + t * dy
        d = math.hypot(x - px, y - py)
        if best is None or d < best["d"]:
            best = {"d": d, "segment_id": s["segment_id"], "t": t, "x": px, "y": py}
    seg = SEGMENTS[best["segment_id"]]
    return {
        "segment_id": best["segment_id"],
        "segment_name": seg["name"],
        "road_type": seg["road_type"],
        "t": best["t"],
        "x": best["x"],
        "y": best["y"],
        "raw_x": x,
        "raw_y": y,
        "offset_m": best["d"] * KM_PER_UNIT * 1000,
        "confidence": max(0.0, 1.0 - best["d"] / 120.0),
    }


def edge_cost(seg_id, clock, frac, mode, degraded):
    if mode == "shortest":
        return SEGMENTS[seg_id]["distance_km"] * frac
    eta, _ = segment_eta(seg_id, interval_of(clock), degraded)
    return eta * frac


def dijkstra(start_costs, depart_sec, mode, degraded):
    dist = dict(start_costs)
    prev = {}
    heap = [(c, n) for n, c in start_costs.items()]
    heapq.heapify(heap)
    while heap:
        d, n = heapq.heappop(heap)
        if d > dist.get(n, float("inf")):
            continue
        clock = depart_sec + (d if mode == "fastest" else 0)
        for m, sid in ADJ[n]:
            nd = d + edge_cost(sid, clock, 1.0, mode, degraded)
            if nd < dist.get(m, float("inf")):
                dist[m] = nd
                prev[m] = (n, sid)
                heapq.heappush(heap, (nd, m))
    return dist, prev


def make_leg(seg, frac, p_from, p_to):
    return {"segment_id": seg["segment_id"], "frac": frac, "from": p_from, "to": p_to}


def plan_route(mo, md, depart_sec, mode, degraded):
    so, sd = SEGMENTS[mo["segment_id"]], SEGMENTS[md["segment_id"]]
    po, pd = (mo["x"], mo["y"]), (md["x"], md["y"])
    if so["segment_id"] == sd["segment_id"]:
        return [make_leg(so, abs(mo["t"] - md["t"]), po, pd)]

    start_fracs = {so["u"]: mo["t"], so["v"]: 1 - mo["t"]}
    start_costs = {n: edge_cost(so["segment_id"], depart_sec, f, mode, degraded)
                   for n, f in start_fracs.items()}
    dist, prev = dijkstra(start_costs, depart_sec, mode, degraded)

    end_fracs = {sd["u"]: md["t"], sd["v"]: 1 - md["t"]}
    best = None
    for n, f in end_fracs.items():
        if n not in dist:
            continue
        clock = depart_sec + (dist[n] if mode == "fastest" else 0)
        total = dist[n] + edge_cost(sd["segment_id"], clock, f, mode, degraded)
        if best is None or total < best[0]:
            best = (total, n, f)
    _, end_node, end_frac = best

    path = [end_node]
    while path[-1] in prev:
        path.append(prev[path[-1]][0])
    path.reverse()
    start_node = path[0]

    legs = [make_leg(so, start_fracs[start_node], po, NODE_XY[start_node])]
    for a, b in zip(path, path[1:]):
        legs.append(make_leg(SEGMENTS[prev[b][1]], 1.0, NODE_XY[a], NODE_XY[b]))
    legs.append(make_leg(sd, end_frac, NODE_XY[end_node], pd))
    return [lg for lg in legs if lg["frac"] > 1e-3]


def aggregate(legs, depart_sec, degraded):
    clock = depart_sec
    rows, path = [], []
    total_dist = total_eta = total_ff = 0.0
    fallbacks = 0
    for lg in legs:
        seg = SEGMENTS[lg["segment_id"]]
        interval = interval_of(clock)
        full_eta, fb = segment_eta(seg["segment_id"], interval, degraded)
        fallbacks += int(fb)
        eta = full_eta * lg["frac"]
        dist = seg["distance_km"] * lg["frac"]
        ff = float(FREE_FLOW_ETA[seg["segment_id"]]) * lg["frac"]
        rows.append({
            "segment_id": seg["segment_id"],
            "name": seg["name"],
            "road_type": seg["road_type"],
            "distance_km": dist,
            "eta_s": eta,
            "speed_kmh": dist / eta * 3600 if eta > 0 else 0,
            "free_flow_s": ff,
            "interval": interval,
            "interval_label": interval_label(interval),
            "fallback": fb,
        })
        path.append(list(lg["from"]))
        clock += eta
        total_dist += dist
        total_eta += eta
        total_ff += ff
    if legs:
        path.append(list(legs[-1]["to"]))
    return {
        "legs": rows,
        "path": path,
        "eta_s": total_eta,
        "distance_km": total_dist,
        "free_flow_s": total_ff,
        "delay_s": total_eta - total_ff,
        "avg_speed": total_dist / total_eta * 3600 if total_eta > 0 else 0,
        "fallbacks": fallbacks,
    }


app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.ERROR)


@app.route("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


@app.route("/api/map")
def api_map():
    return jsonify({
        "nodes": NODES,
        "segments": SEGMENTS,
        "meta": {"days": DAYS, "intervals_per_week": INTERVALS_PER_WEEK,
                 "width": MAP_W, "height": MAP_H, "deploy_year": DEPLOY_YEAR},
    })


@app.route("/api/traffic")
def api_traffic():
    interval = int(request.args.get("interval", 0)) % INTERVALS_PER_WEEK
    degraded = request.args.get("degraded") == "1"
    if degraded:
        speeds = SEG_FF
    else:
        speeds = SPEED_TABLE[:, interval]
    return jsonify({"interval": interval,
                    "speed": [round(float(v), 1) for v in speeds],
                    "ratio": [round(float(v / f), 3) for v, f in zip(speeds, SEG_FF)]})


@app.route("/api/eta")
def api_eta():
    try:
        seg_id = int(request.args["segment_id"])
        interval = int(request.args["interval"]) % INTERVALS_PER_WEEK
        assert 0 <= seg_id < N_SEG
    except Exception:
        return jsonify({"error": "segment_id / interval invalid"}), 400
    degraded = request.args.get("degraded") == "1"
    STATS["requests"] += 1
    eta, fb = segment_eta(seg_id, interval, degraded)
    seg = SEGMENTS[seg_id]
    profile = [round(float(v), 2) for v in (FREE_FLOW_ETA[seg_id] * np.ones(INTERVALS_PER_WEEK)
                                           if degraded else ETA_TABLE[seg_id])]
    return jsonify({
        "segment": seg,
        "interval": interval,
        "interval_label": interval_label(interval),
        "eta_s": eta,
        "speed_kmh": seg["distance_km"] / eta * 3600,
        "free_flow_s": float(FREE_FLOW_ETA[seg_id]),
        "fallback": fb,
        "profile": profile,
    })


@app.route("/api/route", methods=["POST"])
def api_route():
    t_start = time.perf_counter()
    body = request.get_json(force=True, silent=True) or {}
    try:
        ox, oy = float(body["origin"]["x"]), float(body["origin"]["y"])
        dx, dy = float(body["dest"]["x"]), float(body["dest"]["y"])
        dow = int(body["dow"]) % 7
        hh, mm = [int(v) for v in str(body["time"]).split(":")[:2]]
    except Exception:
        return jsonify({"error": "Invalid request body"}), 400
    degraded = bool(body.get("degraded"))
    depart_sec = dow * 86400 + hh * 3600 + mm * 60
    STATS["requests"] += 1
    lookups_before = STATS["lookups"]

    t0 = time.perf_counter()
    mo, md = map_match(ox, oy), map_match(dx, dy)
    t_match = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    legs_fast = plan_route(mo, md, depart_sec, "fastest", degraded)
    legs_short = plan_route(mo, md, depart_sec, "shortest", degraded)
    t_route = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    fastest = aggregate(legs_fast, depart_sec, degraded)
    shortest = aggregate(legs_short, depart_sec, degraded)
    t_agg = (time.perf_counter() - t0) * 1000
    lookups_used = STATS["lookups"] - lookups_before


    snap = dict(STATS)
    profile = []
    for k in range(INTERVALS_PER_DAY):
        ds = dow * 86400 + k * INTERVAL_SECONDS + 900
        res = aggregate(plan_route(mo, md, ds, "fastest", degraded), ds, degraded)
        profile.append({"minute": k * 30, "eta_s": res["eta_s"]})
    STATS.clear()
    STATS.update(snap)

    return jsonify({
        "match": {"origin": mo, "dest": md},
        "fastest": fastest,
        "shortest": shortest,
        "profile": profile,
        "depart_sec": depart_sec,
        "degraded": degraded,
        "pipeline": {
            "map_matching_ms": round(t_match, 2),
            "routing_ms": round(t_route, 2),
            "aggregator_ms": round(t_agg, 2),
            "total_ms": round((time.perf_counter() - t_start) * 1000, 2),
            "lookups": lookups_used,
            "segments_read": len(fastest["legs"]) + len(shortest["legs"]),
        },
    })


@app.route("/api/model")
def api_model():
    rng = np.random.default_rng(5)
    idx = rng.choice(len(TRAVEL), 14, replace=False)
    sample = [{
        "segment_id": int(TRAVEL[i, 0]),
        "year": int(TRAVEL[i, 1]),
        "interval": int(TRAVEL[i, 2]),
        "interval_label": interval_label(int(TRAVEL[i, 2])),
        "num_car": int(TRAVEL[i, 3]),
        "avg_speed": round(float(TRAVEL[i, 4]), 1),
    } for i in idx]
    return jsonify({
        "metrics": METRICS,
        "features": FEATURE_NAMES,
        "travel_rows": int(len(TRAVEL)),
        "n_segments": N_SEG,
        "table_cells": int(ETA_TABLE.size),
        "table_kb": round(ETA_TABLE.nbytes / 1024, 1),
        "sample": sample,
    })


@app.route("/api/stats")
def api_stats():
    return jsonify({"requests": STATS["requests"], "lookups": STATS["lookups"],
                    "fallbacks": STATS["fallbacks"]})


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>ETA for Maps</title>
    <style>
        :root {
            --accent: #1e4fb3;
            --border: #d8dce3;
            --text: #1d2330;
            --muted: #667085;
            --surface: #ffffff;
            --bg: #f6f7f9;
            --danger: #b42318;
            --ok: #1a7f4b;
            --map-bg: #f1f3f6;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
            background: var(--bg);
            color: var(--text);
            font-size: 14px;
        }

        .topbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 10px;
            padding: 12px 20px;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
        }

        .brand h1 {
            margin: 0;
            font-size: 18px;
            font-weight: 600;
        }

        .brand small {
            display: block;
            color: var(--muted);
            font-size: 12px;
        }

        .badges {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }

        .badge {
            border: 1px solid var(--border);
            padding: 3px 9px;
            border-radius: 4px;
            font-size: 12px;
            color: var(--muted);
        }

        .wrap {
            max-width: 1320px;
            margin: 0 auto;
            padding: 16px 20px 32px;
        }

        .pipeline {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
            flex-wrap: wrap;
            margin-bottom: 14px;
        }

        .arrow {
            color: var(--muted);
        }

        .stage {
            min-width: 110px;
            padding: 8px 12px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            text-align: center;
            font-weight: 500;
        }

        .stage small {
            display: block;
            font-weight: 400;
            color: var(--muted);
            font-size: 11px;
            min-height: 14px;
        }

        .stage.done {
            border-color: var(--accent);
        }

        .model-box {
            border: 1px solid var(--border);
            background: #eef1f6;
            border-radius: 6px;
            padding: 8px;
            min-width: 240px;
        }

        .model-box .title {
            text-align: center;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.6px;
            color: var(--muted);
            margin-bottom: 6px;
        }

        .model-box .stage {
            min-width: 0;
            margin-bottom: 5px;
            padding: 5px 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
            text-align: left;
        }

        .model-box .stage:last-child {
            margin-bottom: 0;
        }

        .toolbar {
            display: flex;
            flex-wrap: wrap;
            align-items: flex-end;
            gap: 12px 16px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 12px 14px;
            margin-bottom: 14px;
        }

        .field label {
            display: block;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--muted);
            margin-bottom: 4px;
        }

        select,
        input[type="time"] {
            font: inherit;
            padding: 6px 8px;
            border: 1px solid var(--border);
            border-radius: 4px;
            background: #fff;
            color: var(--text);
        }

        button {
            font: inherit;
            cursor: pointer;
            border: 1px solid var(--border);
            background: #fff;
            color: var(--text);
            padding: 6px 11px;
            border-radius: 4px;
        }

        button:hover {
            background: #f0f2f6;
        }

        button.primary,
        button.active {
            background: var(--accent);
            border-color: var(--accent);
            color: #fff;
        }

        .presets {
            display: flex;
            gap: 6px;
            flex-wrap: wrap;
        }

        .toggle {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 13px;
            padding: 6px 0;
        }

        .tabs {
            display: flex;
            gap: 4px;
            margin-bottom: 12px;
            border-bottom: 1px solid var(--border);
        }

        .tabs button {
            border: none;
            border-bottom: 2px solid transparent;
            border-radius: 0;
            background: none;
            padding: 8px 14px;
            color: var(--muted);
        }

        .tabs button.active {
            color: var(--accent);
            border-bottom-color: var(--accent);
        }

        .tab {
            display: none;
        }

        .tab.active {
            display: block;
        }

        .grid {
            display: grid;
            grid-template-columns: 320px 1fr;
            gap: 14px;
            align-items: start;
        }

        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 14px 16px;
            margin-bottom: 14px;
        }

        .card h3 {
            margin: 0 0 10px;
            font-size: 13px;
            font-weight: 600;
        }

        .card p.hint {
            margin: 0 0 10px;
            color: var(--muted);
            font-size: 12.5px;
            line-height: 1.5;
        }

        .row {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }

        svg.map {
            width: 100%;
            height: auto;
            display: block;
            border: 1px solid var(--border);
            border-radius: 4px;
            cursor: crosshair;
            background: var(--map-bg);
        }

        .legend {
            display: flex;
            gap: 14px;
            flex-wrap: wrap;
            margin-top: 8px;
            font-size: 12px;
            color: var(--muted);
        }

        .legend i {
            display: inline-block;
            width: 18px;
            height: 5px;
            margin-right: 5px;
            vertical-align: middle;
        }

        .kpis {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-bottom: 14px;
        }

        .kpi {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 11px 13px;
        }

        .kpi .label {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--muted);
        }

        .kpi .value {
            font-size: 20px;
            font-weight: 600;
            margin-top: 4px;
        }

        .kpi .sub {
            font-size: 12px;
            color: var(--muted);
            margin-top: 2px;
        }

        .kpi.primary {
            border-color: var(--accent);
        }

        .kpi.primary .value {
            color: var(--accent);
            font-size: 24px;
        }

        .bad {
            color: var(--danger);
        }

        .good {
            color: var(--ok);
        }

        .tip {
            border-left: 3px solid var(--ok);
            padding: 6px 10px;
            margin-top: 10px;
            font-size: 13px;
            background: #f6faf8;
        }

        .warn {
            border-left: 3px solid #c77700;
            padding: 8px 12px;
            margin-bottom: 12px;
            font-size: 13px;
            background: #fffaf0;
        }

        .match-box {
            font-size: 12.5px;
            line-height: 1.55;
            padding: 8px 10px;
            border: 1px solid var(--border);
            border-radius: 4px;
            margin-bottom: 8px;
        }

        .conf {
            height: 4px;
            background: #e4e8ef;
            margin-top: 4px;
        }

        .conf span {
            display: block;
            height: 100%;
            background: var(--accent);
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 12.5px;
        }

        th,
        td {
            text-align: left;
            padding: 6px 8px;
            border-bottom: 1px solid #e9ecf1;
            white-space: nowrap;
        }

        th {
            color: var(--muted);
            font-weight: 600;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.4px;
            position: sticky;
            top: 0;
            background: #fff;
        }

        .scroll {
            max-height: 300px;
            overflow: auto;
            border: 1px solid var(--border);
            border-radius: 4px;
        }

        .pill {
            font-size: 11px;
            color: var(--muted);
            border: 1px solid var(--border);
            border-radius: 3px;
            padding: 0 5px;
        }

        .code {
            font-family: Consolas, "Courier New", monospace;
            background: #1b2130;
            color: #dfe6f5;
            padding: 10px 14px;
            border-radius: 4px;
            font-size: 13px;
            overflow-x: auto;
            margin-bottom: 12px;
        }

        .two {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 14px;
        }

        .steps {
            margin: 0;
            padding-left: 18px;
            line-height: 1.7;
        }

        .footer {
            display: flex;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 8px;
            color: var(--muted);
            font-size: 12px;
            margin-top: 10px;
        }

        .empty {
            color: var(--muted);
            padding: 10px 0;
        }

        @media (max-width: 980px) {
            .grid,
            .two {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <header class="topbar">
        <div class="brand">
            <div>
                <h1>ETA for Maps</h1>
                <small>Estimated Time of Arrival &middot; no login required</small>
            </div>
        </div>
        <div class="badges">
            <span class="badge">10M DAU design</span>
            <span class="badge">Available &gt;&gt;&gt; Consistent</span>
            <span class="badge" id="deployBadge"></span>
        </div>
    </header>

    <div class="wrap">
        <section class="pipeline">
            <div class="stage" id="st-user">User<small id="m-user">click the map</small></div>
            <div class="arrow">&rarr;</div>
            <div class="stage" id="st-match">Map Matching<small id="m-match">snap GPS to road</small></div>
            <div class="arrow">&rarr;</div>
            <div class="model-box">
                <div class="title">Model</div>
                <div class="stage" id="st-client">Client App<small id="m-client">request</small></div>
                <div class="stage" id="st-agg">Aggregator<small id="m-agg">sum segments</small></div>
                <div class="stage" id="st-eta">ETA Prediction<small id="m-eta">f(segment, interval)</small></div>
                <div class="stage" id="st-data">Map Data<small id="m-data">segments</small></div>
            </div>
            <div class="arrow">&rarr;</div>
            <div class="stage" id="st-route">Routing<small id="m-route">fastest path</small></div>
        </section>

        <section class="toolbar">
            <div class="field">
                <label for="day">Day of week</label>
                <select id="day"></select>
            </div>
            <div class="field">
                <label for="time">Departure time</label>
                <input type="time" id="time" value="08:30">
            </div>
            <div class="field">
                <label>Quick set</label>
                <div class="presets">
                    <button id="btnNow">Now</button>
                    <button data-day="0" data-time="08:30">Mon rush</button>
                    <button data-day="1" data-time="13:00">Midday</button>
                    <button data-day="3" data-time="17:30">Evening rush</button>
                    <button data-day="5" data-time="11:00">Sat 11:00</button>
                    <button data-day="2" data-time="02:00">Night</button>
                </div>
            </div>
            <label class="toggle" title="Demonstrates graceful degradation: free-flow ETA is served when the model is unavailable">
                <input type="checkbox" id="degraded">
                Simulate model outage
            </label>
        </section>

        <nav class="tabs">
            <button class="active" data-tab="trip">Trip ETA</button>
            <button data-tab="segment">Segment ETA f( )</button>
            <button data-tab="model">System &amp; Model</button>
        </nav>

        <section class="tab active" id="tab-trip">
            <div class="grid">
                <aside>
                    <div class="card">
                        <h3>1. Pick your trip</h3>
                        <p class="hint">
                            Click anywhere on the map. Your raw GPS point is map-matched to the nearest road,
                            then the system predicts travel time using the traffic pattern for the chosen weekly interval.
                        </p>
                        <div class="row">
                            <button id="pickOrigin" class="active">Set origin A</button>
                            <button id="pickDest">Set destination B</button>
                        </div>
                        <div class="row" style="margin-top: 8px;">
                            <button id="btnRandom">Random trip</button>
                            <button id="btnSwap">Swap A/B</button>
                            <button id="btnClear">Clear</button>
                        </div>
                    </div>

                    <div class="card">
                        <h3>2. Map matching</h3>
                        <div id="matchInfo" class="empty">No points yet.</div>
                    </div>

                    <div class="card">
                        <h3>Pipeline timing</h3>
                        <div id="timing" class="empty">Run a trip to see timings.</div>
                    </div>
                </aside>

                <div>
                    <div class="card">
                        <h3>Road network &middot; live traffic for the selected interval</h3>
                        <svg id="mapTrip" class="map" viewBox="0 0 1000 640"></svg>
                        <div class="legend">
                            <span><i style="background: #22b573;"></i>Free flowing</span>
                            <span><i style="background: #f2c230;"></i>Moderate</span>
                            <span><i style="background: #f08a24;"></i>Slow</span>
                            <span><i style="background: #d6303b;"></i>Congested</span>
                            <span><i style="background: #1e4fb3;"></i>Fastest route</span>
                            <span><i style="background: #8a94a8;"></i>Shortest-distance route</span>
                        </div>
                    </div>

                    <div id="results"></div>
                </div>
            </div>
        </section>

        <section class="tab" id="tab-segment">
            <div class="grid">
                <aside>
                    <div class="card">
                        <h3>Query the model directly</h3>
                        <p class="hint">
                            The whole system boils down to one function:
                            <b>eta = f(segment_id, interval_within_week)</b>.
                            Choose a segment; day and time come from the toolbar above.
                        </p>
                        <div class="field">
                            <label for="segSelect">Segment</label>
                            <select id="segSelect" style="width: 100%;"></select>
                        </div>
                        <div class="row" style="margin-top: 10px;">
                            <button class="primary" id="btnSeg">Get ETA</button>
                        </div>
                    </div>
                    <div class="card">
                        <h3>Segment location</h3>
                        <svg id="mapSeg" class="map" viewBox="0 0 1000 640" style="cursor: default;"></svg>
                    </div>
                </aside>
                <div id="segResults"></div>
            </div>
        </section>

        <section class="tab" id="tab-model">
            <div id="modelView" class="empty">Loading ...</div>
        </section>

        <div class="footer">
            <span>Map and travel data are synthetic (generated at start-up) to demonstrate the design.</span>
            <span id="statsChip">Requests 0 &middot; Table lookups 0 &middot; Fallbacks 0</span>
        </div>
    </div>

    <script>
        const $ = (id) => document.getElementById(id);
        const state = { meta: null, nodes: [], segs: [], traffic: null, origin: null, dest: null,
                    pick: "origin", route: null, modelLoaded: false };

        async function api(url, body) {
            const opt = body ? { method: "POST", headers: { "Content-Type": "application/json" },
                                 body: JSON.stringify(body) } : {};
            const r = await fetch(url, opt);
            const data = await r.json().catch(() => ({}));
            if (!r.ok) throw new Error(data.error || r.statusText);
            return data;
        }

        function pad(n) { return String(n).padStart(2, "0"); }

        function fmtDur(s) {
            s = Math.round(s);
            if (s < 60) return s + " s";
            const m = Math.floor(s / 60), r = s % 60;
            if (m < 60) return m + " min " + pad(r) + " s";
            return Math.floor(m / 60) + " h " + pad(m % 60) + " min";
        }

        function fmtClock(weekSec) {
            weekSec = ((Math.round(weekSec) % 604800) + 604800) % 604800;
            const d = Math.floor(weekSec / 86400), rem = weekSec % 86400;
            return state.meta.days[d].slice(0, 3) + " " + pad(Math.floor(rem / 3600)) + ":" + pad(Math.floor(rem % 3600 / 60));
        }

        function timeState() {
            const dow = +$("day").value;
            const [h, m] = $("time").value.split(":").map(Number);
            return { dow, h, m, time: pad(h) + ":" + pad(m),
                     interval: dow * 48 + Math.floor((h * 60 + m) / 30),
                     sec: dow * 86400 + h * 3600 + m * 60,
                     degraded: $("degraded").checked };
        }

        function roadColor(ratio) {
            if (ratio > 0.85) return "#22b573";
            if (ratio > 0.65) return "#f2c230";
            if (ratio > 0.45) return "#f08a24";
            return "#d6303b";
        }

        function setStage(id, text, done) {
            $("st-" + id).classList.toggle("done", !!done);
            if (text !== undefined) $("m-" + id).textContent = text;
        }

        function pin(x, y, color, label) {
            return `<g transform="translate(${x},${y})">
                        <path d="M0 0 L-9 -16 A13 13 0 1 1 9 -16 Z" fill="${color}" stroke="#fff" stroke-width="2"></path>
                        <text y="-20" text-anchor="middle" font-size="13" font-weight="700" fill="#fff">${label}</text>
                    </g>`;
        }

        function drawMap(svg, o = {}) {
            const widths = { highway: 9, arterial: 6, local: 3.5 };
            const casings = [], lines = [];
            state.segs.forEach((s) => {
                const a = state.nodes[s.u], b = state.nodes[s.v], w = widths[s.road_type];
                const ratio = o.traffic ? o.traffic.ratio[s.segment_id] : 1;
                const speed = o.traffic ? o.traffic.speed[s.segment_id] : s.free_flow_speed;
                const isHi = o.highlight === s.segment_id;
                const dim = o.highlight !== undefined && !isHi;
                const op = dim ? 0.3 : 1;
                casings.push(`<line x1="${a.x}" y1="${a.y}" x2="${b.x}" y2="${b.y}" stroke="${isHi ? "#1e4fb3" : "#ffffff"}"
                                   stroke-width="${w + (isHi ? 8 : 3)}" stroke-linecap="round" opacity="${op}"></line>`);
                lines.push(`<line x1="${a.x}" y1="${a.y}" x2="${b.x}" y2="${b.y}" stroke="${roadColor(ratio)}"
                                  stroke-width="${w}" stroke-linecap="round" opacity="${op}">
                                <title>S${pad(s.segment_id)} ${s.name} (${s.road_type}) - ${speed} km/h</title>
                            </line>`);
            });

            let overlay = "";
            if (o.alt && o.alt.length > 1) {
                overlay += `<polyline points="${o.alt.map((p) => p.join(",")).join(" ")}" fill="none" stroke="#fff"
                                      stroke-width="9" stroke-linecap="round" stroke-linejoin="round" opacity="0.8"></polyline>
                            <polyline points="${o.alt.map((p) => p.join(",")).join(" ")}" fill="none" stroke="#8a94a8"
                                      stroke-width="4" stroke-dasharray="3 8" stroke-linecap="round" stroke-linejoin="round"></polyline>`;
            }
            if (o.route && o.route.length > 1) {
                const pts = o.route.map((p) => p.join(",")).join(" ");
                overlay += `<polyline points="${pts}" fill="none" stroke="#fff" stroke-width="13" stroke-linecap="round" stroke-linejoin="round"></polyline>
                            <polyline points="${pts}" fill="none" stroke="#1e4fb3" stroke-width="7" stroke-linecap="round" stroke-linejoin="round"></polyline>`;
            }
            let marks = "";
            const m = o.match || {};
            [["origin", "#17a673", "A"], ["dest", "#d6303b", "B"]].forEach(([key, color, label]) => {
                const pt = o[key];
                if (!pt) return;
                const sn = m[key];
                if (sn) {
                    marks += `<line x1="${pt.x}" y1="${pt.y}" x2="${sn.x}" y2="${sn.y}" stroke="${color}" stroke-width="2" stroke-dasharray="4 3"></line>
                              <circle cx="${sn.x}" cy="${sn.y}" r="5.5" fill="#fff" stroke="${color}" stroke-width="3"></circle>`;
                }
                marks += pin(pt.x, pt.y, color, label);
            });

            svg.innerHTML = `
                <rect x="0" y="0" width="1000" height="640" fill="var(--map-bg)"></rect>
                ${casings.join("")}${lines.join("")}
                ${state.nodes.map((n) => `<circle cx="${n.x}" cy="${n.y}" r="3.2" fill="#fff" stroke="#9fb4dd" stroke-width="1.2"></circle>`).join("")}
                ${overlay}${marks}`;
        }

        function redrawTrip() {
            const r = state.route;
            drawMap($("mapTrip"), {
                traffic: state.traffic,
                route: r ? r.fastest.path : null,
                alt: r ? r.shortest.path : null,
                origin: state.origin, dest: state.dest,
                match: r ? r.match : null,
            });
        }

        function redrawSeg() {
            drawMap($("mapSeg"), { traffic: state.traffic, highlight: +$("segSelect").value });
        }

        function barChart(profile, selMinute) {
            const W = 720, H = 200, L = 36, R = 8, T = 14, B = 26;
            const vals = profile.map((p) => p.eta_s);
            const max = Math.max(...vals), min = Math.min(...vals);
            const bw = (W - L - R) / vals.length;
            let g = "";
            [0, 0.5, 1].forEach((f) => {
                const y = T + (H - T - B) * (1 - f);
                g += `<line x1="${L}" x2="${W - R}" y1="${y}" y2="${y}" stroke="#e5edfb"></line>
                      <text x="${L - 5}" y="${y + 4}" text-anchor="end" font-size="10" fill="#5d6b85">${Math.round(max * f / 60 * 10) / 10}m</text>`;
            });
            vals.forEach((v, i) => {
                const h = (H - T - B) * (v / max);
                const x = L + i * bw, y = H - B - h;
                const sel = profile[i].minute === selMinute;
                const best = v === min;
                const col = best ? "#17a673" : roadColor(1 - (v - min) / Math.max(max - min, 1) * 0.9);
                g += `<rect x="${x + 1}" y="${y}" width="${bw - 2}" height="${h}" rx="2" fill="${col}" opacity="${sel ? 1 : 0.75}"
                            ${sel ? 'stroke="#12327a" stroke-width="2"' : ""}>
                          <title>${pad(Math.floor(profile[i].minute / 60))}:${pad(profile[i].minute % 60)} - ${fmtDur(v)}</title>
                      </rect>`;
            });
            for (let h = 0; h <= 24; h += 3) {
                const x = L + (h * 2) * bw;
                g += `<text x="${x}" y="${H - 8}" text-anchor="middle" font-size="10" fill="#5d6b85">${pad(h % 24)}:00</text>`;
            }
            return `<svg viewBox="0 0 ${W} ${H}" style="width: 100%; height: auto;">${g}</svg>`;
        }

        function lineChart(values, freeFlow, selInterval) {
            const W = 900, H = 230, L = 44, R = 10, T = 14, B = 28;
            const max = Math.max(...values, freeFlow) * 1.08, min = 0;
            const sx = (i) => L + (W - L - R) * i / (values.length - 1);
            const sy = (v) => T + (H - T - B) * (1 - (v - min) / (max - min));
            let g = "";
            [0, 0.5, 1].forEach((f) => {
                const v = max * f, y = sy(v);
                g += `<line x1="${L}" x2="${W - R}" y1="${y}" y2="${y}" stroke="#e5edfb"></line>
                      <text x="${L - 5}" y="${y + 4}" text-anchor="end" font-size="10" fill="#5d6b85">${Math.round(v)}s</text>`;
            });
            state.meta.days.forEach((d, i) => {
                const x = sx(i * 48);
                g += `<line x1="${x}" x2="${x}" y1="${T}" y2="${H - B}" stroke="#d3e0f8"></line>
                      <text x="${x + (W - L - R) / 14}" y="${H - 9}" text-anchor="middle" font-size="11" fill="#5d6b85">${d.slice(0, 3)}</text>`;
            });
            g += `<line x1="${L}" x2="${W - R}" y1="${sy(freeFlow)}" y2="${sy(freeFlow)}" stroke="#17a673" stroke-dasharray="5 4"></line>
                  <text x="${W - R}" y="${sy(freeFlow) - 4}" text-anchor="end" font-size="10" fill="#17a673">free-flow ETA</text>`;
            const d = values.map((v, i) => (i ? "L" : "M") + sx(i).toFixed(1) + " " + sy(v).toFixed(1)).join(" ");
            g += `<path d="${d}" fill="none" stroke="#1e4fb3" stroke-width="2" stroke-linejoin="round"></path>
                  <line x1="${sx(selInterval)}" x2="${sx(selInterval)}" y1="${T}" y2="${H - B}" stroke="#d6303b" stroke-width="2"></line>
                  <circle cx="${sx(selInterval)}" cy="${sy(values[selInterval])}" r="5" fill="#d6303b"></circle>`;
            return `<svg viewBox="0 0 ${W} ${H}" style="width: 100%; height: auto;">${g}</svg>`;
        }

        async function refreshTraffic() {
            const t = timeState();
            state.traffic = await api(`api/traffic?interval=${t.interval}&degraded=${t.degraded ? 1 : 0}`);
            redrawTrip();
            redrawSeg();
        }

        async function updateStats() {
            try {
                const s = await api("api/stats");
                $("statsChip").innerHTML = `Requests ${s.requests} &middot; Table lookups ${s.lookups} &middot; Fallbacks ${s.fallbacks}`;
            } catch (e) {}
        }

        function setPick(which) {
            state.pick = which;
            $("pickOrigin").classList.toggle("active", which === "origin");
            $("pickDest").classList.toggle("active", which === "dest");
        }

        function setPoint(x, y) {
            x = Math.max(5, Math.min(995, x));
            y = Math.max(40, Math.min(635, y));
            state[state.pick] = { x: Math.round(x * 10) / 10, y: Math.round(y * 10) / 10 };
            if (state.pick === "origin") setPick("dest");
            state.route = null;
            setStage("user", state.origin && state.dest ? "A \u2192 B selected" : "point set", true);
            redrawTrip();
            if (state.origin && state.dest) computeRoute();
        }

        async function computeRoute() {
            if (!state.origin || !state.dest) return;
            const t = timeState();
            try {
                const r = await api("api/route", { origin: state.origin, dest: state.dest, dow: t.dow, time: t.time, degraded: t.degraded });
                state.route = r;
                redrawTrip();
                renderRoute(r, t);
                animatePipeline(r.pipeline);
                updateStats();
            } catch (e) {
                $("results").innerHTML = `<div class="warn">Could not compute: ${e.message}</div>`;
            }
        }

        function animatePipeline(p) {
            ["match", "client", "agg", "eta", "data", "route"].forEach((k) => setStage(k, undefined, false));
            const steps = [
                ["match", `${p.map_matching_ms} ms`],
                ["client", "request built"],
                ["agg", `${p.aggregator_ms} ms`],
                ["eta", `${p.lookups} lookups`],
                ["data", `${p.segments_read} segments`],
                ["route", `${p.routing_ms} ms`],
            ];
            steps.forEach((s, i) => setTimeout(() => setStage(s[0], s[1], true), i * 140));
        }

        function matchBox(label, m) {
            return `<div class="match-box">
                        <b>${label}</b> &rarr; S${pad(m.segment_id)} &middot; ${m.segment_name}
                        <span class="pill ${m.road_type}">${m.road_type}</span><br>
                        Snapped ${Math.round(m.offset_m)} m from raw GPS point &middot; position ${Math.round(m.t * 100)}% along the segment
                        <div class="conf"><span style="width: ${Math.round(m.confidence * 100)}%;"></span></div>
                    </div>`;
        }

        function renderRoute(r, t) {
            const f = r.fastest, sh = r.shortest;
            $("matchInfo").innerHTML = matchBox("A", r.match.origin) + matchBox("B", r.match.dest);
            const p = r.pipeline;
            $("timing").innerHTML = `<table>
                <tr><td>Map matching</td><td>${p.map_matching_ms} ms</td></tr>
                <tr><td>Routing (+ ETA lookups)</td><td>${p.routing_ms} ms</td></tr>
                <tr><td>Aggregator</td><td>${p.aggregator_ms} ms</td></tr>
                <tr><td><b>Total request</b></td><td><b>${p.total_ms} ms</b></td></tr></table>`;

            const delayPct = f.free_flow_s > 0 ? (f.delay_s / f.free_flow_s * 100) : 0;
            const best = r.profile.reduce((a, b) => (b.eta_s < a.eta_s ? b : a));
            const now = r.profile.find((x) => x.minute === Math.floor((t.h * 60 + t.m) / 30) * 30);
            let tip = `<div class="tip">You are already leaving at a near-optimal time for this trip.</div>`;
            if (now && now.eta_s - best.eta_s > 30) {
                tip = `<div class="tip">Leaving around <b>${pad(Math.floor(best.minute / 60))}:${pad(best.minute % 60)}</b>
                       would take about <b>${fmtDur(best.eta_s)}</b> &mdash; saves <b>${fmtDur(now.eta_s - best.eta_s)}</b> versus this departure.</div>`;
            }
            const warn = r.degraded
                ? `<div class="warn"><b>Degraded mode:</b> the ETA model is unavailable, so free-flow ETAs are served (availability over accuracy).</div>` : "";
            const diff = sh.eta_s - f.eta_s;
            const altText = diff > 5
                ? `Fastest route saves <b class="good">${fmtDur(diff)}</b> compared with the shortest-distance route.`
                : `The shortest-distance route is also the fastest one for this time.`;

            $("results").innerHTML = `
                ${warn}
                <div class="kpis">
                    <div class="kpi primary"><div class="label">Estimated time of arrival</div>
                        <div class="value">${fmtDur(f.eta_s)}</div>
                        <div class="sub">Arrive ${fmtClock(r.depart_sec + f.eta_s)}</div></div>
                    <div class="kpi"><div class="label">Distance</div><div class="value">${f.distance_km.toFixed(2)} km</div>
                        <div class="sub">${f.legs.length} road pieces</div></div>
                    <div class="kpi"><div class="label">Average speed</div><div class="value">${f.avg_speed.toFixed(1)} km/h</div>
                        <div class="sub">predicted</div></div>
                    <div class="kpi"><div class="label">Traffic delay</div>
                        <div class="value ${f.delay_s > 30 ? "bad" : "good"}">+${fmtDur(Math.max(f.delay_s, 0))}</div>
                        <div class="sub">vs free-flow ${fmtDur(f.free_flow_s)} (${delayPct.toFixed(0)}%)</div></div>
                </div>

                <div class="card">
                    <h3>Route comparison</h3>
                    <div class="two">
                        <div class="match-box"><b style="color: #1e4fb3;">Fastest (by predicted ETA)</b><br>
                            ${fmtDur(f.eta_s)} &middot; ${f.distance_km.toFixed(2)} km</div>
                        <div class="match-box"><b style="color: #5d6b85;">Shortest distance</b><br>
                            ${fmtDur(sh.eta_s)} &middot; ${sh.distance_km.toFixed(2)} km</div>
                    </div>
                    <div style="font-size: 13px;">${altText}</div>
                </div>

                <div class="card">
                    <h3>ETA by departure time &middot; ${state.meta.days[t.dow]}</h3>
                    ${barChart(r.profile, Math.floor((t.h * 60 + t.m) / 30) * 30)}
                    ${tip}
                </div>

                <div class="card">
                    <h3>Aggregator &middot; per-segment ETA along the fastest route</h3>
                    <div class="scroll">
                        <table>
                            <thead><tr><th>#</th><th>Segment</th><th>Type</th><th>Interval</th><th>Distance</th>
                                <th>Pred. speed</th><th>ETA</th><th>Free-flow</th></tr></thead>
                            <tbody>
                                ${f.legs.map((l, i) => `<tr>
                                    <td>${i + 1}</td>
                                    <td>S${pad(l.segment_id)} ${l.name}</td>
                                    <td><span class="pill ${l.road_type}">${l.road_type}</span></td>
                                    <td>${l.interval_label}</td>
                                    <td>${l.distance_km.toFixed(2)} km</td>
                                    <td>${l.speed_kmh.toFixed(1)} km/h</td>
                                    <td>${fmtDur(l.eta_s)}${l.fallback ? " *" : ""}</td>
                                    <td>${fmtDur(l.free_flow_s)}</td></tr>`).join("")}
                            </tbody>
                        </table>
                    </div>
                </div>`;
        }

        function randomTrip() {
            const rnd = (a, b) => a + Math.random() * (b - a);
            state.origin = { x: rnd(80, 420), y: rnd(80, 560) };
            state.dest = { x: rnd(580, 920), y: rnd(80, 560) };
            setPick("dest");
            setStage("user", "A \u2192 B selected", true);
            computeRoute();
        }

        function clearTrip() {
            state.origin = state.dest = state.route = null;
            setPick("origin");
            ["user", "match", "client", "agg", "eta", "data", "route"].forEach((k) => setStage(k, undefined, false));
            $("m-user").textContent = "click the map";
            $("matchInfo").innerHTML = "No points yet.";
            $("matchInfo").className = "empty";
            $("timing").innerHTML = "Run a trip to see timings.";
            $("timing").className = "empty";
            $("results").innerHTML = "";
            redrawTrip();
        }

        async function loadSegment() {
            const t = timeState();
            const id = +$("segSelect").value;
            try {
                const r = await api(`api/eta?segment_id=${id}&interval=${t.interval}&degraded=${t.degraded ? 1 : 0}`);
                const s = r.segment;
                const ratio = r.eta_s / r.free_flow_s;
                $("segResults").innerHTML = `
                    <div class="code">
                        &gt;&gt;&gt; f(segment_id=${id}, interval_within_week=${r.interval})  <span style="color: #7fe0b3;">#  ${r.interval_label}</span><br>
                        ${r.eta_s.toFixed(1)}  <span style="color: #8fa6d6;">&nbsp;# seconds${r.fallback ? " (free-flow fallback)" : ""}</span>
                    </div>
                    <div class="kpis">
                        <div class="kpi primary"><div class="label">Segment ETA</div><div class="value">${fmtDur(r.eta_s)}</div>
                            <div class="sub">${s.name} &middot; ${s.distance_km} km</div></div>
                        <div class="kpi"><div class="label">Predicted speed</div><div class="value">${r.speed_kmh.toFixed(1)} km/h</div>
                            <div class="sub">limit ${s.speed_limit} &middot; free-flow ${s.free_flow_speed}</div></div>
                        <div class="kpi"><div class="label">Free-flow ETA</div><div class="value">${fmtDur(r.free_flow_s)}</div>
                            <div class="sub">no traffic</div></div>
                        <div class="kpi"><div class="label">Slowdown</div>
                            <div class="value ${ratio > 1.3 ? "bad" : "good"}">${ratio.toFixed(2)}x</div>
                            <div class="sub">${s.road_type}</div></div>
                    </div>
                    <div class="card">
                        <h3>Weekly ETA profile &middot; S${pad(id)} ${s.name}</h3>
                        ${lineChart(r.profile, r.free_flow_s, r.interval)}
                    </div>`;
                redrawSeg();
                updateStats();
            } catch (e) {
                $("segResults").innerHTML = `<div class="warn">${e.message}</div>`;
            }
        }

        async function loadModel() {
            const m = await api("api/model");
            const x = m.metrics;
            const qps = Math.round(10e6 * 8 / 86400);
            $("modelView").className = "";
            $("modelView").innerHTML = `
                <div class="kpis">
                    <div class="kpi"><div class="label">Travel rows</div><div class="value">${m.travel_rows.toLocaleString()}</div>
                        <div class="sub">${x.train_rows.toLocaleString()} train / ${x.test_rows.toLocaleString()} test</div></div>
                    <div class="kpi"><div class="label">Speed MAE</div><div class="value">${x.mae_speed.toFixed(2)} km/h</div>
                        <div class="sub">RMSE ${x.rmse_speed.toFixed(2)}</div></div>
                    <div class="kpi"><div class="label">R&sup2; (test)</div><div class="value">${x.r2.toFixed(3)}</div>
                        <div class="sub">hold-out split</div></div>
                    <div class="kpi"><div class="label">ETA error</div><div class="value">${x.mae_eta_s.toFixed(1)} s</div>
                        <div class="sub">MAPE ${x.mape_eta.toFixed(1)}% per segment</div></div>
                </div>
                <div class="two">
                    <div class="card">
                        <h3>High-level design</h3>
                        <ol class="steps">
                            <li><b>Data</b> &ndash; map data and travel data made usable for training.</li>
                            <li><b>Model</b> &ndash; parametrisation: eta = f(segment_id, interval_within_week).</li>
                            <li><b>Training</b> &ndash; features: ${m.features.join(", ")}.</li>
                            <li><b>Validation</b> &ndash; 80 / 20 train-test split.</li>
                            <li><b>Deployment</b> &ndash; predictions pre-computed into a ${m.n_segments} &times; 336 table (${m.table_kb} KB).</li>
                        </ol>
                    </div>
                    <div class="card">
                        <h3>Non-functional requirements</h3>
                        <ol class="steps">
                            <li><b>Scale 10M DAU</b> &ndash; assuming ~8 ETA requests per user per day &asymp; ${qps.toLocaleString()} req/s on average
                                (peak several times higher). Each request is an O(1) table lookup per segment.</li>
                            <li><b>Available &gt;&gt;&gt; Consistent (AP)</b> &ndash; the table is read-only between retrains, so it can be
                                replicated to every region; stale-but-available beats fresh-but-down.</li>
                            <li><b>Graceful fallback</b> &ndash; if prediction fails, free-flow ETA is served
                                (try the outage switch in the toolbar).</li>
                        </ol>
                    </div>
                </div>
                <div class="two">
                    <div class="card">
                        <h3>Map data &middot; segment_id, distance, speed_limit, free_flow_speed</h3>
                        <div class="scroll"><table>
                            <thead><tr><th>segment_id</th><th>name</th><th>type</th><th>distance</th><th>limit</th><th>free-flow</th></tr></thead>
                            <tbody>${state.segs.map((s) => `<tr><td>${s.segment_id}</td><td>${s.name}</td>
                                <td><span class="pill ${s.road_type}">${s.road_type}</span></td>
                                <td>${s.distance_km} km</td><td>${s.speed_limit}</td><td>${s.free_flow_speed}</td></tr>`).join("")}</tbody>
                        </table></div>
                    </div>
                    <div class="card">
                        <h3>Travel data &middot; year, num_car, avg_speed, interval (random sample)</h3>
                        <div class="scroll"><table>
                            <thead><tr><th>segment_id</th><th>year</th><th>interval</th><th>num_car</th><th>avg_speed</th></tr></thead>
                            <tbody>${m.sample.map((s) => `<tr><td>${s.segment_id}</td><td>${s.year}</td>
                                <td>${s.interval} (${s.interval_label})</td><td>${s.num_car}</td><td>${s.avg_speed} km/h</td></tr>`).join("")}</tbody>
                        </table></div>
                    </div>
                </div>`;
        }

        function bindMapClick(svg, handler) {
            svg.addEventListener("click", (e) => {
                const pt = svg.createSVGPoint();
                pt.x = e.clientX;
                pt.y = e.clientY;
                const p = pt.matrixTransform(svg.getScreenCTM().inverse());
                handler(p.x, p.y);
            });
        }

        async function onTimeChange() {
            await refreshTraffic();
            if (state.origin && state.dest) computeRoute();
            if ($("tab-segment").classList.contains("active")) loadSegment();
            updateStats();
        }

        async function init() {
            const data = await api("api/map");
            state.meta = data.meta;
            state.nodes = data.nodes;
            state.segs = data.segments;
            $("deployBadge").textContent = "Model year " + state.meta.deploy_year;

            $("day").innerHTML = state.meta.days.map((d, i) => `<option value="${i}">${d}</option>`).join("");
            $("segSelect").innerHTML = state.segs.map((s) =>
                `<option value="${s.segment_id}">S${pad(s.segment_id)} - ${s.name} (${s.road_type}, ${s.distance_km} km)</option>`).join("");
            const busiest = state.segs.reduce((a, b) => (Math.hypot(b.mx - 450, b.my - 315) < Math.hypot(a.mx - 450, a.my - 315) ? b : a));
            $("segSelect").value = busiest.segment_id;

            const now = new Date();
            $("day").value = (now.getDay() + 6) % 7;
            $("time").value = pad(now.getHours()) + ":" + pad(now.getMinutes());

            bindMapClick($("mapTrip"), setPoint);
            $("pickOrigin").onclick = () => setPick("origin");
            $("pickDest").onclick = () => setPick("dest");
            $("btnRandom").onclick = randomTrip;
            $("btnClear").onclick = clearTrip;
            $("btnSwap").onclick = () => {
                [state.origin, state.dest] = [state.dest, state.origin];
                if (state.origin && state.dest) computeRoute(); else redrawTrip();
            };
            $("btnNow").onclick = () => {
                const n = new Date();
                $("day").value = (n.getDay() + 6) % 7;
                $("time").value = pad(n.getHours()) + ":" + pad(n.getMinutes());
                onTimeChange();
            };
            document.querySelectorAll("[data-day]").forEach((b) => {
                b.onclick = () => { $("day").value = b.dataset.day; $("time").value = b.dataset.time; onTimeChange(); };
            });
            ["day", "time", "degraded"].forEach((id) => $(id).addEventListener("change", onTimeChange));
            $("segSelect").addEventListener("change", loadSegment);
            $("btnSeg").onclick = loadSegment;

            document.querySelectorAll(".tabs button").forEach((b) => {
                b.onclick = () => {
                    document.querySelectorAll(".tabs button").forEach((x) => x.classList.toggle("active", x === b));
                    document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.id === "tab-" + b.dataset.tab));
                    if (b.dataset.tab === "segment") loadSegment();
                    if (b.dataset.tab === "model" && !state.modelLoaded) { state.modelLoaded = true; loadModel(); }
                };
            });

            setStage("data", state.segs.length + " segments", true);
            await refreshTraffic();
            updateStats();
        }

        init();
    </script>
</body>
</html>
"""


def free_port(start=5050):
    for p in range(start, start + 60):
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", p)) != 0:
                return p
    return start


def launch():
    port = free_port()
    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False),
        daemon=True,
    ).start()
    time.sleep(1.5)
    try:
        from google.colab import output
        print(f"ETA for Maps is running on port {port} - interface below.")
        output.serve_kernel_port_as_iframe(port, height="1250")
    except ImportError:
        print(f"Not in Colab - open http://127.0.0.1:{port} in your browser.")


if __name__ == "__main__":
    launch()