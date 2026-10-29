'''
Ride Sharing Platform

1. Requirement Analysis
1B . Functional Requirements
- Rider should be able to get a fare estimation based upon location.
- Rider should be able to request a ride.
- Rider should be able to [choose/request] different category.
- Drivers should be able to accept/decline.
- Rider matching with driver.
- Real time tracking of rider/driver.
- Rider should be able to pay.

1B. Non-functional Requirements
- Scale: 10M Users + 20K Drivers
- CAP Theorem:  User → Available & Driver → Consistent

2. Core Entity
- Rider
- Driver
- Fare
- Ride

3. API Creation
3A. User Side :
Fare Estimation : GET /v1/api/fares?location={long+lat} → `List<Fare>` with Request ID
Request Ride : POST /v1/api/rides/request {request ID}
GET /v1/api/rides/{request ID} → Details
POST /v1/api/rides/History → Past
POST /v1/api/rides/{ride ID}/cancel → Cancel
POST /v1/api/rides/{driver ID}

3B. Driver Side : 
Location : WS /v1/driver/location → Update driver location
Accept Ride : POST /v1/api/ride/{ridesID}/accept/deny → Ride ID
Ride Start : POST /v1/api/ride/{ride_ID}/start
Ride End : POST /v1/api/ride/{ride_ID}/end

4. High level design 
user -> api gateway -> sever ( users & drivers ) -> database ( rideDB & paymentDB )

5. low level design 
uploaded image
'''
import threading, time, math, random, uuid, hmac, hashlib, base64, json, itertools
from collections import deque
from flask import Flask, request, jsonify, Response, g
from werkzeug.serving import make_server

PORT = 8050
SIM_KM_PER_TICK = 0.35
OFFER_TTL = 25
SECRET = uuid.uuid4().bytes
BOUNDS = (27.63, 27.76, 85.26, 85.44)

PLACES = {
    "Thamel": (27.7150, 85.3100), "Pulchowk": (27.6820, 85.3190),
    "Baneshwor": (27.6890, 85.3420), "Koteshwor": (27.6780, 85.3490),
    "Boudha": (27.7210, 85.3620), "Patan": (27.6670, 85.3250),
    "Bhaktapur": (27.6710, 85.4290), "Kalanki": (27.6930, 85.2810),
    "Balaju": (27.7350, 85.3040), "Airport": (27.6970, 85.3590),
    "Maitighar": (27.6940, 85.3220), "Swayambhu": (27.7150, 85.2900),
}
CATS = {
    "Bike":    dict(base=40,  km=22, min=1, seats=1, icon="Bike"),
    "Economy": dict(base=80,  km=40, min=2, seats=3, icon="Economy"),
    "Comfort": dict(base=120, km=55, min=3, seats=4, icon="Comfort"),
    "XL":      dict(base=160, km=70, min=4, seats=6, icon="XL"),
}


LOCK = threading.RLock()
DRIVERS, RIDES, QUOTES, PAYMENTS, NOTES = {}, {}, {}, [], {}
EVENTS = deque(maxlen=40)
NID = itertools.count(1)
HITS = {}


def hav(a, b):
    p = math.pi / 180
    x = math.sin((b["lat"] - a["lat"]) * p / 2) ** 2 + math.cos(a["lat"] * p) * \
        math.cos(b["lat"] * p) * math.sin((b["lng"] - a["lng"]) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(x))


def pt(lat, lng, name=""):
    return dict(lat=float(lat), lng=float(lng), name=name)


def event(kind, **d):
    EVENTS.appendleft(dict(t=time.strftime("%H:%M:%S"), type=kind, **d))


def notify(to, msg):
    box = NOTES.setdefault(to, [])
    box.append(dict(id=next(NID), ch=random.choice(["Push", "SMS", "Email"]), msg=msg))
    del box[:-30]


def seed_drivers():
    first = ["Ram", "Sita", "Hari", "Gita", "Bikash", "Anita", "Suman", "Rita", "Prakash", "Sunita", "Dipak", "Mina"]
    last = ["Shrestha", "Thapa"]
    places = list(PLACES.values())
    for i in range(24):
        la, ln = random.choice(places)
        did = f"D{i + 1:02d}"
        DRIVERS[did] = dict(
            id=did, name=f"{first[i % 12]} {last[i // 12]}", cat=list(CATS)[i % 4],
            plate=f"Ba {random.randint(1, 30)} Pa {random.randint(1000, 9999)}",
            lat=la + random.uniform(-.01, .01), lng=ln + random.uniform(-.01, .01),
            rating=round(random.uniform(4.2, 4.9), 1), acc=round(random.uniform(.6, .95), 2),
            status="AVAILABLE", manual=False, trips=0, earned=0)


def fare_for(cat, dist, mins, surge):
    c = CATS[cat]
    return int(round((c["base"] + c["km"] * dist + c["min"] * mins) * surge / 5) * 5)


def estimate(p, d):
    dist = round(hav(p, d) * 1.3, 1)
    mins = max(3, round(dist / 22 * 60))
    with LOCK:
        demand = sum(r["state"] == "MATCHING" for r in RIDES.values())
        near = [x for x in DRIVERS.values() if x["status"] == "AVAILABLE" and hav(x, p) < 5]
        surge = round(min(2.0, max(1.0, (demand + 1) / (len(near) + 1) * 1.5)), 1)
        fares = []
        for c, v in CATS.items():
            same = [hav(x, p) for x in near if x["cat"] == c]
            fares.append(dict(category=c, fare=fare_for(c, dist, mins, surge), seats=v["seats"],
                              eta=round(min(same) / 22 * 60) + 1 if same else None))
    return dist, mins, surge, fares


def offer_next(r):
    best = None
    for d in DRIVERS.values():
        if d["status"] != "AVAILABLE" or d["cat"] != r["cat"] or d["id"] in r["declined"]:
            continue
        km = hav(d, r["pickup"])
        if km > 8:
            continue
        score = .5 * (1 - km / 8) + .3 * (d["rating"] - 3.5) / 1.5 + .2 * d["acc"]
        if not best or score > best[0]:
            best = (score, d, km)
    if not best or len(r["declined"]) >= 5:
        r["state"], r["offer"] = "NO_DRIVERS", None
        r["log"].append("No drivers available nearby")
        notify(r["rider"], "No drivers found. Please try again.")
        return
    score, d, km = best
    d["status"] = "OFFERED"
    r["offer"] = dict(driver=d["id"], exp=time.time() + OFFER_TTL, at=time.time() + random.uniform(2, 5))
    r["log"].append(f"Offered to {d['name']} ({km:.1f} km away, score {score:.2f})")
    notify(d["id"], f"New ride request: {r['pickup']['name']} to {r['drop']['name']}")


def accept(r):
    d = DRIVERS[r["offer"]["driver"]]
    d["status"], r["driver"], r["offer"], r["state"] = "BUSY", d["id"], None, "DRIVER_ASSIGNED"
    r["log"].append(f"{d['name']} accepted and is on the way")
    notify(r["rider"], f"{d['name']} ({d['plate']}) is arriving")
    event("ride", id=r["id"], what="accepted")


def decline(r, why):
    d = DRIVERS[r["offer"]["driver"]]
    d["status"] = "AVAILABLE"
    r["declined"].append(d["id"])
    r["log"].append(f"{d['name']} {why} - reassigning")
    r["offer"] = None
    event("ride", id=r["id"], what="reassign")
    offer_next(r)


def start(r):
    r["state"] = "IN_PROGRESS"
    r["log"].append("Trip started")
    notify(r["rider"], "Your trip has started")
    event("ride", id=r["id"], what="started")


def end(r):
    d = DRIVERS[r["driver"]]
    r["state"] = "COMPLETED"
    d["status"] = "AVAILABLE"
    d["trips"] += 1
    d["earned"] += int(r["fare"] * .8)
    r["log"].append("Trip completed")
    notify(r["rider"], f"Trip completed. Please pay NPR {r['fare']}")
    event("ride", id=r["id"], what="completed")


def move(d, target):
    dist = hav(d, target)
    if dist <= SIM_KM_PER_TICK:
        d["lat"], d["lng"] = target["lat"], target["lng"]
        return True
    f = SIM_KM_PER_TICK / dist
    d["lat"] += (target["lat"] - d["lat"]) * f
    d["lng"] += (target["lng"] - d["lng"]) * f
    return False


def step(r, t):
    s = r["state"]
    if s == "MATCHING" and r["offer"]:
        o = r["offer"]
        d = DRIVERS[o["driver"]]
        if not d["manual"] and t >= o["at"]:
            accept(r) if random.random() < d["acc"] else decline(r, "declined")
        elif t > o["exp"]:
            decline(r, "did not respond")
    elif s == "DRIVER_ASSIGNED":
        if move(DRIVERS[r["driver"]], r["pickup"]):
            r["state"], r["arrived"] = "DRIVER_ARRIVED", t
            r["log"].append("Driver arrived at pickup")
            notify(r["rider"], "Your driver has arrived")
    elif s == "DRIVER_ARRIVED":
        if not DRIVERS[r["driver"]]["manual"] and t - r["arrived"] > 3:
            start(r)
    elif s == "IN_PROGRESS":
        d = DRIVERS[r["driver"]]
        if move(d, r["drop"]):
            r["at_dest"] = True
            if not d["manual"]:
                end(r)


RUN = globals().get("RUN") or {"on": False}
RUN["on"] = False


def tick_loop(run):
    while run["on"]:
        time.sleep(1)
        with LOCK:
            for r in list(RIDES.values()):
                try:
                    step(r, time.time())
                except Exception as e:
                    print("tick error:", e)
            for d in DRIVERS.values():
                if d["status"] in ("AVAILABLE", "OFFERED"):
                    d["lat"] = min(BOUNDS[1], max(BOUNDS[0], d["lat"] + random.uniform(-.0004, .0004)))
                    d["lng"] = min(BOUNDS[3], max(BOUNDS[2], d["lng"] + random.uniform(-.0004, .0004)))
            if random.random() < .3:
                d = random.choice(list(DRIVERS.values()))
                event("location", driver=d["id"], lat=round(d["lat"], 4), lng=round(d["lng"], 4))


def ride_view(r):
    v = {k: r[k] for k in ("id", "state", "cat", "fare", "dist", "mins", "surge", "pickup", "drop",
                           "paid", "fee", "at_dest", "created")}
    v["log"] = r["log"][-6:]
    if r["driver"]:
        d = DRIVERS[r["driver"]]
        tgt = r["pickup"] if r["state"] in ("DRIVER_ASSIGNED", "DRIVER_ARRIVED") else r["drop"]
        v["driver"] = dict(name=d["name"], plate=d["plate"], rating=d["rating"], lat=d["lat"], lng=d["lng"],
                           eta=max(1, round(hav(d, tgt) / 22 * 60)))
    return v


def sign(sub):
    body = base64.urlsafe_b64encode(json.dumps({"sub": sub, "exp": time.time() + 3600}).encode()).decode()
    return body + "." + hmac.new(SECRET, body.encode(), hashlib.sha256).hexdigest()[:24]


def verify(tok):
    try:
        body, sig = tok.split(".")
        ok = hmac.compare_digest(sig, hmac.new(SECRET, body.encode(), hashlib.sha256).hexdigest()[:24])
        data = json.loads(base64.urlsafe_b64decode(body))
        return data["sub"] if ok and data["exp"] > time.time() else None
    except Exception:
        return None


app = Flask(__name__)


@app.before_request
def gateway():
    if not request.path.startswith("/v1/") or request.path == "/v1/api/guest":
        return
    sub = verify(request.headers.get("Authorization", "")[7:])
    if not sub:
        return jsonify(error="unauthorized"), 401
    g.guest = sub
    q = HITS.setdefault(sub, deque())
    while q and q[0] < time.time() - 10:
        q.popleft()
    if len(q) >= 150:
        return jsonify(error="rate limit exceeded"), 429
    q.append(time.time())


def err(msg, code=400):
    return jsonify(error=msg), code


@app.get("/v1/api/guest")
def guest():
    gid = request.args.get("id") or "G" + uuid.uuid4().hex[:8]
    return jsonify(id=gid, token=sign(gid))


@app.get("/v1/api/fares")
def fares():
    try:
        p = pt(*map(float, request.args["pickup"].split(",")), name=request.args.get("pn", "Pickup"))
        d = pt(*map(float, request.args["drop"].split(",")), name=request.args.get("dn", "Drop-off"))
    except Exception:
        return err("pickup and drop required as lat,lng")
    for x in (p, d):
        if not (BOUNDS[0] <= x["lat"] <= BOUNDS[1] and BOUNDS[2] <= x["lng"] <= BOUNDS[3]):
            return err("Location is outside the service area")
    if hav(p, d) < .3:
        return err("Pickup and drop-off are too close")
    dist, mins, surge, fl = estimate(p, d)
    rid = "Q" + uuid.uuid4().hex[:8]
    QUOTES[rid] = dict(p=p, d=d, dist=dist, mins=mins, surge=surge, fares=fl, exp=time.time() + 300)
    return jsonify(request_id=rid, distance_km=dist, duration_min=mins, surge=surge, fares=fl)


@app.get("/v1/api/drivers/nearby")
def nearby():
    return jsonify([dict(id=d["id"], name=d["name"], cat=d["cat"], lat=d["lat"], lng=d["lng"], status=d["status"])
                    for d in DRIVERS.values()])


@app.post("/v1/api/rides/request")
def ride_request():
    b = request.json or {}
    q = QUOTES.get(b.get("request_id"))
    if not q or q["exp"] < time.time():
        return err("Fare quote expired - please estimate again")
    if b.get("category") not in CATS:
        return err("Choose a category")
    with LOCK:
        if any(r["rider"] == g.guest and r["state"] in ("MATCHING", "DRIVER_ASSIGNED", "DRIVER_ARRIVED", "IN_PROGRESS")
               for r in RIDES.values()):
            return err("You already have an active ride", 409)
        fare = next(f["fare"] for f in q["fares"] if f["category"] == b["category"])
        r = dict(id="R" + uuid.uuid4().hex[:6].upper(), rider=g.guest, pickup=q["p"], drop=q["d"], cat=b["category"],
                 fare=fare, dist=q["dist"], mins=q["mins"], surge=q["surge"], state="MATCHING", driver=None,
                 offer=None, declined=[], log=["Ride requested - finding a driver"], created=time.time(),
                 paid=None, fee=0, at_dest=False)
        RIDES[r["id"]] = r
        event("ride", id=r["id"], what="requested")
        offer_next(r)
        return jsonify(ride_view(r))


@app.get("/v1/api/rides/active")
def active():
    mine = [r for r in RIDES.values() if r["rider"] == g.guest and (
        r["state"] in ("MATCHING", "DRIVER_ASSIGNED", "DRIVER_ARRIVED", "IN_PROGRESS") or
        (r["state"] == "COMPLETED" and not r["paid"]))]
    return jsonify(ride_view(mine[-1]) if mine else {})


@app.get("/v1/api/rides/<rid>")
def ride_get(rid):
    r = RIDES.get(rid)
    return jsonify(ride_view(r)) if r else err("Ride not found", 404)


@app.route("/v1/api/rides/History", methods=["GET", "POST"])
def history():
    mine = [ride_view(r) for r in RIDES.values() if r["rider"] == g.guest]
    return jsonify(sorted(mine, key=lambda x: -x["created"]))


@app.post("/v1/api/rides/<rid>/cancel")
def cancel(rid):
    with LOCK:
        r = RIDES.get(rid)
        if not r or r["rider"] != g.guest:
            return err("Ride not found", 404)
        if r["state"] not in ("MATCHING", "DRIVER_ASSIGNED", "DRIVER_ARRIVED"):
            return err("This ride can no longer be cancelled")
        did = r["offer"]["driver"] if r["offer"] else r["driver"]
        if did:
            DRIVERS[did]["status"] = "AVAILABLE"
            notify(did, "Ride was cancelled by the rider")
        if r["driver"]:
            r["fee"] = 50
            PAYMENTS.append(dict(ride=rid, rider=g.guest, amount=50, method="wallet", type="cancel_fee"))
        r["state"], r["offer"] = "CANCELLED", None
        r["log"].append("Ride cancelled")
        event("ride", id=rid, what="cancelled")
        return jsonify(ride_view(r))


@app.get("/v1/driver/<did>/state")
def driver_state(did):
    d = DRIVERS.get(did)
    if not d:
        return err("Driver not found", 404)
    with LOCK:
        offer = next((r for r in RIDES.values() if r["offer"] and r["offer"]["driver"] == did), None)
        ride = next((r for r in RIDES.values() if r["driver"] == did and
                     r["state"] in ("DRIVER_ASSIGNED", "DRIVER_ARRIVED", "IN_PROGRESS")), None)
        return jsonify(driver=d, notes=NOTES.get(did, [])[-6:],
                       offer=dict(ride=ride_view(offer), left=max(0, round(offer["offer"]["exp"] - time.time()))) if offer else None,
                       ride=ride_view(ride) if ride else None)


@app.post("/v1/driver/<did>/status")
def driver_status(did):
    d, b = DRIVERS.get(did), request.json or {}
    if not d:
        return err("Driver not found", 404)
    with LOCK:
        if "manual" in b:
            d["manual"] = bool(b["manual"])
        if "online" in b and d["status"] in ("AVAILABLE", "OFFLINE"):
            d["status"] = "AVAILABLE" if b["online"] else "OFFLINE"
    return jsonify(d)


@app.post("/v1/driver/location")
def driver_location():
    b = request.json or {}
    d = DRIVERS.get(b.get("driver_id"))
    if not d:
        return err("Driver not found", 404)
    with LOCK:
        d["lat"], d["lng"] = float(b["lat"]), float(b["lng"])
    event("location", driver=d["id"], lat=round(d["lat"], 4), lng=round(d["lng"], 4))
    return jsonify(ok=True)


def driver_action(rid, fn):
    did = (request.json or {}).get("driver_id")
    with LOCK:
        r = RIDES.get(rid)
        if not r:
            return err("Ride not found", 404)
        res = fn(r, did)
        return res if res else jsonify(ride_view(r))


def _accept(r, did):
    if r["state"] != "MATCHING" or not r["offer"] or r["offer"]["driver"] != did:
        return err("Offer is no longer valid", 409)
    accept(r)


def _deny(r, did):
    if r["state"] != "MATCHING" or not r["offer"] or r["offer"]["driver"] != did:
        return err("Offer is no longer valid", 409)
    decline(r, "declined")


def _start(r, did):
    if r["driver"] != did or r["state"] != "DRIVER_ARRIVED":
        return err("Reach the pickup point first", 409)
    start(r)


def _end(r, did):
    if r["driver"] != did or r["state"] != "IN_PROGRESS" or not r["at_dest"]:
        return err("Reach the drop-off point first", 409)
    end(r)


for _name, _fn in (("accept", _accept), ("deny", _deny), ("start", _start), ("end", _end)):
    app.add_url_rule(f"/v1/api/ride/<rid>/{_name}", f"ride_{_name}",
                     (lambda f: lambda rid: driver_action(rid, f))(_fn), methods=["POST"])


@app.post("/v1/api/payments/pay")
def pay():
    b = request.json or {}
    with LOCK:
        r = RIDES.get(b.get("ride_id"))
        if not r or r["rider"] != g.guest or r["state"] != "COMPLETED":
            return err("Nothing to pay for")
        if r["paid"]:
            return err("Already paid", 409)
        m = b.get("method")
        if m not in ("cash", "wallet", "card"):
            return err("Choose a payment method")
        if m == "card" and not (str(b.get("card", "")).replace(" ", "").isdigit() and len(str(b["card"]).replace(" ", "")) == 16):
            return err("Enter a valid 16-digit card number")
        r["paid"] = m
        PAYMENTS.append(dict(ride=r["id"], rider=g.guest, amount=r["fare"], method=m, type="payment"))
        event("payment", ride=r["id"], amount=r["fare"], method=m)
        return jsonify(ride_view(r))


@app.post("/v1/api/payments/refund")
def refund():
    with LOCK:
        r = RIDES.get((request.json or {}).get("ride_id"))
        if not r or r["rider"] != g.guest or r["paid"] in (None, "cash", "refunded"):
            return err("This ride cannot be refunded")
        PAYMENTS.append(dict(ride=r["id"], rider=g.guest, amount=-r["fare"], method=r["paid"], type="refund"))
        r["paid"] = "refunded"
        return jsonify(ride_view(r))


@app.get("/v1/api/notifications")
def notifications():
    key = request.args.get("driver_id") or g.guest
    return jsonify(NOTES.get(key, [])[-8:])


@app.get("/v1/api/analytics")
def analytics():
    with LOCK:
        rides = list(RIDES.values())
        done = [r for r in rides if r["state"] == "COMPLETED"]
        by_cat = {c: dict(rides=sum(r["cat"] == c for r in done), revenue=sum(r["fare"] for r in done if r["cat"] == c))
                  for c in CATS}
        mix = {}
        for p in PAYMENTS:
            mix[p["method"]] = mix.get(p["method"], 0) + 1
        top = sorted(DRIVERS.values(), key=lambda d: -d["earned"])[:5]
        return jsonify(
            total=len(rides), completed=len(done), cancelled=sum(r["state"] == "CANCELLED" for r in rides),
            no_driver=sum(r["state"] == "NO_DRIVERS" for r in rides),
            revenue=sum(p["amount"] for p in PAYMENTS), avg_fare=round(sum(r["fare"] for r in done) / len(done)) if done else 0,
            online=sum(d["status"] != "OFFLINE" for d in DRIVERS.values()), by_cat=by_cat, pay_mix=mix,
            drivers=[dict(name=d["name"], trips=d["trips"], earned=d["earned"], rating=d["rating"], acc=d["acc"]) for d in top],
            events=list(EVENTS)[:15])


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Safar</title>
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
            background: #fff;
            color: #000;
        }
        header {
            height: 56px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 24px;
            background: #000;
            color: #fff;
        }
        .brand { font-size: 26px; font-weight: 700; letter-spacing: -1px; }
        nav { display: flex; gap: 4px; }
        nav button {
            background: transparent;
            border: 0;
            color: #fff;
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
        }
        nav button.on { background: #fff; color: #000; }
        .tab { display: none; }
        .tab.on { display: block; }
        .stage { position: relative; height: calc(100vh - 56px); min-height: 640px; overflow: hidden; }
        canvas { position: absolute; inset: 0; width: 100%; height: 100%; }
        .sheet {
            position: absolute;
            top: 16px;
            left: 16px;
            bottom: 16px;
            width: 400px;
            overflow-y: auto;
            background: #fff;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 4px 16px rgba(0, 0, 0, .2);
        }
        h1 { margin: 0 0 18px; font-size: 30px; font-weight: 700; letter-spacing: -.5px; }
        h2 { margin: 0 0 4px; font-size: 22px; font-weight: 700; }
        h4 { margin: 22px 0 8px; font-size: 14px; font-weight: 700; }
        .sub { color: #545454; font-size: 14px; }
        .route { position: relative; padding-left: 28px; }
        .route::before {
            content: "";
            position: absolute;
            left: 8px;
            top: 24px;
            bottom: 24px;
            width: 2px;
            background: #000;
        }
        .route .dot, .route .sq { position: absolute; left: 3px; width: 12px; height: 12px; background: #000; }
        .route .dot { top: 16px; border-radius: 50%; }
        .route .sq { bottom: 16px; }
        select {
            width: 100%;
            margin-bottom: 8px;
            padding: 13px 12px;
            border: 0;
            border-radius: 8px;
            background: #f3f3f3;
            font-size: 16px;
            color: #000;
        }
        input[type=text] {
            width: 100%;
            padding: 13px 12px;
            border: 0;
            border-radius: 8px;
            background: #f3f3f3;
            font-size: 16px;
        }
        .btn {
            display: block;
            width: 100%;
            margin-top: 14px;
            padding: 15px;
            border: 0;
            border-radius: 8px;
            background: #000;
            color: #fff;
            font-size: 16px;
            font-weight: 500;
            cursor: pointer;
        }
        .btn.light { background: #eee; color: #000; }
        .btn:disabled { background: #e2e2e2; color: #9a9a9a; cursor: not-allowed; }
        .row { display: flex; gap: 10px; }
        .row .btn { flex: 1; }
        .opt {
            display: flex;
            align-items: center;
            gap: 14px;
            padding: 12px;
            margin-top: 6px;
            border: 2px solid transparent;
            border-radius: 10px;
            cursor: pointer;
        }
        .opt:hover { background: #f6f6f6; }
        .opt.sel { border-color: #000; background: #f6f6f6; }
        .opt .ic { font-size: 34px; width: 48px; text-align: center; }
        .opt .mid { flex: 1; }
        .opt .mid b { font-size: 17px; }
        .opt .price { font-size: 17px; font-weight: 700; }
        .link { background: none; border: 0; padding: 0; font-size: 14px; text-decoration: underline; cursor: pointer; }
        .bar { height: 4px; margin: 14px 0; background: #e2e2e2; border-radius: 2px; overflow: hidden; }
        .bar i { display: block; height: 100%; width: 35%; background: #000; animation: slide 1.2s infinite ease-in-out; }
        .bar.still i { animation: none; width: 100%; }
        @keyframes slide { from { margin-left: -35%; } to { margin-left: 100%; } }
        .person { display: flex; align-items: center; gap: 14px; padding: 14px 0; border-top: 1px solid #eee; border-bottom: 1px solid #eee; }
        .avatar {
            width: 52px;
            height: 52px;
            border-radius: 50%;
            background: #000;
            color: #fff;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 20px;
            font-weight: 700;
        }
        .plate { display: inline-block; padding: 2px 8px; border: 2px solid #000; border-radius: 6px; font-weight: 700; font-size: 13px; }
        .trip { display: flex; align-items: center; gap: 12px; padding: 8px 0; font-size: 15px; }
        .trip i { width: 10px; height: 10px; background: #000; }
        .trip i.c { border-radius: 50%; }
        ul.log { margin: 12px 0 0; padding-left: 18px; font-size: 13px; color: #545454; }
        .seg { display: flex; gap: 8px; margin-top: 14px; }
        .seg button {
            flex: 1;
            padding: 12px;
            border: 2px solid #eee;
            border-radius: 8px;
            background: #fff;
            font-size: 15px;
            cursor: pointer;
        }
        .seg button.on { border-color: #000; background: #f6f6f6; font-weight: 700; }
        .amt { font-size: 40px; font-weight: 700; margin: 6px 0 4px; }
        .tiles { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-top: 14px; }
        .tile { background: #f3f3f3; border-radius: 8px; padding: 12px; text-align: center; }
        .tile b { display: block; font-size: 18px; }
        .tile span { font-size: 12px; color: #545454; }
        .switch { display: flex; justify-content: space-between; align-items: center; padding: 12px 0; border-bottom: 1px solid #eee; font-size: 15px; }
        .switch input { width: 20px; height: 20px; accent-color: #000; }
        .offer { margin-top: 16px; padding: 16px; border: 2px solid #000; border-radius: 10px; }
        .note { padding: 8px 0; border-bottom: 1px solid #f0f0f0; font-size: 13px; color: #333; }
        table { width: 100%; border-collapse: collapse; font-size: 14px; }
        td, th { padding: 9px 4px; border-bottom: 1px solid #eee; text-align: left; }
        .page { max-width: 1100px; margin: 0 auto; padding: 28px 24px; }
        .stats { display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; margin-bottom: 24px; }
        .stat { background: #f3f3f3; border-radius: 10px; padding: 16px; }
        .stat b { display: block; font-size: 26px; }
        .cols { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
        #toasts { position: fixed; right: 16px; bottom: 16px; display: flex; flex-direction: column; gap: 8px; z-index: 9; }
        .toast { background: #000; color: #fff; padding: 12px 16px; border-radius: 8px; font-size: 14px; max-width: 320px; box-shadow: 0 2px 10px rgba(0, 0, 0, .3); }
        .hide { display: none !important; }
        @media (max-width: 800px) {
            .sheet { width: auto; right: 16px; top: auto; height: 62%; }
            .stats { grid-template-columns: repeat(2, 1fr); }
            .cols { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <header>
        <div class="brand">Safar</div>
        <nav>
            <button data-tab="rider" class="on">Ride</button>
            <button data-tab="driver">Drive</button>
            <button data-tab="analytics">Insights</button>
        </nav>
    </header>

    <section id="tab-rider" class="tab on">
        <div class="stage">
            <canvas id="map-r"></canvas>
            <div class="sheet">
                <div id="sec-trip">
                    <h1>Get a ride</h1>
                    <div class="route">
                        <span class="dot"></span>
                        <span class="sq"></span>
                        <select id="pickup"></select>
                        <select id="drop"></select>
                    </div>
                    <div class="sub">Or tap the map to set your <b id="selecting">pickup</b>.</div>
                    <button class="btn" id="btn-fares">See prices</button>
                </div>

                <div id="sec-fares" class="hide">
                    <h2>Choose a ride</h2>
                    <div class="sub" id="trip-info"></div>
                    <div class="sub" id="surge"></div>
                    <div id="fare-list"></div>
                    <button class="btn" id="btn-request">Choose</button>
                    <button class="btn light" id="btn-back">Back</button>
                </div>

                <div id="sec-ride" class="hide">
                    <h2 id="ride-head"></h2>
                    <div class="sub" id="ride-sub"></div>
                    <div class="bar" id="ride-bar"><i></i></div>
                    <div id="ride-body"></div>
                    <ul class="log" id="ride-log"></ul>
                    <button class="btn light" id="btn-cancel">Cancel ride</button>
                </div>

                <div id="sec-pay" class="hide">
                    <h2>You have arrived</h2>
                    <div class="amt" id="pay-amt"></div>
                    <div class="sub">Choose how you would like to pay</div>
                    <div class="seg" id="pay-seg">
                        <button data-m="cash" class="on">Cash</button>
                        <button data-m="wallet">Wallet</button>
                        <button data-m="card">Card</button>
                    </div>
                    <div id="card-box" class="hide" style="margin-top:10px">
                        <input type="text" id="card-no" placeholder="Card number (16 digits)">
                    </div>
                    <button class="btn" id="btn-pay">Pay</button>
                </div>

                <h4>Activity</h4>
                <div id="history" class="sub">No trips yet.</div>
                <h4>Notifications</h4>
                <div id="notes" class="sub">Nothing yet.</div>
            </div>
        </div>
    </section>

    <section id="tab-driver" class="tab">
        <div class="stage">
            <canvas id="map-d"></canvas>
            <div class="sheet">
                <h1>Drive with Safar</h1>
                <select id="drv-select"></select>
                <div class="switch"><span>Go online</span><input type="checkbox" id="drv-online" checked></div>
                <div class="switch"><span>Respond manually</span><input type="checkbox" id="drv-manual"></div>
                <h4>Relocate</h4>
                <select id="drv-loc"></select>
                <div class="tiles" id="drv-stats"></div>

                <div id="drv-offer" class="offer hide">
                    <h2>New request <span class="sub" id="drv-timer"></span></h2>
                    <div id="drv-offer-body"></div>
                    <div class="row">
                        <button class="btn" id="btn-accept">Accept</button>
                        <button class="btn light" id="btn-deny">Decline</button>
                    </div>
                </div>

                <div id="drv-ride" class="hide">
                    <h4>Current trip <span class="sub" id="drv-ride-state"></span></h4>
                    <div id="drv-ride-body"></div>
                    <div class="row">
                        <button class="btn" id="btn-start">Start trip</button>
                        <button class="btn" id="btn-end">End trip</button>
                    </div>
                </div>

                <h4>Notifications</h4>
                <div id="drv-notes" class="sub">Nothing yet.</div>
            </div>
        </div>
    </section>

    <section id="tab-analytics" class="tab">
        <div class="page">
            <h1>Insights</h1>
            <div class="stats" id="stats"></div>
            <div class="cols">
                <div><h4>Rides by category</h4><div id="an-cat"></div></div>
                <div><h4>Top drivers</h4><div id="an-drv"></div></div>
            </div>
            <h4>Live event stream</h4>
            <div id="an-events"></div>
        </div>
    </section>

    <div id="toasts"></div>

    <script>
        const PLACES = __PLACES__;
        const B = { la0: 27.63, la1: 27.76, lo0: 85.26, lo1: 85.44 };
        const ROADS = [
            ["Kalanki", "Thamel", "Maitighar", "Baneshwor", "Koteshwor", "Bhaktapur"],
            ["Balaju", "Thamel", "Swayambhu", "Kalanki"],
            ["Maitighar", "Pulchowk", "Patan"],
            ["Baneshwor", "Airport", "Boudha"]
        ];
        const ICONS = { Bike: "🏍️", Economy: "🚗", Comfort: "🚙", XL: "🚐" };
        const STATE_TEXT = {
            MATCHING: "Finding driver", DRIVER_ASSIGNED: "Driver on the way", DRIVER_ARRIVED: "Driver arrived",
            IN_PROGRESS: "On trip", COMPLETED: "Completed", CANCELLED: "Cancelled", NO_DRIVERS: "No drivers"
        };
        const HEAD = {
            MATCHING: "Looking for nearby drivers", DRIVER_ASSIGNED: "Your driver is on the way",
            DRIVER_ARRIVED: "Your driver has arrived", IN_PROGRESS: "Heading to your destination",
            CANCELLED: "Ride cancelled", NO_DRIVERS: "No drivers available"
        };
        const LIVE = ["MATCHING", "DRIVER_ASSIGNED", "DRIVER_ARRIVED", "IN_PROGRESS"];
        const $ = id => document.getElementById(id);
        let TOKEN = null, GUEST = null;
        let trip = { pickup: null, drop: null }, selecting = "pickup";
        let quote = null, category = null, ride = null, drivers = [], tab = "rider", lastNote = 0;
        let driverState = null, payMethod = "cash";

        async function auth() {
            let id = null;
            try { id = localStorage.getItem("guest"); } catch (e) {}
            const r = await (await fetch("v1/api/guest" + (id ? "?id=" + id : ""))).json();
            TOKEN = r.token; GUEST = r.id;
            try { localStorage.setItem("guest", r.id); } catch (e) {}
        }
        async function api(path, method = "GET", body = null, retry = true) {
            if (!TOKEN) await auth();
            const res = await fetch(path, {
                method,
                headers: { "Content-Type": "application/json", Authorization: "Bearer " + TOKEN },
                body: body ? JSON.stringify(body) : undefined
            });
            if (res.status === 401 && retry) { await auth(); return api(path, method, body, false); }
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "Request failed");
            return data;
        }
        function toast(msg) {
            const t = document.createElement("div");
            t.className = "toast"; t.textContent = msg;
            $("toasts").appendChild(t);
            setTimeout(() => t.remove(), 4500);
        }
        const guard = fn => async (...a) => { try { await fn(...a); } catch (e) { toast(e.message); } };

        function proj(c, lat, lng) {
            return [(lng - B.lo0) / (B.lo1 - B.lo0) * c.width, (B.la1 - lat) / (B.la1 - B.la0) * c.height];
        }
        function trace(x, c, names) {
            x.beginPath();
            names.forEach((n, i) => { const p = proj(c, ...PLACES[n]); i ? x.lineTo(...p) : x.moveTo(...p); });
        }
        function drawMap(c, s) {
            c.width = c.clientWidth; c.height = c.clientHeight;
            if (!c.width) return;
            const x = c.getContext("2d");
            x.fillStyle = "#e8e8e8"; x.fillRect(0, 0, c.width, c.height);
            x.lineCap = "round"; x.lineJoin = "round";
            ROADS.forEach(r => {
                trace(x, c, r); x.strokeStyle = "#d2d2d2"; x.lineWidth = 12; x.stroke();
                trace(x, c, r); x.strokeStyle = "#fff"; x.lineWidth = 9; x.stroke();
            });
            x.font = "12px Helvetica, Arial, sans-serif"; x.fillStyle = "#7a7a7a";
            Object.entries(PLACES).forEach(([n, v]) => { const p = proj(c, ...v); x.fillText(n, p[0] + 8, p[1] - 8); });
            (s.drivers || []).forEach(d => {
                const p = proj(c, d.lat, d.lng);
                x.globalAlpha = d.status === "AVAILABLE" ? 1 : .3;
                x.fillStyle = "#000"; x.fillRect(p[0] - 5, p[1] - 8, 10, 16);
                x.fillStyle = "#fff"; x.fillRect(p[0] - 3, p[1] - 5, 6, 4);
            });
            x.globalAlpha = 1;
            if (s.pickup && s.drop) {
                const a = proj(c, s.pickup.lat, s.pickup.lng), b = proj(c, s.drop.lat, s.drop.lng);
                x.strokeStyle = "#000"; x.lineWidth = 4;
                x.beginPath(); x.moveTo(...a); x.lineTo(...b); x.stroke();
            }
            if (s.pickup) {
                const p = proj(c, s.pickup.lat, s.pickup.lng);
                x.fillStyle = "#000"; x.beginPath(); x.arc(p[0], p[1], 9, 0, 7); x.fill();
                x.fillStyle = "#fff"; x.beginPath(); x.arc(p[0], p[1], 3.5, 0, 7); x.fill();
            }
            if (s.drop) {
                const p = proj(c, s.drop.lat, s.drop.lng);
                x.fillStyle = "#000"; x.fillRect(p[0] - 9, p[1] - 9, 18, 18);
                x.fillStyle = "#fff"; x.fillRect(p[0] - 3.5, p[1] - 3.5, 7, 7);
            }
            if (s.me) {
                const p = proj(c, s.me.lat, s.me.lng);
                x.fillStyle = "#fff"; x.fillRect(p[0] - 10, p[1] - 15, 20, 30);
                x.fillStyle = "#000"; x.fillRect(p[0] - 8, p[1] - 13, 16, 26);
                x.fillStyle = "#fff"; x.fillRect(p[0] - 5, p[1] - 8, 10, 6);
            }
        }
        function redrawRider() {
            const showing = ride && (LIVE.includes(ride.state) || ride.state === "COMPLETED");
            drawMap($("map-r"), {
                pickup: showing ? ride.pickup : trip.pickup, drop: showing ? ride.drop : trip.drop,
                drivers: showing ? [] : drivers, me: showing && ride.driver ? ride.driver : null
            });
        }
        $("map-r").addEventListener("click", e => {
            if (ride && !["CANCELLED", "NO_DRIVERS"].includes(ride.state) && !(ride.state === "COMPLETED" && ride.paid)) return;
            const r = e.target.getBoundingClientRect();
            const lng = B.lo0 + (e.clientX - r.left) / r.width * (B.lo1 - B.lo0);
            const lat = B.la1 - (e.clientY - r.top) / r.height * (B.la1 - B.la0);
            trip[selecting] = { lat, lng, name: "Custom point" };
            $(selecting).value = "";
            selecting = selecting === "pickup" ? "drop" : "pickup";
            $("selecting").textContent = selecting;
            hideFares(); redrawRider();
        });

        function fillSelect(sel, withCustom) {
            sel.innerHTML = (withCustom ? '<option value="">Custom location (map)</option>' : "") +
                Object.keys(PLACES).map(n => `<option>${n}</option>`).join("");
        }
        function setPlace(which) {
            const n = $(which).value;
            if (n) trip[which] = { lat: PLACES[n][0], lng: PLACES[n][1], name: n };
            hideFares(); redrawRider();
        }
        function hideFares() {
            $("sec-fares").classList.add("hide"); $("sec-trip").classList.remove("hide"); quote = null;
        }
        $("pickup").onchange = () => setPlace("pickup");
        $("drop").onchange = () => setPlace("drop");

        $("btn-fares").onclick = guard(async () => {
            if (!trip.pickup || !trip.drop) throw new Error("Choose pickup and drop-off");
            const q = `pickup=${trip.pickup.lat},${trip.pickup.lng}&drop=${trip.drop.lat},${trip.drop.lng}`;
            quote = await api("v1/api/fares?" + q);
            category = quote.fares[1].category;
            $("sec-trip").classList.add("hide"); $("sec-fares").classList.remove("hide");
            $("surge").textContent = quote.surge > 1 ? `Prices are higher than usual (x${quote.surge})` : "";
            $("trip-info").textContent = `${quote.distance_km} km - ${quote.duration_min} min`;
            renderFares();
        });
        function pick(c) { category = c; renderFares(); }
        function renderFares() {
            $("fare-list").innerHTML = quote.fares.map(f => `
                <div class="opt ${f.category === category ? "sel" : ""}" onclick="pick('${f.category}')">
                    <div class="ic">${ICONS[f.category]}</div>
                    <div class="mid">
                        <b>${f.category}</b>
                        <div class="sub">${f.seats} seat(s) - ${f.eta ? f.eta + " min away" : "No drivers nearby"}</div>
                    </div>
                    <div class="price">NPR ${f.fare}</div>
                </div>`).join("");
            $("btn-request").textContent = "Choose " + category;
        }
        $("btn-back").onclick = hideFares;
        $("btn-request").onclick = guard(async () => {
            ride = await api("v1/api/rides/request", "POST", { request_id: quote.request_id, category });
            quote = null; renderRide();
        });
        $("btn-cancel").onclick = guard(async () => {
            if (ride && ["CANCELLED", "NO_DRIVERS"].includes(ride.state)) { ride = null; renderRide(); return; }
            ride = await api(`v1/api/rides/${ride.id}/cancel`, "POST"); renderRide(); loadHistory();
        });
        document.querySelectorAll("#pay-seg button").forEach(b => b.onclick = () => {
            payMethod = b.dataset.m;
            document.querySelectorAll("#pay-seg button").forEach(x => x.classList.toggle("on", x === b));
            $("card-box").classList.toggle("hide", payMethod !== "card");
        });
        $("btn-pay").onclick = guard(async () => {
            ride = await api("v1/api/payments/pay", "POST", { ride_id: ride.id, method: payMethod, card: $("card-no").value });
            toast("Payment successful. Thanks for riding!");
            ride = null; trip = { pickup: null, drop: null }; setPlace("pickup"); setPlace("drop");
            renderRide(); loadHistory();
        });

        function renderRide() {
            const live = ride && LIVE.includes(ride.state);
            const pay = ride && ride.state === "COMPLETED" && !ride.paid;
            const dead = ride && ["CANCELLED", "NO_DRIVERS"].includes(ride.state);
            $("sec-ride").classList.toggle("hide", !(live || dead));
            $("sec-pay").classList.toggle("hide", !pay);
            $("sec-trip").classList.toggle("hide", !!(live || dead || pay) || !!quote);
            if (live || dead || pay) $("sec-fares").classList.add("hide");
            if (ride && (live || dead)) {
                const d = ride.driver;
                $("ride-head").textContent = HEAD[ride.state];
                $("ride-sub").textContent = {
                    MATCHING: ride.log[ride.log.length - 1],
                    DRIVER_ASSIGNED: d ? `Arriving in ${d.eta} min` : "",
                    DRIVER_ARRIVED: `Meet your driver at ${ride.pickup.name}`,
                    IN_PROGRESS: d ? `${d.eta} min to ${ride.drop.name}` : ""
                }[ride.state] || "";
                $("ride-bar").className = "bar" + (ride.state === "MATCHING" ? "" : " still");
                $("ride-body").innerHTML = `
                    ${d ? `<div class="person">
                        <div class="avatar">${d.name[0]}</div>
                        <div style="flex:1"><b>${d.name}</b><div class="sub">${d.rating} stars - ${ride.cat}</div></div>
                        <span class="plate">${d.plate}</span>
                    </div>` : ""}
                    <div class="trip"><i class="c"></i>${ride.pickup.name}</div>
                    <div class="trip"><i></i>${ride.drop.name}</div>
                    <div class="trip"><b>NPR ${ride.fare}</b><span class="sub">${ride.dist} km - ${ride.cat}</span></div>`;
                $("ride-log").innerHTML = ride.log.map(l => `<li>${l}</li>`).join("");
                $("btn-cancel").textContent = dead ? "Book another ride" : "Cancel ride";
            }
            if (pay) $("pay-amt").textContent = "NPR " + ride.fare;
            redrawRider();
        }

        async function loadHistory() {
            const h = await api("v1/api/rides/History", "POST");
            if (!h.length) return;
            $("history").innerHTML = `<table>${h.map(r => `
                <tr>
                    <td><b>${r.drop.name}</b><div class="sub">${r.cat} - ${STATE_TEXT[r.state]}</div></td>
                    <td>NPR ${r.fare}</td>
                    <td>${r.paid && r.paid !== "cash" && r.paid !== "refunded"
                        ? `<button class="link" onclick="refund('${r.id}')">Refund</button>` : (r.paid || "")}</td>
                </tr>`).join("")}</table>`;
        }
        window.refund = guard(async id => { await api("v1/api/payments/refund", "POST", { ride_id: id }); toast("Refund issued"); loadHistory(); });

        const DID = () => $("drv-select").value;
        const driverCall = (path, body = {}) => api(path, "POST", { driver_id: DID(), ...body });
        $("drv-online").onchange = guard(() => api(`v1/driver/${DID()}/status`, "POST", { online: $("drv-online").checked }));
        $("drv-manual").onchange = guard(() => api(`v1/driver/${DID()}/status`, "POST", { manual: $("drv-manual").checked }));
        $("drv-loc").onchange = guard(() => {
            const p = PLACES[$("drv-loc").value];
            return api("v1/driver/location", "POST", { driver_id: DID(), lat: p[0], lng: p[1] });
        });
        $("drv-select").onchange = () => { driverState = null; pollDriver(); };
        $("btn-accept").onclick = guard(() => driverCall(`v1/api/ride/${driverState.offer.ride.id}/accept`));
        $("btn-deny").onclick = guard(() => driverCall(`v1/api/ride/${driverState.offer.ride.id}/deny`));
        $("btn-start").onclick = guard(() => driverCall(`v1/api/ride/${driverState.ride.id}/start`));
        $("btn-end").onclick = guard(() => driverCall(`v1/api/ride/${driverState.ride.id}/end`));

        async function pollDriver() {
            const s = driverState = await api(`v1/driver/${DID()}/state`);
            const d = s.driver;
            const tile = (v, l) => `<div class="tile"><b>${v}</b><span>${l}</span></div>`;
            $("drv-stats").innerHTML = tile("NPR " + d.earned, "Earnings") + tile(d.trips, "Trips") + tile(d.rating, "Rating");
            $("drv-offer").classList.toggle("hide", !s.offer);
            if (s.offer) {
                const r = s.offer.ride;
                $("drv-timer").textContent = s.offer.left + "s" + (d.manual ? "" : " - autopilot");
                $("drv-offer-body").innerHTML = `
                    <div class="trip"><i class="c"></i>${r.pickup.name}</div>
                    <div class="trip"><i></i>${r.drop.name}</div>
                    <div class="sub">${r.dist} km - ${r.cat}</div>
                    <div class="amt">NPR ${r.fare}</div>`;
                $("btn-accept").disabled = $("btn-deny").disabled = !d.manual;
            }
            $("drv-ride").classList.toggle("hide", !s.ride);
            if (s.ride) {
                $("drv-ride-state").textContent = STATE_TEXT[s.ride.state];
                $("drv-ride-body").innerHTML = `
                    <div class="trip"><i class="c"></i>${s.ride.pickup.name}</div>
                    <div class="trip"><i></i>${s.ride.drop.name}</div>
                    <div class="sub">NPR ${s.ride.fare} - ${d.manual ? "Use the buttons when you arrive" : "Autopilot is driving"}</div>`;
                $("btn-start").disabled = !d.manual || s.ride.state !== "DRIVER_ARRIVED";
                $("btn-end").disabled = !d.manual || !(s.ride.state === "IN_PROGRESS" && s.ride.at_dest);
            }
            $("drv-notes").innerHTML = s.notes.length ? s.notes.map(n => `<div class="note">${n.msg}</div>`).reverse().join("") : "Nothing yet.";
            const r = s.ride || (s.offer && s.offer.ride);
            drawMap($("map-d"), { drivers, me: d, pickup: r ? r.pickup : null, drop: r ? r.drop : null });
        }

        async function pollAnalytics() {
            const a = await api("v1/api/analytics");
            const stat = (l, v) => `<div class="stat"><b>${v}</b><span class="sub">${l}</span></div>`;
            $("stats").innerHTML = stat("Total trips", a.total) + stat("Completed", a.completed) + stat("Cancelled", a.cancelled) +
                stat("Revenue (NPR)", a.revenue) + stat("Avg fare (NPR)", a.avg_fare) + stat("Drivers online", a.online);
            $("an-cat").innerHTML = "<table>" + Object.entries(a.by_cat).map(([c, v]) =>
                `<tr><td>${c}</td><td>${v.rides} trips</td><td>NPR ${v.revenue}</td></tr>`).join("") + "</table>";
            $("an-drv").innerHTML = "<table>" + a.drivers.map(d =>
                `<tr><td>${d.name}</td><td>${d.trips} trips</td><td>NPR ${d.earned}</td><td>${d.rating} stars</td></tr>`).join("") + "</table>";
            $("an-events").innerHTML = "<table>" + a.events.map(e =>
                `<tr><td>${e.t}</td><td>${e.type}</td><td class="sub">${JSON.stringify(e).replace(/[{}"]/g, "")}</td></tr>`).join("") + "</table>";
        }

        async function poll() {
            try {
                if (tab === "rider") {
                    if (ride && LIVE.includes(ride.state)) {
                        ride = await api("v1/api/rides/" + ride.id); renderRide();
                    } else {
                        drivers = await api("v1/api/drivers/nearby"); redrawRider();
                    }
                    const notes = await api("v1/api/notifications");
                    notes.filter(n => n.id > lastNote).forEach(n => { toast(n.msg); lastNote = n.id; });
                    if (notes.length) $("notes").innerHTML = notes.map(n => `<div class="note">${n.msg}</div>`).reverse().join("");
                    if (ride && ride.state === "COMPLETED") loadHistory();
                } else if (tab === "driver") {
                    drivers = await api("v1/api/drivers/nearby"); await pollDriver();
                } else {
                    await pollAnalytics();
                }
            } catch (e) { console.log(e); }
            setTimeout(poll, 1000);
        }

        document.querySelectorAll("nav button").forEach(b => b.onclick = () => {
            tab = b.dataset.tab;
            document.querySelectorAll("nav button").forEach(x => x.classList.toggle("on", x === b));
            document.querySelectorAll(".tab").forEach(t => t.classList.toggle("on", t.id === "tab-" + tab));
        });

        (async function init() {
            await auth();
            fillSelect($("pickup"), true); fillSelect($("drop"), true);
            $("pickup").value = "Thamel"; $("drop").value = "Pulchowk";
            setPlace("pickup"); setPlace("drop");
            fillSelect($("drv-loc"), false);
            drivers = await api("v1/api/drivers/nearby");
            $("drv-select").innerHTML = drivers.map(d => `<option value="${d.id}">${d.name} - ${d.cat} (${d.id})</option>`).join("");
            const a = await api("v1/api/rides/active");
            if (a.id) { ride = a; renderRide(); }
            loadHistory();
            poll();
        })();
        window.addEventListener("resize", redrawRider);
    </script>
</body>
</html>
"""


@app.get("/")
def index():
    return Response(HTML.replace("__PLACES__", json.dumps(PLACES)), mimetype="text/html")


try:
    SERVER.shutdown()
except Exception:
    pass
DRIVERS.clear(); RIDES.clear(); QUOTES.clear(); PAYMENTS.clear(); NOTES.clear()
seed_drivers()
RUN["on"] = True
threading.Thread(target=tick_loop, args=(RUN,), daemon=True).start()
SERVER = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=SERVER.serve_forever, daemon=True).start()

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height=900)
except ImportError:
    print(f"Open http://localhost:{PORT} in your browser")