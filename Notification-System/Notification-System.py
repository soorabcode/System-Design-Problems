'''
Notification-System

1. Requirement Analysis
1A. Functional Requirements
- Support multiple systems (Android, iOS).
- Create dynamic template.
- Reporting.

1B. Non-Functional Requirements
- Scale: 1M notification
- CAP Theorem: Available >>>>> Consistent
- Latency: 200 ms

2. Identifying Entities
- User
- Template
- Client

3. API Creation
3A. Notification : 
POST /v1/notification {ID + client ID}
GET /v1/notification/{ID}/status

3B. Template
POST /v1/templates
GET /v1/templates/{ID}/version

3C.  User
PUT /api/v1/preference/{client ID}

4. High level design 
user -> api gateway -> sever ( templates , user preferences & notfication srver ) -> database ( clientDB, templatesDB , SMSDB , emailDB & pushDB )

5. low level design 
uploaded image
'''
import re
import time
import uuid
import queue
import random
import threading
from datetime import datetime

from flask import Flask, request, jsonify, Response
from werkzeug.serving import make_server

PORT = 5050
FAIL_RATE = 0.06
MAX_RETRIES = 3
CONSUMER_WORKERS = 6

app = Flask(__name__)
LOCK = threading.RLock()

CLIENT_DB = {}
TEMPLATE_DB = {}
PREFERENCE_DB = {}
NOTIFICATION_DB = {}
SMS_DB, EMAIL_DB, PUSH_DB = [], [], []
API_LATENCIES = []

PREF_TOPIC = queue.Queue()
NOTIF_TOPIC = queue.Queue()

VAR_PATTERN = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def extract_vars(*texts):
    found = set()
    for t in texts:
        if t:
            found.update(VAR_PATTERN.findall(t))
    return sorted(found)


def render(text, variables):
    return VAR_PATTERN.sub(lambda m: str(variables.get(m.group(1), "")), text or "")


def seed():
    clients = [
        ("Aarav Sharma", "aarav@example.com", "+9779800000001", "android"),
        ("Sita Thapa", "sita@example.com", "+9779800000002", "ios"),
        ("Ram Gurung", "ram@example.com", "+9779800000003", "android"),
        ("Maya Karki", "maya@example.com", "+9779800000004", "ios"),
    ]
    for name, email, phone, platform in clients:
        cid = "cl_" + uuid.uuid4().hex[:6]
        CLIENT_DB[cid] = {
            "id": cid, "name": name, "email": email, "phone": phone,
            "platform": platform, "device_token": "tok_" + uuid.uuid4().hex[:10],
        }
        PREFERENCE_DB[cid] = {"sms": True, "email": True, "push": True, "dnd": False, "version": 1}

    templates = [
        ("Order Confirmation",
         "Hi {{name}}, your order {{order_id}} is confirmed.",
         "Order {{order_id}} confirmed",
         "Hello {{name}},\n\nThanks for your order {{order_id}} worth {{amount}}.\nWe will notify you once it ships.",
         "Order confirmed",
         "{{order_id}} is on its way, {{name}}!"),
        ("OTP Verification",
         "{{otp}} is your verification code, {{name}}. Do not share it.",
         "Your verification code",
         "Hi {{name}}, your one-time code is {{otp}}. It expires in 5 minutes.",
         "Verification code",
         "Use {{otp}} to verify your account."),
        ("Festival Offer",
         "{{name}}, enjoy {{discount}} off this festival season!",
         "A special {{discount}} offer for you",
         "Dear {{name}},\n\nCelebrate with {{discount}} off on all items. Offer ends soon.",
         "Festival sale",
         "{{discount}} off just for you, {{name}}."),
    ]
    for name, sms, subj, body, ptitle, pbody in templates:
        tid = "tpl_" + uuid.uuid4().hex[:6]
        TEMPLATE_DB[tid] = {"id": tid, "name": name, "versions": [{
            "version": 1, "sms": sms, "email_subject": subj, "email_body": body,
            "push_title": ptitle, "push_body": pbody,
            "variables": extract_vars(sms, subj, body, ptitle, pbody),
            "created_at": now_iso(),
        }]}


seed()


def channel_send(channel, nid, client, content):
    if channel == "sms":
        provider = "SMS-Gateway"
    elif channel == "email":
        provider = "SMTP-Relay"
    else:
        provider = "FCM (Android)" if client["platform"] == "android" else "APNs (iOS)"

    ok, attempts = False, 0
    for attempts in range(1, MAX_RETRIES + 1):
        time.sleep(random.uniform(0.002, 0.012))
        if random.random() > FAIL_RATE:
            ok = True
            break

    record = {"notification_id": nid, "client_id": client["id"], "provider": provider,
              "content": content, "status": "DELIVERED" if ok else "FAILED",
              "attempts": attempts, "at": now_iso()}
    with LOCK:
        {"sms": SMS_DB, "email": EMAIL_DB, "push": PUSH_DB}[channel].append(record)
    return ok, attempts, provider


def preference_consumer():
    while True:
        event = PREF_TOPIC.get()
        time.sleep(0.05)
        with LOCK:
            current = PREFERENCE_DB.get(event["client_id"], {"version": 0})
            PREFERENCE_DB[event["client_id"]] = {
                "sms": event["sms"], "email": event["email"], "push": event["push"],
                "dnd": event["dnd"], "version": current.get("version", 0) + 1,
            }


def build_content(channel, tpl_version, variables):
    if channel == "sms":
        return {"text": render(tpl_version["sms"], variables)}
    if channel == "email":
        return {"subject": render(tpl_version["email_subject"], variables),
                "body": render(tpl_version["email_body"], variables)}
    return {"title": render(tpl_version["push_title"], variables),
            "body": render(tpl_version["push_body"], variables)}


def notification_consumer():
    while True:
        nid = NOTIF_TOPIC.get()
        with LOCK:
            n = NOTIFICATION_DB.get(nid)
            if not n:
                continue
            n["status"] = "PROCESSING"
            client = CLIENT_DB[n["client_id"]]
            pref = dict(PREFERENCE_DB.get(n["client_id"], {"sms": True, "email": True, "push": True, "dnd": False}))
            tpl_version = TEMPLATE_DB[n["template_id"]]["versions"][n["version"] - 1]
            variables = dict(n["variables"])
            requested = list(n["channels"].keys())
            priority = n["priority"]

        destinations = {"sms": client["phone"], "email": client["email"], "push": client["device_token"]}
        results = {}
        for ch in requested:
            if pref.get("dnd") and priority != "critical":
                results[ch] = {"status": "SKIPPED", "reason": "Do-not-disturb enabled"}
            elif not pref.get(ch, True):
                results[ch] = {"status": "SKIPPED", "reason": "User opted out of " + ch}
            elif not destinations[ch]:
                results[ch] = {"status": "SKIPPED", "reason": "No " + ch + " destination"}
            else:
                content = build_content(ch, tpl_version, variables)
                ok, attempts, provider = channel_send(ch, nid, client, content)
                results[ch] = {"status": "DELIVERED" if ok else "FAILED", "attempts": attempts,
                               "provider": provider, "content": content,
                               "reason": "" if ok else "Provider failed after retries"}

        statuses = [r["status"] for r in results.values()]
        if all(s == "SKIPPED" for s in statuses):
            overall = "SKIPPED"
        elif all(s in ("DELIVERED", "SKIPPED") for s in statuses):
            overall = "DELIVERED"
        elif "DELIVERED" in statuses:
            overall = "PARTIAL"
        else:
            overall = "FAILED"

        with LOCK:
            n["channels"] = results
            n["status"] = overall
            n["completed_at"] = now_iso()
            n["e2e_ms"] = round((time.time() - n["created_ts"]) * 1000, 1)


for _ in range(CONSUMER_WORKERS):
    threading.Thread(target=notification_consumer, daemon=True).start()
threading.Thread(target=preference_consumer, daemon=True).start()


def create_notification(template_id, version, client_id, variables, channels, priority):
    nid = "ntf_" + uuid.uuid4().hex[:10]
    with LOCK:
        NOTIFICATION_DB[nid] = {
            "id": nid, "client_id": client_id, "template_id": template_id, "version": version,
            "variables": variables, "priority": priority, "status": "QUEUED",
            "channels": {c: {"status": "QUEUED"} for c in channels},
            "created_at": now_iso(), "created_ts": time.time(), "completed_at": None, "e2e_ms": None,
        }
    NOTIF_TOPIC.put(nid)
    return nid


def public_notification(n):
    return {k: v for k, v in n.items() if k != "created_ts"}


def merged_client(cid):
    c = dict(CLIENT_DB[cid])
    c["preference"] = dict(PREFERENCE_DB.get(cid, {"sms": True, "email": True, "push": True, "dnd": False, "version": 1}))
    return c


@app.get("/v1/clients")
def list_clients():
    with LOCK:
        return jsonify([merged_client(cid) for cid in CLIENT_DB])


@app.post("/v1/clients")
def create_client():
    d = request.get_json(force=True, silent=True) or {}
    name = (d.get("name") or "").strip()
    platform = (d.get("platform") or "android").lower()
    if not name:
        return jsonify({"error": "name is required"}), 400
    if platform not in ("android", "ios"):
        return jsonify({"error": "platform must be android or ios"}), 400
    cid = "cl_" + uuid.uuid4().hex[:6]
    with LOCK:
        CLIENT_DB[cid] = {"id": cid, "name": name, "email": (d.get("email") or "").strip(),
                          "phone": (d.get("phone") or "").strip(), "platform": platform,
                          "device_token": "tok_" + uuid.uuid4().hex[:10]}
        PREFERENCE_DB[cid] = {"sms": True, "email": True, "push": True, "dnd": False, "version": 1}
        return jsonify(merged_client(cid)), 201


@app.post("/v1/templates")
def create_template():
    d = request.get_json(force=True, silent=True) or {}
    fields = {k: (d.get(k) or "").strip() for k in
              ("sms", "email_subject", "email_body", "push_title", "push_body")}
    if not any(fields.values()):
        return jsonify({"error": "provide content for at least one channel"}), 400
    with LOCK:
        tid = d.get("template_id")
        if tid:
            if tid not in TEMPLATE_DB:
                return jsonify({"error": "template not found"}), 404
        else:
            name = (d.get("name") or "").strip()
            if not name:
                return jsonify({"error": "name is required"}), 400
            tid = "tpl_" + uuid.uuid4().hex[:6]
            TEMPLATE_DB[tid] = {"id": tid, "name": name, "versions": []}
        tpl = TEMPLATE_DB[tid]
        version = dict(fields)
        version["version"] = len(tpl["versions"]) + 1
        version["variables"] = extract_vars(*fields.values())
        version["created_at"] = now_iso()
        tpl["versions"].append(version)
        return jsonify(tpl), 201


@app.get("/v1/templates")
def list_templates():
    with LOCK:
        return jsonify(list(TEMPLATE_DB.values()))


@app.get("/v1/templates/<tid>/version")
def template_versions(tid):
    with LOCK:
        tpl = TEMPLATE_DB.get(tid)
        if not tpl:
            return jsonify({"error": "template not found"}), 404
        v = request.args.get("v")
        if v:
            if not v.isdigit() or not (1 <= int(v) <= len(tpl["versions"])):
                return jsonify({"error": "version not found"}), 404
            return jsonify(tpl["versions"][int(v) - 1])
        return jsonify({"id": tid, "name": tpl["name"], "latest": len(tpl["versions"]),
                        "versions": tpl["versions"]})


@app.get("/api/v1/preference/<cid>")
def get_preference(cid):
    with LOCK:
        if cid not in CLIENT_DB:
            return jsonify({"error": "client not found"}), 404
        return jsonify(PREFERENCE_DB[cid])


@app.put("/api/v1/preference/<cid>")
def put_preference(cid):
    d = request.get_json(force=True, silent=True) or {}
    with LOCK:
        if cid not in CLIENT_DB:
            return jsonify({"error": "client not found"}), 404
        cur = PREFERENCE_DB[cid]
    event = {"client_id": cid,
             "sms": bool(d.get("sms", cur["sms"])), "email": bool(d.get("email", cur["email"])),
             "push": bool(d.get("push", cur["push"])), "dnd": bool(d.get("dnd", cur["dnd"]))}
    PREF_TOPIC.put(event)
    return jsonify({"status": "ACCEPTED", "message": "Preference event published to Kafka"}), 202


@app.post("/v1/notification")
def send_notification():
    t0 = time.perf_counter()
    d = request.get_json(force=True, silent=True) or {}
    tid = d.get("template_id")
    client_ids = d.get("client_ids") or ([d["client_id"]] if d.get("client_id") else [])
    channels = [c for c in (d.get("channels") or ["sms", "email", "push"]) if c in ("sms", "email", "push")]
    priority = "critical" if d.get("priority") == "critical" else "normal"
    variables = d.get("variables") or {}

    with LOCK:
        if tid not in TEMPLATE_DB:
            return jsonify({"error": "template not found"}), 404
        if not client_ids or any(c not in CLIENT_DB for c in client_ids):
            return jsonify({"error": "valid client_id is required"}), 404
        latest = len(TEMPLATE_DB[tid]["versions"])
    if not channels:
        return jsonify({"error": "select at least one channel"}), 400
    version = int(d.get("version") or latest)
    if not (1 <= version <= latest):
        return jsonify({"error": "template version not found"}), 404

    ids = [create_notification(tid, version, cid, variables, channels, priority) for cid in client_ids]
    ack = round((time.perf_counter() - t0) * 1000, 3)
    with LOCK:
        API_LATENCIES.append(ack)
        del API_LATENCIES[:-5000]
    return jsonify({"notification_id": ids[0], "notification_ids": ids,
                    "status": "QUEUED", "ack_ms": ack}), 202


@app.get("/v1/notification/<nid>/status")
def notification_status(nid):
    with LOCK:
        n = NOTIFICATION_DB.get(nid)
        if not n:
            return jsonify({"error": "notification not found"}), 404
        out = public_notification(n)
        out["client_name"] = CLIENT_DB[n["client_id"]]["name"]
        out["template_name"] = TEMPLATE_DB[n["template_id"]]["name"]
        out["platform"] = CLIENT_DB[n["client_id"]]["platform"]
        return jsonify(out)


@app.post("/v1/simulate")
def simulate():
    d = request.get_json(force=True, silent=True) or {}
    count = max(1, min(int(d.get("count", 500)), 5000))
    with LOCK:
        client_list = list(CLIENT_DB.values())
        template_list = list(TEMPLATE_DB.values())
    t0 = time.perf_counter()
    for _ in range(count):
        c, t = random.choice(client_list), random.choice(template_list)
        variables = {}
        for var in t["versions"][-1]["variables"]:
            variables[var] = c["name"].split()[0] if var == "name" else (
                str(random.randint(100000, 999999)) if var == "otp" else
                (str(random.choice([10, 20, 30])) + "%" if var == "discount" else
                 ("ORD-" + str(random.randint(1000, 9999)) if var == "order_id" else "Rs. " + str(random.randint(200, 5000)))))
        t0n = time.perf_counter()
        create_notification(t["id"], len(t["versions"]), c["id"], variables,
                            ["sms", "email", "push"], "critical" if "OTP" in t["name"] else "normal")
        with LOCK:
            API_LATENCIES.append(round((time.perf_counter() - t0n) * 1000, 3))
            del API_LATENCIES[:-5000]
    return jsonify({"enqueued": count, "enqueue_seconds": round(time.perf_counter() - t0, 3)}), 202


@app.post("/v1/reset")
def reset():
    with LOCK:
        NOTIFICATION_DB.clear(); SMS_DB.clear(); EMAIL_DB.clear(); PUSH_DB.clear(); API_LATENCIES.clear()
    return jsonify({"status": "cleared"})


@app.get("/v1/reports")
def report():
    with LOCK:
        notifs = list(NOTIFICATION_DB.values())
        by_status, by_template = {}, {}
        by_channel = {c: {"DELIVERED": 0, "FAILED": 0, "SKIPPED": 0} for c in ("sms", "email", "push")}
        by_platform = {"android": {"total": 0, "delivered": 0}, "ios": {"total": 0, "delivered": 0}}
        e2e = []
        for n in notifs:
            by_status[n["status"]] = by_status.get(n["status"], 0) + 1
            name = TEMPLATE_DB[n["template_id"]]["name"]
            by_template[name] = by_template.get(name, 0) + 1
            plat = CLIENT_DB[n["client_id"]]["platform"]
            by_platform[plat]["total"] += 1
            if n["status"] in ("DELIVERED", "PARTIAL"):
                by_platform[plat]["delivered"] += 1
            for ch, r in n["channels"].items():
                if r["status"] in by_channel[ch]:
                    by_channel[ch][r["status"]] += 1
            if n["e2e_ms"] is not None:
                e2e.append(n["e2e_ms"])
        lat = sorted(API_LATENCIES)
        recent = sorted(notifs, key=lambda x: x["created_ts"], reverse=True)[:8]
        total = len(notifs)
        done = sum(v for k, v in by_status.items() if k not in ("QUEUED", "PROCESSING"))
        delivered = by_status.get("DELIVERED", 0) + by_status.get("PARTIAL", 0)
        return jsonify({
            "total": total,
            "completed": done,
            "delivery_rate": round(100 * delivered / done, 1) if done else 0,
            "by_status": by_status,
            "by_channel": by_channel,
            "by_platform": by_platform,
            "by_template": by_template,
            "api_ack_avg_ms": round(sum(lat) / len(lat), 3) if lat else 0,
            "api_ack_p95_ms": lat[int(len(lat) * 0.95) - 1] if len(lat) > 1 else (lat[0] if lat else 0),
            "e2e_avg_ms": round(sum(e2e) / len(e2e), 1) if e2e else 0,
            "kafka": {"notification_topic_lag": NOTIF_TOPIC.qsize(), "preference_topic_lag": PREF_TOPIC.qsize()},
            "databases": {"clients": len(CLIENT_DB), "templates": len(TEMPLATE_DB),
                          "preferences": len(PREFERENCE_DB), "sms": len(SMS_DB),
                          "email": len(EMAIL_DB), "push": len(PUSH_DB)},
            "recent": [{"id": n["id"], "client": CLIENT_DB[n["client_id"]]["name"],
                        "template": TEMPLATE_DB[n["template_id"]]["name"],
                        "status": n["status"], "e2e_ms": n["e2e_ms"]} for n in recent],
        })


PAGE = r"""<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NotifyHub - Notification / Preference System</title>
    <style>
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        :root {
            --navy: #0f1f4d;
            --blue: #2563eb;
            --blue-soft: #dbeafe;
            --purple-soft: #ede9fe;
            --green: #16a34a;
            --green-soft: #dcfce7;
            --orange: #ea580c;
            --orange-soft: #ffedd5;
            --red: #dc2626;
            --red-soft: #fee2e2;
            --grey: #64748b;
            --border: #e2e8f0;
            --bg: #f1f5f9;
        }

        body {
            font-family: "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background: var(--bg);
            color: #0f172a;
            min-height: 100vh;
        }

        header {
            background: var(--navy);
            color: #fff;
            padding: 16px 26px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 10px;
        }

        header h1 {
            font-size: 20px;
            font-weight: 600;
        }

        header span {
            font-size: 12px;
            color: #93c5fd;
        }

        nav {
            display: flex;
            gap: 4px;
            background: #fff;
            padding: 8px 20px 0;
            border-bottom: 1px solid var(--border);
            overflow-x: auto;
        }

        nav button {
            border: none;
            background: none;
            padding: 11px 16px;
            font-size: 14px;
            font-weight: 600;
            color: var(--grey);
            cursor: pointer;
            border-bottom: 3px solid transparent;
            white-space: nowrap;
        }

        nav button.active {
            color: var(--blue);
            border-bottom-color: var(--blue);
        }

        main {
            max-width: 1100px;
            margin: 0 auto;
            padding: 22px 18px 50px;
        }

        .tab {
            display: none;
        }

        .tab.active {
            display: block;
        }

        .card {
            background: #fff;
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 18px;
            margin-bottom: 18px;
        }

        .card h2 {
            font-size: 16px;
            margin-bottom: 12px;
            color: var(--navy);
        }

        .grid {
            display: grid;
            gap: 14px;
        }

        .grid.stats {
            grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        }

        .grid.two {
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
        }

        .stat {
            border-radius: 12px;
            padding: 14px 16px;
            border: 1px solid var(--border);
        }

        .stat small {
            display: block;
            font-size: 12px;
            color: var(--grey);
            margin-bottom: 6px;
        }

        .stat b {
            font-size: 24px;
        }

        .stat.blue {
            background: var(--blue-soft);
        }

        .stat.green {
            background: var(--green-soft);
        }

        .stat.orange {
            background: var(--orange-soft);
        }

        .stat.purple {
            background: var(--purple-soft);
        }

        label {
            display: block;
            font-size: 12px;
            font-weight: 600;
            color: var(--grey);
            margin: 10px 0 4px;
        }

        input[type=text],
        input[type=number],
        select,
        textarea {
            width: 100%;
            padding: 9px 11px;
            border: 1px solid #cbd5e1;
            border-radius: 8px;
            font-size: 14px;
            font-family: inherit;
            background: #fff;
        }

        textarea {
            min-height: 70px;
            resize: vertical;
        }

        input:focus,
        select:focus,
        textarea:focus {
            outline: 2px solid var(--blue-soft);
            border-color: var(--blue);
        }

        .btn {
            background: var(--blue);
            color: #fff;
            border: none;
            padding: 10px 18px;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            margin-top: 14px;
        }

        .btn:hover {
            opacity: 0.9;
        }

        .btn.secondary {
            background: #fff;
            color: var(--blue);
            border: 1px solid var(--blue);
        }

        .btn.danger {
            background: var(--red);
        }

        .btn.small {
            padding: 6px 12px;
            font-size: 12px;
            margin-top: 0;
        }

        .row {
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
            align-items: center;
        }

        .checks label {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            margin: 6px 14px 0 0;
            font-size: 14px;
            color: #0f172a;
            font-weight: 500;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }

        th,
        td {
            text-align: left;
            padding: 9px 8px;
            border-bottom: 1px solid var(--border);
        }

        th {
            color: var(--grey);
            font-size: 12px;
            font-weight: 600;
        }

        .table-wrap {
            overflow-x: auto;
        }

        .badge {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: 700;
        }

        .badge.DELIVERED {
            background: var(--green-soft);
            color: var(--green);
        }

        .badge.FAILED {
            background: var(--red-soft);
            color: var(--red);
        }

        .badge.PARTIAL {
            background: var(--orange-soft);
            color: var(--orange);
        }

        .badge.SKIPPED {
            background: #e2e8f0;
            color: #475569;
        }

        .badge.QUEUED,
        .badge.PROCESSING {
            background: var(--blue-soft);
            color: var(--blue);
        }

        .badge.android {
            background: var(--green-soft);
            color: var(--green);
        }

        .badge.ios {
            background: var(--purple-soft);
            color: #6d28d9;
        }

        .status-box {
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 14px;
            margin-top: 14px;
            background: #f8fafc;
        }

        .status-box .ch {
            border-top: 1px dashed #cbd5e1;
            margin-top: 10px;
            padding-top: 10px;
            font-size: 13px;
        }

        .status-box pre {
            white-space: pre-wrap;
            font-family: inherit;
            color: #334155;
            margin-top: 4px;
        }

        .muted {
            color: var(--grey);
            font-size: 12px;
        }

        .pill {
            display: inline-block;
            background: var(--purple-soft);
            color: #5b21b6;
            font-size: 11px;
            padding: 2px 8px;
            border-radius: 6px;
            margin: 2px 3px 2px 0;
        }

        .bar {
            height: 8px;
            background: #e2e8f0;
            border-radius: 6px;
            overflow: hidden;
            margin-top: 4px;
        }

        .bar i {
            display: block;
            height: 100%;
            background: var(--green);
        }

        code {
            background: #e2e8f0;
            padding: 2px 6px;
            border-radius: 5px;
            font-size: 12px;
        }

        .toast {
            position: fixed;
            right: 18px;
            bottom: 18px;
            background: var(--navy);
            color: #fff;
            padding: 12px 18px;
            border-radius: 10px;
            font-size: 14px;
            opacity: 0;
            transform: translateY(10px);
            transition: all 0.25s;
            z-index: 99;
            max-width: 320px;
        }

        .toast.show {
            opacity: 1;
            transform: translateY(0);
        }

        .flow {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            align-items: center;
            font-size: 13px;
            font-weight: 600;
        }

        .flow span.n {
            padding: 8px 12px;
            border-radius: 8px;
            border: 1px solid var(--border);
        }

        .n.b {
            background: var(--blue-soft);
        }

        .n.p {
            background: var(--purple-soft);
        }

        .n.g {
            background: var(--green-soft);
        }

        .n.o {
            background: var(--orange-soft);
        }
    </style>
</head>

<body>

    <header>
        <h1>&#128276; NotifyHub &mdash; Notification / Preference System</h1>
        <span>Android + iOS &bull; SMS / Email / Push &bull; Kafka-backed &bull; No login</span>
    </header>

    <nav>
        <button class="active" data-tab="dashboard">Dashboard &amp; Reports</button>
        <button data-tab="send">Send Notification</button>
        <button data-tab="templates">Templates</button>
        <button data-tab="clients">Clients &amp; Preferences</button>
        <button data-tab="status">Status Lookup</button>
        <button data-tab="design">Design &amp; APIs</button>
    </nav>

    <main>

        <section id="dashboard" class="tab active">
            <div class="grid stats">
                <div class="stat blue"><small>Total notifications</small><b id="s-total">0</b></div>
                <div class="stat green"><small>Delivery rate</small><b id="s-rate">0%</b></div>
                <div class="stat orange"><small>API ack avg / p95 (target &lt; 200 ms)</small><b id="s-ack">0 ms</b></div>
                <div class="stat purple"><small>End-to-end avg delivery</small><b id="s-e2e">0 ms</b></div>
                <div class="stat blue"><small>Kafka lag (notif / pref)</small><b id="s-lag">0 / 0</b></div>
            </div>

            <div class="card" style="margin-top:18px;">
                <h2>Load test (scale demo)</h2>
                <div class="row">
                    <input type="number" id="sim-count" value="500" min="1" max="5000" style="width:120px;">
                    <button class="btn small" onclick="simulate()">Simulate notifications</button>
                    <button class="btn small danger" onclick="resetAll()">Clear notification data</button>
                    <span class="muted">Messages are accepted instantly (HTTP 202) and processed asynchronously by consumers.</span>
                </div>
            </div>

            <div class="grid two">
                <div class="card">
                    <h2>Channel report</h2>
                    <div class="table-wrap">
                        <table>
                            <thead><tr><th>Channel</th><th>Delivered</th><th>Failed</th><th>Skipped</th></tr></thead>
                            <tbody id="t-channel"></tbody>
                        </table>
                    </div>
                </div>
                <div class="card">
                    <h2>Platform report</h2>
                    <div class="table-wrap">
                        <table>
                            <thead><tr><th>Platform</th><th>Total</th><th>Delivered</th><th>Rate</th></tr></thead>
                            <tbody id="t-platform"></tbody>
                        </table>
                    </div>
                </div>
            </div>

            <div class="grid two">
                <div class="card">
                    <h2>Status &amp; template usage</h2>
                    <div id="t-status"></div>
                    <div id="t-template" style="margin-top:10px;"></div>
                </div>
                <div class="card">
                    <h2>Databases</h2>
                    <div id="t-db"></div>
                </div>
            </div>

            <div class="card">
                <h2>Recent notifications</h2>
                <div class="table-wrap">
                    <table>
                        <thead><tr><th>ID</th><th>Client</th><th>Template</th><th>Status</th><th>E2E</th></tr></thead>
                        <tbody id="t-recent"></tbody>
                    </table>
                </div>
            </div>
        </section>

        <section id="send" class="tab">
            <div class="card">
                <h2>POST /v1/notification</h2>
                <div class="grid two">
                    <div>
                        <label>Client</label>
                        <select id="send-client"></select>
                    </div>
                    <div>
                        <label>Template</label>
                        <select id="send-template" onchange="onTemplateChange()"></select>
                    </div>
                    <div>
                        <label>Template version</label>
                        <select id="send-version" onchange="buildVars()"></select>
                    </div>
                    <div>
                        <label>Priority</label>
                        <select id="send-priority">
                            <option value="normal">Normal (respects Do-Not-Disturb)</option>
                            <option value="critical">Critical (bypasses Do-Not-Disturb)</option>
                        </select>
                    </div>
                </div>

                <label>Channels</label>
                <div class="checks">
                    <label><input type="checkbox" class="send-ch" value="sms" checked> SMS</label>
                    <label><input type="checkbox" class="send-ch" value="email" checked> Email</label>
                    <label><input type="checkbox" class="send-ch" value="push" checked> Push</label>
                </div>

                <label>Template variables</label>
                <div id="send-vars" class="grid two"></div>

                <button class="btn" onclick="sendNotification()">Send notification</button>
                <div id="send-result"></div>
            </div>
        </section>

        <section id="templates" class="tab">
            <div class="card">
                <h2>POST /v1/templates &mdash; create template or new version</h2>
                <p class="muted">Use <code>{{variable}}</code> placeholders to make templates dynamic.</p>
                <div class="grid two">
                    <div>
                        <label>Target</label>
                        <select id="tpl-target" onchange="toggleTplName()"></select>
                    </div>
                    <div id="tpl-name-wrap">
                        <label>Template name</label>
                        <input type="text" id="tpl-name" placeholder="e.g. Password Reset">
                    </div>
                </div>
                <label>SMS text</label>
                <input type="text" id="tpl-sms" placeholder="Hi {{name}}, ...">
                <label>Email subject</label>
                <input type="text" id="tpl-subject" placeholder="Subject with {{variables}}">
                <label>Email body</label>
                <textarea id="tpl-body" placeholder="Hello {{name}}, ..."></textarea>
                <div class="grid two">
                    <div>
                        <label>Push title</label>
                        <input type="text" id="tpl-ptitle">
                    </div>
                    <div>
                        <label>Push body</label>
                        <input type="text" id="tpl-pbody">
                    </div>
                </div>
                <button class="btn" onclick="saveTemplate()">Save template</button>
            </div>

            <div class="card">
                <h2>Templates &amp; versions &mdash; GET /v1/templates/{ID}/version</h2>
                <div id="tpl-list"></div>
            </div>
        </section>

        <section id="clients" class="tab">
            <div class="card">
                <h2>Add client (device / user)</h2>
                <div class="grid two">
                    <div><label>Name</label><input type="text" id="cl-name"></div>
                    <div>
                        <label>Platform</label>
                        <select id="cl-platform">
                            <option value="android">Android</option>
                            <option value="ios">iOS</option>
                        </select>
                    </div>
                    <div><label>Email</label><input type="text" id="cl-email"></div>
                    <div><label>Phone</label><input type="text" id="cl-phone"></div>
                </div>
                <button class="btn" onclick="addClient()">Add client</button>
            </div>

            <div class="card">
                <h2>PUT /api/v1/preference/{client ID}</h2>
                <p class="muted">Updates are published to Kafka and persisted asynchronously (availability over consistency).</p>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr><th>Client</th><th>Platform</th><th>SMS</th><th>Email</th><th>Push</th><th>DND</th><th></th></tr>
                        </thead>
                        <tbody id="cl-table"></tbody>
                    </table>
                </div>
            </div>
        </section>

        <section id="status" class="tab">
            <div class="card">
                <h2>GET /v1/notification/{ID}/status</h2>
                <div class="row">
                    <input type="text" id="lookup-id" placeholder="ntf_xxxxxxxxxx" style="max-width:340px;">
                    <button class="btn small" onclick="lookup()">Check status</button>
                </div>
                <div id="lookup-result"></div>
            </div>
        </section>

        <section id="design" class="tab">
            <div class="card">
                <h2>Low level design</h2>
                <div class="flow">
                    <span class="n b">User</span> &rarr;
                    <span class="n b">API Gateway</span> &rarr;
                    <span class="n p">Template &rarr; Template DB</span>
                    <span class="n g">User Preference &rarr; Kafka &rarr; Preference DB</span>
                    <span class="n o">Notification Service &rarr; Kafka</span> &rarr;
                    <span class="n g">SMS</span><span class="n g">Email</span><span class="n g">Push</span>
                </div>
            </div>
            <div class="grid two">
                <div class="card">
                    <h2>Requirements</h2>
                    <p><b>Functional:</b> multi-platform (Android, iOS), dynamic templates, reporting.</p>
                    <p style="margin-top:8px;"><b>Non-functional:</b> 1M notifications scale, Available &gt;&gt;&gt; Consistent, ack latency &lt; 200 ms.</p>
                    <p style="margin-top:8px;"><b>Entities:</b> User, Template, Client.</p>
                </div>
                <div class="card">
                    <h2>Implemented APIs</h2>
                    <p><code>POST /v1/notification</code></p><br>
                    <p><code>GET /v1/notification/{ID}/status</code></p><br>
                    <p><code>POST /v1/templates</code></p><br>
                    <p><code>GET /v1/templates/{ID}/version</code></p><br>
                    <p><code>PUT /api/v1/preference/{client ID}</code></p><br>
                    <p><code>GET /v1/reports</code> &bull; <code>POST /v1/simulate</code></p>
                </div>
            </div>
        </section>

    </main>

    <div class="toast" id="toast"></div>

    <script>
        let CLIENTS = [];
        let TEMPLATES = [];
        let pollTimer = null;

        function esc(s) {
            return String(s === undefined || s === null ? "" : s)
                .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
        }

        function toast(msg) {
            const t = document.getElementById("toast");
            t.textContent = msg;
            t.classList.add("show");
            setTimeout(() => t.classList.remove("show"), 2600);
        }

        async function api(method, url, body) {
            const opts = { method: method, headers: { "Content-Type": "application/json" } };
            if (body) opts.body = JSON.stringify(body);
            const res = await fetch(url, opts);
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || "Request failed");
            return data;
        }

        document.querySelectorAll("nav button").forEach(btn => {
            btn.addEventListener("click", () => {
                document.querySelectorAll("nav button").forEach(b => b.classList.remove("active"));
                document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
                btn.classList.add("active");
                document.getElementById(btn.dataset.tab).classList.add("active");
                refreshAll();
            });
        });

        async function loadBase() {
            CLIENTS = await api("GET", "/v1/clients");
            TEMPLATES = await api("GET", "/v1/templates");
            fillSelects();
            renderTemplates();
            renderClients();
        }

        function fillSelects() {
            const cSel = document.getElementById("send-client");
            const prevC = cSel.value;
            cSel.innerHTML = CLIENTS.map(c =>
                `<option value="${c.id}">${esc(c.name)} (${c.platform})</option>`).join("");
            if (prevC) cSel.value = prevC;

            const tSel = document.getElementById("send-template");
            const prevT = tSel.value;
            tSel.innerHTML = TEMPLATES.map(t =>
                `<option value="${t.id}">${esc(t.name)}</option>`).join("");
            if (prevT) tSel.value = prevT;

            const target = document.getElementById("tpl-target");
            const prevTarget = target.value;
            target.innerHTML = `<option value="">+ Create new template</option>` +
                TEMPLATES.map(t => `<option value="${t.id}">New version of: ${esc(t.name)}</option>`).join("");
            if (prevTarget) target.value = prevTarget;
            onTemplateChange();
            toggleTplName();
        }

        function currentTemplate() {
            const id = document.getElementById("send-template").value;
            return TEMPLATES.find(t => t.id === id);
        }

        function onTemplateChange() {
            const t = currentTemplate();
            const vSel = document.getElementById("send-version");
            if (!t) { vSel.innerHTML = ""; return; }
            vSel.innerHTML = t.versions.map(v =>
                `<option value="${v.version}">v${v.version}</option>`).join("");
            vSel.value = t.versions.length;
            buildVars();
        }

        function buildVars() {
            const t = currentTemplate();
            const box = document.getElementById("send-vars");
            if (!t) { box.innerHTML = ""; return; }
            const ver = t.versions[Number(document.getElementById("send-version").value) - 1];
            const old = {};
            box.querySelectorAll("input").forEach(i => old[i.dataset.var] = i.value);
            const clientId = document.getElementById("send-client").value;
            const client = CLIENTS.find(c => c.id === clientId);
            box.innerHTML = ver.variables.map(v => {
                let val = old[v] !== undefined ? old[v] : (v === "name" && client ? client.name.split(" ")[0] : "");
                return `<div><label>{{${v}}}</label><input type="text" data-var="${v}" value="${esc(val)}"></div>`;
            }).join("") || `<span class="muted">This version has no variables.</span>`;
        }

        document.getElementById("send-client").addEventListener("change", () => {
            document.getElementById("send-vars").innerHTML = "";
            buildVars();
        });

        async function sendNotification() {
            const variables = {};
            document.querySelectorAll("#send-vars input").forEach(i => variables[i.dataset.var] = i.value);
            const channels = Array.from(document.querySelectorAll(".send-ch:checked")).map(c => c.value);
            try {
                const res = await api("POST", "/v1/notification", {
                    template_id: document.getElementById("send-template").value,
                    version: Number(document.getElementById("send-version").value),
                    client_id: document.getElementById("send-client").value,
                    priority: document.getElementById("send-priority").value,
                    channels: channels,
                    variables: variables
                });
                document.getElementById("send-result").innerHTML =
                    `<div class="status-box"><b>Accepted (202)</b> &mdash; ID <code>${res.notification_id}</code>
                     <span class="muted">ack in ${res.ack_ms} ms</span><div id="live-status"></div></div>`;
                pollStatus(res.notification_id, "live-status");
            } catch (e) {
                toast(e.message);
            }
        }

        function statusHTML(s) {
            let html = `<div style="margin-top:8px;">
                <span class="badge ${s.status}">${s.status}</span>
                <span class="badge ${s.platform}">${s.platform}</span>
                <span class="muted"> ${esc(s.client_name)} &bull; ${esc(s.template_name)} v${s.version}
                &bull; priority ${s.priority} &bull; e2e ${s.e2e_ms === null ? "pending" : s.e2e_ms + " ms"}</span></div>`;
            Object.keys(s.channels).forEach(ch => {
                const r = s.channels[ch];
                let content = "";
                if (r.content) {
                    content = Object.keys(r.content).map(k => `<b>${k}:</b> ${esc(r.content[k])}`).join("<br>");
                }
                html += `<div class="ch"><b>${ch.toUpperCase()}</b> <span class="badge ${r.status}">${r.status}</span>
                    <span class="muted">${r.provider ? r.provider + " &bull; attempts " + r.attempts : ""} ${esc(r.reason || "")}</span>
                    <pre>${content}</pre></div>`;
            });
            return html;
        }

        function pollStatus(id, targetId) {
            if (pollTimer) clearInterval(pollTimer);
            let tries = 0;
            pollTimer = setInterval(async () => {
                tries++;
                try {
                    const s = await api("GET", `/v1/notification/${id}/status`);
                    document.getElementById(targetId).innerHTML = statusHTML(s);
                    if (!["QUEUED", "PROCESSING"].includes(s.status) || tries > 25) clearInterval(pollTimer);
                } catch (e) { clearInterval(pollTimer); }
            }, 300);
        }

        async function lookup() {
            const id = document.getElementById("lookup-id").value.trim();
            const box = document.getElementById("lookup-result");
            try {
                const s = await api("GET", `/v1/notification/${id}/status`);
                box.innerHTML = `<div class="status-box">${statusHTML(s)}</div>`;
            } catch (e) {
                box.innerHTML = `<div class="status-box">${esc(e.message)}</div>`;
            }
        }

        function toggleTplName() {
            document.getElementById("tpl-name-wrap").style.display =
                document.getElementById("tpl-target").value ? "none" : "block";
        }

        async function saveTemplate() {
            try {
                await api("POST", "/v1/templates", {
                    template_id: document.getElementById("tpl-target").value || undefined,
                    name: document.getElementById("tpl-name").value,
                    sms: document.getElementById("tpl-sms").value,
                    email_subject: document.getElementById("tpl-subject").value,
                    email_body: document.getElementById("tpl-body").value,
                    push_title: document.getElementById("tpl-ptitle").value,
                    push_body: document.getElementById("tpl-pbody").value
                });
                toast("Template saved");
                ["tpl-name", "tpl-sms", "tpl-subject", "tpl-body", "tpl-ptitle", "tpl-pbody"]
                    .forEach(id => document.getElementById(id).value = "");
                await loadBase();
            } catch (e) { toast(e.message); }
        }

        function renderTemplates() {
            document.getElementById("tpl-list").innerHTML = TEMPLATES.map(t => {
                const latest = t.versions[t.versions.length - 1];
                return `<div class="status-box">
                    <b>${esc(t.name)}</b> <code>${t.id}</code>
                    <span class="muted"> ${t.versions.length} version(s)</span>
                    <div>${latest.variables.map(v => `<span class="pill">{{${v}}}</span>`).join("")}</div>
                    <button class="btn small secondary" style="margin-top:8px;" onclick="showVersions('${t.id}')">View versions</button>
                    <div id="ver-${t.id}"></div></div>`;
            }).join("");
        }

        async function showVersions(id) {
            const data = await api("GET", `/v1/templates/${id}/version`);
            document.getElementById("ver-" + id).innerHTML = data.versions.map(v =>
                `<div class="ch" style="border-top:1px dashed #cbd5e1;margin-top:10px;padding-top:8px;font-size:13px;">
                    <b>v${v.version}</b> <span class="muted">${v.created_at}</span><br>
                    <b>SMS:</b> ${esc(v.sms)}<br>
                    <b>Email:</b> ${esc(v.email_subject)} &mdash; ${esc(v.email_body)}<br>
                    <b>Push:</b> ${esc(v.push_title)} &mdash; ${esc(v.push_body)}</div>`).join("");
        }

        function renderClients() {
            document.getElementById("cl-table").innerHTML = CLIENTS.map(c => `
                <tr>
                    <td><b>${esc(c.name)}</b><br><span class="muted">${esc(c.email)} ${esc(c.phone)}</span><br><code>${c.id}</code></td>
                    <td><span class="badge ${c.platform}">${c.platform}</span></td>
                    <td><input type="checkbox" id="p-sms-${c.id}" ${c.preference.sms ? "checked" : ""}></td>
                    <td><input type="checkbox" id="p-email-${c.id}" ${c.preference.email ? "checked" : ""}></td>
                    <td><input type="checkbox" id="p-push-${c.id}" ${c.preference.push ? "checked" : ""}></td>
                    <td><input type="checkbox" id="p-dnd-${c.id}" ${c.preference.dnd ? "checked" : ""}></td>
                    <td><button class="btn small" onclick="savePref('${c.id}')">Save</button></td>
                </tr>`).join("");
        }

        async function savePref(id) {
            try {
                await api("PUT", `/api/v1/preference/${id}`, {
                    sms: document.getElementById("p-sms-" + id).checked,
                    email: document.getElementById("p-email-" + id).checked,
                    push: document.getElementById("p-push-" + id).checked,
                    dnd: document.getElementById("p-dnd-" + id).checked
                });
                toast("Preference event published to Kafka");
                setTimeout(loadBase, 400);
            } catch (e) { toast(e.message); }
        }

        async function addClient() {
            try {
                await api("POST", "/v1/clients", {
                    name: document.getElementById("cl-name").value,
                    platform: document.getElementById("cl-platform").value,
                    email: document.getElementById("cl-email").value,
                    phone: document.getElementById("cl-phone").value
                });
                ["cl-name", "cl-email", "cl-phone"].forEach(id => document.getElementById(id).value = "");
                toast("Client added");
                await loadBase();
            } catch (e) { toast(e.message); }
        }

        async function loadReport() {
            const r = await api("GET", "/v1/reports");
            document.getElementById("s-total").textContent = r.total;
            document.getElementById("s-rate").textContent = r.delivery_rate + "%";
            document.getElementById("s-ack").textContent = r.api_ack_avg_ms + " / " + r.api_ack_p95_ms + " ms";
            document.getElementById("s-e2e").textContent = r.e2e_avg_ms + " ms";
            document.getElementById("s-lag").textContent = r.kafka.notification_topic_lag + " / " + r.kafka.preference_topic_lag;

            document.getElementById("t-channel").innerHTML = Object.keys(r.by_channel).map(ch => {
                const c = r.by_channel[ch];
                return `<tr><td><b>${ch.toUpperCase()}</b></td><td>${c.DELIVERED}</td><td>${c.FAILED}</td><td>${c.SKIPPED}</td></tr>`;
            }).join("");

            document.getElementById("t-platform").innerHTML = Object.keys(r.by_platform).map(p => {
                const x = r.by_platform[p];
                const pct = x.total ? Math.round(100 * x.delivered / x.total) : 0;
                return `<tr><td><span class="badge ${p}">${p}</span></td><td>${x.total}</td><td>${x.delivered}</td>
                        <td>${pct}%<div class="bar"><i style="width:${pct}%"></i></div></td></tr>`;
            }).join("");

            document.getElementById("t-status").innerHTML = Object.keys(r.by_status).map(s =>
                `<span class="badge ${s}" style="margin:2px;">${s}: ${r.by_status[s]}</span>`).join("") ||
                `<span class="muted">No notifications yet.</span>`;
            document.getElementById("t-template").innerHTML = Object.keys(r.by_template).map(t =>
                `<span class="pill">${esc(t)}: ${r.by_template[t]}</span>`).join("");

            document.getElementById("t-db").innerHTML = Object.keys(r.databases).map(k =>
                `<span class="pill">${k}DB: ${r.databases[k]}</span>`).join("");

            document.getElementById("t-recent").innerHTML = r.recent.map(n =>
                `<tr><td><code>${n.id}</code></td><td>${esc(n.client)}</td><td>${esc(n.template)}</td>
                 <td><span class="badge ${n.status}">${n.status}</span></td>
                 <td>${n.e2e_ms === null ? "-" : n.e2e_ms + " ms"}</td></tr>`).join("");
        }

        async function simulate() {
            const count = Number(document.getElementById("sim-count").value) || 500;
            const res = await api("POST", "/v1/simulate", { count: count });
            toast(`Enqueued ${res.enqueued} in ${res.enqueue_seconds}s`);
            loadReport();
        }

        async function resetAll() {
            await api("POST", "/v1/reset");
            toast("Notification data cleared");
            loadReport();
        }

        function refreshAll() {
            if (document.getElementById("dashboard").classList.contains("active")) loadReport();
        }

        loadBase().then(loadReport);
        setInterval(refreshAll, 1500);
    </script>
</body>

</html>
"""


@app.get("/")
def home():
    return Response(PAGE, mimetype="text/html")


if "SERVER" in globals():
    try:
        SERVER.shutdown()
    except Exception:
        pass

SERVER = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=SERVER.serve_forever, daemon=True).start()
print("Notification System running on port", PORT)

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, width="100%", height="950")
except Exception as exc:
    print("Not running inside Colab (" + str(exc) + "). Open http://localhost:" + str(PORT))