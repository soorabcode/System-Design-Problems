''' 
Job Scheduler Platfrom 

1. Requirement analysis 
A. functional requrement 
- create / schedule a job
- maintain / monitor status of job
- cancel a scheduled job

B. non functional requrement 
- scale : 10k jobs per seconds 
- Cap theroem - availabe >>>>>>>>>>> consistent 

2. Identifying Entities 
- job 
- schedule 
- executor 

3. API 
A. Create / Schedule a Job
POST /v1/api/jobs -> create a job 
B. Maintain / Monitor Status of Job
GET /v1/api/jobs{jobsID} -> status of job with details 
C. Cancel a Scheduled Job
POST /v1/jobs{jobsID}/cancel -> cancel a job 

4. High level design 
user -> api gateway -> sever ( job service , job search ) -> database  

5. low level design 
uploaded image

'''


import json
import logging
import queue
import random
import sqlite3
import threading
import time
import uuid
import zlib
import copy
from collections import Counter, deque
from datetime import datetime

from flask import Flask, Response, jsonify, request
from werkzeug.serving import make_server

try:
    _stop_event.set()
    _server.shutdown()
    _server.server_close()
    print("Previous instance stopped.")
except NameError:
    pass
_stop_event = threading.Event()

PORT = 8000
NUM_PARTITIONS = 3
NUM_WORKERS = 4
CONSUMER_DELAY = 0.3
RATE_LIMIT = 300
RATE_WINDOW = 10
MAX_AHEAD = 365 * 24 * 3600
JOB_TYPES = ("log", "email", "http_ping", "compute", "sleep", "fail")
CANCELLABLE = ("PENDING", "SCHEDULED", "QUEUED")

logging.getLogger("werkzeug").setLevel(logging.ERROR)


class JobDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.conn.execute(
                """
                CREATE TABLE jobs (
                    id          TEXT PRIMARY KEY,
                    name        TEXT,
                    job_type    TEXT,
                    run_at      REAL NOT NULL,
                    payload     TEXT,
                    status      TEXT NOT NULL,
                    attempts    INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 1,
                    created_at  REAL,
                    updated_at  REAL,
                    started_at  REAL,
                    finished_at REAL,
                    worker      TEXT,
                    result      TEXT,
                    events      TEXT
                )
                """
            )
            self.conn.execute("CREATE INDEX idx_due ON jobs(status, run_at)")
            self.conn.commit()

    @staticmethod
    def _row(row):
        if row is None:
            return None
        job = dict(row)
        job["payload"] = json.loads(job["payload"] or "{}")
        job["events"] = json.loads(job["events"] or "[]")
        job["result"] = json.loads(job["result"]) if job["result"] else None
        return job

    def insert_many(self, jobs):
        with self.lock:
            self.conn.executemany(
                """
                INSERT OR REPLACE INTO jobs
                (id, name, job_type, run_at, payload, status, attempts, max_retries,
                 created_at, updated_at, started_at, finished_at, worker, result, events)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        j["id"], j["name"], j["job_type"], j["run_at"],
                        json.dumps(j["payload"]), j["status"], j["attempts"], j["max_retries"],
                        j["created_at"], j["updated_at"], j["started_at"], j["finished_at"],
                        j["worker"], json.dumps(j["result"]) if j["result"] else None,
                        json.dumps(j["events"]),
                    )
                    for j in jobs
                ],
            )
            self.conn.commit()

    def get(self, job_id):
        with self.lock:
            row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row(row)

    def list_all(self, limit=500):
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._row(r) for r in rows]

    def counts(self):
        with self.lock:
            rows = self.conn.execute(
                "SELECT status, COUNT(*) AS c FROM jobs GROUP BY status"
            ).fetchall()
        return {r["status"]: r["c"] for r in rows}

    def transition(self, job_id, expected, new_status, note="", **fields):
        with self.lock:
            row = self.conn.execute(
                "SELECT status, events FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if row is None or row["status"] not in expected:
                return False
            now = time.time()
            events = json.loads(row["events"] or "[]")
            events.append({"status": new_status, "at": now, "note": note})
            fields.update(status=new_status, updated_at=now, events=json.dumps(events))
            columns = ", ".join(f"{key}=?" for key in fields)
            self.conn.execute(
                f"UPDATE jobs SET {columns} WHERE id=?", (*fields.values(), job_id)
            )
            self.conn.commit()
            return True

    def claim_due(self, now, limit=200):
        with self.lock:
            ids = [
                r["id"]
                for r in self.conn.execute(
                    "SELECT id FROM jobs WHERE status='SCHEDULED' AND run_at<=? "
                    "ORDER BY run_at LIMIT ?",
                    (now, limit),
                )
            ]
        claimed = []
        for job_id in ids:
            if self.transition(job_id, ("SCHEDULED",), "QUEUED", "Picked up by Execution Service"):
                claimed.append(job_id)
        return claimed


db = JobDB()


class MiniKafka:
    def __init__(self, partitions):
        self.partitions = [queue.Queue() for _ in range(partitions)]
        self.produced = [0] * partitions
        self.consumed = [0] * partitions
        self.lock = threading.Lock()

    def publish(self, key, value):
        index = zlib.crc32(key.encode()) % len(self.partitions)
        self.partitions[index].put(value)
        with self.lock:
            self.produced[index] += 1

    def mark_consumed(self, index, count):
        with self.lock:
            self.consumed[index] += count

    def snapshot(self):
        with self.lock:
            return [
                {
                    "id": i,
                    "produced": self.produced[i],
                    "consumed": self.consumed[i],
                    "lag": self.partitions[i].qsize(),
                }
                for i in range(len(self.partitions))
            ]


kafka = MiniKafka(NUM_PARTITIONS)

pending = {}
pending_lock = threading.RLock()

metrics_lock = threading.Lock()
accepted_total = 0
accepted_per_sec = Counter()


def record_accept():
    global accepted_total
    now = int(time.time())
    with metrics_lock:
        accepted_total += 1
        accepted_per_sec[now] += 1
        if len(accepted_per_sec) > 40:
            for key in [k for k in accepted_per_sec if k < now - 30]:
                del accepted_per_sec[key]


def validate_payload(job_type, payload):
    if len(json.dumps(payload)) > 4000:
        return "Payload too large (max 4000 bytes)"
    try:
        if job_type == "email" and not payload.get("to"):
            return "email jobs need payload.to"
        if job_type == "http_ping" and not str(payload.get("url", "")).startswith(("http://", "https://")):
            return "http_ping jobs need payload.url starting with http:// or https://"
        if job_type == "compute" and int(payload.get("n", 10000)) < 2:
            return "compute jobs need payload.n >= 2"
        if job_type == "sleep" and float(payload.get("seconds", 3)) < 0:
            return "sleep jobs need payload.seconds >= 0"
    except (TypeError, ValueError):
        return "Invalid payload values for this job type"
    return None


def resolve_run_at(body, now):
    if body.get("delay_seconds") not in (None, ""):
        delay = float(body["delay_seconds"])
        if delay < 0 or delay > MAX_AHEAD:
            raise ValueError("delay_seconds out of range")
        return now + delay
    if body.get("run_at") not in (None, ""):
        value = body["run_at"]
        if isinstance(value, (int, float)):
            timestamp = float(value)
        else:
            timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        if timestamp > now + MAX_AHEAD:
            raise ValueError("run_at is too far in the future")
        return timestamp
    return now


def create_job(body):
    name = str(body.get("name", "")).strip()
    job_type = body.get("job_type", "log")
    payload = body.get("payload", {})
    if not 1 <= len(name) <= 60:
        return None, "name is required (1-60 characters)", 400
    if job_type not in JOB_TYPES:
        return None, f"job_type must be one of {', '.join(JOB_TYPES)}", 400
    if not isinstance(payload, dict):
        return None, "payload must be a JSON object", 400
    problem = validate_payload(job_type, payload)
    if problem:
        return None, problem, 400
    try:
        max_retries = int(body.get("max_retries", 1))
        if not 0 <= max_retries <= 3:
            raise ValueError
    except (TypeError, ValueError):
        return None, "max_retries must be between 0 and 3", 400

    now = time.time()
    try:
        run_at = resolve_run_at(body, now)
    except (TypeError, ValueError) as exc:
        return None, f"Invalid schedule: {exc}", 400

    job = {
        "id": "job_" + uuid.uuid4().hex[:12],
        "name": name,
        "job_type": job_type,
        "run_at": run_at,
        "payload": payload,
        "status": "PENDING",
        "attempts": 0,
        "max_retries": max_retries,
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "finished_at": None,
        "worker": None,
        "result": None,
        "events": [{"status": "PENDING", "at": now, "note": "Accepted by Job Service, published to Kafka"}],
    }
    with pending_lock:
        pending[job["id"]] = job
    kafka.publish(job["id"], job)
    record_accept()
    return job, None, 202


def get_job(job_id):
    with pending_lock:
        job = pending.get(job_id)
        job = copy.deepcopy(job) if job is not None else db.get(job_id)
    return job


def list_jobs(status=None, limit=200):
    with pending_lock:
        merged = [copy.deepcopy(j) for j in pending.values()]
        seen = {j["id"] for j in merged}
        merged += [j for j in db.list_all() if j["id"] not in seen]
    if status:
        merged = [j for j in merged if j["status"] == status]
    merged.sort(key=lambda j: j["created_at"], reverse=True)
    return merged[:limit]


def cancel_job(job_id):
    with pending_lock:
        job = pending.get(job_id)
        if job is not None:
            if job["status"] != "PENDING":
                return None, f"Job is already {job['status']}", 409
            now = time.time()
            job["status"] = "CANCELLED"
            job["updated_at"] = now
            job["events"].append({"status": "CANCELLED", "at": now, "note": "Cancelled before it reached the DB"})
            return copy.deepcopy(job), None, 200
        current = db.get(job_id)
    if current is None:
        return None, "Job not found", 404
    if db.transition(job_id, CANCELLABLE, "CANCELLED", "Cancelled by user", finished_at=time.time()):
        return db.get(job_id), None, 200
    current = db.get(job_id)
    return None, f"Cannot cancel a job that is {current['status']}", 409


def public_job(job):
    keys = (
        "id", "name", "job_type", "payload", "status", "run_at", "created_at", "updated_at",
        "started_at", "finished_at", "attempts", "max_retries", "worker", "result", "events",
    )
    return {key: job[key] for key in keys}


def persist_batch(batch):
    with pending_lock:
        rows = []
        for message in batch:
            job = pending.get(message["id"], message)
            if job["status"] != "CANCELLED":
                now = time.time()
                job["status"] = "SCHEDULED"
                job["updated_at"] = now
                job["events"].append({"status": "SCHEDULED", "at": now, "note": "Persisted to Job DB by consumer"})
            rows.append(job)
        db.insert_many(rows)
        for message in batch:
            pending.pop(message["id"], None)


def consumer_loop(partition_index, stop):
    source = kafka.partitions[partition_index]
    while not stop.is_set():
        try:
            batch = [source.get(timeout=0.5)]
        except queue.Empty:
            continue
        while len(batch) < 200:
            try:
                batch.append(source.get_nowait())
            except queue.Empty:
                break
        stop.wait(CONSUMER_DELAY)
        persist_batch(batch)
        kafka.mark_consumed(partition_index, len(batch))


def execute_job(job):
    job_type, payload = job["job_type"], job["payload"]
    if job_type == "log":
        time.sleep(0.3)
        return {"logged": str(payload.get("message", ""))[:200]}
    if job_type == "email":
        time.sleep(random.uniform(0.3, 0.8))
        return {"sent_to": payload.get("to"), "subject": payload.get("subject", "(no subject)"), "simulated": True}
    if job_type == "http_ping":
        latency = random.randint(40, 400)
        time.sleep(latency / 1000)
        return {"url": payload.get("url"), "status": 200, "latency_ms": latency, "simulated": True}
    if job_type == "compute":
        n = max(2, min(int(payload.get("n", 10000)), 2_000_000))
        sieve = bytearray([1]) * (n + 1)
        sieve[0] = sieve[1] = 0
        for i in range(2, int(n ** 0.5) + 1):
            if sieve[i]:
                sieve[i * i::i] = bytearray(len(range(i * i, n + 1, i)))
        return {"n": n, "prime_count": sum(sieve)}
    if job_type == "sleep":
        seconds = min(float(payload.get("seconds", 3)), 30)
        time.sleep(seconds)
        return {"slept_seconds": seconds}
    raise RuntimeError(str(payload.get("reason", "Simulated failure")))


class Worker(threading.Thread):
    def __init__(self, name, stop):
        super().__init__(daemon=True)
        self.worker_name = name
        self.stop = stop
        self.inbox = queue.Queue()
        self.busy_job = None
        self.processed = 0
        self.failed = 0

    def run(self):
        while not self.stop.is_set():
            try:
                job_id = self.inbox.get(timeout=0.5)
            except queue.Empty:
                continue
            job = db.get(job_id)
            if job is None:
                continue
            attempt = job["attempts"] + 1
            started = db.transition(
                job_id, ("QUEUED",), "RUNNING", f"Started on {self.worker_name} (attempt {attempt})",
                started_at=time.time(), worker=self.worker_name, attempts=attempt,
            )
            if not started:
                continue
            self.busy_job = job_id
            try:
                result = execute_job(job)
                db.transition(
                    job_id, ("RUNNING",), "COMPLETED", "Finished successfully",
                    finished_at=time.time(), result=json.dumps(result),
                )
                self.processed += 1
            except Exception as exc:
                self.failed += 1
                if attempt <= job["max_retries"]:
                    delay = 2 ** attempt
                    db.transition(
                        job_id, ("RUNNING",), "SCHEDULED",
                        f"Failed ({exc}); retry {attempt}/{job['max_retries']} in {delay}s",
                        run_at=time.time() + delay,
                    )
                else:
                    db.transition(
                        job_id, ("RUNNING",), "FAILED", f"Failed: {exc}",
                        finished_at=time.time(), result=json.dumps({"error": str(exc)}),
                    )
            finally:
                self.busy_job = None

    def snapshot(self):
        return {
            "name": self.worker_name,
            "alive": self.is_alive(),
            "busy_job": self.busy_job,
            "queue": self.inbox.qsize(),
            "processed": self.processed,
            "failed": self.failed,
        }


class Coordinator:

    def __init__(self, workers):
        self.workers = workers

    def dispatch(self, job_id):
        healthy = [w for w in self.workers if w.is_alive()]
        if not healthy:
            return
        target = min(healthy, key=lambda w: w.inbox.qsize() + (1 if w.busy_job else 0))
        target.inbox.put(job_id)


def execution_service_loop(coordinator, stop):
    while not stop.is_set():
        due = db.claim_due(time.time())
        for job_id in due:
            coordinator.dispatch(job_id)
        if not due:
            stop.wait(0.3)


workers = [Worker(f"Worker-{i + 1}", _stop_event) for i in range(NUM_WORKERS)]
coordinator = Coordinator(workers)

for _worker in workers:
    _worker.start()
for _index in range(NUM_PARTITIONS):
    threading.Thread(target=consumer_loop, args=(_index, _stop_event), daemon=True).start()
threading.Thread(target=execution_service_loop, args=(coordinator, _stop_event), daemon=True).start()


rate_hits = {}
rate_lock = threading.Lock()


def rate_allowed(client):
    now = time.time()
    with rate_lock:
        hits = rate_hits.setdefault(client, deque())
        while hits and hits[0] < now - RATE_WINDOW:
            hits.popleft()
        if len(hits) >= RATE_LIMIT:
            return False
        hits.append(now)
        return True


app = Flask(__name__)
PUBLIC_ENDPOINTS = {"index", "health"}


@app.before_request
def api_gateway():
    if request.endpoint is None or request.endpoint in PUBLIC_ENDPOINTS:
        return None
    if not rate_allowed(request.remote_addr or "client"):
        return jsonify(error="Rate limit exceeded, slow down"), 429
    return None


@app.get("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


@app.get("/health")
def health():
    return jsonify(status="ok")


@app.post("/v1/api/jobs")
def api_create_job():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify(error="Body must be a JSON object"), 400
    job, error, code = create_job(body)
    if error:
        return jsonify(error=error), code
    return jsonify(public_job(job)), 202


@app.get("/v1/api/jobs")
def api_list_jobs():
    status = request.args.get("status", "").upper() or None
    jobs = list_jobs(status)
    return jsonify(jobs=[public_job(j) for j in jobs])


@app.get("/v1/api/jobs/<job_id>")
def api_get_job(job_id):
    job = get_job(job_id)
    if job is None:
        return jsonify(error="Job not found"), 404
    return jsonify(public_job(job))


@app.post("/v1/api/jobs/<job_id>/cancel")
@app.post("/v1/jobs/<job_id>/cancel")
def api_cancel_job(job_id):
    job, error, code = cancel_job(job_id)
    if error:
        return jsonify(error=error), code
    return jsonify(public_job(job)), 200


@app.get("/v1/api/system/stats")
def api_stats():
    now = time.time()
    with pending_lock:
        counts = Counter(db.counts())
        for job in pending.values():
            counts[job["status"]] += 1
    with metrics_lock:
        recent = sum(accepted_per_sec.get(int(now) - i, 0) for i in range(1, 6)) / 5
        total = accepted_total
    return jsonify(
        server_time=now,
        jobs=dict(counts),
        kafka=kafka.snapshot(),
        workers=[w.snapshot() for w in workers],
        accepted_total=total,
        accepted_per_sec=round(recent, 1),
    )


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Job Scheduler</title>
  <style>
    body {
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      font-size: 14px;
      color: #222222;
      background: #ffffff;
    }

    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: bold;
    }

    h2 {
      margin: 20px 0 8px;
      font-size: 14px;
      font-weight: bold;
      border-bottom: 1px solid #cccccc;
      padding-bottom: 4px;
    }

    .hidden {
      display: none;
    }

    label {
      display: block;
      margin-top: 10px;
    }

    input[type=text],
    input[type=number],
    input[type=datetime-local],
    select,
    textarea {
      display: block;
      width: 100%;
      margin-top: 3px;
      padding: 5px 6px;
      border: 1px solid #999999;
      border-radius: 2px;
      font-size: 14px;
      font-family: inherit;
      box-sizing: border-box;
      background: #ffffff;
    }

    textarea {
      min-height: 80px;
      font-family: Consolas, Menlo, monospace;
      font-size: 13px;
    }

    button {
      padding: 5px 12px;
      border: 1px solid #777777;
      border-radius: 2px;
      background: #f0f0f0;
      font-size: 14px;
      font-family: inherit;
      cursor: pointer;
    }

    button:hover {
      background: #e2e2e2;
    }

    table {
      width: 100%;
      border-collapse: collapse;
    }

    th,
    td {
      padding: 5px 8px;
      border: 1px solid #cccccc;
      text-align: left;
      vertical-align: top;
    }

    th {
      background: #f3f3f3;
      font-weight: bold;
      white-space: nowrap;
    }

    tr.job-row {
      cursor: pointer;
    }

    tr.job-row:hover td {
      background: #f7f7f7;
    }

    tr.selected td {
      background: #eaf1fb;
    }

    pre {
      margin: 0;
      padding: 8px;
      border: 1px solid #cccccc;
      background: #f7f7f7;
      font-size: 12px;
      overflow-x: auto;
    }

    .mono {
      font-family: Consolas, Menlo, monospace;
      font-size: 12px;
    }

    .muted {
      color: #666666;
    }

    .error {
      margin-top: 10px;
      color: #aa0000;
    }

    .bar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 20px;
      border-bottom: 1px solid #999999;
      background: #f3f3f3;
    }

    .message {
      min-height: 20px;
      padding: 6px 20px;
      font-size: 13px;
    }

    .message.ok {
      color: #1a6b1a;
    }

    .message.err {
      color: #aa0000;
    }

    .page {
      display: grid;
      grid-template-columns: 340px 1fr;
      gap: 24px;
      padding: 0 20px 40px;
      align-items: start;
    }

    .inline {
      display: flex;
      gap: 12px;
      align-items: center;
      margin-top: 6px;
    }

    .inline label {
      display: inline;
      margin: 0;
    }

    .inline label input {
      margin-right: 4px;
    }

    .two {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }

    .buttons {
      margin-top: 14px;
      display: flex;
      gap: 8px;
    }

    .toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin-bottom: 8px;
    }

    .toolbar select {
      width: auto;
      margin: 0;
    }

    .st-FAILED {
      color: #aa0000;
    }

    .st-COMPLETED {
      color: #1a6b1a;
    }

    .st-RUNNING {
      font-weight: bold;
    }

    .st-CANCELLED {
      color: #777777;
    }

    .details-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .details-head h2 {
      flex: 1;
    }

    .details-actions {
      display: flex;
      gap: 8px;
      margin-top: 12px;
    }

    .details table {
      margin-bottom: 10px;
    }

    .details th {
      width: 130px;
    }

    @media (max-width: 900px) {
      .page {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <section id="appView">
    <header class="bar">
      <h1>Job Scheduler</h1>
    </header>

    <div id="message" class="message"></div>

    <main class="page">
      <div>
        <h2 style="margin-top: 0;">New job</h2>

        <form id="jobForm" onsubmit="submitJob(event)" autocomplete="off">
          <label for="jobName">Name
            <input id="jobName" type="text" required maxlength="60">
          </label>

          <div class="two">
            <label for="jobType">Type
              <select id="jobType" onchange="applyTemplate()">
                <option value="log">log</option>
                <option value="email">email (simulated)</option>
                <option value="http_ping">http_ping (simulated)</option>
                <option value="compute">compute</option>
                <option value="sleep">sleep</option>
                <option value="fail">fail</option>
              </select>
            </label>

            <label for="maxRetries">Max retries
              <select id="maxRetries">
                <option value="0">0</option>
                <option value="1" selected>1</option>
                <option value="2">2</option>
                <option value="3">3</option>
              </select>
            </label>
          </div>

          <label for="payload">Payload (JSON)
            <textarea id="payload" spellcheck="false"></textarea>
          </label>

          <label>Run
            <div class="inline">
              <label><input type="radio" name="mode" value="now" checked onchange="toggleMode()">now</label>
              <label><input type="radio" name="mode" value="delay" onchange="toggleMode()">after delay</label>
              <label><input type="radio" name="mode" value="at" onchange="toggleMode()">at time</label>
            </div>
          </label>

          <label id="delayRow" class="hidden" for="delaySeconds">Delay in seconds
            <input id="delaySeconds" type="number" min="0" value="20">
          </label>

          <label id="atRow" class="hidden" for="runAt">Run at (local time)
            <input id="runAt" type="datetime-local">
          </label>

          <div class="buttons">
            <button type="submit">Schedule job</button>
            <button type="button" onclick="burstTest()">Submit 100 test jobs</button>
          </div>
        </form>

        <h2>System</h2>
        <p class="muted" style="margin: 0 0 8px;">
          Accepted <span id="acceptedTotal">0</span> jobs, <span id="acceptedRate">0</span> per second
        </p>

        <table>
          <thead>
            <tr><th>Partition</th><th>Lag</th><th>In</th><th>Out</th></tr>
          </thead>
          <tbody id="kafkaRows"></tbody>
        </table>

        <table style="margin-top: 10px;">
          <thead>
            <tr><th>Worker</th><th>State</th><th>Queue</th><th>Done</th><th>Failed</th></tr>
          </thead>
          <tbody id="workerRows"></tbody>
        </table>
      </div>

      <div>
        <h2 style="margin-top: 0;">Summary</h2>
        <table>
          <thead id="summaryHead"></thead>
          <tbody id="summaryBody"></tbody>
        </table>

        <h2>Jobs</h2>
        <div class="toolbar">
          <span class="muted">Refreshes every 1.5 seconds. Select a row for details.</span>
          <span>
            <select id="statusFilter" onchange="setFilter()">
              <option value="">All statuses</option>
              <option>PENDING</option>
              <option>SCHEDULED</option>
              <option>QUEUED</option>
              <option>RUNNING</option>
              <option>COMPLETED</option>
              <option>FAILED</option>
              <option>CANCELLED</option>
            </select>
            <button type="button" onclick="refresh()">Refresh</button>
          </span>
        </div>

        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Name</th>
              <th>Type</th>
              <th>Scheduled for</th>
              <th>Status</th>
              <th>Worker</th>
              <th></th>
            </tr>
          </thead>
          <tbody id="jobRows"></tbody>
        </table>

        <div id="details" class="details hidden">
          <div class="details-head">
            <h2 id="detailsTitle"></h2>
          </div>
          <div id="detailsBody"></div>
          <div class="details-actions">
            <button id="detailsCancel" type="button" onclick="cancelOpenJob()">Cancel job</button>
            <button type="button" onclick="closeDetails()">Close</button>
          </div>
        </div>
      </div>
    </main>
  </section>

  <script>
    const CANCELLABLE = ['PENDING', 'SCHEDULED', 'QUEUED'];
    const STATUSES = ['PENDING', 'SCHEDULED', 'QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'];
    const TEMPLATES = {
      log: { message: 'Hello from the scheduler' },
      email: { to: 'user@example.com', subject: 'Scheduled report', body: 'Your report is ready.' },
      http_ping: { url: 'https://example.com/health' },
      compute: { n: 500000 },
      sleep: { seconds: 5 },
      fail: { reason: 'Simulated failure' }
    };

    const state = {
      filter: '',
      openJobId: null,
      offset: 0,
      busy: false,
      jobs: []
    };

    const $ = (id) => document.getElementById(id);

    function esc(value) {
      return String(value === null || value === undefined ? '' : value).replace(/[&<>"']/g, (c) => (
        { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
      ));
    }

    async function api(method, path, body) {
      const response = await fetch(path, {
        method: method,
        headers: { 'Content-Type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined
      });
      let data = {};
      try {
        data = await response.json();
      } catch (e) {
        data = {};
      }
      if (!response.ok) {
        const error = new Error(data.error || ('HTTP ' + response.status));
        error.status = response.status;
        throw error;
      }
      return data;
    }

    function notify(text, kind) {
      const box = $('message');
      box.textContent = text;
      box.className = 'message ' + (kind || 'ok');
      clearTimeout(notify.timer);
      notify.timer = setTimeout(() => {
        box.textContent = '';
      }, 5000);
    }

    function applyTemplate() {
      $('payload').value = JSON.stringify(TEMPLATES[$('jobType').value], null, 2);
    }

    function toggleMode() {
      const mode = document.querySelector('input[name=mode]:checked').value;
      $('delayRow').classList.toggle('hidden', mode !== 'delay');
      $('atRow').classList.toggle('hidden', mode !== 'at');
    }

    async function submitJob(event) {
      event.preventDefault();
      let payload;
      try {
        payload = JSON.parse($('payload').value || '{}');
      } catch (e) {
        notify('Payload must be valid JSON', 'err');
        return;
      }
      const body = {
        name: $('jobName').value.trim(),
        job_type: $('jobType').value,
        payload: payload,
        max_retries: Number($('maxRetries').value)
      };
      const mode = document.querySelector('input[name=mode]:checked').value;
      if (mode === 'delay') {
        body.delay_seconds = Number($('delaySeconds').value);
      }
      if (mode === 'at') {
        const value = $('runAt').value;
        if (!value) {
          notify('Choose a date and time', 'err');
          return;
        }
        body.run_at = new Date(value).getTime() / 1000;
      }
      try {
        const job = await api('POST', '/v1/api/jobs', body);
        notify('Accepted ' + job.id + ' (202)', 'ok');
        $('jobName').value = '';
        refresh();
      } catch (error) {
        notify(error.message, 'err');
      }
    }

    async function burstTest() {
      const started = performance.now();
      const calls = [];
      for (let i = 1; i <= 100; i++) {
        calls.push(api('POST', '/v1/api/jobs', {
          name: 'Test job ' + i,
          job_type: 'log',
          payload: { message: 'test ' + i },
          delay_seconds: Math.floor(Math.random() * 10) + 1
        }));
      }
      const results = await Promise.allSettled(calls);
      const accepted = results.filter((r) => r.status === 'fulfilled').length;
      const seconds = ((performance.now() - started) / 1000).toFixed(2);
      notify(accepted + ' of 100 jobs accepted in ' + seconds + ' seconds', 'ok');
      refresh();
    }

    async function cancelJob(jobId) {
      try {
        await api('POST', '/v1/api/jobs/' + jobId + '/cancel');
        notify('Cancelled ' + jobId, 'ok');
      } catch (error) {
        notify(error.message, 'err');
      }
      refresh();
    }

    function cancelOpenJob() {
      if (state.openJobId) {
        cancelJob(state.openJobId);
      }
    }

    function fmtTime(ts) {
      if (!ts) {
        return '-';
      }
      return new Date(ts * 1000).toLocaleString([], {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
      });
    }

    function fmtDuration(seconds) {
      seconds = Math.ceil(seconds);
      if (seconds < 60) {
        return seconds + 's';
      }
      if (seconds < 3600) {
        return Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's';
      }
      return Math.floor(seconds / 3600) + 'h ' + Math.floor((seconds % 3600) / 60) + 'm';
    }

    function statusText(status) {
      return '<span class="st-' + esc(status) + '">' + esc(status) + '</span>';
    }

    function countdown(job) {
      if (job.status !== 'PENDING' && job.status !== 'SCHEDULED') {
        return '';
      }
      const diff = job.run_at - (Date.now() / 1000 + state.offset);
      return diff > 0 ? 'in ' + fmtDuration(diff) : 'due';
    }

    function renderStats(stats) {
      $('summaryHead').innerHTML = '<tr>' + STATUSES.map((s) => '<th>' + s + '</th>').join('') + '</tr>';
      $('summaryBody').innerHTML = '<tr>' + STATUSES.map((s) => '<td>' + (stats.jobs[s] || 0) + '</td>').join('') + '</tr>';

      $('acceptedTotal').textContent = stats.accepted_total;
      $('acceptedRate').textContent = stats.accepted_per_sec;

      $('kafkaRows').innerHTML = stats.kafka.map((p) => (
        '<tr><td>' + p.id + '</td><td>' + p.lag + '</td><td>' + p.produced + '</td><td>' + p.consumed + '</td></tr>'
      )).join('');

      $('workerRows').innerHTML = stats.workers.map((w) => {
        const label = !w.alive ? 'down' : (w.busy_job ? 'running' : 'idle');
        return '<tr><td>' + esc(w.name) + '</td><td>' + label + '</td><td>' + w.queue +
          '</td><td>' + w.processed + '</td><td>' + w.failed + '</td></tr>';
      }).join('');
    }

    function renderJobs() {
      const body = $('jobRows');
      if (!state.jobs.length) {
        body.innerHTML = '<tr><td colspan="7" class="muted">No jobs.</td></tr>';
        return;
      }
      body.innerHTML = state.jobs.map((job) => {
        const cancelButton = CANCELLABLE.indexOf(job.status) >= 0
          ? '<button type="button" data-cancel="' + esc(job.id) + '">Cancel</button>'
          : '';
        const selected = job.id === state.openJobId ? ' selected' : '';
        return '<tr class="job-row' + selected + '" data-id="' + esc(job.id) + '">' +
          '<td class="mono">' + esc(job.id) + '</td>' +
          '<td>' + esc(job.name) + '</td>' +
          '<td>' + esc(job.job_type) + '</td>' +
          '<td>' + fmtTime(job.run_at) + ' <span class="muted">' + countdown(job) + '</span></td>' +
          '<td>' + statusText(job.status) + '</td>' +
          '<td>' + esc(job.worker || '-') + '</td>' +
          '<td>' + cancelButton + '</td>' +
          '</tr>';
      }).join('');
    }

    function renderDetails(job) {
      $('detailsTitle').textContent = 'Job ' + job.id + ': ' + job.name;
      $('detailsCancel').classList.toggle('hidden', CANCELLABLE.indexOf(job.status) < 0);

      const history = job.events.map((e) => (
        '<tr><td>' + fmtTime(e.at) + '</td><td>' + statusText(e.status) + '</td><td>' + esc(e.note) + '</td></tr>'
      )).join('');

      $('detailsBody').innerHTML =
        '<table>' +
        '<tr><th>Status</th><td>' + statusText(job.status) + ' <span class="muted">' + countdown(job) + '</span></td></tr>' +
        '<tr><th>Type</th><td>' + esc(job.job_type) + '</td></tr>' +
        '<tr><th>Scheduled for</th><td>' + fmtTime(job.run_at) + '</td></tr>' +
        '<tr><th>Created</th><td>' + fmtTime(job.created_at) + '</td></tr>' +
        '<tr><th>Started</th><td>' + fmtTime(job.started_at) + '</td></tr>' +
        '<tr><th>Finished</th><td>' + fmtTime(job.finished_at) + '</td></tr>' +
        '<tr><th>Worker</th><td>' + esc(job.worker || '-') + '</td></tr>' +
        '<tr><th>Attempts</th><td>' + job.attempts + ' (max retries ' + job.max_retries + ')</td></tr>' +
        '</table>' +
        '<table><tr><th>Payload</th><td><pre>' + esc(JSON.stringify(job.payload, null, 2)) + '</pre></td></tr>' +
        '<tr><th>Result</th><td><pre>' + esc(job.result ? JSON.stringify(job.result, null, 2) : 'none yet') + '</pre></td></tr></table>' +
        '<table><thead><tr><th>Time</th><th>Status</th><th>Note</th></tr></thead><tbody>' + history + '</tbody></table>';
    }

    async function openJob(jobId) {
      state.openJobId = jobId;
      $('details').classList.remove('hidden');
      $('detailsBody').textContent = 'Loading...';
      try {
        renderDetails(await api('GET', '/v1/api/jobs/' + jobId));
      } catch (error) {
        $('detailsBody').innerHTML = '<div class="error">' + esc(error.message) + '</div>';
      }
      renderJobs();
    }

    function closeDetails() {
      state.openJobId = null;
      $('details').classList.add('hidden');
      renderJobs();
    }

    $('jobRows').addEventListener('click', (event) => {
      const cancelId = event.target.getAttribute('data-cancel');
      if (cancelId) {
        cancelJob(cancelId);
        return;
      }
      const row = event.target.closest('tr[data-id]');
      if (row) {
        openJob(row.getAttribute('data-id'));
      }
    });

    function setFilter() {
      state.filter = $('statusFilter').value;
      refresh();
    }

    async function refresh() {
      if (state.busy) {
        return;
      }
      state.busy = true;
      try {
        const query = state.filter ? '?status=' + state.filter : '';
        const results = await Promise.all([
          api('GET', '/v1/api/jobs' + query),
          api('GET', '/v1/api/system/stats')
        ]);
        state.jobs = results[0].jobs;
        state.offset = results[1].server_time - Date.now() / 1000;
        renderStats(results[1]);
        renderJobs();
        if (state.openJobId) {
          renderDetails(await api('GET', '/v1/api/jobs/' + state.openJobId));
        }
      } catch (error) {
        state.busy = false;
      } finally {
        state.busy = false;
      }
    }

    setInterval(refresh, 1500);

    applyTemplate();
    refresh();
  </script>
</body>
</html>
"""


_server = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=_server.serve_forever, daemon=True).start()
print(f"Job Scheduler running on port {PORT}")

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height=950)
except ImportError:
    print(f"Not running in Colab. Open http://127.0.0.1:{PORT} in your browser.")