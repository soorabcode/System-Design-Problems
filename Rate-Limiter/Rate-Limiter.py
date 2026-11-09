
'''
Rate Limiter : System Design Notes

1. Requirement Analysis

Functional Requirements

* Limit the rate of number of requests.
* Rules for rate limiting.
* Return 429 for too many requests.
* Should handle burst requests.

Non-Functional Requirements

* Scale: 10K requests per second.
* CAP Theorem: Availability >>> Consistency.
* Latency: Ultra low.

2. Identifying Entities

* User
* Rules
* Request Counter

3. API Design

Admin API

POST /v1/admin/{adminId}/rules

Rules API

GET /v1/admin/rules/rate-limit

Return 429 API

GET /v1/rules/rate-limit?req=4993

4. High Level Design
user -> api gateway -> sever ( ) -> database ( )

5. low level design 
![Rate-Limiter Low-Level Design](Rate-Limiter.jpg)
https://github.com/soorabcode/System-Design-Problems/tree/main/Rate-Limiter

'''
import math
import re
import secrets
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from functools import wraps

from flask import Flask, g, jsonify, make_response, request
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.serving import make_server

HOST = "127.0.0.1"
PORT = 8050

SESSION_TTL_SECONDS = 30 * 60
MAX_FAILED_LOGINS = 5
LOCKOUT_SECONDS = 5 * 60
LOGIN_ATTEMPT_LIMIT = 10
LOGIN_ATTEMPT_WINDOW = 60

USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_]{3,20}$")
PASSWORD_PATTERN = re.compile(r"^(?=.*[A-Za-z])(?=.*\d).{8,128}$")
REQUEST_ID_PATTERN = re.compile(r"^\d{1,12}$")
ALGORITHMS = ("token_bucket", "sliding_window")


@dataclass
class Rule:
    rule_id: int
    name: str
    algorithm: str
    limit: int
    window: int
    burst: int
    subject: str


@dataclass
class Decision:
    allowed: bool
    remaining: int
    retry_after: float
    limit: int


class AuthError(Exception):
    def __init__(self, message, status):
        super().__init__(message)
        self.message = message
        self.status = status


class RuleStore:
    def __init__(self):
        self._rules = {}
        self._next_id = 1
        self._lock = threading.Lock()

    def upsert(self, name, algorithm, limit, window, burst, subject):
        with self._lock:
            for rule in self._rules.values():
                if rule.subject == subject:
                    rule.name = name
                    rule.algorithm = algorithm
                    rule.limit = limit
                    rule.window = window
                    rule.burst = burst
                    return rule
            rule = Rule(self._next_id, name, algorithm, limit, window, burst, subject)
            self._rules[rule.rule_id] = rule
            self._next_id += 1
            return rule

    def delete(self, rule_id):
        with self._lock:
            return self._rules.pop(rule_id, None)

    def resolve(self, username):
        with self._lock:
            for rule in self._rules.values():
                if rule.subject == username:
                    return rule
            for rule in self._rules.values():
                if rule.subject == "*":
                    return rule
        raise LookupError("No default rule configured.")

    def all(self):
        with self._lock:
            return sorted(self._rules.values(), key=lambda rule: rule.rule_id)


class RateLimiter:
    def __init__(self):
        self._buckets = {}
        self._windows = {}
        self._totals = {}
        self._lock = threading.Lock()

    def check(self, rule, client):
        now = time.monotonic()
        with self._lock:
            if rule.algorithm == "token_bucket":
                decision = self._token_bucket(rule, client, now)
            else:
                decision = self._sliding_window(rule, client, now)
            counts = self._totals.setdefault(client, [0, 0])
            counts[0 if decision.allowed else 1] += 1
            return decision

    def _token_bucket(self, rule, client, now):
        key = (rule.rule_id, client)
        rate = rule.limit / rule.window
        tokens, updated = self._buckets.get(key, (float(rule.burst), now))
        tokens = min(float(rule.burst), tokens + (now - updated) * rate)
        if tokens >= 1:
            self._buckets[key] = (tokens - 1, now)
            return Decision(True, int(tokens - 1), 0.0, rule.burst)
        self._buckets[key] = (tokens, now)
        return Decision(False, 0, (1 - tokens) / rate, rule.burst)

    def _sliding_window(self, rule, client, now):
        key = (rule.rule_id, client)
        hits = self._windows.setdefault(key, deque())
        while hits and now - hits[0] >= rule.window:
            hits.popleft()
        if len(hits) < rule.limit:
            hits.append(now)
            return Decision(True, rule.limit - len(hits), 0.0, rule.limit)
        return Decision(False, 0, hits[0] + rule.window - now, rule.limit)

    def totals(self, client):
        with self._lock:
            allowed, blocked = self._totals.get(client, [0, 0])
            return {"allowed": allowed, "blocked": blocked}

    def reset_rule(self, rule_id):
        with self._lock:
            for store in (self._buckets, self._windows):
                for key in [key for key in store if key[0] == rule_id]:
                    del store[key]

    def reset_counters(self):
        with self._lock:
            self._buckets.clear()
            self._windows.clear()
            self._totals.clear()


class AuthService:
    def __init__(self):
        self._users = {}
        self._sessions = {}
        self._failures = {}
        self._next_id = 1
        self._lock = threading.Lock()

    def register(self, username, password, role="user"):
        if not USERNAME_PATTERN.match(username):
            raise AuthError("Username must be 3-20 characters: letters, digits or underscore.", 400)
        if not PASSWORD_PATTERN.match(password):
            raise AuthError("Password must be at least 8 characters and include a letter and a digit.", 400)
        with self._lock:
            if username.lower() in {name.lower() for name in self._users}:
                raise AuthError("Username is already taken.", 409)
            user = {
                "id": self._next_id,
                "username": username,
                "role": role,
                "hash": generate_password_hash(password),
            }
            self._users[username] = user
            self._next_id += 1
            return self._public(user)

    def login(self, username, password):
        now = time.time()
        with self._lock:
            record = self._failures.get(username, {"count": 0, "locked_until": 0.0})
            if record["locked_until"] > now:
                wait = math.ceil(record["locked_until"] - now)
                raise AuthError(f"Account locked. Try again in {wait} seconds.", 423)
            user = self._users.get(username)
            if user is None or not check_password_hash(user["hash"], password):
                record["count"] += 1
                if record["count"] >= MAX_FAILED_LOGINS:
                    record = {"count": 0, "locked_until": now + LOCKOUT_SECONDS}
                self._failures[username] = record
                raise AuthError("Invalid username or password.", 401)
            self._failures.pop(username, None)
            token = secrets.token_urlsafe(32)
            self._sessions[token] = {"username": username, "expires": now + SESSION_TTL_SECONDS}
            return token, self._public(user)

    def authenticate(self, token):
        now = time.time()
        with self._lock:
            for stale in [key for key, value in self._sessions.items() if value["expires"] <= now]:
                del self._sessions[stale]
            session = self._sessions.get(token)
            if session is None:
                return None
            session["expires"] = now + SESSION_TTL_SECONDS
            return self._public(self._users[session["username"]])

    def logout(self, token):
        with self._lock:
            self._sessions.pop(token, None)

    def exists(self, username):
        with self._lock:
            return username in self._users

    def usernames(self):
        with self._lock:
            return sorted(self._users)

    @staticmethod
    def _public(user):
        return {"id": user["id"], "username": user["username"], "role": user["role"]}


app = Flask(__name__)
rules = RuleStore()
limiter = RateLimiter()
auth = AuthService()

LOGIN_THROTTLE = Rule(0, "login-throttle", "sliding_window", LOGIN_ATTEMPT_LIMIT,
                      LOGIN_ATTEMPT_WINDOW, LOGIN_ATTEMPT_LIMIT, "ip")

rules.upsert("default", "token_bucket", 5, 10, 8, "*")
auth.register("admin", "Admin@1234", "admin")
auth.register("demo", "Demo@1234", "user")


def error_response(message, status, **extra):
    response = jsonify(error=message, **extra)
    response.status_code = status
    return response


def require_auth(admin_only=False):
    def decorator(view):
        @wraps(view)
        def wrapper(*args, **kwargs):
            header = request.headers.get("Authorization", "")
            token = header[7:] if header.startswith("Bearer ") else ""
            user = auth.authenticate(token)
            if user is None:
                return error_response("Authentication required.", 401)
            if admin_only and user["role"] != "admin":
                return error_response("Admin access required.", 403)
            g.user = user
            g.token = token
            return view(*args, **kwargs)
        return wrapper
    return decorator


def rate_limited(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        rule = rules.resolve(g.user["username"])
        decision = limiter.check(rule, g.user["username"])
        headers = {
            "X-RateLimit-Limit": str(decision.limit),
            "X-RateLimit-Remaining": str(decision.remaining),
            "X-RateLimit-Rule": rule.name,
        }
        if not decision.allowed:
            retry_after = max(1, math.ceil(decision.retry_after))
            response = error_response("Too Many Requests", 429, retry_after=retry_after, rule=rule.name)
            response.headers.update(headers)
            response.headers["Retry-After"] = str(retry_after)
            return response
        response = make_response(view(*args, **kwargs))
        response.headers.update(headers)
        return response
    return wrapper


def parse_rule_payload(payload):
    if not isinstance(payload, dict):
        raise ValueError("Request body must be a JSON object.")
    name = str(payload.get("name", "")).strip()
    algorithm = payload.get("algorithm")
    subject = str(payload.get("subject", "*")).strip() or "*"
    try:
        limit = int(payload.get("limit"))
        window = int(payload.get("window"))
        burst = int(payload.get("burst", limit))
    except (TypeError, ValueError):
        raise ValueError("Limit, window and burst must be whole numbers.")
    if not 1 <= len(name) <= 40:
        raise ValueError("Rule name must be 1-40 characters.")
    if algorithm not in ALGORITHMS:
        raise ValueError("Unsupported algorithm.")
    if not 1 <= limit <= 10000:
        raise ValueError("Limit must be between 1 and 10000.")
    if not 1 <= window <= 3600:
        raise ValueError("Window must be between 1 and 3600 seconds.")
    if algorithm == "sliding_window":
        burst = limit
    if not 1 <= burst <= 10000:
        raise ValueError("Burst must be between 1 and 10000.")
    if subject != "*" and not auth.exists(subject):
        raise ValueError("Subject must be * or an existing username.")
    return name, algorithm, limit, window, burst, subject


@app.errorhandler(AuthError)
def handle_auth_error(error):
    return error_response(error.message, error.status)


@app.get("/")
def index():
    return PAGE


@app.post("/v1/auth/register")
def register():
    payload = request.get_json(silent=True) or {}
    user = auth.register(str(payload.get("username", "")).strip(), str(payload.get("password", "")))
    return jsonify(user=user), 201


@app.post("/v1/auth/login")
def login():
    decision = limiter.check(LOGIN_THROTTLE, request.remote_addr or "unknown")
    if not decision.allowed:
        retry_after = max(1, math.ceil(decision.retry_after))
        response = error_response("Too many login attempts. Slow down.", 429, retry_after=retry_after)
        response.headers["Retry-After"] = str(retry_after)
        return response
    payload = request.get_json(silent=True) or {}
    token, user = auth.login(str(payload.get("username", "")).strip(), str(payload.get("password", "")))
    return jsonify(token=token, user=user, expires_in=SESSION_TTL_SECONDS)


@app.post("/v1/auth/logout")
@require_auth()
def logout():
    auth.logout(g.token)
    return jsonify(ok=True)


@app.get("/v1/auth/me")
@require_auth()
def me():
    return jsonify(
        user=g.user,
        rule=asdict(rules.resolve(g.user["username"])),
        counters=limiter.totals(g.user["username"]),
    )


@app.post("/v1/admin/<int:admin_id>/rules")
@require_auth(admin_only=True)
def upsert_rule(admin_id):
    if admin_id != g.user["id"]:
        return error_response("Admin id does not match the signed-in account.", 403)
    try:
        values = parse_rule_payload(request.get_json(silent=True))
    except ValueError as error:
        return error_response(str(error), 400)
    rule = rules.upsert(*values)
    limiter.reset_rule(rule.rule_id)
    return jsonify(rule=asdict(rule)), 201


@app.delete("/v1/admin/<int:admin_id>/rules/<int:rule_id>")
@require_auth(admin_only=True)
def delete_rule(admin_id, rule_id):
    if admin_id != g.user["id"]:
        return error_response("Admin id does not match the signed-in account.", 403)
    target = next((rule for rule in rules.all() if rule.rule_id == rule_id), None)
    if target is None:
        return error_response("Rule not found.", 404)
    if target.subject == "*":
        return error_response("The default rule cannot be deleted.", 400)
    rules.delete(rule_id)
    limiter.reset_rule(rule_id)
    return jsonify(ok=True)


@app.post("/v1/admin/<int:admin_id>/reset")
@require_auth(admin_only=True)
def reset_counters(admin_id):
    if admin_id != g.user["id"]:
        return error_response("Admin id does not match the signed-in account.", 403)
    limiter.reset_counters()
    return jsonify(ok=True)


@app.get("/v1/admin/rules/rate-limit")
@require_auth(admin_only=True)
def list_rules():
    usernames = auth.usernames()
    return jsonify(
        rules=[asdict(rule) for rule in rules.all()],
        users=usernames,
        counters=[{"username": name, **limiter.totals(name)} for name in usernames],
    )


@app.get("/v1/rules/rate-limit")
@require_auth()
@rate_limited
def rate_limit_endpoint():
    request_id = request.args.get("req", "")
    if not REQUEST_ID_PATTERN.match(request_id):
        return error_response("Query parameter 'req' must be a numeric request id.", 400)
    return jsonify(status="ok", req=int(request_id), user=g.user["username"])


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Rate Limiter</title>
    <style>
        :root {
            --bg: #f4f6f8;
            --surface: #ffffff;
            --border: #d9dee5;
            --text: #1c2430;
            --muted: #5d6877;
            --accent: #1f5fd1;
            --accent-dark: #1849a3;
            --ok: #17794a;
            --ok-bg: #e3f4ea;
            --bad: #b42318;
            --bad-bg: #fde7e4;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            background: var(--bg);
            color: var(--text);
            font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            font-size: 14px;
            line-height: 1.5;
        }

        .hidden {
            display: none !important;
        }

        .topbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 12px 24px;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
        }

        .brand {
            font-size: 16px;
            font-weight: 600;
        }

        .session {
            display: flex;
            align-items: center;
            gap: 12px;
            color: var(--muted);
        }

        main {
            max-width: 1040px;
            margin: 0 auto;
            padding: 24px;
        }

        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 20px;
        }

        .card h2 {
            margin: 0 0 4px;
            font-size: 15px;
        }

        .card .hint {
            margin: 0 0 16px;
            color: var(--muted);
        }

        .auth {
            display: grid;
            grid-template-columns: minmax(0, 380px) minmax(0, 1fr);
            gap: 24px;
            align-items: start;
        }

        .tabs {
            display: flex;
            gap: 4px;
            margin-bottom: 16px;
            border-bottom: 1px solid var(--border);
        }

        .tab {
            padding: 8px 14px;
            border: 0;
            border-bottom: 2px solid transparent;
            background: none;
            color: var(--muted);
            font: inherit;
            cursor: pointer;
        }

        .tab.active {
            border-bottom-color: var(--accent);
            color: var(--text);
            font-weight: 600;
        }

        label {
            display: block;
            margin-bottom: 4px;
            font-size: 12px;
            font-weight: 600;
            color: var(--muted);
        }

        input,
        select {
            width: 100%;
            padding: 8px 10px;
            margin-bottom: 14px;
            border: 1px solid var(--border);
            border-radius: 6px;
            background: var(--surface);
            color: var(--text);
            font: inherit;
        }

        input:focus,
        select:focus {
            outline: 2px solid rgba(31, 95, 209, 0.25);
            border-color: var(--accent);
        }

        input:disabled {
            background: var(--bg);
            color: var(--muted);
        }

        .btn {
            padding: 8px 16px;
            border: 1px solid var(--accent);
            border-radius: 6px;
            background: var(--accent);
            color: #ffffff;
            font: inherit;
            font-weight: 600;
            cursor: pointer;
        }

        .btn:hover {
            background: var(--accent-dark);
        }

        .btn-block {
            width: 100%;
        }

        .btn-ghost {
            border-color: var(--border);
            background: var(--surface);
            color: var(--text);
        }

        .btn-ghost:hover {
            background: var(--bg);
        }

        .btn-small {
            padding: 3px 10px;
            font-size: 12px;
        }

        .message {
            min-height: 20px;
            margin-top: 12px;
            font-size: 13px;
        }

        .message.error {
            color: var(--bad);
        }

        .message.success {
            color: var(--ok);
        }

        .policy {
            margin: 0;
            padding-left: 18px;
            color: var(--muted);
        }

        .policy li {
            margin-bottom: 6px;
        }

        .dashboard {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 20px;
        }

        .span-2 {
            grid-column: span 2;
        }

        .tiles {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
            margin-bottom: 16px;
        }

        .tile {
            padding: 12px;
            border: 1px solid var(--border);
            border-radius: 6px;
        }

        .tile .value {
            font-size: 24px;
            font-weight: 600;
        }

        .tile .caption {
            font-size: 12px;
            color: var(--muted);
        }

        .row {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
        }

        .row-3 {
            grid-template-columns: repeat(3, 1fr);
        }

        .row-4 {
            grid-template-columns: repeat(4, 1fr);
        }

        .actions {
            display: flex;
            gap: 8px;
        }

        .scroll {
            max-height: 300px;
            overflow-y: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th,
        td {
            padding: 8px 10px;
            border-bottom: 1px solid var(--border);
            text-align: left;
            font-size: 13px;
        }

        th {
            position: sticky;
            top: 0;
            background: var(--surface);
            color: var(--muted);
            font-size: 12px;
            font-weight: 600;
        }

        .badge {
            display: inline-block;
            padding: 1px 8px;
            border-radius: 10px;
            font-size: 12px;
            font-weight: 600;
        }

        .badge.ok {
            background: var(--ok-bg);
            color: var(--ok);
        }

        .badge.bad {
            background: var(--bad-bg);
            color: var(--bad);
        }

        .empty {
            padding: 16px 0;
            color: var(--muted);
            text-align: center;
        }

        .pipeline {
            margin-top: 24px;
            color: var(--muted);
            font-size: 12px;
            text-align: center;
        }

        @media (max-width: 760px) {
            .auth,
            .dashboard {
                grid-template-columns: 1fr;
            }

            .span-2 {
                grid-column: auto;
            }

            .row-3,
            .row-4 {
                grid-template-columns: repeat(2, 1fr);
            }
        }
    </style>
</head>
<body>
    <header class="topbar">
        <div class="brand">Rate Limiter</div>
        <div id="session" class="session hidden">
            <span id="session-user"></span>
            <button id="logout" class="btn btn-ghost btn-small" type="button">Sign out</button>
        </div>
    </header>

    <main>
        <section id="auth-view" class="auth">
            <div class="card">
                <div class="tabs">
                    <button id="tab-login" class="tab active" type="button">Sign in</button>
                    <button id="tab-register" class="tab" type="button">Create account</button>
                </div>
                <form id="auth-form" autocomplete="off">
                    <label for="username">Username</label>
                    <input id="username" type="text" required>
                    <label for="password">Password</label>
                    <input id="password" type="password" required>
                    <button id="auth-submit" class="btn btn-block" type="submit">Sign in</button>
                </form>
                <div id="auth-message" class="message"></div>
            </div>

            <div class="card">
                <h2>Access policy</h2>
                <p class="hint">Every request to the rate-limited API requires a signed-in account.</p>
                <ul class="policy">
                    <li>Usernames are 3-20 characters: letters, digits or underscore.</li>
                    <li>Passwords need at least 8 characters with a letter and a digit, and are stored hashed.</li>
                    <li>Five failed sign-ins lock the account for five minutes.</li>
                    <li>Sign-in attempts are themselves rate limited per client address.</li>
                    <li>Sessions expire after 30 minutes of inactivity.</li>
                    <li>Only administrators can create, change or remove rules.</li>
                </ul>
            </div>
        </section>

        <section id="app-view" class="hidden">
            <div class="dashboard">
                <div class="card">
                    <h2>Active rule</h2>
                    <p id="rule-summary" class="hint"></p>
                    <div class="tiles">
                        <div class="tile">
                            <div id="count-allowed" class="value">0</div>
                            <div class="caption">Allowed</div>
                        </div>
                        <div class="tile">
                            <div id="count-blocked" class="value">0</div>
                            <div class="caption">Blocked (429)</div>
                        </div>
                    </div>
                    <div class="row">
                        <div>
                            <label for="req-id">Request id</label>
                            <input id="req-id" type="number" min="0" value="4993">
                        </div>
                        <div>
                            <label for="burst-count">Requests to send at once</label>
                            <input id="burst-count" type="number" min="1" max="50" value="1">
                        </div>
                    </div>
                    <button id="send" class="btn" type="button">Send</button>
                </div>

                <div class="card">
                    <h2>Responses</h2>
                    <p class="hint">GET /v1/rules/rate-limit?req=&lt;id&gt;</p>
                    <div class="scroll">
                        <table>
                            <thead>
                                <tr>
                                    <th>Time</th>
                                    <th>Req</th>
                                    <th>Status</th>
                                    <th>Remaining</th>
                                    <th>Retry after</th>
                                </tr>
                            </thead>
                            <tbody id="log-body"></tbody>
                        </table>
                        <div id="log-empty" class="empty">No requests sent yet.</div>
                    </div>
                </div>

                <div id="admin-rules" class="card span-2 hidden">
                    <h2>Rules</h2>
                    <p class="hint">A rule bound to a user overrides the default rule (*) for that user.</p>
                    <form id="rule-form" autocomplete="off">
                        <div class="row row-4">
                            <div>
                                <label for="rule-name">Name</label>
                                <input id="rule-name" type="text" maxlength="40" required>
                            </div>
                            <div>
                                <label for="rule-subject">Applies to</label>
                                <select id="rule-subject"></select>
                            </div>
                            <div>
                                <label for="rule-algorithm">Algorithm</label>
                                <select id="rule-algorithm">
                                    <option value="token_bucket">Token bucket</option>
                                    <option value="sliding_window">Sliding window</option>
                                </select>
                            </div>
                            <div>
                                <label for="rule-limit">Requests per window</label>
                                <input id="rule-limit" type="number" min="1" max="10000" value="5" required>
                            </div>
                        </div>
                        <div class="row row-4">
                            <div>
                                <label for="rule-window">Window (seconds)</label>
                                <input id="rule-window" type="number" min="1" max="3600" value="10" required>
                            </div>
                            <div>
                                <label for="rule-burst">Burst capacity</label>
                                <input id="rule-burst" type="number" min="1" max="10000" value="8" required>
                            </div>
                        </div>
                        <div class="actions">
                            <button class="btn" type="submit">Save rule</button>
                            <button id="reset-counters" class="btn btn-ghost" type="button">Reset counters</button>
                        </div>
                    </form>
                    <div id="rule-message" class="message"></div>
                    <table>
                        <thead>
                            <tr>
                                <th>Applies to</th>
                                <th>Name</th>
                                <th>Algorithm</th>
                                <th>Limit</th>
                                <th>Burst</th>
                                <th></th>
                            </tr>
                        </thead>
                        <tbody id="rules-body"></tbody>
                    </table>
                </div>

                <div id="admin-counters" class="card span-2 hidden">
                    <h2>Request counters</h2>
                    <p class="hint">Totals per user since the last reset.</p>
                    <table>
                        <thead>
                            <tr>
                                <th>User</th>
                                <th>Allowed</th>
                                <th>Blocked</th>
                            </tr>
                        </thead>
                        <tbody id="counters-body"></tbody>
                    </table>
                </div>
            </div>
            <div class="pipeline">Client &rarr; API gateway (rate limiter) &rarr; Server &rarr; Rule and counter store</div>
        </section>
    </main>

    <script>
        const ALGORITHM_LABELS = {
            token_bucket: "Token bucket",
            sliding_window: "Sliding window"
        };

        const state = {
            token: sessionStorage.getItem("rl_token"),
            user: null,
            mode: "login",
            logRows: 0
        };

        const $ = (id) => document.getElementById(id);

        async function api(path, options = {}) {
            const headers = { "Content-Type": "application/json" };
            if (state.token) {
                headers["Authorization"] = "Bearer " + state.token;
            }
            const response = await fetch(path, { ...options, headers });
            let data = null;
            try {
                data = await response.json();
            } catch (error) {
                data = null;
            }
            return { status: response.status, data: data, headers: response.headers };
        }

        function errorText(result) {
            return (result.data && result.data.error) || "Request failed.";
        }

        function setMessage(id, text, kind) {
            const node = $(id);
            node.textContent = text || "";
            node.className = "message" + (kind ? " " + kind : "");
        }

        function showView(name) {
            $("auth-view").classList.toggle("hidden", name !== "auth");
            $("app-view").classList.toggle("hidden", name !== "app");
            $("session").classList.toggle("hidden", name !== "app");
        }

        function appendRow(body, values) {
            const row = document.createElement("tr");
            values.forEach((value) => {
                const cell = document.createElement("td");
                if (value instanceof Node) {
                    cell.appendChild(value);
                } else {
                    cell.textContent = value;
                }
                row.appendChild(cell);
            });
            body.appendChild(row);
            return row;
        }

        function badge(status) {
            const node = document.createElement("span");
            node.className = "badge " + (status === 200 ? "ok" : "bad");
            node.textContent = status;
            return node;
        }

        function ruleSummary(rule) {
            const base = ALGORITHM_LABELS[rule.algorithm] + ": " + rule.limit + " requests per " + rule.window + "s";
            return rule.algorithm === "token_bucket" ? base + ", burst up to " + rule.burst : base;
        }

        function switchMode(mode) {
            state.mode = mode;
            $("tab-login").classList.toggle("active", mode === "login");
            $("tab-register").classList.toggle("active", mode === "register");
            $("auth-submit").textContent = mode === "login" ? "Sign in" : "Create account";
            $("password").autocomplete = mode === "login" ? "current-password" : "new-password";
            setMessage("auth-message", "");
        }

        async function submitAuth(event) {
            event.preventDefault();
            const body = JSON.stringify({
                username: $("username").value.trim(),
                password: $("password").value
            });
            const path = state.mode === "login" ? "v1/auth/login" : "v1/auth/register";
            const result = await api(path, { method: "POST", body: body });
            if (result.status >= 400) {
                setMessage("auth-message", errorText(result), "error");
                return;
            }
            if (state.mode === "register") {
                switchMode("login");
                $("password").value = "";
                setMessage("auth-message", "Account created. Sign in to continue.", "success");
                return;
            }
            state.token = result.data.token;
            state.user = result.data.user;
            sessionStorage.setItem("rl_token", state.token);
            $("password").value = "";
            await enterApp();
        }

        async function enterApp() {
            const isAdmin = state.user.role === "admin";
            $("session-user").textContent = state.user.username + " (" + state.user.role + ")";
            $("admin-rules").classList.toggle("hidden", !isAdmin);
            $("admin-counters").classList.toggle("hidden", !isAdmin);
            showView("app");
            await refreshProfile();
            if (isAdmin) {
                await loadAdmin();
            }
        }

        async function signOut(notifyServer) {
            if (notifyServer && state.token) {
                await api("v1/auth/logout", { method: "POST" });
            }
            state.token = null;
            state.user = null;
            sessionStorage.removeItem("rl_token");
            $("log-body").innerHTML = "";
            $("log-empty").classList.remove("hidden");
            showView("auth");
        }

        async function refreshProfile() {
            const result = await api("v1/auth/me");
            if (result.status === 401) {
                await signOut(false);
                setMessage("auth-message", "Session expired. Sign in again.", "error");
                return;
            }
            state.user = result.data.user;
            $("rule-summary").textContent = ruleSummary(result.data.rule);
            $("count-allowed").textContent = result.data.counters.allowed;
            $("count-blocked").textContent = result.data.counters.blocked;
        }

        async function sendRequests() {
            const count = Math.min(50, Math.max(1, parseInt($("burst-count").value, 10) || 1));
            const base = Math.max(0, parseInt($("req-id").value, 10) || 0);
            const calls = Array.from({ length: count }, (_, index) => callEndpoint(base + index));
            const results = await Promise.all(calls);
            results.forEach(addLogRow);
            $("req-id").value = base + count;
            await refreshProfile();
            if (state.user && state.user.role === "admin") {
                await loadAdmin();
            }
        }

        async function callEndpoint(requestId) {
            const result = await api("v1/rules/rate-limit?req=" + requestId);
            const retry = result.status === 429 && result.data ? result.data.retry_after + "s" : "-";
            return {
                time: new Date().toLocaleTimeString(),
                req: requestId,
                status: result.status,
                remaining: result.headers.get("X-RateLimit-Remaining") || "-",
                retry: retry
            };
        }

        function addLogRow(entry) {
            const body = $("log-body");
            $("log-empty").classList.add("hidden");
            const row = appendRow(body, [entry.time, entry.req, badge(entry.status), entry.remaining, entry.retry]);
            body.insertBefore(row, body.firstChild);
            while (body.children.length > 60) {
                body.removeChild(body.lastChild);
            }
        }

        async function loadAdmin() {
            const result = await api("v1/admin/rules/rate-limit");
            if (result.status !== 200) {
                return;
            }
            const subject = $("rule-subject");
            const selected = subject.value;
            subject.innerHTML = "";
            ["*"].concat(result.data.users).forEach((name) => {
                const option = document.createElement("option");
                option.value = name;
                option.textContent = name === "*" ? "* (default)" : name;
                subject.appendChild(option);
            });
            subject.value = selected || "*";

            const rulesBody = $("rules-body");
            rulesBody.innerHTML = "";
            result.data.rules.forEach((rule) => {
                let action = "";
                if (rule.subject !== "*") {
                    action = document.createElement("button");
                    action.type = "button";
                    action.className = "btn btn-ghost btn-small";
                    action.textContent = "Delete";
                    action.addEventListener("click", () => deleteRule(rule.rule_id));
                }
                appendRow(rulesBody, [
                    rule.subject,
                    rule.name,
                    ALGORITHM_LABELS[rule.algorithm],
                    rule.limit + " / " + rule.window + "s",
                    rule.burst,
                    action
                ]);
            });

            const countersBody = $("counters-body");
            countersBody.innerHTML = "";
            result.data.counters.forEach((entry) => {
                appendRow(countersBody, [entry.username, entry.allowed, entry.blocked]);
            });
        }

        async function saveRule(event) {
            event.preventDefault();
            const body = JSON.stringify({
                name: $("rule-name").value.trim(),
                subject: $("rule-subject").value,
                algorithm: $("rule-algorithm").value,
                limit: $("rule-limit").value,
                window: $("rule-window").value,
                burst: $("rule-burst").value
            });
            const result = await api("v1/admin/" + state.user.id + "/rules", { method: "POST", body: body });
            if (result.status >= 400) {
                setMessage("rule-message", errorText(result), "error");
                return;
            }
            setMessage("rule-message", "Rule saved.", "success");
            await loadAdmin();
            await refreshProfile();
        }

        async function deleteRule(ruleId) {
            const result = await api("v1/admin/" + state.user.id + "/rules/" + ruleId, { method: "DELETE" });
            if (result.status >= 400) {
                setMessage("rule-message", errorText(result), "error");
                return;
            }
            setMessage("rule-message", "Rule deleted.", "success");
            await loadAdmin();
            await refreshProfile();
        }

        async function resetCounters() {
            const result = await api("v1/admin/" + state.user.id + "/reset", { method: "POST" });
            if (result.status >= 400) {
                setMessage("rule-message", errorText(result), "error");
                return;
            }
            setMessage("rule-message", "Counters reset.", "success");
            await loadAdmin();
            await refreshProfile();
        }

        function syncBurstField() {
            const isBucket = $("rule-algorithm").value === "token_bucket";
            $("rule-burst").disabled = !isBucket;
        }

        async function boot() {
            $("tab-login").addEventListener("click", () => switchMode("login"));
            $("tab-register").addEventListener("click", () => switchMode("register"));
            $("auth-form").addEventListener("submit", submitAuth);
            $("logout").addEventListener("click", () => signOut(true));
            $("send").addEventListener("click", sendRequests);
            $("rule-form").addEventListener("submit", saveRule);
            $("reset-counters").addEventListener("click", resetCounters);
            $("rule-algorithm").addEventListener("change", syncBurstField);
            syncBurstField();

            if (!state.token) {
                showView("auth");
                return;
            }
            const result = await api("v1/auth/me");
            if (result.status !== 200) {
                await signOut(false);
                return;
            }
            state.user = result.data.user;
            await enterApp();
        }

        boot();
    </script>
</body>
</html>
"""


def start_server():
    existing = globals().get("_server")
    if existing is not None:
        existing.shutdown()
        existing.server_close()
    server = make_server(HOST, PORT, app, threaded=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    globals()["_server"] = server


def show_interface():
    try:
        from google.colab import output
    except ImportError:
        print(f"Open http://{HOST}:{PORT} in a browser.")
        return
    output.serve_kernel_port_as_iframe(PORT, height="900")


start_server()
print("Admin: admin / Admin@1234    User: demo / Demo@1234")
show_interface()