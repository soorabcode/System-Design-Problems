# bit.ab - URL Shortener 
# No login (anonymous user ID stored in a cookie)
# Paste a long URL, get a 6-character short link
# No custom aliases, 3-day expiry, collision-checked Base62 codes

import http.server
import secrets
import sqlite3
import string
import threading
import time
import urllib.parse
import uuid
from datetime import datetime, timedelta, timezone
from http import cookies

# Configuration 
PORT = 8000
DB_FILE = "bitab.db"
CODE_LENGTH = 6
EXPIRY_DAYS = 3
MAX_URL_LENGTH = 2048
ALPHABET = string.ascii_letters + string.digits      # Base62
PURGE_INTERVAL_SECONDS = 3600

# Database 
db_lock = threading.Lock()

def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db_lock:
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS short_links (
                short_code   TEXT PRIMARY KEY,
                original_url TEXT NOT NULL,
                user_id      TEXT NOT NULL,
                created_at   TEXT NOT NULL,
                expires_at   TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user ON short_links(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_expiry ON short_links(expires_at)")
        conn.commit()
        conn.close()

def now_utc():
    return datetime.now(timezone.utc)

def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def generate_unique_code(conn):
    """Random Base62 code, retried until unused."""
    while True:
        code = "".join(secrets.choice(ALPHABET) for _ in range(CODE_LENGTH))
        row = conn.execute(
            "SELECT 1 FROM short_links WHERE short_code = ?", (code,)
        ).fetchone()
        if row is None:
            return code

def create_short_link(original_url, user_id):
    created = now_utc()
    expires = created + timedelta(days=EXPIRY_DAYS)
    with db_lock:
        conn = get_conn()
        try:
            code = generate_unique_code(conn)
            conn.execute(
                "INSERT INTO short_links VALUES (?, ?, ?, ?, ?)",
                (code, original_url, user_id, iso(created), iso(expires)),
            )
            conn.commit()
            return code, expires
        finally:
            conn.close()

def lookup_long_url(code):
    """Returns (original_url, status): status is 'ok', 'expired', or 'missing'."""
    with db_lock:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT original_url, expires_at FROM short_links WHERE short_code = ?",
                (code,),
            ).fetchone()
        finally:
            conn.close()
    if row is None:
        return None, "missing"
    expires = datetime.strptime(row["expires_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc)
    if now_utc() > expires:
        return None, "expired"
    return row["original_url"], "ok"

def purge_expired():
    with db_lock:
        conn = get_conn()
        try:
            conn.execute("DELETE FROM short_links WHERE expires_at < ?",
                         (iso(now_utc()),))
            conn.commit()
        finally:
            conn.close()

def purge_loop():
    while True:
        time.sleep(PURGE_INTERVAL_SECONDS)
        try:
            purge_expired()
        except Exception as e:
            print("Purge error:", e)

#  Validation 
def is_valid_url(url):
    if not url or len(url) > MAX_URL_LENGTH:
        return False
    parsed = urllib.parse.urlparse(url)
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)

# HTML 
PAGE_STYLE = """
body { font-family: Arial, Helvetica, sans-serif; background:#f7f7f7;
       margin:0; padding:0; color:#222; }
.container { max-width:640px; margin:40px auto; background:#fff;
             padding:32px; border:1px solid #ddd; border-radius:4px; }
h1 { margin-top:0; font-size:26px; }
p.sub { color:#666; margin-top:-8px; }
label { display:block; margin-top:20px; font-weight:bold; }
input[type=text] { width:100%; padding:10px; font-size:15px;
                   border:1px solid #bbb; border-radius:3px;
                   box-sizing:border-box; margin-top:6px; }
button { margin-top:16px; padding:10px 20px; font-size:15px;
         background:#333; color:#fff; border:none; border-radius:3px;
         cursor:pointer; }
button:hover { background:#000; }
.result { margin-top:24px; padding:16px; background:#f0f7f0;
          border:1px solid #cde3cd; border-radius:3px; }
.result a { color:#1a5c1a; font-weight:bold; word-break:break-all; }
.error { margin-top:24px; padding:16px; background:#fbeaea;
         border:1px solid #e5b5b5; border-radius:3px; color:#7a1a1a; }
.meta { font-size:13px; color:#555; margin-top:8px; }
.footer { margin-top:32px; font-size:12px; color:#888; text-align:center; }
"""

def page(body):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>bit.ab - URL Shortener</title>
<style>{PAGE_STYLE}</style>
</head>
<body>
<div class="container">
{body}
<div class="footer">Links expire after {EXPIRY_DAYS} days. No login required.</div>
</div>
</body>
</html>"""

def home_html(error=None, result=None):
    # Relative URLs (no leading "/") so they work behind Colab's port proxy.
    body = """
<h1>bit.ab</h1>
<p class="sub">Paste a long URL and get a short link.</p>
<form method="POST" action="shorten">
  <label for="url">Long URL</label>
  <input type="text" id="url" name="url"
         placeholder="https://example.com/very/long/path" required>
  <button type="submit">Shorten</button>
</form>
"""
    if error:
        body += f'<div class="error">{error}</div>'
    if result:
        code, expires = result
        body += f"""
<div class="result">
  <div>Your short URL:</div>
  <a href="{code}">{code}</a>
  <div class="meta">Expires: {expires.strftime('%Y-%m-%d %H:%M UTC')}</div>
</div>
"""
    return page(body)

def message_html(title, message):
    return page(f"<h1>{title}</h1><p>{message}</p>"
                f"<p><a href='./'>Back to home</a></p>")

# HTTP Handler 
class Handler(http.server.BaseHTTPRequestHandler):

    def _get_user_id(self):
        """Read or create an anonymous user ID stored in a cookie."""
        jar = cookies.SimpleCookie()
        try:
            jar.load(self.headers.get("Cookie", ""))
        except cookies.CookieError:
            pass
        if "bitab_uid" in jar:
            return jar["bitab_uid"].value, None
        uid = uuid.uuid4().hex
        c = cookies.SimpleCookie()
        c["bitab_uid"] = uid
        c["bitab_uid"]["path"] = "/"
        c["bitab_uid"]["max-age"] = str(60 * 60 * 24 * 365)
        c["bitab_uid"]["httponly"] = True
        c["bitab_uid"]["samesite"] = "Lax"
        return uid, c["bitab_uid"].OutputString()

    def _send_html(self, html, status=200, set_cookie=None):
        data = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        if set_cookie:
            self.send_header("Set-Cookie", set_cookie)
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location, status=302):
        self.send_response(status)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        # Colab's proxy may pass a prefix; keep only the last path segment.
        segment = path.rstrip("/").split("/")[-1]

        if path in ("/", "") or segment == "":
            _, set_cookie = self._get_user_id()
            self._send_html(home_html(), set_cookie=set_cookie)
            return

        if len(segment) == CODE_LENGTH and all(ch in ALPHABET for ch in segment):
            original, status = lookup_long_url(segment)
            if status == "ok":
                self._redirect(original)
            elif status == "expired":
                self._send_html(message_html(
                    "Link expired",
                    f"This short link has expired (links last {EXPIRY_DAYS} days)."),
                    status=410)
            else:
                self._send_html(message_html(
                    "Not found", "This short link does not exist."), status=404)
            return

        self._send_html(message_html("Not found", "Page not found."), status=404)

    def do_POST(self):
        segment = urllib.parse.urlparse(self.path).path.rstrip("/").split("/")[-1]
        if segment != "shorten":
            self._send_html(message_html("Not found", "Page not found."), status=404)
            return

        length = int(self.headers.get("Content-Length", 0))
        if length > 10000:
            self._send_html(home_html(error="Request too large."), status=413)
            return
        body = self.rfile.read(length).decode("utf-8", errors="replace")
        form = urllib.parse.parse_qs(body)
        long_url = form.get("url", [""])[0].strip()

        user_id, set_cookie = self._get_user_id()

        if not is_valid_url(long_url):
            self._send_html(
                home_html(error="Please enter a valid http:// or https:// URL "
                                f"(max {MAX_URL_LENGTH} characters)."),
                status=400, set_cookie=set_cookie)
            return

        code, expires = create_short_link(long_url, user_id)
        self._send_html(home_html(result=(code, expires)), set_cookie=set_cookie)

    def log_message(self, format, *args):
        pass 

# Start server in Colab
init_db()
threading.Thread(target=purge_loop, daemon=True).start()

server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
server.daemon_threads = True
threading.Thread(target=server.serve_forever, daemon=True).start()
print(f"bit.ab is running on port {PORT}")

try:
    from google.colab import output
    output.serve_kernel_port_as_iframe(PORT, height=600)
except ImportError:
    print(f"Not in Colab. Open http://localhost:{PORT}")