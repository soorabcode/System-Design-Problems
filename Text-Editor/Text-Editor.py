'''
Text-Editor ( like notion and google docs but for .txt file only )


1. Requirement analysis 
A. functional requrement 
- create / upload a .txt file 
- see all created files 
- versioing of file 
- dowload the file 

B. non functional requrement 
- scale : 10m+ user and 10k+ documents
- Cap theroem - availabe >>>>>>>>>>> consistent 

2. Identifying Entities 
- User
- Document 

3. API 
A. Create / Uplaod A Document
POST /v1/api/docs/create-> create a document
B. See ALL Files 
GET /v1/api/docs{docsID} -> file details 
C. Versions Of File 
GET /v1/api/docs={docsID}/version -> versions 
D. Download The File 
POST /v1/api/docs{docsID}/download -> download the file

4. High level design 
user -> api gateway -> sever ( documents ( create & edit ) & download ) -> database ( documentDB & fileDB )

5. low level design 
uploaded image

'''



import os
import re
import time
import uuid
import queue
import difflib
import tempfile
import threading

from flask import Flask, request, jsonify, Response, send_file
from werkzeug.serving import make_server

PORT = 5000
MAX_BYTES = 2 * 1024 * 1024
VERSION_GAP_SECONDS = 20
MERGE_LIMIT_CHARS = 200000

S3_DIR = tempfile.mkdtemp(prefix="s3_")
os.makedirs(os.path.join(S3_DIR, "exports"), exist_ok=True)

LOCK = threading.RLock()
DOCUMENT_DB = {}
FILE_DB = {}
PRESENCE = {}
EVENT_LOG = []
KAFKA = queue.Queue()


def now():
    return time.time()


def fail(message, code):
    return jsonify({"error": message}), code


def grade(title, content):
    title = (title or "").strip() or "Untitled"
    title = title[:120]
    if not isinstance(content, str):
        raise ValueError("Content must be text")
    content = content.replace("\r\n", "\n")
    if len(content.encode("utf-8")) > MAX_BYTES:
        raise ValueError("Document is larger than 2 MB")
    return title, content


def publish(topic, doc_id, title):
    KAFKA.put({"topic": topic, "doc_id": doc_id, "title": title, "ts": now()})


def consumer():
    while True:
        message = KAFKA.get()
        with LOCK:
            EVENT_LOG.append(message)
            del EVENT_LOG[:-100]
            doc = DOCUMENT_DB.get(message["doc_id"])
            if message["topic"] == "doc.deleted" or doc is None:
                continue
            path = os.path.join(S3_DIR, doc["id"] + ".txt")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(doc["content"])
            FILE_DB[doc["id"]] = {
                "id": doc["id"],
                "name": doc["title"] + ".txt",
                "size": len(doc["content"].encode("utf-8")),
                "pointer": path,
                "updated": doc["updated"],
            }


def diff_edits(base, other):
    matcher = difflib.SequenceMatcher(None, base, other, autojunk=False)
    return [
        (i1, i2, other[j1:j2])
        for tag, i1, i2, j1, j2 in matcher.get_opcodes()
        if tag != "equal"
    ]


def conflicts(a, b):
    a1, a2 = a[0], a[1]
    b1, b2 = b[0], b[1]
    if a1 == a2 and b1 == b2:
        return False
    if a1 == a2:
        return b1 < a1 < b2
    if b1 == b2:
        return a1 < b1 < a2
    return a1 < b2 and b1 < a2


def merge(base, mine, theirs):
    if len(base) + len(mine) + len(theirs) > MERGE_LIMIT_CHARS:
        return mine
    mine_edits = diff_edits(base, mine)
    their_edits = diff_edits(base, theirs)
    their_edits = [
        e for e in their_edits if not any(conflicts(e, m) for m in mine_edits)
    ]
    edits = sorted(
        [(e[0], e[1], 0, e[2]) for e in their_edits]
        + [(e[0], e[1], 1, e[2]) for e in mine_edits]
    )
    out = []
    position = 0
    for start, end, _, text in edits:
        out.append(base[position:start])
        out.append(text)
        position = end
    out.append(base[position:])
    return "".join(out)


def summary(doc):
    return {
        "id": doc["id"],
        "title": doc["title"],
        "updated": doc["updated"],
        "created": doc["created"],
        "rev": doc["rev"],
        "versions": len(doc["versions"]),
        "size": len(doc["content"].encode("utf-8")),
        "words": len(doc["content"].split()),
    }


def detail(doc):
    data = summary(doc)
    data["content"] = doc["content"]
    data["permissions"] = doc["permissions"]
    return data


def create_doc(title, content):
    with LOCK:
        doc_id = uuid.uuid4().hex[:10]
        stamp = now()
        doc = {
            "id": doc_id,
            "title": title,
            "content": content,
            "created": stamp,
            "updated": stamp,
            "rev": 1,
            "revs": {1: content},
            "permissions": "anyone with the link can edit",
            "last_version_ts": stamp,
            "versions": [{"n": 1, "content": content, "ts": stamp, "note": "Created"}],
        }
        DOCUMENT_DB[doc_id] = doc
        publish("doc.created", doc_id, title)
        return doc


def save_doc(doc, content, title, base_rev, snapshot, note=None):
    with LOCK:
        current = doc["content"]
        merged = content
        if base_rev is not None and base_rev != doc["rev"]:
            base = doc["revs"].get(base_rev)
            if base is not None:
                merged = merge(base, content, current)
        title_changed = title != doc["title"]
        if merged == current and not title_changed and not snapshot:
            return doc
        stamp = now()
        if merged != current:
            doc["rev"] += 1
            doc["content"] = merged
            doc["revs"][doc["rev"]] = merged
            for key in sorted(doc["revs"])[:-300]:
                del doc["revs"][key]
        doc["title"] = title
        doc["updated"] = stamp
        latest = doc["versions"][-1]
        if merged != latest["content"]:
            if snapshot or note or stamp - doc["last_version_ts"] >= VERSION_GAP_SECONDS:
                doc["versions"].append(
                    {
                        "n": len(doc["versions"]) + 1,
                        "content": merged,
                        "ts": stamp,
                        "note": note or ("Manual save" if snapshot else "Auto save"),
                    }
                )
                doc["last_version_ts"] = stamp
            else:
                latest["content"] = merged
                latest["ts"] = stamp
        publish("doc.edited", doc["id"], title)
        return doc


app = Flask(__name__)


@app.get("/")
def index():
    return Response(PAGE, mimetype="text/html")


@app.post("/v1/api/docs/create")
def create_route():
    uploaded = request.files.get("file")
    if uploaded:
        name = uploaded.filename or ""
        if not name.lower().endswith(".txt"):
            return fail("Only .txt files can be uploaded", 400)
        raw = uploaded.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            return fail("File is larger than 2 MB", 413)
        content = raw.decode("utf-8", errors="replace")
        title = name[:-4]
    else:
        data = request.get_json(silent=True) or {}
        title = data.get("title")
        content = data.get("content", "")
    try:
        title, content = grade(title, content)
    except ValueError as error:
        return fail(str(error), 400)
    doc = create_doc(title, content)
    return jsonify(summary(doc)), 201


@app.get("/v1/api/docs")
def list_route():
    query = (request.args.get("q") or "").strip().lower()
    with LOCK:
        docs = [
            summary(d)
            for d in DOCUMENT_DB.values()
            if query in d["title"].lower()
        ]
    docs.sort(key=lambda d: d["updated"], reverse=True)
    return jsonify(docs)


@app.get("/v1/api/docs/<doc_id>")
def detail_route(doc_id):
    with LOCK:
        doc = DOCUMENT_DB.get(doc_id)
        if not doc:
            return fail("Document not found", 404)
        return jsonify(detail(doc))


@app.put("/v1/api/docs/<doc_id>")
def update_route(doc_id):
    data = request.get_json(silent=True) or {}
    with LOCK:
        doc = DOCUMENT_DB.get(doc_id)
        if not doc:
            return fail("Document not found", 404)
        try:
            title, content = grade(data.get("title", doc["title"]), data.get("content", doc["content"]))
        except ValueError as error:
            return fail(str(error), 400)
        save_doc(doc, content, title, data.get("base_rev"), bool(data.get("snapshot")))
        PRESENCE.setdefault(doc_id, {})[data.get("client", "")] = now()
        return jsonify(detail(doc))


@app.delete("/v1/api/docs/<doc_id>")
def delete_route(doc_id):
    with LOCK:
        doc = DOCUMENT_DB.pop(doc_id, None)
        if not doc:
            return fail("Document not found", 404)
        FILE_DB.pop(doc_id, None)
        PRESENCE.pop(doc_id, None)
        path = os.path.join(S3_DIR, doc_id + ".txt")
        if os.path.exists(path):
            os.remove(path)
        publish("doc.deleted", doc_id, doc["title"])
    return jsonify({"deleted": doc_id})


@app.get("/v1/api/docs/<doc_id>/versions")
def versions_route(doc_id):
    with LOCK:
        doc = DOCUMENT_DB.get(doc_id)
        if not doc:
            return fail("Document not found", 404)
        return jsonify(
            [
                {
                    "n": v["n"],
                    "ts": v["ts"],
                    "note": v["note"],
                    "size": len(v["content"].encode("utf-8")),
                }
                for v in doc["versions"]
            ]
        )


@app.get("/v1/api/docs/<doc_id>/versions/<int:number>")
def version_route(doc_id, number):
    with LOCK:
        doc = DOCUMENT_DB.get(doc_id)
        if not doc:
            return fail("Document not found", 404)
        if number < 1 or number > len(doc["versions"]):
            return fail("Version not found", 404)
        version = doc["versions"][number - 1]
        return jsonify(
            {
                "n": version["n"],
                "ts": version["ts"],
                "note": version["note"],
                "content": version["content"],
            }
        )


@app.post("/v1/api/docs/<doc_id>/restore/<int:number>")
def restore_route(doc_id, number):
    with LOCK:
        doc = DOCUMENT_DB.get(doc_id)
        if not doc:
            return fail("Document not found", 404)
        if number < 1 or number > len(doc["versions"]):
            return fail("Version not found", 404)
        content = doc["versions"][number - 1]["content"]
        save_doc(doc, content, doc["title"], None, True, "Restored from v" + str(number))
        return jsonify(detail(doc))


@app.post("/v1/api/docs/<doc_id>/download")
def download_route(doc_id):
    data = request.get_json(silent=True) or {}
    with LOCK:
        doc = DOCUMENT_DB.get(doc_id)
        if not doc:
            return fail("Document not found", 404)
        number = data.get("version")
        content = doc["content"]
        suffix = ""
        if number:
            if not isinstance(number, int) or number < 1 or number > len(doc["versions"]):
                return fail("Version not found", 404)
            content = doc["versions"][number - 1]["content"]
            suffix = "_v" + str(number)
        safe = re.sub(r"[^\w\- ]", "_", doc["title"]).strip() or "Untitled"
        filename = safe + suffix + ".txt"
        path = os.path.join(S3_DIR, "exports", doc_id + suffix + ".txt")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
    return send_file(path, as_attachment=True, download_name=filename, mimetype="text/plain")


@app.get("/v1/api/docs/<doc_id>/poll")
def poll_route(doc_id):
    since = request.args.get("since", type=int, default=0)
    client = request.args.get("client", "")
    with LOCK:
        doc = DOCUMENT_DB.get(doc_id)
        if not doc:
            return fail("Document not found", 404)
        stamp = now()
        PRESENCE.setdefault(doc_id, {})[client] = stamp
        editors = sum(1 for t in PRESENCE[doc_id].values() if stamp - t < 6)
        payload = {
            "rev": doc["rev"],
            "changed": doc["rev"] > since,
            "editors": max(editors, 1),
            "title": doc["title"],
            "versions": len(doc["versions"]),
        }
        if payload["changed"]:
            payload["content"] = doc["content"]
        return jsonify(payload)


@app.get("/v1/api/events")
def events_route():
    with LOCK:
        return jsonify(list(reversed(EVENT_LOG[-40:])))


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Text Editor</title>
    <style>
        * {
            box-sizing: border-box;
        }

        html,
        body {
            height: 100%;
            margin: 0;
        }

        body {
            font-family: Arial, Helvetica, sans-serif;
            font-size: 14px;
            color: #222;
            background: #fff;
            display: flex;
            flex-direction: column;
        }

        button {
            font: inherit;
            padding: 5px 10px;
            border: 1px solid #aaa;
            background: #f4f4f4;
            color: #222;
            cursor: pointer;
        }

        button:hover {
            background: #e8e8e8;
        }

        button.danger {
            color: #a00;
        }

        input[type="text"] {
            font: inherit;
            padding: 5px 8px;
            border: 1px solid #aaa;
            background: #fff;
        }

        .topbar {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-bottom: 1px solid #ccc;
            background: #f7f7f7;
        }

        .topbar .brand {
            font-weight: bold;
            margin-right: auto;
        }

        .layout {
            flex: 1;
            display: grid;
            grid-template-columns: 240px 1fr 280px;
            min-height: 0;
        }

        .sidebar,
        .panel {
            display: flex;
            flex-direction: column;
            min-height: 0;
            background: #fafafa;
        }

        .sidebar {
            border-right: 1px solid #ccc;
        }

        .panel {
            border-left: 1px solid #ccc;
        }

        .sidebar .search {
            padding: 8px;
            border-bottom: 1px solid #ddd;
        }

        .sidebar .search input {
            width: 100%;
        }

        .scroll {
            flex: 1;
            overflow-y: auto;
        }

        .item {
            padding: 8px 10px;
            border-bottom: 1px solid #e4e4e4;
            cursor: pointer;
        }

        .item:hover {
            background: #eee;
        }

        .item.active {
            background: #e2e8f0;
        }

        .item .name {
            font-weight: bold;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .item .sub {
            color: #666;
            font-size: 12px;
            margin-top: 2px;
        }

        .main {
            display: flex;
            flex-direction: column;
            min-width: 0;
            min-height: 0;
        }

        .empty {
            margin: auto;
            color: #666;
            text-align: center;
        }

        .workspace {
            display: flex;
            flex-direction: column;
            flex: 1;
            min-height: 0;
        }

        .titlebar {
            display: flex;
            gap: 8px;
            padding: 10px 16px 6px;
        }

        .titlebar input {
            flex: 1;
            font-size: 20px;
            font-weight: bold;
            border: 1px solid transparent;
            padding: 4px 6px;
        }

        .titlebar input:focus {
            border-color: #aaa;
        }

        .toolbar {
            display: flex;
            align-items: center;
            gap: 6px;
            padding: 0 16px 8px;
            border-bottom: 1px solid #ddd;
        }

        .toolbar .state {
            margin-left: auto;
            color: #555;
            font-size: 12px;
        }

        #editor {
            flex: 1;
            width: 100%;
            border: none;
            outline: none;
            resize: none;
            padding: 16px 24px;
            font-family: Georgia, "Times New Roman", serif;
            font-size: 16px;
            line-height: 1.6;
        }

        .footer {
            display: flex;
            justify-content: space-between;
            padding: 6px 16px;
            border-top: 1px solid #ddd;
            background: #f7f7f7;
            color: #555;
            font-size: 12px;
        }

        .tabs {
            display: flex;
            border-bottom: 1px solid #ccc;
        }

        .tabs button {
            flex: 1;
            border: none;
            border-right: 1px solid #ccc;
            background: #f0f0f0;
        }

        .tabs button.active {
            background: #fff;
            font-weight: bold;
        }

        .row {
            padding: 7px 10px;
            border-bottom: 1px solid #e4e4e4;
            font-size: 12px;
            cursor: pointer;
        }

        .row:hover {
            background: #eee;
        }

        .row.active {
            background: #e2e8f0;
        }

        .preview {
            border-top: 1px solid #ccc;
            padding: 8px;
            background: #fff;
            max-height: 45%;
            display: flex;
            flex-direction: column;
            gap: 6px;
        }

        .preview pre {
            margin: 0;
            overflow: auto;
            white-space: pre-wrap;
            font-size: 12px;
            max-height: 160px;
            background: #f4f4f4;
            padding: 6px;
        }

        .preview .actions {
            display: flex;
            gap: 6px;
        }

        .note {
            padding: 10px;
            color: #666;
            font-size: 12px;
        }

        [hidden] {
            display: none !important;
        }
    </style>
</head>
<body>
    <div class="topbar">
        <span class="brand">Text Editor</span>
        <button id="btn-new">New document</button>
        <button id="btn-upload">Upload .txt</button>
        <input type="file" id="file-input" accept=".txt,text/plain" hidden>
    </div>

    <div class="layout">
        <div class="sidebar">
            <div class="search">
                <input type="text" id="search" placeholder="Search files">
            </div>
            <div class="scroll" id="list"></div>
        </div>

        <div class="main">
            <div class="empty" id="empty">
                <p>No document is open.</p>
                <p>Create a new document or upload a .txt file.</p>
            </div>

            <div class="workspace" id="workspace" hidden>
                <div class="titlebar">
                    <input type="text" id="title" placeholder="Untitled" maxlength="120">
                </div>
                <div class="toolbar">
                    <button id="btn-snapshot">Save version</button>
                    <button id="btn-download">Download</button>
                    <button id="btn-delete" class="danger">Delete</button>
                    <span class="state" id="status">Saved</span>
                </div>
                <textarea id="editor" spellcheck="true" placeholder="Start typing"></textarea>
                <div class="footer">
                    <span id="counts">0 words, 0 characters</span>
                    <span id="editors">1 person here</span>
                </div>
            </div>
        </div>

        <div class="panel">
            <div class="tabs">
                <button id="tab-versions" class="active">Versions</button>
                <button id="tab-events">Events</button>
            </div>
            <div class="scroll" id="panel-body"></div>
            <div class="preview" id="preview" hidden>
                <div id="preview-label"></div>
                <pre id="preview-text"></pre>
                <div class="actions">
                    <button id="btn-restore">Restore</button>
                    <button id="btn-download-version">Download</button>
                    <button id="btn-close-preview">Close</button>
                </div>
            </div>
        </div>
    </div>

    <script>
        const clientId = Math.random().toString(36).slice(2, 10);
        const state = {
            id: null,
            rev: 0,
            dirty: false,
            saving: false,
            timer: null,
            panel: "versions",
            viewing: null
        };

        const $ = (id) => document.getElementById(id);

        async function api(url, options) {
            const res = await fetch(url, options);
            if (!res.ok) {
                let message = "Request failed";
                try {
                    message = (await res.json()).error || message;
                } catch (e) {}
                const error = new Error(message);
                error.status = res.status;
                throw error;
            }
            return res;
        }

        async function getJson(url, options) {
            return (await api(url, options)).json();
        }

        function sendJson(method, body) {
            return {
                method: method,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body)
            };
        }

        function formatTime(ts) {
            return new Date(ts * 1000).toLocaleString([], {
                month: "short",
                day: "numeric",
                hour: "2-digit",
                minute: "2-digit"
            });
        }

        function setStatus(text) {
            $("status").textContent = text;
        }

        function updateCounts() {
            const text = $("editor").value;
            const words = text.trim() ? text.trim().split(/\s+/).length : 0;
            $("counts").textContent = words + " words, " + text.length + " characters";
        }

        function showWorkspace(visible) {
            $("workspace").hidden = !visible;
            $("empty").hidden = visible;
        }

        function applyText(text) {
            const area = $("editor");
            const old = area.value;
            if (old === text) {
                return;
            }
            let cursor = area.selectionStart;
            let prefix = 0;
            const limit = Math.min(old.length, text.length);
            while (prefix < limit && old[prefix] === text[prefix]) {
                prefix++;
            }
            if (cursor > prefix) {
                cursor = Math.max(prefix, cursor + (text.length - old.length));
            }
            area.value = text;
            area.setSelectionRange(cursor, cursor);
            updateCounts();
        }

        async function loadList() {
            const q = encodeURIComponent($("search").value);
            const docs = await getJson("/v1/api/docs?q=" + q);
            const list = $("list");
            list.innerHTML = "";
            if (!docs.length) {
                const note = document.createElement("div");
                note.className = "note";
                note.textContent = "No files found.";
                list.appendChild(note);
            }
            docs.forEach((doc) => {
                const item = document.createElement("div");
                item.className = "item" + (doc.id === state.id ? " active" : "");
                const name = document.createElement("div");
                name.className = "name";
                name.textContent = doc.title + ".txt";
                const sub = document.createElement("div");
                sub.className = "sub";
                sub.textContent = formatTime(doc.updated) + " · " + doc.versions + " version" + (doc.versions === 1 ? "" : "s") + " · " + doc.size + " B";
                item.appendChild(name);
                item.appendChild(sub);
                item.onclick = () => openDoc(doc.id);
                list.appendChild(item);
            });
        }

        async function openDoc(id) {
            if (state.dirty) {
                await save(false);
            }
            const doc = await getJson("/v1/api/docs/" + id);
            state.id = doc.id;
            state.rev = doc.rev;
            state.dirty = false;
            state.viewing = null;
            $("preview").hidden = true;
            $("title").value = doc.title;
            $("editor").value = doc.content;
            showWorkspace(true);
            updateCounts();
            setStatus("Saved");
            await loadList();
            await refreshPanel();
        }

        function closeDoc() {
            state.id = null;
            state.dirty = false;
            showWorkspace(false);
            $("panel-body").innerHTML = "";
            $("preview").hidden = true;
            loadList();
        }

        function scheduleSave() {
            state.dirty = true;
            setStatus("Editing");
            clearTimeout(state.timer);
            state.timer = setTimeout(() => save(false), 1000);
        }

        async function save(snapshot) {
            if (!state.id) {
                return;
            }
            if (state.saving) {
                if (snapshot) {
                    setTimeout(() => save(true), 300);
                }
                return;
            }
            clearTimeout(state.timer);
            state.saving = true;
            const sent = $("editor").value;
            const sentTitle = $("title").value;
            setStatus("Saving");
            try {
                const result = await getJson(
                    "/v1/api/docs/" + state.id,
                    sendJson("PUT", {
                        content: sent,
                        title: sentTitle,
                        base_rev: state.rev,
                        snapshot: !!snapshot,
                        client: clientId
                    })
                );
                state.rev = result.rev;
                if ($("editor").value === sent) {
                    if (result.content !== sent) {
                        applyText(result.content);
                    }
                    state.dirty = false;
                    setStatus("Saved");
                } else {
                    state.dirty = true;
                }
                await loadList();
                if (state.panel === "versions") {
                    await refreshPanel();
                }
            } catch (error) {
                setStatus("Save failed: " + error.message);
            }
            state.saving = false;
            if (state.dirty) {
                state.timer = setTimeout(() => save(false), 600);
            }
        }

        async function poll() {
            if (!state.id) {
                return;
            }
            try {
                const result = await getJson("/v1/api/docs/" + state.id + "/poll?since=" + state.rev + "&client=" + clientId);
                $("editors").textContent = result.editors + (result.editors === 1 ? " person here" : " people here");
                if (result.changed && !state.dirty && !state.saving) {
                    state.rev = result.rev;
                    applyText(result.content);
                    if (document.activeElement !== $("title")) {
                        $("title").value = result.title;
                    }
                    setStatus("Updated from another editor");
                    loadList();
                    refreshPanel();
                }
            } catch (error) {
                if (error.status === 404) {
                    closeDoc();
                }
            }
        }

        function selectTab(name) {
            state.panel = name;
            $("tab-versions").className = name === "versions" ? "active" : "";
            $("tab-events").className = name === "events" ? "active" : "";
            if (name !== "versions") {
                $("preview").hidden = true;
            }
            refreshPanel();
        }

        async function refreshPanel() {
            const body = $("panel-body");
            if (state.panel === "events") {
                const events = await getJson("/v1/api/events");
                body.innerHTML = "";
                if (!events.length) {
                    const note = document.createElement("div");
                    note.className = "note";
                    note.textContent = "No events yet.";
                    body.appendChild(note);
                }
                events.forEach((event) => {
                    const row = document.createElement("div");
                    row.className = "row";
                    row.textContent = formatTime(event.ts) + " · " + event.topic + " · " + event.title;
                    body.appendChild(row);
                });
                return;
            }
            if (!state.id) {
                return;
            }
            const versions = await getJson("/v1/api/docs/" + state.id + "/versions");
            body.innerHTML = "";
            versions.slice().reverse().forEach((version) => {
                const row = document.createElement("div");
                row.className = "row" + (state.viewing === version.n ? " active" : "");
                row.textContent = "v" + version.n + " · " + formatTime(version.ts) + " · " + version.note + " · " + version.size + " B";
                row.onclick = () => previewVersion(version.n);
                body.appendChild(row);
            });
        }

        async function previewVersion(number) {
            const version = await getJson("/v1/api/docs/" + state.id + "/versions/" + number);
            state.viewing = number;
            $("preview-label").textContent = "Version " + number + " · " + formatTime(version.ts);
            $("preview-text").textContent = version.content || "(empty)";
            $("preview").hidden = false;
            refreshPanel();
        }

        async function restoreVersion() {
            if (!state.viewing) {
                return;
            }
            const doc = await getJson("/v1/api/docs/" + state.id + "/restore/" + state.viewing, { method: "POST" });
            state.rev = doc.rev;
            state.dirty = false;
            $("editor").value = doc.content;
            updateCounts();
            setStatus("Restored version " + state.viewing);
            state.viewing = null;
            $("preview").hidden = true;
            await loadList();
            await refreshPanel();
        }

        async function download(version) {
            if (state.dirty) {
                await save(false);
            }
            const res = await api("/v1/api/docs/" + state.id + "/download", sendJson("POST", { version: version || null }));
            const blob = await res.blob();
            const url = URL.createObjectURL(blob);
            const link = document.createElement("a");
            const name = ($("title").value || "Untitled") + (version ? "_v" + version : "") + ".txt";
            link.href = url;
            link.download = name;
            document.body.appendChild(link);
            link.click();
            link.remove();
            URL.revokeObjectURL(url);
        }

        async function createDoc() {
            const doc = await getJson("/v1/api/docs/create", sendJson("POST", { title: "Untitled", content: "" }));
            await openDoc(doc.id);
            $("title").focus();
            $("title").select();
        }

        async function uploadDoc(file) {
            const form = new FormData();
            form.append("file", file);
            try {
                const doc = await getJson("/v1/api/docs/create", { method: "POST", body: form });
                await openDoc(doc.id);
            } catch (error) {
                alert(error.message);
            }
        }

        async function deleteDoc() {
            if (!state.id || !confirm("Delete this document and all of its versions?")) {
                return;
            }
            await api("/v1/api/docs/" + state.id, { method: "DELETE" });
            closeDoc();
        }

        $("btn-new").onclick = createDoc;
        $("btn-upload").onclick = () => $("file-input").click();
        $("file-input").onchange = (event) => {
            if (event.target.files.length) {
                uploadDoc(event.target.files[0]);
                event.target.value = "";
            }
        };
        $("btn-snapshot").onclick = () => save(true);
        $("btn-download").onclick = () => download(null);
        $("btn-delete").onclick = deleteDoc;
        $("btn-restore").onclick = restoreVersion;
        $("btn-download-version").onclick = () => download(state.viewing);
        $("btn-close-preview").onclick = () => {
            state.viewing = null;
            $("preview").hidden = true;
            refreshPanel();
        };
        $("tab-versions").onclick = () => selectTab("versions");
        $("tab-events").onclick = () => selectTab("events");
        $("search").oninput = loadList;
        $("editor").oninput = () => {
            updateCounts();
            scheduleSave();
        };
        $("title").oninput = scheduleSave;

        setInterval(poll, 2000);
        setInterval(loadList, 8000);

        (async function start() {
            await loadList();
            const docs = await getJson("/v1/api/docs");
            if (docs.length) {
                await openDoc(docs[0].id);
            }
        })();
    </script>
</body>
</html>
"""


create_doc(
    "Welcome",
    "This is a collaborative text editor with no login.\n\n"
    "Edits save automatically and create versions.\n"
    "Open this page in a second tab to edit the same document at the same time.\n",
)

if "SERVER" in globals() and SERVER is not None:
    try:
        SERVER.shutdown()
    except Exception:
        pass

threading.Thread(target=consumer, daemon=True).start()
SERVER = make_server("0.0.0.0", PORT, app, threaded=True)
threading.Thread(target=SERVER.serve_forever, daemon=True).start()

try:
    from google.colab import output

    output.serve_kernel_port_as_iframe(PORT, height=820)
except ImportError:
    print("Open http://localhost:%d in your browser" % PORT)