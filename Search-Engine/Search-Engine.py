# SEEKLY : Search Engine (Google-style)
'''
1. Requirement analysis 
A. functional requrement 
- search into internet 
- view results from internet
B. non functional requrement 
- scale : 10M sites fetching
- Cap theroem - availabe for search >>>>>>>>>>> consistent 

2. Identifying Entities 
- user
- search engine
- internet

3. API 
A. Search
GET /v1/search?query={search-term}-> List<searchID> : pagenation 
D. View Menu
GET /v1/pages={searchID} -> sites detail from internet  

4. Hih level design 
user -> api gateway -> sever ( search , view ) -> database ( s3 , mongoDB , cache ) 

5. low level design 
uploaded image
'''

import subprocess
import sys

try:
    import flask    
    import requests
    import bs4     
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q",
                           "flask", "requests", "beautifulsoup4"])

import collections
import gzip
import hashlib
import itertools
import math
import os
import queue
import re
import threading
import time
import zlib
from urllib.parse import quote, urldefrag, urlparse
from urllib import robotparser

import requests
from bs4 import BeautifulSoup
from flask import Flask, Response, jsonify, request
from werkzeug.serving import make_server


class Config:
    PORT = 5055
    USER_AGENT = "SeeklyBot/1.0 (educational Colab project)"

    CRAWL_WORKERS = 4
    MAX_PAGES = 1000           
    MAX_DEPTH = 1          
    LINKS_PER_PAGE = 12
    PER_DOMAIN_CAP = 250
    FETCH_TIMEOUT = 8
    MAX_BYTES = 1_500_000
    CRAWL_DELAY = 0.25        
    N_SHARDS = 4                # inverted index is split by hash(term) % N_SHARDS
    REINDEX_INTERVAL = 2.0      # seconds between index snapshot swaps
    MIN_RESULTS = 5             # fewer hits than this -> discover new pages
    BM25_K1 = 1.4
    BM25_B = 0.75


    PAGE_SIZE = 8
    MAX_PAGE_SIZE = 20
    MAX_QUERY_LEN = 200

    CACHE_SIZE = 500
    CACHE_TTL = 300
    RATE_LIMIT = 60             # requests per window per client
    RATE_WINDOW = 10

    BLOB_DIR = "/content/seekly_blobstore"


SEED_URLS = [
    "https://en.wikipedia.org/wiki/Search_engine",
    "https://en.wikipedia.org/wiki/Web_crawler",
    "https://en.wikipedia.org/wiki/Inverted_index",
    "https://en.wikipedia.org/wiki/PageRank",
    "https://en.wikipedia.org/wiki/CAP_theorem",
    "https://en.wikipedia.org/wiki/Machine_learning",
    "https://www.python.org/",
    "https://docs.python.org/3/tutorial/index.html",
]

STOPWORDS = set(
    "a an and are as at be but by for from has have how i in is it its of on or "
    "that the their this to was were what when where which who why will with you your".split()
)

def stem(word):
    """Very light suffix stripper so 'crawling' and 'crawler' meet 'crawl'."""
    if word.endswith("ss"):
        return word
    for suffix in ("ingly", "edly", "ing", "ed", "es", "s"):
        if len(word) > len(suffix) + 2 and word.endswith(suffix):
            return word[: -len(suffix)]
    return word


def tokenize(text, drop_stopwords=True):
    words = re.findall(r"[a-z0-9]+", text.lower())
    if drop_stopwords:
        words = [w for w in words if w not in STOPWORDS]
    return [stem(w) for w in words]


def normalize_url(url):
    try:
        url, _ = urldefrag(url.strip())
        parts = urlparse(url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            return None
        return parts._replace(netloc=parts.netloc.lower(), path=parts.path or "/").geturl()
    except Exception:
        return None


def search_id_of(url):
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]


def now_utc():
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())


def make_snippet(doc, query):
    text = doc.get("preview") or ""
    words = [w for w in re.findall(r"[A-Za-z0-9]+", query) if len(w) > 1]
    if words and text:
        match = re.search(r"\b(" + "|".join(map(re.escape, words)) + ")", text, re.I)
        if match:
            start = max(0, match.start() - 90)
            end = min(len(text), match.start() + 230)
            snippet = text[start:end].strip()
            return ("... " if start > 0 else "") + snippet + (" ..." if end < len(text) else "")
    return (doc.get("description") or text[:240]).strip()


SKIP_EXTENSIONS = (
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".pdf", ".zip", ".gz",
    ".mp3", ".mp4", ".avi", ".mov", ".css", ".js", ".json", ".xml", ".exe", ".dmg",
)


def parse_page(raw_bytes, page_url):
    soup = BeautifulSoup(raw_bytes, "html.parser")

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    if not title:
        first_h1 = soup.find("h1")
        title = first_h1.get_text(" ", strip=True) if first_h1 else page_url
    title = re.sub(r"\s+", " ", title)[:160]

    meta = (
        soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        or soup.find("meta", attrs={"property": re.compile(r"^og:description$", re.I)})
    )
    description = re.sub(r"\s+", " ", meta.get("content", "")).strip()[:300] if meta else ""

    headings = [
        re.sub(r"\s+", " ", h.get_text(" ", strip=True))[:120]
        for h in soup.find_all(["h1", "h2", "h3"])[:12]
    ]
    headings = [h for h in headings if h]

    for tag in soup(["script", "style", "noscript", "nav", "footer", "header",
                     "form", "svg", "iframe", "aside"]):
        tag.decompose()

    links = []
    seen_links = set()
    for anchor in soup.find_all("a", href=True):
        absolute = normalize_url(requests.compat.urljoin(page_url, anchor["href"]))
        if not absolute or absolute in seen_links:
            continue
        parts = urlparse(absolute)
        if parts.path.lower().endswith(SKIP_EXTENSIONS):
            continue
        if "wikipedia.org" in parts.netloc and (
            ":" in parts.path.replace("/wiki/", "", 1) or "index.php" in parts.path
        ):
            continue
        seen_links.add(absolute)
        links.append(absolute)

    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    if len(text) < 200:
        return None

    return {
        "title": title,
        "description": description,
        "headings": headings,
        "text": text,
        "links": links,
    }


#  STORAGE LAYER 
class BlobStore:
    """S3 stand-in: immutable objects addressed by key."""

    def __init__(self, root):
        self.root = root
        os.makedirs(os.path.join(root, "raw"), exist_ok=True)
        os.makedirs(os.path.join(root, "text"), exist_ok=True)
        self.objects = 0

    def put_text(self, sid, text):
        with open(os.path.join(self.root, "text", sid + ".txt"), "w", encoding="utf-8") as f:
            f.write(text)
        self.objects += 1
        return "s3://seekly-pages/text/" + sid + ".txt"

    def put_raw(self, sid, raw_bytes):
        with gzip.open(os.path.join(self.root, "raw", sid + ".html.gz"), "wb") as f:
            f.write(raw_bytes)
        self.objects += 1

    def get_text(self, sid):
        try:
            with open(os.path.join(self.root, "text", sid + ".txt"), encoding="utf-8") as f:
                return f.read()
        except OSError:
            return ""


class DocStore:

    def __init__(self):
        self._docs = {}
        self._lock = threading.Lock()
        self.inlinks = collections.Counter()

    def put(self, doc):
        with self._lock:
            self._docs[doc["searchID"]] = doc

    def get(self, sid):
        return self._docs.get(sid)

    def has(self, sid):
        return sid in self._docs

    def count(self):
        return len(self._docs)

    def add_inlink(self, sid):
        with self._lock:
            self.inlinks[sid] += 1


class LRUCache:
    def __init__(self, capacity, ttl):
        self.capacity = capacity
        self.ttl = ttl
        self._data = collections.OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
            if item is None:
                self.misses += 1
                return None
            expires, value = item
            if expires < time.time():
                del self._data[key]
                self.misses += 1
                return None
            self._data.move_to_end(key)
            self.hits += 1
            return value

    def put(self, key, value):
        with self._lock:
            self._data[key] = (time.time() + self.ttl, value)
            self._data.move_to_end(key)
            while len(self._data) > self.capacity:
                self._data.popitem(last=False)

    def stats(self):
        total = self.hits + self.misses
        return {
            "size": len(self._data),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": round(self.hits / total, 3) if total else 0.0,
        }


# INDEXER + RANKING 
class SearchIndex:
    def __init__(self, docs, n_shards):
        self.docs = docs
        self.n_shards = n_shards
        self.snapshot = self._empty()
        self.pending = []
        self._pending_lock = threading.Lock()

    def _empty(self):
        return {
            "shards": [{} for _ in range(self.n_shards)],
            "doclen": {},
            "title_tokens": {},
            "N": 0,
            "avgdl": 0.0,
            "version": 0,
        }

    def shard_of(self, term):
        return zlib.crc32(term.encode("utf-8")) % self.n_shards

    def enqueue(self, sid, title, text):
        title_tokens = tokenize(title)
        tokens = title_tokens * 2 + tokenize(text)[:6000]   # title counts double
        if not tokens:
            return
        with self._pending_lock:
            self.pending.append((sid, title_tokens, tokens))

    def merge(self):
        with self._pending_lock:
            batch, self.pending = self.pending, []
        if not batch:
            return 0

        old = self.snapshot
        shards = [dict(shard) for shard in old["shards"]]
        doclen = dict(old["doclen"])
        title_tokens = dict(old["title_tokens"])
        touched = set()

        for sid, doc_title_tokens, tokens in batch:
            if sid in doclen:
                continue
            for term, tf in collections.Counter(tokens).items():
                shard_no = self.shard_of(term)
                if (shard_no, term) not in touched:
                    shards[shard_no][term] = dict(shards[shard_no].get(term, {}))
                    touched.add((shard_no, term))
                shards[shard_no][term][sid] = tf
            doclen[sid] = len(tokens)
            title_tokens[sid] = frozenset(doc_title_tokens)

        total_docs = len(doclen)
        self.snapshot = {
            "shards": shards,
            "doclen": doclen,
            "title_tokens": title_tokens,
            "N": total_docs,
            "avgdl": sum(doclen.values()) / max(total_docs, 1),
            "version": old["version"] + 1,
        }
        return len(batch)

    def search(self, query):
        terms = tokenize(query) or tokenize(query, drop_stopwords=False)
        unique_terms = list(dict.fromkeys(terms))
        if not unique_terms:
            return []

        snap = self.snapshot                      # one reference = consistent view, no lock
        N, avgdl = snap["N"], snap["avgdl"] or 1.0
        k1, b = Config.BM25_K1, Config.BM25_B
        scores = collections.defaultdict(float)
        matched = collections.Counter()

        for term in unique_terms:
            postings = snap["shards"][self.shard_of(term)].get(term)
            if not postings:
                continue
            df = len(postings)
            idf = math.log(1 + (N - df + 0.5) / (df + 0.5))
            for sid, tf in postings.items():
                dl = snap["doclen"][sid]
                scores[sid] += idf * tf * (k1 + 1) / (tf + k1 * (1 - b + b * dl / avgdl))
                matched[sid] += 1

        term_set = set(unique_terms)
        ranked = []
        for sid, score in scores.items():
            coverage = (matched[sid] / len(unique_terms)) ** 1.5
            title_overlap = len(snap["title_tokens"].get(sid, frozenset()) & term_set) / len(term_set)
            authority = 1 + 0.15 * math.log1p(self.docs.inlinks.get(sid, 0))
            ranked.append((sid, score * coverage * (1 + 0.8 * title_overlap) * authority))

        ranked.sort(key=lambda pair: pair[1], reverse=True)
        return ranked

class Crawler:
    def __init__(self, docs, blobs, index, stop_event):
        self.docs = docs
        self.blobs = blobs
        self.index = index
        self.stop = stop_event

        self.frontier = queue.PriorityQueue()
        self.seen = set()
        self.processed = set()
        self.domain_counts = collections.Counter()
        self.robots = {}
        self.discoveries = collections.OrderedDict()
        self.counters = collections.Counter()
        self._counter = itertools.count()
        self._lock = threading.Lock()

    def submit(self, url, depth=0, priority=5):
        url = normalize_url(url)
        if not url:
            return None
        sid = search_id_of(url)
        with self._lock:
            if sid in self.seen:
                return url
            self.seen.add(sid)
            self.frontier.put((priority + depth, next(self._counter), url, depth))
        return url

    def start(self):
        for i in range(Config.CRAWL_WORKERS):
            threading.Thread(target=self._worker, name="crawler-" + str(i), daemon=True).start()
        threading.Thread(target=self._indexer_loop, name="indexer", daemon=True).start()

    def _bump(self, key):
        with self._lock:
            self.counters[key] += 1

    def _indexer_loop(self):
        while not self.stop.is_set():
            try:
                self.index.merge()
            except Exception:
                self._bump("index_errors")
            self.stop.wait(Config.REINDEX_INTERVAL)

    def _worker(self):
        while not self.stop.is_set():
            try:
                _, _, url, depth = self.frontier.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self._process(url, depth)
            except Exception:
                self._bump("failed")
            finally:
                self.processed.add(url)
                self.frontier.task_done()
                time.sleep(Config.CRAWL_DELAY)

    def _allowed(self, url):
        parts = urlparse(url)
        base = parts.scheme + "://" + parts.netloc
        parser = self.robots.get(base)
        if parser is None:
            parser = robotparser.RobotFileParser()
            try:
                resp = requests.get(base + "/robots.txt", timeout=5,
                                    headers={"User-Agent": Config.USER_AGENT})
                parser.parse(resp.text.splitlines() if resp.status_code == 200 else [])
            except Exception:
                parser.parse([])
            parser.modified()
            self.robots[base] = parser
        return parser.can_fetch(Config.USER_AGENT, url)

    def _fetch(self, url):
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": Config.USER_AGENT, "Accept": "text/html,application/xhtml+xml"},
                timeout=Config.FETCH_TIMEOUT,
                stream=True,
            )
            content_type = resp.headers.get("Content-Type", "")
            if resp.status_code != 200 or "html" not in content_type.lower():
                resp.close()
                return None
            raw = resp.raw.read(Config.MAX_BYTES + 1, decode_content=True)[: Config.MAX_BYTES]
            resp.close()
            return resp.url, raw, content_type.split(";")[0].strip()
        except Exception:
            return None

    def _process(self, url, depth):
        if self.docs.count() >= Config.MAX_PAGES:
            self._bump("skipped_cap")
            return

        host = urlparse(url).netloc
        with self._lock:
            if self.domain_counts[host] >= Config.PER_DOMAIN_CAP:
                self.counters["skipped_domain_cap"] += 1
                return
            self.domain_counts[host] += 1

        if not self._allowed(url):
            self._bump("robots_blocked")
            return

        fetched = self._fetch(url)
        if not fetched:
            self._bump("failed")
            return

        final_url, raw, content_type = fetched
        final_url = normalize_url(final_url) or url
        sid = search_id_of(final_url)
        if self.docs.has(sid):
            self._bump("duplicates")
            return

        page = parse_page(raw, final_url)
        if page is None:
            self._bump("thin_pages")
            return

        s3_key = self.blobs.put_text(sid, page["text"])
        self.blobs.put_raw(sid, raw)

        self.docs.put({
            "searchID": sid,
            "url": final_url,
            "domain": urlparse(final_url).netloc,
            "title": page["title"],
            "description": page["description"],
            "headings": page["headings"],
            "preview": page["text"][:3000],
            "word_count": len(page["text"].split()),
            "size_bytes": len(raw),
            "content_type": content_type,
            "fetched_at": now_utc(),
            "s3_key": s3_key,
            "outlinks": page["links"][:25],
            "depth": depth,
        })
        self.index.enqueue(sid, page["title"], page["text"])
        self._bump("fetched")

        for link in page["links"]:
            self.docs.add_inlink(search_id_of(link))
        if depth < Config.MAX_DEPTH:
            for link in page["links"][: Config.LINKS_PER_PAGE]:
                self.submit(link, depth + 1, priority=5)

    def discover_async(self, query):
        key = query.lower()
        with self._lock:
            if key in self.discoveries:
                return
            self.discoveries[key] = {"t": time.time(), "urls": [], "submitted": False}
            while len(self.discoveries) > 500:
                self.discoveries.popitem(last=False)
        threading.Thread(target=self._discover, args=(key,), daemon=True).start()

    def _discover(self, key):
        urls = []
        headers = {"User-Agent": Config.USER_AGENT}
        try:
            resp = requests.get(
                "https://en.wikipedia.org/w/api.php",
                params={"action": "query", "list": "search", "srsearch": key,
                        "srlimit": 8, "format": "json"},
                headers=headers, timeout=6,
            )
            for item in resp.json().get("query", {}).get("search", []):
                title = item["title"].replace(" ", "_")
                urls.append("https://en.wikipedia.org/wiki/" + quote(title, safe="_()"))
        except Exception:
            pass
        try:
            resp = requests.get(
                "https://hn.algolia.com/api/v1/search",
                params={"query": key, "tags": "story", "hitsPerPage": 10},
                headers=headers, timeout=6,
            )
            for hit in resp.json().get("hits", []):
                if hit.get("url"):
                    urls.append(hit["url"])
        except Exception:
            pass

        submitted = [u for u in (self.submit(u, 0, priority=0) for u in urls) if u]
        entry = self.discoveries.get(key)
        if entry is not None:
            entry["urls"] = submitted
            entry["submitted"] = True

    def is_discovering(self, query):
        entry = self.discoveries.get(query.lower())
        if not entry:
            return False
        if not entry["submitted"]:
            return True
        if time.time() - entry["t"] > 45:
            return False
        return any(u not in self.processed for u in entry["urls"])


# SERVICES  (Search, View)  +  API GATEWAY
class SearchService:
    def __init__(self, index, docs, cache, crawler):
        self.index = index
        self.docs = docs
        self.cache = cache
        self.crawler = crawler

    def search(self, query, page, size):
        started = time.perf_counter()
        version = self.index.snapshot["version"]
        key = "s|%d|%s|%d|%d" % (version, query.lower(), page, size)

        base = self.cache.get(key)
        cache_state = "HIT"
        if base is None:
            cache_state = "MISS"
            ranked = self.index.search(query)
            total = len(ranked)
            start = (page - 1) * size
            results = []
            for sid, score in ranked[start:start + size]:
                doc = self.docs.get(sid)
                if not doc:
                    continue
                results.append({
                    "searchID": sid,
                    "title": doc["title"],
                    "url": doc["url"],
                    "domain": doc["domain"],
                    "snippet": make_snippet(doc, query),
                    "score": round(score, 3),
                })
            base = {
                "query": query,
                "page": page,
                "size": size,
                "total": total,
                "pages": math.ceil(total / size) if total else 0,
                "search_ids": [r["searchID"] for r in results],
                "results": results,
            }
            if total >= Config.MIN_RESULTS:
                self.cache.put(key, base)

        indexing = False
        if base["total"] < Config.MIN_RESULTS:
            self.crawler.discover_async(query)
            indexing = self.crawler.is_discovering(query) or bool(self.index.pending)

        out = dict(base)
        out["cache"] = cache_state
        out["indexing"] = indexing
        out["took_ms"] = round((time.perf_counter() - started) * 1000, 1)
        return out


class ViewService:
    def __init__(self, docs, blobs, cache):
        self.docs = docs
        self.blobs = blobs
        self.cache = cache

    def view(self, sid):
        key = "v|" + sid
        cached = self.cache.get(key)
        if cached is not None:
            return dict(cached, cache="HIT")

        doc = self.docs.get(sid)
        if not doc:
            return None
        text = self.blobs.get_text(sid)
        detail = {
            "searchID": sid,
            "title": doc["title"],
            "url": doc["url"],
            "domain": doc["domain"],
            "description": doc["description"],
            "headings": doc["headings"],
            "excerpt": text[:2500],
            "word_count": doc["word_count"],
            "size_bytes": doc["size_bytes"],
            "content_type": doc["content_type"],
            "fetched_at": doc["fetched_at"],
            "s3_key": doc["s3_key"],
            "inlinks": self.docs.inlinks.get(sid, 0),
            "outlinks": doc["outlinks"],
        }
        self.cache.put(key, detail)
        return dict(detail, cache="MISS")


class Gateway:

    def __init__(self):
        self._hits = collections.defaultdict(collections.deque)
        self._lock = threading.Lock()
        self._nodes = {
            "search": itertools.cycle(["search-node-1", "search-node-2"]),
            "view": itertools.cycle(["view-node-1", "view-node-2"]),
        }

    def allow(self, client):
        now = time.time()
        with self._lock:
            hits = self._hits[client]
            while hits and now - hits[0] > Config.RATE_WINDOW:
                hits.popleft()
            if len(hits) >= Config.RATE_LIMIT:
                return False, round(Config.RATE_WINDOW - (now - hits[0]), 1)
            hits.append(now)
            return True, 0

    def route(self, service):
        with self._lock:
            return next(self._nodes[service])


# WEB SERVER  
INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Seekly</title>
    <style>
        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            background: #ffffff;
            color: #202124;
            font-family: Arial, Helvetica, sans-serif;
            font-size: 14px;
            line-height: 1.58;
        }

        a {
            color: #1a0dab;
            text-decoration: none;
        }

        a:hover {
            text-decoration: underline;
        }

        a:visited {
            color: #681da8;
        }

        .bar {
            display: flex;
            align-items: center;
            gap: 28px;
            padding: 20px 24px 14px;
            border-bottom: 1px solid #ebebeb;
        }

        .logo {
            font-size: 26px;
            font-weight: 700;
            color: #202124;
        }

        .logo:visited {
            color: #202124;
        }

        .logo:hover {
            text-decoration: none;
        }

        form {
            display: flex;
            align-items: center;
            gap: 10px;
            flex: 1;
            max-width: 640px;
        }

        input[type="search"] {
            flex: 1;
            min-width: 0;
            height: 44px;
            border: 1px solid #dfe1e5;
            border-radius: 24px;
            padding: 0 20px;
            font-size: 16px;
            color: #202124;
            outline: none;
        }

        input[type="search"]:hover,
        input[type="search"]:focus {
            border-color: #bdc1c6;
        }

        button.search {
            height: 40px;
            border: 1px solid #f8f9fa;
            border-radius: 4px;
            background: #f8f9fa;
            color: #3c4043;
            font-size: 14px;
            padding: 0 16px;
            cursor: pointer;
        }

        button.search:hover {
            border-color: #dadce0;
        }

        body.home .bar {
            flex-direction: column;
            gap: 22px;
            border-bottom: 0;
            padding-top: 24vh;
        }

        body.home .logo {
            font-size: 64px;
        }

        body.home form {
            width: 100%;
            max-width: 580px;
        }

        main {
            max-width: 652px;
            margin: 0 0 60px 180px;
            padding: 0 20px;
        }

        body.home main {
            display: none;
        }

        .count {
            color: #70757a;
            font-size: 13px;
            margin: 14px 0 6px;
        }

        .note {
            color: #70757a;
            margin: 14px 0;
        }

        .result {
            margin: 22px 0 28px;
        }

        .result .url {
            color: #202124;
            font-size: 12px;
            line-height: 1.4;
            word-break: break-all;
        }

        .result .title {
            display: inline-block;
            font-size: 20px;
            line-height: 1.3;
            margin: 2px 0 3px;
        }

        .result .snippet {
            margin: 0;
            color: #4d5156;
        }

        .result .view {
            display: inline-block;
            margin-top: 4px;
            font-size: 13px;
        }

        mark {
            background: transparent;
            color: inherit;
            font-weight: 700;
        }

        .pages {
            display: flex;
            gap: 18px;
            margin-top: 30px;
        }

        .pages a {
            cursor: pointer;
        }

        .pages .current {
            font-weight: 700;
        }

        .pages .disabled {
            color: #9aa0a6;
        }

        .back {
            display: inline-block;
            margin: 18px 0 8px;
        }

        .detail h1 {
            font-size: 22px;
            font-weight: 400;
            line-height: 1.3;
            margin: 8px 0 4px;
        }

        .detail .site {
            color: #1a0dab;
            word-break: break-all;
        }

        .detail dl {
            display: grid;
            grid-template-columns: max-content 1fr;
            gap: 4px 20px;
            margin: 20px 0;
        }

        .detail dt {
            color: #70757a;
        }

        .detail dd {
            margin: 0;
            word-break: break-all;
        }

        .detail h2 {
            font-size: 15px;
            margin: 24px 0 6px;
        }

        .detail p,
        .detail li {
            color: #4d5156;
        }

        .detail ul {
            margin: 0;
            padding-left: 18px;
        }

        @media (max-width: 900px) {
            main {
                margin-left: 0;
            }
        }

        @media (max-width: 600px) {
            .bar {
                flex-wrap: wrap;
                gap: 12px;
                padding: 14px 16px;
            }

            form {
                width: 100%;
                max-width: none;
            }

            body.home .bar {
                padding-top: 16vh;
            }

            main {
                padding: 0 16px;
            }
        }
    </style>
</head>

<body class="home">
    <header class="bar">
        <a class="logo" href="#" id="logo">Seekly</a>
        <form id="searchForm" role="search" autocomplete="off">
            <input id="queryInput" type="search" maxlength="200" aria-label="Search" autofocus>
            <button class="search" type="submit">Search</button>
        </form>
    </header>

    <main>
        <div id="results"></div>
        <div id="detail" class="detail" hidden></div>
    </main>

    <script>
        (function () {
            "use strict";

            var state = { query: "", page: 1, size: 8, token: 0, timer: null, polls: 0 };

            function $(selector) {
                return document.querySelector(selector);
            }

            function escapeHtml(value) {
                return String(value).replace(/[&<>"']/g, function (ch) {
                    return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch];
                });
            }

            function highlight(text, query) {
                var words = (query.toLowerCase().match(/[a-z0-9]+/g) || []).filter(function (w) {
                    return w.length > 1;
                });
                if (!words.length) {
                    return escapeHtml(text);
                }
                var re = new RegExp("\\b(" + words.join("|") + ")[a-z0-9]*", "gi");
                var out = "";
                var last = 0;
                var match;
                while ((match = re.exec(text)) !== null) {
                    out += escapeHtml(text.slice(last, match.index)) +
                           "<mark>" + escapeHtml(match[0]) + "</mark>";
                    last = match.index + match[0].length;
                }
                return out + escapeHtml(text.slice(last));
            }

            function api(path) {
                return fetch(path).then(function (res) {
                    return res.json().catch(function () { return {}; }).then(function (data) {
                        if (!res.ok) {
                            throw new Error(data.error || ("Request failed (" + res.status + ")"));
                        }
                        return data;
                    });
                });
            }

            function stopPolling() {
                if (state.timer) {
                    clearTimeout(state.timer);
                    state.timer = null;
                }
            }

            function showResults() {
                $("#detail").hidden = true;
                $("#results").hidden = false;
            }

            function search(query, page, isPoll) {
                query = query.trim();
                if (!query) { return; }
                if (!isPoll) {
                    stopPolling();
                    state.polls = 0;
                    state.token += 1;
                }
                var token = state.token;
                state.query = query;
                state.page = page;

                api("v1/search?query=" + encodeURIComponent(query) + "&page=" + page + "&size=" + state.size)
                    .then(function (data) {
                        if (token !== state.token) { return; }
                        render(data);
                        if (data.indexing && state.polls < 12) {
                            state.polls += 1;
                            state.timer = setTimeout(function () { search(query, page, true); }, 3000);
                        }
                    })
                    .catch(function (err) {
                        if (token !== state.token) { return; }
                        document.body.classList.remove("home");
                        showResults();
                        $("#results").innerHTML = '<p class="note">' + escapeHtml(err.message) +
                            ". Wait a few seconds and search again.</p>";
                    });
            }

            function render(data) {
                document.body.classList.remove("home");
                showResults();

                var html = '<div class="count">About ' + data.total.toLocaleString() +
                           " results (" + (data.took_ms / 1000).toFixed(2) + " seconds)</div>";

                if (data.indexing) {
                    html += '<p class="note">Fetching more pages from the web. This list updates on its own.</p>';
                }

                if (!data.results.length && !data.indexing) {
                    html += '<p class="note">No pages match "' + escapeHtml(data.query) +
                            '". Check the spelling or use fewer words.</p>';
                }

                html += data.results.map(function (r) {
                    return '<div class="result">' +
                        '<div class="url">' + escapeHtml(r.url) + "</div>" +
                        '<a class="title" href="' + escapeHtml(r.url) +
                        '" target="_blank" rel="noopener noreferrer">' + highlight(r.title, data.query) + "</a>" +
                        '<p class="snippet">' + highlight(r.snippet, data.query) + "</p>" +
                        '<a class="view" href="#" data-sid="' + escapeHtml(r.searchID) + '">View details</a>' +
                        "</div>";
                }).join("");

                html += pagination(data);
                $("#results").innerHTML = html;
            }

            function pagination(data) {
                if (data.pages <= 1) { return ""; }
                var first = Math.max(1, data.page - 4);
                var last = Math.min(data.pages, first + 8);
                first = Math.max(1, last - 8);

                var html = '<div class="pages">';
                html += data.page > 1
                    ? '<a data-page="' + (data.page - 1) + '">Previous</a>'
                    : '<span class="disabled">Previous</span>';
                for (var p = first; p <= last; p++) {
                    html += p === data.page
                        ? '<span class="current">' + p + "</span>"
                        : '<a data-page="' + p + '">' + p + "</a>";
                }
                html += data.page < data.pages
                    ? '<a data-page="' + (data.page + 1) + '">Next</a>'
                    : '<span class="disabled">Next</span>';
                return html + "</div>";
            }

            function showDetails(sid) {
                stopPolling();
                $("#results").hidden = true;
                var box = $("#detail");
                box.hidden = false;
                box.innerHTML = '<a class="back" href="#" id="back">Back to results</a><p class="note">Loading.</p>';

                api("v1/pages/" + encodeURIComponent(sid)).then(function (d) {
                    var headings = d.headings.length
                        ? "<h2>Headings</h2><ul>" + d.headings.map(function (h) {
                            return "<li>" + escapeHtml(h) + "</li>";
                        }).join("") + "</ul>"
                        : "";
                    var links = d.outlinks.length
                        ? "<h2>Links on this page</h2><ul>" + d.outlinks.slice(0, 10).map(function (l) {
                            return "<li>" + escapeHtml(l) + "</li>";
                        }).join("") + "</ul>"
                        : "";

                    box.innerHTML =
                        '<a class="back" href="#" id="back">Back to results</a>' +
                        "<h1>" + escapeHtml(d.title) + "</h1>" +
                        '<a class="site" href="' + escapeHtml(d.url) +
                        '" target="_blank" rel="noopener noreferrer">' + escapeHtml(d.url) + "</a>" +
                        (d.description ? "<p>" + escapeHtml(d.description) + "</p>" : "") +
                        "<dl>" +
                        "<dt>Search ID</dt><dd>" + escapeHtml(d.searchID) + "</dd>" +
                        "<dt>Fetched</dt><dd>" + escapeHtml(d.fetched_at) + "</dd>" +
                        "<dt>Size</dt><dd>" + d.size_bytes.toLocaleString() + " bytes, " +
                        d.word_count.toLocaleString() + " words</dd>" +
                        "<dt>Type</dt><dd>" + escapeHtml(d.content_type) + "</dd>" +
                        "<dt>Linked from</dt><dd>" + d.inlinks + " crawled pages</dd>" +
                        "<dt>Stored at</dt><dd>" + escapeHtml(d.s3_key) + "</dd></dl>" +
                        headings +
                        "<h2>Text from the page</h2><p>" + escapeHtml(d.excerpt) + "</p>" +
                        links;
                }).catch(function (err) {
                    box.innerHTML = '<a class="back" href="#" id="back">Back to results</a>' +
                        '<p class="note">' + escapeHtml(err.message) + ". Go back and try another result.</p>";
                });
            }

            $("#searchForm").addEventListener("submit", function (event) {
                event.preventDefault();
                search($("#queryInput").value, 1, false);
            });

            $("#logo").addEventListener("click", function (event) {
                event.preventDefault();
                stopPolling();
                state.token += 1;
                document.body.classList.add("home");
                $("#queryInput").value = "";
                $("#queryInput").focus();
            });

            document.addEventListener("click", function (event) {
                var view = event.target.closest("[data-sid]");
                if (view) {
                    event.preventDefault();
                    showDetails(view.getAttribute("data-sid"));
                    return;
                }
                var page = event.target.closest("[data-page]");
                if (page) {
                    search(state.query, parseInt(page.getAttribute("data-page"), 10), false);
                    window.scrollTo({ top: 0 });
                    return;
                }
                if (event.target.id === "back") {
                    event.preventDefault();
                    showResults();
                    if (state.query) { search(state.query, state.page, false); }
                }
            });
        })();
    </script>
</body>
</html>
"""


def build_app(gateway, search_service, view_service, index, docs, crawler, cache, blobs, started_at):
    app = Flask(__name__)

    @app.before_request
    def gateway_guard():
        if request.path.startswith("/v1/"):
            allowed, retry_after = gateway.allow(request.remote_addr or "client")
            if not allowed:
                return jsonify(error="Too many requests", retry_after=retry_after), 429

    @app.after_request
    def no_store(resp):
        resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.route("/")
    def home():
        return Response(INDEX_HTML, mimetype="text/html")

    # 3A. Search 
    # GET /v1/search?query={search-term}&page=1&size=8 -> List<searchID> (paginated)
    @app.route("/v1/search")
    def api_search():
        query = re.sub(r"\s+", " ", (request.args.get("query") or "")).strip()
        if not query:
            return jsonify(error="Enter a search term"), 400
        if len(query) > Config.MAX_QUERY_LEN:
            return jsonify(error="Search term is too long (max %d characters)" % Config.MAX_QUERY_LEN), 400
        try:
            page = max(1, int(request.args.get("page", 1)))
            size = min(Config.MAX_PAGE_SIZE, max(1, int(request.args.get("size", Config.PAGE_SIZE))))
        except ValueError:
            return jsonify(error="page and size must be numbers"), 400

        node = gateway.route("search")
        payload = search_service.search(query, page, size)
        payload["served_by"] = node
        return jsonify(payload)

    # 3B. View 
    # GET /v1/pages/{searchID}   (also accepted: /v1/pages?searchID={searchID})
    @app.route("/v1/pages", defaults={"search_id": None})
    @app.route("/v1/pages/<search_id>")
    def api_pages(search_id):
        search_id = search_id or request.args.get("searchID") or request.args.get("pages") or ""
        if not re.fullmatch(r"[a-f0-9]{12}", search_id):
            return jsonify(error="Invalid searchID"), 400
        node = gateway.route("view")
        detail = view_service.view(search_id)
        if detail is None:
            return jsonify(error="No page found for this searchID"), 404
        detail["served_by"] = node
        return jsonify(detail)

    return app


# LAUNCH
def launch():
    if "_SEEKLY_STOP" in globals():
        _SEEKLY_STOP.set()
    if "_SEEKLY_SERVER" in globals():
        try:
            _SEEKLY_SERVER.shutdown()
        except Exception:
            pass

    stop_event = threading.Event()
    blobs = BlobStore(Config.BLOB_DIR)
    docs = DocStore()
    cache = LRUCache(Config.CACHE_SIZE, Config.CACHE_TTL)
    index = SearchIndex(docs, Config.N_SHARDS)
    crawler = Crawler(docs, blobs, index, stop_event)
    gateway = Gateway()
    search_service = SearchService(index, docs, cache, crawler)
    view_service = ViewService(docs, blobs, cache)
    app = build_app(gateway, search_service, view_service, index, docs, crawler,
                    cache, blobs, time.time())

    crawler.start()
    for seed in SEED_URLS:
        crawler.submit(seed, depth=0, priority=5)

    server = make_server("0.0.0.0", Config.PORT, app, threaded=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    globals()["_SEEKLY_STOP"] = stop_event
    globals()["_SEEKLY_SERVER"] = server

   # print("Seekly is running on port %d" % Config.PORT)
 #   print("Crawler: %d workers, %d seed pages queued. Index shards: %d."
     #     % (Config.CRAWL_WORKERS, len(SEED_URLS), Config.N_SHARDS))
 #   print("First results appear after a few seconds while the seed pages are indexed.")

    try:
        from google.colab import output
        output.serve_kernel_port_as_iframe(Config.PORT, height=900)
    except ImportError:
        print("Open http://localhost:%d in your browser." % Config.PORT)


launch()