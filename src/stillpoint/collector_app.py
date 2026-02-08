from __future__ import annotations

import asyncio
import hashlib
import json
import os
import queue
import random
import re
import threading
import time
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
from typing import Any, DefaultDict, Deque, Dict, List, Optional, Set, Tuple

from importlib.resources import files, as_file


from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ----------------------------
# Config
# ----------------------------
TOKEN = os.getenv("OBS_TOKEN", "dev-secret")
HOST = os.getenv("OBS_HOST", "127.0.0.1")
PORT = int(os.getenv("OBS_PORT", "7000"))

WINDOW_S = int(os.getenv("OBS_WINDOW_S", "120"))  # rolling window for metrics (seconds)
MAX_EVENTS = int(os.getenv("OBS_MAX_EVENTS", "30000"))

# Log tail mode:
# OBS_LOG_FILES can be:
#   serviceA=/path/to/access.log,serviceB=/path/to/other.log
# or just:
#   /path/to/access.log
OBS_LOG_FILES = os.getenv("OBS_LOG_FILES", "").strip()
TAIL_FROM_START = os.getenv("OBS_TAIL_FROM_START", "0") == "1"

# Sampling + slow threshold
SLOW_MS = int(os.getenv("OBS_SLOW_MS", "750"))
SAMPLE_RATE = float(os.getenv("OBS_SAMPLE_RATE", "0.05"))
TRACE_STORE_MAX = int(os.getenv("OBS_TRACE_STORE", "2000"))
TRACE_LOGS_MAX = int(os.getenv("OBS_TRACE_LOGS", "200"))
TRACE_SPANS_MAX = int(os.getenv("OBS_TRACE_SPANS", "200"))
RECENT_REQS_MAX = int(os.getenv("OBS_RECENT_REQS", "2000"))

# ----------------------------
# App
# ----------------------------
def mount_ui(app):
    ui_root = files("stillpoint").joinpath("ui")
    with as_file(ui_root) as ui_dir:
        app.mount("/static", StaticFiles(directory=str(ui_dir)), name="static")

app = FastAPI(title="Stillpoint")
mount_ui(app)

# ----------------------------
# Event schemas
# ----------------------------
class BaseEvent(BaseModel):
    ts: float = Field(default_factory=lambda: time.time())
    service: str = "unknown"
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    request_id: Optional[str] = None

class LogEvent(BaseEvent):
    kind: str = "log"
    level: str = "INFO"           # DEBUG/INFO/WARN/ERROR
    logger: str = "app"
    message: str
    trace: Optional[str] = None   # traceback / exception text
    meta: dict = Field(default_factory=dict)

class ReqEvent(BaseEvent):
    kind: str = "req"
    method: str
    path: str
    route: Optional[str] = None   # normalized template if known
    status: int
    duration_ms: Optional[float] = None
    meta: dict = Field(default_factory=dict)

class ReqStartEvent(BaseEvent):
    kind: str = "req_start"
    method: str
    path: str
    route: Optional[str] = None
    meta: dict = Field(default_factory=dict)

class ReqEndEvent(BaseEvent):
    kind: str = "req_end"
    method: str
    path: str
    route: Optional[str] = None
    status: int
    duration_ms: Optional[float] = None
    meta: dict = Field(default_factory=dict)

class SpanEvent(BaseEvent):
    kind: str = "span"
    name: str
    duration_ms: Optional[float] = None
    meta: dict = Field(default_factory=dict)

class GaugeEvent(BaseEvent):
    kind: str = "gauge"
    name: str                    # e.g. "inflight_delta"
    value: float

Event = LogEvent | ReqEvent | ReqStartEvent | ReqEndEvent | SpanEvent | GaugeEvent


# ----------------------------
# Storage
# ----------------------------
EVENTS: Deque[Dict[str, Any]] = deque(maxlen=MAX_EVENTS)
SUBS: Set[asyncio.Queue] = set()
TRACE_STORE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

# Rolling windows
# For req windows: store (ts, duration_ms or None, status)
REQ_WINDOW: Deque[Tuple[float, Optional[float], int]] = deque()
ENDPOINT_WINDOWS: DefaultDict[str, Deque[Tuple[float, Optional[float], int]]] = defaultdict(deque)
RECENT_REQS: Deque[Dict[str, Any]] = deque()

# Error signature tracking
ERROR_SIG_WINDOW: Deque[Tuple[float, str]] = deque()
ERROR_SIG_COUNT: DefaultDict[str, int] = defaultdict(int)

# In-flight counters (req_start/req_end or gauge inflight_delta)
INFLIGHT_BY_SERVICE: DefaultDict[str, int] = defaultdict(int)
INFLIGHT_GLOBAL: int = 0
INFLIGHT_REQUESTS: Dict[str, Tuple[str, float]] = {}

# Trends (sparklines) sampled every second from global metrics
TREND_MAX = 120
TREND_RPS: Deque[float] = deque(maxlen=TREND_MAX)
TREND_ERR5XX: Deque[float] = deque(maxlen=TREND_MAX)
TREND_P95: Deque[float] = deque(maxlen=TREND_MAX)


# Static


def read_index_html() -> str:
    ui_root = files("stillpoint").joinpath("ui")
    return ui_root.joinpath("index.html").read_text(encoding="utf-8")

# ----------------------------
# Helpers
# ----------------------------
def _bucket_status(status: int) -> str:
    if 200 <= status < 300: return "2xx"
    if 300 <= status < 400: return "3xx"
    if 400 <= status < 500: return "4xx"
    return "5xx"

def _endpoint_key(ev: Dict[str, Any]) -> str:
    method = ev.get("method") or "GET"
    route = ev.get("route") or ev.get("path") or "unknown"
    return f"{method} {route}"

def _prune_req_deque(dq: Deque[Tuple[float, Optional[float], int]], now: float):
    while dq and (now - dq[0][0] > WINDOW_S):
        dq.popleft()

def _percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if p <= 0:
        return sorted_vals[0]
    if p >= 100:
        return sorted_vals[-1]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)

def _tail_rates(durs: List[float]) -> Dict[str, float]:
    if not durs:
        return {"gt250": 0.0, "gt500": 0.0, "gt1000": 0.0, "gt2000": 0.0}
    n = len(durs)
    return {
        "gt250": sum(1 for x in durs if x > 250.0) / n,
        "gt500": sum(1 for x in durs if x > 500.0) / n,
        "gt1000": sum(1 for x in durs if x > 1000.0) / n,
        "gt2000": sum(1 for x in durs if x > 2000.0) / n,
    }

def _signature_from_trace(trace: str) -> str:
    lines = [ln.strip() for ln in trace.splitlines() if ln.strip()]
    tail = lines[-1] if lines else "Exception"
    frames = [ln for ln in lines if ln.startswith("File ")]
    head = "\n".join(frames[:3])
    base = f"{tail}\n{head}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:10]

def _spark(vals: List[float]) -> str:
    blocks = "▁▂▃▄▅▆▇█"
    if not vals:
        return ""
    mn, mx = min(vals), max(vals)
    if mx - mn < 1e-9:
        return blocks[0] * len(vals)
    out = []
    for v in vals:
        idx = int((v - mn) / (mx - mn) * (len(blocks) - 1))
        out.append(blocks[idx])
    return "".join(out)


def _should_sample_req(data: Dict[str, Any]) -> bool:
    try:
        status = int(data.get("status", 0) or 0)
    except Exception:
        status = 0

    dur_ms = data.get("duration_ms")
    if status >= 500:
        return True
    if dur_ms is not None and dur_ms >= SLOW_MS:
        return True
    if SAMPLE_RATE <= 0:
        return False
    return random.random() < SAMPLE_RATE


def _get_trace_bundle(request_id: Optional[str], create: bool = True) -> Optional[Dict[str, Any]]:
    if not request_id:
        return None
    bundle = TRACE_STORE.get(request_id)
    if bundle is None and create:
        bundle = {
            "request_id": request_id,
            "req_start": None,
            "req_end": None,
            "headers": {},
            "logs": deque(maxlen=TRACE_LOGS_MAX),
            "spans": deque(maxlen=TRACE_SPANS_MAX),
        }
        TRACE_STORE[request_id] = bundle
    if bundle is not None:
        TRACE_STORE.move_to_end(request_id)
        while len(TRACE_STORE) > TRACE_STORE_MAX:
            TRACE_STORE.popitem(last=False)
    return bundle


def _prune_recent(now: float) -> None:
    while RECENT_REQS and (now - RECENT_REQS[0].get("ts", now) > WINDOW_S):
        RECENT_REQS.popleft()


def _record_recent_request(data: Dict[str, Any], now: float) -> None:
    ts = float(data.get("ts", now))
    if data.get("status") is None:
        return
    summary = {
        "ts": ts,
        "request_id": data.get("request_id"),
        "trace_id": data.get("trace_id"),
        "span_id": data.get("span_id"),
        "service": data.get("service"),
        "method": data.get("method"),
        "path": data.get("path"),
        "route": data.get("route"),
        "endpoint": _endpoint_key(data),
        "status": int(data.get("status", 0)),
        "duration_ms": data.get("duration_ms"),
    }
    RECENT_REQS.append(summary)
    if len(RECENT_REQS) > RECENT_REQS_MAX:
        RECENT_REQS.popleft()


# ----------------------------
# Metrics
# ----------------------------
def compute_global_metrics() -> Dict[str, Any]:
    now = time.time()
    _prune_req_deque(REQ_WINDOW, now)
    n = len(REQ_WINDOW)

    status_b = {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0}
    durs = []
    err5xx = 0

    for _, dur, st in REQ_WINDOW:
        b = _bucket_status(st)
        status_b[b] += 1
        if b == "5xx":
            err5xx += 1
        if dur is not None:
            durs.append(dur)

    durs_sorted = sorted(durs)
    return {
        "window_s": WINDOW_S,
        "count": n,
        "rps": (n / WINDOW_S) if WINDOW_S > 0 else 0.0,
        "status": status_b,
        "error_rate_5xx": (err5xx / n) if n else 0.0,
        "p50_ms": _percentile(durs_sorted, 50),
        "p95_ms": _percentile(durs_sorted, 95),
        "p99_ms": _percentile(durs_sorted, 99),
        "inflight": INFLIGHT_GLOBAL,
        "latency_available": bool(durs_sorted),
    }

def compute_endpoint_metrics(limit: int = 15, sort_by: str = "p95") -> List[Dict[str, Any]]:
    now = time.time()
    rows: List[Dict[str, Any]] = []

    for key, w in list(ENDPOINT_WINDOWS.items()):
        _prune_req_deque(w, now)
        if not w:
            continue

        n = len(w)
        status_b = {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0}
        err5xx = 0

        durs = []
        for _, dur, st in w:
            b = _bucket_status(st)
            status_b[b] += 1
            if b == "5xx":
                err5xx += 1
            if dur is not None:
                durs.append(dur)

        durs_sorted = sorted(durs)
        tails = _tail_rates(durs)
        status_rates = {k: (status_b[k] / n) if n else 0.0 for k in status_b}

        rows.append({
            "endpoint": key,
            "count": n,
            "rps": (n / WINDOW_S) if WINDOW_S > 0 else 0.0,
            "status": status_b,
            "status_rates": status_rates,
            "error_rate_5xx": (err5xx / n) if n else 0.0,
            "p50_ms": _percentile(durs_sorted, 50),
            "p95_ms": _percentile(durs_sorted, 95),
            "p99_ms": _percentile(durs_sorted, 99),
            "tails": tails,
            "latency_available": bool(durs_sorted),
        })

    def sort_key(r):
        if sort_by == "error": return r["error_rate_5xx"]
        if sort_by == "rps": return r["rps"]
        if sort_by == "count": return r["count"]
        # default p95 (missing p95 sorts to bottom)
        return (r["p95_ms"] or 0.0)

    rows.sort(key=sort_key, reverse=True)
    return rows[:limit]

def prune_error_sigs():
    now = time.time()
    while ERROR_SIG_WINDOW and (now - ERROR_SIG_WINDOW[0][0] > WINDOW_S):
        _, sig = ERROR_SIG_WINDOW.popleft()
        ERROR_SIG_COUNT[sig] -= 1
        if ERROR_SIG_COUNT[sig] <= 0:
            del ERROR_SIG_COUNT[sig]

def compute_top_error_signatures(limit: int = 10) -> List[Dict[str, Any]]:
    prune_error_sigs()
    items = sorted(ERROR_SIG_COUNT.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    return [{"sig": k, "count": v} for k, v in items]


# ----------------------------
# Trend sampler
# ----------------------------
async def _trend_sampler():
    while True:
        m = compute_global_metrics()
        TREND_RPS.append(float(m["rps"]))
        TREND_ERR5XX.append(float(m["error_rate_5xx"]))
        TREND_P95.append(float(m["p95_ms"] or 0.0))
        await asyncio.sleep(1)

@app.on_event("startup")
async def _startup():
    asyncio.create_task(_trend_sampler())

    # Start log tailers (file mode)
    for service, path in parse_log_files(OBS_LOG_FILES):
        asyncio.create_task(tail_access_log(service=service, path=path, from_start=TAIL_FROM_START))


# ----------------------------
# Routes: UI + API
# ----------------------------
@app.get("/", response_class=HTMLResponse)
def home():
    return read_index_html()


@app.get("/metrics")
def metrics():
    m = compute_global_metrics()
    m["trends"] = {
        "rps": _spark(list(TREND_RPS)),
        "err5xx": _spark(list(TREND_ERR5XX)),
        "p95": _spark(list(TREND_P95)),
    }
    return m

@app.get("/metrics/endpoints")
def metrics_endpoints(limit: int = 15, sort_by: str = "p95"):
    return {"window_s": WINDOW_S, "endpoints": compute_endpoint_metrics(limit=limit, sort_by=sort_by)}

@app.get("/metrics/errorsigs")
def metrics_errorsigs(limit: int = 10):
    return {"window_s": WINDOW_S, "signatures": compute_top_error_signatures(limit=limit)}

@app.get("/metrics/traces")
def metrics_traces(limit: int = 200, slow_limit: int = 10):
    now = time.time()
    _prune_recent(now)
    recent = list(reversed(RECENT_REQS))[:limit]
    slow = sorted(
        (r for r in RECENT_REQS if r.get("duration_ms") is not None),
        key=lambda r: r.get("duration_ms") or 0.0,
        reverse=True,
    )[:slow_limit]
    return {"window_s": WINDOW_S, "recent": recent, "top_slow": slow}

@app.get("/trace/{request_id}")
def trace_detail(request_id: str):
    bundle = TRACE_STORE.get(request_id)
    if not bundle:
        raise HTTPException(404, "trace not found")

    req_start = bundle.get("req_start") or {}
    req_end = bundle.get("req_end") or {}

    method = req_end.get("method") or req_start.get("method")
    path = req_end.get("path") or req_start.get("path")
    route = req_end.get("route") or req_start.get("route")

    summary = {
        "request_id": request_id,
        "trace_id": req_end.get("trace_id") or req_start.get("trace_id"),
        "span_id": req_end.get("span_id") or req_start.get("span_id"),
        "service": req_end.get("service") or req_start.get("service"),
        "method": method,
        "path": path,
        "route": route,
        "endpoint": f"{method or 'GET'} {route or path or 'unknown'}",
        "status": req_end.get("status"),
        "duration_ms": req_end.get("duration_ms"),
        "start_ts": req_start.get("ts"),
        "end_ts": req_end.get("ts"),
    }

    meta = {}
    meta.update(req_start.get("meta") or {})
    meta.update(req_end.get("meta") or {})

    logs = sorted(list(bundle.get("logs", [])), key=lambda l: l.get("ts", 0))
    spans = sorted(list(bundle.get("spans", [])), key=lambda s: s.get("ts", 0))

    return {
        "request": summary,
        "headers": bundle.get("headers", {}),
        "meta": meta,
        "logs": logs,
        "spans": spans,
    }

@app.post("/ingest")
async def ingest(ev: Event, authorization: str | None = Header(default=None)):
    global INFLIGHT_GLOBAL

    if authorization != f"Bearer {TOKEN}":
        raise HTTPException(401, "unauthorized")

    data = ev.model_dump()
    await _ingest_event_dict(data)
    return {"ok": True}

@app.get("/stream")
async def stream(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=5000)
    SUBS.add(q)

    async def gen():
        try:
            while True:
                if await request.is_disconnected():
                    break
                item = await q.get()
                yield f"data: {json.dumps(item)}\n\n"
        finally:
            SUBS.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ----------------------------
# Internal ingest
# ----------------------------
async def _ingest_event_dict(data: Dict[str, Any]):
    global INFLIGHT_GLOBAL
    now = time.time()

    data.setdefault("trace_id", None)
    data.setdefault("span_id", None)
    data.setdefault("request_id", None)

    kind = data.get("kind")
    should_publish = True
    bundle = _get_trace_bundle(data.get("request_id"), create=kind in ("req_start", "req_end", "req", "log", "span"))

    if kind == "req_start":
        svc = data.get("service", "unknown")
        INFLIGHT_BY_SERVICE[svc] += 1
        INFLIGHT_GLOBAL += 1
        rid = data.get("request_id")
        if rid:
            INFLIGHT_REQUESTS[rid] = (svc, now)
        if bundle is not None:
            bundle["req_start"] = data
            headers = (data.get("meta") or {}).get("headers")
            if headers:
                bundle["headers"] = headers
        should_publish = False

    elif kind in ("req", "req_end"):
        REQ_WINDOW.append((data.get("ts", now), data.get("duration_ms"), int(data.get("status", 0))))
        _prune_req_deque(REQ_WINDOW, now)

        epk = _endpoint_key(data)
        w = ENDPOINT_WINDOWS[epk]
        w.append((data.get("ts", now), data.get("duration_ms"), int(data.get("status", 0))))
        _prune_req_deque(w, now)

        if bundle is not None:
            bundle["req_end"] = data
            headers = (data.get("meta") or {}).get("headers")
            if headers:
                bundle["headers"] = headers
        _record_recent_request(data, now)
        _prune_recent(now)

        if kind == "req_end":
            rid = data.get("request_id")
            if rid and rid in INFLIGHT_REQUESTS:
                svc, _ = INFLIGHT_REQUESTS.pop(rid)
                INFLIGHT_BY_SERVICE[svc] -= 1
                INFLIGHT_GLOBAL -= 1
                if INFLIGHT_BY_SERVICE[svc] < 0:
                    INFLIGHT_BY_SERVICE[svc] = 0
                if INFLIGHT_GLOBAL < 0:
                    INFLIGHT_GLOBAL = 0

        should_publish = _should_sample_req(data)

    elif kind == "log":
        trace = data.get("trace")
        if trace:
            sig = _signature_from_trace(trace)
            ERROR_SIG_WINDOW.append((data.get("ts", now), sig))
            ERROR_SIG_COUNT[sig] += 1
            prune_error_sigs()
        if bundle is not None:
            bundle["logs"].append({
                "ts": data.get("ts", now),
                "level": data.get("level"),
                "logger": data.get("logger"),
                "message": data.get("message"),
                "trace": data.get("trace"),
                "meta": data.get("meta"),
            })

    elif kind == "gauge":
        # Used for true inflight from injection mode
        name = data.get("name")
        val = float(data.get("value", 0))
        svc = data.get("service", "unknown")

        if name == "inflight_delta":
            INFLIGHT_BY_SERVICE[svc] += int(val)
            INFLIGHT_GLOBAL += int(val)
            if INFLIGHT_BY_SERVICE[svc] < 0:
                INFLIGHT_BY_SERVICE[svc] = 0
            if INFLIGHT_GLOBAL < 0:
                INFLIGHT_GLOBAL = 0
    elif kind == "span":
        if bundle is not None:
            bundle["spans"].append(data)

    if should_publish:
        EVENTS.append(data)

    # Push to SSE subscribers
    dead = []
    for q in list(SUBS):
        try:
            if should_publish:
                q.put_nowait(data)
        except Exception:
            dead.append(q)
    for q in dead:
        SUBS.discard(q)


# ----------------------------
# Access log file mode
# ----------------------------
_UVICORN_ACCESS_RE = re.compile(
    r"""
    ^.*?\s
    (?P<ip>\d+\.\d+\.\d+\.\d+)(?::\d+)?\s-\s"
    (?P<method>[A-Z]+)\s
    (?P<path>\S+)\s
    HTTP\/[^"]+"\s
    (?P<status>\d{3})
    """,
    re.VERBOSE
)

def parse_log_files(spec: str) -> List[Tuple[str, str]]:
    """
    Supports:
      "" -> []
      "/path/a.log" -> [("accesslog", "/path/a.log")]
      "svcA=/path/a.log,svcB=/path/b.log" -> [...]
    """
    out: List[Tuple[str, str]] = []
    if not spec:
        return out

    parts = [p.strip() for p in spec.split(",") if p.strip()]
    for p in parts:
        if "=" in p:
            svc, path = p.split("=", 1)
            out.append((svc.strip(), path.strip()))
        else:
            out.append(("accesslog", p))
    return out

def parse_access_line(line: str, service: str) -> Optional[Dict[str, Any]]:
    """
    Supports:
    1) JSON lines (recommended for latency):
       {"kind":"req","service":"svc","method":"GET","path":"/x","status":200,"duration_ms":12.3}
    2) Default uvicorn access log lines (no latency):
       INFO:     127.0.0.1:12345 - "GET /x HTTP/1.1" 200 OK
    """
    line = line.strip()
    if not line:
        return None

    # JSON line?
    if line.startswith("{") and line.endswith("}"):
        try:
            obj = json.loads(line)
            if obj.get("kind") in ("req", "req_end", "req_start"):
                obj.setdefault("service", service)
                obj.setdefault("ts", time.time())
                # normalize fields
                obj.setdefault("route", None)
                if obj.get("status") is not None:
                    obj["status"] = int(obj.get("status", 0))
                if obj.get("duration_ms") is not None:
                    obj["duration_ms"] = float(obj["duration_ms"])
                return obj
        except Exception:
            return None

    # Uvicorn style
    m = _UVICORN_ACCESS_RE.match(line)
    if not m:
        return None

    method = m.group("method")
    path = m.group("path")
    status = int(m.group("status"))
    return {
        "kind": "req",
        "service": service,
        "ts": time.time(),
        "method": method,
        "path": path,
        "route": None,
        "status": status,
        "duration_ms": None,   # not available from default uvicorn access log
        "meta": {"source": "uvicorn_access_log"},
    }

@dataclass
class TailHandle:
    thread: threading.Thread
    q: "queue.Queue[str]"

def start_tail_thread(path: str, from_start: bool) -> TailHandle:
    q: "queue.Queue[str]" = queue.Queue(maxsize=10000)

    def worker():
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                if not from_start:
                    f.seek(0, os.SEEK_END)
                while True:
                    line = f.readline()
                    if not line:
                        time.sleep(0.2)
                        continue
                    try:
                        q.put_nowait(line)
                    except queue.Full:
                        # drop lines under pressure
                        pass
        except Exception:
            # If the file can't be opened, keep retrying.
            while True:
                time.sleep(1.0)
                try:
                    with open(path, "r", encoding="utf-8", errors="replace") as f:
                        if not from_start:
                            f.seek(0, os.SEEK_END)
                        while True:
                            line = f.readline()
                            if not line:
                                time.sleep(0.2)
                                continue
                            try:
                                q.put_nowait(line)
                            except queue.Full:
                                pass
                except Exception:
                    continue

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return TailHandle(thread=t, q=q)

async def tail_access_log(service: str, path: str, from_start: bool = False):
    handle = start_tail_thread(path, from_start=from_start)
    while True:
        line = await asyncio.to_thread(handle.q.get)
        ev = parse_access_line(line, service=service)
        if ev:
            await _ingest_event_dict(ev)


# ----------------------------
# Entry hint (optional)
# ----------------------------
# Run with:
#   uvicorn collector_app:app --host 127.0.0.1 --port 7000
