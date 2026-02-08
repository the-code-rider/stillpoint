from __future__ import annotations

import logging
import os
import queue
import threading
import time
import traceback
import uuid
from collections import deque
from typing import Deque, Dict, Optional

import requests
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from stillpoint.trace_context import get_context, new_span_id, new_trace_id, reset_context, set_context

REQ_LOG_LINES = int(os.getenv("OBS_REQ_LOG_LINES", "50"))
SLOW_MS = int(os.getenv("OBS_SLOW_MS", "750"))

_REQ_LOGS: Dict[str, Deque[dict]] = {}
_REQ_LOGS_LOCK = threading.Lock()


def _append_request_log(request_id: Optional[str], payload: dict) -> None:
    if not request_id or REQ_LOG_LINES <= 0:
        return
    with _REQ_LOGS_LOCK:
        buf = _REQ_LOGS.get(request_id)
        if buf is None:
            buf = deque(maxlen=REQ_LOG_LINES)
            _REQ_LOGS[request_id] = buf
        buf.append(payload)


def _consume_request_logs(request_id: Optional[str]) -> list:
    if not request_id:
        return []
    with _REQ_LOGS_LOCK:
        buf = _REQ_LOGS.pop(request_id, None)
    return list(buf) if buf else []


def _header_subset(request: Request) -> dict:
    keys = ("user-agent", "content-type", "accept", "host", "x-request-id", "x-correlation-id")
    out = {}
    for key in keys:
        val = request.headers.get(key)
        if val:
            out[key] = val
    return out


class CollectorClient:
    def __init__(self, url: str, token: str, service: str, max_q: int = 8000, timeout: float = 1.2):
        self.url = url
        self.token = token
        self.service = service
        self.timeout = timeout
        self.q: "queue.Queue[dict]" = queue.Queue(maxsize=max_q)
        threading.Thread(target=self._worker, daemon=True).start()

    def emit(self, payload: dict):
        ctx = get_context()
        payload.setdefault("trace_id", ctx.get("trace_id"))
        payload.setdefault("span_id", ctx.get("span_id"))
        payload.setdefault("request_id", ctx.get("request_id"))
        payload.setdefault("ts", time.time())
        payload.setdefault("service", ctx.get("service") or self.service)
        try:
            self.q.put_nowait(payload)
        except queue.Full:
            # drop under pressure
            pass

    def _worker(self):
        headers = {"Authorization": f"Bearer {self.token}"}
        while True:
            item = self.q.get()
            try:
                requests.post(self.url, json=item, headers=headers, timeout=self.timeout)
            except Exception:
                pass
            finally:
                self.q.task_done()


class RequestObservabilityMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, client: CollectorClient, *, emit_request_span: bool = False):
        super().__init__(app)
        self.client = client
        self.emit_request_span = emit_request_span

    async def dispatch(self, request: Request, call_next):
        start = time.time()
        rid = uuid.uuid4().hex
        trace_id = new_trace_id()
        span_id = new_span_id()
        request.state.request_id = rid
        request.state.trace_id = trace_id
        request.state.span_id = span_id

        tokens = set_context(
            trace_id=trace_id,
            span_id=span_id,
            request_id=rid,
            service=self.client.service,
        )

        route_obj = request.scope.get("route")
        route = getattr(route_obj, "path", None) if route_obj else None
        headers = _header_subset(request)

        self.client.emit({
            "kind": "req_start",
            "request_id": rid,
            "method": request.method,
            "path": request.url.path,
            "route": route,
            "meta": {
                "query": str(request.url.query),
                "client": request.client.host if request.client else None,
                "ua": request.headers.get("user-agent"),
                "headers": headers,
            }
        })

        resp = None
        status = 500
        try:
            resp = await call_next(request)
            status = resp.status_code
            resp.headers["x-stillpoint-trace-id"] = trace_id
            resp.headers["x-stillpoint-request-id"] = rid
            return resp
        finally:
            dur_ms = (time.time() - start) * 1000.0

            route_obj = request.scope.get("route")
            route = getattr(route_obj, "path", None) if route_obj else None

            meta = {
                "query": str(request.url.query),
                "client": request.client.host if request.client else None,
                "ua": request.headers.get("user-agent"),
                "headers": headers,
            }

            is_error = int(status) >= 500
            is_slow = dur_ms >= SLOW_MS
            if is_error or is_slow:
                meta["logs"] = _consume_request_logs(rid)
            else:
                _consume_request_logs(rid)

            self.client.emit({
                "kind": "req_end",
                "request_id": rid,
                "method": request.method,
                "path": request.url.path,
                "route": route,
                "status": int(status),
                "duration_ms": round(dur_ms, 3),
                "meta": meta,
            })
            if self.emit_request_span:
                self.client.emit({
                    "kind": "span",
                    "name": "app.request",
                    "duration_ms": round(dur_ms, 3),
                    "meta": {
                        "type": "app",
                        "path": request.url.path,
                        "route": route,
                        "status": int(status),
                    },
                })
            reset_context(tokens)


class CollectorLogHandler(logging.Handler):
    def __init__(self, client: CollectorClient, level=logging.INFO):
        super().__init__(level=level)
        self.client = client

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            trace = None
            if record.exc_info:
                trace = "".join(traceback.format_exception(*record.exc_info))

            level = record.levelname.upper()
            if level == "WARNING":
                level = "WARN"

            payload = {
                "kind": "log",
                "level": level,
                "logger": record.name,
                "message": msg,
                "trace": trace,
                "meta": {
                    "file": record.pathname,
                    "line": record.lineno,
                    "func": record.funcName,
                }
            }
            self.client.emit(payload)
            _append_request_log(get_context().get("request_id"), {
                "ts": time.time(),
                "level": level,
                "logger": record.name,
                "message": msg,
                "trace": trace,
            })
        except Exception:
            pass


def install_observability(
    app,
    *,
    collector_url: str,
    token: str,
    service_name: str,
    log_level: int = logging.INFO,
    emit_request_span: bool = False,
) -> CollectorClient:
    client = CollectorClient(url=collector_url, token=token, service=service_name)
    app.add_middleware(RequestObservabilityMiddleware, client=client, emit_request_span=emit_request_span)

    handler = CollectorLogHandler(client, level=log_level)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().addHandler(handler)

    return client
