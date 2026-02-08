from __future__ import annotations

import json
import time
import uuid
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from stillpoint.trace_context import new_span_id, new_trace_id


def _header_subset(request: Request) -> dict:
    keys = ("user-agent", "content-type", "accept", "host", "x-request-id", "x-correlation-id")
    out = {}
    for key in keys:
        val = request.headers.get(key)
        if val:
            out[key] = val
    return out


class TimedAccessLogMiddleware(BaseHTTPMiddleware):
    """
    Writes one JSON line per request.
    Collector can tail this file (OBS_LOG_FILES=svc=/path/to/file.log).
    """
    def __init__(self, app, *, service: str, path: str):
        super().__init__(app)
        self.service = service
        self.path = path

    async def dispatch(self, request: Request, call_next):
        start = time.time()
        request_id = uuid.uuid4().hex
        trace_id = new_trace_id()
        span_id = new_span_id()
        headers = _header_subset(request)
        status = 500
        route_obj = request.scope.get("route")
        route = getattr(route_obj, "path", None) if route_obj else None

        start_record = {
            "kind": "req_start",
            "service": self.service,
            "ts": time.time(),
            "trace_id": trace_id,
            "span_id": span_id,
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "route": route,
            "meta": {
                "query": str(request.url.query),
                "client": request.client.host if request.client else None,
                "ua": request.headers.get("user-agent"),
                "headers": headers,
            },
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(start_record) + "\n")

        try:
            resp = await call_next(request)
            status = resp.status_code
            resp.headers["x-stillpoint-trace-id"] = trace_id
            resp.headers["x-stillpoint-request-id"] = request_id
            return resp
        except Exception:
            status = 500
            raise
        finally:
            dur_ms = (time.time() - start) * 1000.0
            record = {
                "kind": "req_end",
                "service": self.service,
                "ts": time.time(),
                "trace_id": trace_id,
                "span_id": span_id,
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "route": route,
                "status": int(status),
                "duration_ms": round(dur_ms, 3),
                "meta": {
                    "query": str(request.url.query),
                    "client": request.client.host if request.client else None,
                    "ua": request.headers.get("user-agent"),
                    "headers": headers,
                },
            }
            # append line
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
