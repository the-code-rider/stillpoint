from __future__ import annotations

import contextvars
import secrets
import uuid
from typing import Dict, Optional

TRACE_ID = contextvars.ContextVar("trace_id", default=None)
SPAN_ID = contextvars.ContextVar("span_id", default=None)
REQUEST_ID = contextvars.ContextVar("request_id", default=None)
SERVICE = contextvars.ContextVar("service", default=None)


def new_trace_id() -> str:
    return uuid.uuid4().hex


def new_span_id() -> str:
    return secrets.token_hex(8)


def set_context(*, trace_id: str, span_id: str, request_id: str, service: str) -> Dict[str, contextvars.Token]:
    return {
        "trace_id": TRACE_ID.set(trace_id),
        "span_id": SPAN_ID.set(span_id),
        "request_id": REQUEST_ID.set(request_id),
        "service": SERVICE.set(service),
    }


def reset_context(tokens: Dict[str, contextvars.Token]) -> None:
    TRACE_ID.reset(tokens["trace_id"])
    SPAN_ID.reset(tokens["span_id"])
    REQUEST_ID.reset(tokens["request_id"])
    SERVICE.reset(tokens["service"])


def get_context() -> Dict[str, str]:
    out: Dict[str, str] = {}
    trace_id = TRACE_ID.get()
    span_id = SPAN_ID.get()
    request_id = REQUEST_ID.get()
    service = SERVICE.get()

    if trace_id:
        out["trace_id"] = trace_id
    if span_id:
        out["span_id"] = span_id
    if request_id:
        out["request_id"] = request_id
    if service:
        out["service"] = service
    return out
