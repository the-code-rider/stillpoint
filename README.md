# stillpoint

Lightweight local observability: a FastAPI collector with a live dashboard for
requests, logs, spans, and quick correlation by trace/request IDs.

## Install (PyPI)
```bash
python -m pip install -U stillpoint
```

## Quick start

### 1) Run the collector
```bash
stillpoint
```
Open `http://127.0.0.1:7000/`.

### 2) Instrument a FastAPI app
```python
from stillpoint.agent import install_observability

client = install_observability(
    app_instance,
    collector_url="http://127.0.0.1:7000/ingest",
    token="dev-secret",
    service_name="my-api:8000",
    emit_request_span=True,  # optional single span per request
)
```

### 3) Tail access logs instead (no code changes)
```bash
set OBS_LOG_FILES=serviceA=/path/to/access.log
set OBS_TAIL_FROM_START=1
stillpoint
```

## Usage scenarios

### Single service + live UI
Run the collector, instrument your API, then browse the dashboard to:
- see live request volume, error rates, and p95 latency
- view recent requests and open trace details
- export a trace bundle as JSON

### Multiple services
Point multiple services at the same collector URL with different `service_name`
values. The dashboard will tag and filter by service.

### Access-log only mode
When code changes are not possible, tail access logs via `OBS_LOG_FILES`.
Note: spans and per-request logs are not available unless emitted by the app.

## Configuration

Collector environment variables:
- `OBS_TOKEN`: bearer token for `/ingest` (default `dev-secret`)
- `OBS_HOST`, `OBS_PORT`: bind host/port (defaults `127.0.0.1:7000`)
- `OBS_RELOAD`: set to `1` for reload (dev only)
- `OBS_LOG_LEVEL`: uvicorn log level (default `info`)
- `OBS_WINDOW_S`: metrics window seconds (default `120`)
- `OBS_MAX_EVENTS`: SSE buffer size (default `30000`)
- `OBS_LOG_FILES`: tail access logs (comma-separated `svc=/path.log`)
- `OBS_TAIL_FROM_START`: set `1` to read logs from start
- `OBS_SLOW_MS`: slow request threshold (default `750`)
- `OBS_SAMPLE_RATE`: sample rate for normal traffic (default `0.05`)
- `OBS_TRACE_STORE`: max traces stored (default `2000`)
- `OBS_TRACE_LOGS`: max logs per trace (default `200`)
- `OBS_TRACE_SPANS`: max spans per trace (default `200`)
- `OBS_RECENT_REQS`: max recent requests kept (default `2000`)

Agent config (per app):
- `collector_url`, `token`, `service_name` in `install_observability`
- `emit_request_span=True` to get a default request span

## Data points collected

Events emitted and stored:
- `req_start` / `req_end`: method, path/route, status, duration, client info,
  `trace_id`, `span_id`, `request_id`, plus headers subset
- `log`: level, logger, message, optional trace
- `span`: name, duration, optional metadata (`type`, `db`, `http`, `cache`)
- `gauge`: used for inflight counts when emitted by the app

Derived metrics:
- RPS, error rates, status buckets (2xx/3xx/4xx/5xx)
- per-endpoint p50/p95/p99 and tail latency thresholds
- recent requests list and slowest individual traces

## Local development

Run from source (no install):
```bash
python -m uvicorn stillpoint.collector_app:app --app-dir src --host 127.0.0.1 --port 7000 --reload
```

Tests:
```bash
python -m pytest
```

Code structure:
- `src/stillpoint/collector_app.py`: FastAPI collector + metrics + API
- `src/stillpoint/agent.py`: app-side middleware + log handler
- `src/stillpoint/ui/`: dashboard assets
