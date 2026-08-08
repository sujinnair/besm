from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from redis.asyncio import Redis

from besm.config.loader import LiveConfig
from besm.config.secrets import load_secrets
from besm.ui.data import (
    get_alerts_from_metrics,
    get_metrics_summary,
    get_node_statuses,
    get_recent_stream_events,
)

_TEMPLATES = Jinja2Templates(directory=Path(__file__).parent / "templates")
_DOMAINS = ["commodity", "financial", "legal", "health", "urban", "market"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_secrets()
    live_config = LiveConfig()
    app.state.live_config = live_config
    app.state.config = live_config.current

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis = Redis.from_url(redis_url)
    app.state.redis = redis

    yield

    await redis.aclose()


app = FastAPI(lifespan=lifespan, title="BESM Dashboard")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return _TEMPLATES.TemplateResponse(request, "dashboard.html")


@app.get("/api/summary")
async def api_summary(request: Request):
    config = request.app.state.config
    return get_metrics_summary(config.logging.metrics_file)


@app.get("/api/nodes")
async def api_nodes(request: Request):
    redis = request.app.state.redis
    config = request.app.state.config
    try:
        return await get_node_statuses(redis, config.nodes)
    except Exception:
        return []


@app.get("/api/signals")
async def api_signals(request: Request):
    config = request.app.state.config
    path = Path(config.logging.metrics_file)
    if not path.exists():
        return []
    entries = []
    with path.open() as f:
        for line in f:
            try:
                e = json.loads(line)
                if e.get("event") == "signal_processed":
                    entries.append(e)
            except json.JSONDecodeError:
                pass
    return list(reversed(entries[-60:]))


@app.get("/api/alerts")
async def api_alerts(request: Request):
    config = request.app.state.config
    return get_alerts_from_metrics(config.logging.metrics_file, limit=30)


@app.get("/api/health")
async def api_health(request: Request):
    redis = request.app.state.redis
    config = request.app.state.config

    try:
        await redis.ping()
    except Exception:
        return {"status": "down", "issues": ["Redis unreachable"]}

    try:
        alive = await redis.exists("besm:heartbeat")
    except Exception:
        alive = 0

    if not alive:
        return {"status": "down", "issues": ["main.py not running"]}

    # Backend is up — check for node-level errors (degraded)
    issues = []
    try:
        from besm.ui.data import NODE_REGISTRY
        for node_id in NODE_REGISTRY:
            if not getattr(config.nodes, node_id, False):
                continue
            raw_status = await redis.hget(f"node_state:{node_id}", "status")
            if raw_status and raw_status.decode() in ("error", "failing"):
                raw_err = await redis.hget(f"node_state:{node_id}", "last_error")
                err = raw_err.decode()[:80] if raw_err else "unknown error"
                issues.append(f"{node_id}: {err}")
    except Exception:
        pass

    status = "degraded" if issues else "up"
    return {"status": status, "issues": issues}


@app.get("/api/config")
async def api_config(request: Request):
    operator = request.app.state.config.operator
    primary = operator.primary_location
    return {
        "locations": [
            {
                "label": loc.label,
                "city": loc.city,
                "state": loc.state,
                "primary": loc.primary,
            }
            for loc in operator.locations
        ],
        "primary_city": primary.city,
        "primary_state": primary.state,
    }


@app.get("/api/stream")
async def api_stream(request: Request):
    redis = request.app.state.redis
    try:
        return await get_recent_stream_events(redis, count=20)
    except Exception:
        return []


@app.get("/api/logs/stream")
async def api_logs_stream(request: Request):
    log_path = Path(request.app.state.config.logging.log_file)

    async def event_gen():
        # Backfill last 80 lines for immediate context on connect
        if log_path.exists():
            lines = log_path.read_text().splitlines()
            for line in lines[-80:]:
                if line.strip():
                    yield f"data: {line.strip()}\n\n"

        pos = log_path.stat().st_size if log_path.exists() else 0
        tick = 0
        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(0.5)
            tick += 1
            if tick % 30 == 0:
                yield ": keepalive\n\n"  # prevent proxy timeout
            if log_path.exists():
                size = log_path.stat().st_size
                if size > pos:
                    with log_path.open() as f:
                        f.seek(pos)
                        chunk = f.read(size - pos)
                    pos = size
                    for line in chunk.splitlines():
                        if line.strip():
                            yield f"data: {line.strip()}\n\n"

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/trends")
async def api_trends(request: Request):
    config = request.app.state.config
    path = Path(config.logging.metrics_file)
    if not path.exists():
        return _empty_trends()

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    hourly: dict[str, dict[str, int]] = {}
    domain_totals: dict[str, int] = defaultdict(int)
    domain_conf: dict[str, list[float]] = defaultdict(list)
    latencies: list[float] = []

    with path.open() as f:
        for line in f:
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if e.get("event") != "signal_processed":
                continue
            try:
                ts = datetime.fromisoformat(e["ts"])
            except (KeyError, ValueError):
                continue

            domain = e.get("domain", "unknown")
            conf = float(e.get("confidence", 0))
            latency = float(e.get("latency_s", 0))

            domain_totals[domain] += 1
            domain_conf[domain].append(conf)
            latencies.append(latency)

            if ts >= cutoff:
                hour_key = ts.strftime("%Y-%m-%dT%H:00")
                if hour_key not in hourly:
                    hourly[hour_key] = {d: 0 for d in _DOMAINS}
                hourly[hour_key][domain] = hourly[hour_key].get(domain, 0) + 1

    hours_list = []
    for i in range(24):
        h = (now - timedelta(hours=23 - i)).strftime("%Y-%m-%dT%H:00")
        row: dict = {"hour": h}
        for d in _DOMAINS:
            row[d] = hourly.get(h, {}).get(d, 0)
        hours_list.append(row)

    total_conf = sum(sum(v) for v in domain_conf.values())
    total_count = sum(len(v) for v in domain_conf.values())

    return {
        "hourly": hours_list,
        "domain_totals": dict(domain_totals),
        "domain_confidence": {
            d: round(sum(v) / len(v), 1) for d, v in domain_conf.items() if v
        },
        "overall": {
            "total_signals": int(sum(domain_totals.values())),
            "avg_confidence": round(total_conf / max(1, total_count), 1),
            "avg_latency_s": round(sum(latencies) / max(1, len(latencies)), 1),
        },
    }


@app.get("/api/node-latest/{node_id}")
async def api_node_latest(request: Request, node_id: str):
    redis = request.app.state.redis
    try:
        raw = await redis.xrevrange("raw_signals", count=500)
        for _msg_id, fields in raw:
            d = {k.decode(): v.decode() for k, v in fields.items()}
            if d.get("node_id") == node_id:
                return {
                    "node_id": node_id,
                    "raw_text": d.get("raw_text", ""),
                    "t_signal": d.get("t_signal", ""),
                    "source_url": d.get("source_url", ""),
                }
    except Exception:
        pass
    return {"node_id": node_id, "raw_text": "", "t_signal": "", "source_url": ""}


def _empty_trends() -> dict:
    now = datetime.now(timezone.utc)
    hours_list = []
    for i in range(24):
        h = (now - timedelta(hours=23 - i)).strftime("%Y-%m-%dT%H:00")
        row: dict = {"hour": h}
        for d in _DOMAINS:
            row[d] = 0
        hours_list.append(row)
    return {
        "hourly": hours_list,
        "domain_totals": {},
        "domain_confidence": {},
        "overall": {"total_signals": 0, "avg_confidence": 0, "avg_latency_s": 0},
    }
