from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue
from redis.asyncio import Redis

NODE_REGISTRY: dict[str, dict] = {
    "commodity_mandi":          {"domain": "commodity",  "interval_min": 60,   "mode": "real-time"},
    "commodity_imd_rainfall":   {"domain": "commodity",  "interval_min": 360,  "mode": "real-time"},
    "commodity_import_duty":    {"domain": "commodity",  "interval_min": 1440, "mode": "batch"},
    "commodity_mrp_monitor":    {"domain": "commodity",  "interval_min": 240,  "mode": "real-time"},
    "financial_forex":          {"domain": "financial",  "interval_min": 240,  "mode": "batch"},
    "financial_brent_crude":    {"domain": "financial",  "interval_min": 240,  "mode": "batch"},
    "financial_gmp":            {"domain": "financial",  "interval_min": 240,  "mode": "batch"},
    "financial_ipo_subscription":{"domain": "financial", "interval_min": 120,  "mode": "real-time"},
    "financial_gold_sentiment":  {"domain": "financial", "interval_min": 240,  "mode": "batch"},
    "financial_panchang":        {"domain": "financial", "interval_min": 1440, "mode": "batch"},
    "legal_gazette":             {"domain": "legal",     "interval_min": 1440, "mode": "batch"},
    "legal_state_portal":        {"domain": "legal",     "interval_min": 120,  "mode": "batch"},
    "legal_slot_sentinel":       {"domain": "legal",     "interval_min": 30,   "mode": "real-time"},
    "legal_power_outage":        {"domain": "legal",     "interval_min": 60,   "mode": "real-time"},
    "legal_flight_status":       {"domain": "legal",     "interval_min": 60,   "mode": "real-time"},
    "health_aqi":                {"domain": "health",    "interval_min": 60,   "mode": "real-time"},
    "health_imd_weather":        {"domain": "health",    "interval_min": 120,  "mode": "real-time"},
    "health_social_sentiment":   {"domain": "health",    "interval_min": 120,  "mode": "real-time"},
    "urban_social_geotagged":    {"domain": "urban",     "interval_min": 60,   "mode": "real-time"},
    "urban_traffic":             {"domain": "urban",     "interval_min": 60,   "mode": "real-time"},
    "market_job_board":          {"domain": "market",    "interval_min": 240,  "mode": "real-time"},
    "market_municipal_gazette":  {"domain": "market",    "interval_min": 1440, "mode": "batch"},
    "market_infrastructure":     {"domain": "market",    "interval_min": 240,  "mode": "real-time"},
    "market_business_directory": {"domain": "market",    "interval_min": 240,  "mode": "real-time"},
}


async def get_node_statuses(redis: Redis, nodes_config) -> list[dict]:
    results = []
    for node_id, meta in NODE_REGISTRY.items():
        enabled = getattr(nodes_config, node_id, False)
        raw = await redis.hgetall(f"node_state:{node_id}")
        state: dict = {k.decode(): v.decode() for k, v in raw.items()} if raw else {}

        if not enabled:
            status = "disabled"
        elif not state:
            status = "never_run"
        else:
            status = state.get("status", "unknown")

        results.append(
            {
                "node_id": node_id,
                "enabled": enabled,
                "status": status,
                "last_poll": _fmt_ts(state.get("last_poll")),
                "last_success": _fmt_ts(state.get("last_success")),
                "last_error": state.get("last_error", ""),
                "consecutive_failure_s": float(state.get("consecutive_failure_s", 0)),
                "signals_total": int(state.get("signals_total", 0)),
                "batch_id": state.get("batch_id", ""),
                **meta,
            }
        )
    return results


async def get_recent_stream_events(redis: Redis, count: int = 40) -> list[dict]:
    raw = await redis.xrevrange("raw_signals", count=count)
    events = []
    for _msg_id, fields in raw:
        d = {k.decode(): v.decode() for k, v in fields.items()}
        try:
            d["metadata"] = json.loads(d.get("metadata", "{}"))
        except json.JSONDecodeError:
            d["metadata"] = {}
        d["raw_text_snippet"] = d.get("raw_text", "")[:180]
        events.append(d)
    return events


async def get_signals_from_qdrant(
    qdrant: QdrantClient,
    limit: int = 60,
    domain: str | None = None,
    suppressed_only: bool = False,
    confirmed_only: bool = False,
) -> list[dict]:
    must = []
    if domain:
        must.append(FieldCondition(key="domain", match=MatchValue(value=domain)))
    if confirmed_only:
        must.append(FieldCondition(key="outcome_confirmed", match=MatchValue(value=True)))

    filt = Filter(must=must) if must else None
    try:
        points, _ = await asyncio.to_thread(
            qdrant.scroll,
            collection_name="signals",
            scroll_filter=filt,
            limit=limit,
            with_payload=True,
        )
    except Exception:
        return []

    records = []
    for p in points:
        payload = dict(p.payload or {})
        payload["lead_time_h"] = _lead_time_h(
            payload.get("t_signal"), payload.get("t_impact_predicted")
        )
        records.append(payload)

    records.sort(key=lambda r: r.get("t_signal", ""), reverse=True)
    return records


def get_alerts_from_metrics(metrics_file: str, limit: int = 100) -> list[dict]:
    path = Path(metrics_file)
    if not path.exists():
        return []
    alerts = []
    with path.open() as f:
        for line in f:
            try:
                entry = json.loads(line)
                if entry.get("event") == "alert_delivered":
                    alerts.append(entry)
            except json.JSONDecodeError:
                pass
    return list(reversed(alerts[-limit:]))


def get_metrics_summary(metrics_file: str) -> dict:
    path = Path(metrics_file)
    if not path.exists():
        return {"signals_today": 0, "alerts_today": 0, "delivery_rate": None}

    today = datetime.now(timezone.utc).date().isoformat()
    signals_today = alerts_today = delivered = failed = 0

    with path.open() as f:
        for line in f:
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not e.get("ts", "").startswith(today):
                continue
            if e.get("event") == "signal_processed":
                signals_today += 1
            elif e.get("event") == "alert_delivered":
                alerts_today += 1
                if e.get("success"):
                    delivered += 1
                else:
                    failed += 1

    total = delivered + failed
    return {
        "signals_today": signals_today,
        "alerts_today": alerts_today,
        "delivery_rate": round(delivered / total * 100) if total else None,
    }


def _fmt_ts(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        s = int(delta.total_seconds())
        if s < 60:
            return f"{s}s ago"
        if s < 3600:
            return f"{s // 60}m ago"
        if s < 86400:
            return f"{s // 3600}h ago"
        return f"{s // 86400}d ago"
    except ValueError:
        return iso


def _lead_time_h(t_signal: str | None, t_impact: str | None) -> float | None:
    if not t_signal or not t_impact:
        return None
    try:
        a = datetime.fromisoformat(t_signal)
        b = datetime.fromisoformat(t_impact)
        return round((b - a).total_seconds() / 3600, 1)
    except ValueError:
        return None
