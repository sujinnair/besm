from __future__ import annotations

import json
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.engine.models import ScoredSignal

_KEY = "fallback_queue"


class RedisFallbackQueue:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def push(self, signal: ScoredSignal) -> None:
        await self._redis.rpush(_KEY, _serialize(signal))

    async def pop(self) -> ScoredSignal | None:
        data = await self._redis.lpop(_KEY)
        if data is None:
            return None
        return _deserialize(data)

    async def length(self) -> int:
        return await self._redis.llen(_KEY)


def _serialize(signal: ScoredSignal) -> str:
    return json.dumps(
        {
            "signal_id": signal.signal_id,
            "domain": signal.domain,
            "raw_text": signal.raw_text,
            "embedding": signal.embedding,
            "t_signal": signal.t_signal.isoformat(),
            "t_impact_predicted": signal.t_impact_predicted.isoformat(),
            "lead_time_hours": signal.lead_time_hours,
            "confidence_score": signal.confidence_score,
            "metadata": signal.metadata,
            "suppressed": signal.suppressed,
            "suppression_reason": signal.suppression_reason,
        }
    )


def _deserialize(data: bytes | str) -> ScoredSignal:
    d = json.loads(data)
    return ScoredSignal(
        signal_id=d["signal_id"],
        domain=d["domain"],
        raw_text=d["raw_text"],
        embedding=d["embedding"],
        t_signal=_parse_dt(d["t_signal"]),
        t_impact_predicted=_parse_dt(d["t_impact_predicted"]),
        lead_time_hours=d["lead_time_hours"],
        confidence_score=d["confidence_score"],
        metadata=d["metadata"],
        suppressed=d["suppressed"],
        suppression_reason=d.get("suppression_reason"),
    )


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
