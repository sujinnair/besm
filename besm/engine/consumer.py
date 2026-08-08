from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import structlog
from redis.asyncio import Redis

from besm.config.schema import BESMConfig
from besm.engine.confidence import calculate_confidence
from besm.engine.embedding import embed
from besm.engine.lead_time import calculate_lead_time
from besm.engine.models import ButterflyChain, RawSignalEvent, ScoredSignal
from besm.ingestion.deduplication import DeduplicationFilter
from besm.vector_store.store import VectorStore

log = structlog.get_logger()

_STREAM = "raw_signals"
_GROUP = "butterfly_engine"
_CONSUMER = "engine_0"
_BLOCK_MS = 5000
_PIPELINE_TIMEOUT_S = 60


class ButterflyEngineConsumer:
    def __init__(
        self,
        redis: Redis,
        vector_store: VectorStore,
        config: BESMConfig,
        on_scored: asyncio.Queue[ScoredSignal],
    ) -> None:
        self._redis = redis
        self._vs = vector_store
        self._cfg = config
        self._out = on_scored
        self._dedup = DeduplicationFilter(redis)

    async def _is_duplicate(self, content_hash: str) -> bool:
        return await self._dedup.is_duplicate(content_hash)

    async def _mark_seen(self, content_hash: str) -> None:
        await self._dedup.mark_seen(content_hash)

    async def start(self) -> None:
        await self._ensure_group()
        while True:
            entries = await self._redis.xreadgroup(
                groupname=_GROUP,
                consumername=_CONSUMER,
                streams={_STREAM: ">"},
                count=10,
                block=_BLOCK_MS,
            )
            if not entries:
                continue
            for _, messages in entries:
                for msg_id, data in messages:
                    asyncio.create_task(self._handle(msg_id, data))

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(_STREAM, _GROUP, id="0", mkstream=True)
        except Exception:
            pass  # group already exists

    async def _handle(self, msg_id: bytes, data: dict) -> None:
        try:
            async with asyncio.timeout(_PIPELINE_TIMEOUT_S):
                event = _deserialize(data)
                scored = await self._process(event)
        except TimeoutError:
            log.error("pipeline_timeout", msg_id=msg_id)
            return
        except Exception as exc:
            log.error("pipeline_error", msg_id=msg_id, error=str(exc))
            return
        finally:
            await self._redis.xack(_STREAM, _GROUP, msg_id)

        await self._out.put(scored)

    async def _process(self, event: RawSignalEvent) -> ScoredSignal:
        if await self._is_duplicate(event.content_hash):
            return _suppressed(event, "duplicate")

        model_name = self._cfg.vector_store.embedding_model
        vector = await asyncio.to_thread(embed, event.raw_text, model_name)

        chains = await self._vs.similarity_search(vector, top_k=5)

        domain_lag_hours = _domain_lag(event.domain)
        t_impact = event.t_signal + timedelta(hours=domain_lag_hours)
        lead_time = calculate_lead_time(event.t_signal, t_impact)

        if lead_time <= 0:
            log.info(
                "signal_discarded_nonpositive_lead_time",
                signal_id=event.event_id,
                t_signal=event.t_signal.isoformat(),
            )
            return _suppressed(
                event, "non_positive_lead_time", vector, t_impact, lead_time
            )

        confidence = calculate_confidence(
            top_chains=chains,
            independent_source_count=event.metadata.get("source_count", 1),
            t_signal=event.t_signal,
            domain_bonus=bool(event.metadata.get("domain_bonus", False)),
        )

        scored = ScoredSignal(
            signal_id=str(uuid.uuid4()),
            domain=event.domain,
            raw_text=event.raw_text,
            embedding=vector,
            t_signal=event.t_signal,
            t_impact_predicted=t_impact,
            lead_time_hours=lead_time,
            confidence_score=confidence,
            top_similar_chains=chains,
            metadata=event.metadata,
        )

        if confidence < 40:
            scored.suppressed = True
            scored.suppression_reason = "low_confidence"
            await self._vs.store_signal(scored)

        await self._mark_seen(event.content_hash)

        if not scored.suppressed:
            await self._vs.store_signal(scored)
            chain = ButterflyChain(
                chain_id=str(uuid.uuid4()),
                signal_id=scored.signal_id,
                domain=scored.domain,
                source=scored.metadata.get("source_url", "agent_web_search"),
                t_signal=scored.t_signal,
                t_impact_predicted=scored.t_impact_predicted,
                t_impact_actual=None,
                confidence_score=scored.confidence_score,
                embedding=scored.embedding,
                outcome_confirmed=False,
            )
            await self._vs.store_chain(chain)

        return scored


def _domain_lag(domain: str) -> float:
    # Default lag estimates in hours; refined per domain by historical chains
    return {
        "commodity": 24 * 7,  # 7 days midpoint of 7–10
        "financial": 24 * 22,  # 22 days midpoint of 14–30
        "legal": 0.5,
        "health": 24 * 5,
        "urban": 0.5,
        "market": 24 * 14,
    }.get(domain, 24 * 7)


def _suppressed(
    event: RawSignalEvent,
    reason: str,
    vector: list[float] | None = None,
    t_impact: datetime | None = None,
    lead_time: float = 0.0,
) -> ScoredSignal:
    now = datetime.now(timezone.utc)
    return ScoredSignal(
        signal_id=event.event_id,
        domain=event.domain,
        raw_text=event.raw_text,
        embedding=vector or [],
        t_signal=event.t_signal,
        t_impact_predicted=t_impact or now,
        lead_time_hours=lead_time,
        confidence_score=0.0,
        metadata=event.metadata,
        suppressed=True,
        suppression_reason=reason,
    )


def _deserialize(data: dict) -> RawSignalEvent:
    return RawSignalEvent(
        event_id=data[b"event_id"].decode(),
        node_id=data[b"node_id"].decode(),
        domain=data[b"domain"].decode(),
        content_hash=data[b"content_hash"].decode(),
        source_url=data[b"source_url"].decode(),
        raw_text=data[b"raw_text"].decode(),
        metadata=json.loads(data[b"metadata"]),
        t_signal=datetime.fromisoformat(data[b"t_signal"].decode()),
    )
