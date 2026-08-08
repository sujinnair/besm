from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

from qdrant_client import QdrantClient
from qdrant_client.models import (
    DatetimeRange,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
)

from besm.engine.models import ButterflyChain, ScoredSignal

_SIGNALS = "signals"
_CHAINS = "butterfly_chains"


class VectorStore:
    def __init__(self, client: QdrantClient) -> None:
        self._client = client

    async def store_signal(self, signal: ScoredSignal) -> None:
        point = PointStruct(
            id=str(uuid.uuid4()),
            vector=signal.embedding,
            payload={
                "signal_id": signal.signal_id,
                "domain": signal.domain,
                "raw_text": signal.raw_text,
                "source_url": signal.metadata.get("source_url", ""),
                "t_signal": signal.t_signal.isoformat(),
                "t_impact_predicted": signal.t_impact_predicted.isoformat(),
                "t_impact_actual": None,
                "confidence_score": signal.confidence_score,
                "outcome_confirmed": False,
                "content_hash": signal.metadata.get("content_hash", ""),
            },
        )
        await asyncio.to_thread(
            self._client.upsert, collection_name=_SIGNALS, points=[point]
        )

    async def similarity_search(
        self, vector: list[float], top_k: int = 5
    ) -> list[ButterflyChain]:
        results = await asyncio.to_thread(
            self._client.query_points,
            collection_name=_CHAINS,
            query=vector,
            limit=top_k,
            with_payload=True,
        )
        chains = []
        for r in results.points:
            p = r.payload or {}
            chains.append(
                ButterflyChain(
                    chain_id=str(r.id),
                    signal_id=p.get("signal_id", ""),
                    domain=p.get("domain", "commodity"),
                    source=p.get("source", ""),
                    t_signal=_parse_dt(p.get("t_signal")),
                    t_impact_predicted=_parse_dt(p.get("t_impact_predicted")),
                    t_impact_actual=_parse_dt(p.get("t_impact_actual"))
                    if p.get("t_impact_actual")
                    else None,
                    confidence_score=p.get("confidence_score", 0.0),
                    embedding=[],
                    outcome_confirmed=p.get("outcome_confirmed", False),
                    similarity_score=r.score,
                )
            )
        return chains

    async def confirm_impact(self, signal_id: str, t_impact_actual: datetime) -> None:
        points, _ = await asyncio.to_thread(
            self._client.scroll,
            collection_name=_SIGNALS,
            scroll_filter=Filter(
                must=[
                    FieldCondition(key="signal_id", match=MatchValue(value=signal_id))
                ]
            ),
            limit=1,
            with_payload=True,
        )
        if not points:
            return
        point = points[0]
        payload = point.payload or {}
        payload["t_impact_actual"] = t_impact_actual.isoformat()
        payload["outcome_confirmed"] = True
        await asyncio.to_thread(
            self._client.set_payload,
            collection_name=_SIGNALS,
            payload=payload,
            points=[point.id],
        )

    async def store_chain(self, chain: ButterflyChain) -> None:
        point = PointStruct(
            id=str(uuid.uuid4()),
            vector=chain.embedding,
            payload={
                "signal_id": chain.signal_id,
                "domain": chain.domain,
                "source": chain.source,
                "t_signal": chain.t_signal.isoformat(),
                "t_impact_predicted": chain.t_impact_predicted.isoformat(),
                "t_impact_actual": chain.t_impact_actual.isoformat()
                if chain.t_impact_actual
                else None,
                "confidence_score": chain.confidence_score,
                "outcome_confirmed": chain.outcome_confirmed,
            },
        )
        await asyncio.to_thread(
            self._client.upsert, collection_name=_CHAINS, points=[point]
        )

    async def get_unconfirmed_past_signals(
        self, after: datetime, before: datetime
    ) -> list[dict]:
        records: list[dict] = []
        offset = None
        filt = Filter(
            must=[
                FieldCondition(key="outcome_confirmed", match=MatchValue(value=False)),
                FieldCondition(
                    key="t_impact_predicted",
                    range=DatetimeRange(
                        gte=after,
                        lte=before,
                    ),
                ),
            ]
        )
        while True:
            points, offset = await asyncio.to_thread(
                self._client.scroll,
                collection_name=_SIGNALS,
                scroll_filter=filt,
                limit=100,
                offset=offset,
                with_payload=True,
            )
            for p in points:
                records.append(p.payload or {})
            if offset is None:
                break
        return records


def _parse_dt(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
