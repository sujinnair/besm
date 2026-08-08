from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class GoldSentimentNode(BaseIngestionNode):
    node_id = "financial_gold_sentiment"
    domain = "financial"
    poll_interval_minutes = 240
    batch_eligible = True
    fetch_prompt = (
        "Search for the current global gold market sentiment. Report the current "
        "COMEX gold futures price, recent gold ETF flow data (inflows or outflows), "
        "INR-denominated gold spot price per 10 grams on MCX, and the overall market "
        "sentiment (bullish, bearish, or neutral). Report any analyst price targets or "
        "central bank gold buying activity. Do not report Hindu calendar dates, wedding "
        "muhurat, or festive season schedules. Return all findings as plain text."
    )

    def __init__(self, redis: Redis, llm: LLMBackendConfig) -> None:
        super().__init__(redis, llm)

    async def parse(self, raw: RawDataRecord) -> list[RawSignalEvent]:
        t_signal = datetime.now(timezone.utc)
        content_hash = make_content_hash(
            raw.source_url,
            raw.metadata["publication_date"],
            raw.raw_content,
        )
        return [
            RawSignalEvent(
                event_id=str(uuid.uuid4()),
                node_id=self.node_id,
                domain=self.domain,
                content_hash=content_hash,
                source_url=raw.source_url,
                raw_text=raw.raw_content,
                metadata=raw.metadata,
                t_signal=t_signal,
            )
        ]
