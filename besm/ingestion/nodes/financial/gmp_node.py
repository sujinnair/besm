from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class GMPNode(BaseIngestionNode):
    node_id = "financial_gmp"
    domain = "financial"
    poll_interval_minutes = 240
    batch_eligible = True
    fetch_prompt = (
        "Search for the latest Grey Market Premium (GMP) data for all active and "
        "upcoming IPOs on Indian stock exchanges (NSE, BSE). For each IPO report "
        "the company name, issue price, current GMP in rupees and as a percentage "
        "above issue price, implied listing gain percentage, and subscription status "
        "(open/upcoming/closed). Do not report QIB, HNI, or Retail subscription "
        "category breakdowns. Return all findings as plain text."
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
