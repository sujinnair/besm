from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class IPOSubscriptionNode(BaseIngestionNode):
    node_id = "financial_ipo_subscription"
    domain = "financial"
    poll_interval_minutes = 120
    fetch_prompt = (
        "Search for live IPO subscription data on NSE and BSE for all currently "
        "open IPO subscription windows. For each active IPO report the company name, "
        "issue price range, subscription window dates, and current subscription rates "
        "broken down by QIB, HNI, and Retail investor categories. Report the overall "
        "subscription multiple and highlight any IPO with QIB subscription above 5x. "
        "Return all findings as plain text."
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
