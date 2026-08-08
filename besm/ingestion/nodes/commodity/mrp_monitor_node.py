from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class MRPMonitorNode(BaseIngestionNode):
    node_id = "commodity_mrp_monitor"
    domain = "commodity"
    poll_interval_minutes = 1440
    fetch_prompt = (
        "Search for recent announcements of MRP (Maximum Retail Price) increases "
        "on packaged consumer goods in India. Look for FMCG company announcements, "
        "trade news, and manufacturer press releases from the last 48 hours. "
        "Report the product name, brand, old MRP, new MRP, effective date, and "
        "the reason given for the price hike. Return all findings as plain text."
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
