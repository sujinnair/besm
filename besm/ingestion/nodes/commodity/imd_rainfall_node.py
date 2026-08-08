from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class IMDRainfallNode(BaseIngestionNode):
    node_id = "commodity_imd_rainfall"
    domain = "commodity"
    poll_interval_minutes = 1440
    _prompt_template = (
        "Search for the latest IMD (India Meteorological Department) cumulative rainfall "
        "data versus long-period averages for key agricultural districts across India, "
        "focusing on states that supply vegetables and food grains to {primary_state}. "
        "Also cover major pulse-producing districts nationally (UP, MP, Maharashtra, "
        "Rajasthan). Report any district where cumulative seasonal rainfall has fallen "
        "below 75% of the long-period average. Include district name, state, crop "
        "association, and deficit percentage. Do not report short-range weather "
        "forecasts. Return all findings as plain text."
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
