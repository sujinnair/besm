from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class SlotSentinelNode(BaseIngestionNode):
    node_id = "legal_slot_sentinel"
    domain = "legal"
    poll_interval_minutes = 30
    fetch_prompt = (
        "Search for current appointment slot availability on Indian government portals: "
        "Passport Seva (passportindia.gov.in), RTO (Parivahan portal), and VFS Global "
        "for visa appointments. Report available appointment dates, times, service types, "
        "and locations. Flag any newly released slots or cancellations. Include direct "
        "booking URLs. Return all findings as plain text."
    )

    def __init__(self, redis: Redis, llm: LLMBackendConfig) -> None:
        super().__init__(redis, llm)

    async def parse(self, raw: RawDataRecord) -> list[RawSignalEvent]:
        t_signal = datetime.now(timezone.utc)
        return [
            RawSignalEvent(
                event_id=str(uuid.uuid4()),
                node_id=self.node_id,
                domain=self.domain,
                content_hash=make_content_hash(
                    raw.source_url, raw.metadata["publication_date"], raw.raw_content
                ),
                source_url=raw.source_url,
                raw_text=raw.raw_content,
                metadata=raw.metadata,
                t_signal=t_signal,
            )
        ]
