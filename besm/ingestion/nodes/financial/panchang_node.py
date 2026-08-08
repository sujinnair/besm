from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class PanchangNode(BaseIngestionNode):
    node_id = "financial_panchang"
    domain = "financial"
    poll_interval_minutes = 1440
    batch_eligible = True
    fetch_prompt = (
        "Search for Hindu Panchang auspicious wedding dates (Shubh Vivah Muhurat) "
        "for the next 12 months. List all major auspicious marriage dates, the "
        "corresponding Hindu calendar month, and any extended periods of 30 or more "
        "consecutive days with no auspicious wedding dates (known as wedding dead zones). "
        "Also include major Indian festive seasons (Dhanteras, Akshaya Tritiya) that "
        "drive gold jewelry purchases. Return all findings as plain text."
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
