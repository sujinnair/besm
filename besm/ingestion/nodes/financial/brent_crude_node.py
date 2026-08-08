from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class BrentCrudeNode(BaseIngestionNode):
    node_id = "financial_brent_crude"
    domain = "financial"
    poll_interval_minutes = 240
    batch_eligible = True
    _prompt_template = (
        "Search for the current Brent Crude oil price and its movement over the last "
        "5 days. Report the current price in USD per barrel, the price 5 days ago, and "
        "the percentage change. Also report any recent Indian OMC (IOCL, BPCL, HPCL) "
        "fuel price revision announcements in the last 14 days and current retail petrol "
        "and diesel prices in {primary_city_state}.{travel_note} "
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
