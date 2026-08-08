from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class MandiNode(BaseIngestionNode):
    node_id = "commodity_mandi"
    domain = "commodity"
    poll_interval_minutes = 1440
    _prompt_template = (
        "Search for today's wholesale Mandi arrival volumes and prices for onion, "
        "tomato, and pulses (arhar, moong, urad) from markets that supply {primary_state} "
        "— particularly the wholesale markets in {primary_city} and the major APMC "
        "markets in neighbouring states that feed {primary_state}. Include data from "
        "Agmarknet or any reliable Indian agricultural market data source. Report arrival "
        "volumes, modal price, price range, and any significant supply drop or price "
        "spike compared to the same period last year.{travel_note} "
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
