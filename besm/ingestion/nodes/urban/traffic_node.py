from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class TrafficNode(BaseIngestionNode):
    node_id = "urban_traffic"
    domain = "urban"
    poll_interval_minutes = 30
    _prompt_template = (
        "Search for real-time and recent traffic congestion reports for {primary_city}, "
        "focusing on routes through {localities} and major city corridors. Look for "
        "accidents, waterlogging, road closures, VIP convoy restrictions, and hartals "
        "causing disruption. Report the affected road or junction, congestion severity, "
        "estimated delay, cause, and recommended alternate routes.{travel_note} "
        "Return all findings as plain text."
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
