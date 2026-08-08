from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class BusinessDirectoryNode(BaseIngestionNode):
    node_id = "market_business_directory"
    domain = "market"
    poll_interval_minutes = 1440
    _prompt_template = (
        "Search for business density and service gap data in {localities} and the "
        "broader {primary_city} district. Identify localities or wards within 2 km of "
        "active construction sites, industrial zones, or newly announced infrastructure "
        "projects that lack essential services such as food outlets, logistics providers, "
        "retail stores, clinics, or co-working spaces. Report the locality name, missing "
        "service category, estimated residential population to be served, and the nearest "
        "existing competing provider.{travel_note} Return all findings as plain text."
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
