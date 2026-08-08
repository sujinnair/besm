from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class MunicipalGazetteNode(BaseIngestionNode):
    node_id = "market_municipal_gazette"
    domain = "market"
    poll_interval_minutes = 1440
    _prompt_template = (
        "Search for recent council decisions, zoning change approvals (CLU), trade "
        "license grants, and commercial approvals published by municipalities and "
        "panchayats in {localities} and {primary_city} Corporation in the last 7 days. "
        "Report the ward or locality, type of approval, approved land use or business "
        "category, and any associated real estate or business opportunity.{travel_note} "
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
