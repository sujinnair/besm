from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class InfrastructureNode(BaseIngestionNode):
    node_id = "market_infrastructure"
    domain = "market"
    poll_interval_minutes = 1440
    _prompt_template = (
        "Search for recent infrastructure project announcements and construction "
        "milestones in {primary_state}, with priority on {primary_city} district. "
        "Cover projects from state industrial development agencies, port authorities, "
        "smart city missions, and national highway upgrades affecting {primary_state}. "
        "Also include any NITI Aayog or central government infrastructure investments "
        "announced for {primary_state}. Report the project name, location, current "
        "status, estimated completion, investment size, and which nearby localities are "
        "likely to benefit.{travel_note} Return all findings as plain text."
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
