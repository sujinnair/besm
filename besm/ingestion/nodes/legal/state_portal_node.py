from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class StatePortalNode(BaseIngestionNode):
    node_id = "legal_state_portal"
    domain = "legal"
    poll_interval_minutes = 720
    batch_eligible = True
    _prompt_template = (
        "Search for newly launched or updated citizen benefit schemes and subsidy "
        "programs on {primary_state} government portals. Focus on schemes relevant to "
        "technology workers and residents of {primary_city} district and {localities}. "
        "Report the scheme name, eligibility criteria, benefit amount or type, "
        "application deadline, and the direct portal URL to apply.{travel_note} "
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
