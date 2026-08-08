from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class PowerOutageNode(BaseIngestionNode):
    node_id = "legal_power_outage"
    domain = "legal"
    poll_interval_minutes = 60
    _prompt_template = (
        "Search for recent power outage reports and scheduled maintenance notices from "
        "the {primary_state} state electricity distribution company (DISCOM) for "
        "{primary_city} district, particularly {localities}. Report affected zones, "
        "outage start and end times, duration, cause (planned maintenance vs. fault), "
        "and whether the duration exceeds the state electricity regulatory commission "
        "SLA. Include any consumer compensation entitlements for prolonged or repeated "
        "outages.{travel_note} Return all findings as plain text."
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
