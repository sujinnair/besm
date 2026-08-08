from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class AQINode(BaseIngestionNode):
    node_id = "health_aqi"
    domain = "health"
    poll_interval_minutes = 60
    _prompt_template = (
        "Search for the latest AQI (Air Quality Index) readings for {primary_city_state} "
        "from CPCB (Central Pollution Control Board) or the {primary_state} State "
        "Pollution Control Board. Report current AQI values by station, the dominant "
        "pollutant, the AQI category (Good/Satisfactory/Moderate/Poor/Very Poor/Severe), "
        "and any 24-hour forecast. Flag any station where AQI exceeds 150. Also report "
        "AQI for Chennai and Bengaluru as regional comparison.{travel_note} "
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
