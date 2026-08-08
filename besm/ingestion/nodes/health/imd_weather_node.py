from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class IMDWeatherNode(BaseIngestionNode):
    node_id = "health_imd_weather"
    domain = "health"
    poll_interval_minutes = 360
    _prompt_template = (
        "Search for the latest IMD (India Meteorological Department) short-range weather "
        "forecast for {primary_city_state} for the next 5 days. Report temperature "
        "ranges, wind speed, wind direction, humidity, and any temperature inversion or "
        "stagnant wind conditions that could trap air pollution. Report any active IMD "
        "orange or red alerts for {primary_state}, including heatwave, cold wave, or "
        "cyclone warnings that could affect the region. Do not report cumulative rainfall "
        "deficits, long-period averages, or agricultural rainfall data.{travel_note} "
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
