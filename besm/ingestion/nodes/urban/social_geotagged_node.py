from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class SocialGeotaggedNode(BaseIngestionNode):
    node_id = "urban_social_geotagged"
    domain = "urban"
    poll_interval_minutes = 60
    _prompt_template = (
        "Search for recent social media posts, news reports, and crowd-sourced data "
        "about crowd density, congestion, and unusual gatherings in {primary_city}, "
        "focusing on {localities} and major markets and transit hubs. Look for reports "
        "of overcrowding, flash events, hartals, or public disruptions. Report the "
        "specific location, estimated crowd level, time of report, and cause if "
        "mentioned.{travel_note} Return all findings as plain text."
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
