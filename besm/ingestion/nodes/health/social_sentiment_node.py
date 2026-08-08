from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class SocialSentimentNode(BaseIngestionNode):
    node_id = "health_social_sentiment"
    domain = "health"
    poll_interval_minutes = 720
    _prompt_template = (
        "Search for social media posts, news, and search trend data related to illness "
        "symptoms in {primary_state}, with focus on {primary_city} district and "
        "{localities}. Track fever, cough, viral fever, flu, dengue, chikungunya, and "
        "malaria. Report volume of symptom mentions, trending localities, any district "
        "health department alerts, and any indication of a rising outbreak. Include data "
        "from the last 7 days versus the prior 30-day baseline.{travel_note} "
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
