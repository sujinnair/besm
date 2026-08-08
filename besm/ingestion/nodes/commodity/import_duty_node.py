from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class ImportDutyNode(BaseIngestionNode):
    node_id = "commodity_import_duty"
    domain = "commodity"
    poll_interval_minutes = 1440
    fetch_prompt = (
        "Search for the latest Indian government notifications on import duty changes "
        "for edible oils and packaged goods. Check the Ministry of Commerce, DGFT, "
        "and Gazette of India for any new or amended customs duty rates published in "
        "the last 24 hours. Report the commodity name, old duty rate, new duty rate, "
        "effective date, and notification number. Return all findings as plain text."
    )

    def __init__(self, redis: Redis, llm: LLMBackendConfig) -> None:
        super().__init__(redis, llm)

    async def parse(self, raw: RawDataRecord) -> list[RawSignalEvent]:
        t_signal = datetime.now(timezone.utc)
        content_hash = make_content_hash(
            raw.source_url,
            raw.metadata["publication_date"],
            raw.raw_content,
        )
        return [
            RawSignalEvent(
                event_id=str(uuid.uuid4()),
                node_id=self.node_id,
                domain=self.domain,
                content_hash=content_hash,
                source_url=raw.source_url,
                raw_text=raw.raw_content,
                metadata=raw.metadata,
                t_signal=t_signal,
            )
        ]
