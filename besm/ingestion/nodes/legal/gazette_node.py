from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class GazetteNode(BaseIngestionNode):
    node_id = "legal_gazette"
    domain = "legal"
    poll_interval_minutes = 1440
    batch_eligible = True
    _prompt_template = (
        "Search for new official notifications formally published in the Gazette of "
        "India (egazette.gov.in) and the {primary_state} State Gazette in the last "
        "24 hours. Focus on gazette notifications for income tax rebates, GST rate "
        "changes, central subsidy scheme launches, interest subvention programs, and "
        "{primary_state} government welfare schemes. For each notification report the "
        "gazette notification number, scheme name, eligibility criteria, benefit amount "
        "or type, effective date, and issuing ministry or department. Report only "
        "formally gazetted notifications, not press releases or portal announcements."
        "{travel_note} Return all findings as plain text."
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
