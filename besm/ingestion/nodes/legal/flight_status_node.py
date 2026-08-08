from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class FlightStatusNode(BaseIngestionNode):
    node_id = "legal_flight_status"
    domain = "legal"
    poll_interval_minutes = 60
    _prompt_template = (
        "Search for the latest flight delay and cancellation data for {primary_city} "
        "International Airport (IATA: {airport_iata}). Report all flights delayed by "
        "more than 2 hours or cancelled, including airline name, flight number, origin "
        "or destination, delay duration, and stated reason. Also report applicable DGCA "
        "passenger rights including meal entitlements, compensation, and refund "
        "eligibility.{travel_note} Return all findings as plain text."
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
