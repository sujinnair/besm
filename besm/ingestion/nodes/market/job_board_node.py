from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig
from besm.engine.models import RawSignalEvent
from besm.ingestion.base import BaseIngestionNode, RawDataRecord, make_content_hash


class JobBoardNode(BaseIngestionNode):
    node_id = "market_job_board"
    domain = "market"
    poll_interval_minutes = 1440
    fetch_prompt = (
        "Search for current technology job market trends in India on Naukri, LinkedIn, "
        "and Indeed India. Report the top in-demand tech skills and average advertised "
        "salaries for Python, Rust, LLM engineering, data engineering, cloud architecture, "
        "and agentic AI roles. Highlight any skill where advertised salaries have "
        "increased more than 15% over the last 90 days. Include the top 3 hiring "
        "companies per skill and note any remote-friendly roles. "
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
