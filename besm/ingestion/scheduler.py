from __future__ import annotations

import random
from datetime import datetime, timedelta

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from besm.config.loader import LiveConfig
from besm.ingestion.base import BaseIngestionNode

log = structlog.get_logger()


class IngestionScheduler:
    def __init__(self, live_config: LiveConfig) -> None:
        self._scheduler = AsyncIOScheduler()
        self._live_config = live_config
        self._nodes: list[BaseIngestionNode] = []

    def register(self, node: BaseIngestionNode) -> None:
        interval = max(node.poll_interval_minutes, 15)
        jitter = random.randint(0, interval * 60 - 1)
        self._scheduler.add_job(
            self._run_node,
            trigger="interval",
            minutes=interval,
            id=node.node_id,
            max_instances=1,
            coalesce=True,
            args=[node],
            start_date=datetime.now() + timedelta(seconds=jitter),
        )
        self._nodes.append(node)
        log.info("node_registered", node_id=node.node_id, interval_minutes=interval)

    async def _run_node(self, node: BaseIngestionNode) -> None:
        await self._live_config.reload()
        node._llm = self._live_config.current.llm
        node._operator = self._live_config.current.operator
        await node.run()

    def start(self) -> None:
        self._scheduler.start()
        log.info("ingestion_scheduler_started")

    async def stop(self) -> None:
        self._scheduler.shutdown(wait=False)
        for node in self._nodes:
            await node.aclose()
        log.info("ingestion_scheduler_stopped")
