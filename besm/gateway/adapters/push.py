from __future__ import annotations

import structlog

from besm.config.secrets import require_env
from besm.gateway.adapters.base import ChannelAdapter
from besm.gateway.models import DeliveryResult
from besm.reasoning.state import AlertPayload
from besm.utils.tls import tls_client

log = structlog.get_logger()


class PushAdapter(ChannelAdapter):
    def __init__(self, ntfy_topic_env: str, ntfy_server: str) -> None:
        self._ntfy_topic_env = ntfy_topic_env
        self._ntfy_server = ntfy_server.rstrip("/")
        self._http = tls_client(timeout=15.0)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def send(self, payload: AlertPayload, message: str) -> DeliveryResult:
        topic = require_env(self._ntfy_topic_env)
        try:
            resp = await self._http.post(
                f"{self._ntfy_server}/{topic}",
                content=message.encode(),
                headers={
                    "Title": f"[{payload['domain'].upper()}] Signal Alert",
                    "Priority": "high",
                },
            )
            resp.raise_for_status()
            return DeliveryResult(success=True, channel="push")
        except Exception as exc:
            log.error(
                "push_delivery_failed",
                error=str(exc),
                topic=topic,
                server=self._ntfy_server,
            )
            return DeliveryResult(success=False, channel="push", error=str(exc))
