from __future__ import annotations

from besm.config.secrets import require_env
from besm.gateway.adapters.base import ChannelAdapter
from besm.gateway.models import DeliveryResult
from besm.reasoning.state import AlertPayload
from besm.utils.tls import tls_client


class TelegramAdapter(ChannelAdapter):
    def __init__(self, bot_token_env: str, chat_id_env: str) -> None:
        self._bot_token_env = bot_token_env
        self._chat_id_env = chat_id_env
        self._http = tls_client(timeout=15.0)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def send(self, payload: AlertPayload, message: str) -> DeliveryResult:
        token = require_env(self._bot_token_env)
        chat_id = require_env(self._chat_id_env)
        try:
            resp = await self._http.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message},
            )
            resp.raise_for_status()
            return DeliveryResult(success=True, channel="telegram")
        except Exception as exc:
            return DeliveryResult(success=False, channel="telegram", error=str(exc))
