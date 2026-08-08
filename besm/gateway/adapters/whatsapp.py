# gateway/adapters/whatsapp.py
from __future__ import annotations

import asyncio

from twilio.rest import Client

from besm.config.secrets import require_env
from besm.gateway.adapters.base import ChannelAdapter
from besm.gateway.models import DeliveryResult
from besm.reasoning.state import AlertPayload


class WhatsAppAdapter(ChannelAdapter):
    def __init__(
        self,
        account_sid_env: str,
        auth_token_env: str,
        from_number_env: str,
        to_number_env: str,
    ) -> None:
        self._account_sid_env = account_sid_env
        self._auth_token_env = auth_token_env
        self._from_number_env = from_number_env
        self._to_number_env = to_number_env

    async def send(self, payload: AlertPayload, message: str) -> DeliveryResult:
        try:
            await asyncio.to_thread(self._send_sync, message)
            return DeliveryResult(success=True, channel="whatsapp")
        except Exception as exc:
            return DeliveryResult(success=False, channel="whatsapp", error=str(exc))

    def _send_sync(self, message: str) -> None:
        client = Client(
            require_env(self._account_sid_env),
            require_env(self._auth_token_env),
        )
        client.messages.create(
            body=message,
            from_=require_env(self._from_number_env),
            to=require_env(self._to_number_env),
        )
