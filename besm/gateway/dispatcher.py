from __future__ import annotations

import asyncio
from datetime import datetime
from enum import Enum
from zoneinfo import ZoneInfo

import structlog

from besm.config.schema import BESMConfig
from besm.gateway.adapters.base import ChannelAdapter
from besm.gateway.adapters.push import PushAdapter
from besm.gateway.adapters.telegram import TelegramAdapter
from besm.gateway.adapters.whatsapp import WhatsAppAdapter
from besm.gateway.formatter import format_alert
from besm.gateway.rate_limiter import RateLimiter
from besm.reasoning.state import AlertPayload

log = structlog.get_logger()

_SECONDARY_RETRY_DELAY_S = 600


class DispatchResult(Enum):
    DELIVERED = "delivered"
    SUPPRESSED = "suppressed"  # quiet hours / rate limit / low confidence
    FAILED = "failed"  # send attempt made but channel returned error


class Dispatcher:
    def __init__(self, config: BESMConfig, rate_limiter: RateLimiter) -> None:
        self._cfg = config
        self._rate_limiter = rate_limiter
        self._primary = _build_adapter(config, config.alerts.primary_channel)
        self._secondary = (
            _build_adapter(config, config.alerts.secondary_channel)
            if config.alerts.secondary_channel is not None
            else None
        )

    async def aclose(self) -> None:
        await self._primary.aclose()
        if self._secondary is not None:
            await self._secondary.aclose()

    async def dispatch(self, payload: AlertPayload) -> DispatchResult:
        if payload["confidence_score"] < self._cfg.alerts.min_confidence_threshold:
            log.info(
                "alert_suppressed_low_confidence",
                alert_id=payload["alert_id"],
                confidence=payload["confidence_score"],
            )
            return DispatchResult.SUPPRESSED

        if _is_quiet_hours(self._cfg):
            log.info("alert_suppressed_quiet_hours", alert_id=payload["alert_id"])
            return DispatchResult.SUPPRESSED

        if not await self._rate_limiter.is_allowed():
            log.warning("alert_suppressed_daily_limit", alert_id=payload["alert_id"])
            return DispatchResult.SUPPRESSED

        message = format_alert(payload)
        result = await self._primary.send(payload, message)

        if result.success:
            await self._rate_limiter.increment()
            log.info(
                "alert_delivered", channel=result.channel, alert_id=payload["alert_id"]
            )
            return DispatchResult.DELIVERED

        log.warning(
            "primary_delivery_failed",
            channel=result.channel,
            error=result.error,
            alert_id=payload["alert_id"],
        )

        if self._secondary is not None:
            asyncio.create_task(self._retry_secondary(payload, message))
        else:
            log.error(
                "no_secondary_channel",
                alert_id=payload["alert_id"],
                message="Primary delivery failed and no secondary channel configured",
            )

        return DispatchResult.FAILED

    async def _retry_secondary(self, payload: AlertPayload, message: str) -> None:
        await asyncio.sleep(_SECONDARY_RETRY_DELAY_S)
        result = await self._secondary.send(payload, message)  # type: ignore[union-attr]
        if result.success:
            await self._rate_limiter.increment()
            log.info(
                "alert_delivered_secondary",
                channel=result.channel,
                alert_id=payload["alert_id"],
            )
        else:
            log.error(
                "secondary_delivery_failed",
                channel=result.channel,
                error=result.error,
                alert_id=payload["alert_id"],
            )


def _is_quiet_hours(config: BESMConfig) -> bool:
    tz = ZoneInfo(config.alerts.timezone)
    now = datetime.now(tz).time()

    def parse(t: str):
        h, m = t.split(":")
        from datetime import time

        return time(int(h), int(m))

    start = parse(config.alerts.quiet_hours_start)
    end = parse(config.alerts.quiet_hours_end)

    if start > end:
        return now >= start or now < end
    return start <= now < end


def _build_adapter(config: BESMConfig, channel: str) -> ChannelAdapter:
    if channel == "telegram":
        cfg = config.channels.telegram
        assert cfg is not None
        return TelegramAdapter(cfg.bot_token_env, cfg.chat_id_env)
    if channel == "whatsapp":
        cfg = config.channels.whatsapp
        assert cfg is not None
        return WhatsAppAdapter(
            cfg.account_sid_env,
            cfg.auth_token_env,
            cfg.from_number_env,
            cfg.to_number_env,
        )
    cfg = config.channels.push
    assert cfg is not None
    return PushAdapter(cfg.ntfy_topic_env, cfg.ntfy_server)
