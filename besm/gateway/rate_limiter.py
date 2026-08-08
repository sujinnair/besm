from __future__ import annotations

from redis.asyncio import Redis

from besm.config.loader import LiveConfig

_KEY = "daily_alert_count"
_TTL_SECONDS = 60 * 60 * 24


class RateLimiter:
    def __init__(self, redis: Redis, live_config: LiveConfig) -> None:
        self._redis = redis
        self._live_config = live_config

    async def is_allowed(self) -> bool:
        limit = self._live_config.current.alerts.daily_limit
        count = await self._redis.get(_KEY)
        return int(count or 0) < limit

    async def increment(self) -> None:
        pipe = self._redis.pipeline()
        pipe.incr(_KEY)
        pipe.expire(_KEY, _TTL_SECONDS)
        await pipe.execute()
