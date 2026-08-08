from redis.asyncio import Redis

_KEY = "seen_hashes"
_TTL_SECONDS = 60 * 60 * 24 * 7  # 7 days


class DeduplicationFilter:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def is_duplicate(self, content_hash: str) -> bool:
        return bool(await self._redis.sismember(_KEY, content_hash))

    async def mark_seen(self, content_hash: str) -> None:
        pipe = self._redis.pipeline()
        pipe.sadd(_KEY, content_hash)
        pipe.expire(_KEY, _TTL_SECONDS)
        await pipe.execute()
