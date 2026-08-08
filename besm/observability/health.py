from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import structlog
from redis.asyncio import Redis

log = structlog.get_logger()

_SUCCESS_RATE_THRESHOLD = 0.90
_ROLLING_WINDOW_HOURS = 1


class DeliveryMonitor:
    """Tracks per-channel delivery success rate over a rolling 1-hour window."""

    def __init__(self) -> None:
        self._events: dict[str, deque[tuple[datetime, bool]]] = {}

    def record(self, channel: str, success: bool) -> None:
        if channel not in self._events:
            self._events[channel] = deque()
        self._events[channel].append((datetime.now(timezone.utc), success))
        self._evict(channel)
        self._check_threshold(channel)

    def _evict(self, channel: str) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=_ROLLING_WINDOW_HOURS)
        q = self._events[channel]
        while q and q[0][0] < cutoff:
            q.popleft()

    def _check_threshold(self, channel: str) -> None:
        q = self._events[channel]
        if not q:
            return
        rate = sum(1 for _, ok in q if ok) / len(q)
        if rate < _SUCCESS_RATE_THRESHOLD:
            log.error(
                "OPERATIONAL_ALERT",
                message="Delivery success rate below 90%",
                channel=channel,
                success_rate=round(rate, 3),
                window_hours=_ROLLING_WINDOW_HOURS,
            )


async def run_startup_checks(redis: Redis, config_path: str) -> None:
    checks_passed = True

    # Config file exists
    if not Path(config_path).exists():
        log.error("startup_check_failed", check="config_file", path=config_path)
        checks_passed = False
    else:
        log.info("startup_check_passed", check="config_file")

    # Redis reachable
    try:
        await redis.ping()
        log.info("startup_check_passed", check="redis")
    except Exception as exc:
        log.error("startup_check_failed", check="redis", error=str(exc))
        checks_passed = False

    # .env in .gitignore (warning only — non-fatal)
    _check_env_gitignore()

    if not checks_passed:
        raise RuntimeError("Startup checks failed — see logs above")


def _check_env_gitignore() -> None:
    gitignore = Path(".gitignore")
    if not gitignore.exists():
        log.warning(
            "startup_check_warning", check="gitignore", message=".gitignore not found"
        )
        return
    entries = {e.strip().lstrip("/") for e in gitignore.read_text().splitlines()}
    if ".env" not in entries:
        log.warning(
            "startup_check_warning",
            check="gitignore",
            message=".env is not listed in .gitignore",
        )
