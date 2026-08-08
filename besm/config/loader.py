from __future__ import annotations

import asyncio
import os
from pathlib import Path

import structlog
import yaml
from pydantic import ValidationError

from besm.config.schema import BESMConfig

log = structlog.get_logger()

_DEFAULT_CONFIG_PATH = Path("config.yaml")


def _config_path() -> Path:
    env = os.getenv("BESM_CONFIG_PATH")
    return Path(env) if env else _DEFAULT_CONFIG_PATH


def load_config() -> BESMConfig:
    path = _config_path()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"Config file is not a valid YAML mapping: {path}")
    try:
        return BESMConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Config validation failed:\n{exc}") from exc


class LiveConfig:
    """Thread-safe config holder that reloads from disk on each poll cycle."""

    def __init__(self) -> None:
        self._config: BESMConfig = load_config()
        self._lock = asyncio.Lock()

    @property
    def current(self) -> BESMConfig:
        return self._config

    async def reload(self) -> None:
        try:
            fresh = await asyncio.to_thread(load_config)
        except Exception as exc:
            log.warning("config_reload_failed", error=str(exc))
            return
        async with self._lock:
            self._config = fresh
        log.info("config_reloaded")
