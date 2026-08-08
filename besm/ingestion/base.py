from __future__ import annotations

import asyncio
import hashlib
import json
import random
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import structlog
from redis.asyncio import Redis

from besm.config.schema import LLMBackendConfig, OperatorConfig
from besm.config.secrets import require_env
from besm.engine.models import Domain, RawSignalEvent
from besm.utils.gemini import gemini_generate
from besm.utils.tls import tls_client

log = structlog.get_logger()

_STREAM = "raw_signals"
_RETRY_BACKOFF_S = 300
_ALERT_THRESHOLD_S = 3600

_GEMINI_MAX_RETRIES = 4
_GEMINI_BASE_DELAY_S = 5.0
_GEMINI_MAX_DELAY_S = 60.0

_ANTHROPIC_BATCH_BETA = "message-batches-2024-09-24"
_BATCH_KEY_TTL_S = 86400  # 24-hour Redis TTL — matches Anthropic batch expiry


class RawDataRecord:
    __slots__ = ("source_url", "raw_content", "metadata")

    def __init__(self, source_url: str, raw_content: str, metadata: dict) -> None:
        self.source_url = source_url
        self.raw_content = raw_content
        self.metadata = metadata


class BaseIngestionNode(ABC):
    node_id: str
    domain: Domain
    poll_interval_minutes: int
    fetch_prompt: str = ""  # used by non-location-aware nodes
    # Set True on nodes where a ~1-hour async delay is acceptable.
    # Only activates when provider == "anthropic"; other providers use normal fetch.
    batch_eligible: bool = False

    def __init__(self, redis: Redis, llm: LLMBackendConfig) -> None:
        self._redis = redis
        self._llm = llm
        self._operator: OperatorConfig | None = None  # injected by scheduler before each run
        self._consecutive_failure_s: float = 0.0
        self._last_failure_time: datetime | None = None
        self._http = tls_client(timeout=60.0)

    def build_prompt(self) -> str:
        template = getattr(type(self), "_prompt_template", None)
        if template is not None and self._operator is not None:
            try:
                return template.format_map(self._operator.location_vars())
            except (KeyError, ValueError):
                log.warning("prompt_template_format_failed", node_id=self.node_id)
                return template
        return self.fetch_prompt

    @abstractmethod
    async def parse(self, raw: RawDataRecord) -> list[RawSignalEvent]: ...

    async def aclose(self) -> None:
        await self._http.aclose()

    async def fetch(self) -> list[RawDataRecord]:
        if self._llm.provider == "anthropic":
            text, source_count = await self._fetch_anthropic()
        elif self._llm.provider == "gemini":
            text, source_count = await self._fetch_gemini()
        else:
            text, source_count = await self._fetch_openai_compat()

        return [
            RawDataRecord(
                source_url="agent_web_search",
                raw_content=text,
                metadata={
                    "publication_date": datetime.now(timezone.utc).date().isoformat(),
                    "issuing_authority": self.node_id,
                    "data_type": self.domain,
                    "source_count": source_count,
                },
            )
        ]

    async def _fetch_anthropic(self) -> tuple[str, int]:
        payload: dict = {
            "model": self._llm.model,
            "max_tokens": 1000,
            "messages": [{"role": "user", "content": self.build_prompt()}],
        }
        if self._llm.web_search_enabled:
            payload["tools"] = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 1}]

        resp = await self._http.post(
            f"{self._llm.base_url}/messages",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": require_env(self._llm.api_key_env),
                "anthropic-version": "2023-06-01",
            },
        )
        resp.raise_for_status()
        data = resp.json()

        text = "\n".join(
            block["text"]
            for block in data.get("content", [])
            if block.get("type") == "text"
        )
        source_count = sum(
            1 for block in data.get("content", []) if block.get("type") == "tool_result"
        )
        return text, source_count

    async def _fetch_gemini(self) -> tuple[str, int]:
        delay = _GEMINI_BASE_DELAY_S
        for attempt in range(_GEMINI_MAX_RETRIES + 1):
            try:
                return await gemini_generate(
                    http=self._http,
                    base_url=self._llm.base_url,
                    model=self._llm.model,
                    api_key_env=self._llm.api_key_env,
                    prompt=self.build_prompt(),
                    web_search_enabled=self._llm.web_search_enabled,
                )
            except Exception as exc:
                is_429 = "429" in str(exc)
                is_503 = "503" in str(exc)
                if (is_429 or is_503) and attempt < _GEMINI_MAX_RETRIES:
                    jitter = random.uniform(0, delay)
                    wait = min(delay + jitter, _GEMINI_MAX_DELAY_S)
                    log.warning(
                        "gemini_rate_limited",
                        node_id=self.node_id,
                        attempt=attempt + 1,
                        wait_s=round(wait, 1),
                        status="429" if is_429 else "503",
                    )
                    await asyncio.sleep(wait)
                    delay = min(delay * 2, _GEMINI_MAX_DELAY_S)
                else:
                    raise

    async def _fetch_openai_compat(self) -> tuple[str, int]:
        payload = {
            "model": self._llm.model,
            "max_tokens": 1000,
            "messages": [{"role": "user", "content": self.build_prompt()}],
        }
        resp = await self._http.post(
            f"{self._llm.base_url}/chat/completions",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {require_env(self._llm.api_key_env)}",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["choices"][0]["message"]["content"] or ""
        return text, 0

    async def run(self) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        await self._redis.hset(f"node_state:{self.node_id}", mapping={"last_poll": now_iso})

        if self.batch_eligible and self._llm.provider == "anthropic":
            await self._run_batch_flow()
            return

        try:
            records = await self.fetch()
            self._consecutive_failure_s = 0.0
            self._last_failure_time = None
            await self._redis.hset(
                f"node_state:{self.node_id}",
                mapping={
                    "last_success": now_iso,
                    "last_error": "",
                    "consecutive_failure_s": "0",
                    "status": "ok",
                },
            )
        except Exception as exc:
            await self._handle_fetch_error(exc)
            return

        for record in records:
            try:
                events = await self.parse(record)
            except Exception as exc:
                log.error("parse_error", node_id=self.node_id, error=str(exc))
                continue
            for event in events:
                await self._publish(event)

    # ── Batch API flow (Anthropic Message Batches — 50% token discount) ──────

    async def _run_batch_flow(self) -> None:
        """
        Each scheduler tick: retrieve the previous batch result (if ready),
        publish its events, then submit the next batch for the following tick.
        Skips submitting a new batch while the previous one is still processing.
        """
        pending_key = f"batch_pending:{self.node_id}"
        pending_id = await self._redis.get(pending_key)

        if pending_id:
            batch_id = pending_id.decode()
            batch_result = await self._retrieve_anthropic_batch(batch_id)
            if batch_result is None:
                log.info("batch_still_processing", node_id=self.node_id, batch_id=batch_id)
                await self._redis.hset(
                    f"node_state:{self.node_id}",
                    mapping={"status": "batch_pending", "batch_id": batch_id},
                )
                return
            await self._redis.delete(pending_key)
            self._consecutive_failure_s = 0.0
            self._last_failure_time = None
            now_iso = datetime.now(timezone.utc).isoformat()
            await self._redis.hset(
                f"node_state:{self.node_id}",
                mapping={
                    "last_success": now_iso,
                    "last_error": "",
                    "consecutive_failure_s": "0",
                    "status": "ok",
                    "batch_id": "",
                },
            )
            text, source_count = batch_result
            if text:
                await self._publish_raw_text(text, source_count)

        # Submit the next batch (first run or after processing a result)
        try:
            batch_id = await self._submit_anthropic_batch()
            await self._redis.set(pending_key, batch_id, ex=_BATCH_KEY_TTL_S)
            log.info("batch_submitted", node_id=self.node_id, batch_id=batch_id)
            await self._redis.hset(
                f"node_state:{self.node_id}",
                mapping={"status": "batch_pending", "batch_id": batch_id},
            )
        except Exception as exc:
            await self._handle_fetch_error(exc)

    async def _publish_raw_text(self, text: str, source_count: int = 1) -> None:
        record = RawDataRecord(
            source_url="agent_web_search",
            raw_content=text,
            metadata={
                "publication_date": datetime.now(timezone.utc).date().isoformat(),
                "issuing_authority": self.node_id,
                "data_type": self.domain,
                "source_count": source_count,
            },
        )
        try:
            events = await self.parse(record)
        except Exception as exc:
            log.error("parse_error", node_id=self.node_id, error=str(exc))
            return
        for event in events:
            await self._publish(event)

    async def _submit_anthropic_batch(self) -> str:
        payload: dict = {
            "requests": [
                {
                    "custom_id": f"{self.node_id}-{uuid.uuid4()}",
                    "params": {
                        "model": self._llm.model,
                        "max_tokens": 1000,
                        "messages": [{"role": "user", "content": self.build_prompt()}],
                    },
                }
            ]
        }
        if self._llm.web_search_enabled:
            payload["requests"][0]["params"]["tools"] = [
                {"type": "web_search_20250305", "name": "web_search", "max_uses": 1}
            ]

        resp = await self._http.post(
            f"{self._llm.base_url}/messages/batches",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": require_env(self._llm.api_key_env),
                "anthropic-version": "2023-06-01",
                "anthropic-beta": _ANTHROPIC_BATCH_BETA,
            },
        )
        resp.raise_for_status()
        return resp.json()["id"]

    async def _retrieve_anthropic_batch(
        self, batch_id: str
    ) -> tuple[str, int] | None:
        """
        Returns (text, source_count) when the batch has ended.
        Returns None if still processing.
        Returns ("", 0) if the batch ended in error/cancellation.
        """
        resp = await self._http.get(
            f"{self._llm.base_url}/messages/batches/{batch_id}",
            headers={
                "x-api-key": require_env(self._llm.api_key_env),
                "anthropic-version": "2023-06-01",
                "anthropic-beta": _ANTHROPIC_BATCH_BETA,
            },
        )
        resp.raise_for_status()
        status_data = resp.json()

        if status_data["processing_status"] != "ended":
            return None

        results_resp = await self._http.get(
            f"{self._llm.base_url}/messages/batches/{batch_id}/results",
            headers={
                "x-api-key": require_env(self._llm.api_key_env),
                "anthropic-version": "2023-06-01",
                "anthropic-beta": _ANTHROPIC_BATCH_BETA,
            },
        )
        results_resp.raise_for_status()

        for line in results_resp.text.strip().splitlines():
            result = json.loads(line)
            if result["result"]["type"] == "succeeded":
                content = result["result"]["message"]["content"]
                text = "\n".join(
                    block["text"] for block in content if block.get("type") == "text"
                )
                source_count = sum(
                    1 for block in content if block.get("type") == "tool_result"
                )
                return text, source_count
            log.warning(
                "batch_result_not_succeeded",
                node_id=self.node_id,
                batch_id=batch_id,
                result_type=result["result"]["type"],
            )
        return "", 0

    async def _handle_fetch_error(self, exc: Exception) -> None:
        now = datetime.now(timezone.utc)
        if self._last_failure_time is None:
            self._last_failure_time = now
            self._consecutive_failure_s = 0.0
        else:
            self._consecutive_failure_s += (
                now - self._last_failure_time
            ).total_seconds()
            self._last_failure_time = now

        log.warning(
            "fetch_error",
            node_id=self.node_id,
            error=str(exc),
            consecutive_failure_s=self._consecutive_failure_s,
        )

        if self._consecutive_failure_s > _ALERT_THRESHOLD_S:
            log.error(
                "OPERATIONAL_ALERT",
                node_id=self.node_id,
                message="Ingestion node failed for more than 60 consecutive minutes",
                consecutive_failure_s=self._consecutive_failure_s,
            )

        await self._redis.hset(
            f"node_state:{self.node_id}",
            mapping={
                "last_error": str(exc),
                "consecutive_failure_s": str(self._consecutive_failure_s),
                "status": "failing",
            },
        )
        await asyncio.sleep(_RETRY_BACKOFF_S)

    async def _publish(self, event: RawSignalEvent) -> None:
        await self._redis.hincrby(f"node_state:{self.node_id}", "signals_total", 1)
        await self._redis.xadd(
            _STREAM,
            {
                "event_id": event.event_id,
                "node_id": event.node_id,
                "domain": event.domain,
                "content_hash": event.content_hash,
                "source_url": event.source_url,
                "raw_text": event.raw_text,
                "metadata": json.dumps(event.metadata),
                "t_signal": event.t_signal.isoformat(),
            },
            maxlen=10_000,
            approximate=True,
        )


def make_content_hash(source_url: str, publication_date: str, content: str) -> str:
    fingerprint = f"{source_url}|{publication_date}|{content[:512]}"
    return hashlib.sha256(fingerprint.encode()).hexdigest()
