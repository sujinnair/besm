from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import structlog

from besm.config.loader import LiveConfig
from besm.config.schema import LLMBackendConfig
from besm.config.secrets import require_env
from besm.utils.gemini import gemini_generate
from besm.utils.tls import tls_client
from besm.vector_store.store import VectorStore

log = structlog.get_logger()

_CONFIRM_SYSTEM_PROMPT = """You are an impact verification agent.
You receive a signal prediction and must determine if the predicted impact has occurred.
Return only a JSON object with these exact fields:
{
  "confirmed": bool,
  "reason": str
}
No preamble, no explanation."""


class ImpactConfirmer:
    def __init__(
        self,
        vector_store: VectorStore,
        live_config: "LiveConfig",
        check_interval_hours: int = 6,
        confirmation_window_hours: int = 24,
    ) -> None:
        self._vs = vector_store
        self._live_config = live_config
        self._interval_s = check_interval_hours * 3600
        self._window_h = confirmation_window_hours
        self._http = tls_client(timeout=60.0)

    @property
    def _llm(self) -> "LLMBackendConfig":
        return self._live_config.current.llm

    async def run_forever(self) -> None:
        while True:
            await asyncio.sleep(self._interval_s)
            await self._check_pending()

    async def _check_pending(self) -> None:
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(hours=self._window_h)

        pending = await self._vs.get_unconfirmed_past_signals(
            after=window_start, before=now
        )

        if not pending:
            return

        log.info("impact_confirmation_check", pending_count=len(pending))

        await asyncio.gather(
            *[self._confirm(record) for record in pending],
            return_exceptions=True,
        )

    async def _confirm(self, record: dict) -> None:
        signal_id = record["signal_id"]
        domain = record["domain"]
        raw_text = record.get("raw_text", "")
        t_impact_predicted = record["t_impact_predicted"]

        prompt = (
            f"Signal domain: {domain}\n"
            f"Original signal: {raw_text}\n"
            f"Predicted impact date: {t_impact_predicted}\n\n"
            "Search the web for evidence of whether this predicted impact has "
            "occurred. Has the predicted real-world consequence materialised? "
            "Return your verdict as JSON."
        )

        try:
            result = await self._query_llm(prompt)
        except Exception as exc:
            log.warning("impact_confirm_llm_error", signal_id=signal_id, error=str(exc))
            return

        if result.get("confirmed"):
            await self._vs.confirm_impact(
                signal_id=signal_id,
                t_impact_actual=datetime.now(timezone.utc),
            )
            log.info(
                "impact_confirmed",
                signal_id=signal_id,
                reason=result.get("reason", ""),
            )
        else:
            log.info(
                "impact_not_confirmed",
                signal_id=signal_id,
                reason=result.get("reason", ""),
            )

    async def _query_llm(self, prompt: str) -> dict:
        if self._llm.provider == "anthropic":
            text = await self._query_anthropic(prompt)
        elif self._llm.provider == "gemini":
            text = await self._query_gemini(prompt)
        else:
            text = await self._query_openai_compat(prompt)
        return _parse_json(text)

    async def _query_anthropic(self, prompt: str) -> str:
        payload: dict = {
            "model": self._llm.model,
            "max_tokens": 500,
            "system": _CONFIRM_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": prompt}],
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
        return next(b["text"] for b in resp.json()["content"] if b["type"] == "text")

    async def _query_gemini(self, prompt: str) -> str:
        text, _ = await gemini_generate(
            http=self._http,
            base_url=self._llm.base_url,
            model=self._llm.model,
            api_key_env=self._llm.api_key_env,
            prompt=prompt,
            system_prompt=_CONFIRM_SYSTEM_PROMPT,
            web_search_enabled=self._llm.web_search_enabled,
        )
        return text

    async def _query_openai_compat(self, prompt: str) -> str:
        resp = await self._http.post(
            f"{self._llm.base_url}/chat/completions",
            json={
                "model": self._llm.model,
                "max_tokens": 500,
                "messages": [
                    {"role": "system", "content": _CONFIRM_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {require_env(self._llm.api_key_env)}",
            },
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]


def _parse_json(text: str) -> dict:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```")[1]
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
    return json.loads(cleaned.strip())
