from __future__ import annotations

import json
from abc import ABC, abstractmethod

from besm.config.schema import LLMBackendConfig
from besm.config.secrets import require_env
from besm.reasoning.state import AgentOutput, ReasoningState
from besm.utils.gemini import gemini_generate
from besm.utils.tls import tls_client

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

_SYSTEM_PROMPT = """You are a domain intelligence agent in a signal monitoring system.
You receive a raw signal text and must validate it, assess its impact, and return a
structured JSON object with these exact fields:
{
  "alert_approved": bool,
  "confidence": float (0-100),
  "summary": str (max 160 chars),
  "impact_description": str,
  "recommended_action": str
}
Return only valid JSON. No preamble, no explanation."""


class DomainAgent(ABC):
    domain: str

    def __init__(self, llm: LLMBackendConfig) -> None:
        self._llm = llm
        self._http = tls_client(timeout=30.0)

    @property
    @abstractmethod
    def validation_prompt(self) -> str: ...

    async def aclose(self) -> None:
        await self._http.aclose()

    async def validate(self, state: ReasoningState) -> ReasoningState:
        signal = state["signal"]
        user_content = (
            f"Domain: {self.domain}\n"
            f"Signal text:\n{signal.raw_text}\n\n"
            f"{self.validation_prompt}"
        )

        if self._llm.provider == "anthropic":
            output = await self._call_anthropic(user_content)
        elif self._llm.provider == "gemini":
            output = await self._call_gemini(user_content)
        else:
            output = await self._call_openai_compat(user_content)

        outputs = list(state["agent_outputs"])
        outputs.append(output)
        return {**state, "agent_outputs": outputs}

    async def _call_anthropic(self, user_content: str) -> AgentOutput:
        resp = await self._http.post(
            _ANTHROPIC_URL,
            json={
                "model": self._llm.model,
                "max_tokens": 1000,
                "system": _SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_content}],
            },
            headers={
                "Content-Type": "application/json",
                "x-api-key": require_env(self._llm.api_key_env),
                "anthropic-version": "2023-06-01",
            },
        )
        resp.raise_for_status()
        text = next(b["text"] for b in resp.json()["content"] if b["type"] == "text")
        return self._parse_output(text)

    async def _call_gemini(self, user_content: str) -> AgentOutput:
        text, _ = await gemini_generate(
            http=self._http,
            base_url=self._llm.base_url,
            model=self._llm.model,
            api_key_env=self._llm.api_key_env,
            prompt=user_content,
            system_prompt=_SYSTEM_PROMPT,
            web_search_enabled=False,  # agent validation does not need web search
        )
        return self._parse_output(text)

    async def _call_openai_compat(self, user_content: str) -> AgentOutput:
        resp = await self._http.post(
            f"{self._llm.base_url}/chat/completions",
            json={
                "model": self._llm.model,
                "max_tokens": 1000,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {require_env(self._llm.api_key_env)}",
            },
        )
        resp.raise_for_status()
        text = resp.json()["choices"][0]["message"]["content"]
        return self._parse_output(text)

    def _parse_output(self, text: str) -> AgentOutput:
        try:
            data = json.loads(text)
            return AgentOutput(
                domain=self.domain,
                confidence=float(data.get("confidence", 0)),
                alert_approved=bool(data.get("alert_approved", False)),
                summary=str(data.get("summary", ""))[:160],
                impact_description=str(data.get("impact_description", "")),
                recommended_action=str(data.get("recommended_action", "")),
            )
        except (json.JSONDecodeError, KeyError):
            return AgentOutput(
                domain=self.domain,
                confidence=0.0,
                alert_approved=False,
                summary="",
                impact_description="",
                recommended_action="",
            )
