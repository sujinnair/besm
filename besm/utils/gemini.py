from __future__ import annotations

import httpx

from besm.config.secrets import require_env


def gemini_headers(api_key_env: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-goog-api-key": require_env(api_key_env),
    }


def gemini_payload(
    prompt: str,
    system_prompt: str | None,
    web_search_enabled: bool,
) -> dict:
    contents: list[dict] = []
    if system_prompt:
        contents.append({"role": "user", "parts": [{"text": system_prompt}]})
        contents.append({"role": "model", "parts": [{"text": "Understood."}]})
    contents.append({"role": "user", "parts": [{"text": prompt}]})

    payload: dict = {"contents": contents}
    if web_search_enabled:
        payload["tools"] = [{"google_search": {}}]
    return payload


def gemini_url(base_url: str, model: str) -> str:
    return f"{base_url.rstrip('/')}/models/{model}:generateContent"


def extract_gemini_response(data: dict) -> tuple[str, int]:
    """Returns (text, source_count) from a Gemini generateContent response."""
    candidate = data["candidates"][0]
    text = "".join(p["text"] for p in candidate["content"]["parts"] if "text" in p)
    chunks = candidate.get("groundingMetadata", {}).get("groundingChunks", [])
    return text, len(chunks)


async def gemini_generate(
    http: httpx.AsyncClient,
    base_url: str,
    model: str,
    api_key_env: str,
    prompt: str,
    system_prompt: str | None = None,
    web_search_enabled: bool = True,
) -> tuple[str, int]:
    resp = await http.post(
        gemini_url(base_url, model),
        json=gemini_payload(prompt, system_prompt, web_search_enabled),
        headers=gemini_headers(api_key_env),
    )
    resp.raise_for_status()
    return extract_gemini_response(resp.json())
