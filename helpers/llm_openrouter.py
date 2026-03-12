from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any

import httpx

from helpers.rate_limiter import RateLimiter


@dataclass(frozen=True)
class LLMConfig:
    api_key: str
    base_url: str
    model: str
    max_tokens: int | None = None


@dataclass(frozen=True)
class LLMUsage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None

    def merged(self, other: "LLMUsage") -> "LLMUsage":
        def _sum(a: int | None, b: int | None) -> int | None:
            if a is None and b is None:
                return None
            return int(a or 0) + int(b or 0)

        return LLMUsage(
            input_tokens=_sum(self.input_tokens, other.input_tokens),
            output_tokens=_sum(self.output_tokens, other.output_tokens),
            total_tokens=_sum(self.total_tokens, other.total_tokens),
        )


def load_llm_config() -> LLMConfig:
    api_key = (os.environ.get("OPENROUTER_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required.")
    base_url = (os.environ.get("OPENROUTER_BASE_URL") or "https://openrouter.ai/api/v1").strip()
    model = (os.environ.get("OPENROUTER_MODEL") or "meta-llama/llama-3.3-70b-instruct:free").strip()
    max_tokens_raw = (os.environ.get("OPENROUTER_MAX_TOKENS") or "").strip()
    max_tokens: int | None
    if not max_tokens_raw:
        max_tokens = None
    else:
        try:
            max_tokens = int(max_tokens_raw)
        except Exception:
            max_tokens = None
    return LLMConfig(api_key=api_key, base_url=base_url, model=model, max_tokens=max_tokens)


class OpenRouterClient:
    def __init__(self, config: LLMConfig, *, limiter: RateLimiter, timeout_s: float = 60.0):
        self._config = config
        self._limiter = limiter
        self._timeout_s = float(timeout_s)
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "OpenRouterClient":
        self._client = httpx.AsyncClient(base_url=self._config.base_url, timeout=self._timeout_s)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._config.api_key}",
            "Content-Type": "application/json",
        }
        if self._client is None:
            raise RuntimeError("OpenRouterClient must be used as an async context manager.")
        r = await self._client.post("/chat/completions", headers=headers, json=payload)
        r.raise_for_status()
        return r.json()

    async def chat_text(self, *, system: str, user: str, log, model: str | None = None) -> tuple[str, LLMUsage]:
        payload = {
            "model": (model or self._config.model),
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        if self._config.max_tokens is not None:
            payload["max_tokens"] = int(self._config.max_tokens)

        backoffs = [1, 2, 4]
        last_err: Exception | None = None
        for attempt in range(1, 5):
            slept = await self._limiter.acquire()
            if slept > 0:
                log(f"Waiting {slept:.1f}s to respect the LLM rate limit...")
            try:
                data = await self._post(payload)
                content = (
                    (data.get("choices") or [{}])[0]
                    .get("message", {})
                    .get("content", "")
                )
                return str(content or ""), _parse_usage(data)
            except (httpx.HTTPStatusError, httpx.TransportError) as e:
                last_err = e
                status = getattr(getattr(e, "response", None), "status_code", None)
                is_retryable = status in (429, 500, 502, 503, 504) or status is None
                if attempt >= 4 or not is_retryable:
                    raise
                wait = backoffs[min(len(backoffs) - 1, attempt - 1)]
                log(f"LLM call failed ({type(e).__name__}{' ' + str(status) if status else ''}). Retrying in {wait}s...")
                # Note: retry will also pass through the rate limiter again.
                await asyncio.sleep(wait)
        raise last_err or RuntimeError("LLM call failed.")

    async def extract_json(self, *, system: str, user: str, log, model: str | None = None) -> tuple[dict[str, Any], LLMUsage]:
        text, usage1 = await self.chat_text(system=system, user=user, log=log, model=model)
        parsed = _parse_json(text)
        if parsed is not None:
            return parsed, usage1
        # One repair attempt
        repair_user = (
            "The previous response was not valid JSON.\n"
            "Return ONLY valid JSON. Do not include any extra text.\n\n"
            "Invalid output:\n"
            f"{text}\n"
        )
        repaired, usage2 = await self.chat_text(system=system, user=repair_user, log=log, model=model)
        parsed2 = _parse_json(repaired)
        if parsed2 is not None:
            return parsed2, usage1.merged(usage2)
        return {}, usage1.merged(usage2)


def _parse_usage(data: dict[str, Any]) -> LLMUsage:
    usage = data.get("usage") if isinstance(data, dict) else None
    if not isinstance(usage, dict):
        return LLMUsage()
    # OpenAI-style: prompt_tokens / completion_tokens / total_tokens
    in_tok = usage.get("prompt_tokens")
    out_tok = usage.get("completion_tokens")
    total_tok = usage.get("total_tokens")
    try:
        in_tok = int(in_tok) if in_tok is not None else None
    except Exception:
        in_tok = None
    try:
        out_tok = int(out_tok) if out_tok is not None else None
    except Exception:
        out_tok = None
    try:
        total_tok = int(total_tok) if total_tok is not None else None
    except Exception:
        total_tok = None
    return LLMUsage(input_tokens=in_tok, output_tokens=out_tok, total_tokens=total_tok)


def _parse_json(text: str) -> dict[str, Any] | None:
    s = (text or "").strip()
    if not s:
        return None
    # Common: model wraps JSON in code fences
    if s.startswith("```"):
        lines = s.splitlines()
        if lines:
            # drop first line like ```json
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    # If there's surrounding text, try to extract the outermost JSON object.
    if "{" in s and "}" in s:
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            s = s[start : end + 1]
    try:
        v = json.loads(s)
    except json.JSONDecodeError:
        return None
    if isinstance(v, dict):
        return v
    return None
