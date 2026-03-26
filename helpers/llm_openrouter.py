from __future__ import annotations

import asyncio
import json
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from helpers.rate_limiter import RateLimiter


@dataclass(frozen=True)
class LLMConfig:
    provider: str
    model: str
    max_tokens: int | None = None
    api_key: str = ""
    base_url: str = ""
    region: str = ""
    aws_profile: str = ""


@dataclass(frozen=True)
class LLMUsage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None

    def has_values(self) -> bool:
        return self.input_tokens is not None or self.output_tokens is not None or self.total_tokens is not None

    def effective_total_tokens(self) -> int | None:
        if self.total_tokens is not None:
            return int(self.total_tokens)
        if self.input_tokens is None and self.output_tokens is None:
            return None
        return int(self.input_tokens or 0) + int(self.output_tokens or 0)

    def log_parts(self) -> list[str]:
        parts: list[str] = []
        if self.input_tokens is not None:
            parts.append(f"input={self.input_tokens}")
        if self.output_tokens is not None:
            parts.append(f"output={self.output_tokens}")
        total_tokens = self.effective_total_tokens()
        if total_tokens is not None:
            parts.append(f"total={total_tokens}")
        return parts

    def merged(self, other: "LLMUsage") -> "LLMUsage":
        def _sum(a: int | None, b: int | None) -> int | None:
            if a is None and b is None:
                return None
            return int(a or 0) + int(b or 0)

        return LLMUsage(
            input_tokens=_sum(self.input_tokens, other.input_tokens),
            output_tokens=_sum(self.output_tokens, other.output_tokens),
            total_tokens=_sum(self.effective_total_tokens(), other.effective_total_tokens()),
        )


@dataclass
class _TrackedUsage:
    calls: int = 0
    calls_without_usage: int = 0
    usage: LLMUsage = field(default_factory=LLMUsage)


class LLMUsageTracker:
    def __init__(self, *, run_name: str):
        self._run_name = str(run_name or "llm_run").strip() or "llm_run"
        self._started_at = datetime.now(timezone.utc)
        self._lock = threading.Lock()
        self._aggregate = _TrackedUsage()
        self._per_model: dict[str, _TrackedUsage] = {}

    def record(self, *, model: str, usage: LLMUsage) -> None:
        model_name = str(model or "unknown_model").strip() or "unknown_model"
        usage_value = usage if isinstance(usage, LLMUsage) else LLMUsage()
        with self._lock:
            self._aggregate.calls += 1
            self._aggregate.usage = self._aggregate.usage.merged(usage_value)
            if not usage_value.has_values():
                self._aggregate.calls_without_usage += 1

            tracked = self._per_model.setdefault(model_name, _TrackedUsage())
            tracked.calls += 1
            tracked.usage = tracked.usage.merged(usage_value)
            if not usage_value.has_values():
                tracked.calls_without_usage += 1

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            by_model = []
            for model_name in sorted(self._per_model):
                tracked = self._per_model[model_name]
                by_model.append(
                    {
                        "model": model_name,
                        "calls": tracked.calls,
                        "calls_without_usage": tracked.calls_without_usage,
                        "input_tokens": tracked.usage.input_tokens,
                        "output_tokens": tracked.usage.output_tokens,
                        "total_tokens": tracked.usage.effective_total_tokens(),
                    }
                )
            return {
                "run_name": self._run_name,
                "started_at": self._started_at.isoformat(),
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "calls": self._aggregate.calls,
                "calls_without_usage": self._aggregate.calls_without_usage,
                "input_tokens": self._aggregate.usage.input_tokens,
                "output_tokens": self._aggregate.usage.output_tokens,
                "total_tokens": self._aggregate.usage.effective_total_tokens(),
                "by_model": by_model,
            }

    def format_summary(self) -> str:
        snapshot = self.snapshot()
        parts = [f"calls={snapshot['calls']}"]
        if snapshot["input_tokens"] is not None:
            parts.append(f"input={snapshot['input_tokens']}")
        if snapshot["output_tokens"] is not None:
            parts.append(f"output={snapshot['output_tokens']}")
        if snapshot["total_tokens"] is not None:
            parts.append(f"total={snapshot['total_tokens']}")
        if snapshot["calls_without_usage"]:
            parts.append(f"missing_usage_calls={snapshot['calls_without_usage']}")
        return f"LLM usage summary ({self._run_name}): " + ", ".join(parts)

    def append_jsonl(self, path: Path) -> Path:
        snapshot = self.snapshot()
        path = Path(path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(snapshot, ensure_ascii=False) + "\n")
        return path


def _first_env(*names: str) -> str:
    for name in names:
        if not name:
            continue
        value = (os.environ.get(name) or "").strip()
        if value:
            return value
    return ""


def _parse_max_tokens(*names: str) -> int | None:
    raw = _first_env(*names)
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _normalize_provider(value: str) -> str:
    provider = (value or "").strip().lower()
    if provider in {"", "openrouter"}:
        return "openrouter"
    if provider in {"bedrock", "aws_bedrock", "amazon_bedrock"}:
        return "bedrock"
    raise RuntimeError(f"Unsupported LLM_PROVIDER: {value}")


def _normalize_mode(value: str) -> str:
    mode = (value or "").strip().lower()
    if mode in {"", "legacy"}:
        return "legacy"
    if mode in {"openrouter_only", "openrouter"}:
        return "openrouter_only"
    if mode in {"bedrock_only", "bedrock"}:
        return "bedrock_only"
    if mode == "mixed":
        return "mixed"
    raise RuntimeError(f"Unsupported LLM_MODE: {value}")


def _target_suffix(target: str | None) -> str:
    text = str(target or "").strip().lower()
    return text.upper() if text else ""


def _resolve_provider_for_target(target: str | None) -> str:
    mode = _normalize_mode(_first_env("LLM_MODE"))
    target_suffix = _target_suffix(target)

    if mode == "openrouter_only":
        return "openrouter"
    if mode == "bedrock_only":
        return "bedrock"
    if mode == "mixed":
        if not target_suffix:
            raise RuntimeError("LLM_MODE=mixed requires a target such as 'extract' or 'summary'.")
        provider = _first_env(f"MIXED_MODE_PROVIDER_{target_suffix}")
        if not provider:
            raise RuntimeError(f"MIXED_MODE_PROVIDER_{target_suffix} is required when LLM_MODE=mixed.")
        return _normalize_provider(provider)

    return _normalize_provider(_first_env(f"LLM_PROVIDER_{target_suffix}", "LLM_PROVIDER"))


def load_llm_config(*, target: str | None = None) -> LLMConfig:
    provider = _resolve_provider_for_target(target)
    target_suffix = _target_suffix(target)

    if provider == "openrouter":
        api_key = _first_env("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY is required when LLM_PROVIDER=openrouter.")
        base_url = _first_env("OPENROUTER_BASE_URL") or "https://openrouter.ai/api/v1"
        model = (
            _first_env(
                f"OPENROUTER_MODEL_{target_suffix}" if target_suffix else "",
            )
            or "meta-llama/llama-3.3-70b-instruct:free"
        )
        max_tokens = _parse_max_tokens(
            f"OPENROUTER_MAX_TOKENS_{target_suffix}" if target_suffix else "",
            "OPENROUTER_MAX_TOKENS",
        )
        return LLMConfig(
            provider=provider,
            model=model,
            max_tokens=max_tokens,
            api_key=api_key,
            base_url=base_url,
        )

    region = _first_env("BEDROCK_REGION", "AWS_REGION", "AWS_DEFAULT_REGION")
    if not region:
        raise RuntimeError("BEDROCK_REGION or AWS_REGION is required when LLM_PROVIDER=bedrock.")
    model = _first_env(
        f"BEDROCK_MODEL_{target_suffix}" if target_suffix else "",
    )
    if not model:
        if target_suffix:
            raise RuntimeError(f"BEDROCK_MODEL_{target_suffix} is required when LLM_PROVIDER=bedrock.")
        raise RuntimeError("BEDROCK_MODEL is required when LLM_PROVIDER=bedrock.")
    max_tokens = _parse_max_tokens(
        f"BEDROCK_MAX_TOKENS_{target_suffix}" if target_suffix else "",
        "BEDROCK_MAX_TOKENS",
    )
    aws_profile = _first_env("BEDROCK_AWS_PROFILE", "AWS_PROFILE")
    return LLMConfig(
        provider=provider,
        model=model,
        max_tokens=max_tokens,
        region=region,
        aws_profile=aws_profile,
    )


class LLMClient:
    def __init__(
        self,
        config: LLMConfig,
        *,
        limiter: RateLimiter,
        timeout_s: float = 60.0,
        usage_tracker: LLMUsageTracker | None = None,
    ):
        self._config = config
        self._limiter = limiter
        self._timeout_s = float(timeout_s)
        self._usage_tracker = usage_tracker
        self._http_client: httpx.AsyncClient | None = None
        self._bedrock_client: Any = None

    async def __aenter__(self) -> "LLMClient":
        if self._config.provider == "openrouter":
            self._http_client = httpx.AsyncClient(base_url=self._config.base_url, timeout=self._timeout_s)
        elif self._config.provider == "bedrock":
            self._bedrock_client = await asyncio.to_thread(self._create_bedrock_client)
        else:
            raise RuntimeError(f"Unsupported LLM provider: {self._config.provider}")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
        if self._bedrock_client is not None:
            close = getattr(self._bedrock_client, "close", None)
            if callable(close):
                await asyncio.to_thread(close)
            self._bedrock_client = None

    def _create_bedrock_client(self):
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("boto3 is required when LLM_PROVIDER=bedrock.") from exc

        session_kwargs: dict[str, Any] = {}
        if self._config.aws_profile:
            session_kwargs["profile_name"] = self._config.aws_profile
        session = boto3.session.Session(**session_kwargs)
        return session.client("bedrock-runtime", region_name=self._config.region)

    async def _post_openrouter(self, payload: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._config.api_key}",
            "Content-Type": "application/json",
        }
        if self._http_client is None:
            raise RuntimeError("LLMClient must be used as an async context manager.")
        r = await self._http_client.post("/chat/completions", headers=headers, json=payload)
        r.raise_for_status()
        return r.json()

    async def _converse_bedrock(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._bedrock_client is None:
            raise RuntimeError("LLMClient must be used as an async context manager.")
        return await asyncio.to_thread(self._bedrock_client.converse, **payload)

    async def _call_with_retries(self, *, model_name: str, log, send) -> tuple[dict[str, Any], LLMUsage]:
        backoffs = [1, 2, 4]
        last_err: Exception | None = None
        for attempt in range(1, 5):
            slept = await self._limiter.acquire()
            if slept > 0:
                log(f"Waiting {slept:.1f}s to respect the LLM rate limit...")
            try:
                data = await send()
                usage = _parse_usage(data)
                if self._usage_tracker is not None:
                    self._usage_tracker.record(model=f"{self._config.provider}:{model_name}", usage=usage)
                return data, usage
            except Exception as exc:
                last_err = exc
                status_code, retryable = _classify_retryable_exception(exc)
                if attempt >= 4 or not retryable:
                    raise
                wait = backoffs[min(len(backoffs) - 1, attempt - 1)]
                status_text = f" {status_code}" if status_code is not None else ""
                log(f"LLM call failed ({type(exc).__name__}{status_text}). Retrying in {wait}s...")
                await asyncio.sleep(wait)
        raise last_err or RuntimeError("LLM call failed.")

    async def chat_text(self, *, system: str, user: str, log, model: str | None = None) -> tuple[str, LLMUsage]:
        model_name = (model or self._config.model)

        if self._config.provider == "openrouter":
            payload = {
                "model": model_name,
                "temperature": 0,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            }
            if self._config.max_tokens is not None:
                payload["max_tokens"] = int(self._config.max_tokens)
            data, usage = await self._call_with_retries(
                model_name=model_name,
                log=log,
                send=lambda: self._post_openrouter(payload),
            )
            content = ((data.get("choices") or [{}])[0].get("message", {}).get("content", ""))
            return str(content or ""), usage

        payload = {
            "modelId": model_name,
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": user}],
                }
            ],
            "inferenceConfig": {
                "temperature": 0,
            },
        }
        if system.strip():
            payload["system"] = [{"text": system}]
        if self._config.max_tokens is not None:
            payload["inferenceConfig"]["maxTokens"] = int(self._config.max_tokens)

        data, usage = await self._call_with_retries(
            model_name=model_name,
            log=log,
            send=lambda: self._converse_bedrock(payload),
        )
        return _extract_bedrock_text(data), usage

    async def extract_json(self, *, system: str, user: str, log, model: str | None = None) -> tuple[dict[str, Any], LLMUsage]:
        text, usage1 = await self.chat_text(system=system, user=user, log=log, model=model)
        parsed = _parse_json(text)
        if parsed is not None:
            return parsed, usage1
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


def _safe_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _parse_usage(data: dict[str, Any]) -> LLMUsage:
    usage = data.get("usage") if isinstance(data, dict) else None
    if not isinstance(usage, dict):
        return LLMUsage()
    in_tok = usage.get("prompt_tokens")
    if in_tok is None:
        in_tok = usage.get("inputTokens")
    out_tok = usage.get("completion_tokens")
    if out_tok is None:
        out_tok = usage.get("outputTokens")
    total_tok = usage.get("total_tokens")
    if total_tok is None:
        total_tok = usage.get("totalTokens")
    return LLMUsage(
        input_tokens=_safe_int(in_tok),
        output_tokens=_safe_int(out_tok),
        total_tokens=_safe_int(total_tok),
    )


def _extract_bedrock_text(data: dict[str, Any]) -> str:
    message = ((data.get("output") or {}).get("message") or {})
    content = message.get("content") or []
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        text = block.get("text")
        if text:
            parts.append(str(text))
        citations_content = block.get("citationsContent")
        if isinstance(citations_content, dict):
            for citation_block in citations_content.get("content") or []:
                if isinstance(citation_block, dict) and citation_block.get("text"):
                    parts.append(str(citation_block["text"]))
    return "\n".join(part for part in parts if part).strip()


def _classify_retryable_exception(exc: Exception) -> tuple[int | None, bool]:
    if isinstance(exc, (httpx.HTTPStatusError, httpx.TransportError)):
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return status, status in (429, 500, 502, 503, 504) or status is None

    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        metadata = response.get("ResponseMetadata") or {}
        status = _safe_int(metadata.get("HTTPStatusCode"))
        error = response.get("Error") or {}
        code = str(error.get("Code") or "").strip()
        retryable_codes = {
            "InternalServerException",
            "ModelNotReadyException",
            "ServiceUnavailableException",
            "ThrottlingException",
            "TooManyRequestsException",
        }
        return status, status in (429, 500, 502, 503, 504) or code in retryable_codes

    retryable_names = {
        "ConnectTimeoutError",
        "ConnectionClosedError",
        "EndpointConnectionError",
        "ReadTimeoutError",
    }
    return None, type(exc).__name__ in retryable_names


def _parse_json(text: str) -> dict[str, Any] | None:
    s = (text or "").strip()
    if not s:
        return None
    if s.startswith("```"):
        lines = s.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    if "{" in s and "}" in s:
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            s = s[start : end + 1]
    try:
        value = json.loads(s)
    except json.JSONDecodeError:
        return None
    if isinstance(value, dict):
        return value
    return None


OpenRouterClient = LLMClient
