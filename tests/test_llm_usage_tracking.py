from __future__ import annotations

import json

from helpers.llm_openrouter import LLMUsage, LLMUsageTracker


def test_llm_usage_merged_derives_total_from_input_and_output() -> None:
    merged = LLMUsage(input_tokens=10, output_tokens=4).merged(LLMUsage(input_tokens=7, output_tokens=3))

    assert merged.input_tokens == 17
    assert merged.output_tokens == 7
    assert merged.total_tokens == 24


def test_llm_usage_tracker_appends_jsonl(tmp_path) -> None:
    tracker = LLMUsageTracker(run_name="unit_test")
    tracker.record(model="model-a", usage=LLMUsage(input_tokens=11, output_tokens=5))
    tracker.record(model="model-a", usage=LLMUsage())
    tracker.record(model="model-b", usage=LLMUsage(total_tokens=9))

    out_path = tracker.append_jsonl(tmp_path / "llm_usage.jsonl")

    payload = json.loads(out_path.read_text(encoding="utf-8").strip())
    assert payload["run_name"] == "unit_test"
    assert payload["calls"] == 3
    assert payload["calls_without_usage"] == 1
    assert payload["input_tokens"] == 11
    assert payload["output_tokens"] == 5
    assert payload["total_tokens"] == 25
    assert payload["by_model"] == [
        {
            "model": "model-a",
            "calls": 2,
            "calls_without_usage": 1,
            "input_tokens": 11,
            "output_tokens": 5,
            "total_tokens": 16,
        },
        {
            "model": "model-b",
            "calls": 1,
            "calls_without_usage": 0,
            "input_tokens": None,
            "output_tokens": None,
            "total_tokens": 9,
        },
    ]
