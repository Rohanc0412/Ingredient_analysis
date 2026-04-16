from __future__ import annotations

import re
from typing import Any


NOT_AVAILABLE = "Not Reported"


def normalize_headers(headers: list[str]) -> list[str]:
    cleaned: list[str] = []
    for h in headers:
        if h is None:
            continue
        hs = str(h).strip()
        if not hs:
            continue
        cleaned.append(hs)
    return cleaned


def _normalize_key(value: object) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("\u2019", "'").replace("\u2013", "-").replace("\u2014", "-")
    text = text.replace("\u00e2\u20ac\u2122", "'").replace("\u00e2\u20ac\u201d", "-").replace("\u00e2\u20ac\u201c", "-")
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def _flatten_llm_data(data: Any, *, prefix: tuple[str, ...] = ()) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    if not isinstance(data, dict):
        return flat
    for key, value in data.items():
        key_text = str(key).strip()
        if not key_text:
            continue
        current = prefix + (key_text,)
        flat[" ".join(current)] = value
        flat.update(_flatten_llm_data(value, prefix=current))
    return flat


def _build_value_lookup(data: dict[str, Any]) -> dict[str, Any]:
    lookup: dict[str, Any] = {}
    for key, value in data.items():
        normalized = _normalize_key(key)
        if normalized and normalized not in lookup:
            lookup[normalized] = value
    return lookup


def _header_tokens(header: str) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", str(header).lower()) if token]


def _get_direct_value(value_lookup: dict[str, Any], header: str) -> Any:
    normalized = _normalize_key(header)
    if normalized in value_lookup:
        return value_lookup[normalized]

    tokens = _header_tokens(header)
    if not tokens:
        return None

    for key, value in value_lookup.items():
        if all(token in key for token in tokens):
            return value
    return None


def coerce_row_values(headers: list[str], data: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for h in headers:
        v = data.get(h, NOT_AVAILABLE)
        if v is None:
            out[h] = NOT_AVAILABLE
            continue
        if isinstance(v, (int, float)):
            out[h] = str(v)
            continue
        vs = str(v).strip()
        out[h] = vs if vs else NOT_AVAILABLE
    return out


def flatten_llm_to_excel(
    headers: list[str],
    llm_data: dict[str, Any] | None,
) -> dict[str, str]:
    """
    Map LLM JSON output keys to template headers via fuzzy matching.

    No column names are hardcoded — all mapping is driven by the headers
    list read from the actual template. Non-LLM fields (ref numbers,
    ingredient fallbacks, pdf_source) are applied separately by the caller
    via apply_non_llm_fields().
    """
    row: dict[str, str] = {h: NOT_AVAILABLE for h in headers}
    source = llm_data if isinstance(llm_data, dict) else {}
    flattened = _flatten_llm_data(source)
    flattened.update(source)
    value_lookup = _build_value_lookup(flattened)

    for h in headers:
        value = _get_direct_value(value_lookup, h)
        if value is None:
            continue
        row[h] = value

    return row
