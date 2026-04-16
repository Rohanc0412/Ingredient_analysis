from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any


def resolve_metadata_root(storage_root: Path, *, include_storage_name: bool = False) -> Path:
    base = storage_root.parent / "pdf_metadata"
    if include_storage_name:
        return base / storage_root.name
    return base


def pdf_metadata_path(*, pdf_root: Path, pdf_path: Path, metadata_root: Path | None = None) -> Path:
    metadata_root = metadata_root or resolve_metadata_root(pdf_root)
    rel = pdf_path.relative_to(pdf_root)
    suffix = pdf_path.suffix or ".pdf"
    return metadata_root / rel.with_suffix(suffix + ".metadata.json")


def load_pdf_metadata(*, pdf_root: Path, pdf_path: Path, metadata_root: Path | None = None) -> dict[str, Any]:
    path = pdf_metadata_path(pdf_root=pdf_root, pdf_path=pdf_path, metadata_root=metadata_root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_pdf_metadata(
    *,
    pdf_root: Path,
    pdf_path: Path,
    payload: dict[str, Any],
    metadata_root: Path | None = None,
) -> Path:
    path = pdf_metadata_path(pdf_root=pdf_root, pdf_path=pdf_path, metadata_root=metadata_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    token = next(tempfile._get_candidate_names())
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{token}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    return path


def canonicalize_pdf_source_key(value: object) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    text = re.sub(r"\s+", "_", text)
    return text or None


def normalize_pdf_source_label(value: object) -> str | None:
    key = canonicalize_pdf_source_key(value)
    if not key or key in {"unknown", "unknown_source"}:
        return None
    if key == "china":
        return "china article"
    if key == "google_scholar":
        return "google scholar"
    if key == "pubmed":
        return "pubmed"
    return key.replace("_", " ").strip() or None


def infer_pdf_source_key(pdf_root: Path, pdf_path: Path) -> str | None:
    metadata = load_pdf_metadata(pdf_root=pdf_root, pdf_path=pdf_path)
    for field in ("source_key", "source", "source_preference"):
        key = canonicalize_pdf_source_key(metadata.get(field))
        if key and key not in {"unknown", "unknown_source"}:
            return key
    return None
