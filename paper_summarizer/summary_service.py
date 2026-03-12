from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from paper_extractor.schema import NOT_AVAILABLE


SUMMARY_ROOT_DIR = Path("output") / "summaries"
SUMMARY_INGREDIENTS_DIR = SUMMARY_ROOT_DIR / "ingredients"
CONSOLIDATED_SUMMARY_DIR = SUMMARY_ROOT_DIR / "consolidated_summaries"
SUMMARY_CACHE_DIR = Path("paper_summarizer") / "cache"
SUMMARY_WORD_CAP_DEFAULT = 10000


def _read_prompt(path: Path) -> str | None:
    try:
        if path.is_file():
            return path.read_text(encoding="utf-8")
    except OSError:
        return None
    return None


def build_summary_system_prompt(*, prompts_dir: Path, min_sentences: int, word_cap: int) -> str:
    template = _read_prompt(prompts_dir / "paper_summary_system.txt")
    if template:
        return (
            template.format(
                min_sentences=int(min_sentences),
                target_max_sentences=max(int(min_sentences) + 30, 140),
                word_cap=int(word_cap),
            ).strip()
            + "\n"
        )
    return (
        "You are a scientific literature analyst.\n"
        "Write a detailed full-paper summary in English.\n"
        "Rules:\n"
        "- English only.\n"
        "- Use ONLY information explicitly present in the provided paper text (no inference).\n"
        f"- Must be at least {int(min_sentences)} complete sentences.\n"
        f"- Hard cap: {int(word_cap)} words.\n"
        "- Output plain text only.\n"
    )


def build_summary_user_prompt(*, prompts_dir: Path, paper_text: str) -> str:
    template = _read_prompt(prompts_dir / "paper_summary_user.txt")
    if template:
        return template.format(paper_text=paper_text).strip() + "\n"
    return f"Paper text:\n{paper_text}\n"


def truncate_to_words(text: str, *, max_words: int) -> tuple[str, bool]:
    words = (text or "").split()
    if len(words) <= max_words:
        return (text or "").strip(), False
    return " ".join(words[:max_words]).strip(), True


def safe_dirname(name: str | None, max_len: int = 80) -> str:
    import re

    s = (name or "").strip()
    s = re.sub(r"[^\w\-\.\(\) ]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return (s[:max_len] or "unknown_ingredient")


def summary_text_path(*, ingredient: str | None, ref_number: int, sha256: str) -> Path:
    ingredient_dir = safe_dirname(ingredient)
    return SUMMARY_INGREDIENTS_DIR / ingredient_dir / f"ref_{ref_number}__{sha256[:12]}.txt"


def summary_cache_path(*, sha256: str) -> Path:
    return SUMMARY_CACHE_DIR / f"{sha256}.json"


def consolidated_summary_path(*, ingredient: str | None, output_root: Path | None = None) -> Path:
    ingredient_dir = safe_dirname(ingredient)
    root = output_root or CONSOLIDATED_SUMMARY_DIR
    return root / f"consolidated_{ingredient_dir}.txt"


def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    token = next(tempfile._get_candidate_names())
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{token}.tmp")
    tmp.write_text(text, encoding=encoding)
    tmp.replace(path)


def _atomic_write_json(path: Path, payload: dict, *, encoding: str = "utf-8") -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2), encoding=encoding)


def load_cached_summary(*, sha256: str) -> str | None:
    path = summary_cache_path(sha256=sha256)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if "summary_text" not in payload:
        return None
    return str(payload.get("summary_text") or "").strip() or NOT_AVAILABLE


def write_summary_cache(
    *,
    sha256: str,
    relative_path: str,
    model_summary: str,
    summary_text: str,
) -> None:
    _atomic_write_json(
        summary_cache_path(sha256=sha256),
        {
            "sha256": sha256,
            "relative_path": relative_path,
            "model_summary": model_summary,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "summary_text": summary_text,
        },
        encoding="utf-8",
    )


def write_summary_file(
    *,
    ingredient: str | None,
    ref_number: int,
    relative_path: str,
    sha256: str,
    model_summary: str,
    summary_text: str,
    word_cap: int,
) -> Path:
    cleaned = str(summary_text or "").strip() or NOT_AVAILABLE
    cleaned, _ = truncate_to_words(cleaned, max_words=int(word_cap))
    processed_at = datetime.now(timezone.utc).isoformat()
    body = (
        f"Ref #: {ref_number}\n"
        f"Relative PDF Path: {relative_path}\n"
        f"SHA256: {sha256}\n"
        f"Model: {model_summary}\n"
        f"Processed At (ISO): {processed_at}\n\n"
        f"{cleaned}\n"
    )
    out_path = summary_text_path(ingredient=ingredient, ref_number=ref_number, sha256=sha256)
    _atomic_write_text(out_path, body, encoding="utf-8")
    return out_path
