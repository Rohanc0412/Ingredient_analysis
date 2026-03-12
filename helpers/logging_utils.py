from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from helpers.env import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LOG_DIR = ROOT / "output" / "logs"


def _env_flag(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _build_formatter(prefix: str) -> logging.Formatter:
    include_timestamps = _env_flag("PROJECT_LOG_TIMESTAMPS", default=False)
    if include_timestamps:
        return logging.Formatter(f"%(asctime)s {prefix} %(message)s", datefmt="%m-%d %H:%M")
    return logging.Formatter(f"{prefix} %(message)s")


def _resolve_log_level() -> int:
    level_name = (os.environ.get("PROJECT_LOG_LEVEL") or "INFO").strip().upper()
    return getattr(logging, level_name, logging.INFO)


def _normalize_log_filename(name: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in name)
    return (safe.strip("._") or "project") + ".log"


def get_logger(name: str, *, prefix: str, stderr: bool = False) -> logging.Logger:
    load_dotenv(ROOT / ".env", override=False)
    logger = logging.getLogger(name)
    if getattr(logger, "_paper_downloader_configured", False):
        return logger

    formatter = _build_formatter(prefix)
    stream_handler = logging.StreamHandler(sys.stderr if stderr else sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if _env_flag("PROJECT_LOG_TO_FILE", default=False):
        log_dir = Path(os.environ.get("PROJECT_LOG_DIR") or DEFAULT_LOG_DIR).expanduser().resolve()
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / _normalize_log_filename(name), encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.setLevel(_resolve_log_level())
    logger.propagate = False
    logger._paper_downloader_configured = True
    return logger
