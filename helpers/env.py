from __future__ import annotations

from pathlib import Path


def load_dotenv(path: Path, *, override: bool = False) -> None:
    """
    Minimal .env loader (no external dependency).
    Supports lines like KEY=VALUE, optional quotes, and ignores comments/blanks.
    """
    import os

    if not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            value = value[1:-1]
        if not override and key in os.environ:
            continue
        os.environ[key] = value

