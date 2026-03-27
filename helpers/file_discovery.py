from __future__ import annotations

from pathlib import Path


def is_ignored_repo_file(path: Path) -> bool:
    return path.name == ".gitkeep"


def sorted_glob_files(root: Path, pattern: str) -> list[Path]:
    return sorted(
        [path for path in root.glob(pattern) if path.is_file() and not is_ignored_repo_file(path)],
        key=lambda p: p.name.lower(),
    )


def sorted_rglob_files(root: Path, pattern: str) -> list[Path]:
    return sorted(
        [path for path in root.rglob(pattern) if path.is_file() and not is_ignored_repo_file(path)],
        key=lambda p: str(p).lower(),
    )
