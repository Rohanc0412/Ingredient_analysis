from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from helpers.env import load_dotenv
from helpers.file_discovery import sorted_rglob_files
from helpers.logging_utils import get_logger
from helpers.pdf_metadata import infer_pdf_source_key, pdf_metadata_path, resolve_metadata_root


DEFAULT_PDF_ROOT = Path("input") / "pdfs"
DEFAULT_QUARANTINE_ROOT = Path("output") / "quarantine" / "pdf_dedupe_quarantine"
DEFAULT_REPORT_ROOT = Path("output") / "quarantine" / "pdf_dedupe_reports"
SOURCE_PRIORITY = {
    "pubmed": 0,
    "china": 1,
    "google_scholar": 2,
}
logger = get_logger(__name__, prefix="[ Dedupe: ]")


def _configure_stdout_utf8() -> None:
    return


def safe_print(msg: str) -> None:
    logger.info("%s", msg)


def format_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_duration(total_seconds: float) -> str:
    total_seconds = max(0, int(round(total_seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def format_log(message: str) -> str:
    return f"[ Dedupe: ] {message}"


def discover_pdfs(pdf_root: Path) -> list[Path]:
    return sorted_rglob_files(pdf_root, "*.pdf")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def detect_source_name(pdf_root: Path, pdf_path: Path) -> str:
    return infer_pdf_source_key(pdf_root, pdf_path) or ""


def canonical_sort_key(pdf_root: Path, pdf_path: Path) -> tuple[int, str]:
    source_name = detect_source_name(pdf_root, pdf_path)
    rel = str(pdf_path.relative_to(pdf_root)).replace("\\", "/").lower()
    return (SOURCE_PRIORITY.get(source_name, 99), rel)


@dataclass(frozen=True)
class DuplicateGroup:
    sha256: str
    file_size: int
    kept_path: Path
    duplicate_paths: tuple[Path, ...]


def find_duplicate_groups(pdf_root: Path, pdf_paths: list[Path]) -> list[DuplicateGroup]:
    by_hash: dict[str, list[Path]] = {}
    for path in pdf_paths:
        digest = sha256_file(path)
        by_hash.setdefault(digest, []).append(path)

    groups: list[DuplicateGroup] = []
    for sha256, paths in sorted(by_hash.items()):
        if len(paths) <= 1:
            continue
        ordered = sorted(paths, key=lambda p: canonical_sort_key(pdf_root, p))
        kept_path = ordered[0]
        duplicate_paths = tuple(ordered[1:])
        groups.append(
            DuplicateGroup(
                sha256=sha256,
                file_size=int(kept_path.stat().st_size),
                kept_path=kept_path,
                duplicate_paths=duplicate_paths,
            )
        )
    return groups


def build_session_name(now: datetime) -> str:
    return now.strftime("%Y%m%d_%H%M%S")


def build_report(
    *,
    started_at: datetime,
    ended_at: datetime,
    elapsed_seconds: float,
    pdf_root: Path,
    dry_run: bool,
    quarantine_dir: Path | None,
    report_json: Path,
    scanned_files: int,
    unique_hashes: int,
    duplicate_groups: list[DuplicateGroup],
) -> dict:
    moved_files = sum(len(group.duplicate_paths) for group in duplicate_groups)
    reclaimed_bytes = sum(group.file_size * len(group.duplicate_paths) for group in duplicate_groups)
    return {
        "run": {
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "elapsed_seconds": round(float(elapsed_seconds), 3),
            "elapsed_hms": format_duration(elapsed_seconds),
            "pdf_root": str(pdf_root),
            "dry_run": bool(dry_run),
            "quarantine_dir": str(quarantine_dir) if quarantine_dir else None,
            "report_json": str(report_json),
        },
        "totals": {
            "scanned_files": int(scanned_files),
            "unique_hashes": int(unique_hashes),
            "duplicate_groups": len(duplicate_groups),
            "moved_files": int(moved_files),
            "reclaimed_bytes": int(reclaimed_bytes),
        },
        "groups": [
            {
                "sha256": group.sha256,
                "file_size": int(group.file_size),
                "kept_relative_path": str(group.kept_path.relative_to(pdf_root)).replace("\\", "/"),
                "moved_relative_paths": [
                    str(path.relative_to(pdf_root)).replace("\\", "/") for path in group.duplicate_paths
                ],
            }
            for group in duplicate_groups
        ],
    }


def write_report(report_json: Path, payload: dict) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def quarantine_duplicates(*, pdf_root: Path, quarantine_dir: Path, duplicate_groups: list[DuplicateGroup], dry_run: bool) -> None:
    if dry_run:
        return
    quarantine_metadata_root = resolve_metadata_root(quarantine_dir, include_storage_name=True)
    for group in duplicate_groups:
        for path in group.duplicate_paths:
            rel = path.relative_to(pdf_root)
            dest = quarantine_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(dest))
            meta_src = pdf_metadata_path(pdf_root=pdf_root, pdf_path=path)
            if meta_src.exists():
                meta_dest = pdf_metadata_path(pdf_root=quarantine_dir, pdf_path=dest, metadata_root=quarantine_metadata_root)
                meta_dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(meta_src), str(meta_dest))


def main(argv: list[str] | None = None) -> int:
    started_at = datetime.now()
    started_perf = time.perf_counter()
    _configure_stdout_utf8()
    load_dotenv(Path(".env"), override=False)
    safe_print(f"Pipeline started at {format_timestamp(started_at)}")

    try:
        parser = argparse.ArgumentParser(prog="paper_deduper")
        parser.add_argument(
            "--pdf-root",
            default=str(DEFAULT_PDF_ROOT),
            help="Root directory containing PDFs to deduplicate (default: input/pdfs).",
        )
        parser.add_argument(
            "--quarantine-root",
            default=str(DEFAULT_QUARANTINE_ROOT),
            help="Root directory where duplicates will be moved (default: output/quarantine/pdf_dedupe_quarantine).",
        )
        parser.add_argument(
            "--report-json",
            default="",
            help="Path for the JSON report file (default: output/quarantine/pdf_dedupe_reports/dedupe_<timestamp>.json).",
        )
        parser.add_argument("--dry-run", action="store_true", help="Report duplicate moves without changing files.")
        args = parser.parse_args(argv)

        pdf_root = Path(args.pdf_root).expanduser().resolve()
        quarantine_root = Path(args.quarantine_root).expanduser().resolve()
        session_name = build_session_name(started_at)
        quarantine_dir = quarantine_root / session_name
        report_json = (
            Path(args.report_json).expanduser().resolve()
            if (args.report_json or "").strip()
            else (DEFAULT_REPORT_ROOT / f"dedupe_{session_name}.json").resolve()
        )

        if not pdf_root.exists() or not pdf_root.is_dir():
            safe_print(f"Error: --pdf-root must be an existing directory: {pdf_root}")
            return 2

        pdf_paths = discover_pdfs(pdf_root)
        scanned_files = len(pdf_paths)
        safe_print(f"Found {scanned_files} PDF(s) under {pdf_root}.")
        if scanned_files == 0:
            ended_at = datetime.now()
            report = build_report(
                started_at=started_at,
                ended_at=ended_at,
                elapsed_seconds=time.perf_counter() - started_perf,
                pdf_root=pdf_root,
                dry_run=bool(args.dry_run),
                quarantine_dir=None if args.dry_run else quarantine_dir,
                report_json=report_json,
                scanned_files=0,
                unique_hashes=0,
                duplicate_groups=[],
            )
            write_report(report_json, report)
            safe_print(f"Wrote report: {report_json}")
            return 0

        by_hash: dict[str, list[Path]] = {}
        for path in pdf_paths:
            digest = sha256_file(path)
            by_hash.setdefault(digest, []).append(path)
        duplicate_groups = find_duplicate_groups(pdf_root, pdf_paths)

        if duplicate_groups:
            total_groups = len(duplicate_groups)
            for idx_group, group in enumerate(duplicate_groups, start=1):
                kept_rel = str(group.kept_path.relative_to(pdf_root)).replace("\\", "/")
                safe_print("")
                safe_print(
                    f"Duplicate group {idx_group}/{total_groups}: sha256={group.sha256[:12]}..., "
                    f"keep={kept_rel}, duplicates={len(group.duplicate_paths)}"
                )
                for dup in group.duplicate_paths:
                    dup_rel = str(dup.relative_to(pdf_root)).replace("\\", "/")
                    action = "Would move duplicate" if args.dry_run else "Moving duplicate"
                    safe_print(f"{action}: {dup_rel}")
            quarantine_duplicates(
                pdf_root=pdf_root,
                quarantine_dir=quarantine_dir,
                duplicate_groups=duplicate_groups,
                dry_run=bool(args.dry_run),
            )
        else:
            safe_print("No duplicate PDFs found.")

        ended_at = datetime.now()
        elapsed_seconds = time.perf_counter() - started_perf
        report = build_report(
            started_at=started_at,
            ended_at=ended_at,
            elapsed_seconds=elapsed_seconds,
            pdf_root=pdf_root,
            dry_run=bool(args.dry_run),
            quarantine_dir=None if args.dry_run else quarantine_dir,
            report_json=report_json,
            scanned_files=scanned_files,
            unique_hashes=len(by_hash),
            duplicate_groups=duplicate_groups,
        )
        write_report(report_json, report)

        totals = report["totals"]
        if args.dry_run:
            safe_print("Dry run only. No files were moved.")
        else:
            safe_print(f"Quarantine directory: {quarantine_dir}")
        safe_print(f"Wrote report: {report_json}")
        safe_print(
            "Dedupe summary: "
            f"scanned={totals['scanned_files']}, "
            f"unique_hashes={totals['unique_hashes']}, "
            f"duplicate_groups={totals['duplicate_groups']}, "
            f"moved_files={totals['moved_files']}, "
            f"bytes_reclaimed={totals['reclaimed_bytes']}"
        )
        return 0
    finally:
        ended_at = datetime.now()
        elapsed = time.perf_counter() - started_perf
        safe_print(f"Pipeline ended at {format_timestamp(ended_at)}")
        safe_print(f"Total pipeline time: {format_duration(elapsed)}")
