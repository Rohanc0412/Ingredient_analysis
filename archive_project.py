"""
archive_project.py

Archives all input/ and output/ contents under a named project folder,
then clears input/ and output/ (preserving .gitkeep files).

Usage:
    python archive_project.py --project <name> [--archive-dir <path>] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent

from helpers.env import load_dotenv  # noqa: E402 — must come after ROOT is set

load_dotenv(ROOT / ".env")

INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"


# ---------------------------------------------------------------------------
# Resolution helpers
# ---------------------------------------------------------------------------

def resolve_archive_root(cli_path: str | None) -> Path:
    """Return the archive root directory, prompting the user if not configured."""
    if cli_path:
        return Path(cli_path).expanduser().resolve()

    env_path = os.environ.get("ARCHIVE_ROOT_DIR", "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()

    print("\nARCHIVE_ROOT_DIR is not set in .env and --archive-dir was not provided.")
    while True:
        raw = input("Enter the archive root directory path: ").strip()
        if raw:
            return Path(raw).expanduser().resolve()
        print("  Path cannot be empty. Please try again.")


def resolve_project_name(cli_name: str | None) -> str:
    """Return the project name, prompting the user if not provided."""
    if cli_name and cli_name.strip():
        return cli_name.strip()

    while True:
        raw = input("Enter a project name for this archive: ").strip()
        if raw:
            return raw
        print("  Project name cannot be empty. Please try again.")


def make_archive_dest(archive_root: Path, project_name: str) -> Path:
    """
    Return the archive destination path.
    If <archive_root>/<project_name> already exists, a _YYYYMMDD_HHMMSS suffix
    is appended automatically.
    """
    dest = archive_root / project_name
    if dest.exists():
        suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_dest = archive_root / f"{project_name}_{suffix}"
        print(f"\n  '{dest}' already exists — using '{new_dest.name}' instead.")
        return new_dest
    return dest


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------

def collect_files(src: Path) -> list[tuple[Path, Path]]:
    """
    Walk src and return (absolute_path, relative_path) pairs for all files,
    excluding .gitkeep.
    """
    if not src.exists():
        return []
    return [
        (f, f.relative_to(src))
        for f in sorted(src.rglob("*"))
        if f.is_file() and f.name != ".gitkeep"
    ]


def copy_tree(src: Path, dest_base: Path, label: str, dry_run: bool) -> int:
    """Copy all non-.gitkeep files from src into dest_base/label/."""
    pairs = collect_files(src)
    dest_dir = dest_base / label

    for abs_path, rel_path in pairs:
        target = dest_dir / rel_path
        if dry_run:
            print(f"    copy  {abs_path.relative_to(ROOT)}  →  {target}")
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(abs_path, target)

    return len(pairs)


def clear_tree(src: Path, dry_run: bool) -> int:
    """
    Delete every non-.gitkeep file under src.
    Subdirectory structure and .gitkeep files are preserved.
    """
    if not src.exists():
        return 0

    deleted = 0
    for item in sorted(src.rglob("*")):
        if item.is_file() and item.name != ".gitkeep":
            if dry_run:
                print(f"    delete  {item.relative_to(ROOT)}")
            else:
                item.unlink()
            deleted += 1
    return deleted


def write_manifest(dest: Path, project_name: str, input_count: int, output_count: int, dry_run: bool) -> None:
    manifest = {
        "project_name": project_name,
        "archived_at": datetime.now().isoformat(timespec="seconds"),
        "source_directory": str(ROOT),
        "files_archived": {
            "input": input_count,
            "output": output_count,
            "total": input_count + output_count,
        },
    }
    manifest_path = dest / "manifest.json"
    if dry_run:
        print(f"\n  manifest would be written to: {manifest_path}")
        print("  " + json.dumps(manifest, indent=2).replace("\n", "\n  "))
    else:
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Archive pipeline inputs/outputs and reset the repo to a clean state."
    )
    parser.add_argument("--project", metavar="NAME", help="Project name for the archive folder.")
    parser.add_argument("--archive-dir", metavar="PATH", help="Archive root directory (overrides ARCHIVE_ROOT_DIR in .env).")
    parser.add_argument("--dry-run", action="store_true", help="Preview what would happen without making any changes.")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip the confirmation prompt before clearing.")
    args = parser.parse_args()

    if args.dry_run:
        print("=" * 60)
        print("DRY RUN — no files will be moved or deleted")
        print("=" * 60)

    archive_root = resolve_archive_root(args.archive_dir)
    project_name = resolve_project_name(args.project)
    dest = make_archive_dest(archive_root, project_name)

    print(f"\nProject name  : {project_name}")
    print(f"Archive root  : {archive_root}")
    print(f"Destination   : {dest}")

    # --- Count files that will be archived ---
    input_files = collect_files(INPUT_DIR)
    output_files = collect_files(OUTPUT_DIR)
    total = len(input_files) + len(output_files)

    print(f"\nFiles to archive: {len(input_files)} from input/, {len(output_files)} from output/ ({total} total)")

    if total == 0:
        print("\nNothing to archive — both input/ and output/ are already empty (only .gitkeep present).")
        sys.exit(0)

    # --- Confirmation ---
    if not args.dry_run and not args.yes:
        print()
        confirm = input(f"This will copy {total} file(s) to '{dest}' and then delete them from input/ and output/.\nProceed? [y/N] ").strip().lower()
        if confirm not in ("y", "yes"):
            print("Aborted.")
            sys.exit(0)

    # --- Archive ---
    print("\nArchiving input/ ...")
    if args.dry_run:
        for f, _ in input_files:
            print(f"    copy  {f.relative_to(ROOT)}")
    else:
        dest.mkdir(parents=True, exist_ok=True)
        copy_tree(INPUT_DIR, dest, "input", dry_run=False)
    print(f"  {len(input_files)} file(s) copied.")

    print("\nArchiving output/ ...")
    if args.dry_run:
        for f, _ in output_files:
            print(f"    copy  {f.relative_to(ROOT)}")
    else:
        copy_tree(OUTPUT_DIR, dest, "output", dry_run=False)
    print(f"  {len(output_files)} file(s) copied.")

    write_manifest(dest, project_name, len(input_files), len(output_files), args.dry_run)

    # --- Clear ---
    print("\nClearing input/ ...")
    deleted_input = clear_tree(INPUT_DIR, args.dry_run)
    print(f"  {deleted_input} file(s) removed.")

    print("\nClearing output/ ...")
    deleted_output = clear_tree(OUTPUT_DIR, args.dry_run)
    print(f"  {deleted_output} file(s) removed.")

    # --- Done ---
    print()
    if args.dry_run:
        print("Dry run complete. No changes were made.")
    else:
        print(f"Done. Archive saved to: {dest}")
        print("Repo is now in a clean state.")


if __name__ == "__main__":
    main()
