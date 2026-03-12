from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from helpers.logging_utils import get_logger
from . import summary_service

logger = get_logger(__name__, prefix="[ Consolidator: ]")


def _configure_stdout_utf8():
    return


def safe_print(msg: str):
    logger.info("%s", msg)


def discover_ingredient_dirs(ingredients_root: Path) -> list[Path]:
    return sorted([p for p in ingredients_root.iterdir() if p.is_dir()], key=lambda p: p.name.lower())


def discover_summary_files(ingredient_dir: Path) -> list[Path]:
    return sorted([p for p in ingredient_dir.iterdir() if p.is_file() and p.suffix.lower() == ".txt"], key=lambda p: p.name.lower())


def build_consolidated_text(*, ingredient_dir: Path, ingredients_root: Path, summary_paths: list[Path]) -> str:
    ingredient_name = ingredient_dir.name
    generated_at = datetime.now(timezone.utc).isoformat()
    lines: list[str] = [
        f"Ingredient: {ingredient_name}",
        f"Generated At (ISO): {generated_at}",
        f"Source Directory: {ingredient_dir.relative_to(ingredients_root.parent)}",
        f"Summary File Count: {len(summary_paths)}",
        "",
    ]

    for idx, summary_path in enumerate(summary_paths, start=1):
        rel_path = summary_path.relative_to(ingredients_root.parent)
        content = summary_path.read_text(encoding="utf-8", errors="replace").rstrip()
        lines.extend(
            [
                "=" * 80,
                f"Section {idx}",
                f"Source Summary Filename: {summary_path.name}",
                f"Source Summary Path: {rel_path}",
                "=" * 80,
                "",
                content,
                "",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    _configure_stdout_utf8()

    parser = argparse.ArgumentParser(prog="paper_summarizer.consolidate_summaries")
    parser.add_argument(
        "--ingredients-root",
        default=str(summary_service.SUMMARY_INGREDIENTS_DIR),
        help="Directory containing ingredient summary subfolders (default: output/summaries/ingredients).",
    )
    parser.add_argument(
        "--output-root",
        default=str(summary_service.CONSOLIDATED_SUMMARY_DIR),
        help="Directory where consolidated files will be written (default: output/summaries/consolidated_summaries).",
    )
    parser.add_argument("--limit", type=int, default=0, help="Process only the first N ingredient folders (0 = no limit).")
    parser.add_argument("--dry-run", action="store_true", help="Show planned actions without writing files.")
    args = parser.parse_args(argv)

    ingredients_root = Path(args.ingredients_root).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()

    if not ingredients_root.exists() or not ingredients_root.is_dir():
        safe_print(f"Error: --ingredients-root must be an existing directory: {ingredients_root}")
        return 2

    ingredient_dirs = discover_ingredient_dirs(ingredients_root)
    if args.limit and args.limit > 0:
        ingredient_dirs = ingredient_dirs[: args.limit]

    safe_print(f"Scanning ingredient summaries in: {ingredients_root}")

    processed_count = 0
    skipped_count = 0
    skipped_names: list[str] = []

    for ingredient_dir in ingredient_dirs:
        summary_paths = discover_summary_files(ingredient_dir)
        ingredient_name = ingredient_dir.name
        if not summary_paths:
            safe_print(f"[SKIP] {ingredient_name} - found 0 summary text files.")
            skipped_count += 1
            skipped_names.append(ingredient_name)
            continue

        safe_print(f"[FOUND] {ingredient_name} - found {len(summary_paths)} summary text file(s).")
        out_path = summary_service.consolidated_summary_path(ingredient=ingredient_name, output_root=output_root)

        if args.dry_run:
            safe_print(f"[DRY RUN] Would write consolidated summary to: {out_path}")
            processed_count += 1
            continue

        text = build_consolidated_text(
            ingredient_dir=ingredient_dir,
            ingredients_root=ingredients_root,
            summary_paths=summary_paths,
        )
        summary_service._atomic_write_text(out_path, text, encoding="utf-8")
        safe_print(f"[WROTE] {ingredient_name} - consolidated summary saved to: {out_path}")
        processed_count += 1

    summary_line = f"Completed consolidation. Processed {processed_count} ingredient folder(s); skipped {skipped_count}."
    if skipped_names:
        summary_line += f" Skipped: {', '.join(skipped_names)}"
    safe_print(summary_line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
