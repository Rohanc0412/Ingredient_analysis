from __future__ import annotations

import json
import sys
from pathlib import Path

from helpers.logging_utils import get_logger
from helpers.pdf_text_extract import ExtractedText, PdfTextExtractError, extract_text_with_page_markers

logger = get_logger(__name__, prefix="[ Extract Worker: ]", stderr=True)


def _usage() -> str:
    return "Usage: python -m paper_extractor.pdf_extract_worker <pdf_path> <output_text_path>"


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    if len(args) != 2:
        logger.error(_usage())
        return 2

    pdf_path = Path(args[0]).expanduser().resolve()
    out_path = Path(args[1]).expanduser().resolve()

    try:
        extracted: ExtractedText = extract_text_with_page_markers(pdf_path)
    except PdfTextExtractError as e:
        logger.error("%s", e)
        return 3
    except Exception as e:
        logger.error("Unexpected error extracting PDF text: %s: %s", type(e).__name__, e)
        return 4

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(extracted.text, encoding="utf-8", errors="replace")

    meta = {"chars": extracted.chars, "cjk_chars": extracted.cjk_chars}
    sys.stdout.write(json.dumps(meta, ensure_ascii=False))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
