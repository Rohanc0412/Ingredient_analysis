from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from pdfminer.high_level import extract_pages, extract_text
from pdfminer.layout import LTTextContainer


CJK_RE = re.compile(r"[\u4e00-\u9fff]")


@dataclass(frozen=True)
class ExtractedText:
    text: str
    chars: int
    cjk_chars: int


class PdfTextExtractError(RuntimeError):
    pass


def _iter_page_text(pdf_path: Path) -> Iterable[str]:
    page_num = 0
    for page_layout in extract_pages(str(pdf_path)):
        page_num += 1
        parts: list[str] = []
        for element in page_layout:
            if isinstance(element, LTTextContainer):
                parts.append(element.get_text())
        yield f"--- Page {page_num} ---\n" + "".join(parts).strip() + "\n"


def extract_text_with_page_markers(pdf_path: Path) -> ExtractedText:
    try:
        raw = "\n".join(_iter_page_text(pdf_path))
    except Exception as e:
        # Some PDFs are truncated/corrupt (e.g., "Unexpected EOF") and pdfminer can fail hard.
        # Try a simpler extraction path as a fallback; if that fails too, surface a clear error
        # so the caller can skip the PDF instead of crashing the whole run.
        try:
            raw = extract_text(str(pdf_path)) or ""
            if raw.strip():
                raw = f"--- Page 1 ---\n{raw.strip()}\n"
        except Exception as e2:
            raise PdfTextExtractError(
                f"Failed to extract text from PDF: {pdf_path} ({type(e).__name__}: {e}; fallback {type(e2).__name__}: {e2})"
            ) from e2

    # Light normalization: keep markers; collapse excessive spaces elsewhere
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    chars = len(raw)
    cjk_chars = len(CJK_RE.findall(raw))
    return ExtractedText(text=raw, chars=chars, cjk_chars=cjk_chars)
