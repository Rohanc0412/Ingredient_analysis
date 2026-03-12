from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment


logger = logging.getLogger(__name__)
LOG_PREFIX = "[ Excel Layout: ]"


@dataclass(frozen=True)
class WorkbookContext:
    path: Path
    wb: Any
    review_sheets: dict[str, Any]
    headers: list[str]
    header_row_idx: int


FILE_INDEX_SHEET = "File Index"
PAPER_SHEET = "Paper Extraction"
PDF_SOURCE_HEADER = "pdf_source"
DEFAULT_PDF_SOURCE_AFTER = "Journal / Source"
LIT_REVIEW_ALL_SHEET = "Lit_Review_All"
LIT_REVIEW_CHINA_SHEET = "Lit_Review_China"
LIT_REVIEW_GOOGLE_SHEET = "Lit_Review_Google_scholar"
LIT_REVIEW_PUBMED_SHEET = "Lit_Review_Pubmed"
REVIEW_SHEET_NAMES = (
    LIT_REVIEW_ALL_SHEET,
    LIT_REVIEW_CHINA_SHEET,
    LIT_REVIEW_GOOGLE_SHEET,
    LIT_REVIEW_PUBMED_SHEET,
)
PDF_SOURCE_TO_REVIEW_SHEET = {
    "china article": LIT_REVIEW_CHINA_SHEET,
    "google scholar": LIT_REVIEW_GOOGLE_SHEET,
    "pubmed": LIT_REVIEW_PUBMED_SHEET,
}


def _normalize_header_name(value: object) -> str:
    s = str(value).strip() if value is not None else ""
    if s == "SL No":
        return "Ref #"
    return s


def _find_paper_header_row(ws, *, max_scan_rows: int = 10) -> int:
    best_row = 1
    best_score = -1
    for row_idx in range(1, min(ws.max_row, max_scan_rows) + 1):
        values = [c.value for c in next(ws.iter_rows(min_row=row_idx, max_row=row_idx))]
        headers = {_normalize_header_name(v) for v in values if v is not None and str(v).strip()}
        score = sum(
            1
            for required in ("Ref #", "Title", "Journal / Source", "Primary Ingredient")
            if required in headers
        )
        if score > best_score:
            best_score = score
            best_row = row_idx
    return best_row


def load_workbook_context(xlsx_path: Path) -> WorkbookContext:
    wb = openpyxl.load_workbook(xlsx_path)
    review_sheets = ensure_review_sheets(wb)
    ws = review_sheets[LIT_REVIEW_ALL_SHEET]
    header_row_idx = _find_paper_header_row(ws)
    for review_ws in review_sheets.values():
        ensure_column(review_ws, PDF_SOURCE_HEADER, after_header=DEFAULT_PDF_SOURCE_AFTER, header_row_idx=header_row_idx)
    header_row = [c.value for c in next(ws.iter_rows(min_row=header_row_idx, max_row=header_row_idx))]
    headers = [_normalize_header_name(h) for h in header_row if h is not None and str(h).strip()]
    if not headers:
        raise RuntimeError(f"Sheet {LIT_REVIEW_ALL_SHEET} has no detectable headers.")
    return WorkbookContext(path=xlsx_path, wb=wb, review_sheets=review_sheets, headers=headers, header_row_idx=header_row_idx)


def ensure_review_sheets(wb) -> dict[str, Any]:
    template_ws = None

    if LIT_REVIEW_ALL_SHEET in wb.sheetnames:
        template_ws = wb[LIT_REVIEW_ALL_SHEET]
    elif PAPER_SHEET in wb.sheetnames:
        template_ws = wb[PAPER_SHEET]
        template_ws.title = LIT_REVIEW_ALL_SHEET
    else:
        raise RuntimeError(
            f"Workbook is missing required template/review sheet: {PAPER_SHEET} or {LIT_REVIEW_ALL_SHEET}"
        )

    for sheet_name in REVIEW_SHEET_NAMES[1:]:
        if sheet_name not in wb.sheetnames:
            new_ws = wb.copy_worksheet(template_ws)
            new_ws.title = sheet_name

    if PAPER_SHEET in wb.sheetnames:
        del wb[PAPER_SHEET]

    return {sheet_name: wb[sheet_name] for sheet_name in REVIEW_SHEET_NAMES}


def write_timestamped_copy(xlsx_path: Path) -> Path:
    ts = datetime.now().strftime("%m%d_%H%M")
    versioned_path = xlsx_path.with_name(f"{xlsx_path.stem}.{ts}{xlsx_path.suffix}")
    shutil.copy2(xlsx_path, versioned_path)
    return versioned_path
def ensure_file_index_sheet(wb) -> Any:
    if FILE_INDEX_SHEET in wb.sheetnames:
        ws = wb[FILE_INDEX_SHEET]
        ensure_column(ws, PDF_SOURCE_HEADER, after_header=None)
        return ws
    ws = wb.create_sheet(FILE_INDEX_SHEET)
    ws.append(["Ref #", "Relative PDF Path", "SHA256", "Chars Extracted", "Model", "Processed At (ISO)", PDF_SOURCE_HEADER])
    return ws


def ensure_column(ws, header_name: str, *, after_header: str | None, header_row_idx: int = 1) -> None:
    """
    Ensure a header exists on row 1. If missing, insert a new column.

    - If after_header is provided and exists, insert immediately after it.
    - Otherwise append at the end.
    """
    header_name = str(header_name).strip()
    if not header_name:
        return

    values = [c.value for c in next(ws.iter_rows(min_row=header_row_idx, max_row=header_row_idx))]
    headers = [_normalize_header_name(v) if v is not None else "" for v in values]
    if header_name in headers:
        return

    insert_at = len(headers) + 1  # append
    if after_header:
        after_header = _normalize_header_name(after_header)
        if after_header and after_header in headers:
            insert_at = headers.index(after_header) + 2

    ws.insert_cols(insert_at, 1)
    ws.cell(row=header_row_idx, column=insert_at).value = header_name


def _find_row_by_ref(ws, ref_number: int, ref_col_index: int, *, start_row: int = 2) -> int | None:
    for row_idx in range(start_row, ws.max_row + 1):
        v = ws.cell(row=row_idx, column=ref_col_index).value
        if v is None:
            continue
        try:
            if int(v) == int(ref_number):
                return row_idx
        except Exception:
            continue
    return None


def _find_file_index_row_by_sha(file_index_ws, sha256: str) -> int | None:
    # Columns are fixed for File Index.
    sha_col = 3
    for row_idx in range(2, file_index_ws.max_row + 1):
        v = file_index_ws.cell(row=row_idx, column=sha_col).value
        if v and str(v).strip() == sha256:
            return row_idx
    return None


def _clear_row(ws, row_idx: int, *, max_col: int) -> None:
    for col_idx in range(1, max_col + 1):
        ws.cell(row=row_idx, column=col_idx).value = None


MAX_OUTPUT_COLUMN_WIDTH = 85.0


def apply_output_sheet_layout(ws, *, min_row: int = 1, max_width: float = MAX_OUTPUT_COLUMN_WIDTH, padding: float = 2.0) -> None:
    for col_idx in range(1, ws.max_column + 1):
        column_letter = get_column_letter(col_idx)
        existing_width = ws.column_dimensions[column_letter].width
        target_width = float(existing_width) if existing_width else 0.0

        for row_idx in range(min_row, ws.max_row + 1):
            value = ws.cell(row=row_idx, column=col_idx).value
            if value is None:
                continue
            text = str(value)
            if not text:
                continue
            longest_line = max(len(line) for line in text.splitlines()) if text else 0
            target_width = max(target_width, min(float(max_width), float(longest_line) + float(padding)))

        if target_width > 0:
            ws.column_dimensions[column_letter].width = target_width

    for row_idx in range(min_row, ws.max_row + 1):
        row_dim = ws.row_dimensions[row_idx]
        row_dim.height = None
        if hasattr(row_dim, "ht"):
            try:
                row_dim.ht = None
            except Exception:
                pass
        for col_idx in range(1, ws.max_column + 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            base_alignment = cell.alignment or Alignment()
            cell.alignment = copy_alignment_with_wrap(base_alignment)


def autofit_workbook_with_excel(xlsx_path: Path, *, max_width: float = MAX_OUTPUT_COLUMN_WIDTH) -> None:
    try:
        import pythoncom
        import win32com.client
    except Exception:
        logger.info("%s Excel COM autofit unavailable; skipped Excel-native autofit for %s", LOG_PREFIX, xlsx_path)
        return

    pythoncom.CoInitialize()
    excel = None
    workbook = None
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        workbook = excel.Workbooks.Open(str(xlsx_path.resolve()))

        for worksheet in workbook.Worksheets:
            used_range = worksheet.UsedRange
            used_range.WrapText = True
            used_range.Columns.AutoFit()

            for column in used_range.Columns:
                if float(column.ColumnWidth) > float(max_width):
                    column.ColumnWidth = float(max_width)

            used_range.Rows.AutoFit()

        workbook.Save()
        logger.info("%s Applied Excel-native autofit to %s", LOG_PREFIX, xlsx_path)
    except Exception as e:
        logger.warning(
            "%s Excel COM autofit failed for %s: %s: %s",
            LOG_PREFIX,
            xlsx_path,
            type(e).__name__,
            e,
        )
    finally:
        if workbook is not None:
            workbook.Close(SaveChanges=False)
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()


def copy_alignment_with_wrap(alignment: Alignment) -> Alignment:
    return Alignment(
        horizontal=alignment.horizontal,
        vertical=alignment.vertical or "top",
        textRotation=alignment.textRotation,
        wrapText=True,
        shrinkToFit=alignment.shrinkToFit,
        indent=alignment.indent,
        relativeIndent=alignment.relativeIndent,
        justifyLastLine=alignment.justifyLastLine,
        readingOrder=alignment.readingOrder,
    )


def write_paper_row(
    ctx: WorkbookContext,
    *,
    row_data: dict[str, str],
    file_index: dict[str, Any],
    overwrite_existing: bool,
):
    headers = ctx.headers
    file_ws = ensure_file_index_sheet(ctx.wb)
    all_ws = ctx.review_sheets[LIT_REVIEW_ALL_SHEET]

    # Locate Ref # column in the paper sheet
    try:
        ref_col_index = headers.index("Ref #") + 1
    except ValueError:
        ref_col_index = 1

    try:
        ref_number = int(row_data.get("Ref #") or 0) if row_data.get("Ref #") else 0
    except (TypeError, ValueError):
        ref_number = 0
    sha256 = str(file_index["sha256"])

    target_row_idx: int | None = None
    if overwrite_existing:
        existing_file_row = _find_file_index_row_by_sha(file_ws, sha256)
        if existing_file_row is not None:
            existing_ref = file_ws.cell(row=existing_file_row, column=1).value
            try:
                existing_ref_int = int(existing_ref)
            except Exception:
                existing_ref_int = ref_number
            target_row_idx = _find_row_by_ref(all_ws, existing_ref_int, ref_col_index, start_row=ctx.header_row_idx + 1)
        if target_row_idx is None and ref_number:
            target_row_idx = _find_row_by_ref(all_ws, ref_number, ref_col_index, start_row=ctx.header_row_idx + 1)

    if target_row_idx is None:
        target_row_idx = all_ws.max_row + 1

    target_sheet_names = [LIT_REVIEW_ALL_SHEET]
    source_specific_sheet = PDF_SOURCE_TO_REVIEW_SHEET.get(str(file_index.get("pdf_source", "")).strip().lower())
    if source_specific_sheet:
        target_sheet_names.append(source_specific_sheet)

    existing_ref_int = None
    if overwrite_existing:
        existing_file_row = _find_file_index_row_by_sha(file_ws, sha256)
        if existing_file_row is not None:
            existing_ref = file_ws.cell(row=existing_file_row, column=1).value
            try:
                existing_ref_int = int(existing_ref)
            except Exception:
                existing_ref_int = None
        if existing_ref_int is None and ref_number:
            existing_ref_int = ref_number

    for sheet_name in REVIEW_SHEET_NAMES:
        ws = ctx.review_sheets[sheet_name]
        if sheet_name in target_sheet_names:
            row_idx = target_row_idx if sheet_name == LIT_REVIEW_ALL_SHEET else None
            if overwrite_existing and existing_ref_int:
                row_idx = _find_row_by_ref(ws, existing_ref_int, ref_col_index, start_row=ctx.header_row_idx + 1) or row_idx
            if row_idx is None:
                row_idx = ws.max_row + 1
            for col_idx, header in enumerate(headers, start=1):
                ws.cell(row=row_idx, column=col_idx).value = row_data.get(header)
        elif overwrite_existing and existing_ref_int and sheet_name != LIT_REVIEW_ALL_SHEET:
            stale_row_idx = _find_row_by_ref(ws, existing_ref_int, ref_col_index, start_row=ctx.header_row_idx + 1)
            if stale_row_idx is not None:
                _clear_row(ws, stale_row_idx, max_col=len(headers))

    # Update File Index (append or overwrite by SHA)
    processed_at = datetime.now(timezone.utc).isoformat()
    existing_file_row = _find_file_index_row_by_sha(file_ws, sha256)
    # Find pdf_source column index if present
    file_headers = [c.value for c in next(file_ws.iter_rows(min_row=1, max_row=1))]
    file_headers_norm = [str(h).strip() if h is not None else "" for h in file_headers]
    try:
        pdf_source_col = file_headers_norm.index(PDF_SOURCE_HEADER) + 1
    except ValueError:
        pdf_source_col = None

    if existing_file_row is None:
        file_ws.append(
            [
                ref_number,
                file_index["relative_path"],
                sha256,
                file_index["chars_extracted"],
                file_index["model"],
                processed_at,
                file_index.get("pdf_source", ""),
            ]
        )
    else:
        file_ws.cell(row=existing_file_row, column=1).value = ref_number
        file_ws.cell(row=existing_file_row, column=2).value = file_index["relative_path"]
        file_ws.cell(row=existing_file_row, column=4).value = file_index["chars_extracted"]
        file_ws.cell(row=existing_file_row, column=5).value = file_index["model"]
        file_ws.cell(row=existing_file_row, column=6).value = processed_at
        if pdf_source_col is not None:
            file_ws.cell(row=existing_file_row, column=pdf_source_col).value = file_index.get("pdf_source", "")


def save_workbook(ctx: WorkbookContext):
    for review_ws in ctx.review_sheets.values():
        apply_output_sheet_layout(review_ws, min_row=1)
    file_ws = ensure_file_index_sheet(ctx.wb)
    apply_output_sheet_layout(file_ws, min_row=1)
    ctx.wb.save(ctx.path)
    autofit_workbook_with_excel(ctx.path)
    write_timestamped_copy(ctx.path)
