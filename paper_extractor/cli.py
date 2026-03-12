from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from helpers.env import load_dotenv
from helpers.excel_writer import load_workbook_context, save_workbook, write_paper_row
from helpers.llm_openrouter import LLMUsage, OpenRouterClient, load_llm_config
from helpers.logging_utils import get_logger
from helpers.pdf_text_extract import ExtractedText, PdfTextExtractError, extract_text_with_page_markers
from helpers.rate_limiter import RateLimiter
from .schema import NOT_AVAILABLE, coerce_row_values, flatten_llm_to_excel, normalize_headers


SCANNED_NOTE = "Not Reported"
TOO_LONG_NOTE = "Not Reported"
LLM_ERROR_NOTE = "Not Reported"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
DEFAULT_TEMPLATE_DIR = Path("input") / "templates"
DEFAULT_TEMPLATE_GLOB = "weight_management_paper_extraction_template*.xlsx"
DEFAULT_OUTPUT_XLSX = Path("output") / "paper_wise_analysis" / "paper_analysis.xlsx"
logger = get_logger(__name__, prefix="[ Extractor: ]")


KEY_FINDING_HEADERS = (
    "Key Finding Summary (4–5 sentences)",
    "Key Finding Summary (4â€“5 sentences)",
)


def _configure_stdout_utf8():
    return


def safe_print(msg: str):
    logger.info("%s", msg)


def format_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_duration(total_seconds: float) -> str:
    total_seconds = max(0, int(round(total_seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def discover_pdfs(pdf_root: Path) -> list[Path]:
    return sorted([p for p in pdf_root.rglob("*.pdf") if p.is_file()])


def _norm_rel_path(p: str) -> str:
    return str(p or "").strip().replace("\\", "/")


def _load_processed_relative_paths(wb) -> set[str]:
    """
    Returns a set of normalized relative PDF paths already present in the workbook's
    "File Index" sheet. This is used for --resume.
    """
    sheet_name = "File Index"
    if sheet_name not in getattr(wb, "sheetnames", []):
        return set()
    ws = wb[sheet_name]
    processed: set[str] = set()
    # File Index columns: 1=Ref #, 2=Relative PDF Path, 3=SHA256, ...
    for row_idx in range(2, ws.max_row + 1):
        v = ws.cell(row=row_idx, column=2).value
        if v is None:
            continue
        s = _norm_rel_path(str(v))
        if s:
            processed.add(s)
    return processed


def derive_primary_ingredient(pdf_root: Path, pdf_path: Path) -> str | None:
    try:
        rel = pdf_path.relative_to(pdf_root)
    except Exception:
        return None
    parts = rel.parts
    if len(parts) >= 2:
        return parts[0]
    return None


def derive_pdf_source_label(pdf_root: Path, pdf_path: Path) -> str:
    """
    Derive pdf_source from second-level folder under pdf_root:
    pdf_root/<ingredient>/<source>/<file>.pdf
    """
    try:
        rel = pdf_path.relative_to(pdf_root)
    except Exception:
        return NOT_AVAILABLE
    parts = rel.parts
    if len(parts) < 2:
        return NOT_AVAILABLE
    source_folder = (parts[1] or "").strip().lower()
    if not source_folder:
        return NOT_AVAILABLE
    if source_folder == "china":
        return "china article"
    if source_folder == "google_scholar":
        return "google scholar"
    if source_folder == "pubmed":
        return "pubmed"
    normalized = source_folder.replace("_", " ").strip()
    return normalized if normalized else NOT_AVAILABLE
def apply_non_llm_fields(
    row: dict[str, str],
    *,
    headers: list[str],
    ref_number: int,
    primary_ingredient: str | None,
    pdf_source: str,
):
    if "Ref #" in headers:
        row["Ref #"] = str(ref_number)
    if "Primary Ingredient" in headers and primary_ingredient:
        if row.get("Primary Ingredient") in (None, "", NOT_AVAILABLE):
            row["Primary Ingredient"] = str(primary_ingredient).strip() or NOT_AVAILABLE
    if "pdf_source" in headers:
        row["pdf_source"] = pdf_source


def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    token = next(tempfile._get_candidate_names())
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{token}.tmp")
    tmp.write_text(text, encoding=encoding)
    tmp.replace(path)


def _atomic_write_json(path: Path, payload: dict, *, encoding: str = "utf-8") -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2), encoding=encoding)


def _read_prompt(path: Path) -> str | None:
    try:
        if path.is_file():
            return path.read_text(encoding="utf-8")
    except OSError:
        return None
    return None


def build_system_prompt() -> str:
    from_file = _read_prompt(PROMPTS_DIR / "paper_extraction_system.txt")
    if from_file:
        return from_file.strip() + "\n"
    return (
        "You extract structured fields for an Excel sheet from research paper text.\n"
        "Return ONLY valid JSON.\n"
        "Do not guess or invent facts.\n"
        f"If a value is not present in the text, return exactly: {NOT_AVAILABLE}\n"
    )


def build_user_prompt(headers: list[str], paper_text: str) -> str:
    template = _read_prompt(PROMPTS_DIR / "paper_extraction_user.txt")
    if template:
        return template.format(paper_text=paper_text).strip() + "\n"
    return f"Paper text:\n{paper_text}\n"


async def extract_pdf_text(
    pdf_path: Path,
    *,
    timeout_s: float,
) -> ExtractedText:
    """
    Extract PDF text with an optional hard timeout.

    When timeout_s > 0, extraction is done in a separate Python subprocess so that
    a hung PDF parse cannot block termination.
    """

    if timeout_s <= 0:
        return await asyncio.to_thread(extract_text_with_page_markers, pdf_path)

    tmp_dir = Path(tempfile.gettempdir()) / "paper_extractor_pdf_text"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    token = next(tempfile._get_candidate_names())
    out_path = tmp_dir / f"{os.getpid()}.{token}.txt"

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "paper_extractor.pdf_extract_worker",
        str(pdf_path),
        str(out_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        with contextlib.suppress(Exception):
            await proc.communicate()
        raise PdfTextExtractError(f"PDF text extraction timed out after {timeout_s:.1f}s: {pdf_path}")

    if proc.returncode != 0:
        err = (stderr or b"").decode("utf-8", errors="replace").strip()
        if err:
            err = err[:800]
            raise PdfTextExtractError(f"Failed to extract text from PDF: {pdf_path} (worker rc={proc.returncode}): {err}")
        raise PdfTextExtractError(f"Failed to extract text from PDF: {pdf_path} (worker rc={proc.returncode})")

    meta_raw = (stdout or b"").decode("utf-8", errors="replace").strip()
    meta: dict[str, Any] = {}
    if meta_raw:
        try:
            parsed = json.loads(meta_raw)
            if isinstance(parsed, dict):
                meta = parsed
        except Exception:
            meta = {}

    text = ""
    try:
        text = out_path.read_text(encoding="utf-8", errors="replace")
    finally:
        with contextlib.suppress(Exception):
            out_path.unlink(missing_ok=True)

    chars = int(meta.get("chars") or len(text))
    cjk_chars = int(meta.get("cjk_chars") or 0)
    return ExtractedText(text=text, chars=chars, cjk_chars=cjk_chars)


def _set_key_finding_note(row: dict[str, str], note: str, headers: list[str]) -> None:
    for h in KEY_FINDING_HEADERS:
        if h in headers:
            row[h] = note
            return
def main(argv: list[str] | None = None) -> int:
    started_at = datetime.now()
    started_perf = time.perf_counter()
    safe_print(f"Pipeline started at {format_timestamp(started_at)}")

    _configure_stdout_utf8()
    load_dotenv(Path(".env"), override=False)
    try:
        def _env_int(name: str, default: int) -> int:
            raw = (os.environ.get(name) or "").strip()
            if not raw:
                return int(default)
            # Allow common human-friendly formats like "100,000" or "100_000".
            raw = raw.replace(",", "").replace("_", "").strip()
            try:
                return int(raw)
            except Exception:
                return int(default)

        parser = argparse.ArgumentParser(prog="paper_extractor")
        parser.add_argument(
            "--output-xlsx",
            default=(os.environ.get("PAPER_EXTRACT_OUTPUT_XLSX") or str(DEFAULT_OUTPUT_XLSX)),
            help="Path for the generated analysis workbook (default: output/paper_wise_analysis/paper_analysis.xlsx).",
        )
        parser.add_argument(
            "--pdf-root",
            default=((os.environ.get("PAPER_PDF_ROOT") or "").strip() or (os.environ.get("PAPER_EXTRACTOR_PDF_ROOT") or "").strip() or "input/pdfs"),
            help="Root directory containing PDFs (default: input/pdfs).",
        )
        parser.add_argument("--limit", type=int, default=0, help="Process only the first N PDFs (0 = no limit).")
        parser.add_argument("--dry-run", action="store_true", help="Do not write Excel; print extracted JSON for the first PDF.")
        parser.add_argument("--no-cache", action="store_true", help="Do not use cached LLM outputs.")
        parser.add_argument("--overwrite-existing", action="store_true", help="Overwrite existing rows for matching PDFs.")
        parser.add_argument(
            "--resume",
            action="store_true",
            help="Skip PDFs already listed in the workbook's File Index sheet and continue with the remaining PDFs.",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=_env_int(
                "PAPER_EXTRACT_WORKERS",
                _env_int("PAPER_EXTRACTOR_EXTRACT_WORKERS", _env_int("PAPER_EXTRACTOR_WORKERS", 1)),
            ),
            help="Number of concurrent PDF workers (default: 1).",
        )
        parser.add_argument(
            "--llm-max-inflight",
            type=int,
            default=_env_int(
                "PAPER_EXTRACT_MAX_INFLIGHT",
                _env_int("PAPER_EXTRACTOR_EXTRACT_LLM_MAX_INFLIGHT", _env_int("PAPER_EXTRACTOR_LLM_MAX_INFLIGHT", 2)),
            ),
            help="Max number of concurrent in-flight LLM HTTP requests (default: 2).",
        )
        parser.add_argument("--dump-text", action="store_true", help="Write extracted raw text to input/papers/text/.")
        parser.add_argument(
            "--max-input-chars",
            type=int,
            default=_env_int("PAPER_EXTRACTOR_MAX_INPUT_CHARS", 30000),
            help="If extracted paper text exceeds this many characters, log a warning (LLM may fail due to context limits).",
        )
        parser.add_argument(
            "--llm-calls-per-minute",
            type=int,
            default=_env_int(
                "PAPER_EXTRACT_CALLS_PER_MIN",
                _env_int("PAPER_EXTRACTOR_EXTRACT_LLM_CALLS_PER_MINUTE", _env_int("PAPER_EXTRACTOR_LLM_CALLS_PER_MINUTE", 5)),
            ),
            help="Max LLM calls per minute (default: 5).",
        )
        parser.add_argument(
            "--pdf-extract-timeout-s",
            type=int,
            default=_env_int("PAPER_EXTRACTOR_PDF_EXTRACT_TIMEOUT_S", 90),
            help="Hard timeout (seconds) for PDF text extraction (default: 90). Use 0 to disable.",
        )
        parser.add_argument(
            "--pdf-process-timeout-s",
            type=int,
            default=_env_int("PAPER_EXTRACTOR_PDF_PROCESS_TIMEOUT_S", 600),
            help="Hard timeout (seconds) per PDF (includes extraction+LLM) (default: 600). Use 0 to disable.",
        )
        args = parser.parse_args(argv)

        pdf_root = Path(args.pdf_root).expanduser().resolve()
        output_xlsx_path = Path(args.output_xlsx).expanduser().resolve()

        def resolve_template_xlsx() -> Path:
            template_override = (os.environ.get("PAPER_EXTRACT_TEMPLATE_XLSX") or "").strip()
            if template_override:
                path = Path(template_override).expanduser().resolve()
                if not path.exists() or path.suffix.lower() != ".xlsx":
                    raise RuntimeError(f"Configured template workbook does not exist or is not .xlsx: {path}")
                return path

            template_dir = DEFAULT_TEMPLATE_DIR.resolve()
            matches = sorted(template_dir.glob(DEFAULT_TEMPLATE_GLOB))
            if not matches:
                raise RuntimeError(
                    f"No template workbook found in {template_dir} matching {DEFAULT_TEMPLATE_GLOB}."
                )
            return matches[0].resolve()

        try:
            template_xlsx_path = resolve_template_xlsx()
        except RuntimeError as e:
            safe_print(f"Error: {e}")
            return 2

        if not pdf_root.exists() or not pdf_root.is_dir():
            safe_print(f"Error: --pdf-root must be an existing directory: {pdf_root}")
            return 2

        if args.dry_run:
            xlsx_path = template_xlsx_path
        else:
            output_xlsx_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(template_xlsx_path, output_xlsx_path)
            xlsx_path = output_xlsx_path
            safe_print(f"Created analysis workbook from template: {xlsx_path}")

        return asyncio.run(_run(args=args, xlsx_path=xlsx_path, pdf_root=pdf_root))
    finally:
        ended_at = datetime.now()
        elapsed = time.perf_counter() - started_perf
        safe_print(f"Pipeline ended at {format_timestamp(ended_at)}")
        safe_print(f"Total pipeline time: {format_duration(elapsed)}")


@dataclass(frozen=True)
class _Result:
    ref_number: int
    relative_path: str
    sha256: str
    chars_extracted: int
    pdf_source: str
    row_final: dict[str, str] | None
    save_log: str | None = None
    log_lines: tuple[str, ...] = ()


async def _run(*, args, xlsx_path: Path, pdf_root: Path) -> int:
    ctx = load_workbook_context(xlsx_path)
    headers = normalize_headers(ctx.headers)

    pdf_paths = discover_pdfs(pdf_root)
    if args.resume and not args.overwrite_existing:
        processed = _load_processed_relative_paths(ctx.wb)
        if processed:
            before = len(pdf_paths)
            pdf_paths = [p for p in pdf_paths if _norm_rel_path(str(p.relative_to(pdf_root))) not in processed]
            skipped = before - len(pdf_paths)
            if skipped:
                safe_print(f"Resume mode: skipping {skipped} already-processed PDF(s) from File Index.")
    elif args.resume and args.overwrite_existing:
        safe_print("Resume mode ignored because --overwrite-existing was provided.")

    if args.dry_run and pdf_paths:
        # Keep dry-run deterministic: only work on the first PDF and exit.
        pdf_paths = pdf_paths[:1]

    if args.limit and args.limit > 0:
        pdf_paths = pdf_paths[: args.limit]

    total = len(pdf_paths)
    safe_print(f"Found {total} PDF(s) under {pdf_root}.")
    if total == 0:
        return 0

    config = load_llm_config()
    calls_per_min = max(1, int(args.llm_calls_per_minute))
    min_spacing_s = 60.0 / calls_per_min
    limiter = RateLimiter(min_spacing_s=min_spacing_s)

    cache_dir = Path("paper_extractor/cache")
    text_dir = Path("input") / "papers" / "text"
    cache_dir.mkdir(parents=True, exist_ok=True)
    if args.dump_text:
        text_dir.mkdir(parents=True, exist_ok=True)

    system_prompt = build_system_prompt()
    model_extract = (os.environ.get("OPENROUTER_MODEL_EXTRACT") or "").strip() or config.model

    workers = max(1, int(args.workers or 1))
    llm_max_inflight = max(1, int(args.llm_max_inflight or 2))
    if args.dry_run:
        workers = 1
        llm_max_inflight = 1

    def make_logger(ref_number: int, relative_path: str, log_lines: list[str]):
        prefix = f"[{ref_number}/{total}] {relative_path} - "

        def log(message: str):
            log_lines.append(prefix + str(message))

        return log

    def build_not_reported_row(ref_number: int, ingredient: str | None, pdf_source_label: str, note: str) -> dict[str, str]:
        row_base = {h: NOT_AVAILABLE for h in headers}
        _set_key_finding_note(row_base, note, headers)
        apply_non_llm_fields(
            row_base,
            headers=headers,
            ref_number=ref_number,
            primary_ingredient=ingredient,
            pdf_source=pdf_source_label,
        )
        return coerce_row_values(headers, row_base)

    def log_usage(log, usage: LLMUsage):
        if usage.input_tokens is None and usage.output_tokens is None and usage.total_tokens is None:
            log("LLM tokens: (no usage info returned by provider)")
            return
        parts: list[str] = []
        if usage.input_tokens is not None:
            parts.append(f"input={usage.input_tokens}")
        if usage.output_tokens is not None:
            parts.append(f"output={usage.output_tokens}")
        if usage.total_tokens is not None:
            parts.append(f"total={usage.total_tokens}")
        log("LLM tokens: " + ", ".join(parts))

    async def process_one(ref_number: int, pdf_path: Path, llm_sema: asyncio.Semaphore, client: OpenRouterClient, log) -> _Result:
        rel = str(pdf_path.relative_to(pdf_root))

        sha = await asyncio.to_thread(sha256_file, pdf_path)
        ingredient = derive_primary_ingredient(pdf_root, pdf_path)
        pdf_source_label = derive_pdf_source_label(pdf_root, pdf_path)

        log("Extracting text from PDF...")
        extract_timed_out = False
        try:
            extracted = await extract_pdf_text(pdf_path, timeout_s=float(args.pdf_extract_timeout_s or 0))
        except PdfTextExtractError as e:
            extract_timed_out = "timed out" in str(e).lower()
            if extract_timed_out:
                log(f"Timeout occurred while extracting PDF text. Reason: {e}")
            else:
                log(f"Warning: Could not extract text from this PDF. Reason: {e}")
            extracted = ExtractedText(text="", chars=0, cjk_chars=0)

        log(f"Extracted text characters: {extracted.chars} (CJK chars: {extracted.cjk_chars})")

        if args.dump_text:
            try:
                (text_dir / f"{sha}.txt").write_text(extracted.text, encoding="utf-8", errors="replace")
            except Exception as e:
                log(f"Warning: Could not write text dump. Reason: {type(e).__name__}: {e}")

        if extracted.chars < 800:
            if extract_timed_out:
                return _Result(
                    ref_number,
                    rel,
                    sha,
                    extracted.chars,
                    pdf_source_label,
                    None,
                    f"Timeout occurred extracting PDF text; skipped saving Ref # {ref_number}.",
                )
            row_final = build_not_reported_row(ref_number, ingredient, pdf_source_label, SCANNED_NOTE)
            return _Result(ref_number, rel, sha, extracted.chars, pdf_source_label, row_final)

        if extracted.chars > int(args.max_input_chars):
            log(f"Warning: Paper text length ({extracted.chars} chars) exceeds max context limit ({args.max_input_chars} chars).")
            log("Continuing anyway; the LLM may fail due to context length.")

        cache_path = cache_dir / f"{sha}.json"
        cached = None
        if cache_path.exists() and not args.no_cache:
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception:
                cached = None

        row_llm: dict[str, object]
        if cached and isinstance(cached, dict) and "row" in cached:
            log("Using cached LLM output.")
            row_llm_obj = cached.get("row") or {}
            row_llm = row_llm_obj if isinstance(row_llm_obj, dict) else {}
            row_llm = {str(k): v for k, v in row_llm.items()}
            log("LLM tokens: 0 (cached; no API call)")
        else:
            log(f"Calling LLM once with the full paper text (extract model: {model_extract})...")
            user_prompt = build_user_prompt(headers, extracted.text)
            try:
                await llm_sema.acquire()
                try:
                    data, usage = await client.extract_json(system=system_prompt, user=user_prompt, log=log, model=model_extract)
                finally:
                    llm_sema.release()
                log_usage(log, usage)
            except Exception as e:
                log(f"LLM error: {type(e).__name__}: {e}")
                resp = getattr(e, "response", None)
                if resp is not None:
                    try:
                        log(f"LLM HTTP status: {getattr(resp, 'status_code', 'unknown')}")
                        body = getattr(resp, "text", "")
                        if body:
                            log(f"LLM response body (truncated): {str(body)[:800]}")
                    except Exception:
                        pass

                note = TOO_LONG_NOTE if extracted.chars > int(args.max_input_chars) else LLM_ERROR_NOTE
                row_final = build_not_reported_row(ref_number, ingredient, pdf_source_label, note)
                return _Result(ref_number, rel, sha, extracted.chars, pdf_source_label, row_final)

            row_llm = data if isinstance(data, dict) else {}

            cache_payload = {
                "sha256": sha,
                "relative_path": rel,
                "model_extract": model_extract,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "row": row_llm,
            }
            try:
                _atomic_write_json(cache_path, cache_payload, encoding="utf-8")
            except Exception as e:
                log(f"Warning: Could not write cache file. Reason: {type(e).__name__}: {e}")

        row_flat = flatten_llm_to_excel(headers, row_llm, ref_number=ref_number, fallback_primary_ingredient=ingredient)
        apply_non_llm_fields(row_flat, headers=headers, ref_number=ref_number, primary_ingredient=ingredient, pdf_source=pdf_source_label)
        row_final = coerce_row_values(headers, row_flat)
        return _Result(ref_number, rel, sha, extracted.chars, pdf_source_label, row_final)

    async with OpenRouterClient(config, limiter=limiter) as client:
        llm_sema = asyncio.Semaphore(llm_max_inflight)

        if total == 1 or workers == 1:
            for ref_number, pdf_path in enumerate(pdf_paths, start=1):
                rel = str(pdf_path.relative_to(pdf_root))
                safe_print("")
                safe_print("=" * 80)
                safe_print(f"Processing PDF {ref_number}/{total}: {rel}")
                safe_print("=" * 80)

                log_lines: list[str] = []
                log = make_logger(ref_number, rel, log_lines)
                try:
                    if int(args.pdf_process_timeout_s or 0) > 0:
                        try:
                            res = await asyncio.wait_for(
                                process_one(ref_number, pdf_path, llm_sema, client, log),
                                timeout=float(args.pdf_process_timeout_s),
                            )
                        except asyncio.TimeoutError:
                            log(f"Timeout occurred while processing this PDF (includes extraction+LLM).")
                            sha = await asyncio.to_thread(sha256_file, pdf_path)
                            pdf_source_label = derive_pdf_source_label(pdf_root, pdf_path)
                            res = _Result(
                                ref_number,
                                rel,
                                sha,
                                0,
                                pdf_source_label,
                                None,
                                f"Timeout occurred processing PDF; skipped saving Ref # {ref_number}.",
                            )
                    else:
                        res = await process_one(ref_number, pdf_path, llm_sema, client, log)
                except Exception as e:
                    log(f"Unexpected error while processing PDF: {type(e).__name__}: {e}")
                    sha = await asyncio.to_thread(sha256_file, pdf_path)
                    ingredient = derive_primary_ingredient(pdf_root, pdf_path)
                    pdf_source_label = derive_pdf_source_label(pdf_root, pdf_path)
                    row_final = build_not_reported_row(ref_number, ingredient, pdf_source_label, LLM_ERROR_NOTE)
                    res = _Result(ref_number, rel, sha, 0, pdf_source_label, row_final)

                res = _Result(
                    ref_number=res.ref_number,
                    relative_path=res.relative_path,
                    sha256=res.sha256,
                    chars_extracted=res.chars_extracted,
                    pdf_source=res.pdf_source,
                    row_final=res.row_final,
                    save_log=res.save_log,
                    log_lines=tuple(log_lines),
                )
                for line in res.log_lines:
                    safe_print(line)

                if args.dry_run:
                    if res.row_final is None:
                        safe_print(json.dumps({"status": "skipped", "reason": res.save_log or "timeout"}, ensure_ascii=False, indent=2))
                    else:
                        safe_print(json.dumps(res.row_final, ensure_ascii=False, indent=2))
                    return 0

                if res.row_final is not None:
                    write_paper_row(
                        ctx,
                        row_data=res.row_final,
                        file_index={
                            "relative_path": res.relative_path,
                            "sha256": res.sha256,
                            "chars_extracted": res.chars_extracted,
                            "model": model_extract,
                            "pdf_source": res.pdf_source,
                        },
                        overwrite_existing=args.overwrite_existing,
                    )
                    safe_print(res.save_log or f"Saved row Ref # {ref_number}.")
                else:
                    safe_print(res.save_log or f"Skipped row Ref # {ref_number}.")
        else:
            safe_print(f"Parallel mode enabled: workers={workers}, llm_max_inflight={llm_max_inflight}.")
            work_q: asyncio.Queue[tuple[int, Path] | None] = asyncio.Queue()
            results_q: asyncio.Queue[_Result] = asyncio.Queue()
            status_lock = asyncio.Lock()
            active_count = 0
            buffered_count = 0
            completed_count = 0

            for ref_number, pdf_path in enumerate(pdf_paths, start=1):
                work_q.put_nowait((ref_number, pdf_path))
            for _ in range(workers):
                work_q.put_nowait(None)

            async def log_parallel_status(event: str, ref_number: int, relative_path: str):
                safe_print(
                    f"[Parallel] {event} ref={ref_number}/{total} path={relative_path} "
                    f"active={active_count} buffered={buffered_count} completed={completed_count}"
                )

            async def status_heartbeat():
                nonlocal active_count, buffered_count, completed_count
                while True:
                    await asyncio.sleep(60)
                    async with status_lock:
                        safe_print(
                            f"[Parallel] heartbeat active={active_count} buffered={buffered_count} completed={completed_count}"
                        )

            async def worker_loop():
                nonlocal active_count, buffered_count, completed_count
                while True:
                    item = await work_q.get()
                    if item is None:
                        return
                    ref_number, pdf_path = item
                    rel = str(pdf_path.relative_to(pdf_root))
                    async with status_lock:
                        active_count += 1
                        await log_parallel_status("start", ref_number, rel)
                    log_lines: list[str] = []
                    log = make_logger(ref_number, rel, log_lines)
                    try:
                        if int(args.pdf_process_timeout_s or 0) > 0:
                            try:
                                res = await asyncio.wait_for(
                                    process_one(ref_number, pdf_path, llm_sema, client, log),
                                    timeout=float(args.pdf_process_timeout_s),
                                )
                            except asyncio.TimeoutError:
                                log("Timeout occurred while processing this PDF (includes extraction+LLM).")
                                sha = await asyncio.to_thread(sha256_file, pdf_path)
                                pdf_source_label = derive_pdf_source_label(pdf_root, pdf_path)
                                res = _Result(
                                    ref_number,
                                    rel,
                                    sha,
                                    0,
                                    pdf_source_label,
                                    None,
                                    f"Timeout occurred processing PDF; skipped saving Ref # {ref_number}.",
                                )
                        else:
                            res = await process_one(ref_number, pdf_path, llm_sema, client, log)
                    except Exception as e:
                        log(f"Unexpected error while processing PDF: {type(e).__name__}: {e}")
                        sha = await asyncio.to_thread(sha256_file, pdf_path)
                        ingredient = derive_primary_ingredient(pdf_root, pdf_path)
                        pdf_source_label = derive_pdf_source_label(pdf_root, pdf_path)
                        row_final = build_not_reported_row(ref_number, ingredient, pdf_source_label, LLM_ERROR_NOTE)
                        res = _Result(ref_number, rel, sha, 0, pdf_source_label, row_final)
                    res = _Result(
                        ref_number=res.ref_number,
                        relative_path=res.relative_path,
                        sha256=res.sha256,
                        chars_extracted=res.chars_extracted,
                        pdf_source=res.pdf_source,
                        row_final=res.row_final,
                        save_log=res.save_log,
                        log_lines=tuple(log_lines),
                    )
                    async with status_lock:
                        active_count -= 1
                        buffered_count += 1
                        completed_count += 1
                        await log_parallel_status("completed", ref_number, rel)
                    await results_q.put(res)

            worker_tasks = [asyncio.create_task(worker_loop()) for _ in range(workers)]
            heartbeat_task = asyncio.create_task(status_heartbeat())

            pending: dict[int, _Result] = {}
            next_ref = 1
            for _ in range(total):
                res = await results_q.get()
                pending[res.ref_number] = res
                async with status_lock:
                    buffered_count = len(pending)
                while next_ref in pending:
                    r = pending.pop(next_ref)
                    for line in r.log_lines:
                        safe_print(line)
                    if r.row_final is not None:
                        write_paper_row(
                            ctx,
                            row_data=r.row_final,
                            file_index={
                                "relative_path": r.relative_path,
                                "sha256": r.sha256,
                                "chars_extracted": r.chars_extracted,
                                "model": model_extract,
                                "pdf_source": r.pdf_source,
                            },
                            overwrite_existing=args.overwrite_existing,
                        )
                        safe_print(r.save_log or f"Saved row Ref # {next_ref}.")
                    else:
                        safe_print(r.save_log or f"Skipped row Ref # {next_ref}.")
                    next_ref += 1
                async with status_lock:
                    buffered_count = len(pending)

            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            await asyncio.gather(*worker_tasks, return_exceptions=True)

    if not args.dry_run:
        safe_print("Saving workbook...")
        save_workbook(ctx)
        safe_print("Workbook saved.")
        safe_print(f"Done. Updated spreadsheet: {xlsx_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
