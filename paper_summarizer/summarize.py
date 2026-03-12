from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from helpers.env import load_dotenv
from helpers.llm_openrouter import OpenRouterClient, load_llm_config
from helpers.logging_utils import get_logger
from helpers.pdf_text_extract import ExtractedText, PdfTextExtractError, extract_text_with_page_markers
from helpers.rate_limiter import RateLimiter
from paper_extractor.schema import NOT_AVAILABLE
from . import summary_service

logger = get_logger(__name__, prefix="[ Summarizer: ]")


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


def derive_primary_ingredient(pdf_root: Path, pdf_path: Path) -> str | None:
    try:
        rel = pdf_path.relative_to(pdf_root)
    except Exception:
        return None
    parts = rel.parts
    if len(parts) >= 2:
        return parts[0]
    return None


@dataclass(frozen=True)
class _SummaryResult:
    ref_number: int
    relative_path: str
    sha256: str
    summary_text: str
    had_error: bool = False
    log_lines: tuple[str, ...] = ()


async def main(argv: list[str] | None = None) -> int:
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
            raw = raw.replace(",", "").replace("_", "").strip()
            try:
                return int(raw)
            except Exception:
                return int(default)

        parser = argparse.ArgumentParser(prog="paper_summarizer.summarize")
        parser.add_argument(
            "--pdf-root",
            default=((os.environ.get("PAPER_PDF_ROOT") or "").strip() or (os.environ.get("PAPER_EXTRACTOR_PDF_ROOT") or "").strip() or "input/pdfs"),
            help="Root directory containing PDFs (default: input/pdfs).",
        )
        parser.add_argument("--limit", type=int, default=0, help="Process only the first N PDFs (0 = no limit).")
        parser.add_argument("--dry-run", action="store_true", help="Do not write files; print the first summary and exit.")
        parser.add_argument("--no-cache", action="store_true", help="Do not use or write summary cache files.")
        parser.add_argument(
            "--resume",
            action="store_true",
            help="Deprecated: cached summaries are now used automatically unless --no-cache is set.",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=_env_int(
                "PAPER_SUMMARY_WORKERS",
                _env_int("PAPER_EXTRACTOR_SUMMARY_WORKERS", _env_int("PAPER_EXTRACTOR_WORKERS", 1)),
            ),
            help="Number of concurrent PDF workers (default: 1).",
        )
        parser.add_argument(
            "--llm-max-inflight",
            type=int,
            default=_env_int(
                "PAPER_SUMMARY_MAX_INFLIGHT",
                _env_int("PAPER_EXTRACTOR_SUMMARY_LLM_MAX_INFLIGHT", _env_int("PAPER_EXTRACTOR_LLM_MAX_INFLIGHT", 2)),
            ),
            help="Max number of concurrent in-flight LLM HTTP requests (default: 2).",
        )
        parser.add_argument(
            "--llm-calls-per-minute",
            type=int,
            default=_env_int(
                "PAPER_SUMMARY_CALLS_PER_MIN",
                _env_int("PAPER_EXTRACTOR_SUMMARY_LLM_CALLS_PER_MINUTE", _env_int("PAPER_EXTRACTOR_LLM_CALLS_PER_MINUTE", 5)),
            ),
            help="Global max LLM request starts per minute.",
        )
        parser.add_argument(
            "--max-input-chars",
            type=int,
            default=_env_int("PAPER_EXTRACTOR_MAX_INPUT_CHARS", 30000),
            help="If extracted paper text exceeds this many characters, log a warning (LLM may fail).",
        )
        parser.add_argument(
            "--summary-min-sentences",
            type=int,
            default=_env_int("PAPER_EXTRACTOR_SUMMARY_MIN_SENTENCES", 110),
            help="Inserted into the summary prompt (not code-enforced).",
        )
        parser.add_argument(
            "--summary-word-cap",
            type=int,
            default=summary_service.SUMMARY_WORD_CAP_DEFAULT,
            help="Hard cap applied before writing cache/files (default: 10000).",
        )
        parser.add_argument(
            "--pdf-extract-timeout-s",
            type=int,
            default=_env_int("PAPER_SUMMARY_PDF_EXTRACT_TIMEOUT_S", _env_int("PAPER_EXTRACTOR_PDF_EXTRACT_TIMEOUT_S", 90)),
            help="Timeout for PDF text extraction per file in seconds.",
        )
        parser.add_argument(
            "--pdf-process-timeout-s",
            type=int,
            default=_env_int("PAPER_SUMMARY_PDF_PROCESS_TIMEOUT_S", _env_int("PAPER_EXTRACTOR_PDF_PROCESS_TIMEOUT_S", 600)),
            help="End-to-end timeout per PDF in seconds.",
        )
        args = parser.parse_args(argv)

        pdf_root = Path(args.pdf_root).expanduser().resolve()
        if not pdf_root.exists() or not pdf_root.is_dir():
            safe_print(f"Error: --pdf-root must be an existing directory: {pdf_root}")
            return 2

        pdf_paths = discover_pdfs(pdf_root)
        if args.limit and args.limit > 0:
            pdf_paths = pdf_paths[: args.limit]
        if args.dry_run and pdf_paths:
            pdf_paths = pdf_paths[:1]

        total = len(pdf_paths)
        safe_print(f"Found {total} PDF(s) under {pdf_root}.")
        if total == 0:
            return 0

        config = load_llm_config()
        model_summary = (os.environ.get("OPENROUTER_MODEL_SUMMARY") or "").strip() or config.model

        calls_per_min = max(1, int(args.llm_calls_per_minute))
        limiter = RateLimiter(min_spacing_s=(60.0 / calls_per_min))

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

        async def process_one(ref_number: int, pdf_path: Path, llm_sema: asyncio.Semaphore, client: OpenRouterClient, log) -> _SummaryResult:
            rel = str(pdf_path.relative_to(pdf_root))
            extract_timeout_s = max(1, int(args.pdf_extract_timeout_s))
            process_timeout_s = max(extract_timeout_s, int(args.pdf_process_timeout_s))

            async def process_one_impl() -> tuple[str, str, str | None, bool]:
                sha = await asyncio.to_thread(sha256_file, pdf_path)
                ingredient = derive_primary_ingredient(pdf_root, pdf_path)

                if not args.no_cache:
                    cached = summary_service.load_cached_summary(sha256=sha)
                    if cached is not None:
                        log("Using cached summary result.")
                        cached = str(cached or "").strip() or NOT_AVAILABLE
                        cached, truncated = summary_service.truncate_to_words(cached, max_words=int(args.summary_word_cap))
                        if truncated:
                            log(f"Warning: Cached summary exceeded {args.summary_word_cap} words; truncated.")
                        return (sha, cached, ingredient, False)

                log("Extracting text from PDF...")
                try:
                    extracted = await asyncio.wait_for(
                        asyncio.to_thread(extract_text_with_page_markers, pdf_path),
                        timeout=extract_timeout_s,
                    )
                except asyncio.TimeoutError:
                    log(f"Error: PDF extraction timed out after {extract_timeout_s}s.")
                    return (sha, NOT_AVAILABLE, ingredient, True)
                except PdfTextExtractError as e:
                    log(f"Warning: Could not extract text from this PDF. Reason: {e}")
                    extracted = ExtractedText(text="", chars=0, cjk_chars=0)

                log(f"Extracted text characters: {extracted.chars} (CJK chars: {extracted.cjk_chars})")
                if extracted.chars < 800:
                    summary_text = NOT_AVAILABLE
                else:
                    if extracted.chars > int(args.max_input_chars):
                        log(
                            f"Warning: Paper text length ({extracted.chars} chars) exceeds max context limit "
                            f"({args.max_input_chars} chars). Continuing anyway; the summary call may fail due to context length."
                        )

                    system_summary = summary_service.build_summary_system_prompt(
                        prompts_dir=(Path(__file__).resolve().parent / "prompts"),
                        min_sentences=int(args.summary_min_sentences),
                        word_cap=int(args.summary_word_cap),
                    )
                    user_summary = summary_service.build_summary_user_prompt(
                        prompts_dir=(Path(__file__).resolve().parent / "prompts"),
                        paper_text=extracted.text,
                    )

                    log(f"Calling LLM for summary (model: {model_summary})...")
                    try:
                        await llm_sema.acquire()
                        try:
                            summary_text, usage = await client.chat_text(
                                system=system_summary,
                                user=user_summary,
                                log=log,
                                model=model_summary,
                            )
                        finally:
                            llm_sema.release()
                        if usage.input_tokens is not None or usage.output_tokens is not None or usage.total_tokens is not None:
                            parts: list[str] = []
                            if usage.input_tokens is not None:
                                parts.append(f"input={usage.input_tokens}")
                            if usage.output_tokens is not None:
                                parts.append(f"output={usage.output_tokens}")
                            if usage.total_tokens is not None:
                                parts.append(f"total={usage.total_tokens}")
                            log("LLM tokens: " + ", ".join(parts))
                    except Exception as e:
                        log(f"LLM error while generating summary: {type(e).__name__}: {e}")
                        summary_text = NOT_AVAILABLE

                summary_text = str(summary_text or "").strip() or NOT_AVAILABLE
                summary_text, truncated = summary_service.truncate_to_words(summary_text, max_words=int(args.summary_word_cap))
                if truncated:
                    log(f"Warning: Detailed summary exceeded {args.summary_word_cap} words; truncated.")
                return (sha, summary_text, ingredient, False)

            try:
                sha, summary_text, ingredient, timed_out = await asyncio.wait_for(
                    process_one_impl(),
                    timeout=process_timeout_s,
                )
            except asyncio.TimeoutError:
                log(f"Error: PDF processing timed out after {process_timeout_s}s.")
                return _SummaryResult(ref_number, rel, "", NOT_AVAILABLE, had_error=True)

            if timed_out:
                return _SummaryResult(ref_number, rel, sha, summary_text, had_error=True)

            if not args.dry_run:
                try:
                    out_path = summary_service.write_summary_file(
                        ingredient=ingredient,
                        ref_number=ref_number,
                        relative_path=rel,
                        sha256=sha,
                        model_summary=model_summary,
                        summary_text=summary_text,
                        word_cap=int(args.summary_word_cap),
                    )
                    log(f"Saved detailed summary to: {out_path}")
                except Exception as e:
                    log(f"Warning: Could not write summary file. Reason: {type(e).__name__}: {e}")

                if not args.no_cache:
                    try:
                        summary_service.write_summary_cache(
                            sha256=sha,
                            relative_path=rel,
                            model_summary=model_summary,
                            summary_text=summary_text,
                        )
                        log("Saved summary cache entry.")
                    except Exception as e:
                        log(f"Warning: Could not write summary cache file. Reason: {type(e).__name__}: {e}")

                log(f"Processed PDF: {rel}")

            return _SummaryResult(ref_number, rel, sha, summary_text)

        async with OpenRouterClient(config, limiter=limiter) as client:
            llm_sema = asyncio.Semaphore(llm_max_inflight)

            if workers == 1 or total == 1:
                for ref_number, pdf_path in enumerate(pdf_paths, start=1):
                    rel = str(pdf_path.relative_to(pdf_root))
                    log_lines: list[str] = []
                    log = make_logger(ref_number, rel, log_lines)
                    res = await process_one(ref_number, pdf_path, llm_sema, client, log)
                    for line in log_lines:
                        safe_print(line)
                    if args.dry_run:
                        safe_print(res.summary_text)
                        return 0
                return 0

            safe_print(f"Parallel mode enabled: workers={workers}, llm_max_inflight={llm_max_inflight}.")
            work_q: asyncio.Queue[tuple[int, Path] | None] = asyncio.Queue()
            results_q: asyncio.Queue[_SummaryResult] = asyncio.Queue()
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
                        res = await process_one(ref_number, pdf_path, llm_sema, client, log)
                    except Exception as e:
                        log(f"Worker failed unexpectedly: {type(e).__name__}: {e}")
                        res = _SummaryResult(
                            ref_number=ref_number,
                            relative_path=rel,
                            sha256="",
                            summary_text=NOT_AVAILABLE,
                            had_error=True,
                            log_lines=tuple(log_lines),
                        )
                    else:
                        res = _SummaryResult(
                            ref_number=res.ref_number,
                            relative_path=res.relative_path,
                            sha256=res.sha256,
                            summary_text=res.summary_text,
                            had_error=res.had_error,
                            log_lines=tuple(log_lines),
                        )
                    async with status_lock:
                        active_count -= 1
                        buffered_count += 1
                        completed_count += 1
                        await log_parallel_status("completed", ref_number, rel)
                    await results_q.put(res)

            tasks = [asyncio.create_task(worker_loop()) for _ in range(workers)]
            heartbeat_task = asyncio.create_task(status_heartbeat())
            had_errors = False
            pending_results: dict[int, _SummaryResult] = {}
            next_ref_to_print = 1
            for _ in range(total):
                res = await results_q.get()
                pending_results[res.ref_number] = res
                async with status_lock:
                    buffered_count = len(pending_results)
                while next_ref_to_print in pending_results:
                    ready = pending_results.pop(next_ref_to_print)
                    for line in ready.log_lines:
                        safe_print(line)
                    if ready.had_error:
                        had_errors = True
                    next_ref_to_print += 1
                async with status_lock:
                    buffered_count = len(pending_results)
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            for task_result in task_results:
                if isinstance(task_result, Exception):
                    had_errors = True
                    safe_print(f"Worker task exited with exception: {type(task_result).__name__}: {task_result}")
            if had_errors:
                safe_print("Completed with worker errors. Some PDFs may not have produced summaries.")
                return 1
            return 0
    finally:
        ended_at = datetime.now()
        elapsed = time.perf_counter() - started_perf
        safe_print(f"Pipeline ended at {format_timestamp(ended_at)}")
        safe_print(f"Total pipeline time: {format_duration(elapsed)}")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
