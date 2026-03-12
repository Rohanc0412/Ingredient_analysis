import asyncio
import json
import logging
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
from playwright.async_api import async_playwright
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import BrowserContext
from helpers.logging_utils import get_logger

EMAIL = "gowdarohan69@gmail.com"  # change this
HEADLESS = False
MANUAL_ASSIST = True
MANUAL_ASSIST_TIMEOUT_S = 240
SAVE_BLOCKED_HTML = False

# Download step timeouts (these apply to the main automatic flow):
# - Step 1/4 (requests): seconds
# - Step 2/4 (playwright request): milliseconds
REQUESTS_TIMEOUT_S = 25
PLAYWRIGHT_REQUEST_TIMEOUT_MS = 25_000
PAPER_LINKS_DIR = Path("input") / "papers" / "paper_links"
OUT_DIR = Path("input") / "pdfs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
logger = get_logger(__name__, prefix="[ Downloader: ]")


def format_log(message: str) -> str:
    return f"[ Downloader: ] {message}"


def safe_print(msg: str) -> None:
    logger.info("%s", msg)


def format_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_duration(total_seconds: float) -> str:
    total_seconds = max(0, int(round(total_seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def concise_reason(message: str | None, max_len: int = 140) -> str:
    text = " ".join(str(message or "").split()).strip()
    replacements = (
        ("requests:", "direct request:"),
        ("playwright-request:", "browser request:"),
        ("playwright-page:", "browser tab:"),
        ("manual-assist:", "manual assist:"),
    )
    for old, new in replacements:
        if text.startswith(old):
            text = text.replace(old, new, 1)
            break
    if len(text) > max_len:
        text = text[: max_len - 3].rstrip() + "..."
    return text or "unknown error"


def paper_label(*, idx_item: int, total: int, ingredient: str, source: str, doi: str | None) -> str:
    doi_text = doi if doi else "no-doi"
    return f"[{idx_item}/{total}] {ingredient} | {source} | {doi_text}"


def _normalize_loaded_paper(item: dict, *, source_file: Path) -> dict:
    paper = dict(item or {})
    if not paper.get("ingredient"):
        paper["ingredient"] = source_file.stem.replace("_", " ").strip() or "unknown_ingredient"
    if not paper.get("source"):
        paper["source"] = "unknown_source"
    return paper


def load_papers(paper_links_dir: Path) -> list[dict]:
    papers: list[dict] = []
    if not paper_links_dir.exists() or not paper_links_dir.is_dir():
        return papers

    for path in sorted(paper_links_dir.glob("*.json"), key=lambda p: p.name.lower()):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            safe_print(f"Skipping invalid JSON file: {path} ({type(exc).__name__})")
            continue

        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict) and isinstance(payload.get("papers"), list):
            items = payload["papers"]
        else:
            safe_print(f"Skipping unsupported JSON structure: {path}")
            continue

        for item in items:
            if isinstance(item, dict):
                papers.append(_normalize_loaded_paper(item, source_file=path))
    return papers

def safe_filename(name: str, max_len: int = 160) -> str:
    name = name.strip()
    name = re.sub(r"[^\w\-\.\(\) ]+", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return (name[:max_len] or "paper.pdf")

def safe_dirname(name: str, max_len: int = 80) -> str:
    name = (name or "").strip()
    name = re.sub(r"[^\w\-\.\(\) ]+", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return (name[:max_len] or "unknown_source")

def filename_from_url_or_doi(pdf_url: str, doi: str) -> str:
    base = Path(urlparse(pdf_url).path).name
    if base and base.lower().endswith(".pdf"):
        return safe_filename(base)
    safe_doi = re.sub(r"[^A-Za-z0-9._-]+", "_", doi)
    return safe_filename(f"{safe_doi}.pdf")

def default_filename(doi: str | None, pdf_url: str | None) -> str:
    if doi:
        safe_doi = re.sub(r"[^A-Za-z0-9._-]+", "_", doi)
        return safe_filename(f"{safe_doi}.pdf")
    if pdf_url:
        base = Path(urlparse(pdf_url).path).name
        if base:
            return safe_filename(base)
    return "paper.pdf"

def source_key(host_type: str | None, pdf_url: str | None) -> str:
    hostname = ""
    if pdf_url:
        hostname = (urlparse(pdf_url).hostname or "").lower()
    if hostname:
        return safe_dirname(hostname)
    host_type = (host_type or "").strip().lower()
    if host_type:
        return safe_dirname(host_type)
    return "unknown_source"

def ensure_unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    for i in range(2, 10_000):
        candidate = parent / f"{stem} ({i}){suffix}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Too many conflicting filenames for {path}")

def unpaywall_pdf_url(doi: str, email: str):
    try:
        r = requests.get(f"https://api.unpaywall.org/v2/{doi}", params={"email": email}, timeout=30)
        if r.status_code == 404:
            return None, None, "unpaywall", None, f"Unpaywall: DOI not found ({doi})"
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        return None, None, "unpaywall", None, f"Unpaywall: {type(e).__name__}: {e}"

    best = data.get("best_oa_location") or {}
    pdf_url = best.get("url_for_pdf")
    host_type = best.get("host_type")

    if not pdf_url:
        for loc in (data.get("oa_locations") or []):
            if loc.get("url_for_pdf"):
                pdf_url = loc["url_for_pdf"]
                host_type = loc.get("host_type")
                break

    return data.get("is_oa"), pdf_url, source_key(host_type, pdf_url), host_type, None

def pmc_pdf_url_from_doi(doi: str, email: str) -> str | None:
    params = {"format": "json", "ids": doi, "tool": "paper_downloader", "email": email}
    r = requests.get("https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    records = data.get("records") or []
    for rec in records:
        pmcid = rec.get("pmcid")
        if pmcid and isinstance(pmcid, str) and pmcid.upper().startswith("PMC"):
            return f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/"
    return None

def resolve_pdf_source(doi: str, email: str, source_preference: str | None):
    pref = (source_preference or "").strip().lower()
    if pref in ("pubmed", "pmc"):
        try:
            pdf_url = pmc_pdf_url_from_doi(doi, email)
        except requests.RequestException as e:
            pdf_url = None
            pmc_err = f"PMC idconv: {type(e).__name__}: {e}"
        else:
            pmc_err = None

        if pdf_url:
            return True, pdf_url, source_key("repository", pdf_url), "repository", pmc_err

        is_oa, url, resolved, host_type, err = unpaywall_pdf_url(doi, email)
        if pmc_err and not err:
            err = pmc_err
        return is_oa, url, resolved, host_type, err
    if pref in ("google_scholar", "scholar", "google scholar"):
        # Don't scrape Scholar; instead, try Unpaywall for a direct OA PDF, but keep the output folder as "google_scholar".
        return unpaywall_pdf_url(doi, email)
    return unpaywall_pdf_url(doi, email)

def try_download_pdf_via_requests(pdf_url: str, out_path: Path):
    host = (urlparse(pdf_url).hostname or "").lower()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PaperDownloader/1.0",
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    }
    if host.endswith("ncbi.nlm.nih.gov"):
        headers["Referer"] = "https://pmc.ncbi.nlm.nih.gov/"
    try:
        with requests.get(pdf_url, headers=headers, stream=True, timeout=REQUESTS_TIMEOUT_S, allow_redirects=True) as r:
            r.raise_for_status()
            ctype = (r.headers.get("content-type") or "").lower()
            final_url = r.url

            it = r.iter_content(chunk_size=1024 * 64)
            first_chunk = b""
            for _ in range(4):
                first_chunk = next(it, b"")
                if first_chunk:
                    break
            is_pdf = ("application/pdf" in ctype) or first_chunk.startswith(b"%PDF-")

            if is_pdf and first_chunk:
                out_path.write_bytes(b"")
                with out_path.open("wb") as f:
                    f.write(first_chunk)
                    for chunk in it:
                        if chunk:
                            f.write(chunk)
                return True, f"requests: saved {out_path}"

            if SAVE_BLOCKED_HTML:
                blocked = out_path.with_suffix(".blocked.html")
                body = first_chunk + b"".join(list(it)[:8])
                blocked.write_bytes(body[:500_000])
                return False, f"requests: not a PDF (content-type={ctype}, final_url={final_url}). Saved {blocked}"
            return False, f"requests: not a PDF (content-type={ctype}, final_url={final_url}). Not downloaded."
    except requests.RequestException as e:
        return False, f"requests: {type(e).__name__}: {e}"

async def try_download_pdf_via_playwright_request(context: BrowserContext, pdf_url: str, out_path: Path):
    try:
        resp = await context.request.get(pdf_url, timeout=PLAYWRIGHT_REQUEST_TIMEOUT_MS, max_redirects=10)
        ctype = (resp.headers.get("content-type") or "").lower()
        data = await resp.body()
        if ("application/pdf" in ctype) or data.startswith(b"%PDF-"):
            out_path.write_bytes(data)
            return True, f"playwright-request: saved {out_path}"
        if SAVE_BLOCKED_HTML:
            blocked = out_path.with_suffix(".blocked.html")
            blocked.write_bytes(data[:500_000])
            return False, f"playwright-request: not a PDF (content-type={ctype}). Saved {blocked}"
        return False, f"playwright-request: not a PDF (content-type={ctype}). Not downloaded."
    except PlaywrightError as e:
        return False, f"playwright-request: {e}"

async def try_download_pdf_via_playwright_page(context: BrowserContext, page, pdf_url: str, out_path: Path):
    for attempt in range(1, 3):
        try:
            async with page.expect_download(timeout=40_000) as download_info:
                resp = await page.goto(pdf_url, wait_until="domcontentloaded", timeout=40_000)
            download = await download_info.value
            await download.save_as(str(out_path))
            return True, f"playwright-page: saved {out_path}"
        except PlaywrightError:
            try:
                resp = await page.goto(pdf_url, wait_until="domcontentloaded", timeout=40_000)
            except PlaywrightError as e:
                if attempt == 2:
                    return False, f"playwright-page: {e}"
                try:
                    await page.wait_for_timeout(1500)
                except PlaywrightError:
                    pass
                continue

            if resp is None:
                return False, "playwright-page: no response"

            try:
                ctype = (resp.headers.get("content-type") or "").lower()
                data = await resp.body()
            except PlaywrightError as e2:
                if attempt == 2:
                    return False, f"playwright-page: {e2}"
                continue

            if ("application/pdf" in ctype) or data.startswith(b"%PDF-"):
                out_path.write_bytes(data)
                return True, f"playwright-page: saved {out_path}"

            if SAVE_BLOCKED_HTML:
                blocked = out_path.with_suffix(".blocked.html")
                blocked.write_bytes(data[:500_000])
                return False, f"playwright-page: not a PDF (content-type={ctype}). Saved {blocked}"
            return False, f"playwright-page: not a PDF (content-type={ctype}). Not downloaded."

def looks_like_cloudflare_challenge(html: str) -> bool:
    h = (html or "").lower()
    return ("just a moment" in h) and ("_cf_chl_opt" in h or "cdn-cgi/challenge-platform" in h)

async def try_download_with_manual_assist(playwright, pdf_url: str, out_path: Path):
    # One URL per browser window: use a fresh, temporary profile dir per call.
    user_data_dir = Path(tempfile.mkdtemp(prefix="pw-manual-profile-"))
    downloads_dir = Path(tempfile.mkdtemp(prefix="pw-manual-downloads-"))
    started_at = time.time()
    context = await playwright.chromium.launch_persistent_context(
        user_data_dir=str(user_data_dir),
        headless=False,
        accept_downloads=True,
        downloads_path=str(downloads_dir),
    )
    page = await context.new_page()

    async def poll_for_pdf():
        deadline = asyncio.get_event_loop().time() + MANUAL_ASSIST_TIMEOUT_S
        while asyncio.get_event_loop().time() < deadline:
            try:
                resp = await context.request.get(pdf_url, timeout=120000, max_redirects=10)
                ctype = (resp.headers.get("content-type") or "").lower()
                data = await resp.body()
                if ("application/pdf" in ctype) or data.startswith(b"%PDF-"):
                    out_path.write_bytes(data)
                    return True, f"manual-assist: fetched PDF to {out_path}"
            except PlaywrightError:
                pass
            try:
                await page.wait_for_timeout(5000)
            except PlaywrightError:
                await asyncio.sleep(5)
        return False, "manual-assist: timed out waiting for PDF access"

    async def wait_for_browser_close():
        # If the user closes the browser window (persistent context), treat it like "skip this paper".
        while True:
            try:
                _ = context.pages
            except PlaywrightError:
                return False, "manual-assist: browser closed by user"
            await asyncio.sleep(1)

    async def wait_for_download_event_any_tab():
        try:
            download = await context.wait_for_event("download", timeout=MANUAL_ASSIST_TIMEOUT_S * 1000)
            await download.save_as(str(out_path))
            return True, f"manual-assist: saved {out_path}"
        except PlaywrightError as e:
            return False, f"manual-assist: no download event ({e})"

    def can_cancel_via_enter() -> bool:
        if sys.stdin is None or not getattr(sys.stdin, "isatty", lambda: False)():
            return False
        if sys.platform.startswith("win"):
            try:
                import msvcrt  # noqa: F401
            except Exception:
                return False
            return True
        # On Unix-like OSes, asyncio can watch stdin without threads via add_reader.
        try:
            loop = asyncio.get_running_loop()
            if not hasattr(loop, "add_reader"):
                return False
            _ = sys.stdin.fileno()
        except Exception:
            return False
        return True

    async def wait_for_enter_key():
        # Cross-platform "press Enter to cancel" without spawning a thread:
        # - Windows: poll with msvcrt
        # - Unix: use loop.add_reader on stdin
        deadline = asyncio.get_event_loop().time() + MANUAL_ASSIST_TIMEOUT_S

        if sys.platform.startswith("win"):
            import msvcrt  # type: ignore

            while asyncio.get_event_loop().time() < deadline:
                if msvcrt.kbhit():
                    ch = msvcrt.getwch()
                    if ch in ("\r", "\n"):
                        return False, "manual-assist: canceled by user"
                await asyncio.sleep(0.1)
            return False, "manual-assist: cancel timed out"

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[bool] = loop.create_future()

        def _on_stdin_ready():
            try:
                line = sys.stdin.readline()
            except Exception:
                line = ""
            if not fut.done():
                fut.set_result(True if line is not None else True)

        loop.add_reader(sys.stdin.fileno(), _on_stdin_ready)
        try:
            timeout_s = max(0.0, deadline - asyncio.get_event_loop().time())
            await asyncio.wait_for(fut, timeout=timeout_s)
            return False, "manual-assist: canceled by user"
        except TimeoutError:
            return False, "manual-assist: cancel timed out"
        finally:
            try:
                loop.remove_reader(sys.stdin.fileno())
            except Exception:
                pass

    def _looks_like_pdf_file(path: Path) -> bool:
        if not path.is_file():
            return False
        if path.suffix.lower() in (".crdownload", ".tmp", ".part"):
            return False
        try:
            with path.open("rb") as f:
                return f.read(5) == b"%PDF-"
        except OSError:
            return False

    async def watch_downloads_folder():
        # If the user downloads via the browser UI (e.g. Chrome PDF viewer), Playwright may not emit a
        # download event. Watch the controlled downloads folder and copy a completed PDF into out_path.
        deadline = asyncio.get_event_loop().time() + MANUAL_ASSIST_TIMEOUT_S
        last_size: dict[Path, int] = {}
        watch_dirs = [downloads_dir]
        home_downloads = Path.home() / "Downloads"
        if home_downloads.is_dir():
            watch_dirs.append(home_downloads)
        while asyncio.get_event_loop().time() < deadline:
            candidates: list[Path] = []
            for d in watch_dirs:
                try:
                    candidates.extend(d.glob("*"))
                except OSError:
                    continue
            try:
                candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            except OSError:
                pass

            for cand in candidates:
                if cand.suffix.lower() == ".crdownload":
                    continue
                try:
                    if cand.stat().st_mtime < started_at:
                        continue
                    size_now = cand.stat().st_size
                except OSError:
                    continue
                size_prev = last_size.get(cand)
                last_size[cand] = size_now
                if size_prev is None or size_now != size_prev:
                    continue  # still changing
                if _looks_like_pdf_file(cand):
                    shutil.copy2(cand, out_path)
                    return True, f"manual-assist: copied download to {out_path}"

            await asyncio.sleep(1)
        return False, "manual-assist: timed out waiting for a downloaded PDF file"

    try:
        try:
            await page.goto(pdf_url, wait_until="domcontentloaded", timeout=180_000)
        except PlaywrightError:
            pass

        try:
            html = await page.content()
        except PlaywrightError:
            html = ""

        if looks_like_cloudflare_challenge(html):
            safe_print("Manual assist: Cloudflare check detected in the opened browser.")

        # Auto-click attempt (best-effort; may not exist on PDF viewer pages)
        try:
            for selector in (
                "a:has-text('Download PDF')",
                "a:has-text('Download')",
                "button:has-text('Download')",
                "[aria-label*='Download' i]",
                "a[download]",
            ):
                loc = page.locator(selector)
                if await loc.count():
                    await loc.first.click(timeout=3000)
                    break
        except PlaywrightError:
            pass

        safe_print("Manual assist: If auto-download does not start, click Download in the opened browser.")
        if can_cancel_via_enter():
            safe_print("Manual assist: Press Enter here to skip this paper.")
        else:
            safe_print("Manual assist: Close the browser window to skip this paper.")

        download_task = asyncio.create_task(wait_for_download_event_any_tab())
        poll_task = asyncio.create_task(poll_for_pdf())
        watch_task = asyncio.create_task(watch_downloads_folder())
        close_task = asyncio.create_task(wait_for_browser_close())

        cancel_task = asyncio.create_task(wait_for_enter_key()) if can_cancel_via_enter() else None

        tasks = {download_task, poll_task, watch_task, close_task}
        if cancel_task is not None:
            tasks.add(cancel_task)
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()

        finished = done.pop()
        if cancel_task is not None and finished is cancel_task:
            return False, "manual-assist: canceled by user"
        if finished is close_task:
            return False, "manual-assist: browser closed by user"

        ok, msg = finished.result()
        return ok, msg
    finally:
        try:
            try:
                await asyncio.wait_for(context.close(), timeout=15)
            except TimeoutError:
                pass
        finally:
            shutil.rmtree(user_data_dir, ignore_errors=True)
            shutil.rmtree(downloads_dir, ignore_errors=True)

async def download_pdf_with_fallbacks(
    playwright,
    context: BrowserContext,
    page,
    pdf_url: str,
    out_path: Path,
    *,
    manual_assist: bool | None = None,
    log=None,
):
    def _log(message: str):
        if log is not None:
            log(message)

    _log("Trying direct download.")
    ok, msg = try_download_pdf_via_requests(pdf_url, out_path)
    if ok:
        _log(f"Saved via direct download: {out_path}")
        return True, msg, None

    first_failure = msg
    _log(f"Direct download failed: {concise_reason(msg)}")

    _log("Trying browser request.")
    ok2, msg2 = await try_download_pdf_via_playwright_request(context, pdf_url, out_path)

    if ok2:
        _log(f"Saved via browser request: {out_path}")
        return True, msg2, first_failure
    _log(f"Browser request failed: {concise_reason(msg2)}")

    _log("Trying browser tab.")
    if not page.is_closed():
        ok3, msg3 = await try_download_pdf_via_playwright_page(context, page, pdf_url, out_path)
    else:
        page = await context.new_page()
        ok3, msg3 = await try_download_pdf_via_playwright_page(context, page, pdf_url, out_path)

    if ok3:
        _log(f"Saved via browser tab: {out_path}")
        return True, msg3, first_failure
    _log(f"Browser tab failed: {concise_reason(msg3)}")

    allow_manual = MANUAL_ASSIST if manual_assist is None else bool(manual_assist)
    if allow_manual:
        _log("Trying manual assist.")
        ok4, msg4 = await try_download_with_manual_assist(playwright, pdf_url, out_path)
        if ok4:
            _log(f"Saved via manual assist: {out_path}")
            return True, msg4, first_failure
        _log(f"Manual assist failed: {concise_reason(msg4)}")
        return False, f"{msg2}; then {msg3}; then {msg4}", first_failure

    return False, f"{msg2}; then {msg3}", first_failure

async def main():
    started_at = datetime.now()
    started_perf = time.perf_counter()
    exit_code = 0
    safe_print(f"Pipeline started at {format_timestamp(started_at)}")

    try:
        papers = load_papers(PAPER_LINKS_DIR)
        if not papers:
            safe_print(f"No paper JSON files found in: {PAPER_LINKS_DIR}")
            exit_code = 2
            return exit_code

        safe_print(f"Loaded {len(papers)} paper entries from {PAPER_LINKS_DIR}.")
        results = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()

            total = len(papers)
            saved_count = 0
            failed_count = 0
            skipped_count = 0

            def log(message: str):
                safe_print(message)

            def manual_assist_aborted(msg: str | None) -> bool:
                m = (msg or "").lower()
                return ("manual-assist: canceled by user" in m) or ("manual-assist: browser closed by user" in m)

            for idx_item, item in enumerate(papers, start=1):
                if isinstance(item, str):
                    doi = item
                    source_preference = None
                    explicit_pdf_url = None
                    ingredient = "unknown_ingredient"
                    input_filename = None
                    source_url = None
                else:
                    doi = (item or {}).get("doi")
                    source_preference = (item or {}).get("source")
                    explicit_pdf_url = (item or {}).get("pdf_url")
                    ingredient = (item or {}).get("ingredient") or "unknown_ingredient"
                    input_filename = (item or {}).get("filename")
                    source_url = (item or {}).get("source_url")

                doi_str = str(doi).strip()
                doi_present = bool(doi_str) and doi_str.lower() not in ("n/a", "na", "none", "null")

                explicit_pdf_url = (str(explicit_pdf_url).strip() if explicit_pdf_url else None) or None

                doi_is_oa = None
                doi_pdf_url = None
                doi_resolved_source = None
                doi_host_type = None
                doi_resolve_err = None

                # Resolve DOI (if present) to a PDF URL and also capture a hostname-based resolved_source for debugging.
                if doi_present:
                    try:
                        doi_is_oa, doi_pdf_url, doi_resolved_source, doi_host_type, doi_resolve_err = resolve_pdf_source(
                            doi_str,
                            EMAIL,
                            source_preference,
                        )
                    except Exception as e:
                        doi_pdf_url = None
                        doi_resolve_err = f"DOI resolve: {type(e).__name__}: {e}"

                # Folder grouping always follows the input paper's "source".
                output_source = safe_dirname(str(source_preference or "unknown_source"))
                ingredient = safe_dirname(str(ingredient))

                label = paper_label(
                    idx_item=idx_item,
                    total=total,
                    ingredient=ingredient,
                    source=output_source,
                    doi=doi_str if doi_str else None,
                )

                log("")
                log(label)
                if doi_present:
                    if doi_pdf_url:
                        details = f"DOI resolved to a PDF URL"
                        if doi_resolved_source:
                            details += f" via {doi_resolved_source}"
                        log(details + ".")
                    else:
                        log(f"DOI lookup failed: {concise_reason(doi_resolve_err or 'unknown')}")
                else:
                    log("No DOI provided. Using the explicit PDF URL if available.")
                if explicit_pdf_url:
                    log(f"Fallback PDF URL available via {source_key(None, explicit_pdf_url)}.")

                row = {
                    "doi": doi,
                    "is_oa": None,
                    "pdf_url": None,
                    "input_pdf_url": explicit_pdf_url,
                    "doi_present": doi_present,
                    "doi_is_oa": doi_is_oa,
                    "doi_pdf_url": doi_pdf_url,
                    "doi_resolved_source": doi_resolved_source,
                    "doi_host_type": doi_host_type,
                    "doi_resolve_error": doi_resolve_err,
                    "ingredient": ingredient,
                    "source": output_source,
                    "resolved_source": None,
                    "host_type": None,
                    "resolve_error": doi_resolve_err,
                    "ok": False,
                    "msg": None,
                }

                # Fill initial resolution fields (may change if we fall back to explicit pdf_url).
                row["resolved_source"] = doi_resolved_source if doi_pdf_url else (source_key(None, explicit_pdf_url) if explicit_pdf_url else None)
                row["host_type"] = doi_host_type if doi_pdf_url else None
                row["resolve_error"] = doi_resolve_err

                if not (doi_pdf_url or explicit_pdf_url):
                    if (source_preference or "").strip().lower() in ("google_scholar", "scholar", "google scholar"):
                        row["msg"] = "No DOI-resolved PDF and no explicit pdf_url; Google Scholar auto-download is not supported."
                    else:
                        row["msg"] = doi_resolve_err or "No DOI-resolved PDF and no explicit pdf_url"
                    failed_count += 1
                    log(f"Failed: {concise_reason(row['msg'])}")
                    results.append(row)
                    continue

                out_dir = OUT_DIR / ingredient / output_source
                out_dir.mkdir(parents=True, exist_ok=True)

                if input_filename:
                    filename = safe_filename(str(input_filename))
                else:
                    filename = default_filename(doi_str if doi_present else None, (doi_pdf_url or explicit_pdf_url))

                out_path = ensure_unique_path(out_dir / filename)

                attempts: list[dict] = []

                async def run_one(url: str, kind: str, *, is_oa_val, resolved_src, host_t, resolve_error):
                    label = "DOI URL" if kind == "doi" else "fallback PDF URL"
                    log(f"Attempting {label}.")
                    ok, msg, first_failure = await download_pdf_with_fallbacks(
                        p,
                        context,
                        page,
                        url,
                        out_path,
                        manual_assist=bool(MANUAL_ASSIST),
                        log=log,
                    )
                    aborted = (not ok) and manual_assist_aborted(msg)
                    attempts.append(
                        {
                            "kind": kind,
                            "pdf_url": url,
                            "resolved_source": resolved_src,
                            "host_type": host_t,
                            "resolve_error": resolve_error,
                            "manual_assist": bool(MANUAL_ASSIST),
                            "ok": ok,
                            "msg": msg,
                            "first_failure": first_failure,
                            "aborted": aborted,
                        }
                    )
                    row["first_failure"] = first_failure
                    if ok:
                        row["ok"] = True
                        row["msg"] = str(out_path)
                        row["is_oa"] = is_oa_val
                        row["pdf_url"] = url
                        row["resolved_source"] = resolved_src
                        row["host_type"] = host_t
                        row["resolve_error"] = resolve_error
                    return ok, msg, aborted

                if doi_pdf_url:
                    ok, msg, aborted = await run_one(
                        doi_pdf_url,
                        "doi",
                        is_oa_val=doi_is_oa,
                        resolved_src=doi_resolved_source,
                        host_t=doi_host_type,
                        resolve_error=doi_resolve_err,
                    )
                    if aborted:
                        row["attempts"] = attempts
                        row["msg"] = msg
                        skipped_count += 1
                        log(f"Skipped: {concise_reason(msg)}")
                        results.append(row)
                        continue

                if (not row["ok"]) and explicit_pdf_url:
                    ok, msg, aborted = await run_one(
                        explicit_pdf_url,
                        "explicit",
                        is_oa_val=True,
                        resolved_src=source_key(None, explicit_pdf_url),
                        host_t=None,
                        resolve_error=None,
                    )
                    if aborted:
                        row["attempts"] = attempts
                        row["msg"] = msg
                        skipped_count += 1
                        log(f"Skipped: {concise_reason(msg)}")
                        results.append(row)
                        continue

                if not row["ok"]:
                    row["attempts"] = attempts
                    row["msg"] = attempts[-1]["msg"] if attempts else (doi_resolve_err or "No OA PDF found")
                    failed_count += 1
                    log(f"Failed: {concise_reason(row['msg'])}")
                    results.append(row)
                else:
                    row["attempts"] = attempts
                    saved_count += 1
                    log(f"Saved: {out_path}")
                    results.append(row)

            await context.close()
            await browser.close()

        safe_print("")
        safe_print(f"Download summary: total={total}, saved={saved_count}, failed={failed_count}, skipped={skipped_count}")
        return exit_code
    finally:
        ended_at = datetime.now()
        elapsed = time.perf_counter() - started_perf
        safe_print(f"Pipeline ended at {format_timestamp(ended_at)}")
        safe_print(f"Total pipeline time: {format_duration(elapsed)}")

if __name__ == "__main__":
    asyncio.run(main())
