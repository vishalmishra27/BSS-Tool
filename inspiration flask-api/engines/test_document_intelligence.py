"""
Document Intelligence Test Engine
==================================
Tests the Document_Intelligence module end-to-end using the SAME
extract_text() and parse_document() wrappers that TOD/TOE engines use.

Credentials are loaded from config.py (edit once, used everywhere).

Usage:
    1. Fill in engines/config.py with your API keys.
    2. Set INPUT_PATH and OUTPUT_PATH below.
    3. Run:  python test_document_intelligence.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

# ── Load centralised config (API keys) ──
try:
    import config as _config  # noqa: F401
except ImportError:
    pass  # config.py is optional — falls back to env vars

# ═══════════════════════════════════════════════════════════════════════════════
#  TEST-SPECIFIC SETTINGS (credentials come from config.py)
# ═══════════════════════════════════════════════════════════════════════════════

# Input: single file path OR folder path (all files in folder will be tested)
INPUT_PATH  = ""

# Output: folder where results are saved (extracted text, parsed JSON, report)
OUTPUT_PATH = ""

# Options
VERBOSE          = False   # print extracted text to console
RECURSIVE        = False   # recurse into subfolders when INPUT_PATH is a directory
SAVE_EXTRACTED   = True    # save .txt files of extracted text to OUTPUT_PATH
SAVE_REPORT      = True    # save test_report.json to OUTPUT_PATH

# ── Resolve import path (same as TOD/TOE engines) ──
_engine_dir = Path(__file__).resolve().parent.parent
if str(_engine_dir) not in sys.path:
    sys.path.insert(0, str(_engine_dir))

from Document_Intelligence import (
    ALLOWED_EXTENSIONS,
    ParseStatus,
    extract_text,       # <-- same function TOD/TOE _extract_file() calls
    parse_document,     # <-- full detailed parse
)


# ── ANSI colours ──
_GREEN  = "\033[92m"
_RED    = "\033[91m"
_YELLOW = "\033[93m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_RESET  = "\033[0m"

def _ok(msg):   return f"  {_GREEN}PASS{_RESET}  {msg}"
def _fail(msg): return f"  {_RED}FAIL{_RESET}  {msg}"
def _warn(msg): return f"  {_YELLOW}WARN{_RESET}  {msg}"
def _info(msg): return f"  {_CYAN}INFO{_RESET}  {msg}"


# ═══════════════════════════════════════════════════════════════════════════════
#  1. ENVIRONMENT CHECKS
# ═══════════════════════════════════════════════════════════════════════════════

def check_environment() -> list[dict]:
    results: list[dict] = []

    print(f"\n{_BOLD}{'=' * 60}")
    print(" ENVIRONMENT & CONFIGURATION CHECKS")
    print(f"{'=' * 60}{_RESET}\n")

    # -- Doc Intelligence creds --
    di_key = os.environ.get("AZURE_DOC_INTELLIGENCE_KEY", "")
    di_ep  = os.environ.get("AZURE_DOC_INTELLIGENCE_ENDPOINT", "")

    if di_key:
        masked = di_key[:4] + "****" + di_key[-4:] if len(di_key) > 8 else "****"
        print(_ok(f"Doc Intelligence KEY: {masked}"))
        results.append({"check": "di_key", "status": "pass"})
    else:
        print(_fail("Doc Intelligence KEY not set  ->  set DI_KEY in CONFIG"))
        results.append({"check": "di_key", "status": "fail"})

    if di_ep:
        print(_ok(f"Doc Intelligence ENDPOINT: {di_ep}"))
        results.append({"check": "di_endpoint", "status": "pass"})
    else:
        print(_fail("Doc Intelligence ENDPOINT not set  ->  set DI_ENDPOINT in CONFIG"))
        results.append({"check": "di_endpoint", "status": "fail"})

    # -- SDK --
    try:
        from azure.ai.documentintelligence import DocumentIntelligenceClient  # noqa: F401
        print(_ok("azure-ai-documentintelligence SDK installed"))
        results.append({"check": "sdk", "status": "pass"})
    except ImportError:
        print(_fail("SDK not installed  ->  pip install azure-ai-documentintelligence"))
        results.append({"check": "sdk", "status": "fail"})

    # -- Client instantiation --
    if di_key and di_ep:
        try:
            from azure.ai.documentintelligence import DocumentIntelligenceClient
            from azure.core.credentials import AzureKeyCredential
            DocumentIntelligenceClient(endpoint=di_ep, credential=AzureKeyCredential(di_key))
            print(_ok("Client instantiation successful"))
            results.append({"check": "client", "status": "pass"})
        except Exception as e:
            print(_fail(f"Client instantiation failed: {e}"))
            results.append({"check": "client", "status": "fail", "error": str(e)})

    # -- OpenAI (optional) --
    oai_ep  = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
    oai_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
    if oai_ep and oai_key:
        print(_ok("Azure OpenAI creds found (LLM refinement enabled)"))
        results.append({"check": "llm", "status": "pass"})
    else:
        print(_warn("Azure OpenAI creds not set (LLM refinement skipped)"))
        results.append({"check": "llm", "status": "warn"})

    print(f"\n{_info('Supported extensions:')}")
    print(f"       {', '.join(sorted(ALLOWED_EXTENSIONS))}")

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  2. TEST SINGLE FILE
# ═══════════════════════════════════════════════════════════════════════════════

def test_file(filepath: Path, output_dir: Path | None = None) -> dict:
    result: dict = {
        "file": str(filepath),
        "filename": filepath.name,
        "extension": filepath.suffix.lower(),
        "file_size_kb": round(filepath.stat().st_size / 1024, 1),
    }
    ext = filepath.suffix.lower()
    supported = ext in ALLOWED_EXTENSIONS
    result["format_supported"] = supported

    print(f"\n  {_BOLD}{filepath.name}{_RESET}  ({result['file_size_kb']} KB, {ext})")

    # ── extract_text() — the same call TOD/TOE engines make ──
    t0 = time.perf_counter()
    text, doc_type, ok = extract_text(str(filepath))
    ms = round((time.perf_counter() - t0) * 1000)

    result["extract_text"] = {
        "success": ok, "doc_type": doc_type,
        "text_length": len(text), "duration_ms": ms,
        "preview": text[:200] if text else "",
    }

    if ok:
        print(_ok(f"extract_text  ->  {doc_type}  |  {len(text):,} chars  |  {ms} ms"))
    else:
        print(_fail(f"extract_text  ->  {doc_type}  |  {ms} ms"))
        print(f"       {text[:300]}")

    # Save extracted text
    if output_dir and SAVE_EXTRACTED and ok and text:
        txt_out = output_dir / "extracted_text" / f"{filepath.stem}.txt"
        txt_out.parent.mkdir(parents=True, exist_ok=True)
        txt_out.write_text(text, encoding="utf-8")
        result["extracted_text_path"] = str(txt_out)

    # ── parse_document() — full detailed parse ──
    if supported:
        t0 = time.perf_counter()
        try:
            doc = parse_document(str(filepath))
            ms = round((time.perf_counter() - t0) * 1000)

            pd = {
                "success": doc.parse_status != ParseStatus.FAILED,
                "parse_status": doc.parse_status.value,
                "parser_used": doc.parse_meta.parser_used,
                "ocr_used": doc.parse_meta.ocr_used,
                "confidence": doc.parse_meta.confidence,
                "text_length": len(doc.full_text or ""),
                "section_count": len(doc.sections),
                "table_count": len(doc.tables),
                "duration_ms": ms,
                "warnings": doc.parse_meta.warnings,
                "errors": doc.errors or [],
            }
            meta = doc.metadata.to_dict() if doc.metadata else {}
            if meta:
                pd["metadata"] = meta
            result["parse_document"] = pd

            extras = []
            if doc.sections: extras.append(f"{len(doc.sections)} sections")
            if doc.tables:   extras.append(f"{len(doc.tables)} tables")
            if doc.parse_meta.ocr_used: extras.append("OCR")
            extra = f"  |  {', '.join(extras)}" if extras else ""
            status = doc.parse_status.value.upper()

            if doc.parse_status == ParseStatus.FAILED:
                print(_fail(f"parse_document  ->  {status}  |  {ms} ms"))
                for e in (doc.errors or []):
                    print(f"       {e}")
            elif doc.parse_status == ParseStatus.PARTIAL:
                print(_warn(f"parse_document  ->  {status}  |  {len(doc.full_text):,} chars  |  {ms} ms{extra}"))
            else:
                print(_ok(f"parse_document  ->  {status}  |  {len(doc.full_text):,} chars  |  {ms} ms{extra}"))

            for w in doc.parse_meta.warnings:
                print(f"       {_YELLOW}warning:{_RESET} {w}")

            # Save full parsed JSON
            if output_dir:
                json_out = output_dir / "parsed_documents" / f"{filepath.stem}.json"
                json_out.parent.mkdir(parents=True, exist_ok=True)
                json_out.write_text(json.dumps(doc.to_dict(), indent=2, default=str), encoding="utf-8")
                result["parsed_document_path"] = str(json_out)

        except Exception as e:
            ms = round((time.perf_counter() - t0) * 1000)
            print(_fail(f"parse_document  ->  EXCEPTION  |  {ms} ms"))
            print(f"       {e}")
            result["parse_document"] = {"success": False, "error": str(e), "duration_ms": ms}
    else:
        print(_warn(f"parse_document  ->  SKIPPED ({ext} not in supported list)"))

    # Verbose output
    if VERBOSE and ok and text:
        print(f"\n       {_CYAN}--- Extracted Text (first 500 chars) ---{_RESET}")
        for line in text[:500].splitlines():
            print(f"       {line}")
        if len(text) > 500:
            print(f"       {_CYAN}... ({len(text) - 500:,} more chars){_RESET}")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  3. TEST FOLDER
# ═══════════════════════════════════════════════════════════════════════════════

def test_folder(folder: Path, output_dir: Path | None = None) -> list[dict]:
    if RECURSIVE:
        files = sorted(f for f in folder.rglob("*") if f.is_file() and not f.name.startswith("."))
    else:
        files = sorted(f for f in folder.iterdir() if f.is_file() and not f.name.startswith("."))

    if not files:
        print(_warn(f"No files found in {folder}"))
        return []

    print(f"\n{_BOLD}{'=' * 60}")
    print(f" PARSING TESTS  ({len(files)} files)")
    print(f"{'=' * 60}{_RESET}")

    return [test_file(f, output_dir=output_dir) for f in files]


# ═══════════════════════════════════════════════════════════════════════════════
#  4. SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════

def print_summary(env_results: list[dict], file_results: list[dict]):
    print(f"\n{_BOLD}{'=' * 60}")
    print(" SUMMARY")
    print(f"{'=' * 60}{_RESET}\n")

    ep = sum(1 for r in env_results if r["status"] == "pass")
    ef = sum(1 for r in env_results if r["status"] == "fail")
    ew = sum(1 for r in env_results if r["status"] == "warn")
    print(f"  Environment:  {_GREEN}{ep} pass{_RESET}  {_RED}{ef} fail{_RESET}  {_YELLOW}{ew} warn{_RESET}")

    if not file_results:
        return

    by_ext: dict[str, dict] = {}
    tp = tf = tpa = 0
    for r in file_results:
        ext = r["extension"]
        if ext not in by_ext:
            by_ext[ext] = {"pass": 0, "fail": 0, "partial": 0, "total": 0}
        by_ext[ext]["total"] += 1
        if r.get("extract_text", {}).get("success"):
            by_ext[ext]["pass"] += 1; tp += 1
        elif r.get("parse_document", {}).get("parse_status") == "partial":
            by_ext[ext]["partial"] += 1; tpa += 1
        else:
            by_ext[ext]["fail"] += 1; tf += 1

    print(f"  Files tested: {len(file_results)}")
    print(f"  Results:      {_GREEN}{tp} pass{_RESET}  {_RED}{tf} fail{_RESET}  {_YELLOW}{tpa} partial{_RESET}\n")

    print(f"  {'Format':<10} {'Total':>6} {'Pass':>6} {'Fail':>6} {'Partial':>8}")
    print(f"  {'------':<10} {'-----':>6} {'----':>6} {'----':>6} {'-------':>8}")
    for ext in sorted(by_ext):
        s = by_ext[ext]
        print(f"  {ext:<10} {s['total']:>6} {s['pass']:>6} {s['fail']:>6} {s['partial']:>8}")

    durations = [r["extract_text"]["duration_ms"] for r in file_results if "extract_text" in r]
    if durations:
        print(f"\n  Timing (extract_text):")
        print(f"    Avg: {sum(durations) / len(durations):.0f} ms  |  Min: {min(durations)} ms  |  Max: {max(durations)} ms")

    failures = [r for r in file_results if not r.get("extract_text", {}).get("success")]
    if failures:
        print(f"\n  {_RED}Failed files:{_RESET}")
        for r in failures:
            print(f"    - {r['filename']}")


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{_BOLD}Document Intelligence Test Engine{_RESET}")
    print(f"{'─' * 40}")

    env_results = check_environment()
    file_results: list[dict] = []
    output_dir: Path | None = None

    if OUTPUT_PATH:
        output_dir = Path(OUTPUT_PATH)
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n{_info('Output directory:')} {output_dir.resolve()}")

    if not INPUT_PATH:
        print(_fail("INPUT_PATH not set  ->  edit the CONFIG section at the top of this file"))
        print_summary(env_results, file_results)
        sys.exit(1)

    target = Path(INPUT_PATH)
    if not target.exists():
        print(_fail(f"Path not found: {target}"))
        sys.exit(1)

    if target.is_file():
        print(f"\n{_BOLD}{'=' * 60}")
        print(" PARSING TEST  (single file)")
        print(f"{'=' * 60}{_RESET}")
        file_results.append(test_file(target, output_dir=output_dir))
    elif target.is_dir():
        file_results = test_folder(target, output_dir=output_dir)

    print_summary(env_results, file_results)

    if output_dir and SAVE_REPORT:
        report = {
            "config": {
                "di_endpoint": os.environ.get("AZURE_DOC_INTELLIGENCE_ENDPOINT", ""),
                "openai_endpoint": os.environ.get("AZURE_OPENAI_ENDPOINT", ""),
                "openai_deployment": os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME", ""),
                "input_path": INPUT_PATH,
                "output_path": OUTPUT_PATH,
            },
            "environment": env_results,
            "files": file_results,
            "summary": {
                "total": len(file_results),
                "passed": sum(1 for r in file_results if r.get("extract_text", {}).get("success")),
                "failed": sum(1 for r in file_results if not r.get("extract_text", {}).get("success")),
            },
        }
        report_path = output_dir / "test_report.json"
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(f"\n  Report saved to: {report_path.resolve()}")

    print()


if __name__ == "__main__":
    main()
