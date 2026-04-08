"""
Document Intelligence — BSS Tool document parser.

Routes all document parsing through Azure Document Intelligence (prebuilt-layout),
with lightweight stdlib fallbacks for CSV/TSV and email formats.

Decision logic (used by ocr_endpoints.py):
  - Readable PDFs (has text layer)  →  existing OCR path (pdfplumber, already implemented)
  - Scanned PDFs, images, DOCX, XLSX, PPTX, EML, MSG  →  this module

Supported formats:
  PDF, DOCX, DOC, XLSX, XLS, XLSM, PPTX, PNG, JPG, JPEG, TIFF, TIF, BMP,
  CSV, TSV, EML, MSG

Configuration (env vars):
  AZURE_DOC_INTELLIGENCE_KEY       or  AZURE_DOCUMENT_INTELLIGENCE_KEY
  AZURE_DOC_INTELLIGENCE_ENDPOINT  or  AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT
  DI_MAX_RETRIES          (default 3)
  DI_RETRY_BASE_DELAY     (default 2s)
  MAX_FILE_SIZE_MB        (default 50)
  PARSE_LOGGING           (default 1 — write per-doc logs to parse_logs/)
  PARSE_LOG_DIR           (default <this file's dir>/parse_logs)
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import tempfile
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# TYPES & DATACLASSES
# ═══════════════════════════════════════════════════════════════════════════════

class SectionType(str, Enum):
    HEADING      = "heading"
    PARAGRAPH    = "paragraph"
    TABLE        = "table"
    LIST         = "list"
    IMAGE        = "image"
    CODE         = "code"
    SLIDE        = "slide"
    EMAIL_HEADER = "email_header"


class ParseStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED  = "failed"


@dataclass
class Section:
    type:       SectionType
    text:       str               = ""
    level:      Optional[int]     = None
    page:       Optional[int]     = None
    headers:    Optional[list]    = None
    rows:       Optional[list]    = None
    items:      Optional[list]    = None
    confidence: Optional[float]   = None

    def to_dict(self) -> dict:
        d = {"type": self.type.value, "text": self.text}
        for k in ("level", "page", "headers", "rows", "items", "confidence"):
            v = getattr(self, k)
            if v is not None:
                d[k] = v
        return d


@dataclass
class TableData:
    headers:    list
    rows:       list
    page:       Optional[int]  = None
    sheet_name: Optional[str]  = None
    confidence: Optional[float] = None

    def to_dict(self) -> dict:
        d = {"headers": self.headers, "rows": self.rows}
        if self.page is not None:     d["page"] = self.page
        if self.sheet_name is not None: d["sheet_name"] = self.sheet_name
        if self.confidence is not None: d["confidence"] = self.confidence
        return d


@dataclass
class DocumentMetadata:
    author:        Optional[str]  = None
    page_count:    Optional[int]  = None
    language:      Optional[str]  = None
    created_at:    Optional[str]  = None
    subject:       Optional[str]  = None
    title:         Optional[str]  = None
    sheet_names:   Optional[list] = None
    slide_count:   Optional[int]  = None
    email_from:    Optional[str]  = None
    email_to:      Optional[list] = None
    email_date:    Optional[str]  = None
    email_subject: Optional[str]  = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class ParseMeta:
    parser_used:      str
    ocr_used:         bool         = False
    confidence:       float        = 1.0
    warnings:         list         = field(default_factory=list)
    parse_duration_ms: Optional[int] = None

    def to_dict(self) -> dict:
        d = {
            "parser_used":  self.parser_used,
            "ocr_used":     self.ocr_used,
            "confidence":   round(self.confidence, 4),
            "warnings":     self.warnings,
        }
        if self.parse_duration_ms is not None:
            d["parse_duration_ms"] = self.parse_duration_ms
        return d


@dataclass
class ParsedDocument:
    filename:     str
    file_type:    str
    full_text:    str
    sections:     list = field(default_factory=list)
    tables:       list = field(default_factory=list)
    metadata:     DocumentMetadata = field(default_factory=DocumentMetadata)
    parse_meta:   ParseMeta = field(default_factory=lambda: ParseMeta(parser_used="unknown"))
    parse_status: ParseStatus = ParseStatus.SUCCESS
    document_id:  str = field(default_factory=lambda: str(uuid.uuid4()))
    parsed_at:    str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    errors:       Optional[list] = None

    def to_dict(self) -> dict:
        result = {
            "document_id":  self.document_id,
            "filename":     self.filename,
            "file_type":    self.file_type,
            "parsed_at":    self.parsed_at,
            "parse_status": self.parse_status.value,
            "content": {
                "full_text": self.full_text,
                "sections":  [s.to_dict() for s in self.sections],
                "tables":    [t.to_dict() for t in self.tables],
                "metadata":  self.metadata.to_dict(),
            },
            "parse_meta": self.parse_meta.to_dict(),
        }
        if self.errors:
            result["errors"] = self.errors
        return result


# ═══════════════════════════════════════════════════════════════════════════════
# AZURE DOCUMENT INTELLIGENCE CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

_OCR_AVAILABLE   = False
_doc_client      = None
_MAX_RETRIES     = int(os.environ.get("DI_MAX_RETRIES", "3"))
_RETRY_BASE_DELAY = float(os.environ.get("DI_RETRY_BASE_DELAY", "2"))
_MAX_FILE_SIZE_MB = int(os.environ.get("MAX_FILE_SIZE_MB", "50"))

try:
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
    from azure.core.credentials import AzureKeyCredential

    _KEY = (os.environ.get("AZURE_DOC_INTELLIGENCE_KEY", "")
            or os.environ.get("AZURE_DOCUMENT_INTELLIGENCE_KEY", ""))
    _ENDPOINT = (os.environ.get("AZURE_DOC_INTELLIGENCE_ENDPOINT", "")
                 or os.environ.get("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT", ""))

    if _KEY and _ENDPOINT:
        _doc_client    = DocumentIntelligenceClient(
            endpoint=_ENDPOINT,
            credential=AzureKeyCredential(_KEY),
        )
        _OCR_AVAILABLE = True
        logger.info("Azure Document Intelligence initialised ✓")
    else:
        logger.warning(
            "Azure Document Intelligence: AZURE_DOC_INTELLIGENCE_KEY or "
            "AZURE_DOC_INTELLIGENCE_ENDPOINT not set — scanned document parsing disabled"
        )
except ImportError:
    logger.warning("azure-ai-documentintelligence not installed — scanned document parsing disabled")
except Exception as e:
    logger.warning("Azure Document Intelligence init failed: %s", e)


@dataclass
class OCRResult:
    text:       str   = ""
    confidence: float = 0.0
    success:    bool  = False
    warning:    Optional[str] = None
    char_count: int   = 0


def is_ocr_available() -> bool:
    return _OCR_AVAILABLE


def _call_doc_intelligence(file_bytes: bytes, filename: str):
    """Call Azure DI with exponential-backoff retry. Returns AnalyzeResult."""
    from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
    last_error = None
    for attempt in range(_MAX_RETRIES):
        try:
            poller = _doc_client.begin_analyze_document(
                "prebuilt-layout",
                AnalyzeDocumentRequest(bytes_source=file_bytes),
            )
            return poller.result()
        except Exception as e:
            last_error = e
            if attempt < _MAX_RETRIES - 1:
                delay = _RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "DI attempt %d/%d failed for %s: %s. Retrying in %.1fs…",
                    attempt + 1, _MAX_RETRIES, filename, e, delay,
                )
                time.sleep(delay)
    raise last_error


def _run_doc_intelligence(image_bytes: bytes, label: str) -> OCRResult:
    """Run DI on raw bytes and return an OCRResult."""
    try:
        result    = _call_doc_intelligence(image_bytes, label)
        full_text = (result.content or "").strip()
        if not full_text:
            return OCRResult(
                success=False,
                warning=f"{label}: Document Intelligence ran but no readable text detected",
            )
        confidences: list[float] = []
        if result.pages:
            for page in result.pages:
                if page.words:
                    for word in page.words:
                        if word.confidence is not None:
                            confidences.append(word.confidence)
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
        return OCRResult(
            text=full_text,
            confidence=avg_confidence,
            success=True,
            char_count=len(full_text),
        )
    except Exception as e:
        return OCRResult(success=False, warning=f"{label}: DI error — {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# BASE PARSER
# ═══════════════════════════════════════════════════════════════════════════════

_INFO_PREFIXES = (
    "OCR extracted text from",
    "Multi-sheet workbook:",
    "more than",
)


class BaseParser(ABC):
    parser_name: str = "base"

    def parse(self, file_path: str) -> ParsedDocument:
        path = Path(file_path)
        if not path.exists():
            return self._error_result(path.name, path.suffix.lstrip("."),
                                      f"File not found: {file_path}")
        start = time.monotonic()
        try:
            result = self._parse(file_path)
            result.parse_meta.parse_duration_ms = int((time.monotonic() - start) * 1000)
            if result.parse_meta.warnings and result.parse_status == ParseStatus.SUCCESS:
                has_real_warning = any(
                    not w.startswith(_INFO_PREFIXES) for w in result.parse_meta.warnings
                )
                if has_real_warning:
                    result.parse_status = ParseStatus.PARTIAL
            return result
        except Exception as e:
            return self._error_result(
                path.name, path.suffix.lstrip("."), str(e),
                duration_ms=int((time.monotonic() - start) * 1000),
            )

    @abstractmethod
    def _parse(self, file_path: str) -> ParsedDocument:
        ...

    def _error_result(self, filename: str, file_type: str, error: str,
                      duration_ms: Optional[int] = None) -> ParsedDocument:
        return ParsedDocument(
            filename=filename, file_type=file_type, full_text="",
            parse_status=ParseStatus.FAILED,
            parse_meta=ParseMeta(parser_used=self.parser_name,
                                 warnings=[error], parse_duration_ms=duration_ms),
            errors=[error],
        )


# ═══════════════════════════════════════════════════════════════════════════════
# DOCUMENT INTELLIGENCE PARSER  (PDF, DOCX, PPTX, images, …)
# ═══════════════════════════════════════════════════════════════════════════════

_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp"}
_LEGACY_FORMATS   = {".doc", ".xls", ".ppt"}


class DocIntelligenceParser(BaseParser):
    parser_name = "azure-doc-intelligence"

    def _parse(self, file_path: str) -> ParsedDocument:
        path     = Path(file_path)
        ext      = path.suffix.lower()
        warnings: list[str] = []

        if not _OCR_AVAILABLE:
            raise RuntimeError(
                "Azure Document Intelligence is not configured. "
                "Set AZURE_DOC_INTELLIGENCE_KEY and AZURE_DOC_INTELLIGENCE_ENDPOINT in .env"
            )

        file_size = path.stat().st_size
        if file_size > _MAX_FILE_SIZE_MB * 1024 * 1024:
            raise RuntimeError(
                f"File size ({file_size / (1024*1024):.1f} MB) exceeds "
                f"limit ({_MAX_FILE_SIZE_MB} MB). Increase MAX_FILE_SIZE_MB to process."
            )

        if ext in _LEGACY_FORMATS:
            modern = {".doc": ".docx", ".xls": ".xlsx", ".ppt": ".pptx"}.get(ext, ext)
            warnings.append(
                f"Legacy format '{ext}' may not be fully supported. "
                f"Consider converting to '{modern}' for best results."
            )

        with open(file_path, "rb") as f:
            file_bytes = f.read()

        result    = _call_doc_intelligence(file_bytes, path.name)
        full_text = (result.content or "").strip()
        tables    = self._extract_tables(result, warnings)
        sections  = self._extract_sections(result)

        # Build text representations of tables and append as sections
        table_text_parts: list[str] = []
        for td in tables:
            parts = []
            if td.sheet_name:
                parts.append(f"Sheet: {td.sheet_name}")
            if td.headers:
                parts.append(" | ".join(td.headers))
            for row in td.rows[:200]:
                parts.append(" | ".join(row))
            if len(td.rows) > 200:
                parts.append(f"... ({len(td.rows) - 200} more rows)")
            text_repr = "\n".join(parts)
            sections.append(Section(
                type=SectionType.TABLE, text=text_repr,
                headers=td.headers, rows=td.rows, page=td.page,
            ))
            table_text_parts.append(text_repr)

        if not full_text and table_text_parts:
            full_text = "\n\n".join(table_text_parts)
            warnings.append("No prose text detected — text reconstructed from tables")

        # OCR embedded images inside DOCX/PPTX
        if ext in (".docx", ".doc", ".pptx", ".ppt"):
            image_text = self._ocr_embedded_office_images(file_path, path.name, warnings)
            if image_text:
                full_text = (full_text + "\n\n" + image_text).strip() if full_text else image_text
                sections.append(Section(type=SectionType.IMAGE, text=image_text))

        if not full_text:
            warnings.append("Document Intelligence returned no text content")

        page_count    = len(result.pages) if result.pages else 1
        confidences: list[float] = []
        if result.pages:
            for page in result.pages:
                if page.words:
                    for word in page.words:
                        if word.confidence is not None:
                            confidences.append(word.confidence)
        avg_confidence = (sum(confidences) / len(confidences)
                          if confidences else (0.0 if not full_text else 1.0))

        ocr_used = ext in _IMAGE_EXTENSIONS
        if not ocr_used and ext in (".docx", ".doc", ".pptx", ".ppt"):
            ocr_used = any(s.type == SectionType.IMAGE for s in sections)
        if not ocr_used and confidences:
            ocr_used = any(c < 0.99 for c in confidences)

        metadata = DocumentMetadata(page_count=page_count)
        try:
            if hasattr(result, "languages") and result.languages:
                metadata.language = getattr(result.languages[0], "locale", None)
        except Exception:
            pass

        return ParsedDocument(
            filename=path.name, file_type=ext.lstrip("."),
            full_text=full_text, sections=sections, tables=tables,
            metadata=metadata,
            parse_meta=ParseMeta(
                parser_used="azure-doc-intelligence",
                ocr_used=ocr_used,
                confidence=avg_confidence,
                warnings=warnings,
            ),
        )

    def _extract_tables(self, result, warnings: list[str]) -> list[TableData]:
        tables: list[TableData] = []
        if not result.tables:
            return tables
        for idx, table in enumerate(result.tables):
            try:
                if table.row_count < 1 or table.column_count < 1:
                    continue
                grid = [[""] * table.column_count for _ in range(table.row_count)]
                for cell in table.cells:
                    r, c = cell.row_index, cell.column_index
                    if r < table.row_count and c < table.column_count:
                        grid[r][c] = (cell.content or "").strip()
                if not grid:
                    continue
                headers = grid[0]
                rows    = grid[1:]
                page_num = None
                if table.bounding_regions:
                    page_num = table.bounding_regions[0].page_number
                tables.append(TableData(headers=headers, rows=rows, page=page_num))
            except Exception as e:
                warnings.append(f"Table {idx+1} extraction failed: {e}")
        return tables

    def _extract_sections(self, result) -> list[Section]:
        sections: list[Section] = []
        if not hasattr(result, "paragraphs") or not result.paragraphs:
            return sections
        for para in result.paragraphs:
            text = (para.content or "").strip()
            if not text:
                continue
            role     = getattr(para, "role", None)
            page_num = None
            if para.bounding_regions:
                page_num = para.bounding_regions[0].page_number
            if role == "title":
                sections.append(Section(type=SectionType.HEADING, text=text, level=1, page=page_num))
            elif role == "sectionHeading":
                sections.append(Section(type=SectionType.HEADING, text=text, level=2, page=page_num))
            elif role in ("pageHeader", "pageFooter", "pageNumber"):
                continue
            else:
                sections.append(Section(type=SectionType.PARAGRAPH, text=text, page=page_num))
        return sections

    def _ocr_embedded_office_images(self, file_path: str, filename: str,
                                     warnings: list[str]) -> str:
        """Extract and OCR images embedded inside DOCX/PPTX ZIP archives."""
        import zipfile
        if not zipfile.is_zipfile(file_path):
            return ""
        _IMG_EXTS    = {".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".gif"}
        MIN_IMG_SIZE = 2000  # skip tiny icons/bullets
        ocr_parts:    list[str] = []
        images_found  = 0
        images_ocred  = 0
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                for entry in zf.namelist():
                    if "/media/" not in entry:
                        continue
                    ext_lower = Path(entry).suffix.lower()
                    if ext_lower not in _IMG_EXTS:
                        continue
                    img_bytes = zf.read(entry)
                    if len(img_bytes) < MIN_IMG_SIZE:
                        continue
                    images_found += 1
                    img_name = Path(entry).name
                    try:
                        ocr_result = _run_doc_intelligence(img_bytes, f"{filename}/{img_name}")
                        if ocr_result.success and ocr_result.text:
                            ocr_parts.append(f"[Image: {img_name}]\n{ocr_result.text}")
                            images_ocred += 1
                    except Exception as e:
                        logger.debug("OCR failed for embedded image %s: %s", img_name, e)
            if ocr_parts:
                warnings.append(f"OCR extracted text from {images_ocred}/{images_found} embedded images")
                return "\n\n".join(ocr_parts).strip()
            if images_found > 0:
                warnings.append(f"Found {images_found} embedded images but OCR returned no text")
        except Exception as e:
            warnings.append(f"Failed to extract embedded images: {e}")
        return ""


# ═══════════════════════════════════════════════════════════════════════════════
# CSV / TSV PARSER  (stdlib csv — no Azure DI call)
# ═══════════════════════════════════════════════════════════════════════════════

MAX_CSV_ROWS = 50_000


class CsvParser(BaseParser):
    parser_name = "text-reader"

    def _parse(self, file_path: str) -> ParsedDocument:
        warnings: list[str] = []
        path = Path(file_path)
        ext  = path.suffix.lower()

        encoding = self._detect_encoding(file_path)
        if encoding not in ("utf-8", "ascii"):
            warnings.append(f"Detected encoding: {encoding}")

        try:
            with open(file_path, "r", encoding=encoding, errors="replace") as f:
                sample = f.read(8192)
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV: {e}")

        delimiter = ","
        if ext == ".tsv":
            delimiter = "\t"
        elif sample.count("\t") > sample.count(","):
            delimiter = "\t"
        elif sample.count(";") > sample.count(","):
            delimiter = ";"
            warnings.append("Detected semicolon delimiter")

        all_rows: list[list[str]] = []
        try:
            with open(file_path, "r", encoding=encoding, errors="replace", newline="") as f:
                reader = csv.reader(f, delimiter=delimiter)
                for i, row in enumerate(reader):
                    if i >= MAX_CSV_ROWS:
                        warnings.append(f"File exceeds {MAX_CSV_ROWS} rows — truncated")
                        break
                    all_rows.append([str(c) for c in row])
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV: {e}")

        if not all_rows:
            return ParsedDocument(
                filename=path.name, file_type=ext.lstrip("."), full_text="",
                parse_meta=ParseMeta(parser_used="text-reader", warnings=["File is empty"]),
            )

        headers = all_rows[0]
        rows    = all_rows[1:]
        text_parts = [" | ".join(headers)]
        for row in rows[:500]:
            text_parts.append(" | ".join(row))
        if len(rows) > 500:
            text_parts.append(f"... ({len(rows) - 500} more rows)")

        return ParsedDocument(
            filename=path.name, file_type=ext.lstrip("."),
            full_text="\n".join(text_parts),
            sections=[Section(type=SectionType.TABLE,
                              text=f"CSV data: {len(rows)} rows x {len(headers)} columns",
                              headers=headers, rows=rows)],
            tables=[TableData(headers=headers, rows=rows)],
            metadata=DocumentMetadata(page_count=1),
            parse_meta=ParseMeta(parser_used="text-reader", warnings=warnings),
        )

    def _detect_encoding(self, file_path: str) -> str:
        try:
            with open(file_path, "rb") as f:
                raw = f.read(4096)
            if raw.startswith(b"\xef\xbb\xbf"):  return "utf-8-sig"
            if raw.startswith(b"\xff\xfe"):       return "utf-16-le"
            if raw.startswith(b"\xfe\xff"):       return "utf-16-be"
            raw.decode("utf-8")
            return "utf-8"
        except Exception:
            return "latin-1"


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL PARSER  (pandas / openpyxl — no Azure DI call)
# ═══════════════════════════════════════════════════════════════════════════════

MAX_EXCEL_ROWS = 100_000


class ExcelParser(BaseParser):
    parser_name = "excel-reader"

    def _parse(self, file_path: str) -> ParsedDocument:
        warnings: list[str] = []
        path = Path(file_path)
        ext  = path.suffix.lower()

        import pandas as pd

        engine = "xlrd" if ext == ".xls" else "openpyxl"
        try:
            sheets = pd.read_excel(file_path, sheet_name=None, dtype=str,
                                   header=None, engine=engine)
        except ImportError as ie:
            raise RuntimeError(f"Missing library for {ext}: {ie}. Run: pip install {engine}")
        except Exception as e:
            raise RuntimeError(f"Failed to read Excel file: {e}")

        if not sheets:
            return ParsedDocument(
                filename=path.name, file_type=ext.lstrip("."), full_text="",
                parse_meta=ParseMeta(parser_used="excel-reader", warnings=["No sheets found"]),
            )

        sheet_names = list(sheets.keys())
        all_tables:  list[TableData] = []
        all_sections: list[Section]  = []
        text_parts:  list[str]       = []

        for sheet_name in sheet_names:
            df = sheets[sheet_name]
            df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
            if df.empty:
                continue

            header_row = df.iloc[0].astype(str).tolist()
            headers = [
                str(h).strip() if str(h).strip().lower() not in ("nan", "none", "") else ""
                for h in header_row
            ]
            rows: list[list[str]] = []
            for _, row in df.iloc[1:].iterrows():
                vals = [str(v).strip() if str(v).strip().lower() not in ("nan", "none") else ""
                        for v in row]
                if all(v == "" for v in vals):
                    continue
                rows.append(vals)
                if len(rows) >= MAX_EXCEL_ROWS:
                    warnings.append(f"Sheet '{sheet_name}' truncated at {MAX_EXCEL_ROWS} rows")
                    break

            if len(sheet_names) > 1:
                text_parts.append(f"[Sheet: {sheet_name}]")
            text_parts.append(" | ".join(h for h in headers if h))
            for row in rows[:500]:
                text_parts.append(" | ".join(row))
            if len(rows) > 500:
                text_parts.append(f"... ({len(rows) - 500} more rows)")
            text_parts.append("")

            all_tables.append(TableData(headers=headers, rows=rows, sheet_name=str(sheet_name)))
            all_sections.append(Section(
                type=SectionType.TABLE,
                text=f"Sheet '{sheet_name}': {len(rows)} rows x {len(headers)} columns",
                headers=headers, rows=rows,
            ))

        if not all_tables:
            return ParsedDocument(
                filename=path.name, file_type=ext.lstrip("."), full_text="",
                parse_meta=ParseMeta(parser_used="excel-reader", warnings=["All sheets empty"]),
            )

        if len(sheet_names) > 1:
            warnings.append(
                f"Multi-sheet workbook: {len(sheet_names)} sheets "
                f"({', '.join(str(s) for s in sheet_names)})"
            )

        return ParsedDocument(
            filename=path.name, file_type=ext.lstrip("."),
            full_text="\n".join(text_parts).strip(),
            sections=all_sections, tables=all_tables,
            metadata=DocumentMetadata(
                page_count=len(sheet_names),
                sheet_names=[str(s) for s in sheet_names],
            ),
            parse_meta=ParseMeta(parser_used="excel-reader", warnings=warnings),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# EML PARSER  (stdlib email)
# ═══════════════════════════════════════════════════════════════════════════════

class EmlParser(BaseParser):
    parser_name = "email-parser"

    def _parse(self, file_path: str) -> ParsedDocument:
        from email import policy as email_policy
        from email.parser import BytesParser
        from html import unescape as html_unescape

        path     = Path(file_path)
        sections: list[Section] = []
        text_parts: list[str]   = []
        warnings: list[str]     = []

        with open(file_path, "rb") as f:
            msg = BytesParser(policy=email_policy.default).parse(f)

        header_lines = []
        for h in ("From", "To", "Cc", "Subject", "Date", "Message-ID"):
            v = msg.get(h, "")
            if v:
                header_lines.append(f"{h}: {v}")
        if header_lines:
            header_text = "\n".join(header_lines)
            sections.append(Section(type=SectionType.EMAIL_HEADER, text=header_text))
            text_parts.append(header_text)

        plain_parts: list[str] = []
        html_parts:  list[str] = []
        att_sections: list[str] = []

        if msg.is_multipart():
            for part in msg.walk():
                disp  = part.get_content_disposition()
                ctype = (part.get_content_type() or "").lower()
                if disp == "attachment":
                    fname = part.get_filename() or "attachment"
                    payload_bytes = part.get_payload(decode=True)
                    if payload_bytes:
                        ext = Path(fname).suffix.lower()
                        if ext and ext != ".eml" and is_supported(ext):
                            tmp_path = None
                            try:
                                with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                                    tmp.write(payload_bytes)
                                    tmp_path = Path(tmp.name)
                                att_result = parse_document(str(tmp_path))
                                if att_result.full_text:
                                    att_sections.append(
                                        f"[ATTACHMENT: {fname}]\n{att_result.full_text[:8000]}"
                                    )
                                else:
                                    warnings.append(f"Attachment '{fname}' returned no text")
                            except Exception as e:
                                warnings.append(f"Attachment '{fname}' failed: {e}")
                            finally:
                                try:
                                    if tmp_path:
                                        tmp_path.unlink(missing_ok=True)
                                except Exception:
                                    pass
                    continue
                try:
                    payload = part.get_content()
                except Exception:
                    continue
                if not payload:
                    continue
                if ctype == "text/plain":
                    plain_parts.append(str(payload).strip())
                elif ctype == "text/html":
                    txt = re.sub(r"<[^>]+>", " ", str(payload))
                    html_parts.append(html_unescape(txt).strip())
        else:
            ctype = (msg.get_content_type() or "").lower()
            try:
                payload = msg.get_content()
            except Exception:
                payload = None
            if payload:
                if ctype == "text/html":
                    txt = re.sub(r"<[^>]+>", " ", str(payload))
                    html_parts.append(html_unescape(txt).strip())
                else:
                    plain_parts.append(str(payload).strip())

        body = "\n\n".join(p for p in plain_parts if p).strip()
        if not body and html_parts:
            body = "\n\n".join(p for p in html_parts if p).strip()
        if body:
            sections.append(Section(type=SectionType.PARAGRAPH, text=body))
            text_parts.append(body)
        for att in att_sections:
            sections.append(Section(type=SectionType.PARAGRAPH, text=att))
            text_parts.append(att)

        metadata = DocumentMetadata(
            email_from=msg.get("From", ""),
            email_to=[a.strip() for a in (msg.get("To", "") or "").split(",") if a.strip()],
            email_date=msg.get("Date", ""),
            email_subject=msg.get("Subject", ""),
            page_count=1,
        )
        return ParsedDocument(
            filename=path.name, file_type="eml",
            full_text="\n\n".join(text_parts),
            sections=sections, tables=[],
            metadata=metadata,
            parse_meta=ParseMeta(parser_used="email-parser", warnings=warnings),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# MSG PARSER  (Outlook .msg — optional: pip install extract-msg)
# ═══════════════════════════════════════════════════════════════════════════════

class MsgParser(BaseParser):
    parser_name = "extract-msg"

    def _parse(self, file_path: str) -> ParsedDocument:
        try:
            import extract_msg
        except ImportError:
            raise RuntimeError(
                "extract-msg is not installed. "
                "Install it with: pip install extract-msg"
            )
        sections:   list[Section] = []
        text_parts: list[str]     = []
        warnings:   list[str]     = []

        msg = extract_msg.Message(file_path)
        try:
            sender  = msg.sender  or ""
            to      = msg.to      or ""
            cc      = msg.cc      or ""
            subject = msg.subject or ""
            date    = msg.date    or ""

            header_text = f"From: {sender}\nTo: {to}\nCC: {cc}\nDate: {date}\nSubject: {subject}"
            sections.append(Section(type=SectionType.EMAIL_HEADER, text=header_text))
            text_parts.append(header_text)

            body = (msg.body or "").strip()
            if body:
                sections.append(Section(type=SectionType.PARAGRAPH, text=body))
                text_parts.append(body)
            else:
                html_body = msg.htmlBody
                if html_body:
                    plain = re.sub(r"<[^>]+>", " ",
                                   html_body.decode("utf-8", errors="replace"))
                    plain = re.sub(r"\s+", " ", plain).strip()
                    if plain:
                        sections.append(Section(type=SectionType.PARAGRAPH, text=plain))
                        text_parts.append(plain)
                        warnings.append("Used HTML body (no plain text body found)")

            att_names = []
            for att in msg.attachments:
                att_name = (getattr(att, "longFilename", None)
                            or getattr(att, "shortFilename", "unknown"))
                att_names.append(att_name)
            if att_names:
                att_text = "Attachments: " + ", ".join(att_names)
                sections.append(Section(type=SectionType.PARAGRAPH, text=att_text))
                text_parts.append(att_text)

            to_list   = [a.strip() for a in to.split(";") if a.strip()] if to else []
            metadata  = DocumentMetadata(
                email_from=sender, email_to=to_list,
                email_date=str(date), email_subject=subject, page_count=1,
            )
        finally:
            msg.close()

        return ParsedDocument(
            filename=Path(file_path).name, file_type="msg",
            full_text="\n\n".join(text_parts),
            sections=sections, tables=[], metadata=metadata,
            parse_meta=ParseMeta(parser_used="extract-msg", warnings=warnings),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PARSER REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

_PARSER_MAP: dict[str, type[BaseParser]] = {
    # Azure DI handles all document / image formats
    ".pdf":  DocIntelligenceParser,
    ".docx": DocIntelligenceParser,
    ".doc":  DocIntelligenceParser,
    ".pptx": DocIntelligenceParser,
    ".png":  DocIntelligenceParser,
    ".jpg":  DocIntelligenceParser,
    ".jpeg": DocIntelligenceParser,
    ".tiff": DocIntelligenceParser,
    ".tif":  DocIntelligenceParser,
    ".bmp":  DocIntelligenceParser,
    # Excel via pandas (reliable for all sheet types)
    ".xlsx": ExcelParser,
    ".xls":  ExcelParser,
    ".xlsm": ExcelParser,
    # Plain-text via stdlib
    ".csv":  CsvParser,
    ".tsv":  CsvParser,
    # Email
    ".eml":  EmlParser,
    ".msg":  MsgParser,
}

ALLOWED_EXTENSIONS: set[str] = set(_PARSER_MAP.keys())

# Human-readable label for each extension (used in UI messages)
FORMAT_LABELS: dict[str, str] = {
    ".pdf": "PDF", ".docx": "Word", ".doc": "Word (legacy)",
    ".pptx": "PowerPoint", ".xlsx": "Excel", ".xls": "Excel (legacy)",
    ".xlsm": "Excel", ".csv": "CSV", ".tsv": "TSV",
    ".png": "PNG image", ".jpg": "JPEG image", ".jpeg": "JPEG image",
    ".tiff": "TIFF image", ".tif": "TIFF image", ".bmp": "BMP image",
    ".eml": "Email (.eml)", ".msg": "Outlook email (.msg)",
}


def get_parser(extension: str) -> BaseParser:
    ext = extension.lower() if extension.startswith(".") else f".{extension.lower()}"
    if ext not in _PARSER_MAP:
        raise ValueError(
            f"Unsupported file type: '{ext}'. "
            f"Supported: {sorted(ALLOWED_EXTENSIONS)}"
        )
    return _PARSER_MAP[ext]()


def is_supported(extension: str) -> bool:
    ext = extension.lower() if extension.startswith(".") else f".{extension.lower()}"
    return ext in _PARSER_MAP


# ═══════════════════════════════════════════════════════════════════════════════
# PARSE LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

_PARSE_LOGGING_ENABLED = (
    os.environ.get("PARSE_LOGGING", "1").strip().lower() in {"1", "true", "yes", "on"}
)
_PARSE_LOG_DIR = os.environ.get("PARSE_LOG_DIR", "")

# One folder per server process so logs from concurrent requests don't interleave
_SESSION_FOLDER: Optional[str] = None


def _get_session_folder() -> str:
    global _SESSION_FOLDER
    if _SESSION_FOLDER is None:
        _SESSION_FOLDER = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    return _SESSION_FOLDER


def _get_parse_log_dir() -> Optional[Path]:
    if not _PARSE_LOGGING_ENABLED:
        return None
    if _PARSE_LOG_DIR:
        base = Path(_PARSE_LOG_DIR)
    else:
        base = Path(__file__).resolve().parent / "parse_logs"
    doc_dir = base / _get_session_folder() / "documents"
    try:
        doc_dir.mkdir(parents=True, exist_ok=True)
        return doc_dir
    except Exception as e:
        logger.debug("Could not create parse log dir %s: %s", doc_dir, e)
        return None


def _write_parse_log(result: ParsedDocument, file_path: str) -> None:
    log_dir = _get_parse_log_dir()
    if log_dir is None:
        return
    try:
        ts        = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_name = re.sub(r"[^\w.\-]", "_", result.filename)
        log_path  = log_dir / f"{ts}_{safe_name}.log"

        try:
            size_bytes = Path(file_path).stat().st_size
            if size_bytes >= 1024 * 1024:
                size_str = f"{size_bytes / (1024*1024):.2f} MB"
            elif size_bytes >= 1024:
                size_str = f"{size_bytes / 1024:.1f} KB"
            else:
                size_str = f"{size_bytes} bytes"
        except Exception:
            size_str = "unknown"

        sep  = "═" * 70
        thin = "─" * 70
        lines = [
            sep, "BSS TOOL — DOCUMENT PARSE LOG", sep,
            f"Timestamp:      {result.parsed_at}",
            f"Source Path:    {file_path}",
            f"Filename:       {result.filename}",
            f"File Type:      {result.file_type}",
            f"File Size:      {size_str}",
            f"Document ID:    {result.document_id}",
            f"Parser Used:    {result.parse_meta.parser_used}",
            f"Parse Status:   {result.parse_status.value.upper()}",
            f"Confidence:     {result.parse_meta.confidence:.4f}",
            f"OCR Used:       {result.parse_meta.ocr_used}",
            f"Duration:       {result.parse_meta.parse_duration_ms or 0}ms",
            f"Warnings:       {result.parse_meta.warnings or 'None'}",
            f"Errors:         {result.errors or 'None'}",
            "",
            thin, "METADATA", thin,
        ]

        meta = result.metadata
        for label, val in [
            ("Page Count", meta.page_count), ("Language", meta.language),
            ("Author", meta.author), ("Title", meta.title),
            ("Subject", meta.subject), ("Created At", meta.created_at),
            ("Sheet Names", meta.sheet_names), ("Slide Count", meta.slide_count),
            ("Email From", meta.email_from), ("Email To", meta.email_to),
            ("Email Date", meta.email_date), ("Email Subject", meta.email_subject),
        ]:
            if val is not None:
                lines.append(f"{label:<16s}  {val}")
        lines.append("")

        lines += [thin, f"SECTIONS ({len(result.sections)} total)", thin]
        for idx, sec in enumerate(result.sections, 1):
            page_str = f" (page {sec.page})" if sec.page else ""
            preview  = sec.text[:120].replace("\n", " ").strip()
            if len(sec.text) > 120:
                preview += "..."
            lines.append(f"[{idx}] {sec.type.value.upper()}{page_str}: {preview}")
        lines.append("")

        lines += [thin, f"TABLES ({len(result.tables)} total)", thin]
        for idx, tbl in enumerate(result.tables, 1):
            page_str  = f" (page {tbl.page})" if tbl.page else ""
            sheet_str = f" [{tbl.sheet_name}]" if tbl.sheet_name else ""
            lines.append(
                f"Table {idx}{page_str}{sheet_str}: "
                f"{len(tbl.headers)} cols x {len(tbl.rows)} rows"
            )
            lines.append(f"  Headers: {' | '.join(tbl.headers[:10])}")
            if len(tbl.headers) > 10:
                lines.append(f"  ... ({len(tbl.headers)-10} more columns)")
            for ri, row in enumerate(tbl.rows[:5], 1):
                lines.append(f"  Row {ri}: {' | '.join(row[:10])}")
            if len(tbl.rows) > 5:
                lines.append(f"  ... ({len(tbl.rows)-5} more rows)")
            lines.append("")

        lines += [sep, "FULL PARSED TEXT", sep, result.full_text or "(empty)", "", sep,
                  f"END OF LOG — {result.filename}", sep]

        log_path.write_text("\n".join(lines), encoding="utf-8")
        logger.debug("Parse log written: %s", log_path)
    except Exception as e:
        logger.debug("Failed to write parse log for %s: %s", result.filename, e)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def parse_document(file_path: str, document_id: Optional[str] = None) -> ParsedDocument:
    """
    Parse any supported document and return a ParsedDocument.

    Routing:
      - .csv/.tsv          → CsvParser   (stdlib)
      - .eml               → EmlParser   (stdlib)
      - .msg               → MsgParser   (optional extract-msg)
      - .xlsx/.xls/.xlsm   → ExcelParser (pandas/openpyxl)
      - everything else    → DocIntelligenceParser (Azure DI)
    """
    path = Path(file_path)

    if not path.exists():
        result = ParsedDocument(
            filename=path.name, file_type=path.suffix.lstrip("."), full_text="",
            parse_status=ParseStatus.FAILED,
            parse_meta=ParseMeta(parser_used="none"),
            errors=[f"File not found: {file_path}"],
        )
        _write_parse_log(result, file_path)
        return result

    ext = path.suffix.lower()
    if not is_supported(ext):
        result = ParsedDocument(
            filename=path.name, file_type=ext.lstrip("."), full_text="",
            parse_status=ParseStatus.FAILED,
            parse_meta=ParseMeta(parser_used="none"),
            errors=[f"Unsupported file type '{ext}'. Supported: {sorted(ALLOWED_EXTENSIONS)}"],
        )
        _write_parse_log(result, file_path)
        return result

    try:
        parser = get_parser(ext)
        result = parser.parse(file_path)
        if document_id:
            result.document_id = document_id
        _write_parse_log(result, file_path)
        logger.info(
            "Parsed %s → %s | parser=%s, %d chars, %d tables, %dms",
            path.name, result.parse_status.value.upper(),
            result.parse_meta.parser_used, len(result.full_text or ""),
            len(result.tables), result.parse_meta.parse_duration_ms or 0,
        )
        return result
    except Exception as e:
        logger.exception("Unexpected error parsing %s", file_path)
        result = ParsedDocument(
            filename=path.name, file_type=ext.lstrip("."), full_text="",
            parse_status=ParseStatus.FAILED,
            parse_meta=ParseMeta(parser_used="none"),
            errors=[f"Unexpected error: {e}"],
        )
        _write_parse_log(result, file_path)
        return result
