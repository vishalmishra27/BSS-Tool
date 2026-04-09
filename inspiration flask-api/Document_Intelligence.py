"""
Document Parser — Library for parsing evidence documents.
Uses Azure Document Intelligence as the primary parser for all document formats,
eliminating the need for format-specific libraries (pdfplumber, openpyxl, python-docx, etc.).

Supports: PDF, XLSX, XLS, XLSM, CSV, TSV, DOCX, PPTX, MSG, PNG, JPG, JPEG, TIFF, BMP, EML

Usage (library):
    from Document_Intelligence import parse_document, extract_text
    result = parse_document("/path/to/file.pdf")
    text, doc_type, ok = extract_text("/path/to/file.pdf")

Configuration (env vars — set whichever naming convention you prefer):
    AZURE_DOC_INTELLIGENCE_KEY        or  AZURE_DOCUMENT_INTELLIGENCE_KEY
    AZURE_DOC_INTELLIGENCE_ENDPOINT   or  AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import tempfile
import time
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# BASE TYPES & DATACLASSES
# ═══════════════════════════════════════════════════════════════════════════════

class SectionType(str, Enum):
    HEADING = "heading"
    PARAGRAPH = "paragraph"
    TABLE = "table"
    LIST = "list"
    IMAGE = "image"
    CODE = "code"
    SLIDE = "slide"
    EMAIL_HEADER = "email_header"


class ParseStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass
class Section:
    type: SectionType
    text: str = ""
    level: Optional[int] = None
    page: Optional[int] = None
    headers: Optional[list[str]] = None
    rows: Optional[list[list[str]]] = None
    items: Optional[list[str]] = None
    confidence: Optional[float] = None

    def to_dict(self) -> dict:
        d = {"type": self.type.value, "text": self.text}
        if self.level is not None:
            d["level"] = self.level
        if self.page is not None:
            d["page"] = self.page
        if self.headers is not None:
            d["headers"] = self.headers
        if self.rows is not None:
            d["rows"] = self.rows
        if self.items is not None:
            d["items"] = self.items
        if self.confidence is not None:
            d["confidence"] = self.confidence
        return d


@dataclass
class TableData:
    headers: list[str]
    rows: list[list[str]]
    page: Optional[int] = None
    sheet_name: Optional[str] = None
    confidence: Optional[float] = None

    def to_dict(self) -> dict:
        d = {"headers": self.headers, "rows": self.rows}
        if self.page is not None:
            d["page"] = self.page
        if self.sheet_name is not None:
            d["sheet_name"] = self.sheet_name
        if self.confidence is not None:
            d["confidence"] = self.confidence
        return d


@dataclass
class DocumentMetadata:
    author: Optional[str] = None
    page_count: Optional[int] = None
    language: Optional[str] = None
    created_at: Optional[str] = None
    subject: Optional[str] = None
    title: Optional[str] = None
    sheet_names: Optional[list[str]] = None
    slide_count: Optional[int] = None
    email_from: Optional[str] = None
    email_to: Optional[list[str]] = None
    email_date: Optional[str] = None
    email_subject: Optional[str] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class ParseMeta:
    parser_used: str
    ocr_used: bool = False
    confidence: float = 1.0
    warnings: list[str] = field(default_factory=list)
    parse_duration_ms: Optional[int] = None

    def to_dict(self) -> dict:
        d = {
            "parser_used": self.parser_used,
            "ocr_used": self.ocr_used,
            "confidence": round(self.confidence, 4),
            "warnings": self.warnings,
        }
        if self.parse_duration_ms is not None:
            d["parse_duration_ms"] = self.parse_duration_ms
        return d


@dataclass
class ParsedDocument:
    filename: str
    file_type: str
    full_text: str
    sections: list[Section] = field(default_factory=list)
    tables: list[TableData] = field(default_factory=list)
    metadata: DocumentMetadata = field(default_factory=DocumentMetadata)
    parse_meta: ParseMeta = field(default_factory=lambda: ParseMeta(parser_used="unknown"))
    parse_status: ParseStatus = ParseStatus.SUCCESS
    document_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parsed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    errors: Optional[list[str]] = None

    def to_dict(self) -> dict:
        result = {
            "document_id": self.document_id,
            "filename": self.filename,
            "file_type": self.file_type,
            "parsed_at": self.parsed_at,
            "parse_status": self.parse_status.value,
            "content": {
                "full_text": self.full_text,
                "sections": [s.to_dict() for s in self.sections],
                "tables": [t.to_dict() for t in self.tables],
                "metadata": self.metadata.to_dict(),
            },
            "parse_meta": self.parse_meta.to_dict(),
        }
        if self.errors:
            result["errors"] = self.errors
        return result


# ═══════════════════════════════════════════════════════════════════════════════
# AZURE DOCUMENT INTELLIGENCE ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

_OCR_AVAILABLE = False
_doc_client = None

_MAX_RETRIES = int(os.environ.get("DI_MAX_RETRIES", "3"))
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
        _doc_client = DocumentIntelligenceClient(
            endpoint=_ENDPOINT,
            credential=AzureKeyCredential(_KEY),
        )
        _OCR_AVAILABLE = True
    else:
        logger.warning(
            "Azure Document Intelligence: AZURE_DOC_INTELLIGENCE_KEY or "
            "AZURE_DOC_INTELLIGENCE_ENDPOINT not set — document parsing disabled"
        )
except ImportError:
    logger.warning("azure-ai-documentintelligence not installed — document parsing disabled")
except Exception as e:
    logger.warning("Azure Document Intelligence init failed: %s", e)


@dataclass
class OCRResult:
    text: str = ""
    confidence: float = 0.0
    success: bool = False
    warning: str | None = None
    char_count: int = 0


def is_ocr_available() -> bool:
    return _OCR_AVAILABLE


def _call_doc_intelligence(file_bytes: bytes, filename: str):
    """Call Azure Document Intelligence with exponential-backoff retry. Returns AnalyzeResult."""
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
                    "DI attempt %d/%d failed for %s: %s. Retrying in %.1fs...",
                    attempt + 1, _MAX_RETRIES, filename, e, delay,
                )
                time.sleep(delay)
    raise last_error


def _run_doc_intelligence(image_bytes: bytes, label: str) -> OCRResult:
    """Run DI on raw image bytes and return an OCRResult. Kept for backward compatibility."""
    try:
        result = _call_doc_intelligence(image_bytes, label)
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
        return OCRResult(success=False, warning=f"{label}: DI error — {str(e)}")


def ocr_pil_image(img, label: str = "image", min_width: int = 50,
                   min_height: int = 50, min_confidence: float = 10.0) -> OCRResult:
    if not _OCR_AVAILABLE:
        return OCRResult(success=False, warning=f"{label}: Azure Document Intelligence not configured — OCR skipped")
    try:
        width, height = img.size
        if width < min_width or height < min_height:
            return OCRResult(success=False, warning=f"{label}: image too small ({width}x{height}px) — skipped")
        if img.mode not in ("L", "RGB"):
            img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return _run_doc_intelligence(buf.getvalue(), label)
    except Exception as e:
        logger.error("OCR failed for %s: %s", label, str(e))
        return OCRResult(success=False, warning=f"{label}: OCR error — {str(e)}")


def ocr_image_bytes(data: bytes, label: str = "image", min_width: int = 50,
                    min_height: int = 50) -> OCRResult:
    if not _OCR_AVAILABLE:
        return OCRResult(success=False, warning=f"{label}: Azure Document Intelligence not configured — OCR skipped")
    try:
        return _run_doc_intelligence(data, label)
    except Exception as e:
        return OCRResult(success=False, warning=f"{label}: could not process image bytes — {str(e)}")


def ocr_image_file(file_path: str, label: str | None = None, min_width: int = 50,
                   min_height: int = 50) -> OCRResult:
    if not _OCR_AVAILABLE:
        return OCRResult(success=False, warning=f"{label or file_path}: Azure Document Intelligence not configured — OCR skipped")
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        return _run_doc_intelligence(data, label=label or file_path)
    except Exception as e:
        return OCRResult(success=False, warning=f"{label or file_path}: could not open image file — {str(e)}")


# ═══════════════════════════════════════════════════════════════════════════════
# BASE PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class BaseParser(ABC):
    parser_name: str = "base"

    def parse(self, file_path: str) -> ParsedDocument:
        path = Path(file_path)
        if not path.exists():
            return self._error_result(filename=path.name, file_type=path.suffix.lstrip("."),
                                      error=f"File not found: {file_path}")
        start = time.monotonic()
        try:
            result = self._parse(file_path)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            result.parse_meta.parse_duration_ms = elapsed_ms
            if result.parse_meta.warnings and result.parse_status == ParseStatus.SUCCESS:
                # Informational warnings (e.g. successful OCR of embedded images)
                # should not downgrade status to PARTIAL
                _INFO_PREFIXES = (
                    "OCR extracted text from",
                    "Multi-sheet workbook:",
                    "more than",
                )
                has_real_warning = any(
                    not w.startswith(_INFO_PREFIXES) for w in result.parse_meta.warnings
                )
                if has_real_warning:
                    result.parse_status = ParseStatus.PARTIAL
            return result
        except Exception as e:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return self._error_result(filename=path.name, file_type=path.suffix.lstrip("."),
                                      error=str(e), duration_ms=elapsed_ms)

    @abstractmethod
    def _parse(self, file_path: str) -> ParsedDocument:
        ...

    def _error_result(self, filename: str, file_type: str, error: str,
                      duration_ms: int | None = None) -> ParsedDocument:
        return ParsedDocument(
            filename=filename, file_type=file_type, full_text="",
            parse_status=ParseStatus.FAILED,
            parse_meta=ParseMeta(parser_used=self.parser_name, warnings=[error],
                                 parse_duration_ms=duration_ms),
            errors=[error],
        )


# ═══════════════════════════════════════════════════════════════════════════════
# DOCUMENT INTELLIGENCE PARSER (PDF, DOCX, XLSX, PPTX, Images)
# All document/image formats are sent directly to Azure Document Intelligence.
# No format-specific libraries (pdfplumber, openpyxl, python-docx, etc.) needed.
# ═══════════════════════════════════════════════════════════════════════════════

# Image extensions (for setting ocr_used flag)
_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".heif"}

# Legacy formats that DI may not support — warn but still try
_LEGACY_FORMATS = {".doc", ".xls", ".ppt"}


class DocIntelligenceParser(BaseParser):
    parser_name = "azure-doc-intelligence"

    def _parse(self, file_path: str) -> ParsedDocument:
        path = Path(file_path)
        ext = path.suffix.lower()
        warnings: list[str] = []

        if not _OCR_AVAILABLE:
            raise RuntimeError(
                "Azure Document Intelligence is not configured. "
                "Set AZURE_DOC_INTELLIGENCE_KEY and AZURE_DOC_INTELLIGENCE_ENDPOINT env vars."
            )

        # File size validation
        file_size = path.stat().st_size
        max_bytes = _MAX_FILE_SIZE_MB * 1024 * 1024
        if file_size > max_bytes:
            raise RuntimeError(
                f"File size ({file_size / (1024 * 1024):.1f}MB) exceeds "
                f"limit ({_MAX_FILE_SIZE_MB}MB). Set MAX_FILE_SIZE_MB env var to increase."
            )

        if ext in _LEGACY_FORMATS:
            modern = {".doc": ".docx", ".xls": ".xlsx", ".ppt": ".pptx"}.get(ext, ext)
            warnings.append(
                f"Legacy format '{ext}' may not be supported by Azure Document Intelligence. "
                f"Consider converting to '{modern}' for best results."
            )

        with open(file_path, "rb") as f:
            file_bytes = f.read()

        result = _call_doc_intelligence(file_bytes, path.name)

        # ── Extract full text ──
        full_text = (result.content or "").strip()

        # ── Extract tables ──
        tables = self._extract_tables(result, warnings)

        # ── Extract sections (paragraphs, headings) ──
        sections = self._extract_sections(result)

        # ── Build table text representations ──
        table_text_parts: list[str] = []
        for td in tables:
            text_repr = ""
            if td.sheet_name:
                text_repr = f"Sheet: {td.sheet_name}\n"
            if td.headers:
                text_repr += " | ".join(td.headers) + "\n"
            for row in td.rows[:200]:
                text_repr += " | ".join(row) + "\n"
            if len(td.rows) > 200:
                text_repr += f"... ({len(td.rows) - 200} more rows)\n"
            sections.append(Section(
                type=SectionType.TABLE, text=text_repr,
                headers=td.headers, rows=td.rows, page=td.page,
            ))
            table_text_parts.append(text_repr.strip())

        # ── If DI returned no text but did extract tables, build text from tables ──
        # This handles Excel files where content is empty but tables are populated
        if not full_text and table_text_parts:
            full_text = "\n\n".join(table_text_parts)
            warnings.append("No prose text detected — text reconstructed from extracted tables")

        # ── For Office docs, extract and OCR embedded images ──
        # DI extracts text/tables from DOCX/PPTX but does NOT OCR embedded images.
        # We open the file as a ZIP, pull out images from word/media/ or ppt/media/,
        # and OCR each one via DI. The result is appended to whatever text DI already found.
        if ext in (".docx", ".doc", ".pptx", ".ppt"):
            image_text = self._ocr_embedded_office_images(file_path, path.name, warnings)
            if image_text:
                if full_text:
                    full_text = full_text + "\n\n" + image_text
                else:
                    full_text = image_text
                sections.append(Section(type=SectionType.IMAGE, text=image_text))

        if not full_text:
            warnings.append("Document Intelligence returned no text content")

        # ── Page count ──
        page_count = len(result.pages) if result.pages else 1

        # ── Confidence from word-level scores ──
        confidences: list[float] = []
        if result.pages:
            for page in result.pages:
                if page.words:
                    for word in page.words:
                        if word.confidence is not None:
                            confidences.append(word.confidence)
        avg_confidence = sum(confidences) / len(confidences) if confidences else (0.0 if not full_text else 1.0)

        # ── Detect if OCR was actually used ──
        # DI performs OCR on scanned PDFs and image regions. We detect this by:
        # 1. File is a standalone image format → always OCR
        # 2. We extracted embedded Office images above → OCR was used
        # 3. DI returned word-level confidence < 1.0 → OCR was used on that content
        ocr_used = ext in _IMAGE_EXTENSIONS
        if not ocr_used and ext in (".docx", ".doc", ".pptx", ".ppt"):
            # If we OCR'd embedded images, mark OCR as used
            ocr_used = any(s.type == SectionType.IMAGE for s in sections)
        if not ocr_used and confidences:
            # DI returns confidence < 1.0 for OCR'd text, no confidences for native digital text
            ocr_used = any(c < 0.99 for c in confidences)

        metadata = DocumentMetadata(page_count=page_count)

        # Language detection (may not be available on all API versions)
        try:
            if hasattr(result, "languages") and result.languages:
                lang_obj = result.languages[0]
                metadata.language = getattr(lang_obj, "locale", None)
        except Exception:
            pass

        return ParsedDocument(
            filename=path.name,
            file_type=ext.lstrip("."),
            full_text=full_text,
            sections=sections,
            tables=tables,
            metadata=metadata,
            parse_meta=ParseMeta(
                parser_used="azure-doc-intelligence",
                ocr_used=ocr_used,
                confidence=avg_confidence,
                warnings=warnings,
            ),
        )

    def _extract_tables(self, result, warnings: list[str]) -> list[TableData]:
        """Extract structured table data from DI AnalyzeResult."""
        tables: list[TableData] = []
        if not result.tables:
            return tables

        for tbl_idx, table in enumerate(result.tables):
            try:
                if table.row_count < 1 or table.column_count < 1:
                    continue

                # Build grid from cells
                grid = [["" for _ in range(table.column_count)]
                        for _ in range(table.row_count)]

                for cell in table.cells:
                    r, c = cell.row_index, cell.column_index
                    if r < table.row_count and c < table.column_count:
                        grid[r][c] = (cell.content or "").strip()

                if not grid:
                    continue

                headers = grid[0]
                rows = grid[1:]

                page_num = None
                if table.bounding_regions:
                    page_num = table.bounding_regions[0].page_number

                tables.append(TableData(headers=headers, rows=rows, page=page_num))
            except Exception as e:
                warnings.append(f"Table {tbl_idx + 1} extraction failed: {e}")

        return tables

    def _extract_sections(self, result) -> list[Section]:
        """Extract paragraph/heading sections from DI AnalyzeResult."""
        sections: list[Section] = []
        if not hasattr(result, "paragraphs") or not result.paragraphs:
            return sections

        for para in result.paragraphs:
            text = (para.content or "").strip()
            if not text:
                continue

            role = getattr(para, "role", None)
            page_num = None
            if para.bounding_regions:
                page_num = para.bounding_regions[0].page_number

            if role == "title":
                sections.append(Section(type=SectionType.HEADING, text=text, level=1, page=page_num))
            elif role == "sectionHeading":
                sections.append(Section(type=SectionType.HEADING, text=text, level=2, page=page_num))
            elif role in ("pageHeader", "pageFooter", "pageNumber"):
                continue  # Skip decorative elements
            else:
                sections.append(Section(type=SectionType.PARAGRAPH, text=text, page=page_num))

        return sections

    def _ocr_embedded_office_images(
        self, file_path: str, filename: str, warnings: list[str],
    ) -> str:
        """Extract embedded images from DOCX/PPTX and OCR each one via DI.

        Office Open XML files (.docx, .pptx) are ZIP archives containing
        image files at word/media/* or ppt/media/*. We extract these images
        and send each one to Azure Document Intelligence for OCR — no
        external libraries needed, just Python's built-in zipfile module.
        """
        import zipfile

        if not zipfile.is_zipfile(file_path):
            return ""

        _IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".gif"}
        MIN_IMAGE_SIZE = 2000  # skip tiny icons/bullets

        ocr_parts: list[str] = []
        images_found = 0
        images_ocred = 0

        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                for entry in zf.namelist():
                    # Images live in word/media/ (DOCX) or ppt/media/ (PPTX)
                    if "/media/" not in entry:
                        continue
                    ext_lower = Path(entry).suffix.lower()
                    if ext_lower not in _IMAGE_EXTS:
                        continue

                    img_bytes = zf.read(entry)
                    if len(img_bytes) < MIN_IMAGE_SIZE:
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
                warnings.append(
                    f"OCR extracted text from {images_ocred}/{images_found} embedded images"
                )
                return "\n\n".join(ocr_parts).strip()

            if images_found > 0:
                warnings.append(
                    f"Found {images_found} embedded images but OCR returned no text"
                )

        except Exception as e:
            warnings.append(f"Failed to extract embedded images: {e}")

        return ""


# ═══════════════════════════════════════════════════════════════════════════════
# CSV/TSV PARSER (plain text — stdlib csv module, no external libraries)
# ═══════════════════════════════════════════════════════════════════════════════

MAX_CSV_ROWS = 50_000


class CsvParser(BaseParser):
    parser_name = "text-reader"

    def _parse(self, file_path: str) -> ParsedDocument:
        warnings: list[str] = []
        path = Path(file_path)
        ext = path.suffix.lower()

        encoding = self._detect_encoding(file_path)
        if encoding not in ("utf-8", "ascii"):
            warnings.append(f"Detected encoding: {encoding}")

        # Detect delimiter from a sample
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

        # Parse rows
        all_rows: list[list[str]] = []
        try:
            with open(file_path, "r", encoding=encoding, errors="replace", newline="") as f:
                reader = csv.reader(f, delimiter=delimiter)
                for i, row in enumerate(reader):
                    if i >= MAX_CSV_ROWS:
                        warnings.append(f"File has more than {MAX_CSV_ROWS} rows — truncated")
                        break
                    all_rows.append([str(cell) for cell in row])
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV: {e}")

        if not all_rows:
            return ParsedDocument(
                filename=path.name, file_type=ext.lstrip("."), full_text="",
                parse_meta=ParseMeta(parser_used="text-reader", warnings=["File is empty"]),
            )

        headers = all_rows[0]
        rows = all_rows[1:]

        # Build text representation
        text_parts = [" | ".join(headers)]
        for row in rows[:500]:
            text_parts.append(" | ".join(row))
        if len(rows) > 500:
            text_parts.append(f"... ({len(rows) - 500} more rows)")

        table = TableData(headers=headers, rows=rows)
        section = Section(
            type=SectionType.TABLE,
            text=f"CSV data: {len(rows)} rows x {len(headers)} columns",
            headers=headers, rows=rows,
        )

        return ParsedDocument(
            filename=path.name, file_type=ext.lstrip("."),
            full_text="\n".join(text_parts),
            sections=[section], tables=[table],
            metadata=DocumentMetadata(page_count=1),
            parse_meta=ParseMeta(parser_used="text-reader", warnings=warnings),
        )

    def _detect_encoding(self, file_path: str) -> str:
        """Detect encoding using BOM markers and UTF-8 validation — no external libraries."""
        try:
            with open(file_path, "rb") as f:
                raw = f.read(4096)
            if raw.startswith(b"\xef\xbb\xbf"):
                return "utf-8-sig"
            if raw.startswith(b"\xff\xfe"):
                return "utf-16-le"
            if raw.startswith(b"\xfe\xff"):
                return "utf-16-be"
            raw.decode("utf-8")
            return "utf-8"
        except (UnicodeDecodeError, Exception):
            return "latin-1"


# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL PARSER (xlsx/xls/xlsm — Python libraries only, no DI)
# ═══════════════════════════════════════════════════════════════════════════════

MAX_EXCEL_ROWS = 100_000


class ExcelParser(BaseParser):
    """Parse Excel files using openpyxl/pandas — no Azure Document Intelligence.

    Reads all sheets, converts each to a TableData + text representation.
    Handles single-sheet and multi-sheet workbooks reliably.
    """
    parser_name = "excel-reader"

    def _parse(self, file_path: str) -> ParsedDocument:
        warnings: list[str] = []
        path = Path(file_path)
        ext = path.suffix.lower()

        import pandas as pd

        # Pick the right engine — openpyxl for xlsx/xlsm, xlrd for legacy xls
        if ext == ".xls":
            engine = "xlrd"
        else:
            engine = "openpyxl"

        # Read all sheets using pandas
        try:
            sheets = pd.read_excel(file_path, sheet_name=None, dtype=str, header=None, engine=engine)
        except ImportError as ie:
            raise RuntimeError(
                f"Missing library for {ext} files: {ie}. "
                f"Install it with: pip install {engine}"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to read Excel file: {e}")

        if not sheets:
            return ParsedDocument(
                filename=path.name, file_type=ext.lstrip("."), full_text="",
                parse_meta=ParseMeta(parser_used="excel-reader", warnings=["No sheets found"]),
            )

        sheet_names = list(sheets.keys())
        all_tables: list[TableData] = []
        all_sections: list[Section] = []
        text_parts: list[str] = []
        total_rows = 0

        for sheet_name in sheet_names:
            df = sheets[sheet_name]

            # Drop completely empty rows and columns
            df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

            if df.empty:
                continue

            # Use first row as header (most common Excel layout)
            header_row = df.iloc[0].astype(str).tolist()
            # Check if the first row looks like actual headers (not data)
            # If most cells are short text and non-numeric, treat as header
            data_rows = df.iloc[1:]

            # Clean up header names
            headers = []
            for h in header_row:
                h_clean = str(h).strip()
                if h_clean.lower() in ("nan", "none", ""):
                    h_clean = ""
                headers.append(h_clean)

            rows: list[list[str]] = []
            for _, row in data_rows.iterrows():
                row_vals = [str(v).strip() if str(v).strip().lower() not in ("nan", "none") else "" for v in row]
                # Skip rows that are entirely empty
                if all(v == "" for v in row_vals):
                    continue
                rows.append(row_vals)
                if len(rows) >= MAX_EXCEL_ROWS:
                    warnings.append(f"Sheet '{sheet_name}' has more than {MAX_EXCEL_ROWS} rows — truncated")
                    break

            total_rows += len(rows)

            table = TableData(
                headers=headers, rows=rows,
                sheet_name=str(sheet_name),
            )
            all_tables.append(table)

            # Text representation
            sheet_label = f"[Sheet: {sheet_name}]" if len(sheet_names) > 1 else ""
            if sheet_label:
                text_parts.append(sheet_label)
            text_parts.append(" | ".join(h for h in headers if h))
            for row in rows[:500]:
                text_parts.append(" | ".join(row))
            if len(rows) > 500:
                text_parts.append(f"... ({len(rows) - 500} more rows)")
            text_parts.append("")  # blank line between sheets

            section = Section(
                type=SectionType.TABLE,
                text=f"Sheet '{sheet_name}': {len(rows)} rows x {len(headers)} columns",
                headers=headers, rows=rows,
            )
            all_sections.append(section)

        if not all_tables:
            return ParsedDocument(
                filename=path.name, file_type=ext.lstrip("."), full_text="",
                parse_meta=ParseMeta(parser_used="excel-reader", warnings=["All sheets empty"]),
            )

        if len(sheet_names) > 1:
            warnings.append(f"Multi-sheet workbook: {len(sheet_names)} sheets ({', '.join(str(s) for s in sheet_names)})")

        metadata = DocumentMetadata(
            page_count=len(sheet_names),
            sheet_names=[str(s) for s in sheet_names],
        )

        return ParsedDocument(
            filename=path.name, file_type=ext.lstrip("."),
            full_text="\n".join(text_parts).strip(),
            sections=all_sections, tables=all_tables,
            metadata=metadata,
            parse_meta=ParseMeta(parser_used="excel-reader", warnings=warnings),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# EML PARSER (RFC822 email — Python stdlib only)
# ═══════════════════════════════════════════════════════════════════════════════

class EmlParser(BaseParser):
    parser_name = "email-parser"

    def _parse(self, file_path: str) -> ParsedDocument:
        from email import policy as email_policy
        from email.parser import BytesParser
        from html import unescape as html_unescape

        path = Path(file_path)
        sections: list[Section] = []
        full_text_parts: list[str] = []
        warnings: list[str] = []

        with open(file_path, "rb") as f:
            msg = BytesParser(policy=email_policy.default).parse(f)

        # -- Headers --
        header_lines = []
        for h in ("From", "To", "Cc", "Subject", "Date", "Message-ID"):
            v = msg.get(h, "")
            if v:
                header_lines.append(f"{h}: {v}")
        if header_lines:
            header_text = "\n".join(header_lines)
            sections.append(Section(type=SectionType.EMAIL_HEADER, text=header_text))
            full_text_parts.append(header_text)

        # -- Body & Attachments --
        plain_parts: list[str] = []
        html_parts: list[str] = []
        attachment_sections: list[str] = []

        if msg.is_multipart():
            for part in msg.walk():
                disp = part.get_content_disposition()
                if disp == "attachment":
                    filename = part.get_filename() or "attachment"
                    payload_bytes = part.get_payload(decode=True)
                    if payload_bytes:
                        ext = Path(filename).suffix.lower()
                        if ext and ext != ".eml" and is_supported(ext):
                            tmp_path = None
                            try:
                                with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                                    tmp.write(payload_bytes)
                                    tmp_path = Path(tmp.name)
                                att_result = parse_document(str(tmp_path))
                                if att_result.full_text:
                                    attachment_sections.append(
                                        f"[ATTACHMENT: {filename}]\n{att_result.full_text[:8000]}"
                                    )
                                else:
                                    warnings.append(f"Attachment '{filename}' returned no text")
                            except Exception as e:
                                warnings.append(f"Attachment '{filename}' failed: {e}")
                            finally:
                                try:
                                    if tmp_path:
                                        tmp_path.unlink(missing_ok=True)
                                except Exception:
                                    pass
                    continue
                ctype = (part.get_content_type() or "").lower()
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
            full_text_parts.append(body)
        if attachment_sections:
            for att in attachment_sections:
                sections.append(Section(type=SectionType.PARAGRAPH, text=att))
                full_text_parts.append(att)

        metadata = DocumentMetadata(
            email_from=msg.get("From", ""),
            email_to=[a.strip() for a in (msg.get("To", "") or "").split(",") if a.strip()],
            email_date=msg.get("Date", ""),
            email_subject=msg.get("Subject", ""),
            page_count=1,
        )

        return ParsedDocument(
            filename=path.name, file_type="eml",
            full_text="\n\n".join(full_text_parts),
            sections=sections, tables=[],
            metadata=metadata,
            parse_meta=ParseMeta(parser_used="email-parser", warnings=warnings),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# MSG PARSER (Outlook .msg — requires extract-msg, optional dependency)
# ═══════════════════════════════════════════════════════════════════════════════

class MsgParser(BaseParser):
    parser_name = "extract-msg"

    def _parse(self, file_path: str) -> ParsedDocument:
        try:
            import extract_msg
        except ImportError:
            raise RuntimeError(
                "extract-msg is not installed. .msg files require this package. "
                "Run: pip install extract-msg"
            )

        sections: list[Section] = []
        full_text_parts: list[str] = []
        warnings: list[str] = []

        msg = extract_msg.Message(file_path)
        try:
            sender = msg.sender or ""
            to = msg.to or ""
            cc = msg.cc or ""
            subject = msg.subject or ""
            date = msg.date or ""

            header_text = f"From: {sender}\nTo: {to}\nCC: {cc}\nDate: {date}\nSubject: {subject}"
            sections.append(Section(type=SectionType.EMAIL_HEADER, text=header_text))
            full_text_parts.append(header_text)

            body = (msg.body or "").strip()
            if body:
                sections.append(Section(type=SectionType.PARAGRAPH, text=body))
                full_text_parts.append(body)
            else:
                html_body = msg.htmlBody
                if html_body:
                    plain = re.sub(r"<[^>]+>", " ", html_body.decode("utf-8", errors="replace"))
                    plain = re.sub(r"\s+", " ", plain).strip()
                    if plain:
                        sections.append(Section(type=SectionType.PARAGRAPH, text=plain))
                        full_text_parts.append(plain)
                        warnings.append("Used HTML body (no plain text body found)")

            attachment_names = []
            for att in msg.attachments:
                att_name = getattr(att, "longFilename", None) or getattr(att, "shortFilename", "unknown")
                attachment_names.append(att_name)

            if attachment_names:
                att_text = "Attachments: " + ", ".join(attachment_names)
                sections.append(Section(type=SectionType.PARAGRAPH, text=att_text))
                full_text_parts.append(att_text)

            to_list = [addr.strip() for addr in to.split(";") if addr.strip()] if to else []

            metadata = DocumentMetadata(
                email_from=sender, email_to=to_list, email_date=str(date),
                email_subject=subject, page_count=1,
            )
        finally:
            msg.close()

        return ParsedDocument(
            filename=Path(file_path).name, file_type="msg",
            full_text="\n\n".join(full_text_parts), sections=sections, tables=[],
            metadata=metadata,
            parse_meta=ParseMeta(parser_used="extract-msg", warnings=warnings),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PARSER REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

_PARSER_MAP: dict[str, type[BaseParser]] = {
    # Document Intelligence handles all document + image formats
    ".pdf": DocIntelligenceParser,
    ".docx": DocIntelligenceParser,
    ".doc": DocIntelligenceParser,
    ".xlsx": ExcelParser,
    ".xls": ExcelParser,
    ".xlsm": ExcelParser,
    ".pptx": DocIntelligenceParser,
    ".png": DocIntelligenceParser,
    ".jpg": DocIntelligenceParser,
    ".jpeg": DocIntelligenceParser,
    ".tiff": DocIntelligenceParser,
    ".tif": DocIntelligenceParser,
    ".bmp": DocIntelligenceParser,
    # Plain text — read directly with stdlib csv
    ".csv": CsvParser,
    ".tsv": CsvParser,
    # Email — stdlib / optional parsers
    ".eml": EmlParser,
    ".msg": MsgParser,
}

ALLOWED_EXTENSIONS: set[str] = set(_PARSER_MAP.keys())


def get_parser(extension: str) -> BaseParser:
    ext = extension.lower() if extension.startswith(".") else f".{extension.lower()}"
    if ext not in _PARSER_MAP:
        raise ValueError(f"Unsupported file type: '{ext}'. Supported types: {sorted(ALLOWED_EXTENSIONS)}")
    return _PARSER_MAP[ext]()


def is_supported(extension: str) -> bool:
    ext = extension.lower() if extension.startswith(".") else f".{extension.lower()}"
    return ext in _PARSER_MAP


# ═══════════════════════════════════════════════════════════════════════════════
# PARSE LOGGING — writes detailed per-document logs to disk
# ═══════════════════════════════════════════════════════════════════════════════

_PARSE_LOG_DIR = os.environ.get("PARSE_LOG_DIR", "")
_PARSE_LOGGING_ENABLED = os.environ.get("PARSE_LOGGING", "1").strip().lower() in {"1", "true", "yes", "on"}

# Session folder — created once per process start, shared by all log calls.
# Structure: parse_logs/2026-03-26_14-30-00/documents/
#            parse_logs/2026-03-26_14-30-00/embeddings/
_SESSION_FOLDER_NAME: str | None = None


def _get_session_folder() -> str:
    """Return a stable session folder name (created once per process)."""
    global _SESSION_FOLDER_NAME
    if _SESSION_FOLDER_NAME is None:
        _SESSION_FOLDER_NAME = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    return _SESSION_FOLDER_NAME


_active_phase: str = ""  # "TOD" or "TOE" — set by set_parse_phase()


def set_parse_phase(phase: str) -> None:
    """Set the active parsing phase (TOD or TOE) for log directory separation."""
    global _active_phase
    _active_phase = phase.upper() if phase else ""
    logger.debug("Parse phase set to: %s", _active_phase or "(none)")


def _get_phase_base() -> Path:
    """Return the base log directory, optionally with a TOD/TOE subfolder."""
    if _PARSE_LOG_DIR:
        base = Path(_PARSE_LOG_DIR)
    else:
        base = Path(__file__).resolve().parent / "parse_logs"
    session_dir = base / _get_session_folder()
    if _active_phase:
        return session_dir / _active_phase
    return session_dir


def _get_parse_log_dir() -> Path | None:
    """Return the parse_logs/<session>/<phase>/documents directory, creating it if needed."""
    if not _PARSE_LOGGING_ENABLED:
        return None
    doc_dir = _get_phase_base() / "documents"
    try:
        doc_dir.mkdir(parents=True, exist_ok=True)
        return doc_dir
    except Exception as e:
        logger.debug("Could not create parse log dir %s: %s", doc_dir, e)
        return None


def get_embedding_log_dir() -> Path | None:
    """Return the parse_logs/<session>/<phase>/embeddings directory, creating it if needed.

    Exported so TOD/TOE engines can write embedding logs.
    """
    if not _PARSE_LOGGING_ENABLED:
        return None
    emb_dir = _get_phase_base() / "embeddings"
    try:
        emb_dir.mkdir(parents=True, exist_ok=True)
        return emb_dir
    except Exception as e:
        logger.debug("Could not create embedding log dir %s: %s", emb_dir, e)
        return None


def _write_parse_log(result: ParsedDocument, file_path: str) -> None:
    """Write a detailed parse log file for one document."""
    log_dir = _get_parse_log_dir()
    if log_dir is None:
        return

    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        safe_name = re.sub(r"[^\w.\-]", "_", result.filename)
        log_path = log_dir / f"{ts}_{safe_name}.log"

        # File size
        try:
            file_size_bytes = Path(file_path).stat().st_size
            if file_size_bytes >= 1024 * 1024:
                file_size_str = f"{file_size_bytes / (1024 * 1024):.2f} MB"
            elif file_size_bytes >= 1024:
                file_size_str = f"{file_size_bytes / 1024:.1f} KB"
            else:
                file_size_str = f"{file_size_bytes} bytes"
        except Exception:
            file_size_str = "unknown"

        lines: list[str] = []
        sep = "═" * 70
        thin = "─" * 70

        # Header
        lines.append(sep)
        lines.append("DOCUMENT PARSE LOG")
        lines.append(sep)
        lines.append(f"Timestamp:      {result.parsed_at}")
        lines.append(f"Source Path:    {file_path}")
        lines.append(f"Filename:       {result.filename}")
        lines.append(f"File Type:      {result.file_type}")
        lines.append(f"File Size:      {file_size_str}")
        lines.append(f"Document ID:    {result.document_id}")
        lines.append(f"Parser Used:    {result.parse_meta.parser_used}")
        lines.append(f"Parse Status:   {result.parse_status.value.upper()}")
        lines.append(f"Confidence:     {result.parse_meta.confidence:.4f}")
        lines.append(f"OCR Used:       {result.parse_meta.ocr_used}")
        lines.append(f"Duration:       {result.parse_meta.parse_duration_ms or 0}ms")
        lines.append(f"Warnings:       {result.parse_meta.warnings or 'None'}")
        lines.append(f"Errors:         {result.errors or 'None'}")
        lines.append("")

        # Metadata
        lines.append(thin)
        lines.append("METADATA")
        lines.append(thin)
        meta = result.metadata
        if meta.page_count is not None:
            lines.append(f"Page Count:     {meta.page_count}")
        if meta.language:
            lines.append(f"Language:       {meta.language}")
        if meta.author:
            lines.append(f"Author:         {meta.author}")
        if meta.title:
            lines.append(f"Title:          {meta.title}")
        if meta.subject:
            lines.append(f"Subject:        {meta.subject}")
        if meta.created_at:
            lines.append(f"Created At:     {meta.created_at}")
        if meta.sheet_names:
            lines.append(f"Sheet Names:    {meta.sheet_names}")
        if meta.slide_count is not None:
            lines.append(f"Slide Count:    {meta.slide_count}")
        if meta.email_from:
            lines.append(f"Email From:     {meta.email_from}")
        if meta.email_to:
            lines.append(f"Email To:       {meta.email_to}")
        if meta.email_date:
            lines.append(f"Email Date:     {meta.email_date}")
        if meta.email_subject:
            lines.append(f"Email Subject:  {meta.email_subject}")
        lines.append("")

        # Sections summary
        lines.append(thin)
        lines.append(f"SECTIONS ({len(result.sections)} total)")
        lines.append(thin)
        for idx, sec in enumerate(result.sections, 1):
            page_str = f" (page {sec.page})" if sec.page else ""
            preview = sec.text[:120].replace("\n", " ").strip()
            if len(sec.text) > 120:
                preview += "..."
            lines.append(f"[{idx}] {sec.type.value.upper()}{page_str}: {preview}")
        lines.append("")

        # Tables detail
        lines.append(thin)
        lines.append(f"TABLES ({len(result.tables)} total)")
        lines.append(thin)
        for idx, tbl in enumerate(result.tables, 1):
            page_str = f" (page {tbl.page})" if tbl.page else ""
            sheet_str = f" [{tbl.sheet_name}]" if tbl.sheet_name else ""
            lines.append(f"Table {idx}{page_str}{sheet_str}: {len(tbl.headers)} cols x {len(tbl.rows)} rows")
            lines.append(f"  Headers: {' | '.join(tbl.headers[:10])}")
            if len(tbl.headers) > 10:
                lines.append(f"  ... ({len(tbl.headers) - 10} more columns)")
            for row_idx, row in enumerate(tbl.rows[:5], 1):
                lines.append(f"  Row {row_idx}: {' | '.join(row[:10])}")
            if len(tbl.rows) > 5:
                lines.append(f"  ... ({len(tbl.rows) - 5} more rows)")
            lines.append("")

        # Full parsed text
        lines.append(sep)
        lines.append("FULL PARSED TEXT")
        lines.append(sep)
        full_text = result.full_text or "(empty)"
        lines.append(full_text)
        lines.append("")
        lines.append(sep)
        lines.append(f"END OF LOG — {result.filename}")
        lines.append(sep)

        log_path.write_text("\n".join(lines), encoding="utf-8")
        logger.debug("Parse log written: %s", log_path)
    except Exception as e:
        logger.debug("Failed to write parse log for %s: %s", result.filename, e)


def _log_parse_summary(result: ParsedDocument, file_path: str) -> None:
    """Log a concise one-line parsing summary to the application logger."""
    phase_tag = f"[{_active_phase}] " if _active_phase else ""
    text_len = len(result.full_text) if result.full_text else 0
    tables_info = f", {len(result.tables)} tables" if result.tables else ""
    sheets_info = ""
    if result.metadata.sheet_names:
        sheets_info = f", {len(result.metadata.sheet_names)} sheets"
    duration = result.parse_meta.parse_duration_ms or 0
    warn_count = len(result.parse_meta.warnings) if result.parse_meta.warnings else 0
    warn_tag = f", {warn_count} warnings" if warn_count else ""

    logger.info(
        "%sParsed: %s → %s | parser=%s, %d chars%s%s%s, %dms",
        phase_tag, Path(file_path).name, result.parse_status.value.upper(),
        result.parse_meta.parser_used, text_len,
        tables_info, sheets_info, warn_tag, duration,
    )


def write_parse_summary(parsed_results: list[ParsedDocument], phase: str = "") -> None:
    """Write a summary log showing all parsed files with status columns.

    Placed inside the same folder that contains the detailed per-file logs.
    Format:
        Filename                          | Parser        | Status  | Chars  | Tables | Duration
        invoice.pdf                       | azure-doc-int | SUCCESS |  4523  |   2    | 312ms
        report.xlsx                       | excel-reader  | SUCCESS |  1200  |   3    |  45ms

    Args:
        parsed_results: list of ParsedDocument objects from this parsing run
        phase: "TOD" or "TOE" (included in header)
    """
    log_dir = _get_parse_log_dir()
    if log_dir is None:
        return

    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        summary_path = log_dir / f"_PARSE_SUMMARY_{ts}.log"

        succeeded = sum(1 for r in parsed_results if r.parse_status == ParseStatus.SUCCESS)
        partial = sum(1 for r in parsed_results if r.parse_status == ParseStatus.PARTIAL)
        failed = sum(1 for r in parsed_results if r.parse_status == ParseStatus.FAILED)

        lines = [
            "═" * 110,
            f"  PARSING SUMMARY — {phase or 'General'}",
            f"  {ts}",
            "═" * 110,
            f"  Total: {len(parsed_results)}   |   Success: {succeeded}   |   Partial: {partial}   |   Failed: {failed}",
            "═" * 110,
            "",
            f"  {'Filename':<45s} {'Parser':<18s} {'Status':<10s} {'Chars':>8s} {'Tables':>8s} {'Sections':>10s} {'Duration':>10s}",
            "  " + "─" * 106,
        ]

        for r in parsed_results:
            fname = r.filename[:44]
            parser = r.parse_meta.parser_used[:17]
            status = r.parse_status.value.upper()
            chars = str(len(r.full_text)) if r.full_text else "0"
            tables = str(len(r.tables))
            sections = str(len(r.sections))
            dur = f"{r.parse_meta.parse_duration_ms or 0}ms"
            lines.append(
                f"  {fname:<45s} {parser:<18s} {status:<10s} {chars:>8s} {tables:>8s} {sections:>10s} {dur:>10s}"
            )

        # Warnings/errors detail for non-success files
        problem_files = [r for r in parsed_results if r.parse_status != ParseStatus.SUCCESS]
        if problem_files:
            lines.append("")
            lines.append("  " + "─" * 106)
            lines.append("  DETAILS (partial/failed files):")
            lines.append("  " + "─" * 106)
            for r in problem_files:
                lines.append(f"  {r.filename}:")
                if r.parse_meta.warnings:
                    for w in r.parse_meta.warnings:
                        lines.append(f"    warning: {w}")
                if r.errors:
                    for e in r.errors:
                        lines.append(f"    error:   {e}")

        lines.append("")
        lines.append("═" * 110)

        summary_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info("Parse summary written: %s (%d files — %d ok, %d partial, %d fail)",
                     summary_path, len(parsed_results), succeeded, partial, failed)
    except Exception as e:
        logger.debug("Failed to write parse summary: %s", e)


def write_extraction_summary(
    extract_cache: dict,
    control_files_map: dict | None = None,
    phase: str = "",
) -> None:
    """Write a tabular parsing summary from an engine's extraction cache.

    Called by TOD/TOE evidence loaders after all files are extracted.
    The summary sits in the same folder as detailed per-file logs.

    Args:
        extract_cache: dict[path_str_or_Path -> (content, doc_type, ok)]
        control_files_map: optional dict[control_id -> list[Path]] for grouping
        phase: "TOD" or "TOE"
    """
    log_dir = _get_parse_log_dir()
    if log_dir is None:
        return

    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        summary_path = log_dir / f"_PARSE_SUMMARY_{ts}.log"

        # Flatten cache entries
        entries: list[tuple[str, str, str, bool]] = []  # (filename, doc_type, status, ok)
        for path_key, val in extract_cache.items():
            fname = Path(str(path_key)).name
            content, doc_type, ok = val
            chars = len(content) if content else 0
            entries.append((fname, doc_type, chars, ok))

        succeeded = sum(1 for *_, ok in entries if ok)
        failed = len(entries) - succeeded

        lines = [
            "═" * 110,
            f"  PARSING SUMMARY — {phase or 'General'}",
            f"  {ts}",
            "═" * 110,
            f"  Total: {len(entries)}   |   Success: {succeeded}   |   Failed: {failed}",
            "═" * 110,
            "",
            f"  {'Filename':<50s} {'Doc Type':<25s} {'Status':<10s} {'Chars':>8s}",
            "  " + "─" * 106,
        ]

        # Group by control if mapping provided
        if control_files_map:
            for cid in sorted(control_files_map.keys()):
                lines.append(f"  [{cid}]")
                for fp in control_files_map[cid]:
                    abs_key = str(fp.resolve()) if hasattr(fp, 'resolve') else str(fp)
                    val = extract_cache.get(abs_key) or extract_cache.get(fp)
                    if val:
                        content, doc_type, ok = val
                        chars = len(content) if content else 0
                    else:
                        doc_type, ok, chars = "Text File", True, 0
                    status = "SUCCESS" if ok else "FAIL"
                    lines.append(
                        f"    {fp.name:<48s} {doc_type:<25s} {status:<10s} {chars:>8d}"
                    )
                lines.append("")
        else:
            for fname, doc_type, chars, ok in sorted(entries, key=lambda x: x[0]):
                status = "SUCCESS" if ok else "FAIL"
                lines.append(
                    f"  {fname:<50s} {doc_type:<25s} {status:<10s} {chars:>8d}"
                )

        lines.append("")
        lines.append("═" * 110)

        summary_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info("Parse summary written: %s (%d files — %d ok, %d fail)",
                     summary_path, len(entries), succeeded, failed)
    except Exception as e:
        logger.debug("Failed to write extraction summary: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
# PARSE DOCUMENT (main entry point)
# ═══════════════════════════════════════════════════════════════════════════════

def parse_document(file_path: str, document_id: str | None = None) -> ParsedDocument:
    path = Path(file_path)

    if not path.exists():
        not_found = ParsedDocument(
            filename=path.name, file_type=path.suffix.lstrip("."), full_text="",
            parse_status=ParseStatus.FAILED, parse_meta=ParseMeta(parser_used="none"),
            errors=[f"File not found: {file_path}"],
        )
        _write_parse_log(not_found, file_path)
        return not_found

    ext = path.suffix.lower()
    if not is_supported(ext):
        unsupported = ParsedDocument(
            filename=path.name, file_type=ext.lstrip("."), full_text="",
            parse_status=ParseStatus.FAILED, parse_meta=ParseMeta(parser_used="none"),
            errors=[f"Unsupported file type: '{ext}'. Supported: {sorted(ALLOWED_EXTENSIONS)}"],
        )
        _write_parse_log(unsupported, file_path)
        return unsupported

    try:
        parser = get_parser(ext)
        result = parser.parse(file_path)
        if document_id:
            result.document_id = document_id
        _log_parse_summary(result, file_path)
        _write_parse_log(result, file_path)
        return result
    except Exception as e:
        logger.exception("Unexpected error parsing %s", file_path)
        err_result = ParsedDocument(
            filename=path.name, file_type=ext.lstrip("."), full_text="",
            parse_status=ParseStatus.FAILED, parse_meta=ParseMeta(parser_used="none"),
            errors=[f"Unexpected error: {str(e)}"],
        )
        _write_parse_log(err_result, file_path)
        return err_result


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════

_PRIORITY: dict[str, int] = {
    ".pdf": 0,
    ".pptx": 1, ".ppt": 1,
    ".docx": 2, ".doc": 2,
    ".xlsx": 3, ".xls": 3, ".xlsm": 3,
    ".csv": 4, ".tsv": 4,
    ".msg": 5, ".eml": 5,
    ".png": 6, ".jpg": 6, ".jpeg": 6, ".tiff": 6, ".tif": 6, ".bmp": 6,
}
DEFAULT_PRIORITY = 7


def _sort_key(file_path: str) -> tuple[int, str]:
    ext = Path(file_path).suffix.lower()
    return (_PRIORITY.get(ext, DEFAULT_PRIORITY), ext)


def get_processing_groups(file_paths: list[str]) -> list[dict]:
    sorted_paths = sorted(file_paths, key=_sort_key)
    groups: list[dict] = []
    current_ext = None
    for fp in sorted_paths:
        ext = Path(fp).suffix.lower()
        priority = _PRIORITY.get(ext, DEFAULT_PRIORITY)
        if ext != current_ext:
            groups.append({"extension": ext, "priority": priority, "files": []})
            current_ext = ext
        groups[-1]["files"].append(fp)
    return groups


@dataclass
class BatchResult:
    results: list[ParsedDocument] = field(default_factory=list)
    total_files: int = 0
    succeeded: int = 0
    partial: int = 0
    failed: int = 0
    total_duration_ms: int = 0
    file_order: list[str] = field(default_factory=list)
    processing_groups: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "summary": {
                "total_files": self.total_files,
                "succeeded": self.succeeded,
                "partial": self.partial,
                "failed": self.failed,
                "total_duration_ms": self.total_duration_ms,
                "processing_order": self.file_order,
                "groups": [
                    {"extension": g["extension"], "priority": g["priority"], "count": len(g["files"])}
                    for g in self.processing_groups
                ],
            },
            "documents": [r.to_dict() for r in self.results],
        }


def parse_batch(file_paths: list[str], max_workers: int = 4,
                document_ids: dict[str, str] | None = None) -> BatchResult:
    if not file_paths:
        return BatchResult()

    document_ids = document_ids or {}
    sorted_paths = sorted(file_paths, key=_sort_key)
    groups = get_processing_groups(file_paths)

    batch = BatchResult(
        total_files=len(sorted_paths),
        file_order=[Path(p).name for p in sorted_paths],
        processing_groups=groups,
    )

    start = time.monotonic()

    if max_workers <= 1:
        for fp in sorted_paths:
            result = parse_document(fp, document_id=document_ids.get(fp))
            batch.results.append(result)
    else:
        results_map: dict[str, ParsedDocument] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_path = {}
            for fp in sorted_paths:
                future = executor.submit(parse_document, fp, document_id=document_ids.get(fp))
                future_to_path[future] = fp
            for future in as_completed(future_to_path):
                fp = future_to_path[future]
                try:
                    result = future.result()
                except Exception as e:
                    logger.exception("Batch parse failed for %s", fp)
                    result = ParsedDocument(
                        filename=Path(fp).name, file_type=Path(fp).suffix.lstrip("."),
                        full_text="", parse_status=ParseStatus.FAILED,
                        errors=[f"Batch error: {str(e)}"],
                    )
                results_map[fp] = result
        batch.results = [results_map[fp] for fp in sorted_paths]

    batch.total_duration_ms = int((time.monotonic() - start) * 1000)

    for r in batch.results:
        if r.parse_status == ParseStatus.SUCCESS:
            batch.succeeded += 1
        elif r.parse_status == ParseStatus.PARTIAL:
            batch.partial += 1
        else:
            batch.failed += 1

    logger.info(
        "Batch: %d files (%d groups) in %dms — %d ok, %d partial, %d failed",
        batch.total_files, len(groups), batch.total_duration_ms,
        batch.succeeded, batch.partial, batch.failed,
    )

    # Write summary into the same folder as the detailed logs
    write_parse_summary(batch.results, phase=_active_phase)

    return batch


# ═══════════════════════════════════════════════════════════════════════════════
# BRIDGE FUNCTION — drop-in replacement for TOD/TOE engine _extract_file()
# ═══════════════════════════════════════════════════════════════════════════════

# File type labels for the bridge
_DOC_TYPE_LABELS: dict[str, str] = {
    ".pdf": "PDF Document", ".docx": "Word Document", ".doc": "Word Document (legacy)",
    ".xlsx": "Excel Spreadsheet", ".xls": "Excel Spreadsheet (legacy)", ".xlsm": "Excel Spreadsheet",
    ".csv": "CSV Data", ".tsv": "TSV Data",
    ".pptx": "PowerPoint Presentation", ".ppt": "PowerPoint (legacy)",
    ".msg": "Outlook Message", ".eml": "Email Message",
    ".png": "Image (OCR)", ".jpg": "Image (OCR)", ".jpeg": "Image (OCR)",
    ".tiff": "Image (OCR)", ".tif": "Image (OCR)", ".bmp": "Image (OCR)",
}


def extract_text(file_path: str) -> tuple[str, str, bool]:
    """
    Parse any supported file and return plain text.

    This is the bridge function used by TOD/TOE engines.

    Returns: (content_text, doc_type_label, extraction_succeeded)
    """
    path = Path(file_path)
    ext = path.suffix.lower()
    doc_type = _DOC_TYPE_LABELS.get(ext, "Attachment")

    if not path.exists():
        return f"[File not found: {path.name}]", doc_type, False

    if not is_supported(ext):
        # For unsupported formats, try reading as text
        try:
            raw = path.read_bytes()
            if not raw:
                return f"[Empty file: {path.name}]", "Attachment (empty)", False
            for enc in ("utf-8", "utf-16", "latin-1"):
                try:
                    candidate = raw.decode(enc, errors="ignore").strip()
                    if len(candidate) > 0:
                        return candidate, "Attachment (generic parsed)", True
                except Exception:
                    pass
        except Exception:
            pass
        return (
            f"[File attached: {path.name} -- format not directly supported. "
            f"No recoverable text extracted.]",
            "Attachment (not parsed)", False,
        )

    try:
        result = parse_document(file_path)

        if result.parse_status == ParseStatus.FAILED:
            error_msg = "; ".join(result.errors) if result.errors else "unknown error"
            return (
                f"[Could not extract text from {path.name} -- {error_msg}]",
                doc_type, False,
            )

        text = (result.full_text or "").strip()

        # If full_text is empty but we have sections or tables, assemble text from them
        if not text:
            fallback_parts: list[str] = []
            for sec in result.sections:
                if sec.text and sec.text.strip():
                    fallback_parts.append(sec.text.strip())
            if fallback_parts:
                text = "\n\n".join(fallback_parts)

        if not text:
            return (
                f"[Could not extract text from {path.name} -- "
                f"file may be empty, password-protected, or image-only]",
                doc_type, False,
            )

        succeeded = result.parse_status == ParseStatus.SUCCESS
        return text, doc_type, succeeded

    except Exception as e:
        return f"[Extraction error for {path.name}: {e}]", doc_type, False
