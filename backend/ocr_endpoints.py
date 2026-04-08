"""
OCR / Document Analysis Endpoints.

Routing strategy:
  • Readable PDFs (has a text layer)  →  pdfplumber extraction (fast, no API call)
  • Scanned PDFs, images, DOCX, XLSX, PPTX, EML, MSG, CSV, TSV
                                      →  Azure Document Intelligence

Endpoints:
  POST   /api/ocr/upload          — upload documents; detect readable vs DI-required
  POST   /api/ocr/analyse         — Q&A over all loaded documents
  POST   /api/ocr/batch-extract   — extract specific fields → table + Excel
  POST   /api/ocr/export-excel    — convert free-text answer to Excel download
  GET    /api/ocr/stats           — info about currently-loaded documents
  DELETE /api/ocr/reset           — clear session
"""

import os
import io
import json
import base64
import logging
from datetime import datetime
from flask import Blueprint, request, jsonify, send_file
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

from pdf_parser import extract_text_from_pdf
from document_analyser import DocumentAnalyser
from excel_exporter import (
    dataframe_to_excel_bytes,
    response_to_dataframe,
    batch_results_to_dataframe,
)
import document_intelligence as di

load_dotenv()
logger = logging.getLogger(__name__)

ocr_bp = Blueprint('ocr', __name__)

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads', 'pdfs')
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Accepted file extensions
_PDF_EXT   = {".pdf"}
_DI_EXTS   = di.ALLOWED_EXTENSIONS - _PDF_EXT   # everything except PDF is always DI
_ALL_EXTS  = di.ALLOWED_EXTENSIONS               # PDFs + all others

# ── In-memory analyser (one per server process; fine for single-user tool) ────
_analyser = DocumentAnalyser()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _file_ext(filename: str) -> str:
    return os.path.splitext(filename)[1].lower()


def _is_pdf_readable(pdf_path: str, min_chars: int = 100) -> bool:
    """True when the PDF has a text layer with at least min_chars of content."""
    try:
        raw = extract_text_from_pdf(pdf_path)
        return len(raw.strip()) >= min_chars
    except Exception:
        return False


def _ingest_via_di(name: str, file_path: str) -> dict:
    """
    Parse a file through Azure Document Intelligence, then store the extracted
    text as chunks in the shared DocumentAnalyser.
    Returns a result dict compatible with the upload response schema.
    """
    result = di.parse_document(file_path)
    if result.parse_status == di.ParseStatus.FAILED or not result.full_text:
        error_msg = (result.errors[0] if result.errors
                     else "Document Intelligence returned no text")
        return {
            'name':     name,
            'path':     file_path,
            'readable': False,
            'di_used':  True,
            'chars':    0,
            'chunks':   0,
            'error':    error_msg,
        }
    full_text, chunks = _analyser.add_document_from_text(name, result.full_text)
    return {
        'name':        name,
        'path':        file_path,
        'readable':    True,
        'di_used':     True,
        'parser':      result.parse_meta.parser_used,
        'ocr_used':    result.parse_meta.ocr_used,
        'chars':       len(full_text),
        'chunks':      len(chunks),
        'page_count':  result.metadata.page_count,
        'warnings':    result.parse_meta.warnings or [],
    }


# ── Upload ─────────────────────────────────────────────────────────────────────

@ocr_bp.route('/upload', methods=['POST'])
def upload_documents():
    """
    Accept one or more files of any supported type.
    Saves to disk, routes through the correct parser, ingests into the analyser.

    Returns:
      { files: [{ name, path, readable, di_used, chars, chunks, ... }] }
    """
    files = request.files.getlist('files') or request.files.getlist('file')
    if not files:
        return jsonify({'error': 'No files provided'}), 400

    results = []
    for f in files:
        if not f.filename:
            continue

        ext = _file_ext(f.filename)
        if ext not in _ALL_EXTS:
            results.append({
                'name':     f.filename,
                'readable': False,
                'di_used':  False,
                'error':    (
                    f"Unsupported file type '{ext}'. "
                    f"Supported: {', '.join(sorted(_ALL_EXTS))}"
                ),
            })
            continue

        safe_name = secure_filename(f.filename)
        file_path = os.path.join(UPLOAD_DIR, safe_name)
        f.save(file_path)

        # ── Already loaded? Return cached stats ──────────────────────────────
        if _analyser.document_loaded(f.filename):
            full_text = _analyser.full_text_by_doc.get(f.filename, "")
            results.append({
                'name':    f.filename,
                'path':    file_path,
                'readable': True,
                'di_used': False,
                'chars':   len(full_text),
                'chunks':  len(_analyser.chunks_by_doc.get(f.filename, [])),
            })
            continue

        # ── PDFs: check for text layer first ─────────────────────────────────
        if ext == ".pdf":
            if _is_pdf_readable(file_path):
                # Readable PDF → fast pdfplumber path
                try:
                    full_text, chunks = _analyser.add_document(f.filename, file_path)
                    results.append({
                        'name':     f.filename,
                        'safe_name': safe_name,
                        'path':     file_path,
                        'readable': True,
                        'di_used':  False,
                        'chars':    len(full_text),
                        'chunks':   len(chunks),
                    })
                except Exception as e:
                    logger.error(f"OCR ingest failed for '{f.filename}': {e}")
                    results.append({
                        'name':     f.filename,
                        'path':     file_path,
                        'readable': False,
                        'di_used':  False,
                        'error':    str(e),
                    })
            else:
                # Scanned PDF → Document Intelligence
                if not di.is_ocr_available():
                    results.append({
                        'name':     f.filename,
                        'path':     file_path,
                        'readable': False,
                        'di_used':  True,
                        'error': (
                            "Scanned PDF detected but Azure Document Intelligence is not "
                            "configured. Set AZURE_DOC_INTELLIGENCE_KEY and "
                            "AZURE_DOC_INTELLIGENCE_ENDPOINT in your .env file."
                        ),
                    })
                else:
                    result_entry = _ingest_via_di(f.filename, file_path)
                    result_entry['safe_name'] = safe_name
                    results.append(result_entry)

        # ── All other formats → Document Intelligence (or stdlib/pandas parsers) ─
        else:
            # These parsers use stdlib/pandas — no Azure DI key required
            _NO_DI_REQUIRED = {'.csv', '.tsv', '.eml', '.msg', '.xlsx', '.xls', '.xlsm'}
            if ext in _NO_DI_REQUIRED:
                result_entry = _ingest_via_di(f.filename, file_path)
            else:
                # DOCX, PPTX, images, legacy .doc → Azure DI required
                if not di.is_ocr_available():
                    results.append({
                        'name':     f.filename,
                        'path':     file_path,
                        'readable': False,
                        'di_used':  True,
                        'error': (
                            f"{di.FORMAT_LABELS.get(ext, ext)} files require Azure Document "
                            "Intelligence. Set AZURE_DOC_INTELLIGENCE_KEY and "
                            "AZURE_DOC_INTELLIGENCE_ENDPOINT in your .env file."
                        ),
                    })
                    continue
                result_entry = _ingest_via_di(f.filename, file_path)
            result_entry['safe_name'] = safe_name
            results.append(result_entry)

    if not results:
        return jsonify({'error': 'No valid files uploaded'}), 400

    return jsonify({'success': True, 'files': results}), 201


# ── Analyse ───────────────────────────────────────────────────────────────────

@ocr_bp.route('/analyse', methods=['POST'])
def analyse():
    """
    Answer a question over all currently-loaded documents.
    Body: { question: str, file_paths?: [{ name, path }] }
    Returns: { answer, docs_processed, unreadable_docs, stats }
    """
    data     = request.json or {}
    question = data.get('question', '').strip()
    file_paths = data.get('file_paths', [])

    if not question:
        return jsonify({'error': 'No question provided'}), 400

    unreadable = []
    for fp in file_paths:
        name = fp.get('name', '')
        path = fp.get('path', '')
        if not name or not path or _analyser.document_loaded(name):
            continue
        if not os.path.isfile(path):
            unreadable.append({'name': name, 'reason': 'File not found on server'})
            continue

        ext = _file_ext(name)

        if ext == ".pdf":
            if _is_pdf_readable(path):
                try:
                    _analyser.add_document(name, path)
                except Exception as e:
                    unreadable.append({'name': name, 'reason': str(e)})
            elif di.is_ocr_available():
                entry = _ingest_via_di(name, path)
                if not entry.get('readable'):
                    unreadable.append({
                        'name': name,
                        'reason': entry.get('error', 'Document Intelligence parsing failed'),
                    })
            else:
                unreadable.append({
                    'name': name,
                    'reason': (
                        'Scanned PDF — Azure Document Intelligence not configured. '
                        'Add AZURE_DOC_INTELLIGENCE_KEY / ENDPOINT to .env'
                    ),
                })
        elif ext in {'.csv', '.tsv', '.eml', '.msg', '.xlsx', '.xls', '.xlsm'}:
            # stdlib/pandas parsers — no Azure DI needed
            entry = _ingest_via_di(name, path)
            if not entry.get('readable'):
                unreadable.append({'name': name, 'reason': entry.get('error', 'Parse failed')})
        elif di.is_ocr_available():
            entry = _ingest_via_di(name, path)
            if not entry.get('readable'):
                unreadable.append({'name': name, 'reason': entry.get('error', 'Parse failed')})
        else:
            unreadable.append({
                'name': name,
                'reason': (
                    f'{di.FORMAT_LABELS.get(ext, ext)} requires Azure Document Intelligence — '
                    'not configured'
                ),
            })

    if not _analyser.full_text_by_doc:
        return jsonify({
            'error': 'No documents are loaded. Upload files first.',
            'unreadable_docs': unreadable,
        }), 422

    try:
        answer = _analyser.query(question)
    except RuntimeError as e:
        logger.warning(f"Query unavailable: {e}")
        return jsonify({'error': str(e)}), 503
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return jsonify({'error': f'Analysis failed: {e}'}), 500

    stats = _analyser.get_stats()
    return jsonify({
        'success':       True,
        'answer':        answer,
        'docs_processed': stats['documents_loaded'],
        'unreadable_docs': unreadable,
        'stats':         stats,
    })


# ── Batch extract ─────────────────────────────────────────────────────────────

@ocr_bp.route('/batch-extract', methods=['POST'])
def batch_extract():
    """
    Extract specific fields from all loaded documents → table + Excel.
    Body: { items: ["Invoice Number", "Total Amount", ...], file_paths?: [...] }
    """
    data       = request.json or {}
    items      = data.get('items', [])
    file_paths = data.get('file_paths', [])

    if not items:
        return jsonify({'error': 'No items to extract provided'}), 400

    unreadable = []
    for fp in file_paths:
        name = fp.get('name', '')
        path = fp.get('path', '')
        if not name or not path or _analyser.document_loaded(name):
            continue
        if not os.path.isfile(path):
            unreadable.append({'name': name, 'reason': 'File not found'})
            continue

        ext = _file_ext(name)
        _NO_DI = {'.csv', '.tsv', '.eml', '.msg', '.xlsx', '.xls', '.xlsm'}
        if ext == ".pdf" and _is_pdf_readable(path):
            try:
                _analyser.add_document(name, path)
            except Exception as e:
                unreadable.append({'name': name, 'reason': str(e)})
        elif ext in _NO_DI or di.is_ocr_available():
            entry = _ingest_via_di(name, path)
            if not entry.get('readable'):
                unreadable.append({'name': name, 'reason': entry.get('error', 'Parse failed')})
        else:
            unreadable.append({'name': name, 'reason': 'Azure DI not configured'})

    if not _analyser.full_text_by_doc:
        return jsonify({'error': 'No documents loaded'}), 422

    try:
        results    = _analyser.batch_extract(items)
        df         = batch_results_to_dataframe(results, items)
        excel_bytes = dataframe_to_excel_bytes(df, sheet_name="Extractions")
        excel_b64  = base64.b64encode(excel_bytes).decode('utf-8')
        table      = df.to_dict(orient='records')
    except RuntimeError as e:
        logger.warning(f"Batch extract unavailable: {e}")
        return jsonify({'error': str(e)}), 503
    except Exception as e:
        logger.error(f"Batch extract failed: {e}")
        return jsonify({'error': str(e)}), 500

    return jsonify({
        'success':       True,
        'items':         items,
        'table':         table,
        'columns':       list(df.columns),
        'excel_b64':     excel_b64,
        'unreadable_docs': unreadable,
    })


# ── Export Excel ──────────────────────────────────────────────────────────────

@ocr_bp.route('/export-excel', methods=['POST'])
def export_excel():
    """
    Convert a free-text answer to an Excel file download.
    Body: { answer: str, filename?: str }
    """
    data   = request.json or {}
    answer = data.get('answer', '').strip()
    if not answer:
        return jsonify({'error': 'No answer text provided'}), 400

    import pandas as pd
    df = response_to_dataframe(answer)
    if df is None or df.empty:
        df = pd.DataFrame([{'Response': answer}])

    excel_bytes = dataframe_to_excel_bytes(df, sheet_name='Output')
    timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename    = data.get('filename') or f'doc_analysis_{timestamp}.xlsx'

    return send_file(
        io.BytesIO(excel_bytes),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename,
    )


# ── Stats / Reset ─────────────────────────────────────────────────────────────

@ocr_bp.route('/stats', methods=['GET'])
def stats():
    return jsonify(_analyser.get_stats())


@ocr_bp.route('/reset', methods=['DELETE'])
def reset():
    _analyser.reset()
    return jsonify({'success': True, 'message': 'Session reset. Upload documents to start fresh.'})
