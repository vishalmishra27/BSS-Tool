"""
OCR Endpoints — PDF text extraction and Q&A for readable (text-layer) PDFs.

Pipeline (identical to AAA 2 / streamlit_app.py):
  Upload PDF → pdfplumber extraction + chunking (pdf_parser.py)
             → keyword retrieval → LLM answer (document_analyser.py)
             → optional Excel export (excel_exporter.py)

Endpoints:
  POST /api/ocr/upload          — save PDFs, detect readable vs scanned
  POST /api/ocr/analyse         — Q&A over uploaded documents
  POST /api/ocr/batch-extract   — extract specific fields from all docs (table output)
  POST /api/ocr/export-excel    — convert last answer to Excel download
  DELETE /api/ocr/reset         — clear session (clears in-memory engine)
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

from pdf_parser import process_pdf, extract_text_from_pdf, clean_text
from document_analyser import DocumentAnalyser
from excel_exporter import (
    dataframe_to_excel_bytes,
    response_to_dataframe,
    batch_results_to_dataframe,
)

load_dotenv()
logger = logging.getLogger(__name__)

ocr_bp = Blueprint('ocr', __name__)

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads', 'pdfs')
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ── In-memory analyser (one per server process; sessions share it) ─────────────
# For multi-user production, key by session token. Fine for single-user tool.
_analyser = DocumentAnalyser()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_readable(pdf_path: str, min_chars: int = 100) -> bool:
    """True if the PDF has a text layer with at least min_chars."""
    try:
        raw = extract_text_from_pdf(pdf_path)
        return len(raw.strip()) >= min_chars
    except Exception:
        return False


# ── Endpoints ─────────────────────────────────────────────────────────────────

@ocr_bp.route('/upload', methods=['POST'])
def upload_pdfs():
    """
    Accept one or more PDF files.
    Saves to disk, checks readability, ingests readable ones into the analyser.
    Returns: { files: [{name, path, readable, chars, chunks}] }
    """
    files = request.files.getlist('files') or request.files.getlist('file')
    if not files:
        return jsonify({'error': 'No files provided'}), 400

    results = []
    for f in files:
        if not f.filename:
            continue
        safe_name = secure_filename(f.filename)
        file_path = os.path.join(UPLOAD_DIR, safe_name)
        f.save(file_path)

        readable = _is_readable(file_path)
        chars = 0
        chunks = 0

        if readable and not _analyser.document_loaded(f.filename):
            try:
                full_text, chunk_list = _analyser.add_document(f.filename, file_path)
                chars = len(full_text)
                chunks = len(chunk_list)
            except Exception as e:
                logger.error(f"Failed to ingest '{f.filename}': {e}")
                readable = False

        elif readable and _analyser.document_loaded(f.filename):
            # Already loaded — report existing stats
            full_text = _analyser.full_text_by_doc.get(f.filename, "")
            chars = len(full_text)
            chunks = len(_analyser.chunks_by_doc.get(f.filename, []))

        results.append({
            'name': f.filename,
            'safe_name': safe_name,
            'path': file_path,
            'readable': readable,
            'chars': chars,
            'chunks': chunks,
        })

    if not results:
        return jsonify({'error': 'No valid PDF files uploaded'}), 400

    return jsonify({'success': True, 'files': results}), 201


@ocr_bp.route('/analyse', methods=['POST'])
def analyse():
    """
    Answer a question over all currently-loaded documents.
    Body: { question: str, file_paths?: [{name, path}] }
    Returns: { answer, docs_processed, unreadable_docs }

    file_paths is optional — if provided, ensures those docs are loaded first.
    """
    data = request.json or {}
    question = data.get('question', '').strip()
    file_paths = data.get('file_paths', [])

    if not question:
        return jsonify({'error': 'No question provided'}), 400

    # Optionally load any docs passed explicitly (e.g. fresh session after restart)
    unreadable = []
    for fp in file_paths:
        name = fp.get('name', '')
        path = fp.get('path', '')
        if not name or not path:
            continue
        if _analyser.document_loaded(name):
            continue
        if not os.path.isfile(path):
            unreadable.append({'name': name, 'reason': 'File not found on server'})
            continue
        if not _is_readable(path):
            unreadable.append({
                'name': name,
                'reason': 'No text layer — scanned PDF. Document Intelligence support coming soon.'
            })
            continue
        try:
            _analyser.add_document(name, path)
        except Exception as e:
            unreadable.append({'name': name, 'reason': str(e)})

    if not _analyser.full_text_by_doc:
        return jsonify({
            'error': 'No readable documents are loaded. Upload readable (text-layer) PDFs first.',
            'unreadable_docs': unreadable,
        }), 422

    try:
        answer = _analyser.query(question)
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return jsonify({'error': f'Analysis failed: {e}'}), 500

    stats = _analyser.get_stats()
    return jsonify({
        'success': True,
        'answer': answer,
        'docs_processed': stats['documents_loaded'],
        'unreadable_docs': unreadable,
        'stats': stats,
    })


@ocr_bp.route('/batch-extract', methods=['POST'])
def batch_extract():
    """
    Extract specific fields from all loaded documents and return as a table.
    Mirrors AAA 2's batch_extract_flow().
    Body: { items: ["Invoice Number", "Total Amount", ...], file_paths?: [...] }
    Returns: { table: [{Document, item1, item2, ...}], excel_b64: "base64..." }
    """
    data = request.json or {}
    items = data.get('items', [])
    file_paths = data.get('file_paths', [])

    if not items:
        return jsonify({'error': 'No items to extract provided'}), 400

    # Ensure docs loaded (same logic as analyse)
    unreadable = []
    for fp in file_paths:
        name = fp.get('name', '')
        path = fp.get('path', '')
        if not name or not path or _analyser.document_loaded(name):
            continue
        if not os.path.isfile(path):
            unreadable.append({'name': name, 'reason': 'File not found'})
            continue
        if not _is_readable(path):
            unreadable.append({'name': name, 'reason': 'Scanned PDF'})
            continue
        try:
            _analyser.add_document(name, path)
        except Exception as e:
            unreadable.append({'name': name, 'reason': str(e)})

    if not _analyser.full_text_by_doc:
        return jsonify({'error': 'No readable documents loaded'}), 422

    try:
        results = _analyser.batch_extract(items)
        df = batch_results_to_dataframe(results, items)
        excel_bytes = dataframe_to_excel_bytes(df, sheet_name="Extractions")
        excel_b64 = base64.b64encode(excel_bytes).decode('utf-8')
        table = df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Batch extract failed: {e}")
        return jsonify({'error': str(e)}), 500

    return jsonify({
        'success': True,
        'items': items,
        'table': table,
        'columns': list(df.columns),
        'excel_b64': excel_b64,
        'unreadable_docs': unreadable,
    })


@ocr_bp.route('/export-excel', methods=['POST'])
def export_excel():
    """
    Convert a free-text answer to an Excel file and return it as a download.
    Body: { answer: str, filename?: str }
    Mirrors AAA 2's render_export() / dataframe_to_excel_bytes() flow.
    """
    data = request.json or {}
    answer = data.get('answer', '').strip()
    if not answer:
        return jsonify({'error': 'No answer text provided'}), 400

    import pandas as pd
    df = response_to_dataframe(answer)
    if df is None or df.empty:
        df = pd.DataFrame([{'Response': answer}])

    excel_bytes = dataframe_to_excel_bytes(df, sheet_name='Output')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = data.get('filename') or f'ocr_output_{timestamp}.xlsx'

    return send_file(
        io.BytesIO(excel_bytes),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename,
    )


@ocr_bp.route('/stats', methods=['GET'])
def stats():
    """Return info about currently-loaded documents."""
    return jsonify(_analyser.get_stats())


@ocr_bp.route('/reset', methods=['DELETE'])
def reset():
    """Clear all loaded documents from memory (mirrors AAA 2 Reset Cached Graph)."""
    _analyser.reset()
    return jsonify({'success': True, 'message': 'Session reset. Upload new documents to start fresh.'})
