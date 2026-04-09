"""
Agent API Blueprint — POST /chat, /confirm, /reject, GET /alerts, /audit-log
"""

from flask import Blueprint, request, jsonify, current_app
import logging
import os
from agent_service import AgentService
from agent_tools import alerts_tool, _query

logger = logging.getLogger(__name__)

agent_bp = Blueprint('agent', __name__)
service = AgentService()


@agent_bp.route('/chat', methods=['POST'])
def agent_chat():
    """
    POST { message, history, current_page }
    Returns { reply, pending_confirmation?, tool_used?, alerts? }
    """
    try:
        data = request.json or {}
        message = data.get('message', '').strip()
        history = data.get('history', [])
        current_page = data.get('current_page', '/')

        if not message:
            return jsonify({'error': 'No message provided'}), 400

        result = service.chat(message, history, current_page)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Agent chat error: {e}")
        return jsonify({'error': str(e)}), 500


@agent_bp.route('/confirm', methods=['POST'])
def agent_confirm():
    """
    POST { pending_id }
    Executes confirmed write, returns { success, rows_affected, diff }
    """
    try:
        data = request.json or {}
        pending_id = data.get('pending_id')
        if not pending_id:
            return jsonify({'error': 'No pending_id provided'}), 400

        result = service.execute_confirmed_write(pending_id)
        status_code = 200 if result.get('success') else 400
        return jsonify(result), status_code
    except Exception as e:
        logger.error(f"Agent confirm error: {e}")
        return jsonify({'error': str(e)}), 500


@agent_bp.route('/reject', methods=['POST'])
def agent_reject():
    """
    POST { pending_id }
    Marks write as rejected, returns { success }
    """
    try:
        data = request.json or {}
        pending_id = data.get('pending_id')
        if not pending_id:
            return jsonify({'error': 'No pending_id provided'}), 400

        result = service.reject_write(pending_id)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Agent reject error: {e}")
        return jsonify({'error': str(e)}), 500


@agent_bp.route('/alerts', methods=['GET'])
def agent_alerts():
    """
    GET — runs alerts_tool() and returns { alerts: [...] }
    """
    try:
        result = alerts_tool()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Agent alerts error: {e}")
        return jsonify({'error': str(e)}), 500


@agent_bp.route('/audit-log', methods=['GET'])
def agent_audit_log():
    """
    GET — returns last 50 rows from agent_audit_log
    """
    try:
        rows = _query("SELECT * FROM agent_audit_log ORDER BY timestamp DESC LIMIT 50")
        result = []
        for r in (rows or []):
            row = dict(r)
            # Ensure diff_json is parsed
            if isinstance(row.get('diff_json'), str):
                import json
                try:
                    row['diff_json'] = json.loads(row['diff_json'])
                except Exception:
                    pass
            result.append(row)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Agent audit-log error: {e}")
        return jsonify({'error': str(e)}), 500


@agent_bp.route('/upload-pdf', methods=['POST'])
def upload_pdf():
    """Upload a PDF file for OCR analysis. Returns the server file path."""
    import os
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        f = request.files['file']
        if not f.filename:
            return jsonify({'error': 'Empty filename'}), 400
        upload_dir = os.path.join(os.path.dirname(__file__), 'uploads', 'pdfs')
        os.makedirs(upload_dir, exist_ok=True)
        from werkzeug.utils import secure_filename
        safe_name = secure_filename(f.filename)
        file_path = os.path.join(upload_dir, safe_name)
        f.save(file_path)
        return jsonify({'success': True, 'file_path': file_path, 'file_name': safe_name}), 201
    except Exception as e:
        logger.error(f"PDF upload error: {e}")
        return jsonify({'error': str(e)}), 500


@agent_bp.route('/extract-pdf', methods=['POST'])
def extract_pdf_text():
    """Extract text from one or more uploaded PDFs. Returns page-wise text."""
    import fitz
    data = request.json or {}
    file_paths = data.get('file_paths', [])
    if not file_paths:
        return jsonify({'error': 'No file paths provided'}), 400

    results = []
    for fp in file_paths:
        if not os.path.isfile(fp):
            results.append({'file': os.path.basename(fp), 'error': 'File not found', 'pages': [], 'full_text': ''})
            continue
        try:
            doc = fitz.open(fp)
            pages = []
            full_text = ''
            for i, page in enumerate(doc):
                txt = page.get_text()
                pages.append({'page': i + 1, 'text': txt})
                full_text += f"--- Page {i + 1} ---\n{txt}\n\n"
            doc.close()
            results.append({
                'file': os.path.basename(fp),
                'page_count': len(pages),
                'pages': pages,
                'full_text': full_text.strip(),
            })
        except Exception as e:
            results.append({'file': os.path.basename(fp), 'error': str(e), 'pages': [], 'full_text': ''})

    return jsonify({'results': results})


@agent_bp.route('/extract-pdf/download', methods=['POST'])
def download_extracted_text():
    """Download extracted PDF text as txt, csv, or xlsx with user-configured options."""
    import fitz
    data = request.json or {}
    file_paths = data.get('file_paths', [])
    fmt = data.get('format', 'txt')
    selected_pages = data.get('selected_pages', {})  # { file_path: [1,2,3] }
    include_file_col = data.get('include_file_col', True)
    include_page_col = data.get('include_page_col', True)
    include_text_col = data.get('include_text_col', True)
    text_mode = data.get('text_mode', 'full')  # full | lines
    include_page_breaks = data.get('include_page_breaks', True)

    if not file_paths:
        return jsonify({'error': 'No file paths provided'}), 400

    # Extract text with page filtering
    all_rows = []  # list of dicts
    for fp in file_paths:
        if not os.path.isfile(fp):
            continue
        allowed_pages = selected_pages.get(fp)  # None = all pages
        try:
            doc = fitz.open(fp)
            for i, page in enumerate(doc):
                page_num = i + 1
                if allowed_pages and page_num not in allowed_pages:
                    continue
                txt = page.get_text().strip()
                if text_mode == 'lines':
                    for line in txt.split('\n'):
                        line = line.strip()
                        if line:
                            all_rows.append({'file': os.path.basename(fp), 'page': page_num, 'text': line})
                else:
                    all_rows.append({'file': os.path.basename(fp), 'page': page_num, 'text': txt})
            doc.close()
        except:
            pass

    if fmt == 'txt':
        content = ''
        for row in all_rows:
            if include_page_breaks and text_mode == 'full':
                content += f"=== {row['file']} — Page {row['page']} ===\n"
            content += row['text'] + '\n'
            if text_mode == 'full':
                content += '\n'
        resp = current_app.response_class(content, mimetype='text/plain')
        resp.headers['Content-Disposition'] = 'attachment; filename=extracted_text.txt'
        return resp

    # Build header and row data based on column config
    headers = []
    if include_file_col: headers.append('File')
    if include_page_col: headers.append('Page')
    if include_text_col: headers.append('Text')
    if not headers:
        return jsonify({'error': 'At least one column must be selected'}), 400

    def row_vals(r):
        vals = []
        if include_file_col: vals.append(r['file'])
        if include_page_col: vals.append(r['page'])
        if include_text_col: vals.append(r['text'])
        return vals

    if fmt == 'csv':
        import csv as csv_mod, io
        output = io.StringIO()
        writer = csv_mod.writer(output)
        writer.writerow(headers)
        for r in all_rows:
            writer.writerow(row_vals(r))
        resp = current_app.response_class(output.getvalue(), mimetype='text/csv')
        resp.headers['Content-Disposition'] = 'attachment; filename=extracted_text.csv'
        return resp

    elif fmt == 'xlsx':
        import io
        try:
            import openpyxl
        except ImportError:
            return jsonify({'error': 'openpyxl not installed. Run: pip install openpyxl'}), 500
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = 'Extracted Text'
        ws.append(headers)
        for r in all_rows:
            ws.append(row_vals(r))
        for col in ws.columns:
            max_len = max(len(str(cell.value or '')[:200]) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 80)
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        resp = current_app.response_class(buf.getvalue(), mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        resp.headers['Content-Disposition'] = 'attachment; filename=extracted_text.xlsx'
        return resp

    return jsonify({'error': f'Unknown format: {fmt}'}), 400
