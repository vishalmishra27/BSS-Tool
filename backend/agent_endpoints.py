"""
Agent API Blueprint — POST /chat, /confirm, /reject, GET /alerts, /audit-log
"""

from flask import Blueprint, request, jsonify
import logging
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
