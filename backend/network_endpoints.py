"""
Network Configuration Parser – Enterprise Assurance
Accepts uploaded config files (.txt/.cfg), parses tag-value sections
delimited by '!' markers, and returns structured CSV data.

Algorithm adapted from tatacomm.py:
  - First pass: collect all unique tags from sections between '!' markers
  - Second pass: for each section, extract tag values and write one CSV row
"""

from flask import Blueprint, request, jsonify, send_file
import re
import os
import io
import csv
import tempfile
import logging
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)

network_bp = Blueprint('network', __name__)

NETWORK_UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'network_uploads')
os.makedirs(NETWORK_UPLOAD_DIR, exist_ok=True)


def _parse_config_text(text):
    """
    Parse a single config file text and return (all_tags, rows).
    Each row is a dict mapping tag -> value for one '!' delimited section.

    Algorithm (from tatacomm.py):
      Pass 1 – collect all unique tags:
        - Lines between '!' markers form sections
        - Within each section, the first token of each line is a tag
      Pass 2 – extract values per section:
        - For each section, match each line's first token against known tags
        - If "no " appears in the line, value = "no"
        - Otherwise value = rest of line after the tag
        - Tags not present in a section get empty string
    """
    lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')

    # ── Pass 1: collect tags ──
    all_tags = []
    read_flag1 = False

    for line in lines:
        if re.search(r'!', line):
            read_flag1 = True
            continue
        if read_flag1:
            if '!' in line:
                read_flag1 = False
                continue
            else:
                stripped = line.strip()
                if stripped:
                    splitted = stripped.split()
                    tag = splitted[0]
                    if tag not in all_tags:
                        all_tags.append(tag)

    if not all_tags:
        return [], []

    # ── Pass 2: extract values ──
    rows = []
    all_values = {}
    for tag in all_tags:
        all_values[tag] = None

    read_inter = False
    read_sign = False

    for line in lines:
        if re.search(r'!', line):
            if read_sign:
                read_inter = True
                # Save current section row
                row_values = {}
                for tag in all_tags:
                    val = all_values[tag]
                    row_values[tag] = val if val is not None else ''
                rows.append(row_values)
                # Reset for next section
                for tag in all_tags:
                    all_values[tag] = ''
            else:
                read_sign = True
                for tag in all_tags:
                    all_values[tag] = ''
            continue

        if read_inter:
            stripped = line.strip()
            if not stripped:
                continue
            for tag in all_tags:
                if tag == stripped.split()[0]:
                    if 'no ' in line:
                        value = 'no'
                        _func_tags(all_values, value, tag)
                        break
                    else:
                        value = ' '.join(stripped.split()[1:])
                        _func_tags(all_values, value, tag)
                        break
            else:
                value = ''
                # Line doesn't match any known tag – skip
                continue

    # Flush last section if pending
    if read_sign and read_inter:
        row_values = {}
        for tag in all_tags:
            val = all_values[tag]
            row_values[tag] = val if val is not None else ''
        rows.append(row_values)

    return all_tags, rows


def _func_tags(all_values, value, tag):
    """Accumulate values for a tag (pipe-separated if multiple)."""
    if value is None or value == ' ' or value == '':
        return
    cleaned = value.replace('\n', '')
    if all_values[tag] is None or all_values[tag] == '':
        all_values[tag] = cleaned
    else:
        all_values[tag] = all_values[tag] + '|' + cleaned


@network_bp.route('/process', methods=['POST'])
def process_network_files():
    """
    Accept one or more config files, parse them, and return JSON with
    headers + rows so the frontend can display and download as CSV/Excel.
    """
    try:
        files = request.files.getlist('files')
        if not files or all(f.filename == '' for f in files):
            return jsonify({'error': 'No files provided'}), 400

        all_results = []

        for f in files:
            if not f.filename:
                continue
            safe_name = secure_filename(f.filename)
            text = f.read().decode('utf-8', errors='replace')
            tags, rows = _parse_config_text(text)

            all_results.append({
                'filename': safe_name,
                'headers': tags,
                'rows': rows,
                'row_count': len(rows),
            })

        return jsonify({'success': True, 'results': all_results})
    except Exception as e:
        logger.error(f"Network process error: {e}")
        return jsonify({'error': str(e)}), 500


@network_bp.route('/download-csv', methods=['POST'])
def download_csv():
    """
    Accept headers + rows JSON and return a downloadable CSV file.
    """
    try:
        data = request.json
        headers = data.get('headers', [])
        rows = data.get('rows', [])
        filename = data.get('filename', 'network_output.csv')

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row.get(h, '') for h in headers])

        resp_bytes = output.getvalue().encode('utf-8')
        bio = io.BytesIO(resp_bytes)
        bio.seek(0)

        return send_file(
            bio,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename if filename.endswith('.csv') else filename + '.csv',
        )
    except Exception as e:
        logger.error(f"Network CSV download error: {e}")
        return jsonify({'error': str(e)}), 500
