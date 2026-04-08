from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os
import json
import base64
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ─── DB Config ────────────────────────────────────────────────────────────────
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'bss_tool'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
}

def get_db():
    return psycopg2.connect(**DB_CONFIG)

def query(sql, params=None, fetch='all'):
    conn = get_db()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or ())
        if fetch == 'all':
            result = cur.fetchall()
        elif fetch == 'one':
            result = cur.fetchone()
        else:
            result = None
        conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        logger.error(f"DB error: {e}")
        raise
    finally:
        conn.close()

# ─── Health ───────────────────────────────────────────────────────────────────
@app.route('/api/health')
def health():
    return jsonify({'status': 'ok'})

# ─── Transformation Dashboard ─────────────────────────────────────────────────
@app.route('/api/transformation/summary')
def transformation_summary():
    try:
        rows = query("""
            SELECT lob, planned, actual,
                   ROUND(actual::numeric / NULLIF(planned,0) * 100, 1) as pct
            FROM transformation_lob_progress
            ORDER BY lob
        """)
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/transformation/activities')
def transformation_activities():
    try:
        rows = query("SELECT * FROM transformation_activities ORDER BY lob, phase_name")
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Milestones / Phases ──────────────────────────────────────────────────────
@app.route('/api/phases')
def get_phases():
    try:
        rows = query("SELECT * FROM phases ORDER BY phase_id")
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/phases/<phase_id>')
def get_phase(phase_id):
    try:
        row = query("SELECT * FROM phases WHERE phase_id = %s", (phase_id,), fetch='one')
        return jsonify(dict(row) if row else {})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/phases/<phase_id>', methods=['PUT'])
def update_phase(phase_id):
    try:
        data = request.json
        fields = []
        values = []
        for col in ('curr_status', 'start_dt', 'end_dt', 'assigned_to'):
            if col in data:
                fields.append(f"{col}=%s")
                values.append(data[col])
        if not fields:
            return jsonify({'error': 'No fields to update'}), 400
        values.append(phase_id)
        query(f"UPDATE phases SET {', '.join(fields)} WHERE phase_id=%s", tuple(values), fetch=None)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Checklist ────────────────────────────────────────────────────────────────
@app.route('/api/checklist/<phase_id>')
def get_checklist(phase_id):
    try:
        rows = query("SELECT * FROM checklist WHERE phase_id = %s ORDER BY ch_id", (phase_id,))
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/checklist/<int:ch_id>/status', methods=['PUT'])
def update_checklist_status(ch_id):
    try:
        data = request.json
        query("UPDATE checklist SET status=%s WHERE ch_id=%s", (data.get('status'), ch_id), fetch=None)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Checklist Comments ───────────────────────────────────────────────────────
@app.route('/api/checklist/<int:ch_id>/comments')
def get_checklist_comments(ch_id):
    try:
        rows = query("SELECT * FROM checklist_comments WHERE ch_id=%s ORDER BY created_at DESC", (ch_id,))
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/checklist/<int:ch_id>/comments', methods=['POST'])
def add_checklist_comment(ch_id):
    try:
        data = request.json
        query(
            "INSERT INTO checklist_comments (ch_id, username, comment) VALUES (%s, %s, %s)",
            (ch_id, data.get('username', 'Programme User'), data['comment']),
            fetch=None
        )
        return jsonify({'success': True}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Checklist Attachments ────────────────────────────────────────────────────
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads')
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.route('/api/checklist/<int:ch_id>/attachments')
def get_checklist_attachments(ch_id):
    try:
        rows = query("SELECT * FROM checklist_attachments WHERE ch_id=%s ORDER BY uploaded_at DESC", (ch_id,))
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/checklist/<int:ch_id>/attachments', methods=['POST'])
def upload_checklist_attachment(ch_id):
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        f = request.files['file']
        if not f.filename:
            return jsonify({'error': 'Empty filename'}), 400
        # Save under uploads/<ch_id>/
        item_dir = os.path.join(UPLOAD_DIR, str(ch_id))
        os.makedirs(item_dir, exist_ok=True)
        from werkzeug.utils import secure_filename
        safe_name = secure_filename(f.filename)
        file_path = os.path.join(item_dir, safe_name)
        f.save(file_path)
        uploaded_by = request.form.get('username', 'Programme User')
        query(
            "INSERT INTO checklist_attachments (ch_id, file_name, file_path, uploaded_by) VALUES (%s, %s, %s, %s)",
            (ch_id, safe_name, file_path, uploaded_by),
            fetch=None
        )
        return jsonify({'success': True, 'file_name': safe_name, 'file_path': file_path}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/checklist/attachments/download/<int:attachment_id>')
def download_checklist_attachment(attachment_id):
    try:
        row = query("SELECT * FROM checklist_attachments WHERE id=%s", (attachment_id,), fetch='one')
        if not row:
            return jsonify({'error': 'Not found'}), 404
        from flask import send_file
        return send_file(row['file_path'], as_attachment=True, download_name=row['file_name'])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Workflow ─────────────────────────────────────────────────────────────────
@app.route('/api/workflow')
def get_workflow():
    try:
        rows = query("SELECT * FROM main_workflow ORDER BY id")
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/workflow/<wf_id>/stages')
def get_stages(wf_id):
    try:
        rows = query("SELECT * FROM stages WHERE main_wf_id = %s ORDER BY id", (wf_id,))
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stages/<stage_id>/phases')
def get_stage_phases(stage_id):
    try:
        rows = query("SELECT * FROM phases WHERE stage_id = %s ORDER BY id", (stage_id,))
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/workflow_nodes')
def get_workflow_nodes():
    try:
        rows = query("SELECT * FROM main_workflow ORDER BY id")
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/transformation-dashboard')
def get_transformation_dashboard():
    try:
        rows = query("SELECT * FROM transformation_activities ORDER BY phase_name, lob")

        # Pivot rows into activities grouped by phase_name
        from collections import OrderedDict
        phase_map = OrderedDict()
        total_planned = 0
        total_actual = 0
        count = 0
        for r in rows:
            pn = r['phase_name']
            if pn not in phase_map:
                phase_map[pn] = {}
            phase_map[pn][r['lob']] = {
                'planned': float(r.get('planned', 0) or 0),
                'actual': float(r.get('actual', 0) or 0),
            }
            total_planned += float(r.get('planned', 0) or 0)
            total_actual += float(r.get('actual', 0) or 0)
            count += 1

        activities = [{'name': pn, 'lobData': lobs} for pn, lobs in phase_map.items()]

        avg_planned = round(total_planned / count, 1) if count else 0
        avg_actual = round(total_actual / count, 1) if count else 0

        overview = {
            'project_name': 'Digital Transformation Initiative',
            'start_date': '2024-01-15',
            'report_date': '2024-12-15',
            'planned_progress': avg_planned,
            'actual_progress': avg_actual,
            'variance': round(avg_actual - avg_planned, 1),
        }

        # Areas where variance is significantly negative
        attention_areas = []
        for r in rows:
            planned = float(r.get('planned', 0) or 0)
            actual = float(r.get('actual', 0) or 0)
            if planned - actual > 10:
                attention_areas.append({
                    'title': f"{r['phase_name']} - {r['lob']}",
                    'description': f"Behind schedule: planned {planned}% vs actual {actual}% (variance: {round(actual - planned, 1)}%)"
                })

        return jsonify({
            'overview': overview,
            'activities': activities,
            'attentionAreas': attention_areas
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Phase name ↔ phase_id mapping (shared with milestones & workflow) ───────
PHASE_NAMES = {
    'phase1': 'Initiation & Planning',
    'phase2': 'SRS Finalization',
    'phase3': 'Product Rationalization',
    'phase4': 'Configuration Validation',
    'phase5': 'Data Cleanup & Preparation',
    'phase6': 'UAT Execution',
    'phase7': 'Trial / Dry Run Migrations',
    'phase8': 'Final Migration & Cutover',
    'phase9': 'Post-Migration Stabilization',
}
PHASE_NAME_TO_ID = {v: k for k, v in PHASE_NAMES.items()}

def _get_checklist_progress():
    """Compute per-phase completion % from checklist table (shared with milestones & workflow)."""
    rows = query("""
        SELECT phase_id,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE status = 'complete') AS done
        FROM checklist
        GROUP BY phase_id
    """)
    progress = {}
    for r in rows:
        total = int(r['total'])
        done = int(r['done'])
        progress[r['phase_id']] = round(done / total * 100, 1) if total else 0
    return progress

# ─── Transformation Dashboard (split endpoints) ─────────────────────────────
@app.route('/api/project_overview')
def get_project_overview():
    try:
        # Derive overview from phases + checklist (same source as milestones)
        phases = query("SELECT * FROM phases ORDER BY phase_id")
        checklist_progress = _get_checklist_progress()

        total_planned = 0
        total_actual = 0
        count = 0
        for p in phases:
            pid = p['phase_id']
            planned = 100 if p['curr_status'] in ('complete', 'current') else 0
            actual = checklist_progress.get(pid, 100 if p['curr_status'] == 'complete' else 0)
            total_planned += planned
            total_actual += actual
            count += 1

        avg_planned = round(total_planned / count, 1) if count else 0
        avg_actual = round(total_actual / count, 1) if count else 0

        return jsonify({
            'project_name': 'Digital Transformation Initiative',
            'start_date': phases[0]['start_dt'] if phases else '2024-01-15',
            'report_date': phases[-1]['end_dt'] if phases else '2024-12-15',
            'planned_progress': avg_planned,
            'actual_progress': avg_actual,
            'variance': round(avg_actual - avg_planned, 1),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/project_activities')
def get_project_activities():
    try:
        # Use transformation_activities for per-LOB data, but enrich with phase_id
        phase_order = list(PHASE_NAMES.keys())  # phase1, phase2, ... phase9
        rows = query("SELECT * FROM transformation_activities ORDER BY lob")
        activities = []
        for r in rows:
            phase_name = r['phase_name']
            phase_id = PHASE_NAME_TO_ID.get(phase_name, '')
            activities.append({
                'activity_name': phase_name,
                'phase_id': phase_id,
                'lob': r['lob'],
                'planned_progress': float(r.get('planned', 0) or 0),
                'actual_progress': float(r.get('actual', 0) or 0),
            })
        # Sort by phase order (phase1→phase9), then by lob
        activities.sort(key=lambda a: (phase_order.index(a['phase_id']) if a['phase_id'] in phase_order else 99, a.get('lob', '')))
        return jsonify(activities)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/attention_areas')
def get_attention_areas():
    try:
        rows = query("SELECT * FROM transformation_activities")
        attention = []
        for r in rows:
            planned = float(r.get('planned', 0) or 0)
            actual = float(r.get('actual', 0) or 0)
            if planned - actual > 10:
                attention.append({
                    'section': r['phase_name'],
                    'lob': r['lob'],
                    'notes': f"Behind schedule: planned {planned}% vs actual {actual}% (variance: {round(actual - planned, 1)}%)"
                })
        return jsonify(attention)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/phase/<phase_id>')
def get_single_phase(phase_id):
    """Get phase details enriched with name from PHASE_NAMES mapping."""
    try:
        row = query("SELECT * FROM phases WHERE phase_id = %s", (phase_id,), fetch='one')
        if not row:
            return jsonify({}), 404
        result = dict(row)
        result['phase_name'] = PHASE_NAMES.get(phase_id, phase_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Products ─────────────────────────────────────────────────────────────────
@app.route('/api/products')
def get_products():
    try:
        lob = request.args.get('lob')
        if lob:
            rows = query("SELECT * FROM products WHERE lob = %s ORDER BY product_id", (lob,))
        else:
            rows = query("SELECT * FROM products ORDER BY lob, product_id")
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/products/summary')
def products_summary():
    try:
        rows = query("""
            SELECT lob,
                   COUNT(*) as total,
                   SUM(CASE WHEN migration_flag='migrate' THEN 1 ELSE 0 END) as to_migrate,
                   SUM(CASE WHEN migration_flag='purge' THEN 1 ELSE 0 END) as to_purge,
                   SUM(CASE WHEN status='configured' THEN 1 ELSE 0 END) as configured
            FROM products GROUP BY lob ORDER BY lob
        """)
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/products/<product_id>', methods=['PUT'])
def update_product(product_id):
    try:
        data = request.json
        fields = ', '.join(f"{k}=%s" for k in data)
        vals = list(data.values()) + [product_id]
        query(f"UPDATE products SET {fields} WHERE product_id=%s", vals, fetch=None)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/products', methods=['POST'])
def add_product():
    try:
        data = request.json
        query("""
            INSERT INTO products (product_id, product_name, lob, migration_flag, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (data['product_id'], data['product_name'], data['lob'],
              data.get('migration_flag', 'migrate'), data.get('status', 'pending')), fetch=None)
        return jsonify({'success': True}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Parameters ───────────────────────────────────────────────────────────────
@app.route('/api/parameters')
def get_parameters():
    try:
        rows = query("SELECT * FROM product_parameters ORDER BY param_name, lob")
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/parameters/<int:param_id>', methods=['PUT'])
def update_parameter(param_id):
    try:
        data = request.json
        query("UPDATE product_parameters SET status=%s WHERE id=%s",
              (data.get('status'), param_id), fetch=None)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── UAT / Test Cases ─────────────────────────────────────────────────────────
@app.route('/api/uat/summary')
def uat_summary():
    try:
        row = query("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='CLOSED' THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) as open,
                SUM(CASE WHEN status='DEFECT' THEN 1 ELSE 0 END) as defects,
                COUNT(DISTINCT lob) as lob_count
            FROM uat_cases
        """, fetch='one')
        return jsonify(dict(row))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/uat/cases')
def get_uat_cases():
    try:
        lob = request.args.get('lob')
        status = request.args.get('status')
        conditions, params = [], []
        if lob:
            conditions.append("lob = %s"); params.append(lob)
        if status:
            conditions.append("status = %s"); params.append(status)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        rows = query(f"SELECT * FROM uat_cases {where} ORDER BY test_case_id", params)
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/uat/cases', methods=['POST'])
def add_uat_case():
    try:
        data = request.json
        query("""
            INSERT INTO uat_cases (test_case_id, lob, priority, status, description)
            VALUES (%s, %s, %s, %s, %s)
        """, (data['test_case_id'], data['lob'], data.get('priority', 'Medium'),
              data.get('status', 'OPEN'), data.get('description', '')), fetch=None)
        return jsonify({'success': True}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/uat/cases/<test_case_id>', methods=['PUT'])
def update_uat_case(test_case_id):
    try:
        data = request.json
        fields = ', '.join(f"{k}=%s" for k in data)
        vals = list(data.values()) + [test_case_id]
        query(f"UPDATE uat_cases SET {fields}, updated_at=NOW() WHERE test_case_id=%s", vals, fetch=None)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/uat/lob-distribution')
def uat_lob_distribution():
    try:
        rows = query("""
            SELECT lob,
                SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) as open,
                SUM(CASE WHEN status='CLOSED' THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN status='REOPENED' THEN 1 ELSE 0 END) as reopened,
                SUM(CASE WHEN status='CANCELLED' THEN 1 ELSE 0 END) as cancelled,
                SUM(CASE WHEN status='READY_FOR_TESTING' THEN 1 ELSE 0 END) as ready_for_testing,
                SUM(CASE WHEN status='NEEDS_FIX' THEN 1 ELSE 0 END) as needs_fix,
                SUM(CASE WHEN status='DEFECT' THEN 1 ELSE 0 END) as defect
            FROM uat_cases GROUP BY lob ORDER BY lob
        """)
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/uat/priority-distribution')
def uat_priority_distribution():
    try:
        rows = query("""
            SELECT priority,
                COUNT(*) as count
            FROM uat_cases GROUP BY priority ORDER BY priority
        """)
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/legacy-products/raw')
def legacy_products_raw():
    try:
        rows = query("""
            SELECT product_id, product_name, lob,
                   CASE WHEN migration_flag='migrate' THEN 'Migrate' ELSE 'Purge' END as rationalization_status,
                   CASE WHEN status='configured' THEN NULL ELSE 'Pending Configuration' END as pending_on
            FROM products ORDER BY lob, product_id
        """)
        return jsonify({'data': list(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/legacy-products/export')
def legacy_products_export():
    try:
        rows = query("SELECT product_id, product_name, lob, migration_flag, status FROM products ORDER BY lob, product_id")
        import csv, io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['product_id', 'product_name', 'lob', 'migration_flag', 'status'])
        for r in rows:
            writer.writerow([r['product_id'], r['product_name'], r['lob'], r['migration_flag'], r['status']])
        resp = app.response_class(output.getvalue(), mimetype='text/csv')
        resp.headers['Content-Disposition'] = 'attachment; filename=legacy_products.csv'
        return resp
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Workflow Comments ────────────────────────────────────────────────────────
@app.route('/api/workflow/comments/<phase_id>')
def get_workflow_comments(phase_id):
    try:
        rows = query("SELECT * FROM workflow_comments WHERE phase_id=%s ORDER BY created_at DESC", (phase_id,))
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/workflow/comments', methods=['POST'])
def add_workflow_comment():
    try:
        data = request.json
        query("""
            INSERT INTO workflow_comments (phase_id, username, action, comment, attachment_name)
            VALUES (%s, %s, %s, %s, %s)
        """, (data['phase_id'], data.get('username','Programme User'),
              data.get('action','Commented'), data['comment'],
              data.get('attachment_name')), fetch=None)
        return jsonify({'success': True}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Reconciliation ───────────────────────────────────────────────────────────
@app.route('/api/reconciliation/summary')
def recon_summary():
    try:
        rows = query("""
            SELECT status, COUNT(*) as count
            FROM reconciliation_data
            GROUP BY status ORDER BY count DESC
        """)
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/reconciliation/upload', methods=['POST'])
def upload_recon():
    return jsonify({'message': 'Upload endpoint ready'}), 200

# ─── KPI ──────────────────────────────────────────────────────────────────────
@app.route('/api/kpi')
def get_kpis():
    try:
        rows = query("SELECT * FROM kpi_results ORDER BY metric_name")
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── Auth ─────────────────────────────────────────────────────────────────────
try:
    from auth_service import check_password, create_token, get_permissions, require_auth, decode_token
    AUTH_AVAILABLE = True
except Exception as e:
    logger.warning(f"Auth service unavailable: {e}")
    AUTH_AVAILABLE = False


@app.route('/api/auth/login', methods=['POST'])
def auth_login():
    data = request.json or {}
    username = data.get('username', '').strip()
    password = data.get('password', '')
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400
    try:
        user = query("SELECT * FROM users WHERE username=%s AND is_active=TRUE", (username,), fetch='one')
        if not user:
            return jsonify({'error': 'Invalid username or password'}), 401
        user = dict(user)
        if not AUTH_AVAILABLE or not check_password(password, user['password_hash']):
            return jsonify({'error': 'Invalid username or password'}), 401
        query("UPDATE users SET last_login=NOW() WHERE id=%s", (user['id'],), fetch=None)
        token = create_token(user)
        permissions = get_permissions(user['role'])
        return jsonify({
            'token': token,
            'user': {
                'id': user['id'],
                'username': user['username'],
                'full_name': user['full_name'],
                'role': user['role'],
                'organisation': user['organisation'],
                'email': user['email'],
            },
            'permissions': permissions,
        })
    except Exception as e:
        logger.error(f"Login error: {e}")
        return jsonify({'error': 'Login failed'}), 500


@app.route('/api/auth/me')
def auth_me():
    """Return current user info from JWT token."""
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return jsonify({'error': 'Not authenticated'}), 401
    try:
        payload = decode_token(auth_header.split(' ', 1)[1])
        permissions = get_permissions(payload['role'])
        return jsonify({'user': payload, 'permissions': permissions})
    except Exception as e:
        logger.error(f"auth/me token error: {e}")
        return jsonify({'error': str(e)}), 401


@app.route('/api/auth/users')
def list_users():
    """Return user list (no password hashes) — for admin use."""
    try:
        rows = query("SELECT id, username, full_name, email, role, organisation, is_active, created_at, last_login FROM users ORDER BY role, username")
        return jsonify(list(rows))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Agentic AI (new tool-calling agent) ─────────────────────────────────────
try:
    from agent_endpoints import agent_bp
    app.register_blueprint(agent_bp, url_prefix='/api/agent')
    AGENT_AVAILABLE = True
    logger.info("Agent blueprint registered at /api/agent")
except Exception as e:
    logger.warning(f"Agent blueprint unavailable: {e}")
    AGENT_AVAILABLE = False

try:
    from data_agent_endpoints import data_agent_bp
    app.register_blueprint(data_agent_bp, url_prefix='/api/data-agent')
    logger.info("Data Management blueprint registered at /api/data-agent")
except Exception as e:
    logger.warning(f"Data Management blueprint unavailable: {e}")

# PDF analysis function (moved from old agent_service)
def analyse_pdf_with_claude(base64_content, media_type, filename):
    from groq import Groq as _Groq
    _client = _Groq(api_key=os.getenv("GROQ_API_KEY"))
    response = _client.chat.completions.create(
        model="llama-3.3-70b-versatile", max_tokens=2048,
        messages=[{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": f"data:{media_type};base64,{base64_content}", "detail": "high"}},
            {"type": "text", "text": (
                "This is a contract or tax invoice document. "
                "Extract all key fields and return ONLY a JSON object (no markdown) with these fields "
                "(use null if not found): document_type, invoice_number, tax_number, account_number, po_number, "
                "invoice_date, due_date, supplier_name, supplier_address, supplier_vat, "
                "recipient_name, recipient_address, recipient_account, "
                "subtotal, vat_amount, total_amount, currency, "
                "line_items (array of {description, quantity, unit_price, total}), "
                "raw_text (full extracted text as a single string)"
            )}
        ]}],
    )
    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw_text": raw, "parse_error": True}


# ─── PDF Analysis with Claude Vision ──────────────────────────────────────────
@app.route('/api/pdf/analyse', methods=['POST'])
def pdf_analyse():
    """Accept a base64-encoded PDF page image and extract structured fields via Claude vision."""
    if not AGENT_AVAILABLE:
        return jsonify({'error': 'Agent service not configured. Check ANTHROPIC_API_KEY.'}), 503
    try:
        data = request.json
        pages = data.get('pages', [])  # list of {base64: str, media_type: str}
        filename = data.get('filename', 'document')

        if not pages:
            return jsonify({'error': 'No page data provided'}), 400

        results = []
        for page in pages:
            extracted = analyse_pdf_with_claude(
                page['base64'],
                page.get('media_type', 'image/png'),
                filename
            )
            results.append(extracted)

        # Merge multi-page results: combine raw_text, keep first page's structured fields
        merged = results[0] if results else {}
        if len(results) > 1:
            all_text = "\n\n--- Page Break ---\n\n".join(
                r.get('raw_text', '') for r in results if r.get('raw_text')
            )
            merged['raw_text'] = all_text
            # Merge line_items from all pages
            all_items = []
            for r in results:
                if r.get('line_items'):
                    all_items.extend(r['line_items'])
            if all_items:
                merged['line_items'] = all_items

        return jsonify({'success': True, 'filename': filename, 'extracted': merged})
    except Exception as e:
        logger.error(f"PDF analyse error: {e}")
        return jsonify({'error': str(e)}), 500


# ─── Register reconciliation blueprint ────────────────────────────────────────
try:
    from reconciliation_endpoints import reconciliation_bp
    app.register_blueprint(reconciliation_bp, url_prefix='/api/recon')
except Exception as e:
    logger.warning(f"Could not load reconciliation blueprint: {e}")

try:
    from ocr_endpoints import ocr_bp
    app.register_blueprint(ocr_bp, url_prefix='/api/ocr')
    logger.info("OCR blueprint registered at /api/ocr")
except Exception as e:
    logger.warning(f"OCR blueprint unavailable: {e}")

if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', 5001))
    app.run(debug=True, port=port)
