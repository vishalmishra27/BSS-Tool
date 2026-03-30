"""
Data Management — Flask Blueprint endpoints.
Chatbot-driven via GPT-4o function calling. Handles file upload, sanitization, DB upload, table listing, alter/drop.
"""

import os
import csv
import uuid
import json
import logging
import datetime
import pandas as pd
import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify, send_file
from werkzeug.utils import secure_filename
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

data_agent_bp = Blueprint('data_agent', __name__)

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads', 'data_agent')
os.makedirs(UPLOAD_DIR, exist_ok=True)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'bss_tool'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
}

# In-memory file registry
_file_registry = {}


def _get_db():
    return psycopg2.connect(**DB_CONFIG)


def _query(sql, params=None, fetch='all'):
    conn = _get_db()
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
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─── List all tables ──────────────────────────────────────────────────────────
@data_agent_bp.route('/tables')
def list_tables():
    try:
        rows = _query("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
        """)
        tables = []
        for r in rows:
            count_row = _query(
                f"SELECT COUNT(*) as cnt FROM \"{r['table_schema']}\".\"{r['table_name']}\"",
                fetch='one'
            )
            tables.append({
                'schema': r['table_schema'],
                'table': r['table_name'],
                'row_count': count_row['cnt'] if count_row else 0,
            })
        return jsonify(tables)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Get table schema (columns) ──────────────────────────────────────────────
@data_agent_bp.route('/tables/<table_name>/schema')
def get_table_schema(table_name):
    schema_name = request.args.get('schema', 'public')
    try:
        cols = _query("""
            SELECT column_name, data_type, is_nullable, column_default, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_name, table_name))

        # Get primary key info
        pks = _query("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary AND c.relname = %s AND n.nspname = %s
        """, (table_name, schema_name))
        pk_cols = [p['attname'] for p in pks]

        # Sample rows
        sample = _query(
            f"SELECT * FROM \"{schema_name}\".\"{table_name}\" LIMIT 5"
        )

        # Row count
        count_row = _query(
            f"SELECT COUNT(*) as cnt FROM \"{schema_name}\".\"{table_name}\"",
            fetch='one'
        )

        return jsonify({
            'table': table_name,
            'schema': schema_name,
            'columns': [dict(c) for c in cols],
            'primary_keys': pk_cols,
            'sample_rows': [dict(r) for r in sample] if sample else [],
            'row_count': count_row['cnt'] if count_row else 0,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Upload file ──────────────────────────────────────────────────────────────
@data_agent_bp.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    f = request.files['file']
    if not f.filename:
        return jsonify({'error': 'Empty filename'}), 400

    safe_name = secure_filename(f.filename)
    file_id = str(uuid.uuid4())[:8]
    file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{safe_name}")
    f.save(file_path)

    delimiter = '|' if f.filename.lower().endswith('.psv') else ','

    try:
        df_sample = pd.read_csv(file_path, sep=delimiter, nrows=5, dtype=str)
        headers = list(df_sample.columns)
        sample_rows = df_sample.values.tolist()

        with open(file_path, 'r') as fp:
            row_count = sum(1 for _ in fp) - 1

        file_size = os.path.getsize(file_path)
        est_sec = max(1, int(file_size / (500 * 1024)))

        _file_registry[file_id] = {
            'file_id': file_id,
            'file_path': file_path,
            'original_name': f.filename,
            'delimiter': delimiter,
            'headers': headers,
            'row_count': row_count,
            'file_size': file_size,
            'sanitized': False,
        }

        return jsonify({
            'file_id': file_id,
            'original_name': f.filename,
            'headers': headers,
            'sample_rows': sample_rows,
            'row_count': row_count,
            'file_size_human': f"{file_size/1024:.1f} KB" if file_size < 1024*1024 else f"{file_size/(1024*1024):.2f} MB",
            'estimated_time': f"~{est_sec}s" if est_sec < 60 else f"~{est_sec//60}m {est_sec%60}s",
            'num_columns': len(headers),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Sanitize file ────────────────────────────────────────────────────────────
@data_agent_bp.route('/sanitize', methods=['POST'])
def sanitize_file():
    data = request.json or {}
    file_id = data.get('file_id')
    rules = data.get('rules', {})

    info = _file_registry.get(file_id)
    if not info:
        return jsonify({'error': 'File not found. Upload first.'}), 404

    try:
        df = pd.read_csv(info['file_path'], sep=info['delimiter'], dtype=str)
        rows_before = len(df)
        changes = []

        if rules.get('trim_whitespace', True):
            df = df.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)
            changes.append('Trimmed whitespace')

        df.replace('', pd.NA, inplace=True)

        if rules.get('drop_all_null', True):
            before = len(df)
            df.dropna(how='all', inplace=True)
            d = before - len(df)
            if d: changes.append(f'Dropped {d} empty rows')

        if rules.get('remove_duplicates', True):
            before = len(df)
            df.drop_duplicates(inplace=True)
            d = before - len(df)
            if d: changes.append(f'Removed {d} duplicates')

        if rules.get('standardize_columns', True):
            df.columns = [c.lower().strip().replace(' ', '_').replace('-', '_') for c in df.columns]
            changes.append('Standardized column names')

        rows_after = len(df)
        cleaned_path = info['file_path'].rsplit('.', 1)[0] + '_cleaned.csv'
        df.to_csv(cleaned_path, index=False)

        info['file_path'] = cleaned_path
        info['headers'] = list(df.columns)
        info['row_count'] = rows_after
        info['sanitized'] = True
        info['delimiter'] = ','  # cleaned file is always CSV

        return jsonify({
            'file_id': file_id,
            'rows_before': rows_before,
            'rows_after': rows_after,
            'rows_removed': rows_before - rows_after,
            'changes': changes,
            'new_headers': list(df.columns),
            'sample_rows': df.head(5).values.tolist(),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Upload to DB ─────────────────────────────────────────────────────────────
@data_agent_bp.route('/upload-to-db', methods=['POST'])
def upload_to_db():
    data = request.json or {}
    file_id = data.get('file_id')
    table_name = data.get('table_name', '').lower().strip()
    if_exists = data.get('if_exists', 'fail')  # fail | append | replace

    info = _file_registry.get(file_id)
    if not info:
        return jsonify({'error': 'File not found.'}), 404
    if not table_name:
        return jsonify({'error': 'table_name required.'}), 400

    try:
        # Check if table exists
        exists = _query(
            "SELECT 1 FROM information_schema.tables WHERE table_name=%s AND table_schema='public'",
            (table_name,), fetch='one'
        )

        if exists and if_exists == 'fail':
            cols = _query("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name=%s AND table_schema='public' ORDER BY ordinal_position
            """, (table_name,))
            return jsonify({
                'status': 'table_exists',
                'existing_columns': [dict(c) for c in cols],
                'message': f"Table '{table_name}' exists. Use if_exists='append' or 'replace'.",
            })

        df = pd.read_csv(info['file_path'], sep=info['delimiter'], dtype=str)

        conn = _get_db()
        cur = conn.cursor()

        if exists and if_exists == 'replace':
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')

        if not exists or if_exists == 'replace':
            col_defs = ', '.join(f'"{c}" TEXT' for c in df.columns)
            cur.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

        # COPY from CSV
        with open(info['file_path'], 'r') as fp:
            cur.copy_expert(
                f"""COPY "{table_name}" FROM STDIN WITH CSV HEADER DELIMITER '{info['delimiter']}'""",
                fp
            )

        conn.commit()
        rows_inserted = len(df)
        cur.close()
        conn.close()

        return jsonify({
            'success': True,
            'table_name': table_name,
            'rows_inserted': rows_inserted,
            'columns': list(df.columns),
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Preview table data ──────────────────────────────────────────────────────
@data_agent_bp.route('/tables/<table_name>/data')
def get_table_data(table_name):
    schema_name = request.args.get('schema', 'public')
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    try:
        rows = _query(
            f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT %s OFFSET %s',
            (limit, offset)
        )
        count_row = _query(
            f'SELECT COUNT(*) as cnt FROM "{schema_name}"."{table_name}"',
            fetch='one'
        )
        return jsonify({
            'rows': [dict(r) for r in rows] if rows else [],
            'total': count_row['cnt'] if count_row else 0,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Drop table ───────────────────────────────────────────────────────────────
@data_agent_bp.route('/tables/<table_name>/drop', methods=['POST'])
def drop_table(table_name):
    try:
        _query(f'DROP TABLE IF EXISTS "{table_name}" CASCADE', fetch=None)
        return jsonify({'success': True, 'message': f"Table '{table_name}' dropped."})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Alter table ──────────────────────────────────────────────────────────────
@data_agent_bp.route('/tables/<table_name>/alter', methods=['POST'])
def alter_table(table_name):
    data = request.json or {}
    action = data.get('action')
    try:
        if action == 'add_column':
            col = data['column_name']
            dtype = data.get('data_type', 'TEXT')
            _query(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {dtype}', fetch=None)
            return jsonify({'success': True, 'message': f"Added column '{col}' ({dtype}) to '{table_name}'."})
        elif action == 'drop_column':
            col = data['column_name']
            _query(f'ALTER TABLE "{table_name}" DROP COLUMN "{col}"', fetch=None)
            return jsonify({'success': True, 'message': f"Dropped column '{col}' from '{table_name}'."})
        elif action == 'rename_column':
            _query(f'ALTER TABLE "{table_name}" RENAME COLUMN "{data["old_name"]}" TO "{data["new_name"]}"', fetch=None)
            return jsonify({'success': True, 'message': f"Renamed '{data['old_name']}' to '{data['new_name']}' in '{table_name}'."})
        elif action == 'rename_table':
            _query(f'ALTER TABLE "{table_name}" RENAME TO "{data["new_name"]}"', fetch=None)
            return jsonify({'success': True, 'message': f"Renamed '{table_name}' to '{data['new_name']}'."})
        else:
            return jsonify({'error': f"Unknown action: {action}"}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Execute raw SQL ──────────────────────────────────────────────────────────
@data_agent_bp.route('/execute-sql', methods=['POST'])
def execute_sql():
    data = request.json or {}
    sql = data.get('sql', '').strip()
    if not sql:
        return jsonify({'error': 'No SQL provided'}), 400
    try:
        rows = _query(sql)
        if rows is None:
            return jsonify({'success': True, 'message': 'Query executed.', 'rows': []})
        return jsonify({'success': True, 'rows': [dict(r) for r in rows][:100], 'row_count': len(rows)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── GPT-4o Chat endpoint ────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

_client_instance = None
def _get_groq():
    global _client_instance
    if not _client_instance:
        _client_instance = Groq(api_key=os.getenv("GROQ_API_KEY"))
    return _client_instance

GROQ_MODEL = "llama-3.3-70b-versatile"

TOOL_DEFS = [
    {"type": "function", "function": {
        "name": "list_tables",
        "description": "List all tables in the database with row counts",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }},
    {"type": "function", "function": {
        "name": "show_schema",
        "description": "Show column definitions, types, primary keys, and sample data for a specific table",
        "parameters": {"type": "object", "properties": {
            "table_name": {"type": "string"},
        }, "required": ["table_name"]},
    }},
    {"type": "function", "function": {
        "name": "sanitize_file",
        "description": "Sanitize the currently uploaded file: trim whitespace, remove nulls/duplicates, standardize column names",
        "parameters": {"type": "object", "properties": {
            "file_id": {"type": "string"},
            "trim_whitespace": {"type": "boolean", "default": True},
            "remove_duplicates": {"type": "boolean", "default": True},
            "standardize_columns": {"type": "boolean", "default": True},
        }, "required": ["file_id"]},
    }},
    {"type": "function", "function": {
        "name": "upload_to_database",
        "description": "Upload the currently uploaded (and optionally sanitized) file into a PostgreSQL table. Creates the table if it doesn't exist.",
        "parameters": {"type": "object", "properties": {
            "file_id": {"type": "string"},
            "table_name": {"type": "string", "description": "Target table name"},
            "if_exists": {"type": "string", "enum": ["fail", "append", "replace"], "default": "fail"},
        }, "required": ["file_id", "table_name"]},
    }},
    {"type": "function", "function": {
        "name": "drop_table",
        "description": "Drop/delete a table from the database",
        "parameters": {"type": "object", "properties": {
            "table_name": {"type": "string"},
        }, "required": ["table_name"]},
    }},
    {"type": "function", "function": {
        "name": "alter_table",
        "description": "Alter a table: add column, drop column, rename column, or rename table",
        "parameters": {"type": "object", "properties": {
            "table_name": {"type": "string"},
            "action": {"type": "string", "enum": ["add_column", "drop_column", "rename_column", "rename_table"]},
            "column_name": {"type": "string", "description": "Column name (for add/drop)"},
            "data_type": {"type": "string", "default": "TEXT"},
            "old_name": {"type": "string", "description": "Old column name (for rename)"},
            "new_name": {"type": "string", "description": "New name (for rename)"},
        }, "required": ["table_name", "action"]},
    }},
    {"type": "function", "function": {
        "name": "run_sql",
        "description": "Execute a SQL query against the database. For SELECT queries, aggregations, comparisons, or any custom SQL.",
        "parameters": {"type": "object", "properties": {
            "sql": {"type": "string", "description": "The SQL query to execute"},
        }, "required": ["sql"]},
    }},
]


def _exec_tool(fn_name, args, file_id_ctx):
    """Execute a tool and return result dict."""
    try:
        if fn_name == 'list_tables':
            rows = _query("""SELECT table_schema, table_name FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY table_name""")
            tables = []
            for r in rows:
                cnt = _query(f"SELECT COUNT(*) as c FROM \"{r['table_schema']}\".\"{r['table_name']}\"", fetch='one')
                tables.append({'schema': r['table_schema'], 'table': r['table_name'], 'rows': cnt['c'] if cnt else 0})
            return {'tables': tables}

        elif fn_name == 'show_schema':
            tbl = args['table_name']
            cols = _query("""SELECT column_name, data_type, is_nullable FROM information_schema.columns
                WHERE table_name=%s AND table_schema='public' ORDER BY ordinal_position""", (tbl,))
            sample = _query(f'SELECT * FROM "{tbl}" LIMIT 3')
            cnt = _query(f"SELECT COUNT(*) as c FROM \"{tbl}\"", fetch='one')
            return {'table': tbl, 'columns': [dict(c) for c in cols], 'sample': [dict(r) for r in sample] if sample else [], 'row_count': cnt['c'] if cnt else 0}

        elif fn_name == 'sanitize_file':
            fid = args.get('file_id') or file_id_ctx
            info = _file_registry.get(fid)
            if not info:
                return {'error': 'No file uploaded yet. Please upload a file first.'}
            df = pd.read_csv(info['file_path'], sep=info['delimiter'], dtype=str)
            rows_before = len(df)
            changes = []
            if args.get('trim_whitespace', True):
                df = df.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)
                changes.append('Trimmed whitespace')
            df.replace('', pd.NA, inplace=True)
            b = len(df); df.dropna(how='all', inplace=True); d = b - len(df)
            if d: changes.append(f'Dropped {d} empty rows')
            if args.get('remove_duplicates', True):
                b = len(df); df.drop_duplicates(inplace=True); d = b - len(df)
                if d: changes.append(f'Removed {d} duplicates')
            if args.get('standardize_columns', True):
                df.columns = [c.lower().strip().replace(' ', '_').replace('-', '_') for c in df.columns]
                changes.append('Standardized column names')
            cleaned_path = info['file_path'].rsplit('.', 1)[0] + '_cleaned.csv'
            df.to_csv(cleaned_path, index=False)
            info['file_path'] = cleaned_path
            info['headers'] = list(df.columns)
            info['row_count'] = len(df)
            info['sanitized'] = True
            info['delimiter'] = ','
            return {'rows_before': rows_before, 'rows_after': len(df), 'removed': rows_before - len(df), 'changes': changes, 'headers': list(df.columns)}

        elif fn_name == 'upload_to_database':
            fid = args.get('file_id') or file_id_ctx
            info = _file_registry.get(fid)
            if not info:
                return {'error': 'No file uploaded yet. Please upload a file first.'}
            tbl = args['table_name'].lower().strip()
            ie = args.get('if_exists', 'fail')
            exists = _query("SELECT 1 FROM information_schema.tables WHERE table_name=%s AND table_schema='public'", (tbl,), fetch='one')
            if exists and ie == 'fail':
                cols = _query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name=%s AND table_schema='public' ORDER BY ordinal_position", (tbl,))
                return {'status': 'table_exists', 'table': tbl, 'existing_columns': [dict(c) for c in cols], 'message': f"Table '{tbl}' already exists. Say 'replace it' or 'append to it'."}
            df = pd.read_csv(info['file_path'], sep=info['delimiter'], dtype=str)
            conn = _get_db(); cur = conn.cursor()
            if exists and ie == 'replace':
                cur.execute(f'DROP TABLE IF EXISTS "{tbl}" CASCADE')
            if not exists or ie == 'replace':
                col_defs = ', '.join(f'"{c}" TEXT' for c in df.columns)
                cur.execute(f'CREATE TABLE "{tbl}" ({col_defs})')
            with open(info['file_path'], 'r') as fp:
                cur.copy_expert(f"""COPY "{tbl}" FROM STDIN WITH CSV HEADER DELIMITER '{info['delimiter']}'""", fp)
            conn.commit(); cur.close(); conn.close()
            return {'success': True, 'table': tbl, 'rows_inserted': len(df), 'columns': list(df.columns)}

        elif fn_name == 'drop_table':
            tbl = args['table_name']
            _query(f'DROP TABLE IF EXISTS "{tbl}" CASCADE', fetch=None)
            return {'success': True, 'message': f"Dropped table '{tbl}'."}

        elif fn_name == 'alter_table':
            tbl = args['table_name']
            act = args['action']
            if act == 'add_column':
                _query(f'ALTER TABLE "{tbl}" ADD COLUMN "{args["column_name"]}" {args.get("data_type","TEXT")}', fetch=None)
                return {'success': True, 'message': f"Added column '{args['column_name']}' to '{tbl}'."}
            elif act == 'drop_column':
                _query(f'ALTER TABLE "{tbl}" DROP COLUMN "{args["column_name"]}"', fetch=None)
                return {'success': True, 'message': f"Dropped column '{args['column_name']}' from '{tbl}'."}
            elif act == 'rename_column':
                _query(f'ALTER TABLE "{tbl}" RENAME COLUMN "{args["old_name"]}" TO "{args["new_name"]}"', fetch=None)
                return {'success': True, 'message': f"Renamed '{args['old_name']}' to '{args['new_name']}' in '{tbl}'."}
            elif act == 'rename_table':
                _query(f'ALTER TABLE "{tbl}" RENAME TO "{args["new_name"]}"', fetch=None)
                return {'success': True, 'message': f"Renamed '{tbl}' to '{args['new_name']}'."}
            return {'error': f"Unknown action: {act}"}

        elif fn_name == 'run_sql':
            rows = _query(args['sql'].strip())
            if rows is None:
                return {'success': True, 'message': 'Executed.', 'rows': []}
            result = [dict(r) for r in rows][:100]
            return {'success': True, 'rows': result, 'row_count': len(rows)}

        return {'error': f'Unknown tool: {fn_name}'}
    except Exception as e:
        return {'error': str(e)}


@data_agent_bp.route('/chat', methods=['POST'])
def chat():
    import datetime as dt
    data = request.json or {}
    message = data.get('message', '')
    history = data.get('history', [])
    file_id = data.get('file_id')

    if not message:
        return jsonify({'error': 'No message'}), 400

    file_ctx = "No file uploaded."
    if file_id:
        info = _file_registry.get(file_id)
        if info:
            file_ctx = f"Uploaded file: {info.get('original_name')} | file_id: {file_id} | {info['row_count']} rows | headers: {info['headers']} | sanitized: {info['sanitized']}"

    try:
        tbl_rows = _query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
        tbl_list = ', '.join(r['table_name'] for r in tbl_rows)
    except:
        tbl_list = '(could not fetch)'

    system_prompt = f"""You are a Data Management assistant for a BSS Migration tool with PostgreSQL.
You help users manage database tables and uploaded CSV/PSV files through conversation.

AVAILABLE TABLES: {tbl_list}
FILE STATUS: {file_ctx}
TODAY: {dt.date.today().isoformat()}

You can: list tables, show schema, sanitize files, upload to DB, drop tables, alter tables, run SQL queries.

RULES:
- Use the tools — don't just describe, actually call the tool
- When user says "the file" or "uploaded file", use file_id: {file_id or 'none'}
- Be concise. Show results clearly.
"""

    messages = [{"role": "system", "content": system_prompt}]
    for msg in history:
        messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
    messages.append({"role": "user", "content": message})

    try:
        groq = _get_groq()
        response = groq.chat.completions.create(model=GROQ_MODEL, messages=messages, tools=TOOL_DEFS, tool_choice="auto", max_tokens=1024, temperature=0)
    except Exception as e:
        return jsonify({'reply': f'AI error: {str(e)}', 'tool_used': None})

    choice = response.choices[0]
    tool_used = None
    tool_result_data = None

    if choice.message.tool_calls:
        messages.append(choice.message)
        for tc in choice.message.tool_calls:
            fn_name = tc.function.name
            tool_used = fn_name
            try:
                args = json.loads(tc.function.arguments)
            except:
                args = {}
            result = _exec_tool(fn_name, args, file_id)
            tool_result_data = result
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": json.dumps(result, default=str)})

        try:
            final = groq.chat.completions.create(model=GROQ_MODEL, messages=messages, max_tokens=1024, temperature=0)
            reply = final.choices[0].message.content or ''
        except Exception as e:
            reply = f'Tool ran but reply failed: {str(e)}'
    else:
        reply = choice.message.content or ''

    resp = {'reply': reply, 'tool_used': tool_used}
    if tool_result_data:
        if 'tables' in tool_result_data:
            resp['tables_refreshed'] = True
        if 'rows' in tool_result_data and tool_result_data['rows']:
            resp['query_result'] = tool_result_data['rows'][:50]
        if tool_result_data.get('success') and tool_used in ('upload_to_database', 'drop_table', 'alter_table'):
            resp['tables_refreshed'] = True
        if 'headers' in tool_result_data:
            resp['sanitize_result'] = tool_result_data
    return jsonify(resp)
