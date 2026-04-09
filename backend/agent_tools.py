"""
Agent tool functions for GPT-4o function calling.
Each tool returns a JSON-serialisable dict.
"""

import os
import json
import datetime
import fitz  # PyMuPDF
import psycopg2
import psycopg2.extras
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
)
MODEL = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'bss_tool'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
}

PROTECTED_TABLES = {'agent_audit_log', 'pending_writes'}

ALLOWED_TABLES = {
    'uat_cases', 'products', 'product_parameters',
    'transformation_activities', 'transformation_lob_progress',
    'phases', 'checklist', 'checklist_comments', 'checklist_attachments',
    'reconciliation_data', 'kpi_results', 'users',
}


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


def _serialise(obj):
    """Make query results JSON-safe."""
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, psycopg2.extras.RealDictRow):
        return dict(obj)
    return obj


def _rows_to_dicts(rows):
    if not rows:
        return []
    return [
        {k: _serialise(v) for k, v in dict(r).items()}
        for r in rows
    ]


# ─── 1. CRUD Tool ────────────────────────────────────────────────────────────

def crud_tool(table, operation, filters=None, data=None):
    """
    Handles CREATE, READ, UPDATE, DELETE (soft), BULK_UPDATE.
    Returns dict with result, before/after diff for writes, and generated SQL.
    """
    table = table.lower().strip()
    operation = operation.upper().strip()

    if table in PROTECTED_TABLES:
        return {'error': f'Writes to {table} are not permitted.'}
    if table not in ALLOWED_TABLES:
        return {'error': f'Unknown table: {table}. Allowed: {", ".join(sorted(ALLOWED_TABLES))}'}

    filters = filters or {}
    data = data or {}

    if operation == 'READ':
        return _crud_read(table, filters)
    elif operation == 'CREATE':
        return _crud_create(table, data)
    elif operation == 'UPDATE':
        return _crud_update(table, filters, data)
    elif operation == 'BULK_UPDATE':
        return _crud_bulk_update(table, filters, data)
    elif operation == 'DELETE':
        return _crud_soft_delete(table, filters)
    else:
        return {'error': f'Unknown operation: {operation}'}


def _build_where(filters):
    """Build WHERE clause from filters dict. Returns (clause_str, params_list)."""
    if not filters:
        return '', []
    parts = []
    params = []
    for k, v in filters.items():
        parts.append(f"{k} = %s")
        params.append(v)
    return ' WHERE ' + ' AND '.join(parts), params


def _crud_read(table, filters):
    where, params = _build_where(filters)
    sql = f"SELECT * FROM {table}{where} ORDER BY 1 LIMIT 100"
    rows = _query(sql, params)
    return {'rows': _rows_to_dicts(rows), 'count': len(rows), 'sql': sql}


def _crud_create(table, data):
    if not data:
        return {'error': 'No data provided for CREATE'}
    cols = list(data.keys())
    placeholders = ', '.join(['%s'] * len(cols))
    col_str = ', '.join(cols)
    params = [data[c] for c in cols]
    sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
    _query(sql, params, fetch=None)
    # Fetch inserted row
    after = _query(f"SELECT * FROM {table} ORDER BY 1 DESC LIMIT 1")
    return {
        'success': True,
        'operation': 'CREATE',
        'table': table,
        'before': [],
        'after': _rows_to_dicts(after),
        'sql': sql,
        'params': params,
        'description': f'Inserted 1 row into {table}',
    }


def _crud_update(table, filters, data):
    if not filters:
        return {'error': 'UPDATE requires filters (WHERE clause) to avoid updating all rows'}
    if not data:
        return {'error': 'No data provided for UPDATE'}

    where, where_params = _build_where(filters)
    before = _query(f"SELECT * FROM {table}{where}", where_params)

    set_parts = []
    set_params = []
    for k, v in data.items():
        set_parts.append(f"{k} = %s")
        set_params.append(v)
    set_str = ', '.join(set_parts)
    sql = f"UPDATE {table} SET {set_str}{where}"
    all_params = set_params + where_params
    _query(sql, all_params, fetch=None)

    after = _query(f"SELECT * FROM {table}{where}", where_params)
    return {
        'success': True,
        'operation': 'UPDATE',
        'table': table,
        'before': _rows_to_dicts(before),
        'after': _rows_to_dicts(after),
        'rows_affected': len(before) if before else 0,
        'sql': sql,
        'params': all_params,
        'description': f'Updated {len(before) if before else 0} row(s) in {table}',
    }


def _crud_bulk_update(table, filters, data):
    return _crud_update(table, filters, data)


def _crud_soft_delete(table, filters):
    if not filters:
        return {'error': 'DELETE requires filters to avoid deleting all rows'}

    where, where_params = _build_where(filters)
    before = _query(f"SELECT * FROM {table}{where}", where_params)
    sql = f"UPDATE {table} SET deleted_at = NOW(){where}"
    _query(sql, where_params, fetch=None)
    after = _query(f"SELECT * FROM {table}{where}", where_params)
    return {
        'success': True,
        'operation': 'SOFT_DELETE',
        'table': table,
        'before': _rows_to_dicts(before),
        'after': _rows_to_dicts(after),
        'rows_affected': len(before) if before else 0,
        'sql': sql,
        'params': where_params,
        'description': f'Soft-deleted {len(before) if before else 0} row(s) in {table}',
    }


# ─── 2. Reconciliation Tool ─────────────────────────────────────────────────

def reconciliation_tool(question):
    """
    Takes a natural-language question about reconciliation_data,
    generates a safe SQL query via GPT-4o, executes it, and returns results + explanation.
    """
    schema_hint = """Table: reconciliation_data
Columns: id SERIAL PK, account_link_code TEXT, service_code TEXT, service_name TEXT,
         cbs_status TEXT, clm_status TEXT, status TEXT, uploaded_at TIMESTAMP
Only generate SELECT queries. Never UPDATE/DELETE/DROP."""

    gen_response = client.chat.completions.create(
        model=MODEL,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": f"You are a SQL expert. Given a question about reconciliation data, generate a safe PostgreSQL SELECT query.\n\n{schema_hint}\n\nReturn JSON: {{\"sql\": \"...\", \"explanation\": \"...\"}}"},
            {"role": "user", "content": question},
        ],
        max_tokens=512,
        temperature=0,
    )
    parsed = json.loads(gen_response.choices[0].message.content.strip())
    sql = parsed.get('sql', '')

    # Safety: only allow SELECT
    if not sql.strip().upper().startswith('SELECT'):
        return {'error': 'Only SELECT queries are allowed for reconciliation.', 'generated_sql': sql}

    rows = _query(sql)
    return {
        'rows': _rows_to_dicts(rows),
        'count': len(rows) if rows else 0,
        'sql': sql,
        'explanation': parsed.get('explanation', ''),
    }


# ─── 3. OCR Tool ────────────────────────────────────────────────────────────

def ocr_tool(file_paths, question):
    """
    Extracts text from PDF files using PyMuPDF, then asks GPT-4o to answer
    the question based on the extracted content.
    """
    all_texts = []
    for fp in file_paths:
        if not os.path.isfile(fp):
            all_texts.append(f"[File not found: {fp}]")
            continue
        try:
            doc = fitz.open(fp)
            text = ""
            for page in doc:
                text += page.get_text()
            doc.close()
            all_texts.append(f"=== {os.path.basename(fp)} ===\n{text}")
        except Exception as e:
            all_texts.append(f"[Error reading {fp}: {e}]")

    combined = "\n\n".join(all_texts)

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "You are a document analysis expert. Analyse the extracted PDF text(s) and answer the user's question. If you find discrepancies, flag each one clearly with the document name and details. Return JSON: {\"answer\": \"...\", \"flagged_items\": [{\"file\": \"...\", \"issue\": \"...\", \"details\": \"...\"}]}"},
            {"role": "user", "content": f"Documents:\n{combined}\n\nQuestion: {question}"},
        ],
        response_format={"type": "json_object"},
        max_tokens=2048,
        temperature=0,
    )
    result = json.loads(response.choices[0].message.content.strip())
    return result


# ─── 4. Alerts Tool ─────────────────────────────────────────────────────────

def alerts_tool():
    """
    Scans all tables and returns risk alerts. No user prompt needed.
    """
    alerts = []
    today = datetime.date.today().isoformat()

    # 1. UAT completion below 50%
    try:
        uat_rows = _query("""
            SELECT lob,
                COUNT(*) as total,
                SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) as closed
            FROM uat_cases
            GROUP BY lob
        """)
        for r in (uat_rows or []):
            total = r['total']
            closed = r['closed']
            if total > 0 and (closed / total) < 0.5:
                pct = round(closed / total * 100)
                alerts.append({
                    'severity': 'high',
                    'module': 'UAT',
                    'description': f"UAT completion for {r['lob']} is only {pct}% ({closed}/{total} cases closed)",
                })
    except Exception:
        pass

    # 2. Phases past end_dt with status 'current'
    try:
        phase_rows = _query("""
            SELECT phase_id, end_dt, curr_status FROM phases
            WHERE curr_status = 'current' AND end_dt IS NOT NULL
        """)
        for r in (phase_rows or []):
            if r['end_dt'] and r['end_dt'] < today:
                alerts.append({
                    'severity': 'high',
                    'module': 'Milestones',
                    'description': f"Phase {r['phase_id']} is past its end date ({r['end_dt']}) but still in progress",
                })
    except Exception:
        pass

    # 3. Products with migration_flag=migrate but status != configured
    try:
        prod_rows = _query("""
            SELECT product_id, product_name, status FROM products
            WHERE migration_flag = 'migrate' AND status != 'configured' AND deleted_at IS NULL
        """)
        for r in (prod_rows or []):
            alerts.append({
                'severity': 'medium',
                'module': 'Products',
                'description': f"Product {r['product_name']} ({r['product_id']}) is flagged for migration but status is '{r['status']}'",
            })
    except Exception:
        pass

    # 4. Transformation activities where actual is >20 points behind planned
    try:
        ta_rows = _query("""
            SELECT lob, phase_name, planned, actual
            FROM transformation_activities
            WHERE planned > 0 AND (planned - actual) > 20
        """)
        for r in (ta_rows or []):
            gap = r['planned'] - r['actual']
            alerts.append({
                'severity': 'high' if gap > 40 else 'medium',
                'module': 'Transformation',
                'description': f"{r['lob']} — {r['phase_name']}: actual ({r['actual']}%) is {gap} points behind planned ({r['planned']}%)",
            })
    except Exception:
        pass

    return {'alerts': alerts, 'count': len(alerts)}


# ─── OpenAI Function Definitions ─────────────────────────────────────────────

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "crud_tool",
            "description": "Perform CREATE, READ, UPDATE, DELETE, or BULK_UPDATE operations on database tables. For DELETE, only soft deletes (setting deleted_at) are allowed.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "The database table to operate on",
                        "enum": list(ALLOWED_TABLES),
                    },
                    "operation": {
                        "type": "string",
                        "enum": ["CREATE", "READ", "UPDATE", "DELETE", "BULK_UPDATE"],
                        "description": "The operation to perform",
                    },
                    "filters": {
                        "type": "object",
                        "description": "WHERE clause conditions as key-value pairs (used for READ, UPDATE, DELETE, BULK_UPDATE)",
                    },
                    "data": {
                        "type": "object",
                        "description": "The data to write (used for CREATE, UPDATE, BULK_UPDATE). For UPDATE/BULK_UPDATE these are SET values.",
                    },
                },
                "required": ["table", "operation"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "reconciliation_tool",
            "description": "Answer natural language questions about reconciliation data by dynamically generating and executing safe SQL queries against reconciliation_data table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "A natural language question about reconciliation data",
                    },
                },
                "required": ["question"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ocr_tool",
            "description": "Extract text from PDF files and answer questions about them. Use for document analysis, invoice matching, discrepancy detection.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of absolute file paths to PDF files on the server",
                    },
                    "question": {
                        "type": "string",
                        "description": "A question to answer about the PDF contents",
                    },
                },
                "required": ["file_paths", "question"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "alerts_tool",
            "description": "Scan all database tables and return risk alerts. Checks: UAT completion <50%, overdue phases, unconfigured products flagged for migration, transformation activities behind schedule. No parameters needed.",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
]

# Map tool names to functions
TOOL_DISPATCH = {
    'crud_tool': lambda args: crud_tool(
        table=args.get('table', ''),
        operation=args.get('operation', 'READ'),
        filters=args.get('filters'),
        data=args.get('data'),
    ),
    'reconciliation_tool': lambda args: reconciliation_tool(
        question=args.get('question', ''),
    ),
    'ocr_tool': lambda args: ocr_tool(
        file_paths=args.get('file_paths', []),
        question=args.get('question', ''),
    ),
    'alerts_tool': lambda args: alerts_tool(),
}
