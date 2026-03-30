"""
Data Management Agent — Tool functions for GPT-4o function calling.
Handles: file preview, sanitization, upload to DB, table listing, comparison/analysis.
"""

import os
import csv
import uuid
import datetime
import json
import psycopg2
import psycopg2.extras
import pandas as pd
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))
MODEL = "llama-3.3-70b-versatile"

UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads', 'data_agent')
os.makedirs(UPLOAD_DIR, exist_ok=True)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'bss_tool'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
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


# ─── File metadata store (in-memory, keyed by file_id) ───────────────────────
_file_registry = {}


def register_file(file_path, original_name, delimiter=','):
    """Read file metadata and register it. Returns file_id + metadata."""
    file_id = str(uuid.uuid4())[:8]
    file_size = os.path.getsize(file_path)

    # Detect delimiter from extension if not specified
    if original_name.lower().endswith('.psv'):
        delimiter = '|'

    # Read headers + sample rows + row count
    df_sample = pd.read_csv(file_path, sep=delimiter, nrows=5, dtype=str)
    headers = list(df_sample.columns)
    sample_rows = df_sample.values.tolist()

    # Count total rows (fast line count)
    with open(file_path, 'r') as f:
        row_count = sum(1 for _ in f) - 1  # subtract header

    # Estimate upload time (~500KB/sec for DB insert)
    estimated_seconds = max(1, int(file_size / (500 * 1024)))

    _file_registry[file_id] = {
        'file_id': file_id,
        'file_path': file_path,
        'original_name': original_name,
        'delimiter': delimiter,
        'headers': headers,
        'sample_rows': sample_rows,
        'row_count': row_count,
        'file_size': file_size,
        'file_size_human': f"{file_size / 1024:.1f} KB" if file_size < 1024 * 1024 else f"{file_size / (1024*1024):.2f} MB",
        'estimated_time': f"~{estimated_seconds}s" if estimated_seconds < 60 else f"~{estimated_seconds // 60}m {estimated_seconds % 60}s",
        'sanitized': False,
    }
    return _file_registry[file_id]


def get_file_info(file_id):
    return _file_registry.get(file_id)


# ─── Tool: list_tables ───────────────────────────────────────────────────────
def list_tables_tool(args=None):
    """List all tables in the DB with their schemas (column names + types)."""
    try:
        rows = _query("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
        """)
        tables = []
        for r in rows:
            cols = _query("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (r['table_schema'], r['table_name']))
            tables.append({
                'schema': r['table_schema'],
                'table': r['table_name'],
                'columns': [dict(c) for c in cols],
            })
        return {'tables': tables, 'count': len(tables)}
    except Exception as e:
        return {'error': str(e)}


# ─── Tool: preview_file ──────────────────────────────────────────────────────
def preview_file_tool(args):
    """Return headers, sample rows, stats for an uploaded file."""
    file_id = args.get('file_id')
    info = get_file_info(file_id)
    if not info:
        return {'error': f'File {file_id} not found. Upload a file first.'}
    return {
        'file_id': info['file_id'],
        'original_name': info['original_name'],
        'headers': info['headers'],
        'sample_rows': info['sample_rows'][:5],
        'row_count': info['row_count'],
        'file_size': info['file_size_human'],
        'estimated_upload_time': info['estimated_time'],
        'num_columns': len(info['headers']),
    }


# ─── Tool: sanitize_file ─────────────────────────────────────────────────────
def sanitize_file_tool(args):
    """Clean the uploaded file: trim whitespace, drop null rows, remove dupes, type casting."""
    file_id = args.get('file_id')
    rules = args.get('rules', {})
    info = get_file_info(file_id)
    if not info:
        return {'error': f'File {file_id} not found.'}

    try:
        df = pd.read_csv(info['file_path'], sep=info['delimiter'], dtype=str)
        rows_before = len(df)
        changes = []

        # Trim whitespace
        if rules.get('trim_whitespace', True):
            df = df.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)
            changes.append('Trimmed whitespace from all text columns')

        # Replace empty strings with NaN
        df.replace('', pd.NA, inplace=True)

        # Drop rows where ALL values are null
        if rules.get('drop_all_null', True):
            before_drop = len(df)
            df.dropna(how='all', inplace=True)
            dropped = before_drop - len(df)
            if dropped:
                changes.append(f'Dropped {dropped} completely empty rows')

        # Drop rows where key columns are null
        drop_if_null = rules.get('drop_if_null_columns', [])
        if drop_if_null:
            valid_cols = [c for c in drop_if_null if c in df.columns]
            if valid_cols:
                before_drop = len(df)
                df.dropna(subset=valid_cols, inplace=True)
                dropped = before_drop - len(df)
                if dropped:
                    changes.append(f'Dropped {dropped} rows with null values in {valid_cols}')

        # Remove duplicates
        if rules.get('remove_duplicates', True):
            before_dedup = len(df)
            df.drop_duplicates(inplace=True)
            deduped = before_dedup - len(df)
            if deduped:
                changes.append(f'Removed {deduped} duplicate rows')

        # Standardize column names (lowercase, replace spaces with underscores)
        if rules.get('standardize_columns', True):
            df.columns = [c.lower().strip().replace(' ', '_').replace('-', '_') for c in df.columns]
            changes.append('Standardized column names to lowercase/underscores')

        rows_after = len(df)

        # Save cleaned file
        cleaned_path = info['file_path'].replace('.csv', '_cleaned.csv').replace('.psv', '_cleaned.csv')
        df.to_csv(cleaned_path, index=False)

        # Update registry
        info['file_path'] = cleaned_path
        info['headers'] = list(df.columns)
        info['sample_rows'] = df.head(5).values.tolist()
        info['row_count'] = rows_after
        info['sanitized'] = True

        return {
            'file_id': file_id,
            'rows_before': rows_before,
            'rows_after': rows_after,
            'rows_removed': rows_before - rows_after,
            'changes': changes,
            'new_headers': list(df.columns),
            'sample_rows': df.head(3).values.tolist(),
        }
    except Exception as e:
        return {'error': str(e)}


# ─── Tool: upload_to_db ──────────────────────────────────────────────────────
def upload_to_db_tool(args):
    """
    Upload CSV to a PostgreSQL table.
    If table exists, returns existing schema for user to confirm.
    Returns SQL for CREATE TABLE + row count — goes through pending approval.
    """
    file_id = args.get('file_id')
    table_name = args.get('table_name', '').lower().strip()
    if_exists = args.get('if_exists', 'fail')  # fail | append | replace

    info = get_file_info(file_id)
    if not info:
        return {'error': f'File {file_id} not found.'}
    if not table_name:
        return {'error': 'table_name is required.'}

    try:
        # Check if table exists
        exists = _query(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s AND table_schema = 'public'",
            (table_name,), fetch='one'
        )

        if exists and if_exists == 'fail':
            # Return existing schema so user can decide
            cols = _query("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public' ORDER BY ordinal_position
            """, (table_name,))
            return {
                'status': 'table_exists',
                'table_name': table_name,
                'existing_columns': [dict(c) for c in cols],
                'file_headers': info['headers'],
                'message': f"Table '{table_name}' already exists. Set if_exists to 'append' to add rows or 'replace' to drop and recreate.",
            }

        # Infer column types from file
        df = pd.read_csv(info['file_path'], sep=info.get('delimiter', ','), dtype=str, nrows=100)
        col_types = _infer_pg_types(df)

        # Build CREATE TABLE SQL
        if exists and if_exists == 'replace':
            create_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;\n"
        else:
            create_sql = ""

        if not exists or if_exists == 'replace':
            col_defs = ', '.join(f'"{col}" {dtype}' for col, dtype in col_types.items())
            create_sql += f"CREATE TABLE {table_name} ({col_defs});"

        # Build COPY command description
        copy_desc = f"COPY {info['row_count']} rows from '{info['original_name']}' into '{table_name}'"

        return {
            'status': 'ready_to_upload',
            'table_name': table_name,
            'create_sql': create_sql,
            'columns': col_types,
            'row_count': info['row_count'],
            'file_id': file_id,
            'estimated_time': info['estimated_time'],
            'description': copy_desc,
            'requires_approval': True,
        }
    except Exception as e:
        return {'error': str(e)}


def execute_upload(file_id, table_name, create_sql=None):
    """Actually execute the upload after approval."""
    info = get_file_info(file_id)
    if not info:
        return {'error': f'File {file_id} not found.'}

    conn = _get_db()
    try:
        cur = conn.cursor()

        # Run CREATE TABLE if needed
        if create_sql:
            cur.execute(create_sql)

        # COPY data from CSV
        with open(info['file_path'], 'r') as f:
            cur.copy_expert(
                f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','",
                f
            )

        conn.commit()
        row_count = cur.rowcount
        return {'success': True, 'rows_inserted': row_count, 'table_name': table_name}
    except Exception as e:
        conn.rollback()
        return {'error': str(e)}
    finally:
        conn.close()


# ─── Tool: compare_datasets ──────────────────────────────────────────────────
def compare_tool(args):
    """
    Generate a SQL comparison query based on user's logic.
    Uses GPT-4o to generate SQL with GROUP BY (not full dataset).
    Returns SQL for approval.
    """
    user_logic = args.get('query', args.get('description', ''))
    table1 = args.get('table1', '')
    table2 = args.get('table2', '')

    if not user_logic:
        return {'error': 'Please describe the comparison logic.'}

    # Get schemas of involved tables
    schema_info = []
    for tbl in [table1, table2]:
        if tbl:
            cols = _query("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public' ORDER BY ordinal_position
            """, (tbl,))
            row_count = _query(f"SELECT COUNT(*) as cnt FROM {tbl}", fetch='one')
            schema_info.append({
                'table': tbl,
                'columns': [dict(c) for c in cols],
                'row_count': row_count['cnt'] if row_count else 0,
            })

    # Also get list of all tables if no specific tables mentioned
    if not table1 and not table2:
        all_tables = _query("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' ORDER BY table_name
        """)
        schema_info = [{'available_tables': [r['table_name'] for r in all_tables]}]

    # Ask GPT-4o to generate SQL
    try:
        sql_response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": f"""You are a PostgreSQL expert. Generate a SQL query based on the user's comparison/analysis request.
Available schema: {json.dumps(schema_info, default=str)}

RULES:
- Use GROUP BY for aggregations to minimize result size
- LIMIT results to 100 rows max
- Use parameterized-safe SQL (no user input interpolation — table/column names are from schema)
- Return ONLY the SQL query, nothing else
- Make it readable with line breaks
"""},
                {"role": "user", "content": user_logic},
            ],
            max_tokens=512,
            temperature=0,
        )
        generated_sql = sql_response.choices[0].message.content.strip()
        # Clean markdown code blocks if present
        if generated_sql.startswith('```'):
            generated_sql = '\n'.join(generated_sql.split('\n')[1:-1])

        return {
            'status': 'pending_approval',
            'sql': generated_sql,
            'description': f"Compare/analyze: {user_logic[:100]}",
            'tables_involved': [t.get('table', '') for t in schema_info if 'table' in t],
            'requires_approval': True,
        }
    except Exception as e:
        return {'error': f'Error generating SQL: {str(e)}'}


def execute_comparison_sql(sql):
    """Execute approved comparison SQL and return results."""
    try:
        rows = _query(sql)
        result = [dict(r) for r in rows] if rows else []
        return {
            'success': True,
            'rows': result[:100],
            'row_count': len(result),
        }
    except Exception as e:
        return {'error': str(e)}


# ─── Helper: infer PostgreSQL types from pandas DataFrame ────────────────────
def _infer_pg_types(df):
    """Map pandas dtypes to PostgreSQL types."""
    type_map = {}
    for col in df.columns:
        clean_col = col.lower().strip().replace(' ', '_').replace('-', '_')
        sample = df[col].dropna()
        if len(sample) == 0:
            type_map[clean_col] = 'TEXT'
            continue

        # Try integer
        try:
            sample.astype(int)
            type_map[clean_col] = 'INTEGER'
            continue
        except (ValueError, TypeError):
            pass

        # Try float
        try:
            sample.astype(float)
            type_map[clean_col] = 'NUMERIC'
            continue
        except (ValueError, TypeError):
            pass

        # Try date
        try:
            pd.to_datetime(sample, format='mixed')
            type_map[clean_col] = 'TIMESTAMP'
            continue
        except (ValueError, TypeError):
            pass

        # Default text
        max_len = sample.str.len().max() if hasattr(sample, 'str') else 255
        if max_len and max_len > 500:
            type_map[clean_col] = 'TEXT'
        else:
            type_map[clean_col] = 'TEXT'

    return type_map


# ─── Tool definitions for OpenAI function calling ────────────────────────────
DATA_TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "List all tables in the PostgreSQL database with their schemas (column names, types). Use this when the user wants to see available tables or choose where to upload data.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        }
    },
    {
        "type": "function",
        "function": {
            "name": "preview_file",
            "description": "Preview an uploaded file — shows headers, sample rows, row count, file size, and estimated upload time.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_id": {"type": "string", "description": "The file_id returned from upload"},
                },
                "required": ["file_id"],
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "sanitize_file",
            "description": "Clean/sanitize an uploaded file: trim whitespace, remove empty rows, deduplicate, standardize column names. Call this before uploading to DB.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_id": {"type": "string", "description": "The file_id to sanitize"},
                    "rules": {
                        "type": "object",
                        "description": "Sanitization rules",
                        "properties": {
                            "trim_whitespace": {"type": "boolean", "description": "Trim whitespace from text columns", "default": True},
                            "drop_all_null": {"type": "boolean", "description": "Drop completely empty rows", "default": True},
                            "remove_duplicates": {"type": "boolean", "description": "Remove duplicate rows", "default": True},
                            "standardize_columns": {"type": "boolean", "description": "Lowercase column names, replace spaces with underscores", "default": True},
                            "drop_if_null_columns": {"type": "array", "items": {"type": "string"}, "description": "Drop rows where these specific columns are null"},
                        },
                    },
                },
                "required": ["file_id"],
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "upload_to_db",
            "description": "Upload the file data into a PostgreSQL table. Creates a new table if it doesn't exist, or appends/replaces if specified. Returns SQL for user approval before executing.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_id": {"type": "string", "description": "The file_id to upload"},
                    "table_name": {"type": "string", "description": "Target table name in PostgreSQL"},
                    "if_exists": {
                        "type": "string",
                        "enum": ["fail", "append", "replace"],
                        "description": "'fail' = error if exists, 'append' = add rows, 'replace' = drop and recreate",
                    },
                },
                "required": ["file_id", "table_name"],
            },
        }
    },
    {
        "type": "function",
        "function": {
            "name": "compare_datasets",
            "description": "Generate a SQL query to compare or analyze datasets in the database. Describe the comparison logic in natural language. The generated SQL uses GROUP BY aggregations to keep results small. SQL is shown for user approval before execution.",
            "parameters": {
                "type": "object",
                "properties": {
                    "description": {"type": "string", "description": "Natural language description of the comparison/analysis to perform"},
                    "table1": {"type": "string", "description": "First table name (optional)"},
                    "table2": {"type": "string", "description": "Second table name (optional)"},
                },
                "required": ["description"],
            },
        }
    },
]

DATA_TOOL_DISPATCH = {
    'list_tables': list_tables_tool,
    'preview_file': preview_file_tool,
    'sanitize_file': sanitize_file_tool,
    'upload_to_db': upload_to_db_tool,
    'compare_datasets': compare_tool,
}
