"""
Agentic AI Service — BSS Migration Assurance Tool
Uses OpenAI GPT-4o with function calling and 4 tools.
All writes go through a pending confirmation gate.
"""

import os
import json
import uuid
import datetime
import psycopg2
import psycopg2.extras
import logging
from openai import AzureOpenAI
from dotenv import load_dotenv
from agent_tools import (
    TOOL_DEFINITIONS, TOOL_DISPATCH,
    crud_tool, reconciliation_tool, ocr_tool, alerts_tool,
    _get_db, _query, _rows_to_dicts,
)

load_dotenv()
logger = logging.getLogger(__name__)

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
)
MODEL = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

SCHEMA_CONTEXT = """You are an AI agent for the BSS Migration Assurance Tool (KPMG).
You help users manage BSS migration data across these PostgreSQL tables:

TABLES:
1. uat_cases (test_case_id TEXT PK, lob TEXT, priority TEXT, status TEXT, description TEXT, created_at TIMESTAMP, updated_at TIMESTAMP)
2. products (product_id TEXT PK, product_name TEXT, lob TEXT, migration_flag TEXT, status TEXT, created_at TIMESTAMP, deleted_at TIMESTAMP)
3. product_parameters (id SERIAL PK, param_name TEXT, lob TEXT, product_id TEXT FK, status TEXT, matched INTEGER, total INTEGER, updated_at TIMESTAMP)
4. transformation_activities (id SERIAL PK, lob TEXT, phase_name TEXT, planned NUMERIC, actual NUMERIC, variance NUMERIC GENERATED)
5. transformation_lob_progress (id SERIAL PK, lob TEXT, planned INTEGER, actual INTEGER, updated_at TIMESTAMP)
6. phases (id TEXT PK, phase_id TEXT, curr_status TEXT, start_dt TEXT, end_dt TEXT, lob TEXT, assigned_to TEXT)
7. checklist (ch_id SERIAL PK, phase_id TEXT, wf_id TEXT, item_text TEXT, status TEXT)
8. checklist_comments (id SERIAL PK, ch_id INTEGER FK, username TEXT, comment TEXT, created_at TIMESTAMP)
9. checklist_attachments (id SERIAL PK, ch_id INTEGER FK, file_name TEXT, file_path TEXT, uploaded_by TEXT, uploaded_at TIMESTAMP)
10. reconciliation_data (id SERIAL PK, account_link_code TEXT, service_code TEXT, service_name TEXT, cbs_status TEXT, clm_status TEXT, status TEXT, uploaded_at TIMESTAMP)
11. kpi_results (id SERIAL PK, metric_name TEXT, metric_value NUMERIC, period TEXT, calculated_at TIMESTAMP)

RULES:
- NEVER write to agent_audit_log or pending_writes tables
- NEVER use DELETE FROM — only soft deletes via deleted_at = NOW()
- No DB write executes without showing a diff first — all writes go through pending confirmation
- Today's date: {today}
- Current page context: {page}
- Use the tools provided to answer questions and make changes
- For READ operations, execute immediately and show results
- For any WRITE operation (CREATE, UPDATE, DELETE, BULK_UPDATE), always use the crud_tool so the system can capture the diff
- Be concise and helpful
"""

# Operations that modify data
WRITE_OPERATIONS = {'CREATE', 'UPDATE', 'DELETE', 'BULK_UPDATE'}


class AgentService:

    def chat(self, user_message, conversation_history=None, current_page='/'):
        """
        Main chat method. Calls GPT-4o with tools, handles tool calls.
        Write operations are stored as pending and require confirmation.
        """
        conversation_history = conversation_history or []
        today = datetime.date.today().isoformat()

        system_prompt = SCHEMA_CONTEXT.format(today=today, page=current_page)

        messages = [{"role": "system", "content": system_prompt}]
        for msg in conversation_history:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
        messages.append({"role": "user", "content": user_message})

        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=messages,
                tools=TOOL_DEFINITIONS,
                tool_choice="auto",
                max_tokens=2048,
                temperature=0,
            )
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return {'reply': f'Error calling AI: {str(e)}', 'tool_used': None, 'pending_confirmation': None, 'alerts': None}

        choice = response.choices[0]
        tool_used = None
        pending_confirmation = None
        alerts_data = None

        # Handle tool calls
        if choice.finish_reason == 'tool_calls' or choice.message.tool_calls:
            tool_calls = choice.message.tool_calls
            # Add assistant message with tool calls to history
            messages.append(choice.message)

            for tc in tool_calls:
                fn_name = tc.function.name
                tool_used = fn_name
                try:
                    args = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    args = {}

                # Check if this is a write operation that needs confirmation
                is_write = (
                    fn_name == 'crud_tool'
                    and args.get('operation', 'READ').upper() in WRITE_OPERATIONS
                )

                if is_write:
                    # Don't execute — store as pending write
                    pending = self._create_pending_write(fn_name, args, current_page)
                    pending_confirmation = pending
                    tool_result = json.dumps({
                        'status': 'pending_confirmation',
                        'pending_id': pending['pending_id'],
                        'description': pending['description'],
                        'message': 'This write operation requires user confirmation before execution.',
                    })
                else:
                    # Execute read operations and alerts immediately
                    dispatch_fn = TOOL_DISPATCH.get(fn_name)
                    if dispatch_fn:
                        result = dispatch_fn(args)
                        if fn_name == 'alerts_tool':
                            alerts_data = result.get('alerts', [])
                        tool_result = json.dumps(result, default=str)
                    else:
                        tool_result = json.dumps({'error': f'Unknown tool: {fn_name}'})

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": tool_result,
                })

            # Get final natural language reply
            try:
                final_response = client.chat.completions.create(
                    model=MODEL,
                    messages=messages,
                    max_tokens=1024,
                    temperature=0,
                )
                reply = final_response.choices[0].message.content or ''
            except Exception as e:
                reply = f'Tool executed but error generating reply: {str(e)}'
        else:
            reply = choice.message.content or ''

        return {
            'reply': reply,
            'tool_used': tool_used,
            'pending_confirmation': pending_confirmation,
            'alerts': alerts_data,
        }

    def _create_pending_write(self, tool_name, args, page_context):
        """
        Simulate the write to capture before/after diff, then store as pending.
        Does NOT execute the actual write.
        """
        table = args.get('table', '')
        operation = args.get('operation', '').upper()
        filters = args.get('filters', {})
        data = args.get('data', {})

        # Capture BEFORE state
        before = []
        if filters and operation in ('UPDATE', 'DELETE', 'BULK_UPDATE'):
            try:
                where_parts = []
                where_params = []
                for k, v in filters.items():
                    where_parts.append(f"{k} = %s")
                    where_params.append(v)
                where_clause = ' WHERE ' + ' AND '.join(where_parts) if where_parts else ''
                rows = _query(f"SELECT * FROM {table}{where_clause}", where_params)
                before = _rows_to_dicts(rows)
            except Exception:
                before = []

        # Build simulated AFTER state
        after = []
        if operation == 'CREATE':
            after = [data]
        elif operation in ('UPDATE', 'BULK_UPDATE'):
            for row in before:
                updated = dict(row)
                updated.update(data)
                after.append(updated)
        elif operation == 'DELETE':
            for row in before:
                updated = dict(row)
                updated['deleted_at'] = datetime.datetime.now().isoformat()
                after.append(updated)

        # Build the SQL that would be executed
        sql_preview = self._build_sql_preview(table, operation, filters, data)

        # Compute description
        rows_affected = len(before) if before else 1
        description = f"{operation} on {table}: {rows_affected} row(s) affected"

        diff = {
            'before': before,
            'after': after,
            'table': table,
            'operation': operation,
            'rows_affected': rows_affected,
        }

        # Store in pending_writes table
        pending_id = str(uuid.uuid4())
        try:
            _query(
                """INSERT INTO pending_writes (id, tool_name, sql_to_execute, params_json, diff_json, description, status)
                   VALUES (%s, %s, %s, %s, %s, %s, 'pending')""",
                (
                    pending_id,
                    tool_name,
                    sql_preview,
                    json.dumps(args, default=str),
                    json.dumps(diff, default=str),
                    description,
                ),
                fetch=None,
            )
        except Exception as e:
            logger.error(f"Failed to store pending write: {e}")

        return {
            'pending_id': pending_id,
            'tool_name': tool_name,
            'description': description,
            'diff': diff,
            'sql_preview': sql_preview,
        }

    def _build_sql_preview(self, table, operation, filters, data):
        """Build a human-readable SQL preview (not for execution — preview only)."""
        if operation == 'CREATE':
            cols = ', '.join(data.keys())
            vals = ', '.join(f"'{v}'" for v in data.values())
            return f"INSERT INTO {table} ({cols}) VALUES ({vals})"
        elif operation in ('UPDATE', 'BULK_UPDATE'):
            set_clause = ', '.join(f"{k} = '{v}'" for k, v in data.items())
            where_clause = ' AND '.join(f"{k} = '{v}'" for k, v in (filters or {}).items())
            return f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        elif operation == 'DELETE':
            where_clause = ' AND '.join(f"{k} = '{v}'" for k, v in (filters or {}).items())
            return f"UPDATE {table} SET deleted_at = NOW() WHERE {where_clause}"
        return ''

    def execute_confirmed_write(self, pending_id):
        """
        Execute a previously confirmed pending write.
        Checks TTL, executes via crud_tool, logs to audit.
        """
        try:
            row = _query(
                "SELECT * FROM pending_writes WHERE id = %s",
                (pending_id,),
                fetch='one',
            )
        except Exception as e:
            return {'success': False, 'error': str(e)}

        if not row:
            return {'success': False, 'error': 'Pending write not found'}

        row = dict(row)
        if row['status'] != 'pending':
            return {'success': False, 'error': f"Write already {row['status']}"}

        # Check TTL
        if row['expires_at'] and row['expires_at'] < datetime.datetime.now():
            _query("UPDATE pending_writes SET status = 'expired' WHERE id = %s", (pending_id,), fetch=None)
            return {'success': False, 'error': 'Pending write has expired (10 min TTL)'}

        # Execute via crud_tool
        args = json.loads(row['params_json']) if isinstance(row['params_json'], str) else row['params_json']
        dispatch_fn = TOOL_DISPATCH.get(row['tool_name'])
        if not dispatch_fn:
            return {'success': False, 'error': f"Unknown tool: {row['tool_name']}"}

        result = dispatch_fn(args)

        if result.get('error'):
            return {'success': False, 'error': result['error']}

        # Mark as executed
        _query("UPDATE pending_writes SET status = 'executed' WHERE id = %s", (pending_id,), fetch=None)

        # Write audit log
        diff = result.get('before', []), result.get('after', [])
        try:
            _query(
                """INSERT INTO agent_audit_log (user_id, prompt, tool_called, sql_executed, rows_affected, diff_json, page_context)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    'Programme User',
                    row['description'],
                    row['tool_name'],
                    result.get('sql', row.get('sql_to_execute', '')),
                    result.get('rows_affected', 0),
                    json.dumps({'before': diff[0], 'after': diff[1]}, default=str),
                    'agent',
                ),
                fetch=None,
            )
        except Exception as e:
            logger.warning(f"Audit log write failed: {e}")

        return {
            'success': True,
            'rows_affected': result.get('rows_affected', 0),
            'diff': {
                'before': result.get('before', []),
                'after': result.get('after', []),
            },
        }

    def reject_write(self, pending_id):
        """Mark a pending write as rejected."""
        try:
            _query("UPDATE pending_writes SET status = 'rejected' WHERE id = %s", (pending_id,), fetch=None)
            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}
