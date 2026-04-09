"""
Data Management Agent Service — Orchestrates GPT-4o with data tools.
Handles file upload, sanitization, DB upload, and dataset comparison.
All writes go through pending approval.
"""

import os
import json
import uuid
import datetime
import logging
from openai import AzureOpenAI
from dotenv import load_dotenv
from data_agent_tools import (
    DATA_TOOL_DEFINITIONS, DATA_TOOL_DISPATCH,
    get_file_info, execute_upload, execute_comparison_sql, _query,
)

load_dotenv()
logger = logging.getLogger(__name__)

client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
)
MODEL = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

SYSTEM_PROMPT = """You are the Data Management Agent for the BSS Migration Assurance Tool.
You help users upload, sanitize, and analyze CSV/PSV datasets against a PostgreSQL database.

CAPABILITIES:
1. **List tables** — show all existing DB tables and their schemas
2. **Preview file** — show headers, sample rows, size, estimated upload time for an uploaded file
3. **Sanitize file** — clean data: trim whitespace, remove nulls/duplicates, standardize column names
4. **Upload to DB** — create a new table (or append/replace) and load the file data
5. **Compare datasets** — generate GROUP BY SQL to compare/analyze tables, executed after user approval

WORKFLOW:
- When a user uploads a file, first call preview_file to show them the metadata
- If they want to sanitize, call sanitize_file with appropriate rules
- Before uploading, call list_tables so they can see existing tables and choose
- For upload_to_db: the result will need user approval before actual execution
- For compare_datasets: the generated SQL needs user approval before execution
- NEVER send the full dataset to this conversation — only use headers + sample rows + GROUP BY aggregates

CONTEXT:
- Today: {today}
- Current page: {page}
- Uploaded file: {file_info}

Be concise and helpful. When showing data, format it clearly.
"""


class DataManagementService:

    def chat(self, user_message, conversation_history=None, current_page='/', file_id=None):
        conversation_history = conversation_history or []
        today = datetime.date.today().isoformat()

        # Get file info if available
        file_info_str = "No file uploaded yet"
        if file_id:
            fi = get_file_info(file_id)
            if fi:
                file_info_str = json.dumps({
                    'file_id': fi['file_id'],
                    'name': fi['original_name'],
                    'headers': fi['headers'],
                    'rows': fi['row_count'],
                    'size': fi['file_size_human'],
                    'sanitized': fi['sanitized'],
                }, default=str)

        system_prompt = SYSTEM_PROMPT.format(today=today, page=current_page, file_info=file_info_str)

        messages = [{"role": "system", "content": system_prompt}]
        for msg in conversation_history:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
        messages.append({"role": "user", "content": user_message})

        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=messages,
                tools=DATA_TOOL_DEFINITIONS,
                tool_choice="auto",
                max_tokens=2048,
                temperature=0,
            )
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return {'reply': f'Error calling AI: {str(e)}', 'tool_used': None, 'pending_confirmation': None}

        choice = response.choices[0]
        tool_used = None
        pending_confirmation = None

        if choice.finish_reason == 'tool_calls' or choice.message.tool_calls:
            tool_calls = choice.message.tool_calls
            messages.append(choice.message)

            for tc in tool_calls:
                fn_name = tc.function.name
                tool_used = fn_name
                try:
                    args = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    args = {}

                # If file_id not in args but we have one, inject it
                if file_id and 'file_id' not in args and fn_name in ('preview_file', 'sanitize_file', 'upload_to_db'):
                    args['file_id'] = file_id

                dispatch_fn = DATA_TOOL_DISPATCH.get(fn_name)
                if not dispatch_fn:
                    tool_result = json.dumps({'error': f'Unknown tool: {fn_name}'})
                else:
                    result = dispatch_fn(args)

                    # Check if this needs approval
                    if result.get('requires_approval'):
                        pending = self._store_pending(fn_name, args, result)
                        pending_confirmation = pending
                        tool_result = json.dumps({
                            'status': 'pending_confirmation',
                            'pending_id': pending['pending_id'],
                            'description': result.get('description', ''),
                            'message': 'This operation requires your approval before execution.',
                        }, default=str)
                    else:
                        tool_result = json.dumps(result, default=str)

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": tool_result,
                })

            # Final reply
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
        }

    def _store_pending(self, tool_name, args, result):
        """Store a pending operation for user approval."""
        pending_id = str(uuid.uuid4())

        if tool_name == 'upload_to_db':
            sql_preview = result.get('create_sql', '')
            description = result.get('description', f"Upload to {args.get('table_name')}")
            diff = {
                'operation': 'UPLOAD',
                'table': args.get('table_name', ''),
                'rows_affected': result.get('row_count', 0),
                'columns': result.get('columns', {}),
                'create_sql': result.get('create_sql', ''),
            }
        elif tool_name == 'compare_datasets':
            sql_preview = result.get('sql', '')
            description = result.get('description', 'Compare datasets')
            diff = {
                'operation': 'QUERY',
                'tables_involved': result.get('tables_involved', []),
                'sql': result.get('sql', ''),
            }
        else:
            sql_preview = json.dumps(args)
            description = f"{tool_name} operation"
            diff = {'operation': tool_name, 'args': args}

        try:
            _query(
                """INSERT INTO pending_writes (id, tool_name, sql_to_execute, params_json, diff_json, description, status)
                   VALUES (%s, %s, %s, %s, %s, %s, 'pending')""",
                (pending_id, f"data_{tool_name}", sql_preview,
                 json.dumps(args, default=str), json.dumps(diff, default=str), description),
                fetch=None,
            )
        except Exception as e:
            logger.error(f"Failed to store pending: {e}")

        return {
            'pending_id': pending_id,
            'tool_name': tool_name,
            'description': description,
            'diff': diff,
            'sql_preview': sql_preview,
        }

    def execute_confirmed(self, pending_id):
        """Execute a confirmed pending operation."""
        try:
            row = _query("SELECT * FROM pending_writes WHERE id = %s", (pending_id,), fetch='one')
        except Exception as e:
            return {'success': False, 'error': str(e)}

        if not row:
            return {'success': False, 'error': 'Pending operation not found'}

        row = dict(row)
        if row['status'] != 'pending':
            return {'success': False, 'error': f"Already {row['status']}"}

        if row.get('expires_at') and row['expires_at'] < datetime.datetime.now():
            _query("UPDATE pending_writes SET status = 'expired' WHERE id = %s", (pending_id,), fetch=None)
            return {'success': False, 'error': 'Expired (10 min TTL)'}

        args = json.loads(row['params_json']) if isinstance(row['params_json'], str) else row['params_json']
        tool_name = row['tool_name']

        if tool_name == 'data_upload_to_db':
            file_id = args.get('file_id')
            table_name = args.get('table_name', '')
            diff = json.loads(row['diff_json']) if isinstance(row['diff_json'], str) else row['diff_json']
            create_sql = diff.get('create_sql', '')
            result = execute_upload(file_id, table_name, create_sql)
        elif tool_name == 'data_compare_datasets':
            diff = json.loads(row['diff_json']) if isinstance(row['diff_json'], str) else row['diff_json']
            sql = diff.get('sql', row.get('sql_to_execute', ''))
            result = execute_comparison_sql(sql)
        else:
            return {'success': False, 'error': f'Unknown tool: {tool_name}'}

        if result.get('error'):
            return {'success': False, 'error': result['error']}

        # Mark as executed
        _query("UPDATE pending_writes SET status = 'executed' WHERE id = %s", (pending_id,), fetch=None)

        # Audit log
        try:
            _query(
                """INSERT INTO agent_audit_log (user_id, prompt, tool_called, sql_executed, rows_affected, diff_json, page_context)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                ('Programme User', row['description'], tool_name,
                 row.get('sql_to_execute', ''), result.get('rows_inserted', result.get('row_count', 0)),
                 row.get('diff_json', '{}'), 'data_agent'),
                fetch=None,
            )
        except Exception as e:
            logger.warning(f"Audit log failed: {e}")

        return {
            'success': True,
            'rows_affected': result.get('rows_inserted', result.get('row_count', 0)),
            'result': result,
        }

    def reject(self, pending_id):
        try:
            _query("UPDATE pending_writes SET status = 'rejected' WHERE id = %s", (pending_id,), fetch=None)
            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}
