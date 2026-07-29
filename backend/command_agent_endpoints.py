"""
command_agent_endpoints.py — Conversational Command Agent blueprint.

Lets users manage BPM projects/tasks, Workflow phases/checklists, and
dashboard data via natural-language prompts with:
  - Intent detection & entity extraction (GPT-4o)
  - Follow-up questions for missing required fields
  - Explicit user confirmation before any write
"""

import os
import json
import logging
import datetime
from flask import Blueprint, request, jsonify
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

cmd_agent_bp = Blueprint('command_agent', __name__)

# ── Azure OpenAI client ──────────────────────────────────────────────────────
_client = None

def _get_client():
    global _client
    if _client is None:
        _client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
        )
    return _client

MODEL = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

# `query` is injected from app.py
_query = None

def init_command_agent(query_fn):
    global _query
    _query = query_fn

# ── System prompt ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are the BSS Tool Command Agent — a smart assistant that helps users manage projects, tasks, workflows, phases, and dashboards via natural language.

TODAY'S DATE: __TODAY__

## What you can do:

### BPM (Projects, Tasks, Users)
- Create / update / list projects
- Create / assign / update tasks (with status, dates, recurrence, assignee)
- List / manage users

### Workflow (Phases, Checklists)
- Add / update / list phases (Initiation & Planning, SRS Finalization, etc.)
- Add / update checklist items in phases
- Mark checklist items complete/incomplete
- Assign phases to users

### Dashboards
- Update transformation activities (planned/actual per LOB)
- Update product statuses / parameters
- Update UAT test case statuses

## CRITICAL RULES:

1. **Parse the user's intent** — identify the ACTION (create, update, list, assign, delete, etc.) and the TARGET (project, task, phase, checklist, etc.)

2. **Check for missing required fields** — if the user says "create a project" but doesn't give a name, ASK for it. If they say "assign a task" but don't say who, ASK. Always ask ONE clear question at a time.

3. **NEVER execute without confirmation** — once you have all fields, present a structured summary of what you will do and ask "Shall I proceed? (yes/no)"

4. **Response format** — ALWAYS respond with valid JSON in this exact format:
```json
{
  "type": "question" | "confirmation" | "result" | "info",
  "message": "Human-readable message to show the user",
  "action": null | {
    "intent": "create_project" | "update_project" | "create_task" | "update_task" | "assign_task" | "list_projects" | "list_tasks" | "list_users" | "add_phase" | "update_phase" | "add_checklist" | "update_checklist" | "update_transformation" | "update_uat" | "update_product",
    "params": { ... extracted parameters ... }
  },
  "missing_fields": ["field1", "field2"] | null,
  "awaiting_confirmation": true | false
}
```

### Required fields per action:
- **create_project**: name (required), start_date, end_date, owner (name or username)
- **create_task**: project (name or code), title (required), assignee (name or username), status, start_date, end_date, description, recurrence_type, recurrence_days
- **update_task**: task identifier (code or title), fields to update (status, assignee, dates, etc.)
- **assign_task**: task identifier, assignee (name or username)
- **add_phase / update_phase**: phase_id or phase name, start_date, end_date, assigned_to, status
- **add_checklist**: phase_id, item_text (required)
- **update_checklist**: checklist item id or text, status (complete/incomplete)
- **update_transformation**: lob, phase_name, planned, actual
- **update_uat**: test_case_id, status, priority
- **update_product**: product_id or product_name, status, migration_flag

5. For **list** actions, respond with type "info" and include the data directly.

6. If the user confirms with "yes" / "proceed" / "go ahead", respond with type "result" and the action to execute.

7. Be conversational and helpful. Use the user's language style."""


# ── Execution logic ──────────────────────────────────────────────────────────

def _execute_action(action):
    """Execute a confirmed action against the database. Returns result dict."""
    intent = action.get('intent', '')
    params = action.get('params', {})

    try:
        if intent == 'create_project':
            return _exec_create_project(params)
        elif intent == 'update_project':
            return _exec_update_project(params)
        elif intent == 'create_task':
            return _exec_create_task(params)
        elif intent == 'update_task':
            return _exec_update_task(params)
        elif intent == 'assign_task':
            return _exec_assign_task(params)
        elif intent == 'list_projects':
            return _exec_list_projects(params)
        elif intent == 'list_tasks':
            return _exec_list_tasks(params)
        elif intent == 'list_users':
            return _exec_list_users(params)
        elif intent == 'add_checklist':
            return _exec_add_checklist(params)
        elif intent == 'update_checklist':
            return _exec_update_checklist(params)
        elif intent == 'update_phase':
            return _exec_update_phase(params)
        elif intent == 'update_transformation':
            return _exec_update_transformation(params)
        elif intent == 'update_uat':
            return _exec_update_uat(params)
        elif intent == 'update_product':
            return _exec_update_product(params)
        else:
            return {'success': False, 'error': f'Unknown intent: {intent}'}
    except Exception as e:
        logger.error(f"Action execution error: {e}")
        return {'success': False, 'error': str(e)}


def _resolve_user(name_or_username):
    """Find a user by username or partial full_name match."""
    if not name_or_username:
        return None
    row = _query("SELECT id, username, full_name, role FROM users WHERE username=%s",
                 (name_or_username,), fetch='one')
    if row:
        return row
    row = _query("SELECT id, username, full_name, role FROM users WHERE LOWER(full_name) LIKE %s LIMIT 1",
                 (f"%{name_or_username.lower()}%",), fetch='one')
    return row


def _resolve_project(ref):
    """Find a project by code, id, or partial name match."""
    if not ref:
        return None
    # Try code
    row = _query("SELECT * FROM projects WHERE code=%s", (str(ref).upper(),), fetch='one')
    if row:
        return row
    # Try id
    try:
        row = _query("SELECT * FROM projects WHERE id=%s", (int(ref),), fetch='one')
        if row:
            return row
    except (ValueError, TypeError):
        pass
    # Partial name
    row = _query("SELECT * FROM projects WHERE LOWER(name) LIKE %s LIMIT 1",
                 (f"%{str(ref).lower()}%",), fetch='one')
    return row


def _resolve_task(ref):
    """Find a task by code, id, or partial title match."""
    if not ref:
        return None
    row = _query("SELECT * FROM tasks WHERE code=%s", (str(ref).upper(),), fetch='one')
    if row:
        return row
    try:
        row = _query("SELECT * FROM tasks WHERE id=%s", (int(ref),), fetch='one')
        if row:
            return row
    except (ValueError, TypeError):
        pass
    row = _query("SELECT * FROM tasks WHERE LOWER(title) LIKE %s LIMIT 1",
                 (f"%{str(ref).lower()}%",), fetch='one')
    return row


# ── Action executors ─────────────────────────────────────────────────────────

def _exec_create_project(p):
    name = p.get('name', '').strip()
    if not name:
        return {'success': False, 'error': 'Project name is required'}
    owner = _resolve_user(p.get('owner')) if p.get('owner') else None
    # Default owner to first engagement_manager or programme_director
    if not owner:
        owner = _query("SELECT id FROM users WHERE role IN ('programme_director','engagement_manager') AND is_active=TRUE ORDER BY id LIMIT 1", fetch='one')
    owner_id = owner['id'] if owner else 1
    # Generate code
    row = _query("SELECT COALESCE(MAX(id), 0) AS m FROM projects", fetch='one')
    code = f"PRJ-{(row['m'] + 1):04d}"
    result = _query(
        "INSERT INTO projects (code, name, owner_manager_id, start_date, end_date, created_by) VALUES (%s, %s, %s, %s, %s, %s) RETURNING *",
        (code, name, owner_id, p.get('start_date'), p.get('end_date'), owner_id), fetch='one')
    return {'success': True, 'message': f'Project "{name}" created with code {code}', 'data': dict(result) if result else None}


def _exec_update_project(p):
    project = _resolve_project(p.get('project'))
    if not project:
        return {'success': False, 'error': f'Project not found: {p.get("project")}'}
    sets, vals = [], []
    for field in ('name', 'status', 'start_date', 'end_date'):
        if p.get(field) is not None:
            sets.append(f"{field}=%s")
            vals.append(p[field])
    if not sets:
        return {'success': False, 'error': 'No fields to update'}
    vals.append(project['id'])
    _query(f"UPDATE projects SET {', '.join(sets)} WHERE id=%s", vals, fetch=None)
    return {'success': True, 'message': f'Project {project["code"]} updated'}


def _exec_create_task(p):
    project = _resolve_project(p.get('project'))
    if not project:
        return {'success': False, 'error': f'Project not found: {p.get("project")}'}
    title = p.get('title', '').strip()
    if not title:
        return {'success': False, 'error': 'Task title is required'}
    assignee = _resolve_user(p.get('assignee')) if p.get('assignee') else None
    assignee_id = assignee['id'] if assignee else None
    # Generate code
    row = _query("SELECT COUNT(*) AS c FROM tasks WHERE project_id=%s", (project['id'],), fetch='one')
    code = f"{project['code']}-T{(row['c'] + 1):02d}"
    result = _query(
        """INSERT INTO tasks (code, project_id, title, description, assignee_id, status, start_date, end_date,
           assigned_by, recurrence_type, recurrence_days)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
        (code, project['id'], title, p.get('description'), assignee_id,
         p.get('status', 'todo'), p.get('start_date'), p.get('end_date'),
         assignee_id, p.get('recurrence_type', 'none'), p.get('recurrence_days')),
        fetch='one')
    assignee_str = f" assigned to {assignee['full_name']}" if assignee else ""
    return {'success': True, 'message': f'Task "{title}" created ({code}){assignee_str}', 'data': dict(result) if result else None}


def _exec_update_task(p):
    task = _resolve_task(p.get('task'))
    if not task:
        return {'success': False, 'error': f'Task not found: {p.get("task")}'}
    sets, vals = [], []
    for field in ('title', 'description', 'status', 'start_date', 'end_date', 'recurrence_type', 'recurrence_days'):
        if p.get(field) is not None:
            sets.append(f"{field}=%s")
            vals.append(p[field])
    if p.get('assignee'):
        user = _resolve_user(p['assignee'])
        if user:
            sets.append("assignee_id=%s")
            vals.append(user['id'])
    if not sets:
        return {'success': False, 'error': 'No fields to update'}
    sets.append("updated_at=NOW()")
    vals.append(task['id'])
    _query(f"UPDATE tasks SET {', '.join(sets)} WHERE id=%s", vals, fetch=None)
    return {'success': True, 'message': f'Task {task["code"]} updated'}


def _exec_assign_task(p):
    task = _resolve_task(p.get('task'))
    if not task:
        return {'success': False, 'error': f'Task not found: {p.get("task")}'}
    user = _resolve_user(p.get('assignee'))
    if not user:
        return {'success': False, 'error': f'User not found: {p.get("assignee")}'}
    _query("UPDATE tasks SET assignee_id=%s, updated_at=NOW() WHERE id=%s", (user['id'], task['id']), fetch=None)
    return {'success': True, 'message': f'Task {task["code"]} assigned to {user["full_name"]}'}


def _exec_list_projects(p):
    rows = _query("SELECT p.code, p.name, p.status, p.start_date, p.end_date, u.full_name AS owner FROM projects p LEFT JOIN users u ON u.id=p.owner_manager_id ORDER BY p.created_at DESC")
    return {'success': True, 'data': [dict(r) for r in rows]}


def _exec_list_tasks(p):
    project = _resolve_project(p.get('project')) if p.get('project') else None
    where, params = "", ()
    if project:
        where = "WHERE t.project_id=%s"
        params = (project['id'],)
    elif p.get('assignee'):
        user = _resolve_user(p['assignee'])
        if user:
            where = "WHERE t.assignee_id=%s"
            params = (user['id'],)
    elif p.get('status'):
        where = "WHERE t.status=%s"
        params = (p['status'],)
    rows = _query(
        f"""SELECT t.code, t.title, t.status, t.start_date, t.end_date,
                   u.full_name AS assignee, p.name AS project_name
            FROM tasks t LEFT JOIN users u ON u.id=t.assignee_id
            LEFT JOIN projects p ON p.id=t.project_id
            {where} ORDER BY t.created_at DESC LIMIT 50""", params)
    return {'success': True, 'data': [dict(r) for r in rows]}


def _exec_list_users(p):
    rows = _query("SELECT id, username, full_name, role, organisation, is_active FROM users WHERE is_active=TRUE ORDER BY role, full_name")
    return {'success': True, 'data': [dict(r) for r in rows]}


def _exec_add_checklist(p):
    phase_id = p.get('phase_id', '').strip()
    item_text = p.get('item_text', '').strip()
    if not phase_id or not item_text:
        return {'success': False, 'error': 'phase_id and item_text are required'}
    wf_id = p.get('wf_id', 'pre_migration')
    _query("INSERT INTO checklist (phase_id, wf_id, item_text, status) VALUES (%s, %s, %s, 'incomplete')",
           (phase_id, wf_id, item_text), fetch=None)
    return {'success': True, 'message': f'Checklist item added to {phase_id}: "{item_text}"'}


def _exec_update_checklist(p):
    ch_id = p.get('ch_id')
    status = p.get('status', 'complete')
    if not ch_id:
        return {'success': False, 'error': 'Checklist item ID required'}
    _query("UPDATE checklist SET status=%s WHERE ch_id=%s", (status, ch_id), fetch=None)
    return {'success': True, 'message': f'Checklist item {ch_id} marked {status}'}


def _exec_update_phase(p):
    phase_id = p.get('phase_id', '').strip()
    if not phase_id:
        return {'success': False, 'error': 'phase_id is required'}
    sets, vals = [], []
    for field in ('curr_status', 'start_dt', 'end_dt', 'assigned_to'):
        if p.get(field) is not None:
            sets.append(f"{field}=%s")
            vals.append(p[field])
    if not sets:
        return {'success': False, 'error': 'No fields to update'}
    vals.append(phase_id)
    _query(f"UPDATE phases SET {', '.join(sets)} WHERE phase_id=%s", vals, fetch=None)
    return {'success': True, 'message': f'Phase {phase_id} updated'}


def _exec_update_transformation(p):
    lob = p.get('lob')
    phase_name = p.get('phase_name')
    if not lob or not phase_name:
        return {'success': False, 'error': 'lob and phase_name are required'}
    sets, vals = [], []
    if p.get('planned') is not None:
        sets.append("planned=%s"); vals.append(p['planned'])
    if p.get('actual') is not None:
        sets.append("actual=%s"); vals.append(p['actual'])
    if not sets:
        return {'success': False, 'error': 'Provide planned or actual values'}
    vals.extend([lob, phase_name])
    _query(f"UPDATE transformation_activities SET {', '.join(sets)} WHERE lob=%s AND phase_name=%s", vals, fetch=None)
    return {'success': True, 'message': f'Transformation activity updated for {lob} / {phase_name}'}


def _exec_update_uat(p):
    tc_id = p.get('test_case_id')
    if not tc_id:
        return {'success': False, 'error': 'test_case_id is required'}
    sets, vals = [], []
    for field in ('status', 'priority', 'description'):
        if p.get(field) is not None:
            sets.append(f"{field}=%s")
            vals.append(p[field])
    if not sets:
        return {'success': False, 'error': 'No fields to update'}
    sets.append("updated_at=NOW()")
    vals.append(tc_id)
    _query(f"UPDATE uat_cases SET {', '.join(sets)} WHERE test_case_id=%s", vals, fetch=None)
    return {'success': True, 'message': f'UAT test case {tc_id} updated'}


def _exec_update_product(p):
    pid = p.get('product_id')
    if not pid:
        # Try by name
        if p.get('product_name'):
            row = _query("SELECT product_id FROM products WHERE LOWER(product_name) LIKE %s LIMIT 1",
                         (f"%{p['product_name'].lower()}%",), fetch='one')
            if row:
                pid = row['product_id']
    if not pid:
        return {'success': False, 'error': 'Product not found'}
    sets, vals = [], []
    for field in ('status', 'migration_flag', 'product_name'):
        if p.get(field) is not None:
            sets.append(f"{field}=%s")
            vals.append(p[field])
    if not sets:
        return {'success': False, 'error': 'No fields to update'}
    vals.append(pid)
    _query(f"UPDATE products SET {', '.join(sets)} WHERE product_id=%s", vals, fetch=None)
    return {'success': True, 'message': f'Product {pid} updated'}


# ── Context builder (gives GPT awareness of current data) ───────────────────

def _build_context():
    """Build a summary of current projects, tasks, and users for GPT context."""
    parts = []

    # Projects
    try:
        projects = _query("SELECT code, name, status FROM projects ORDER BY created_at DESC LIMIT 15")
        if projects:
            lines = [f"  - {p['code']}: {p['name']} ({p['status']})" for p in projects]
            parts.append("CURRENT PROJECTS:\n" + "\n".join(lines))
    except Exception:
        pass

    # Users
    try:
        users = _query("SELECT username, full_name, role FROM users WHERE is_active=TRUE ORDER BY role, full_name")
        if users:
            lines = [f"  - {u['username']}: {u['full_name']} ({u['role']})" for u in users]
            parts.append("AVAILABLE USERS:\n" + "\n".join(lines))
    except Exception:
        pass

    # Phases
    try:
        phases = _query("SELECT DISTINCT phase_id, curr_status FROM phases ORDER BY phase_id LIMIT 15")
        if phases:
            lines = [f"  - {p['phase_id']}: {p['curr_status']}" for p in phases]
            parts.append("WORKFLOW PHASES:\n" + "\n".join(lines))
    except Exception:
        pass

    return "\n\n".join(parts)


# ── Chat endpoint ────────────────────────────────────────────────────────────

@cmd_agent_bp.route('/chat', methods=['POST'])
def command_chat():
    data = request.json or {}
    message = (data.get('message') or '').strip()
    history = data.get('history', [])
    pending_action = data.get('pending_action')  # Action awaiting confirmation

    if not message:
        return jsonify({'error': 'No message provided'}), 400

    # If user is confirming a pending action
    if pending_action and message.lower() in ('yes', 'y', 'proceed', 'go ahead', 'confirm', 'do it', 'ok', 'sure'):
        result = _execute_action(pending_action)
        return jsonify({
            'type': 'result',
            'message': result.get('message', 'Action completed') if result.get('success') else f"Failed: {result.get('error')}",
            'success': result.get('success', False),
            'data': result.get('data'),
            'action': pending_action,
        })

    if pending_action and message.lower() in ('no', 'n', 'cancel', 'abort', 'nevermind', 'nope'):
        return jsonify({
            'type': 'info',
            'message': 'Cancelled. No changes were made. What else would you like to do?',
            'action': None,
        })

    # Build messages for GPT
    today = datetime.date.today().isoformat()
    context = _build_context()
    system = SYSTEM_PROMPT.replace("__TODAY__", today) + "\n\n" + context

    messages = [{"role": "system", "content": system}]
    for h in history[-20:]:
        messages.append({"role": h.get("role", "user"), "content": h.get("content", "")})
    messages.append({"role": "user", "content": message})

    try:
        client = _get_client()
        resp = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            max_tokens=1500,
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or '{}'
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # GPT returned non-JSON — wrap it
        parsed = {'type': 'info', 'message': raw, 'action': None}
    except Exception as e:
        logger.error(f"Command agent GPT error: {e}")
        return jsonify({'error': f'AI error: {str(e)}'}), 500

    resp_type = parsed.get('type', 'info')

    # If GPT says it's a list/info action that can execute immediately
    if resp_type == 'info' and parsed.get('action') and parsed['action'].get('intent', '').startswith('list_'):
        result = _execute_action(parsed['action'])
        if result.get('success') and result.get('data'):
            parsed['data'] = result['data']

    return jsonify(parsed)
