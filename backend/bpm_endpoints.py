"""
bpm_endpoints.py — BPM (Business Process Management) blueprint.

Provides project / task management and superadmin user management, all gated
by the existing JWT auth + role system in auth_service.py.

RBAC summary:
  programme_director  (superadmin) — full access to all projects/tasks + user mgmt
  engagement_manager             — owns projects; creates & assigns tasks below hierarchy
  bss_consultant/qa_manager/data_analyst (workers) — update status + comment on OWN tasks
  client_* (sponsor/it_lead/operations) — strictly READ-ONLY across BPM

Schema is created on backend boot via ensure_bpm_schema() (called from app.py).
"""

import logging
from flask import Blueprint, request, jsonify

from auth_service import (
    require_auth,
    hash_password,
    can_assign_to,
    hierarchy_level,
    WORKER_ROLES,
    MANAGER_ROLES,
    CLIENT_ROLES,
    ROLE_PERMISSIONS,
)

logger = logging.getLogger(__name__)

bpm_bp = Blueprint('bpm', __name__)

# `query` is injected from app.py at registration time to reuse the single
# DB-config / connection helper rather than re-opening a second pool.
_query = None


def init_bpm(query_fn):
    """Wire the shared DB query helper into this blueprint."""
    global _query
    _query = query_fn


# ─── Schema (idempotent — runs every boot, never drops data) ────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS projects (
    id               SERIAL PRIMARY KEY,
    code             VARCHAR(20)  UNIQUE NOT NULL,
    name             VARCHAR(160) NOT NULL,
    owner_manager_id INTEGER      NOT NULL REFERENCES users(id),
    start_date       DATE,
    end_date         DATE,
    status           VARCHAR(30)  DEFAULT 'active',
    created_by       INTEGER      REFERENCES users(id),
    created_at       TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tasks (
    id           SERIAL PRIMARY KEY,
    code         VARCHAR(24)  UNIQUE NOT NULL,
    project_id   INTEGER      NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    title        VARCHAR(200) NOT NULL,
    description  TEXT,
    assignee_id  INTEGER      REFERENCES users(id),
    status       VARCHAR(30)  DEFAULT 'todo',
    start_date   DATE,
    end_date     DATE,
    assigned_by  INTEGER      REFERENCES users(id),
    created_at   TIMESTAMPTZ  DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS task_comments (
    id         SERIAL PRIMARY KEY,
    task_id    INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    author_id  INTEGER NOT NULL REFERENCES users(id),
    body       TEXT    NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Extend the existing users table (idempotent):
--   manager_id : which engagement manager a worker reports to
--   is_demo    : seed/demo accounts, hidden from the user-management view
ALTER TABLE users ADD COLUMN IF NOT EXISTS manager_id INTEGER REFERENCES users(id);
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_demo BOOLEAN DEFAULT FALSE;
"""

# Seed accounts created by seed_users.py — login still works, but they are
# hidden from the Users management tab so only real, superadmin-created users show.
DEMO_USERNAMES = (
    'prog_director', 'eng_manager', 'bss_consultant', 'qa_manager',
    'data_analyst', 'client_sponsor', 'client_it', 'client_ops',
    'admin', 'james.okonkwo',
)

VALID_STATUSES = {'todo', 'in_progress', 'blocked', 'done'}


def ensure_bpm_schema(query_fn):
    """Create BPM tables + extend users if missing. Called once on backend boot."""
    query_fn(SCHEMA_SQL, fetch=None)
    # Mark known seed accounts as demo (only flips NULL/false → true once).
    query_fn(
        "UPDATE users SET is_demo=TRUE WHERE username = ANY(%s) AND is_demo IS NOT TRUE",
        (list(DEMO_USERNAMES),), fetch=None,
    )
    logger.info("BPM schema ensured (projects, tasks, task_comments; users extended)")


# ─── Helpers ────────────────────────────────────────────────────────────────────
def _is_superadmin(user):
    return user['role'] == 'programme_director'


def _is_manager(user):
    return user['role'] in MANAGER_ROLES


def _validate_assignee(current_user, assignee_id):
    """Validate a task assignee. Returns (error_message, http_status) or (None, None) if ok.

    Rules: must exist · not a read-only client · strictly below assigner's level ·
    and (unless superadmin) must be on the assigner's own team (manager_id == me).
    """
    assignee = _query(
        "SELECT id, role, manager_id FROM users WHERE id=%s", (assignee_id,), fetch='one'
    )
    if not assignee:
        return 'Assignee not found', 400
    if assignee['role'] in CLIENT_ROLES:
        return 'Tasks cannot be assigned to read-only client roles', 403
    if not can_assign_to(current_user['role'], assignee['role']):
        return 'Tasks can only be assigned below your hierarchy level', 403
    if not _is_superadmin(current_user) and assignee['manager_id'] != int(current_user['sub']):
        return 'You may only assign tasks to members of your own team', 403
    return None, None


def _next_project_code():
    row = _query("SELECT COALESCE(MAX(id), 0) AS m FROM projects", fetch='one')
    return f"PRJ-{(row['m'] + 1):04d}"


def _next_task_code(project_code, project_id):
    row = _query(
        "SELECT COUNT(*) AS c FROM tasks WHERE project_id=%s", (project_id,), fetch='one'
    )
    return f"{project_code}-T{(row['c'] + 1):02d}"


def _project_visible_filter(user):
    """SQL fragment + params restricting projects to those the user may see."""
    if _is_superadmin(user):
        return "TRUE", ()
    if user['role'] == 'engagement_manager':
        return "p.owner_manager_id = %s", (int(user['sub']),)
    # Workers see projects that contain a task assigned to them; clients see all (read-only).
    if user['role'] in WORKER_ROLES:
        return ("p.id IN (SELECT project_id FROM tasks WHERE assignee_id = %s)",
                (int(user['sub']),))
    return "TRUE", ()  # client_* — read-only, may view all


# ─── Projects ───────────────────────────────────────────────────────────────────
@bpm_bp.route('/projects', methods=['GET'])
@require_auth
def list_projects(current_user):
    where, params = _project_visible_filter(current_user)
    rows = _query(
        f"""
        SELECT p.*, u.full_name AS owner_manager_name
        FROM projects p
        LEFT JOIN users u ON u.id = p.owner_manager_id
        WHERE {where}
        ORDER BY p.created_at DESC
        """,
        params,
    )
    return jsonify(list(rows))


@bpm_bp.route('/projects', methods=['POST'])
@require_auth
def create_project(current_user):
    if not _is_manager(current_user):
        return jsonify({'error': 'Only managers may create projects'}), 403
    data = request.json or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'error': 'Project name is required'}), 400

    # Superadmin may assign any manager as owner; an engagement_manager owns its own.
    if _is_superadmin(current_user):
        owner_id = data.get('owner_manager_id') or int(current_user['sub'])
    else:
        owner_id = int(current_user['sub'])

    code = _next_project_code()
    row = _query(
        """
        INSERT INTO projects (code, name, owner_manager_id, start_date, end_date, created_by)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING *
        """,
        (code, name, owner_id, data.get('start_date') or None,
         data.get('end_date') or None, int(current_user['sub'])),
        fetch='one',
    )
    return jsonify(row), 201


def _load_project(project_id):
    return _query("SELECT * FROM projects WHERE id=%s", (project_id,), fetch='one')


def _can_edit_project(user, project):
    return _is_superadmin(user) or project['owner_manager_id'] == int(user['sub'])


@bpm_bp.route('/projects/<int:project_id>', methods=['PUT'])
@require_auth
def update_project(current_user, project_id):
    project = _load_project(project_id)
    if not project:
        return jsonify({'error': 'Project not found'}), 404
    if not _can_edit_project(current_user, project):
        return jsonify({'error': 'You may only edit your own projects'}), 403
    data = request.json or {}
    row = _query(
        """
        UPDATE projects SET
          name       = COALESCE(%s, name),
          start_date = COALESCE(%s, start_date),
          end_date   = COALESCE(%s, end_date),
          status     = COALESCE(%s, status)
        WHERE id=%s RETURNING *
        """,
        (data.get('name'), data.get('start_date'), data.get('end_date'),
         data.get('status'), project_id),
        fetch='one',
    )
    return jsonify(row)


@bpm_bp.route('/projects/<int:project_id>', methods=['DELETE'])
@require_auth
def delete_project(current_user, project_id):
    project = _load_project(project_id)
    if not project:
        return jsonify({'error': 'Project not found'}), 404
    if not _can_edit_project(current_user, project):
        return jsonify({'error': 'You may only delete your own projects'}), 403
    _query("DELETE FROM projects WHERE id=%s", (project_id,), fetch=None)
    return jsonify({'ok': True})


# ─── Tasks ──────────────────────────────────────────────────────────────────────
@bpm_bp.route('/tasks', methods=['GET'])
@require_auth
def list_tasks(current_user):
    project_id = request.args.get('project_id')
    where, params = _project_visible_filter(current_user)
    params = list(params)
    extra = ""
    if project_id:
        extra = " AND t.project_id = %s"
        params.append(int(project_id))
    rows = _query(
        f"""
        SELECT t.*, p.code AS project_code, p.name AS project_name,
               a.full_name AS assignee_name, a.role AS assignee_role
        FROM tasks t
        JOIN projects p ON p.id = t.project_id
        LEFT JOIN users a ON a.id = t.assignee_id
        WHERE {where}{extra}
        ORDER BY t.created_at DESC
        """,
        tuple(params),
    )
    return jsonify(list(rows))


@bpm_bp.route('/tasks', methods=['POST'])
@require_auth
def create_task(current_user):
    if not _is_manager(current_user):
        return jsonify({'error': 'Only managers may create tasks'}), 403
    data = request.json or {}
    project_id = data.get('project_id')
    title = (data.get('title') or '').strip()
    if not project_id or not title:
        return jsonify({'error': 'project_id and title are required'}), 400

    project = _load_project(project_id)
    if not project:
        return jsonify({'error': 'Project not found'}), 404
    if not _can_edit_project(current_user, project):
        return jsonify({'error': 'You may only add tasks to your own projects'}), 403

    assignee_id = data.get('assignee_id')
    if assignee_id:
        err, status = _validate_assignee(current_user, assignee_id)
        if err:
            return jsonify({'error': err}), status

    code = _next_task_code(project['code'], project['id'])
    row = _query(
        """
        INSERT INTO tasks (code, project_id, title, description, assignee_id,
                           status, start_date, end_date, assigned_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
        """,
        (code, project_id, title, data.get('description'), assignee_id or None,
         data.get('status') or 'todo', data.get('start_date') or None,
         data.get('end_date') or None, int(current_user['sub'])),
        fetch='one',
    )
    return jsonify(row), 201


def _load_task(task_id):
    return _query(
        """
        SELECT t.*, p.owner_manager_id
        FROM tasks t JOIN projects p ON p.id = t.project_id
        WHERE t.id=%s
        """,
        (task_id,), fetch='one',
    )


@bpm_bp.route('/tasks/<int:task_id>', methods=['PUT'])
@require_auth
def update_task(current_user, task_id):
    """Full edit / reassign — managers who own the project only."""
    task = _load_task(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    is_owner_mgr = _is_superadmin(current_user) or task['owner_manager_id'] == int(current_user['sub'])
    if not is_owner_mgr:
        return jsonify({'error': 'Only the owning manager may edit this task'}), 403

    data = request.json or {}
    assignee_id = data.get('assignee_id', '__keep__')
    if assignee_id != '__keep__' and assignee_id:
        err, status = _validate_assignee(current_user, assignee_id)
        if err:
            return jsonify({'error': err}), status

    row = _query(
        """
        UPDATE tasks SET
          title       = COALESCE(%s, title),
          description = COALESCE(%s, description),
          assignee_id = CASE WHEN %s THEN %s ELSE assignee_id END,
          status      = COALESCE(%s, status),
          start_date  = COALESCE(%s, start_date),
          end_date    = COALESCE(%s, end_date),
          updated_at  = NOW()
        WHERE id=%s RETURNING *
        """,
        (data.get('title'), data.get('description'),
         assignee_id != '__keep__', (assignee_id or None) if assignee_id != '__keep__' else None,
         data.get('status'), data.get('start_date'), data.get('end_date'), task_id),
        fetch='one',
    )
    return jsonify(row)


@bpm_bp.route('/tasks/<int:task_id>/status', methods=['PATCH'])
@require_auth
def update_task_status(current_user, task_id):
    """Status-only update — assignee (worker) or owning manager. Clients blocked."""
    if current_user['role'] in CLIENT_ROLES:
        return jsonify({'error': 'Read-only role'}), 403
    task = _load_task(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404

    is_owner_mgr = _is_superadmin(current_user) or task['owner_manager_id'] == int(current_user['sub'])
    is_assignee = task['assignee_id'] == int(current_user['sub'])
    if not (is_owner_mgr or is_assignee):
        return jsonify({'error': 'You may only update status of tasks assigned to you'}), 403

    status = (request.json or {}).get('status')
    if status not in VALID_STATUSES:
        return jsonify({'error': f'status must be one of {sorted(VALID_STATUSES)}'}), 400
    row = _query(
        "UPDATE tasks SET status=%s, updated_at=NOW() WHERE id=%s RETURNING *",
        (status, task_id), fetch='one',
    )
    return jsonify(row)


# ─── Task comments (workers + managers; clients blocked) ────────────────────────
@bpm_bp.route('/tasks/<int:task_id>/comments', methods=['GET'])
@require_auth
def list_comments(current_user, task_id):
    rows = _query(
        """
        SELECT c.*, u.full_name AS author_name, u.role AS author_role
        FROM task_comments c LEFT JOIN users u ON u.id = c.author_id
        WHERE c.task_id=%s ORDER BY c.created_at ASC
        """,
        (task_id,),
    )
    return jsonify(list(rows))


@bpm_bp.route('/tasks/<int:task_id>/comments', methods=['POST'])
@require_auth
def add_comment(current_user, task_id):
    if current_user['role'] in CLIENT_ROLES:
        return jsonify({'error': 'Read-only role'}), 403
    task = _load_task(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    is_owner_mgr = _is_superadmin(current_user) or task['owner_manager_id'] == int(current_user['sub'])
    is_assignee = task['assignee_id'] == int(current_user['sub'])
    if not (is_owner_mgr or is_assignee):
        return jsonify({'error': 'You may only comment on tasks assigned to you'}), 403
    body = ((request.json or {}).get('body') or '').strip()
    if not body:
        return jsonify({'error': 'Comment body required'}), 400
    row = _query(
        """
        INSERT INTO task_comments (task_id, author_id, body)
        VALUES (%s, %s, %s) RETURNING *
        """,
        (task_id, int(current_user['sub']), body), fetch='one',
    )
    return jsonify(row), 201


# ─── Assignable users (for the assignee dropdown) ───────────────────────────────
@bpm_bp.route('/users/assignable', methods=['GET'])
@require_auth
def assignable_users(current_user):
    if not _is_manager(current_user):
        return jsonify([])
    my_level = hierarchy_level(current_user['role'])
    rows = _query(
        "SELECT id, full_name, role, organisation, manager_id FROM users WHERE is_active=TRUE ORDER BY full_name"
    )
    me = int(current_user['sub'])
    out = []
    for r in rows:
        # Never assign to read-only client roles, nor above one's own level.
        if r['role'] in CLIENT_ROLES or hierarchy_level(r['role']) >= my_level:
            continue
        # Superadmin may assign anyone; an engagement manager only their own team.
        if _is_superadmin(current_user) or r['manager_id'] == me:
            out.append(r)
    return jsonify(out)


# ─── Superadmin user management ─────────────────────────────────────────────────
def _require_superadmin(current_user):
    return current_user['role'] == 'programme_director'


@bpm_bp.route('/admin/users', methods=['GET'])
@require_auth
def admin_list_users(current_user):
    if not _require_superadmin(current_user):
        return jsonify({'error': 'Superadmin only'}), 403
    # Hide seed/demo accounts — show only real, superadmin-created users.
    rows = _query(
        """
        SELECT u.id, u.username, u.full_name, u.email, u.role, u.organisation,
               u.is_active, u.manager_id, m.full_name AS manager_name,
               u.created_at, u.last_login
        FROM users u
        LEFT JOIN users m ON m.id = u.manager_id
        WHERE u.is_demo IS NOT TRUE
        ORDER BY u.role, u.username
        """
    )
    return jsonify(list(rows))


@bpm_bp.route('/admin/managers', methods=['GET'])
@require_auth
def admin_list_managers(current_user):
    """Engagement managers — for the 'reports to' dropdown when creating a worker."""
    if not _require_superadmin(current_user):
        return jsonify({'error': 'Superadmin only'}), 403
    rows = _query(
        """
        SELECT id, full_name, username FROM users
        WHERE role='engagement_manager' AND is_active=TRUE
        ORDER BY full_name
        """
    )
    return jsonify(list(rows))


@bpm_bp.route('/admin/users', methods=['POST'])
@require_auth
def admin_create_user(current_user):
    if not _require_superadmin(current_user):
        return jsonify({'error': 'Superadmin only'}), 403
    data = request.json or {}
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''
    full_name = (data.get('full_name') or '').strip()
    role = data.get('role')
    if not username or not password or not full_name or not role:
        return jsonify({'error': 'username, password, full_name and role are required'}), 400
    if role not in ROLE_PERMISSIONS:
        return jsonify({'error': f'Invalid role. Valid: {sorted(ROLE_PERMISSIONS)}'}), 400
    existing = _query("SELECT id FROM users WHERE username=%s", (username,), fetch='one')
    if existing:
        return jsonify({'error': 'Username already exists'}), 409

    # Optional reporting manager — only valid for worker roles, must be an engagement manager.
    manager_id = data.get('manager_id') or None
    if manager_id:
        if role not in WORKER_ROLES:
            return jsonify({'error': 'Only worker roles can report to a manager'}), 400
        mgr = _query("SELECT role FROM users WHERE id=%s", (manager_id,), fetch='one')
        if not mgr or mgr['role'] != 'engagement_manager':
            return jsonify({'error': 'manager_id must reference an engagement manager'}), 400

    row = _query(
        """
        INSERT INTO users (username, password_hash, full_name, email, role, organisation, manager_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id, username, full_name, email, role, organisation, manager_id, is_active, created_at
        """,
        (username, hash_password(password), full_name, data.get('email'),
         role, data.get('organisation') or 'KPMG Advisory', manager_id),
        fetch='one',
    )
    return jsonify(row), 201


@bpm_bp.route('/admin/users/<int:user_id>', methods=['PUT'])
@require_auth
def admin_update_user(current_user, user_id):
    if not _require_superadmin(current_user):
        return jsonify({'error': 'Superadmin only'}), 403
    data = request.json or {}
    role = data.get('role')
    if role is not None and role not in ROLE_PERMISSIONS:
        return jsonify({'error': 'Invalid role'}), 400

    # Optional password reset
    new_pw_hash = hash_password(data['password']) if data.get('password') else None

    # manager_id: use sentinel so "not provided" keeps the value, while explicit null clears it.
    set_manager = 'manager_id' in data
    manager_id = data.get('manager_id') or None
    if set_manager and manager_id:
        mgr = _query("SELECT role FROM users WHERE id=%s", (manager_id,), fetch='one')
        if not mgr or mgr['role'] != 'engagement_manager':
            return jsonify({'error': 'manager_id must reference an engagement manager'}), 400

    row = _query(
        """
        UPDATE users SET
          full_name     = COALESCE(%s, full_name),
          email         = COALESCE(%s, email),
          role          = COALESCE(%s, role),
          organisation  = COALESCE(%s, organisation),
          is_active     = COALESCE(%s, is_active),
          password_hash = COALESCE(%s, password_hash),
          manager_id    = CASE WHEN %s THEN %s ELSE manager_id END
        WHERE id=%s
        RETURNING id, username, full_name, email, role, organisation, manager_id, is_active, created_at, last_login
        """,
        (data.get('full_name'), data.get('email'), role, data.get('organisation'),
         data.get('is_active'), new_pw_hash, set_manager, manager_id, user_id),
        fetch='one',
    )
    if not row:
        return jsonify({'error': 'User not found'}), 404
    return jsonify(row)
