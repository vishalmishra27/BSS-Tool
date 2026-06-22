import React, { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../context/AuthContext';

// ─── API helper (uses the JWT from AuthContext) ─────────────────────────────────
function useApi() {
  const { auth, logout } = useAuth();
  const token = auth?.token;
  return useCallback(async (path, { method = 'GET', body } = {}) => {
    const res = await fetch(`/api/bpm${path}`, {
      method,
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    if (res.status === 401) { logout(); throw new Error('Session expired'); }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `Request failed (${res.status})`);
    return data;
  }, [token, logout]);
}

// ─── Design tokens — matched to the Document Analysis Agent page ────────────────
const FONT = 'system-ui, sans-serif';
const GRADIENT = 'linear-gradient(135deg,#00C4A7,#001F5B)';
const C = {
  primary: '#001F5B',          // KPMG navy
  accent: '#00C4A7',           // teal
  border: '#e0e8f0',           // subtle card border
  borderSoft: '#c8d4f0',       // dashed / input border
  page: '#f8faff',             // page background
  surface: '#fff',
  pill: '#f0f4ff',             // light blue chip background
  heading: '#111',
  text: '#222',
  muted: '#666',
  faint: '#999',
  shadow: '0 3px 14px rgba(0,31,91,0.12)',
  success: '#00A98F',          // teal-green (active / save)
  danger: '#c00',              // red (delete / deactivate)
  neutral: '#8895ab',          // muted (cancel)
};
const STATUS_OPTS = ['todo', 'in_progress', 'blocked', 'done'];
const STATUS_LABEL = { todo: 'To Do', in_progress: 'In Progress', blocked: 'Blocked', done: 'Done' };
const STATUS_COLOR = { todo: '#888', in_progress: '#001F5B', blocked: '#c00', done: '#00A98F' };
const ROLES = [
  'programme_director', 'engagement_manager', 'bss_consultant', 'qa_manager',
  'data_analyst', 'client_sponsor', 'client_it_lead', 'client_operations',
];
const WORKER_ROLES_FE = ['bss_consultant', 'qa_manager', 'data_analyst'];

// Primary button uses the navy→teal gradient; pass a solid colour for variants.
const btn = (bg) => ({
  background: bg || GRADIENT, color: '#fff', border: 'none', padding: '8px 16px',
  borderRadius: 8, cursor: 'pointer', fontSize: 13, fontWeight: 600,
});
const inp = {
  padding: '9px 12px', border: `1px solid ${C.borderSoft}`, borderRadius: 8,
  fontSize: 14, width: '100%', boxSizing: 'border-box', fontFamily: FONT,
  background: '#fff', color: C.text,
};
const card = {
  background: C.surface, padding: '1.5rem', borderRadius: 12,
  boxShadow: '0 1px 3px rgba(0,31,91,0.06)', border: `1px solid ${C.border}`,
};

// ════════════════════════════════════════════════════════════════════════════
export default function Bpm() {
  const { role } = useAuth();
  const isSuperadmin = role === 'programme_director';
  const isManager = role === 'programme_director' || role === 'engagement_manager';
  const [tab, setTab] = useState('projects');
  const [err, setErr] = useState('');

  const tabs = [
    { key: 'projects', label: 'Projects' },
    { key: 'tasks', label: 'Tasks' },
    ...(isSuperadmin ? [{ key: 'users', label: 'Users' }] : []),
  ];

  return (
    <div style={{ minHeight: '100vh', padding: '2rem', fontFamily: FONT, color: C.text, background: C.page }}>
      {/* Header — gradient badge + navy title, matching the agent page */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 6 }}>
        <div style={{
          width: 40, height: 40, background: GRADIENT, borderRadius: 10,
          display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#fff', fontSize: 20,
        }}>✦</div>
        <div>
          <h1 style={{ color: C.primary, margin: 0, fontSize: 22, fontWeight: 700 }}>BPM Dashboard</h1>
          <p style={{ color: C.muted, margin: '2px 0 0', fontSize: 13 }}>
            Project, task &amp; user management — your access: <strong style={{ color: C.primary }}>{role || 'unknown'}</strong>
          </p>
        </div>
      </div>

      <div style={{ display: 'flex', gap: 4, margin: '1.5rem 0', borderBottom: `1px solid ${C.border}` }}>
        {tabs.map(t => (
          <button key={t.key} onClick={() => { setTab(t.key); setErr(''); }}
            style={{
              background: 'none', border: 'none', cursor: 'pointer', padding: '10px 18px',
              fontSize: 14, fontWeight: 600, fontFamily: FONT,
              color: tab === t.key ? C.primary : C.muted,
              borderBottom: tab === t.key ? `3px solid ${C.accent}` : '3px solid transparent',
              marginBottom: -1, transition: 'color 0.15s',
            }}>
            {t.label}
          </button>
        ))}
      </div>

      {err && <div style={{ background: '#fff5f5', color: '#b00', border: '1px solid #ffd0d0', padding: '10px 14px', borderRadius: 8, marginBottom: 16, fontSize: 14 }}>{err}</div>}

      {tab === 'projects' && <ProjectsTab isManager={isManager} isSuperadmin={isSuperadmin} onError={setErr} />}
      {tab === 'tasks' && <TasksTab isManager={isManager} role={role} onError={setErr} />}
      {tab === 'users' && isSuperadmin && <UsersTab onError={setErr} />}
    </div>
  );
}

// ─── Projects ───────────────────────────────────────────────────────────────────
function ProjectsTab({ isManager, isSuperadmin, onError }) {
  const api = useApi();
  const [projects, setProjects] = useState([]);
  const [managers, setManagers] = useState([]);
  const [form, setForm] = useState({ name: '', owner_manager_id: '', start_date: '', end_date: '' });
  const [showForm, setShowForm] = useState(false);

  const load = useCallback(async () => {
    try { setProjects(await api('/projects')); }
    catch (e) { onError(e.message); }
  }, [api, onError]);

  useEffect(() => { load(); }, [load]);

  // Superadmin assigns an owner manager → fetch managers via assignable users.
  useEffect(() => {
    if (!isSuperadmin) return;
    api('/users/assignable')
      .then(us => setManagers(us.filter(u => u.role === 'engagement_manager')))
      .catch(() => {});
  }, [api, isSuperadmin]);

  const create = async (e) => {
    e.preventDefault();
    try {
      await api('/projects', { method: 'POST', body: {
        name: form.name,
        owner_manager_id: form.owner_manager_id || undefined,
        start_date: form.start_date || undefined,
        end_date: form.end_date || undefined,
      }});
      setForm({ name: '', owner_manager_id: '', start_date: '', end_date: '' });
      setShowForm(false);
      load();
    } catch (e) { onError(e.message); }
  };

  const remove = async (id) => {
    if (!window.confirm('Delete this project and all its tasks?')) return;
    try { await api(`/projects/${id}`, { method: 'DELETE' }); load(); }
    catch (e) { onError(e.message); }
  };

  return (
    <div>
      {isManager && (
        <div style={{ marginBottom: 16 }}>
          <button style={btn()} onClick={() => setShowForm(s => !s)}>
            {showForm ? 'Cancel' : '+ New Project'}
          </button>
        </div>
      )}

      {showForm && isManager && (
        <form onSubmit={create} style={{ ...card, marginBottom: 20, display: 'grid', gap: 12, maxWidth: 560 }}>
          <label>Project name
            <input style={inp} value={form.name} required
              onChange={e => setForm({ ...form, name: e.target.value })} />
          </label>
          {isSuperadmin && (
            <label>Engagement manager (owner)
              <select style={inp} value={form.owner_manager_id}
                onChange={e => setForm({ ...form, owner_manager_id: e.target.value })}>
                <option value="">— me (creator) —</option>
                {managers.map(m => <option key={m.id} value={m.id}>{m.full_name}</option>)}
              </select>
            </label>
          )}
          <div style={{ display: 'flex', gap: 12 }}>
            <label style={{ flex: 1 }}>Start date
              <input type="date" style={inp} value={form.start_date}
                onChange={e => setForm({ ...form, start_date: e.target.value })} />
            </label>
            <label style={{ flex: 1 }}>End date
              <input type="date" style={inp} value={form.end_date}
                onChange={e => setForm({ ...form, end_date: e.target.value })} />
            </label>
          </div>
          <button type="submit" style={btn(C.success)}>Create project</button>
        </form>
      )}

      <div style={{ display: 'grid', gap: 12 }}>
        {projects.length === 0 && <p style={{ color: C.muted }}>No projects yet.</p>}
        {projects.map(p => (
          <div key={p.id} style={card}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start' }}>
              <div>
                <span style={{ fontFamily: 'monospace', background: C.pill, padding: '3px 10px', borderRadius: 20, color: C.primary, fontWeight: 700, border: `1px solid ${C.borderSoft}`, fontSize: 12 }}>{p.code}</span>
                <h3 style={{ margin: '8px 0 4px', color: C.primary }}>{p.name}</h3>
                <div style={{ color: C.muted, fontSize: 13 }}>
                  Manager: {p.owner_manager_name || '—'} &nbsp;·&nbsp;
                  {p.start_date || '?'} → {p.end_date || '?'} &nbsp;·&nbsp; {p.status}
                </div>
              </div>
              {isManager && (
                <button style={btn(C.danger)} onClick={() => remove(p.id)}>Delete</button>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Tasks ──────────────────────────────────────────────────────────────────────
function TasksTab({ isManager, role, onError }) {
  const api = useApi();
  const [tasks, setTasks] = useState([]);
  const [projects, setProjects] = useState([]);
  const [assignable, setAssignable] = useState([]);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ project_id: '', title: '', description: '', assignee_id: '', start_date: '', end_date: '' });
  const isClient = ['client_sponsor', 'client_it_lead', 'client_operations'].includes(role);
  const myId = JSON.parse(localStorage.getItem('bss_auth') || '{}')?.user?.id;

  const load = useCallback(async () => {
    try { setTasks(await api('/tasks')); }
    catch (e) { onError(e.message); }
  }, [api, onError]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    if (!isManager) return;
    api('/projects').then(setProjects).catch(() => {});
    api('/users/assignable').then(setAssignable).catch(() => {});
  }, [api, isManager]);

  const create = async (e) => {
    e.preventDefault();
    try {
      await api('/tasks', { method: 'POST', body: {
        project_id: Number(form.project_id),
        title: form.title,
        description: form.description || undefined,
        assignee_id: form.assignee_id ? Number(form.assignee_id) : undefined,
        start_date: form.start_date || undefined,
        end_date: form.end_date || undefined,
      }});
      setForm({ project_id: '', title: '', description: '', assignee_id: '', start_date: '', end_date: '' });
      setShowForm(false);
      load();
    } catch (e) { onError(e.message); }
  };

  const changeStatus = async (taskId, status) => {
    try { await api(`/tasks/${taskId}/status`, { method: 'PATCH', body: { status } }); load(); }
    catch (e) { onError(e.message); }
  };

  // Project dropdown: selecting a name autofills the code (shown beside the select).
  const selectedProject = projects.find(p => p.id === Number(form.project_id));

  return (
    <div>
      {isManager && (
        <div style={{ marginBottom: 16 }}>
          <button style={btn()} onClick={() => setShowForm(s => !s)}>
            {showForm ? 'Cancel' : '+ Assign New Task'}
          </button>
        </div>
      )}

      {showForm && isManager && (
        <form onSubmit={create} style={{ ...card, marginBottom: 20, display: 'grid', gap: 12, maxWidth: 600 }}>
          <label>Project
            <select style={inp} value={form.project_id} required
              onChange={e => setForm({ ...form, project_id: e.target.value })}>
              <option value="">— select project —</option>
              {projects.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
            </select>
          </label>
          {selectedProject && (
            <div style={{ fontSize: 13, color: C.muted }}>
              Project ID: <strong style={{ fontFamily: 'monospace', color: C.primary }}>{selectedProject.code}</strong>
              {' '}(task ID auto-generated on save)
            </div>
          )}
          <label>Task title
            <input style={inp} value={form.title} required
              onChange={e => setForm({ ...form, title: e.target.value })} />
          </label>
          <label>Description
            <textarea style={{ ...inp, minHeight: 60 }} value={form.description}
              onChange={e => setForm({ ...form, description: e.target.value })} />
          </label>
          <label>Assign to (below your hierarchy only)
            <select style={inp} value={form.assignee_id}
              onChange={e => setForm({ ...form, assignee_id: e.target.value })}>
              <option value="">— unassigned —</option>
              {assignable.map(u => (
                <option key={u.id} value={u.id}>{u.full_name} ({u.role})</option>
              ))}
            </select>
          </label>
          <div style={{ display: 'flex', gap: 12 }}>
            <label style={{ flex: 1 }}>Start date
              <input type="date" style={inp} value={form.start_date}
                onChange={e => setForm({ ...form, start_date: e.target.value })} />
            </label>
            <label style={{ flex: 1 }}>End date
              <input type="date" style={inp} value={form.end_date}
                onChange={e => setForm({ ...form, end_date: e.target.value })} />
            </label>
          </div>
          <button type="submit" style={btn(C.success)}>Create task</button>
        </form>
      )}

      <div style={{ display: 'grid', gap: 12 }}>
        {tasks.length === 0 && <p style={{ color: C.muted }}>No tasks to show.</p>}
        {tasks.map(t => {
          const isMine = t.assignee_id === myId;
          const canEditStatus = !isClient && (isManager || isMine);
          return (
            <div key={t.id} style={card}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
                <div style={{ flex: 1, minWidth: 240 }}>
                  <span style={{ fontFamily: 'monospace', background: C.pill, padding: '3px 10px', borderRadius: 20, color: C.primary, fontWeight: 700, border: `1px solid ${C.borderSoft}`, fontSize: 12 }}>{t.code}</span>
                  <h3 style={{ margin: '8px 0 4px', color: C.primary }}>{t.title}</h3>
                  {t.description && <p style={{ color: C.muted, margin: '4px 0' }}>{t.description}</p>}
                  <div style={{ color: C.muted, fontSize: 13 }}>
                    {t.project_name} ({t.project_code}) &nbsp;·&nbsp;
                    Assignee: {t.assignee_name || '—'} &nbsp;·&nbsp;
                    {t.start_date || '?'} → {t.end_date || '?'}
                  </div>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8, alignItems: 'flex-end' }}>
                  {canEditStatus ? (
                    <select value={t.status} onChange={e => changeStatus(t.id, e.target.value)}
                      style={{ ...inp, width: 150, color: STATUS_COLOR[t.status], fontWeight: 600 }}>
                      {STATUS_OPTS.map(s => <option key={s} value={s}>{STATUS_LABEL[s]}</option>)}
                    </select>
                  ) : (
                    <span style={{ color: STATUS_COLOR[t.status], fontWeight: 700 }}>{STATUS_LABEL[t.status]}</span>
                  )}
                </div>
              </div>
              <TaskComments taskId={t.id} canComment={!isClient && (isManager || isMine)} onError={onError} />
            </div>
          );
        })}
      </div>
    </div>
  );
}

function TaskComments({ taskId, canComment, onError }) {
  const api = useApi();
  const [open, setOpen] = useState(false);
  const [comments, setComments] = useState([]);
  const [body, setBody] = useState('');

  const load = useCallback(async () => {
    try { setComments(await api(`/tasks/${taskId}/comments`)); }
    catch (e) { onError(e.message); }
  }, [api, taskId, onError]);

  useEffect(() => { if (open) load(); }, [open, load]);

  const submit = async (e) => {
    e.preventDefault();
    if (!body.trim()) return;
    try { await api(`/tasks/${taskId}/comments`, { method: 'POST', body: { body } }); setBody(''); load(); }
    catch (e) { onError(e.message); }
  };

  return (
    <div style={{ marginTop: 12, borderTop: `1px solid ${C.border}`, paddingTop: 10 }}>
      <button onClick={() => setOpen(o => !o)} style={{ background: 'none', border: 'none', color: C.primary, cursor: 'pointer', fontSize: 13, fontWeight: 600, padding: 0 }}>
        {open ? 'Hide comments' : 'Show / add comments'}
      </button>
      {open && (
        <div style={{ marginTop: 10 }}>
          {comments.length === 0 && <p style={{ color: C.muted, fontSize: 13 }}>No comments yet.</p>}
          {comments.map(c => (
            <div key={c.id} style={{ fontSize: 13, marginBottom: 6, background: C.pill, padding: '6px 10px', borderRadius: 6 }}>
              <strong>{c.author_name}</strong> <span style={{ color: C.muted }}>({c.author_role})</span>: {c.body}
            </div>
          ))}
          {canComment && (
            <form onSubmit={submit} style={{ display: 'flex', gap: 8, marginTop: 8 }}>
              <input style={inp} placeholder="Add a comment…" value={body} onChange={e => setBody(e.target.value)} />
              <button type="submit" style={btn()}>Post</button>
            </form>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Users (superadmin only) ────────────────────────────────────────────────────
function UsersTab({ onError }) {
  const api = useApi();
  const [users, setUsers] = useState([]);
  const [managers, setManagers] = useState([]);
  const [showForm, setShowForm] = useState(false);
  const [editing, setEditing] = useState(null);
  const emptyForm = { username: '', full_name: '', email: '', role: 'bss_consultant', organisation: 'KPMG Advisory', password: '', manager_id: '' };
  const [form, setForm] = useState(emptyForm);

  const load = useCallback(async () => {
    try { setUsers(await api('/admin/users')); }
    catch (e) { onError(e.message); }
  }, [api, onError]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { api('/admin/managers').then(setManagers).catch(() => {}); }, [api]);

  const create = async (e) => {
    e.preventDefault();
    try {
      const body = { ...form };
      // manager_id only applies to worker roles; drop it otherwise.
      if (!WORKER_ROLES_FE.includes(body.role) || !body.manager_id) delete body.manager_id;
      await api('/admin/users', { method: 'POST', body });
      setForm(emptyForm);
      setShowForm(false);
      load();
    } catch (e) { onError(e.message); }
  };

  const saveEdit = async (e) => {
    e.preventDefault();
    const { id, ...body } = editing;
    if (!body.password) delete body.password;   // only reset if filled
    try { await api(`/admin/users/${id}`, { method: 'PUT', body }); setEditing(null); load(); }
    catch (e) { onError(e.message); }
  };

  const toggleActive = async (u) => {
    try { await api(`/admin/users/${u.id}`, { method: 'PUT', body: { is_active: !u.is_active } }); load(); }
    catch (e) { onError(e.message); }
  };

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <button style={btn()} onClick={() => setShowForm(s => !s)}>{showForm ? 'Cancel' : '+ Add User'}</button>
      </div>

      {showForm && (
        <form onSubmit={create} style={{ ...card, marginBottom: 20, display: 'grid', gap: 12, maxWidth: 560 }}>
          <label>Username<input style={inp} value={form.username} required onChange={e => setForm({ ...form, username: e.target.value })} /></label>
          <label>Full name<input style={inp} value={form.full_name} required onChange={e => setForm({ ...form, full_name: e.target.value })} /></label>
          <label>Email<input style={inp} type="email" value={form.email} onChange={e => setForm({ ...form, email: e.target.value })} /></label>
          <label>Role
            <select style={inp} value={form.role} onChange={e => setForm({ ...form, role: e.target.value })}>
              {ROLES.map(r => <option key={r} value={r}>{r}</option>)}
            </select>
          </label>
          {WORKER_ROLES_FE.includes(form.role) && (
            <label>Reports to (engagement manager)
              <select style={inp} value={form.manager_id} onChange={e => setForm({ ...form, manager_id: e.target.value })}>
                <option value="">— none yet —</option>
                {managers.map(m => <option key={m.id} value={m.id}>{m.full_name}</option>)}
              </select>
            </label>
          )}
          <label>Organisation<input style={inp} value={form.organisation} onChange={e => setForm({ ...form, organisation: e.target.value })} /></label>
          <label>Password<input style={inp} type="password" value={form.password} required onChange={e => setForm({ ...form, password: e.target.value })} /></label>
          <button type="submit" style={btn(C.success)}>Create user</button>
        </form>
      )}

      <div style={{ ...card, overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
          <thead>
            <tr style={{ textAlign: 'left', background: C.pill }}>
              {['Username', 'Full name', 'Role', 'Reports To', 'Organisation', 'Active', 'Actions'].map(h => (
                <th key={h} style={{ padding: '10px 12px', color: C.primary, fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {users.map(u => (
              <tr key={u.id} style={{ borderBottom: `1px solid ${C.border}` }}>
                <td style={{ padding: '8px 10px' }}>{u.username}</td>
                <td style={{ padding: '8px 10px' }}>{u.full_name}</td>
                <td style={{ padding: '8px 10px' }}>{u.role}</td>
                <td style={{ padding: '8px 10px', color: u.manager_name ? C.text : C.muted }}>{u.manager_name || '—'}</td>
                <td style={{ padding: '8px 10px' }}>{u.organisation}</td>
                <td style={{ padding: '8px 10px' }}>
                  <span style={{ color: u.is_active ? C.success : C.danger, fontWeight: 600 }}>
                    {u.is_active ? 'Active' : 'Inactive'}
                  </span>
                </td>
                <td style={{ padding: '8px 10px', display: 'flex', gap: 6 }}>
                  <button style={{ ...btn(), padding: '4px 10px', fontSize: 13 }} onClick={() => setEditing({ id: u.id, full_name: u.full_name, email: u.email || '', role: u.role, organisation: u.organisation, is_active: u.is_active, manager_id: u.manager_id || '', password: '' })}>Edit</button>
                  <button style={{ ...btn(u.is_active ? C.danger : C.success), padding: '4px 10px', fontSize: 13 }} onClick={() => toggleActive(u)}>
                    {u.is_active ? 'Deactivate' : 'Activate'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {editing && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
          <form onSubmit={saveEdit} style={{ ...card, width: 480, display: 'grid', gap: 12 }}>
            <h3 style={{ color: C.primary, margin: 0 }}>Edit user</h3>
            <label>Full name<input style={inp} value={editing.full_name} onChange={e => setEditing({ ...editing, full_name: e.target.value })} /></label>
            <label>Email<input style={inp} value={editing.email} onChange={e => setEditing({ ...editing, email: e.target.value })} /></label>
            <label>Role
              <select style={inp} value={editing.role} onChange={e => setEditing({ ...editing, role: e.target.value })}>
                {ROLES.map(r => <option key={r} value={r}>{r}</option>)}
              </select>
            </label>
            {WORKER_ROLES_FE.includes(editing.role) && (
              <label>Reports to (engagement manager)
                <select style={inp} value={editing.manager_id} onChange={e => setEditing({ ...editing, manager_id: e.target.value })}>
                  <option value="">— none —</option>
                  {managers.map(m => <option key={m.id} value={m.id}>{m.full_name}</option>)}
                </select>
              </label>
            )}
            <label>Organisation<input style={inp} value={editing.organisation} onChange={e => setEditing({ ...editing, organisation: e.target.value })} /></label>
            <label>Reset password (leave blank to keep)<input style={inp} type="password" value={editing.password} onChange={e => setEditing({ ...editing, password: e.target.value })} /></label>
            <div style={{ display: 'flex', gap: 10 }}>
              <button type="submit" style={btn(C.success)}>Save</button>
              <button type="button" style={btn(C.neutral)} onClick={() => setEditing(null)}>Cancel</button>
            </div>
          </form>
        </div>
      )}
    </div>
  );
}
