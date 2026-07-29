import React, { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../context/AuthContext';
import { colors, font } from '../theme';

// ─── API helper (uses the JWT from AuthContext) ─────────────────────────────────
function useApi() {
  const { auth, logout } = useAuth();
  const token = auth?.token;
  const api = useCallback(async (path, { method = 'GET', body } = {}) => {
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

  const upload = useCallback(async (path, formData) => {
    const res = await fetch(`/api/bpm${path}`, {
      method: 'POST',
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      body: formData,
    });
    if (res.status === 401) { logout(); throw new Error('Session expired'); }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `Upload failed (${res.status})`);
    return data;
  }, [token, logout]);

  return { api, upload };
}

// ─── Date helper — render ISO/date strings as DD Mon YYYY, never raw GMT strings ──
function fmtDate(value) {
  if (!value) return '—';
  const d = new Date(value);
  if (isNaN(d.getTime())) return '—';
  return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
}

// ─── Design tokens — shared KPMG theme (navy / blue / white / gray) ─────────────
const FONT = font.family;
const C = {
  primary: colors.navy,
  accent: colors.kpmgBlueLight,
  border: colors.border,
  borderSoft: colors.gray300,
  page: colors.bgPage,
  surface: colors.bgCard,
  pill: colors.gray100,
  heading: colors.navy,
  text: colors.gray900,
  muted: colors.gray500,
  faint: colors.gray300,
  shadow: '0 1px 3px rgba(0,31,91,0.06)',
  success: colors.green,
  danger: colors.red,
  neutral: colors.gray500,
};
const STATUS_OPTS = ['todo', 'in_progress', 'blocked', 'done'];
const STATUS_LABEL = { todo: 'To Do', in_progress: 'In Progress', blocked: 'Blocked', done: 'Done' };
const STATUS_COLOR = { todo: colors.gray500, in_progress: colors.kpmgBlueLight, blocked: colors.red, done: colors.green };
const PROJECT_STATUS_COLOR = { active: colors.kpmgBlueLight, completed: colors.green, on_hold: colors.amber };
const ROLES = [
  'programme_director', 'engagement_manager', 'bss_consultant', 'qa_manager',
  'data_analyst', 'client_sponsor', 'client_it_lead', 'client_operations',
];
const WORKER_ROLES_FE = ['bss_consultant', 'qa_manager', 'data_analyst'];

// Primary button uses solid KPMG blue; pass a different colour for variants.
const btn = (bg) => ({
  background: bg || colors.kpmgBlue, color: colors.white, border: 'none', padding: '9px 18px',
  borderRadius: 8, cursor: 'pointer', fontSize: 13, fontWeight: 600, fontFamily: FONT,
});
const inp = {
  padding: '9px 12px', border: `1px solid ${colors.border}`, borderRadius: 8,
  fontSize: 14, width: '100%', boxSizing: 'border-box', fontFamily: FONT,
  background: colors.white, color: C.text,
};
const label = { fontSize: 13, fontWeight: 600, color: colors.gray700, display: 'block', marginBottom: 6 };
const card = {
  background: C.surface, padding: '1.5rem', borderRadius: 10,
  border: `1px solid ${C.border}`,
};
const badge = {
  fontFamily: 'monospace', background: colors.gray100, padding: '3px 10px', borderRadius: 6,
  color: colors.navy, fontWeight: 700, border: `1px solid ${colors.border}`, fontSize: 12,
};
const statusPill = (color) => ({
  display: 'inline-flex', alignItems: 'center', gap: 6, padding: '4px 12px', borderRadius: 20,
  fontSize: 12, fontWeight: 700, color, background: `${color}1A`,
});

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
    <div style={{ minHeight: '100vh', padding: '24px 28px', fontFamily: FONT, color: C.text, background: C.page }}>
      {/* Header — centred title, consistent with the rest of the platform */}
      <div style={{ textAlign: 'center', marginBottom: 28 }}>
        <h1 style={{ fontSize: 28, fontWeight: 700, margin: 0, color: colors.navy }}>BPM Dashboard</h1>
        <p style={{ color: colors.gray500, fontSize: 13, margin: '4px 0 0' }}>
          Project, task &amp; user management — your access: <strong style={{ color: colors.navy }}>{(role || 'unknown').replace(/_/g, ' ')}</strong>
        </p>
      </div>

      <div style={{ maxWidth: 1100, margin: '0 auto' }}>
        <div style={{ display: 'flex', gap: 4, marginBottom: 20, borderBottom: `1px solid ${C.border}` }}>
          {tabs.map(t => (
            <button key={t.key} onClick={() => { setTab(t.key); setErr(''); }}
              style={{
                background: 'none', border: 'none', cursor: 'pointer', padding: '10px 18px',
                fontSize: 14, fontWeight: 600, fontFamily: FONT,
                color: tab === t.key ? C.primary : C.muted,
                borderBottom: tab === t.key ? `3px solid ${colors.kpmgBlueLight}` : '3px solid transparent',
                marginBottom: -1, transition: 'color 0.15s',
              }}>
              {t.label}
            </button>
          ))}
        </div>

        {err && <div style={{ background: colors.redLight, color: colors.red, border: `1px solid ${colors.red}`, padding: '10px 14px', borderRadius: 8, marginBottom: 16, fontSize: 13 }}>{err}</div>}

        {tab === 'projects' && <ProjectsTab isManager={isManager} isSuperadmin={isSuperadmin} onError={setErr} />}
        {tab === 'tasks' && <TasksTab isManager={isManager} role={role} onError={setErr} />}
        {tab === 'users' && isSuperadmin && <UsersTab onError={setErr} />}
      </div>
    </div>
  );
}

// ─── Projects ───────────────────────────────────────────────────────────────────
function ProjectsTab({ isManager, isSuperadmin, onError }) {
  const { api, upload } = useApi();
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
        <form onSubmit={create} style={{ ...card, marginBottom: 20, display: 'grid', gap: 16, maxWidth: 560 }}>
          <h3 style={{ margin: 0, fontSize: 15, color: C.primary }}>New project</h3>
          <div>
            <span style={label}>Project name</span>
            <input style={inp} value={form.name} required
              onChange={e => setForm({ ...form, name: e.target.value })} />
          </div>
          {isSuperadmin && (
            <div>
              <span style={label}>Engagement manager (owner)</span>
              <select style={inp} value={form.owner_manager_id}
                onChange={e => setForm({ ...form, owner_manager_id: e.target.value })}>
                <option value="">— me (creator) —</option>
                {managers.map(m => <option key={m.id} value={m.id}>{m.full_name}</option>)}
              </select>
            </div>
          )}
          <div style={{ display: 'flex', gap: 16 }}>
            <div style={{ flex: 1 }}>
              <span style={label}>Start date</span>
              <input type="date" style={inp} value={form.start_date}
                onChange={e => setForm({ ...form, start_date: e.target.value })} />
            </div>
            <div style={{ flex: 1 }}>
              <span style={label}>End date</span>
              <input type="date" style={inp} value={form.end_date}
                onChange={e => setForm({ ...form, end_date: e.target.value })} />
            </div>
          </div>
          <div style={{ display: 'flex', gap: 10 }}>
            <button type="submit" style={btn(C.success)}>Create project</button>
            <button type="button" style={btn(C.neutral)} onClick={() => setShowForm(false)}>Cancel</button>
          </div>
        </form>
      )}

      <div style={{ display: 'grid', gap: 12 }}>
        {projects.length === 0 && <p style={{ color: C.muted, fontSize: 14 }}>No projects yet.</p>}
        {projects.map(p => (
          <div key={p.id} style={card}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'start', gap: 16 }}>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
                  <span style={badge}>{p.code}</span>
                  <span style={statusPill(PROJECT_STATUS_COLOR[p.status] || colors.gray500)}>
                    {(p.status || 'unknown').replace(/_/g, ' ')}
                  </span>
                </div>
                <h3 style={{ margin: '0 0 4px', fontSize: 16, color: C.primary }}>{p.name}</h3>
                <div style={{ color: C.muted, fontSize: 13 }}>
                  Manager: {p.owner_manager_name || '—'} &nbsp;·&nbsp;
                  {fmtDate(p.start_date)} → {fmtDate(p.end_date)}
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
  const { api, upload } = useApi();
  const [tasks, setTasks] = useState([]);
  const [projects, setProjects] = useState([]);
  const [assignable, setAssignable] = useState([]);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ project_id: '', title: '', description: '', assignee_id: '', start_date: '', end_date: '', recurrence_type: 'none', recurrence_days: [] });
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
        recurrence_type: form.recurrence_type || 'none',
        recurrence_days: form.recurrence_type === 'custom' ? form.recurrence_days.join(',') : undefined,
      }});
      setForm({ project_id: '', title: '', description: '', assignee_id: '', start_date: '', end_date: '', recurrence_type: 'none', recurrence_days: [] });
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
        <form onSubmit={create} style={{ ...card, marginBottom: 20, display: 'grid', gap: 16, maxWidth: 600 }}>
          <h3 style={{ margin: 0, fontSize: 15, color: C.primary }}>Assign new task</h3>
          <div>
            <span style={label}>Project</span>
            <select style={inp} value={form.project_id} required
              onChange={e => setForm({ ...form, project_id: e.target.value })}>
              <option value="">— select project —</option>
              {projects.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
            </select>
            {selectedProject && (
              <div style={{ fontSize: 12, color: C.muted, marginTop: 6 }}>
                Project ID: <strong style={{ fontFamily: 'monospace', color: C.primary }}>{selectedProject.code}</strong>
                {' '}(task ID auto-generated on save)
              </div>
            )}
          </div>
          <div>
            <span style={label}>Task title</span>
            <input style={inp} value={form.title} required
              onChange={e => setForm({ ...form, title: e.target.value })} />
          </div>
          <div>
            <span style={label}>Description</span>
            <textarea style={{ ...inp, minHeight: 60 }} value={form.description}
              onChange={e => setForm({ ...form, description: e.target.value })} />
          </div>
          <div>
            <span style={label}>Assign to (below your hierarchy only)</span>
            <select style={inp} value={form.assignee_id}
              onChange={e => setForm({ ...form, assignee_id: e.target.value })}>
              <option value="">— unassigned —</option>
              {assignable.map(u => (
                <option key={u.id} value={u.id}>{u.full_name} ({u.role})</option>
              ))}
            </select>
          </div>
          <div style={{ display: 'flex', gap: 16 }}>
            <div style={{ flex: 1 }}>
              <span style={label}>Start date</span>
              <input type="date" style={inp} value={form.start_date}
                onChange={e => setForm({ ...form, start_date: e.target.value })} />
            </div>
            <div style={{ flex: 1 }}>
              <span style={label}>End date</span>
              <input type="date" style={inp} value={form.end_date}
                onChange={e => setForm({ ...form, end_date: e.target.value })} />
            </div>
          </div>
          <div>
            <span style={label}>Recurrence</span>
            <select style={inp} value={form.recurrence_type}
              onChange={e => setForm({ ...form, recurrence_type: e.target.value, recurrence_days: [] })}>
              <option value="none">None (one-time)</option>
              <option value="daily">Daily</option>
              <option value="weekly">Weekly</option>
              <option value="custom">Custom days</option>
            </select>
          </div>
          {form.recurrence_type === 'custom' && (
            <div>
              <span style={label}>Select days</span>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map(day => {
                  const active = form.recurrence_days.includes(day.toLowerCase());
                  return (
                    <button key={day} type="button"
                      onClick={() => {
                        const d = day.toLowerCase();
                        setForm(f => ({
                          ...f,
                          recurrence_days: active
                            ? f.recurrence_days.filter(x => x !== d)
                            : [...f.recurrence_days, d],
                        }));
                      }}
                      style={{
                        padding: '6px 14px', borderRadius: 20, fontSize: 13, fontWeight: 600,
                        cursor: 'pointer', fontFamily: FONT,
                        border: `2px solid ${active ? colors.kpmgBlue : C.border}`,
                        background: active ? colors.kpmgBlue : C.surface,
                        color: active ? '#fff' : C.text,
                      }}>
                      {day}
                    </button>
                  );
                })}
              </div>
            </div>
          )}
          <div style={{ display: 'flex', gap: 10 }}>
            <button type="submit" style={btn(C.success)}>Create task</button>
            <button type="button" style={btn(C.neutral)} onClick={() => setShowForm(false)}>Cancel</button>
          </div>
        </form>
      )}

      <div style={{ display: 'grid', gap: 12 }}>
        {tasks.length === 0 && <p style={{ color: C.muted, fontSize: 14 }}>No tasks to show.</p>}
        {tasks.map(t => {
          const isMine = t.assignee_id === myId;
          const canEditStatus = !isClient && (isManager || isMine);
          return (
            <div key={t.id} style={card}>
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, flexWrap: 'wrap' }}>
                <div style={{ flex: 1, minWidth: 240 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                    <span style={badge}>{t.code}</span>
                    {t.recurrence_type && t.recurrence_type !== 'none' && (
                      <span style={{
                        display: 'inline-flex', alignItems: 'center', gap: 4,
                        padding: '3px 10px', borderRadius: 12, fontSize: 11, fontWeight: 700,
                        background: `${colors.amber}1A`, color: colors.amber,
                      }}>
                        &#x1f503; {t.recurrence_type === 'daily' ? 'Daily' : t.recurrence_type === 'weekly' ? 'Weekly' : t.recurrence_days?.replace(/,/g, ', ')}
                      </span>
                    )}
                  </div>
                  <h3 style={{ margin: '8px 0 4px', fontSize: 16, color: C.primary }}>{t.title}</h3>
                  {t.description && <p style={{ color: C.muted, margin: '4px 0', fontSize: 13 }}>{t.description}</p>}
                  <div style={{ color: C.muted, fontSize: 13 }}>
                    {t.project_name} ({t.project_code}) &nbsp;·&nbsp;
                    Assignee: {t.assignee_name || '—'} &nbsp;·&nbsp;
                    {fmtDate(t.start_date)} → {fmtDate(t.end_date)}
                  </div>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8, alignItems: 'flex-end' }}>
                  {canEditStatus ? (
                    <select value={t.status} onChange={e => changeStatus(t.id, e.target.value)}
                      style={{ ...inp, width: 150, color: STATUS_COLOR[t.status], fontWeight: 600 }}>
                      {STATUS_OPTS.map(s => <option key={s} value={s}>{STATUS_LABEL[s]}</option>)}
                    </select>
                  ) : (
                    <span style={statusPill(STATUS_COLOR[t.status])}>{STATUS_LABEL[t.status]}</span>
                  )}
                </div>
              </div>
              <TaskComments taskId={t.id} canComment={!isClient && (isManager || isMine)} onError={onError} />
              <TaskAttachments taskId={t.id} canUpload={!isClient && (isManager || isMine)} onError={onError} />
            </div>
          );
        })}
      </div>
    </div>
  );
}

function TaskComments({ taskId, canComment, onError }) {
  const { api, upload } = useApi();
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

function TaskAttachments({ taskId, canUpload, onError }) {
  const { api, upload } = useApi();
  const [open, setOpen] = useState(false);
  const [files, setFiles] = useState([]);
  const [uploading, setUploading] = useState(false);

  const load = useCallback(async () => {
    try { setFiles(await api(`/tasks/${taskId}/attachments`)); }
    catch (e) { onError(e.message); }
  }, [api, taskId, onError]);

  useEffect(() => { if (open) load(); }, [open, load]);

  const handleUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      const fd = new FormData();
      fd.append('file', file);
      await upload(`/tasks/${taskId}/attachments`, fd);
      load();
    } catch (err) { onError(err.message); }
    finally { setUploading(false); e.target.value = ''; }
  };

  const handleDelete = async (id) => {
    if (!window.confirm('Delete this attachment?')) return;
    try { await api(`/attachments/${id}`, { method: 'DELETE' }); load(); }
    catch (err) { onError(err.message); }
  };

  return (
    <div style={{ marginTop: 8, borderTop: `1px solid ${C.border}`, paddingTop: 10 }}>
      <button onClick={() => setOpen(o => !o)} style={{ background: 'none', border: 'none', color: C.primary, cursor: 'pointer', fontSize: 13, fontWeight: 600, padding: 0 }}>
        {open ? 'Hide attachments' : `Attachments${files.length ? ` (${files.length})` : ''}`}
      </button>
      {open && (
        <div style={{ marginTop: 10 }}>
          {files.length === 0 && <p style={{ color: C.muted, fontSize: 13 }}>No attachments yet.</p>}
          {files.map(f => (
            <div key={f.id} style={{
              display: 'flex', alignItems: 'center', gap: 10, fontSize: 13,
              marginBottom: 6, background: C.pill, padding: '6px 10px', borderRadius: 6,
            }}>
              <span style={{ flex: 1 }}>
                <a href={`/api/bpm/attachments/${f.id}/download`}
                  style={{ color: C.primary, fontWeight: 600, textDecoration: 'none' }}
                  target="_blank" rel="noopener noreferrer">
                  {f.file_name}
                </a>
                <span style={{ color: C.muted, marginLeft: 8 }}>by {f.uploader_name} · {fmtDate(f.created_at)}</span>
              </span>
              {canUpload && (
                <button onClick={() => handleDelete(f.id)}
                  style={{ ...btn(C.danger), padding: '3px 10px', fontSize: 11 }}>
                  Delete
                </button>
              )}
            </div>
          ))}
          {canUpload && (
            <div style={{ marginTop: 8 }}>
              <label style={{
                ...btn(), display: 'inline-block', cursor: uploading ? 'not-allowed' : 'pointer',
                opacity: uploading ? 0.6 : 1,
              }}>
                {uploading ? 'Uploading…' : 'Upload file'}
                <input type="file" style={{ display: 'none' }} onChange={handleUpload} disabled={uploading} />
              </label>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Users (superadmin only) ────────────────────────────────────────────────────
function UsersTab({ onError }) {
  const { api, upload } = useApi();
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
        <form onSubmit={create} style={{ ...card, marginBottom: 20, display: 'grid', gap: 16, maxWidth: 560 }}>
          <h3 style={{ margin: 0, fontSize: 15, color: C.primary }}>Add user</h3>
          <div><span style={label}>Username</span><input style={inp} value={form.username} required onChange={e => setForm({ ...form, username: e.target.value })} /></div>
          <div><span style={label}>Full name</span><input style={inp} value={form.full_name} required onChange={e => setForm({ ...form, full_name: e.target.value })} /></div>
          <div><span style={label}>Email</span><input style={inp} type="email" value={form.email} onChange={e => setForm({ ...form, email: e.target.value })} /></div>
          <div>
            <span style={label}>Role</span>
            <select style={inp} value={form.role} onChange={e => setForm({ ...form, role: e.target.value })}>
              {ROLES.map(r => <option key={r} value={r}>{r.replace(/_/g, ' ')}</option>)}
            </select>
          </div>
          {WORKER_ROLES_FE.includes(form.role) && (
            <div>
              <span style={label}>Reports to (engagement manager)</span>
              <select style={inp} value={form.manager_id} onChange={e => setForm({ ...form, manager_id: e.target.value })}>
                <option value="">— none yet —</option>
                {managers.map(m => <option key={m.id} value={m.id}>{m.full_name}</option>)}
              </select>
            </div>
          )}
          <div><span style={label}>Organisation</span><input style={inp} value={form.organisation} onChange={e => setForm({ ...form, organisation: e.target.value })} /></div>
          <div><span style={label}>Password</span><input style={inp} type="password" value={form.password} required onChange={e => setForm({ ...form, password: e.target.value })} /></div>
          <div style={{ display: 'flex', gap: 10 }}>
            <button type="submit" style={btn(C.success)}>Create user</button>
            <button type="button" style={btn(C.neutral)} onClick={() => setShowForm(false)}>Cancel</button>
          </div>
        </form>
      )}

      <div style={{ ...card, overflowX: 'auto', padding: 0 }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
          <thead>
            <tr style={{ textAlign: 'left', background: colors.gray100 }}>
              {['Username', 'Full name', 'Role', 'Reports To', 'Organisation', 'Active', 'Actions'].map(h => (
                <th key={h} style={{ padding: '12px 14px', color: C.primary, fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {users.map(u => (
              <tr key={u.id} style={{ borderBottom: `1px solid ${C.border}` }}>
                <td style={{ padding: '10px 14px' }}>{u.username}</td>
                <td style={{ padding: '10px 14px' }}>{u.full_name}</td>
                <td style={{ padding: '10px 14px', textTransform: 'capitalize' }}>{u.role.replace(/_/g, ' ')}</td>
                <td style={{ padding: '10px 14px', color: u.manager_name ? C.text : C.muted }}>{u.manager_name || '—'}</td>
                <td style={{ padding: '10px 14px' }}>{u.organisation}</td>
                <td style={{ padding: '10px 14px' }}>
                  <span style={statusPill(u.is_active ? colors.green : colors.red)}>
                    {u.is_active ? 'Active' : 'Inactive'}
                  </span>
                </td>
                <td style={{ padding: '10px 14px', display: 'flex', gap: 6 }}>
                  <button style={{ ...btn(), padding: '5px 12px', fontSize: 13 }} onClick={() => setEditing({ id: u.id, full_name: u.full_name, email: u.email || '', role: u.role, organisation: u.organisation, is_active: u.is_active, manager_id: u.manager_id || '', password: '' })}>Edit</button>
                  <button style={{ ...btn(u.is_active ? C.danger : C.success), padding: '5px 12px', fontSize: 13 }} onClick={() => toggleActive(u)}>
                    {u.is_active ? 'Deactivate' : 'Activate'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {editing && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,31,91,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
          <form onSubmit={saveEdit} style={{ ...card, width: 480, display: 'grid', gap: 16 }}>
            <h3 style={{ color: C.primary, margin: 0, fontSize: 16 }}>Edit user</h3>
            <div><span style={label}>Full name</span><input style={inp} value={editing.full_name} onChange={e => setEditing({ ...editing, full_name: e.target.value })} /></div>
            <div><span style={label}>Email</span><input style={inp} value={editing.email} onChange={e => setEditing({ ...editing, email: e.target.value })} /></div>
            <div>
              <span style={label}>Role</span>
              <select style={inp} value={editing.role} onChange={e => setEditing({ ...editing, role: e.target.value })}>
                {ROLES.map(r => <option key={r} value={r}>{r.replace(/_/g, ' ')}</option>)}
              </select>
            </div>
            {WORKER_ROLES_FE.includes(editing.role) && (
              <div>
                <span style={label}>Reports to (engagement manager)</span>
                <select style={inp} value={editing.manager_id} onChange={e => setEditing({ ...editing, manager_id: e.target.value })}>
                  <option value="">— none —</option>
                  {managers.map(m => <option key={m.id} value={m.id}>{m.full_name}</option>)}
                </select>
              </div>
            )}
            <div><span style={label}>Organisation</span><input style={inp} value={editing.organisation} onChange={e => setEditing({ ...editing, organisation: e.target.value })} /></div>
            <div><span style={label}>Reset password (leave blank to keep)</span><input style={inp} type="password" value={editing.password} onChange={e => setEditing({ ...editing, password: e.target.value })} /></div>
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
