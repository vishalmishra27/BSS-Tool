import React, { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../context/AuthContext';
import { colors, font } from '../theme';

function useApi() {
  const { auth, logout } = useAuth();
  const token = auth?.token;
  return useCallback(async (path) => {
    const res = await fetch(`/api/bpm${path}`, {
      headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    });
    if (res.status === 401) { logout(); throw new Error('Session expired'); }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `Request failed (${res.status})`);
    return data;
  }, [token, logout]);
}

const STATUS_LABEL = { todo: 'To Do', in_progress: 'In Progress', blocked: 'Blocked', done: 'Done' };
const STATUS_COLOR = { todo: colors.kpmgBlueLight, in_progress: colors.amber, blocked: colors.red, done: colors.green };

const card = {
  background: colors.bgCard,
  borderRadius: 10,
  padding: 20,
  border: `1px solid ${colors.border}`,
};

const cardLabel = { fontSize: 11, color: colors.gray500, fontWeight: 600, marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.4 };
const statRow = { display: 'flex', justifyContent: 'space-between', fontSize: 14, padding: '6px 0', borderBottom: `1px solid ${colors.borderLight}` };

const Summary = () => {
  const api = useApi();
  const [projects, setProjects] = useState([]);
  const [tasks, setTasks] = useState([]);
  const [err, setErr] = useState('');
  const [loading, setLoading] = useState(true);
  const [selectedUser, setSelectedUser] = useState(null);

  useEffect(() => {
    Promise.all([api('/projects'), api('/tasks')])
      .then(([p, t]) => { setProjects(p); setTasks(t); })
      .catch(e => setErr(e.message))
      .finally(() => setLoading(false));
  }, [api]);

  if (loading) {
    return (
      <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: colors.bgPage, color: colors.kpmgBlue, fontFamily: font.family }}>
        Loading...
      </div>
    );
  }

  // ─── Project summary ─────────────────────────────────────────────────────────
  const totalProjects = projects.length;
  const projectsInProgress = projects.filter(p => p.status === 'active').length;
  const projectsCompleted = projects.filter(p => p.status === 'completed').length;
  const projectsOnHold = projects.filter(p => p.status === 'on_hold').length;

  // ─── Task summary ─────────────────────────────────────────────────────────────
  const totalTasks = tasks.length;
  const todoTasks = tasks.filter(t => t.status === 'todo').length;
  const inProgressTasks = tasks.filter(t => t.status === 'in_progress').length;
  const blockedTasks = tasks.filter(t => t.status === 'blocked').length;
  const doneTasks = tasks.filter(t => t.status === 'done').length;
  const pendingTasks = totalTasks - doneTasks;
  const completionRate = totalTasks ? Math.round((doneTasks / totalTasks) * 100) : 0;

  // ─── User task summary — group tasks by assignee ─────────────────────────────
  const userMap = new Map();
  tasks.forEach(t => {
    if (!t.assignee_id) return;
    if (!userMap.has(t.assignee_id)) {
      userMap.set(t.assignee_id, { id: t.assignee_id, name: t.assignee_name, role: t.assignee_role, tasks: [] });
    }
    userMap.get(t.assignee_id).tasks.push(t);
  });
  const userData = Array.from(userMap.values());

  return (
    <div style={{ minHeight: '100vh', background: colors.bgPage, color: colors.gray900, fontFamily: font.family, overflowY: 'auto', padding: '24px 28px' }}>

      {/* Header */}
      <div style={{ textAlign: 'center', marginBottom: 28 }}>
        <h1 style={{ fontSize: 28, fontWeight: 700, margin: 0, color: colors.navy }}>BPM Summary</h1>
        <p style={{ color: colors.gray500, fontSize: 13, margin: '4px 0 0' }}>Live overview of projects, tasks and team workload</p>
      </div>

      {err && (
        <div style={{ background: colors.redLight, color: colors.red, border: `1px solid ${colors.red}`, padding: '10px 14px', borderRadius: 8, marginBottom: 20, fontSize: 13, maxWidth: 1200, marginLeft: 'auto', marginRight: 'auto' }}>
          {err}
        </div>
      )}

      {/* ──── Summary cards ──────────────────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 16, marginBottom: 20 }}>
        <div style={card}>
          <div style={cardLabel}>Project Summary</div>
          <div style={{ fontSize: 36, fontWeight: 800, color: colors.kpmgBlueLight, marginBottom: 8 }}>{totalProjects}</div>
          <div style={statRow}><span>In Progress</span><strong style={{ color: colors.kpmgBlueLight }}>{projectsInProgress}</strong></div>
          <div style={statRow}><span>Completed</span><strong style={{ color: colors.green }}>{projectsCompleted}</strong></div>
          <div style={{ ...statRow, borderBottom: 'none' }}><span>On Hold</span><strong style={{ color: colors.amber }}>{projectsOnHold}</strong></div>
        </div>

        <div style={card}>
          <div style={cardLabel}>Task Summary</div>
          <div style={{ fontSize: 36, fontWeight: 800, color: colors.kpmgBlueLight, marginBottom: 8 }}>{totalTasks}</div>
          <div style={statRow}><span>To Do</span><strong style={{ color: colors.gray700 }}>{todoTasks}</strong></div>
          <div style={statRow}><span>In Progress</span><strong style={{ color: colors.kpmgBlueLight }}>{inProgressTasks}</strong></div>
          <div style={statRow}><span>Blocked</span><strong style={{ color: colors.red }}>{blockedTasks}</strong></div>
          <div style={{ ...statRow, borderBottom: 'none' }}><span>Completed</span><strong style={{ color: colors.green }}>{doneTasks}</strong></div>
        </div>

        <div style={card}>
          <div style={cardLabel}>Completion Rate</div>
          <div style={{ fontSize: 36, fontWeight: 800, color: colors.green, marginBottom: 8 }}>{completionRate}%</div>
          <div style={{ display: 'flex', borderRadius: 6, overflow: 'hidden', height: 10, background: colors.gray100 }}>
            {completionRate > 0 && <div style={{ width: `${completionRate}%`, background: colors.green }} />}
          </div>
          <div style={{ fontSize: 11, color: colors.gray500, marginTop: 8 }}>{doneTasks} of {totalTasks} tasks done</div>
        </div>

        <div style={card}>
          <div style={cardLabel}>Pending Tasks</div>
          <div style={{ fontSize: 36, fontWeight: 800, color: colors.amber, marginBottom: 8 }}>{pendingTasks}</div>
          <div style={{ fontSize: 11, color: colors.gray500 }}>Not yet marked done</div>
        </div>
      </div>

      {/* ──── User task summary ──────────────────────────────────────────────── */}
      <div style={card}>
        <h2 style={{ fontSize: 16, fontWeight: 700, color: colors.navy, margin: '0 0 16px' }}>User Task Summary</h2>

        {userData.length === 0 && <p style={{ color: colors.gray500, fontSize: 13 }}>No assigned tasks yet.</p>}

        {selectedUser ? (
          <div>
            <button
              onClick={() => setSelectedUser(null)}
              style={{
                background: colors.kpmgBlue,
                color: colors.white,
                border: 'none',
                padding: '8px 16px',
                borderRadius: 8,
                cursor: 'pointer',
                fontSize: 13,
                fontWeight: 600,
                marginBottom: 16,
              }}
            >
              ← Back to Users
            </button>

            <div style={{ marginBottom: 16 }}>
              <h3 style={{ margin: 0, fontSize: 16, color: colors.navy }}>{selectedUser.name}</h3>
              <p style={{ margin: '2px 0 0', color: colors.gray500, fontSize: 13 }}>{selectedUser.role}</p>
            </div>

            <div style={{ display: 'grid', gap: 12 }}>
              {selectedUser.tasks.map(task => (
                <div key={task.id} style={{
                  padding: 14,
                  background: colors.grayBg,
                  borderRadius: 8,
                  borderLeft: `4px solid ${STATUS_COLOR[task.status]}`,
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                    <strong style={{ color: colors.navy, fontSize: 14 }}>{task.title}</strong>
                    <span style={{ color: colors.gray500, fontSize: 12 }}>{task.project_name}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', color: colors.gray500, fontSize: 12 }}>
                    <span style={{ color: STATUS_COLOR[task.status], fontWeight: 600 }}>{STATUS_LABEL[task.status]}</span>
                    <span>Due: {task.end_date || '—'}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 16 }}>
            {userData.map(user => (
              <div
                key={user.id}
                style={{
                  padding: 16,
                  background: colors.grayBg,
                  borderRadius: 10,
                  cursor: 'pointer',
                  border: `1px solid ${colors.border}`,
                  transition: 'box-shadow 0.15s',
                }}
                onClick={() => setSelectedUser(user)}
                onMouseEnter={(e) => { e.currentTarget.style.boxShadow = '0 2px 8px rgba(0,31,91,0.10)'; }}
                onMouseLeave={(e) => { e.currentTarget.style.boxShadow = 'none'; }}
              >
                <h4 style={{ margin: 0, fontSize: 14, color: colors.navy }}>{user.name}</h4>
                <p style={{ margin: '2px 0 12px', color: colors.gray500, fontSize: 12 }}>{user.role}</p>
                <div style={{ display: 'grid', gap: 6, fontSize: 13 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}><span>Tasks</span><strong>{user.tasks.length}</strong></div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}><span>In Progress</span><strong style={{ color: colors.kpmgBlueLight }}>{user.tasks.filter(t => t.status === 'in_progress').length}</strong></div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}><span>Completed</span><strong style={{ color: colors.green }}>{user.tasks.filter(t => t.status === 'done').length}</strong></div>
                  <div style={{ display: 'flex', justifyContent: 'space-between' }}><span>Pending</span><strong style={{ color: colors.amber }}>{user.tasks.filter(t => t.status !== 'done').length}</strong></div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default Summary;
