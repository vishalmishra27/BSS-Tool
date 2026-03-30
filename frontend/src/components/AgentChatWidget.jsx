import React, { useState, useRef, useEffect } from 'react';
import { useLocation } from 'react-router-dom';

const MODULE_LABELS = {
  '/dashboard': 'Transformation Dashboard',
  '/': 'Transformation Dashboard',
  '/milestones': 'Project Milestones',
  '/status': 'Status Tracker',
  '/product-dashboard': 'Product Journey',
  '/uat': 'UAT Management',
  '/reconciliation': 'Reconciliation Dashboard',
  '/pdf-analysis': 'PDF Analysis',
  '/workflow': 'Workflow & Milestones',
};

const SUGGESTIONS = {
  'UAT Management': [
    'Close all open ICT test cases',
    'Show me all high priority UAT cases',
    'How many test cases are still open?',
  ],
  'Product Journey': [
    'Mark all Benefits parameters for ICT as Matched',
    'Show products flagged for migration',
  ],
  'Transformation Dashboard': [
    'Which LOBs are behind schedule?',
    'Show me all risk alerts',
  ],
  'Project Milestones': [
    'Which phases are overdue?',
    'Show checklist status for phase 5',
  ],
};

const OP_BADGE = {
  CREATE: { label: 'Write', bg: '#0070c0', color: '#fff' },
  UPDATE: { label: 'Write', bg: '#0070c0', color: '#fff' },
  BULK_UPDATE: { label: 'Bulk', bg: '#e07b00', color: '#fff' },
  DELETE: { label: 'Delete', bg: '#dc2626', color: '#fff' },
  SOFT_DELETE: { label: 'Delete', bg: '#dc2626', color: '#fff' },
};

// ─── Diff Table ──────────────────────────────────────────────────────────────
function DiffTable({ diff }) {
  if (!diff || !diff.before || !diff.after) return null;
  const before = diff.before[0] || {};
  const after = diff.after[0] || {};
  const allKeys = [...new Set([...Object.keys(before), ...Object.keys(after)])];
  const changed = allKeys.filter(k => JSON.stringify(before[k]) !== JSON.stringify(after[k]));
  if (changed.length === 0) return <div style={{ fontSize: 12, color: '#888' }}>No field changes detected.</div>;

  return (
    <table style={{ width: '100%', fontSize: 11, borderCollapse: 'collapse', marginTop: 6 }}>
      <thead>
        <tr style={{ background: '#f0f4ff' }}>
          <th style={thStyle}>Field</th>
          <th style={thStyle}>Old</th>
          <th style={thStyle}>New</th>
        </tr>
      </thead>
      <tbody>
        {changed.map(k => (
          <tr key={k}>
            <td style={tdStyle}>{k}</td>
            <td style={{ ...tdStyle, color: '#999', textDecoration: 'line-through' }}>{String(before[k] ?? '—')}</td>
            <td style={{ ...tdStyle, color: '#16a34a', fontWeight: 600 }}>{String(after[k] ?? '—')}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ─── Confirmation Card ───────────────────────────────────────────────────────
function ConfirmCard({ pending, onApprove, onReject, onEditSql, loading }) {
  const [showSql, setShowSql] = useState(false);
  const [editMode, setEditMode] = useState(false);
  const [editedSql, setEditedSql] = useState(pending.sql_preview || '');
  const diff = pending.diff || {};
  const op = diff.operation || 'UPDATE';
  const badge = OP_BADGE[op] || OP_BADGE.UPDATE;

  return (
    <div style={{
      background: '#fff', border: '1px solid #d0d8f0', borderRadius: 8,
      padding: 12, marginTop: 4, fontSize: 12,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <span style={{
          padding: '2px 8px', borderRadius: 10, fontSize: 10, fontWeight: 700,
          background: badge.bg, color: badge.color,
        }}>{badge.label}</span>
        <span style={{ fontWeight: 600, color: '#222' }}>{diff.table}</span>
        <span style={{ color: '#888' }}>· {diff.rows_affected || '?'} row(s)</span>
      </div>

      <div style={{ fontSize: 12, color: '#555', marginBottom: 6 }}>{pending.description}</div>

      <DiffTable diff={diff} />

      <button onClick={() => setShowSql(!showSql)} style={{
        background: 'none', border: 'none', color: '#0070c0', fontSize: 11,
        cursor: 'pointer', padding: '4px 0', marginTop: 6,
      }}>
        {showSql ? '▲ Hide SQL' : '▼ Show SQL'}
      </button>
      {showSql && (
        editMode ? (
          <div style={{ marginTop: 4 }}>
            <textarea
              value={editedSql}
              onChange={e => setEditedSql(e.target.value)}
              style={{ width: '100%', padding: 6, fontSize: 11, fontFamily: 'monospace', border: '1px solid #d0d8f0', borderRadius: 4, minHeight: 60, boxSizing: 'border-box' }}
            />
          </div>
        ) : (
          <pre style={{
            background: '#f8faff', padding: 8, borderRadius: 4, fontSize: 10,
            fontFamily: 'monospace', whiteSpace: 'pre-wrap', margin: '4px 0 0',
            border: '1px solid #e0e8f0', color: '#333',
          }}>{pending.sql_preview}</pre>
        )
      )}

      <div style={{ display: 'flex', gap: 6, marginTop: 10 }}>
        <button onClick={() => onApprove(pending.pending_id)} disabled={loading}
          style={{ ...actionBtn, background: '#16a34a', color: '#fff', opacity: loading ? 0.6 : 1 }}>
          {loading ? '...' : 'Approve'}
        </button>
        <button onClick={() => onReject(pending.pending_id)} disabled={loading}
          style={{ ...actionBtn, background: '#dc2626', color: '#fff' }}>
          Reject
        </button>
        <button onClick={() => setEditMode(!editMode)}
          style={{ ...actionBtn, background: '#f1f5f9', color: '#444', border: '1px solid #d1d5db' }}>
          {editMode ? 'Cancel Edit' : 'Edit SQL'}
        </button>
      </div>
    </div>
  );
}

// ─── Alert Banners ───────────────────────────────────────────────────────────
function AlertBanner({ alert, onDismiss }) {
  const isHigh = alert.severity === 'high';
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      padding: '6px 10px', borderRadius: 6, fontSize: 11,
      background: isHigh ? '#fef2f2' : '#fffbeb',
      border: `1px solid ${isHigh ? '#fecaca' : '#fde68a'}`,
      color: isHigh ? '#991b1b' : '#92400e',
    }}>
      <span style={{ fontWeight: 700 }}>{isHigh ? '!' : '⚠'}</span>
      <span style={{ flex: 1 }}><b>{alert.module}:</b> {alert.description}</span>
      <button onClick={onDismiss} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'inherit', fontSize: 14, lineHeight: 1 }}>×</button>
    </div>
  );
}

// ─── Main Widget ─────────────────────────────────────────────────────────────
export default function AgentChatWidget({ username = 'Programme User', canUseAgent = true }) {
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState([
    { role: 'agent', text: "Hello! I'm your BSS Migration AI assistant. Ask me anything or describe a data change in plain English.", ts: new Date() },
  ]);
  const [history, setHistory] = useState([]); // conversation history for API
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [alerts, setAlerts] = useState([]);
  const [unread, setUnread] = useState(false);
  const bottomRef = useRef(null);
  const location = useLocation();
  const currentPage = location.pathname;
  const moduleContext = MODULE_LABELS[currentPage] || 'General';
  const suggestions = SUGGESTIONS[moduleContext] || [];

  // Scroll to bottom
  useEffect(() => {
    if (open) bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, open]);

  // Poll alerts every 5 minutes
  useEffect(() => {
    const fetchAlerts = () => {
      fetch('/api/agent/alerts')
        .then(r => r.json())
        .then(data => {
          if (data.alerts && data.alerts.length > 0) {
            setAlerts(data.alerts);
            if (!open) setUnread(true);
          }
        })
        .catch(() => {});
    };
    fetchAlerts();
    const interval = setInterval(fetchAlerts, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);

  const addMessage = (role, text, extra = {}) => {
    setMessages(prev => [...prev, { role, text, ts: new Date(), ...extra }]);
  };

  const handleSend = async (text) => {
    const message = (text || input).trim();
    if (!message || loading) return;
    setInput('');
    addMessage('user', message);

    // Update conversation history
    const newHistory = [...history, { role: 'user', content: message }];
    setHistory(newHistory);
    setLoading(true);

    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message, history: newHistory, current_page: currentPage }),
      });
      const data = await res.json();

      if (!res.ok || data.error) {
        addMessage('agent', `Error: ${data.error || 'Something went wrong'}`, { isError: true });
      } else {
        // Add agent reply
        if (data.reply) {
          addMessage('agent', data.reply, {
            pending: data.pending_confirmation || null,
            toolUsed: data.tool_used || null,
          });
          setHistory(prev => [...prev, { role: 'assistant', content: data.reply }]);
        }

        // Handle alerts from tool
        if (data.alerts && data.alerts.length > 0) {
          setAlerts(data.alerts);
        }
      }
    } catch (err) {
      addMessage('agent', 'Network error. Is the server running?', { isError: true });
    } finally {
      setLoading(false);
    }
  };

  const handleApprove = async (pendingId) => {
    setLoading(true);
    try {
      const res = await fetch('/api/agent/confirm', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pending_id: pendingId }),
      });
      const data = await res.json();
      if (data.success) {
        addMessage('agent', `Done! ${data.rows_affected || 0} row(s) updated. Change logged to audit trail.`, { isSuccess: true });
        // Remove pending from messages
        setMessages(prev => prev.map(m => m.pending?.pending_id === pendingId ? { ...m, pending: null } : m));
      } else {
        addMessage('agent', `Execution failed: ${data.error}`, { isError: true });
      }
    } catch (err) {
      addMessage('agent', 'Error executing confirmed write.', { isError: true });
    } finally {
      setLoading(false);
    }
  };

  const handleReject = async (pendingId) => {
    try {
      await fetch('/api/agent/reject', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pending_id: pendingId }),
      });
      addMessage('agent', 'Change rejected — no data was modified.');
      setMessages(prev => prev.map(m => m.pending?.pending_id === pendingId ? { ...m, pending: null } : m));
    } catch (err) {
      addMessage('agent', 'Error rejecting write.', { isError: true });
    }
  };

  const dismissAlert = (idx) => {
    setAlerts(prev => prev.filter((_, i) => i !== idx));
  };

  if (!canUseAgent) return null;

  return (
    <>
      {/* Floating button */}
      {!open && (
        <button
          onClick={() => { setOpen(true); setUnread(false); }}
          title="AI Assistant"
          style={{
            position: 'fixed', bottom: 28, right: 28, zIndex: 9000,
            width: 56, height: 56, borderRadius: '50%',
            background: 'linear-gradient(135deg, #001F5B 0%, #003087 100%)',
            border: '2px solid #00B0F0',
            color: '#fff', fontSize: 24, cursor: 'pointer',
            boxShadow: '0 4px 20px rgba(0,31,91,0.4)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}
        >
          🤖
          {unread && (
            <span style={{
              position: 'absolute', top: 2, right: 2, width: 12, height: 12,
              borderRadius: '50%', background: '#dc2626', border: '2px solid #fff',
            }} />
          )}
        </button>
      )}

      {/* Chat panel */}
      {open && (
        <div style={{
          position: 'fixed', bottom: 24, right: 24, zIndex: 9000,
          width: 400, height: 560, borderRadius: 12,
          background: '#fff', boxShadow: '0 8px 40px rgba(0,31,91,0.25)',
          display: 'flex', flexDirection: 'column',
          fontFamily: 'system-ui, sans-serif', overflow: 'hidden',
          border: '1px solid #d0d8f0',
        }}>
          {/* Header */}
          <div style={{
            background: 'linear-gradient(135deg, #001F5B 0%, #003087 100%)',
            padding: '12px 16px', display: 'flex', alignItems: 'center', gap: 10,
          }}>
            <span style={{ fontSize: 20 }}>🤖</span>
            <div style={{ flex: 1 }}>
              <div style={{ color: '#fff', fontWeight: 700, fontSize: 14 }}>AI Agent</div>
              <div style={{ color: '#00B0F0', fontSize: 11 }}>{moduleContext}</div>
            </div>
            <button onClick={() => setOpen(false)}
              style={{ background: 'none', border: 'none', color: 'rgba(255,255,255,0.7)', cursor: 'pointer', fontSize: 18, lineHeight: 1 }}>
              ×
            </button>
          </div>

          {/* Messages */}
          <div style={{ flex: 1, overflowY: 'auto', padding: '14px 14px 6px', display: 'flex', flexDirection: 'column', gap: 10 }}>
            {messages.map((msg, i) => (
              <div key={i}>
                <div style={{
                  display: 'flex',
                  justifyContent: msg.role === 'user' ? 'flex-end' : 'flex-start',
                }}>
                  <div style={{
                    maxWidth: '85%', padding: '8px 12px',
                    borderRadius: msg.role === 'user' ? '12px 12px 4px 12px' : '12px 12px 12px 4px',
                    background: msg.role === 'user' ? '#001F5B'
                      : msg.isError ? '#fff5f5'
                      : msg.isSuccess ? '#f0fff4'
                      : '#f5f7fa',
                    color: msg.role === 'user' ? '#fff'
                      : msg.isError ? '#c00000'
                      : msg.isSuccess ? '#006600'
                      : '#222',
                    fontSize: 13, lineHeight: 1.5,
                    whiteSpace: 'pre-wrap',
                  }}>
                    {msg.text}
                    {msg.toolUsed && (
                      <div style={{ marginTop: 4, fontSize: 10, color: '#888' }}>Tool: {msg.toolUsed}</div>
                    )}
                  </div>
                </div>
                {/* Inline confirmation card */}
                {msg.pending && (
                  <ConfirmCard
                    pending={msg.pending}
                    onApprove={handleApprove}
                    onReject={handleReject}
                    loading={loading}
                  />
                )}
              </div>
            ))}
            {loading && (
              <div style={{ display: 'flex', justifyContent: 'flex-start' }}>
                <div style={{ background: '#f5f7fa', borderRadius: 12, padding: '8px 14px', fontSize: 13, color: '#666' }}>
                  <span style={{ display: 'inline-flex', gap: 4 }}>
                    <span className="dot-pulse">·</span>
                    <span className="dot-pulse" style={{ animationDelay: '200ms' }}>·</span>
                    <span className="dot-pulse" style={{ animationDelay: '400ms' }}>·</span>
                  </span>
                </div>
              </div>
            )}
            <div ref={bottomRef} />
          </div>

          {/* Alert banners */}
          {alerts.length > 0 && (
            <div style={{ padding: '0 10px 4px', display: 'flex', flexDirection: 'column', gap: 4, maxHeight: 120, overflowY: 'auto' }}>
              {alerts.slice(0, 5).map((a, i) => (
                <AlertBanner key={i} alert={a} onDismiss={() => dismissAlert(i)} />
              ))}
            </div>
          )}

          {/* Suggestions */}
          {suggestions.length > 0 && messages.length <= 2 && (
            <div style={{ padding: '0 12px 6px', display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {suggestions.map((s, i) => (
                <button key={i} onClick={() => handleSend(s)}
                  style={{
                    background: '#f0f4ff', border: '1px solid #c8d4f0',
                    borderRadius: 12, padding: '4px 10px', fontSize: 11,
                    color: '#001F5B', cursor: 'pointer',
                  }}>
                  {s}
                </button>
              ))}
            </div>
          )}

          {/* Input */}
          <div style={{ padding: '10px 12px', borderTop: '1px solid #eee', display: 'flex', gap: 8 }}>
            <input
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && !e.shiftKey && handleSend()}
              placeholder="Ask anything or describe a change…"
              disabled={loading}
              style={{
                flex: 1, padding: '8px 12px', borderRadius: 6,
                border: '1px solid #d0d8f0', fontSize: 13, outline: 'none',
                background: loading ? '#f9f9f9' : '#fff',
              }}
            />
            <button
              onClick={() => handleSend()}
              disabled={loading || !input.trim()}
              style={{
                padding: '8px 14px', borderRadius: 6, border: 'none',
                background: (!loading && input.trim()) ? '#001F5B' : '#ccc',
                color: '#fff', cursor: (!loading && input.trim()) ? 'pointer' : 'not-allowed',
                fontSize: 16, display: 'flex', alignItems: 'center',
              }}
            >
              ➤
            </button>
          </div>
        </div>
      )}

      <style>{`
        @keyframes dotPulse { 0%,80%,100%{opacity:0.2} 40%{opacity:1} }
        .dot-pulse { animation: dotPulse 1.2s infinite ease-in-out; font-size: 18px; }
      `}</style>
    </>
  );
}

const thStyle = { padding: '4px 6px', textAlign: 'left', borderBottom: '1px solid #e0e8f0', fontSize: 10, fontWeight: 600, color: '#555' };
const tdStyle = { padding: '3px 6px', borderBottom: '1px solid #f0f0f0' };
const actionBtn = { padding: '5px 12px', borderRadius: 4, border: 'none', fontSize: 11, fontWeight: 600, cursor: 'pointer' };
