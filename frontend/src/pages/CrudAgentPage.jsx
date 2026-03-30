import { useState, useRef, useEffect } from 'react';

export default function CrudAgentPage() {
  const [messages, setMessages] = useState([
    { role: 'agent', text: "I'm the Data Management Agent. I can CREATE, READ, UPDATE, and DELETE records across all BSS tables. Describe what you need in plain English.", ts: new Date() },
  ]);
  const [history, setHistory] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const addMsg = (role, text, extra = {}) => {
    setMessages(prev => [...prev, { role, text, ts: new Date(), ...extra }]);
  };

  const send = async (text) => {
    const msg = (text || input).trim();
    if (!msg || loading) return;
    setInput('');
    addMsg('user', msg);
    const newHistory = [...history, { role: 'user', content: msg }];
    setHistory(newHistory);
    setLoading(true);
    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg, history: newHistory, current_page: '/agent/crud' }),
      });
      const data = await res.json();
      if (data.error) {
        addMsg('agent', `Error: ${data.error}`, { isError: true });
      } else {
        if (data.reply) {
          addMsg('agent', data.reply, { pending: data.pending_confirmation });
          setHistory(prev => [...prev, { role: 'assistant', content: data.reply }]);
        }
      }
    } catch {
      addMsg('agent', 'Network error.', { isError: true });
    }
    setLoading(false);
  };

  const approve = async (id) => {
    setLoading(true);
    try {
      const res = await fetch('/api/agent/confirm', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pending_id: id }),
      });
      const data = await res.json();
      if (data.success) {
        addMsg('agent', `Done! ${data.rows_affected || 0} row(s) updated.`, { isSuccess: true });
        setMessages(prev => prev.map(m => m.pending?.pending_id === id ? { ...m, pending: null } : m));
      } else {
        addMsg('agent', `Failed: ${data.error}`, { isError: true });
      }
    } catch { addMsg('agent', 'Error.', { isError: true }); }
    setLoading(false);
  };

  const reject = async (id) => {
    await fetch('/api/agent/reject', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pending_id: id }) });
    addMsg('agent', 'Rejected — no changes made.');
    setMessages(prev => prev.map(m => m.pending?.pending_id === id ? { ...m, pending: null } : m));
  };

  const SUGGESTIONS = [
    'Show all open UAT test cases',
    'Close all ICT test cases',
    'Add a new Broadband product called Fibre 1Gbps',
    'Update phase 5 status to complete',
    'Show products flagged for migration',
  ];

  return (
    <div style={{ padding: 24, fontFamily: 'system-ui, sans-serif', background: '#f8faff', minHeight: '100vh' }}>
      <div style={{ marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: '#001F5B' }}>Data Management Agent</h1>
        <p style={{ margin: '4px 0 0', color: '#666', fontSize: 13 }}>CRUD operations across all BSS tables via natural language</p>
      </div>

      {/* How it works */}
      <div style={{ maxWidth: 800, margin: '0 auto 16px', background: '#fff', borderRadius: 10, border: '1px solid #e0e8f0', padding: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: '#001F5B', marginBottom: 8 }}>How this agent works</div>
        <div style={{ fontSize: 12, color: '#555', lineHeight: 1.7 }}>
          This agent can <b>Create</b>, <b>Read</b>, <b>Update</b>, and <b>Delete</b> (soft delete) records across all BSS tables
          including UAT cases, products, parameters, phases, checklist items, and transformation activities.
          All write operations require your explicit approval before execution — you'll see a diff preview with before/after values.
        </div>
        <div style={{ fontSize: 12, fontWeight: 600, color: '#001F5B', marginTop: 10, marginBottom: 4 }}>Sample queries you can try:</div>
        <ul style={{ fontSize: 12, color: '#444', margin: 0, paddingLeft: 18, lineHeight: 1.8 }}>
          <li><i>"Show all open UAT test cases for ICT"</i> — reads from uat_cases</li>
          <li><i>"Close all ICT test cases with priority High"</i> — bulk update with confirmation</li>
          <li><i>"Add a new product called Fibre 1Gbps to Broadband LOB"</i> — creates a new row</li>
          <li><i>"Update phase 5 end date to 2026-01-31"</i> — updates phases table</li>
          <li><i>"Mark all Benefits parameters as Matched"</i> — bulk update product_parameters</li>
          <li><i>"Delete product PROD-0002"</i> — soft delete (sets deleted_at)</li>
          <li><i>"Show transformation activities for Prepaid B2C"</i> — reads progress data</li>
          <li><i>"Set Data Cleanup actual progress to 80% for Prepaid B2C"</i> — updates activity</li>
        </ul>
      </div>

      <div style={{ maxWidth: 800, margin: '0 auto' }}>
        {/* Messages */}
        <div style={{ background: '#fff', borderRadius: 10, border: '1px solid #e0e8f0', padding: 20, minHeight: 400, maxHeight: 500, overflowY: 'auto', marginBottom: 16 }}>
          {messages.map((m, i) => (
            <div key={i} style={{ marginBottom: 12 }}>
              <div style={{ display: 'flex', justifyContent: m.role === 'user' ? 'flex-end' : 'flex-start' }}>
                <div style={{
                  maxWidth: '80%', padding: '10px 14px', borderRadius: 10,
                  background: m.role === 'user' ? '#001F5B' : m.isError ? '#fff5f5' : m.isSuccess ? '#f0fff4' : '#f5f7fa',
                  color: m.role === 'user' ? '#fff' : m.isError ? '#c00' : m.isSuccess ? '#060' : '#222',
                  fontSize: 13, lineHeight: 1.6, whiteSpace: 'pre-wrap',
                }}>
                  {m.text}
                </div>
              </div>
              {m.pending && <ConfirmInline pending={m.pending} onApprove={approve} onReject={reject} loading={loading} />}
            </div>
          ))}
          {loading && <div style={{ color: '#888', fontSize: 13 }}>Thinking...</div>}
          <div ref={bottomRef} />
        </div>

        {/* Suggestions */}
        {messages.length <= 2 && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 12 }}>
            {SUGGESTIONS.map((s, i) => (
              <button key={i} onClick={() => send(s)} style={{ background: '#f0f4ff', border: '1px solid #c8d4f0', borderRadius: 16, padding: '6px 14px', fontSize: 12, color: '#001F5B', cursor: 'pointer' }}>
                {s}
              </button>
            ))}
          </div>
        )}

        {/* Input */}
        <div style={{ display: 'flex', gap: 10 }}>
          <input value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && send()}
            placeholder="Describe a data operation..." disabled={loading}
            style={{ flex: 1, padding: '12px 16px', borderRadius: 8, border: '1px solid #d0d8f0', fontSize: 14, outline: 'none' }} />
          <button onClick={() => send()} disabled={loading || !input.trim()}
            style={{ padding: '12px 24px', borderRadius: 8, border: 'none', background: input.trim() ? '#001F5B' : '#ccc', color: '#fff', fontWeight: 600, fontSize: 14, cursor: input.trim() ? 'pointer' : 'not-allowed' }}>
            Send
          </button>
        </div>
      </div>
    </div>
  );
}

function ConfirmInline({ pending, onApprove, onReject, loading }) {
  const [showSql, setShowSql] = useState(false);
  const diff = pending.diff || {};
  const before = (diff.before || [])[0] || {};
  const after = (diff.after || [])[0] || {};
  const allKeys = [...new Set([...Object.keys(before), ...Object.keys(after)])];
  const changed = allKeys.filter(k => JSON.stringify(before[k]) !== JSON.stringify(after[k]));

  const OP_COLORS = { CREATE: '#0070c0', UPDATE: '#0070c0', BULK_UPDATE: '#e07b00', DELETE: '#dc2626', SOFT_DELETE: '#dc2626' };

  return (
    <div style={{ margin: '8px 0 8px 0', background: '#fff', border: '1px solid #d0d8f0', borderRadius: 8, padding: 14, fontSize: 12 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <span style={{ padding: '2px 8px', borderRadius: 10, fontSize: 10, fontWeight: 700, background: OP_COLORS[diff.operation] || '#0070c0', color: '#fff' }}>
          {diff.operation}
        </span>
        <span style={{ fontWeight: 600 }}>{diff.table}</span>
        <span style={{ color: '#888' }}>· {diff.rows_affected || '?'} row(s)</span>
      </div>
      <div style={{ color: '#555', marginBottom: 8 }}>{pending.description}</div>
      {changed.length > 0 && (
        <table style={{ width: '100%', fontSize: 11, borderCollapse: 'collapse', marginBottom: 8 }}>
          <thead><tr style={{ background: '#f0f4ff' }}>
            <th style={{ padding: '4px 6px', textAlign: 'left', borderBottom: '1px solid #e0e8f0' }}>Field</th>
            <th style={{ padding: '4px 6px', textAlign: 'left', borderBottom: '1px solid #e0e8f0' }}>Old</th>
            <th style={{ padding: '4px 6px', textAlign: 'left', borderBottom: '1px solid #e0e8f0' }}>New</th>
          </tr></thead>
          <tbody>{changed.map(k => (
            <tr key={k}>
              <td style={{ padding: '3px 6px', borderBottom: '1px solid #f0f0f0' }}>{k}</td>
              <td style={{ padding: '3px 6px', borderBottom: '1px solid #f0f0f0', color: '#999', textDecoration: 'line-through' }}>{String(before[k] ?? '—')}</td>
              <td style={{ padding: '3px 6px', borderBottom: '1px solid #f0f0f0', color: '#16a34a', fontWeight: 600 }}>{String(after[k] ?? '—')}</td>
            </tr>
          ))}</tbody>
        </table>
      )}
      <button onClick={() => setShowSql(!showSql)} style={{ background: 'none', border: 'none', color: '#0070c0', fontSize: 11, cursor: 'pointer', padding: 0, marginBottom: 8 }}>
        {showSql ? '▲ Hide SQL' : '▼ Show SQL'}
      </button>
      {showSql && <pre style={{ background: '#f8faff', padding: 8, borderRadius: 4, fontSize: 10, fontFamily: 'monospace', whiteSpace: 'pre-wrap', border: '1px solid #e0e8f0', margin: '4px 0 8px' }}>{pending.sql_preview}</pre>}
      <div style={{ display: 'flex', gap: 8 }}>
        <button onClick={() => onApprove(pending.pending_id)} disabled={loading} style={{ padding: '6px 16px', borderRadius: 5, border: 'none', background: '#16a34a', color: '#fff', fontSize: 12, fontWeight: 600, cursor: 'pointer' }}>Approve</button>
        <button onClick={() => onReject(pending.pending_id)} style={{ padding: '6px 16px', borderRadius: 5, border: 'none', background: '#dc2626', color: '#fff', fontSize: 12, fontWeight: 600, cursor: 'pointer' }}>Reject</button>
      </div>
    </div>
  );
}
