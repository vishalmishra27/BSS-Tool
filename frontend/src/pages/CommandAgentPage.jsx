import { useState, useRef, useEffect } from 'react';
import { colors, font } from '../theme';

const C = {
  navy: colors.navy || '#001F5B',
  blue: colors.kpmgBlue || '#00338D',
  blueLight: colors.kpmgBlueLight || '#0091DA',
  green: '#16a34a',
  red: '#dc2626',
  amber: '#d97706',
  bg: colors.bgPage || '#F4F6FA',
  card: colors.bgCard || '#FFFFFF',
  border: colors.border || '#E0E8F0',
  muted: colors.gray500 || '#6B7280',
  text: colors.gray900 || '#1a1a2e',
};
const FONT = font?.family || 'system-ui, sans-serif';

const SUGGESTIONS = [
  { label: 'Create a new project', desc: 'Start a new BPM project with team assignments', icon: '+' },
  { label: 'Assign a task to Priya Sharma', desc: 'Delegate work to a team member', icon: '>' },
  { label: 'Show all in-progress tasks', desc: 'View tasks currently being worked on', icon: '?' },
  { label: 'Add a checklist item to Phase 3', desc: 'Add work items to workflow phases', icon: '#' },
  { label: 'Update UAT test case TC-005 to Closed', desc: 'Change test case status', icon: 'v' },
  { label: 'List all active projects', desc: 'See all current projects and their status', icon: '=' },
  { label: 'Create a weekly recurring task', desc: 'Set up tasks that repeat on a schedule', icon: 'R' },
  { label: 'Mark Phase 2 as complete', desc: 'Update workflow phase status', icon: 'C' },
];

function MarkdownTable({ raw }) {
  const lines = raw.trim().split('\n').filter(l => l.includes('|'));
  if (lines.length < 2) return <span style={{ whiteSpace: 'pre-wrap' }}>{raw}</span>;
  const parse = line => line.split('|').map(c => c.trim()).filter((_, i, a) => i > 0 && i < a.length - 1);
  const headers = parse(lines[0]);
  const rows = lines.slice(2).map(parse);
  return (
    <div style={{ overflowX: 'auto', margin: '8px 0' }}>
      <table style={{ borderCollapse: 'collapse', fontSize: 12, width: '100%' }}>
        <thead>
          <tr>{headers.map((h, i) => (
            <th key={i} style={{ padding: '6px 10px', background: C.navy, color: '#fff', border: `1px solid ${C.navy}`, fontWeight: 600, textAlign: 'left', whiteSpace: 'nowrap' }}>{h}</th>
          ))}</tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri} style={{ background: ri % 2 === 0 ? '#fff' : '#f8faff' }}>
              {row.map((cell, ci) => (
                <td key={ci} style={{ padding: '5px 10px', border: '1px solid #e5e7eb', fontSize: 12 }}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DataTable({ data }) {
  if (!data || data.length === 0) return null;
  const keys = Object.keys(data[0]).filter(k => !['id', 'created_at', 'updated_at', 'created_by', 'assigned_by', 'owner_manager_id'].includes(k));
  return (
    <div style={{ overflowX: 'auto', margin: '8px 0', borderRadius: 8, border: `1px solid ${C.border}` }}>
      <table style={{ borderCollapse: 'collapse', fontSize: 12, width: '100%' }}>
        <thead>
          <tr>{keys.map(k => (
            <th key={k} style={{ padding: '7px 12px', background: C.navy, color: '#fff', fontWeight: 600, textAlign: 'left', whiteSpace: 'nowrap', fontSize: 11, textTransform: 'uppercase', letterSpacing: 0.5 }}>
              {k.replace(/_/g, ' ')}
            </th>
          ))}</tr>
        </thead>
        <tbody>
          {data.slice(0, 30).map((row, ri) => (
            <tr key={ri} style={{ background: ri % 2 === 0 ? '#fff' : '#f8faff' }}>
              {keys.map(k => {
                let val = row[k];
                if (val === null || val === undefined) val = '-';
                else if (typeof val === 'string' && val.match(/^\d{4}-\d{2}-\d{2}/)) val = new Date(val).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
                return <td key={k} style={{ padding: '6px 12px', border: '1px solid #f0f0f0', fontSize: 12 }}>{String(val)}</td>;
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > 30 && <div style={{ padding: 8, fontSize: 11, color: C.muted, textAlign: 'center' }}>Showing 30 of {data.length} rows</div>}
    </div>
  );
}

function StatusBadge({ type }) {
  const map = {
    question: { bg: '#EFF6FF', color: '#1D4ED8', label: 'Needs Info' },
    confirmation: { bg: '#FFF7ED', color: '#C2410C', label: 'Confirm?' },
    result: { bg: '#F0FDF4', color: '#15803D', label: 'Done' },
    info: { bg: '#F8FAFC', color: '#475569', label: 'Info' },
    error: { bg: '#FEF2F2', color: '#DC2626', label: 'Error' },
  };
  const s = map[type] || map.info;
  return <span style={{ display: 'inline-block', fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 10, background: s.bg, color: s.color, textTransform: 'uppercase', letterSpacing: 0.5 }}>{s.label}</span>;
}

export default function CommandAgentPage() {
  const [messages, setMessages] = useState([]);
  const [history, setHistory] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [pendingAction, setPendingAction] = useState(null);
  const bottomRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  const addMsg = (role, content, extra = {}) => {
    setMessages(prev => [...prev, { role, content, ts: new Date().toISOString(), ...extra }]);
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
      const res = await fetch('/api/command-agent/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: msg,
          history: newHistory,
          pending_action: pendingAction,
        }),
      });
      const data = await res.json();

      if (data.error) {
        addMsg('agent', data.error, { type: 'error' });
      } else {
        const agentMsg = data.message || 'Done.';
        const type = data.type || 'info';

        addMsg('agent', agentMsg, {
          type,
          data: data.data,
          action: data.action,
          awaitingConfirmation: data.awaiting_confirmation || type === 'confirmation',
        });

        setHistory(prev => [...prev, { role: 'assistant', content: agentMsg }]);

        // Track pending action for confirmation
        if (type === 'confirmation' && data.action) {
          setPendingAction(data.action);
        } else if (type === 'result') {
          setPendingAction(null);
        }
      }
    } catch (e) {
      addMsg('agent', 'Network error — is the backend running?', { type: 'error' });
    }
    setLoading(false);
    inputRef.current?.focus();
  };

  const handleConfirm = () => send('yes');
  const handleCancel = () => {
    setPendingAction(null);
    send('cancel');
  };

  const clearChat = () => {
    setMessages([]);
    setHistory([]);
    setPendingAction(null);
  };

  const isEmpty = messages.length === 0;

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column', fontFamily: FONT, background: C.bg }}>
      {/* Header */}
      <div style={{ padding: '20px 28px 16px', borderBottom: `1px solid ${C.border}`, background: C.card }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', maxWidth: 900, margin: '0 auto' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <div style={{
              width: 38, height: 38, borderRadius: 10,
              background: `linear-gradient(135deg, ${C.blue}, ${C.blueLight})`,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: '#fff', fontSize: 18, fontWeight: 800,
            }}>AI</div>
            <div>
              <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: C.navy }}>Command Agent</h1>
              <p style={{ margin: 0, fontSize: 12, color: C.muted }}>Manage projects, tasks, workflows & dashboards via prompts</p>
            </div>
          </div>
          {messages.length > 0 && (
            <button onClick={clearChat} style={{
              background: 'none', border: `1px solid ${C.border}`, borderRadius: 8,
              padding: '6px 14px', fontSize: 12, color: C.muted, cursor: 'pointer', fontWeight: 600,
            }}>Clear Chat</button>
          )}
        </div>
      </div>

      {/* Chat area */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '24px 28px' }}>
        <div style={{ maxWidth: 900, margin: '0 auto' }}>
          {/* Empty state — suggestions */}
          {isEmpty && (
            <div style={{ textAlign: 'center', padding: '40px 0 20px' }}>
              <div style={{ fontSize: 40, marginBottom: 12 }}>
                <span style={{
                  display: 'inline-flex', width: 64, height: 64, borderRadius: 16,
                  background: `linear-gradient(135deg, ${C.blue}, ${C.blueLight})`,
                  alignItems: 'center', justifyContent: 'center', color: '#fff', fontSize: 28, fontWeight: 800,
                }}>AI</span>
              </div>
              <h2 style={{ color: C.navy, fontSize: 22, fontWeight: 700, margin: '0 0 6px' }}>What would you like to do?</h2>
              <p style={{ color: C.muted, fontSize: 14, margin: '0 0 28px' }}>
                Tell me in plain English — I'll handle the rest. I'll ask for missing details and confirm before making changes.
              </p>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: 10 }}>
                {SUGGESTIONS.map((s, i) => (
                  <button key={i} onClick={() => send(s.label)} style={{
                    background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: '14px 16px',
                    textAlign: 'left', cursor: 'pointer', transition: 'all 0.15s',
                  }}
                    onMouseEnter={e => { e.currentTarget.style.borderColor = C.blueLight; e.currentTarget.style.boxShadow = '0 2px 8px rgba(0,51,141,0.08)'; }}
                    onMouseLeave={e => { e.currentTarget.style.borderColor = C.border; e.currentTarget.style.boxShadow = 'none'; }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                      <span style={{
                        width: 28, height: 28, borderRadius: 7, background: `${C.blueLight}15`,
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        color: C.blue, fontSize: 13, fontWeight: 800, flexShrink: 0,
                      }}>{s.icon}</span>
                      <div>
                        <div style={{ fontSize: 13, fontWeight: 600, color: C.navy }}>{s.label}</div>
                        <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>{s.desc}</div>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Messages */}
          {messages.map((m, i) => (
            <div key={i} style={{
              display: 'flex', justifyContent: m.role === 'user' ? 'flex-end' : 'flex-start',
              marginBottom: 14,
            }}>
              <div style={{
                maxWidth: '80%', padding: '12px 16px', borderRadius: 14,
                background: m.role === 'user' ? C.navy : C.card,
                color: m.role === 'user' ? '#fff' : C.text,
                border: m.role === 'user' ? 'none' : `1px solid ${C.border}`,
                boxShadow: m.role === 'user' ? 'none' : '0 1px 3px rgba(0,0,0,0.04)',
              }}>
                {m.role === 'agent' && m.type && (
                  <div style={{ marginBottom: 6 }}><StatusBadge type={m.type} /></div>
                )}

                {/* Message text */}
                <div style={{ fontSize: 13, lineHeight: 1.6, whiteSpace: 'pre-wrap' }}>
                  {m.content.includes('|') && m.content.includes('\n') ? <MarkdownTable raw={m.content} /> : m.content}
                </div>

                {/* Data table */}
                {m.data && Array.isArray(m.data) && m.data.length > 0 && <DataTable data={m.data} />}

                {/* Confirmation buttons */}
                {m.awaitingConfirmation && i === messages.length - 1 && pendingAction && (
                  <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
                    <button onClick={handleConfirm} style={{
                      background: C.green, color: '#fff', border: 'none', borderRadius: 8,
                      padding: '8px 20px', fontSize: 13, fontWeight: 600, cursor: 'pointer',
                    }}>Yes, proceed</button>
                    <button onClick={handleCancel} style={{
                      background: '#fff', color: C.red, border: `1px solid ${C.red}`, borderRadius: 8,
                      padding: '8px 20px', fontSize: 13, fontWeight: 600, cursor: 'pointer',
                    }}>Cancel</button>
                  </div>
                )}

                <div style={{ fontSize: 10, color: m.role === 'user' ? 'rgba(255,255,255,0.5)' : C.muted, marginTop: 6 }}>
                  {new Date(m.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                </div>
              </div>
            </div>
          ))}

          {loading && (
            <div style={{ display: 'flex', justifyContent: 'flex-start', marginBottom: 14 }}>
              <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 14, padding: '12px 20px' }}>
                <div style={{ display: 'flex', gap: 4 }}>
                  {[0, 1, 2].map(i => (
                    <div key={i} style={{
                      width: 7, height: 7, borderRadius: '50%', background: C.blueLight,
                      animation: `pulse 1.2s ease-in-out ${i * 0.2}s infinite`,
                    }} />
                  ))}
                </div>
              </div>
            </div>
          )}
          <div ref={bottomRef} />
        </div>
      </div>

      {/* Input bar */}
      <div style={{ borderTop: `1px solid ${C.border}`, background: C.card, padding: '16px 28px' }}>
        <div style={{ maxWidth: 900, margin: '0 auto', display: 'flex', gap: 10 }}>
          <input
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && !e.shiftKey && send()}
            placeholder="Tell me what you'd like to do..."
            disabled={loading}
            style={{
              flex: 1, padding: '12px 16px', borderRadius: 10,
              border: `1.5px solid ${C.border}`, fontSize: 14,
              fontFamily: FONT, color: C.text, outline: 'none',
              background: '#fff',
            }}
            onFocus={e => e.target.style.borderColor = C.blueLight}
            onBlur={e => e.target.style.borderColor = C.border}
          />
          <button
            onClick={() => send()}
            disabled={loading || !input.trim()}
            style={{
              background: loading || !input.trim() ? '#ccc' : `linear-gradient(135deg, ${C.blue}, ${C.blueLight})`,
              color: '#fff', border: 'none', borderRadius: 10, padding: '0 20px',
              fontSize: 14, fontWeight: 600, cursor: loading || !input.trim() ? 'not-allowed' : 'pointer',
              display: 'flex', alignItems: 'center', gap: 6,
            }}
          >
            Send
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M22 2L11 13"/><path d="M22 2L15 22L11 13L2 9L22 2Z"/></svg>
          </button>
        </div>
        <div style={{ maxWidth: 900, margin: '6px auto 0', fontSize: 11, color: C.muted, textAlign: 'center' }}>
          Manages BPM projects & tasks, workflow phases & checklists, UAT cases, products, and transformation data. Always confirms before making changes.
        </div>
      </div>

      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 0.3; transform: scale(0.8); }
          50% { opacity: 1; transform: scale(1.2); }
        }
      `}</style>
    </div>
  );
}
