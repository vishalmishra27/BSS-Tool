import { useState, useRef, useEffect } from 'react';

export default function ReconciliationAgentPage() {
  const [messages, setMessages] = useState([
    { role: 'agent', text: "I'm the Reconciliation Agent. Ask me anything about reconciliation data — I'll generate safe SQL queries, run them, and explain the results in plain English.", ts: new Date() },
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
        body: JSON.stringify({
          message: `Use the reconciliation_tool to answer: ${msg}`,
          history: newHistory,
          current_page: '/agent/reconciliation',
        }),
      });
      const data = await res.json();
      if (data.error) {
        addMsg('agent', `Error: ${data.error}`, { isError: true });
      } else if (data.reply) {
        addMsg('agent', data.reply, { toolUsed: data.tool_used });
        setHistory(prev => [...prev, { role: 'assistant', content: data.reply }]);
      }
    } catch {
      addMsg('agent', 'Network error.', { isError: true });
    }
    setLoading(false);
  };

  const SUGGESTIONS = [
    'How many records have mismatched CBS vs CLM status?',
    'Show all services where CBS status is active but CLM is inactive',
    'Summarise reconciliation data by status',
    'Which account link codes have the most discrepancies?',
    'Show top 10 service codes by count',
  ];

  return (
    <div style={{ padding: 24, fontFamily: 'system-ui, sans-serif', background: '#f8faff', minHeight: '100vh' }}>
      <div style={{ marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: '#001F5B' }}>Reconciliation Agent</h1>
        <p style={{ margin: '4px 0 0', color: '#666', fontSize: 13 }}>Ask natural language questions about reconciliation data</p>
      </div>

      {/* How it works */}
      <div style={{ maxWidth: 800, margin: '0 auto 16px', background: '#fff', borderRadius: 10, border: '1px solid #e0e8f0', padding: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: '#001F5B', marginBottom: 8 }}>How this agent works</div>
        <div style={{ fontSize: 12, color: '#555', lineHeight: 1.7 }}>
          This agent takes your natural language questions about reconciliation data, dynamically generates a safe PostgreSQL SELECT query,
          executes it, and returns both the raw results and a plain-English explanation. It only reads data — no writes are performed.
        </div>
        <div style={{ fontSize: 12, fontWeight: 600, color: '#001F5B', marginTop: 10, marginBottom: 4 }}>Sample queries you can try:</div>
        <ul style={{ fontSize: 12, color: '#444', margin: 0, paddingLeft: 18, lineHeight: 1.8 }}>
          <li><i>"How many records have mismatched CBS vs CLM status?"</i></li>
          <li><i>"Show all services where CBS status is active but CLM is inactive"</i></li>
          <li><i>"Summarise reconciliation data by status"</i></li>
          <li><i>"Which account link codes have the most discrepancies?"</i></li>
          <li><i>"Show top 10 service codes by count"</i></li>
          <li><i>"What percentage of services are fully reconciled?"</i></li>
          <li><i>"List all service names with status 'mismatch'"</i></li>
          <li><i>"Compare CBS and CLM statuses — how many match vs differ?"</i></li>
        </ul>
      </div>

      <div style={{ maxWidth: 800, margin: '0 auto' }}>
        <div style={{ background: '#fff', borderRadius: 10, border: '1px solid #e0e8f0', padding: 20, minHeight: 400, maxHeight: 500, overflowY: 'auto', marginBottom: 16 }}>
          {messages.map((m, i) => (
            <div key={i} style={{ marginBottom: 12 }}>
              <div style={{ display: 'flex', justifyContent: m.role === 'user' ? 'flex-end' : 'flex-start' }}>
                <div style={{
                  maxWidth: '80%', padding: '10px 14px', borderRadius: 10,
                  background: m.role === 'user' ? '#001F5B' : m.isError ? '#fff5f5' : '#f5f7fa',
                  color: m.role === 'user' ? '#fff' : m.isError ? '#c00' : '#222',
                  fontSize: 13, lineHeight: 1.6, whiteSpace: 'pre-wrap',
                }}>
                  {m.text}
                  {m.toolUsed && <div style={{ marginTop: 4, fontSize: 10, color: '#888' }}>Tool: {m.toolUsed}</div>}
                </div>
              </div>
            </div>
          ))}
          {loading && <div style={{ color: '#888', fontSize: 13 }}>Analysing reconciliation data...</div>}
          <div ref={bottomRef} />
        </div>

        {messages.length <= 2 && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 12 }}>
            {SUGGESTIONS.map((s, i) => (
              <button key={i} onClick={() => send(s)} style={{ background: '#f0f4ff', border: '1px solid #c8d4f0', borderRadius: 16, padding: '6px 14px', fontSize: 12, color: '#001F5B', cursor: 'pointer' }}>
                {s}
              </button>
            ))}
          </div>
        )}

        <div style={{ display: 'flex', gap: 10 }}>
          <input value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && send()}
            placeholder="Ask about reconciliation data..." disabled={loading}
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
