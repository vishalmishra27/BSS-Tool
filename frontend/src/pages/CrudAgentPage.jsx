import { useState, useRef, useEffect, useCallback } from 'react';
import {
  Sparkles, BookOpen, PenLine, RefreshCw, Trash2, Search, BarChart3,
  Pencil, ArrowUp, X, Plus, History
} from 'lucide-react';

const STORAGE_KEY = 'bss_crud_agent_history';
function loadSessions() { try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); } catch { return []; } }
function saveSessions(s) { localStorage.setItem(STORAGE_KEY, JSON.stringify(s)); }

export default function CrudAgentPage() {
  const [messages, setMessages] = useState([]);
  const [chatHistory, setChatHistory] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  const [historyOpen, setHistoryOpen] = useState(false);
  const [sessions, setSessions] = useState(loadSessions);
  const [activeSessionId, setActiveSessionId] = useState(null);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  const saveCurrentSession = useCallback(() => {
    if (messages.length === 0) return;
    const title = messages.find(m => m.role === 'user')?.text?.slice(0, 50) || 'New conversation';
    const updated = loadSessions();
    if (activeSessionId) {
      const idx = updated.findIndex(s => s.id === activeSessionId);
      if (idx >= 0) { updated[idx].messages = messages; updated[idx].title = title; updated[idx].updatedAt = new Date().toISOString(); }
    } else {
      const id = Date.now().toString();
      updated.unshift({ id, title, messages, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });
      setActiveSessionId(id);
    }
    saveSessions(updated); setSessions(updated);
  }, [messages, activeSessionId]);
  useEffect(() => { if (messages.length > 0) saveCurrentSession(); }, [messages.length]);

  const loadSession = (s) => { setMessages(s.messages || []); setActiveSessionId(s.id); setChatHistory([]); };
  const deleteSession = (id) => { const u = loadSessions().filter(s => s.id !== id); saveSessions(u); setSessions(u); if (activeSessionId === id) { setMessages([]); setChatHistory([]); setActiveSessionId(null); } };
  const startNewChat = () => { setMessages([]); setChatHistory([]); setActiveSessionId(null); };

  const addMsg = (role, text, extra = {}) => { setMessages(prev => [...prev, { role, text, ts: new Date().toISOString(), ...extra }]); };

  const send = async (text) => {
    const msg = (text || input).trim(); if (!msg || loading) return;
    setInput(''); addMsg('user', msg);
    const newHistory = [...chatHistory, { role: 'user', content: msg }]; setChatHistory(newHistory);
    setLoading(true);
    try {
      const res = await fetch('/api/agent/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: msg, history: newHistory, current_page: '/agent/crud' }) });
      const data = await res.json();
      if (data.error) addMsg('agent', `Error: ${data.error}`, { isError: true });
      else if (data.reply) { addMsg('agent', data.reply, { pending: data.pending_confirmation }); setChatHistory(prev => [...prev, { role: 'assistant', content: data.reply }]); }
    } catch { addMsg('agent', 'Network error.', { isError: true }); }
    setLoading(false);
  };

  const approve = async (id) => {
    setLoading(true);
    try {
      const res = await fetch('/api/agent/confirm', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pending_id: id }) });
      const data = await res.json();
      if (data.success) { addMsg('agent', `Done! ${data.rows_affected || 0} row(s) affected.`, { isSuccess: true }); setMessages(prev => prev.map(m => m.pending?.pending_id === id ? { ...m, pending: null } : m)); }
      else addMsg('agent', `Failed: ${data.error}`, { isError: true });
    } catch { addMsg('agent', 'Error confirming.', { isError: true }); }
    setLoading(false);
  };

  const reject = async (id) => {
    await fetch('/api/agent/reject', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pending_id: id }) });
    addMsg('agent', 'Rejected — no changes made.');
    setMessages(prev => prev.map(m => m.pending?.pending_id === id ? { ...m, pending: null } : m));
  };

  const CARDS = [
    { icon: BookOpen, title: 'Show all open UAT test cases', desc: 'Read from uat_cases table' },
    { icon: PenLine, title: 'Close all ICT test cases', desc: 'Bulk update with confirmation' },
    { icon: Plus, title: 'Add a new Broadband product', desc: 'Create a new product record' },
    { icon: RefreshCw, title: 'Update phase 5 status to complete', desc: 'Modify phases table' },
    { icon: Search, title: 'Show products flagged for migration', desc: 'Query product_parameters' },
    { icon: BarChart3, title: 'Show transformation activities', desc: 'View progress data by LOB' },
  ];

  const hasMessages = messages.length > 0;

  return (
    <div className="flex flex-col h-screen bg-[#f5f7fb] font-sans">
      <div className="flex-1 flex overflow-hidden">
        {historyOpen && <HistoryPanel sessions={sessions} activeSessionId={activeSessionId} onLoad={loadSession} onDelete={deleteSession} onNew={startNewChat} onClose={() => setHistoryOpen(false)} />}

        <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
          {/* Top bar */}
          <div className="flex items-center justify-between px-6 py-3 bg-white border-b border-gray-100">
            <div className="flex items-center gap-2">
              <button onClick={() => setHistoryOpen(!historyOpen)} className="p-2 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600 transition-colors" title="Chat History"><History size={18} /></button>
              {hasMessages && <button onClick={startNewChat} className="p-2 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600 transition-colors" title="New Chat"><Plus size={18} /></button>}
            </div>
          </div>

          {!hasMessages ? (
            <div className="flex-1 flex flex-col items-center justify-center px-6 py-12">
              <div className="w-14 h-14 rounded-2xl bg-violet-500 flex items-center justify-center mb-6 shadow-lg shadow-violet-500/20">
                <Sparkles size={28} className="text-white" />
              </div>
              <h1 className="text-2xl font-bold text-gray-900 mb-2 text-center">What data operation can I perform?</h1>
              <p className="text-sm text-gray-500 text-center max-w-lg mb-10">
                Describe any CRUD operation in plain English. All writes require your explicit approval before execution — you'll see a diff preview with before/after values.
              </p>

              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 max-w-3xl w-full mb-10">
                {CARDS.map((card, i) => {
                  const Icon = card.icon;
                  return (
                    <button key={i} onClick={() => send(card.title)}
                      className="bg-white rounded-xl p-5 text-left shadow-sm hover:shadow-md transition-shadow border border-gray-50 group cursor-pointer">
                      <Icon size={20} className="text-gray-400 group-hover:text-violet-500 transition-colors mb-3" />
                      <div className="text-sm font-semibold text-gray-800 mb-1">{card.title}</div>
                      <div className="text-xs text-gray-500 leading-relaxed">{card.desc}</div>
                    </button>
                  );
                })}
              </div>

              <div className="w-full max-w-2xl">
                <ChatInput input={input} setInput={setInput} loading={loading} onSend={send}
                  placeholder="Describe a data operation…" accent="violet" />
              </div>
            </div>
          ) : (
            <>
              <div className="flex-1 overflow-y-auto px-6 py-5 space-y-4">
                {messages.map((m, i) => (
                  <div key={i}>
                    <div className={`flex items-start gap-3 ${m.role === 'user' ? 'justify-end' : ''}`}>
                      {m.role === 'agent' && <AgentBadge color="violet" />}
                      <div className={`max-w-[72%] px-4 py-2.5 rounded-2xl text-sm leading-relaxed whitespace-pre-wrap ${
                        m.role === 'user' ? 'bg-gray-900 text-white rounded-br-md'
                          : m.isError ? 'bg-red-50 text-red-600 border border-red-100 rounded-bl-md'
                          : m.isSuccess ? 'bg-emerald-50 text-emerald-700 border border-emerald-100 rounded-bl-md'
                          : 'bg-white text-gray-700 border border-gray-100 shadow-sm rounded-bl-md'
                      }`}>{m.text}</div>
                    </div>
                    {m.pending && <ConfirmInline pending={m.pending} onApprove={approve} onReject={reject} loading={loading} />}
                  </div>
                ))}
                {loading && <TypingDots color="violet" />}
                <div ref={bottomRef} />
              </div>
              <div className="px-6 pb-4 pt-2 bg-[#f5f7fb]">
                <div className="max-w-3xl mx-auto">
                  <ChatInput input={input} setInput={setInput} loading={loading} onSend={send}
                    placeholder="Describe a data operation…" accent="violet" />
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

/* ─── Shared ─── */

function ChatInput({ input, setInput, loading, onSend, placeholder, accent = 'blue' }) {
  const btnClass = accent === 'violet' ? 'bg-violet-500 hover:bg-violet-600 shadow-violet-500/20' : 'bg-blue-500 hover:bg-blue-600 shadow-blue-500/20';
  return (
    <div className="flex items-center gap-3 bg-white rounded-full px-4 py-3 shadow-sm border border-gray-100">
      <Pencil size={16} className="text-gray-400 flex-shrink-0" />
      <input value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && onSend()} placeholder={placeholder} disabled={loading}
        className="flex-1 bg-transparent outline-none text-sm text-gray-700 placeholder-gray-400 disabled:opacity-70" />
      <button onClick={() => onSend()} disabled={loading || !input.trim()}
        className={`w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 transition-all ${input.trim() ? `${btnClass} text-white shadow-sm` : 'bg-gray-100 text-gray-300 cursor-not-allowed'}`}>
        <ArrowUp size={16} />
      </button>
    </div>
  );
}

function AgentBadge({ color = 'blue' }) {
  const bg = color === 'violet' ? 'bg-violet-500 shadow-violet-500/20' : color === 'cyan' ? 'bg-cyan-500 shadow-cyan-500/20' : color === 'emerald' ? 'bg-emerald-500 shadow-emerald-500/20' : 'bg-blue-500 shadow-blue-500/20';
  return <div className={`w-7 h-7 rounded-lg ${bg} flex items-center justify-center flex-shrink-0 shadow-sm`}><Sparkles size={14} className="text-white" /></div>;
}

function TypingDots({ color = 'blue' }) {
  return (
    <div className="flex items-center gap-3">
      <AgentBadge color={color} />
      <div className="bg-white border border-gray-100 rounded-2xl rounded-bl-md px-4 py-3 shadow-sm flex gap-1.5">
        {[0,1,2].map(i => <div key={i} className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-pulse" style={{ animationDelay: `${i*200}ms` }} />)}
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
  const OP_COLORS = { CREATE: 'bg-blue-500', UPDATE: 'bg-violet-500', BULK_UPDATE: 'bg-amber-500', DELETE: 'bg-red-500', SOFT_DELETE: 'bg-red-500' };

  return (
    <div className="ml-10 mt-2 bg-white border border-gray-100 rounded-xl p-4 shadow-sm text-xs">
      <div className="flex items-center gap-2 mb-2">
        <span className={`px-2.5 py-0.5 rounded-full text-[10px] font-bold text-white uppercase tracking-wide ${OP_COLORS[diff.operation] || 'bg-blue-500'}`}>{diff.operation}</span>
        <span className="font-bold text-gray-800">{diff.table}</span>
        <span className="text-gray-300">{diff.rows_affected || '?'} row(s)</span>
      </div>
      <div className="text-gray-500 mb-3 leading-relaxed">{pending.description}</div>
      {changed.length > 0 && (
        <table className="w-full text-[11px] mb-3">
          <thead><tr className="bg-gray-50">
            <th className="px-2 py-1 text-left text-gray-400 font-semibold">Field</th>
            <th className="px-2 py-1 text-left text-gray-400 font-semibold">Before</th>
            <th className="px-2 py-1 text-left text-gray-400 font-semibold">After</th>
          </tr></thead>
          <tbody>{changed.map(k => (
            <tr key={k} className="border-t border-gray-50">
              <td className="px-2 py-1 font-semibold text-gray-700">{k}</td>
              <td className="px-2 py-1 text-red-400 line-through">{String(before[k] ?? '—')}</td>
              <td className="px-2 py-1 text-emerald-600 font-bold">{String(after[k] ?? '—')}</td>
            </tr>
          ))}</tbody>
        </table>
      )}
      <button onClick={() => setShowSql(!showSql)} className="text-violet-500 font-semibold text-[10px] mb-2 hover:underline">{showSql ? '▲ Hide SQL' : '▼ Show SQL'}</button>
      {showSql && <pre className="bg-gray-50 p-2.5 rounded-lg text-[10px] font-mono whitespace-pre-wrap border border-gray-100 mb-3">{pending.sql_preview}</pre>}
      <div className="flex gap-2">
        <button onClick={() => onApprove(pending.pending_id)} disabled={loading}
          className="px-4 py-1.5 rounded-lg bg-emerald-500 text-white text-[11px] font-bold hover:bg-emerald-600 shadow-sm shadow-emerald-500/20 transition-colors">Approve</button>
        <button onClick={() => onReject(pending.pending_id)}
          className="px-4 py-1.5 rounded-lg border-2 border-red-500 text-red-500 text-[11px] font-bold hover:bg-red-50 transition-colors">Reject</button>
      </div>
    </div>
  );
}

function HistoryPanel({ sessions, activeSessionId, onLoad, onDelete, onNew, onClose }) {
  return (
    <div className="w-64 flex-shrink-0 bg-white border-r border-gray-100 flex flex-col overflow-hidden">
      <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
        <div className="text-xs font-semibold text-gray-700 uppercase tracking-wider">History</div>
        <div className="flex items-center gap-1">
          <button onClick={onNew} className="p-1.5 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600"><Plus size={14} /></button>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600"><X size={14} /></button>
        </div>
      </div>
      {sessions.length === 0 ? <div className="p-6 text-center text-xs text-gray-300">No conversations yet</div> : (
        <div className="flex-1 overflow-y-auto">
          {sessions.map(s => {
            const isActive = s.id === activeSessionId;
            const count = s.messages?.length || 0;
            const date = s.updatedAt ? new Date(s.updatedAt) : null;
            return (
              <div key={s.id} onClick={() => onLoad(s)}
                className={`px-4 py-3 cursor-pointer border-b border-gray-50 transition-colors ${isActive ? 'bg-violet-50 border-l-2 border-l-violet-500' : 'hover:bg-gray-50 border-l-2 border-l-transparent'}`}>
                <div className="text-xs font-medium text-gray-700 truncate mb-1">{s.title || 'Untitled'}</div>
                <div className="flex items-center justify-between">
                  <span className="text-[10px] text-gray-300">{count} msg{count !== 1 ? 's' : ''}{date ? ` · ${date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}` : ''}</span>
                  <button onClick={e => { e.stopPropagation(); onDelete(s.id); }} className="text-gray-200 hover:text-red-400 transition-colors"><Trash2 size={12} /></button>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
