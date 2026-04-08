import { useState, useEffect, useRef, useCallback } from 'react';
import {
  Sparkles, ClipboardList, Brush, BarChart3, Upload, Search, Trash2,
  Pencil, ArrowUp, X, Plus, History, PanelRightOpen, PanelRightClose, Paperclip, RotateCcw
} from 'lucide-react';

const STORAGE_KEY = 'bss_data_agent_history';
function loadSessions() { try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); } catch { return []; } }
function saveSessions(s) { localStorage.setItem(STORAGE_KEY, JSON.stringify(s)); }

export default function DataManagementAgentPage() {
  const [tables, setTables] = useState([]);
  const [loadingTables, setLoadingTables] = useState(true);
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableSchema, setTableSchema] = useState(null);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const [schemaOpen, setSchemaOpen] = useState(false);
  const [tableSearch, setTableSearch] = useState('');

  const [fileId, setFileId] = useState(null);
  const [fileInfo, setFileInfo] = useState(null);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef(null);

  const [messages, setMessages] = useState([]);
  const [chatHistory, setChatHistory] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  const [historyOpen, setHistoryOpen] = useState(false);
  const [sessions, setSessions] = useState(loadSessions);
  const [activeSessionId, setActiveSessionId] = useState(null);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  // Auto-save session
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

  const fetchTables = () => {
    setLoadingTables(true);
    fetch('/api/data-agent/tables').then(r => r.json()).then(data => { setTables(Array.isArray(data) ? data : []); setLoadingTables(false); }).catch(() => setLoadingTables(false));
  };
  useEffect(() => { fetchTables(); }, []);

  const handleTableClick = (t) => {
    setSelectedTable(t); setLoadingSchema(true);
    fetch(`/api/data-agent/tables/${t.table}/schema?schema=${t.schema}`).then(r => r.json()).then(data => { setTableSchema(data); setLoadingSchema(false); }).catch(() => setLoadingSchema(false));
  };

  const handleFileUpload = async (e) => {
    const file = e.target.files[0]; if (!file) return;
    setUploading(true);
    addMsg('user', `Uploading: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`);
    const formData = new FormData(); formData.append('file', file);
    try {
      const res = await fetch('/api/data-agent/upload', { method: 'POST', body: formData });
      const data = await res.json();
      if (data.error) addMsg('agent', `Upload error: ${data.error}`, { isError: true });
      else {
        setFileId(data.file_id); setFileInfo(data);
        addMsg('agent', `File ready!\n**${data.original_name}** — ${data.row_count?.toLocaleString()} rows, ${data.num_columns} columns, ${data.file_size_human}\nEst. upload time: ${data.estimated_time}\n\nWhat would you like to do?\n• "Sanitize the file"\n• "Upload to table_name"\n• "Show me existing tables"`, { filePreview: data });
      }
    } catch (err) { addMsg('agent', `Network error: ${err.message}`, { isError: true }); }
    setUploading(false); e.target.value = '';
  };

  const addMsg = (role, text, extra = {}) => { setMessages(prev => [...prev, { role, text, ts: new Date().toISOString(), ...extra }]); };

  const send = async (text) => {
    const msg = (text || input).trim(); if (!msg || loading) return;
    setInput(''); addMsg('user', msg);
    const newHistory = [...chatHistory, { role: 'user', content: msg }]; setChatHistory(newHistory);
    setLoading(true);
    try {
      const res = await fetch('/api/data-agent/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: msg, history: newHistory, file_id: fileId }) });
      const data = await res.json();
      if (data.error) addMsg('agent', `Error: ${data.error}`, { isError: true });
      else {
        const extra = {};
        if (data.query_result) extra.queryResult = data.query_result;
        if (data.sanitize_result) setFileInfo(prev => prev ? { ...prev, headers: data.sanitize_result.headers, row_count: data.sanitize_result.rows_after } : prev);
        if (data.tables_refreshed) fetchTables();
        addMsg('agent', data.reply || 'Done.', extra);
        setChatHistory(prev => [...prev, { role: 'assistant', content: data.reply || '' }]);
      }
    } catch (err) { addMsg('agent', `Network error: ${err.message}`, { isError: true }); }
    setLoading(false);
  };

  const CARDS = [
    { icon: ClipboardList, title: 'List all tables', desc: 'View every table in the database' },
    { icon: Brush, title: 'Sanitize the uploaded file', desc: 'Clean headers, trim whitespace, deduplicate data' },
    { icon: BarChart3, title: 'Show schema of checklist', desc: 'View columns, types, and sample data' },
    { icon: Upload, title: 'Upload file to a new table', desc: 'Bulk-load CSV into PostgreSQL' },
    { icon: Search, title: 'Run: SELECT COUNT(*) FROM phases', desc: 'Execute a read-only SQL query' },
    { icon: Trash2, title: 'Drop a table', desc: 'Remove a table with confirmation' },
  ];

  const hasMessages = messages.length > 0;
  const filteredTables = tables.filter(t => t.table.toLowerCase().includes(tableSearch.toLowerCase()));
  const userTables = filteredTables.filter(t => !['users','pending_writes','agent_audit_log','stages'].includes(t.table));
  const systemTables = filteredTables.filter(t => ['users','pending_writes','agent_audit_log','stages'].includes(t.table));

  return (
    <div className="flex flex-col h-screen bg-[#f5f7fb] font-sans">
      <div className="flex-1 flex overflow-hidden">

        {/* History panel */}
        {historyOpen && (
          <HistoryPanel sessions={sessions} activeSessionId={activeSessionId}
            onLoad={loadSession} onDelete={deleteSession} onNew={startNewChat}
            onClose={() => setHistoryOpen(false)} />
        )}

        {/* Main content */}
        <div className="flex-1 flex flex-col min-w-0 overflow-hidden">

          {/* Top bar */}
          <div className="flex items-center justify-between px-6 py-3 bg-white border-b border-gray-100">
            <div className="flex items-center gap-2">
              <button onClick={() => setHistoryOpen(!historyOpen)} className="p-2 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600 transition-colors" title="Chat History">
                <History size={18} />
              </button>
              {hasMessages && (
                <button onClick={startNewChat} className="p-2 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600 transition-colors" title="New Chat">
                  <Plus size={18} />
                </button>
              )}
            </div>
            <div className="flex items-center gap-2">
              <button onClick={() => fileInputRef.current?.click()} disabled={uploading}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-gray-500 hover:bg-gray-50 hover:text-gray-700 transition-colors border border-gray-200">
                <Paperclip size={14} />
                {uploading ? 'Uploading...' : 'Upload CSV/PSV'}
              </button>
              <input ref={fileInputRef} type="file" accept=".csv,.psv,.txt" onChange={handleFileUpload} className="hidden" />
              <button onClick={() => setSchemaOpen(!schemaOpen)}
                className={`p-2 rounded-lg transition-colors ${schemaOpen ? 'bg-blue-50 text-blue-600' : 'text-gray-400 hover:bg-gray-50 hover:text-gray-600'}`} title="Database Tables">
                {schemaOpen ? <PanelRightClose size={18} /> : <PanelRightOpen size={18} />}
              </button>
            </div>
          </div>

          {/* File info bar */}
          {fileInfo && (
            <div className="mx-6 mt-3 px-4 py-2.5 bg-white rounded-xl border border-gray-100 shadow-sm flex items-center gap-3 text-xs">
              <div className="w-8 h-8 rounded-lg bg-blue-50 flex items-center justify-center"><Upload size={16} className="text-blue-500" /></div>
              <div className="flex-1 min-w-0">
                <div className="font-semibold text-gray-900 truncate">{fileInfo.original_name}</div>
                <div className="text-gray-400 mt-0.5">{fileInfo.row_count?.toLocaleString()} rows · {fileInfo.headers?.length || fileInfo.num_columns} cols · {fileInfo.file_size_human}
                  {fileInfo.sanitized !== false && <span className="ml-2 text-emerald-500 font-medium">✓ Sanitized</span>}
                </div>
              </div>
              <button onClick={() => { setFileId(null); setFileInfo(null); }} className="p-1 rounded hover:bg-gray-100 text-gray-300 hover:text-gray-500"><X size={14} /></button>
            </div>
          )}

          {/* Welcome state */}
          {!hasMessages ? (
            <div className="flex-1 flex flex-col items-center justify-center px-6 py-12">
              {/* Hero */}
              <div className="w-14 h-14 rounded-2xl bg-blue-500 flex items-center justify-center mb-6 shadow-lg shadow-blue-500/20">
                <Sparkles size={28} className="text-white" />
              </div>
              <h1 className="text-2xl font-bold text-gray-900 mb-2 text-center">How can I assist your ledger today?</h1>
              <p className="text-sm text-gray-500 text-center max-w-lg mb-10">
                Upload files, run SQL queries, sanitize data, and manage your database tables — all through natural language.
              </p>

              {/* Cards grid */}
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 max-w-3xl w-full mb-10">
                {CARDS.map((card, i) => {
                  const Icon = card.icon;
                  return (
                    <button key={i} onClick={() => send(card.title)}
                      className="bg-white rounded-xl p-5 text-left shadow-sm hover:shadow-md transition-shadow border border-gray-50 group cursor-pointer">
                      <Icon size={20} className="text-gray-400 group-hover:text-blue-500 transition-colors mb-3" />
                      <div className="text-sm font-semibold text-gray-800 mb-1">{card.title}</div>
                      <div className="text-xs text-gray-500 leading-relaxed">{card.desc}</div>
                    </button>
                  );
                })}
              </div>

              {/* Input bar */}
              <div className="w-full max-w-2xl">
                <ChatInput input={input} setInput={setInput} loading={loading} onSend={send} onFile={() => fileInputRef.current?.click()}
                  placeholder="Ask me to list tables, sanitise, upload, run SQL…" />
              </div>
            </div>
          ) : (
            /* Chat view */
            <>
              <div className="flex-1 overflow-y-auto px-6 py-5 space-y-4">
                {messages.map((m, i) => <MessageBubble key={i} m={m} />)}
                {loading && <TypingDots />}
                <div ref={bottomRef} />
              </div>
              <div className="px-6 pb-4 pt-2 bg-[#f5f7fb]">
                <div className="max-w-3xl mx-auto">
                  <ChatInput input={input} setInput={setInput} loading={loading} onSend={send} onFile={() => fileInputRef.current?.click()}
                    placeholder="Ask me to list tables, sanitise, upload, run SQL…" />
                </div>
              </div>
            </>
          )}
        </div>

        {/* Tables panel */}
        {schemaOpen && (
          <div className="w-72 flex-shrink-0 bg-white border-l border-gray-100 flex flex-col overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
              <div>
                <div className="text-xs font-semibold text-gray-800 uppercase tracking-wider">Tables</div>
                <div className="text-[10px] text-gray-400 mt-0.5">{tables.length} total · {tables.filter(t => t.row_count > 0).length} with data</div>
              </div>
              <button onClick={fetchTables} className="p-1.5 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600"><RotateCcw size={14} /></button>
            </div>
            <div className="px-3 py-2 border-b border-gray-50">
              <input value={tableSearch} onChange={e => setTableSearch(e.target.value)} placeholder="Search tables..."
                className="w-full px-3 py-1.5 rounded-lg bg-gray-50 border-0 text-xs text-gray-700 placeholder-gray-300 outline-none focus:ring-1 focus:ring-blue-200" />
            </div>
            <div className={`overflow-y-auto ${selectedTable ? 'max-h-48' : 'flex-1'}`}>
              {loadingTables ? <div className="p-4 text-center text-xs text-gray-300">Loading...</div> : (
                <>
                  {userTables.length > 0 && <SectionLabel text="Project Tables" />}
                  {userTables.map((t, i) => <TableItem key={i} t={t} active={selectedTable?.table === t.table} onClick={() => handleTableClick(t)} />)}
                  {systemTables.length > 0 && <SectionLabel text="System Tables" />}
                  {systemTables.map((t, i) => <TableItem key={i} t={t} active={selectedTable?.table === t.table} onClick={() => handleTableClick(t)} />)}
                  {filteredTables.length === 0 && <div className="p-4 text-center text-xs text-gray-300">No matches</div>}
                </>
              )}
            </div>
            {selectedTable && (
              <div className="flex-1 overflow-y-auto p-4 border-t border-gray-100">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-sm font-bold text-gray-800">{selectedTable.table}</span>
                  <button onClick={() => { setSelectedTable(null); setTableSchema(null); }} className="text-gray-300 hover:text-gray-500"><X size={14} /></button>
                </div>
                {loadingSchema ? <div className="text-xs text-gray-300">Loading...</div> : tableSchema && (
                  <>
                    <div className="text-[10px] text-gray-400 mb-2">{tableSchema.row_count} rows · {tableSchema.columns?.length} cols</div>
                    <table className="w-full text-[10px]">
                      <thead><tr className="bg-gray-50">
                        <th className="px-2 py-1 text-left font-semibold text-gray-500">Column</th>
                        <th className="px-2 py-1 text-left font-semibold text-gray-500">Type</th>
                      </tr></thead>
                      <tbody>{(tableSchema.columns || []).map((c, i) => (
                        <tr key={i} className="border-t border-gray-50">
                          <td className="px-2 py-1 font-medium text-gray-700">{c.column_name}{tableSchema.primary_keys?.includes(c.column_name) && <span className="ml-1 text-amber-400">🔑</span>}</td>
                          <td className="px-2 py-1 text-gray-400">{c.data_type}</td>
                        </tr>
                      ))}</tbody>
                    </table>
                  </>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

/* ─── Shared Components ─── */

function ChatInput({ input, setInput, loading, onSend, onFile, placeholder }) {
  return (
    <div className="flex items-center gap-3 bg-white rounded-full px-4 py-3 shadow-sm border border-gray-100">
      {onFile && (
        <button onClick={onFile} className="text-gray-400 hover:text-gray-600 transition-colors flex-shrink-0">
          <Pencil size={16} />
        </button>
      )}
      <input value={input} onChange={e => setInput(e.target.value)}
        onKeyDown={e => e.key === 'Enter' && onSend()}
        placeholder={placeholder}
        disabled={loading}
        className="flex-1 bg-transparent outline-none text-sm text-gray-700 placeholder-gray-400 disabled:opacity-70" />
      <button onClick={() => onSend()} disabled={loading || !input.trim()}
        className={`w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 transition-all ${
          input.trim() ? 'bg-blue-500 hover:bg-blue-600 text-white shadow-sm shadow-blue-500/20' : 'bg-gray-100 text-gray-300 cursor-not-allowed'
        }`}>
        <ArrowUp size={16} />
      </button>
    </div>
  );
}

function MessageBubble({ m }) {
  const isUser = m.role === 'user';
  return (
    <div className={`flex items-start gap-3 ${isUser ? 'justify-end' : ''} animate-fadeIn`}>
      {!isUser && (
        <div className="w-7 h-7 rounded-lg bg-blue-500 flex items-center justify-center flex-shrink-0 shadow-sm shadow-blue-500/20">
          <Sparkles size={14} className="text-white" />
        </div>
      )}
      <div className={`max-w-[72%] px-4 py-2.5 rounded-2xl text-sm leading-relaxed whitespace-pre-wrap ${
        isUser
          ? 'bg-gray-900 text-white rounded-br-md'
          : m.isError
            ? 'bg-red-50 text-red-600 border border-red-100 rounded-bl-md'
            : 'bg-white text-gray-700 border border-gray-100 shadow-sm rounded-bl-md'
      }`}>
        {m.text}
      </div>

      {/* File preview */}
      {!isUser && m.filePreview?.sample_rows?.length > 0 && (
        <div className="mt-2 ml-10 overflow-x-auto rounded-xl border border-gray-100 shadow-sm">
          <table className="text-[10px] w-full">
            <thead><tr className="bg-gray-50">
              {m.filePreview.headers.map((h, j) => <th key={j} className="px-3 py-1.5 text-left font-semibold text-gray-600 whitespace-nowrap">{h}</th>)}
            </tr></thead>
            <tbody>{m.filePreview.sample_rows.slice(0, 3).map((row, ri) => (
              <tr key={ri} className="border-t border-gray-50">{row.map((c, ci) => <td key={ci} className="px-3 py-1 text-gray-500 max-w-[120px] truncate">{c ?? '—'}</td>)}</tr>
            ))}</tbody>
          </table>
        </div>
      )}

      {/* Query result */}
      {!isUser && m.queryResult?.length > 0 && (
        <div className="mt-2 ml-10 overflow-x-auto max-h-64 overflow-y-auto rounded-xl border border-gray-100 shadow-sm">
          <table className="text-[10px] w-full">
            <thead><tr className="bg-gray-50 sticky top-0">
              {Object.keys(m.queryResult[0]).map(k => <th key={k} className="px-3 py-1.5 text-left font-semibold text-gray-600 whitespace-nowrap">{k}</th>)}
            </tr></thead>
            <tbody>{m.queryResult.map((row, ri) => (
              <tr key={ri} className="border-t border-gray-50">{Object.values(row).map((v, ci) => <td key={ci} className="px-3 py-1 text-gray-500">{v != null ? String(v) : <span className="text-gray-200">NULL</span>}</td>)}</tr>
            ))}</tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function TypingDots() {
  return (
    <div className="flex items-center gap-3">
      <div className="w-7 h-7 rounded-lg bg-blue-500 flex items-center justify-center flex-shrink-0 shadow-sm shadow-blue-500/20">
        <Sparkles size={14} className="text-white" />
      </div>
      <div className="bg-white border border-gray-100 rounded-2xl rounded-bl-md px-4 py-3 shadow-sm flex gap-1.5">
        {[0,1,2].map(i => <div key={i} className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-pulse" style={{ animationDelay: `${i*200}ms` }} />)}
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
      {sessions.length === 0 ? (
        <div className="p-6 text-center text-xs text-gray-300">No conversations yet</div>
      ) : (
        <div className="flex-1 overflow-y-auto">
          {sessions.map(s => {
            const isActive = s.id === activeSessionId;
            const count = s.messages?.length || 0;
            const date = s.updatedAt ? new Date(s.updatedAt) : null;
            return (
              <div key={s.id} onClick={() => onLoad(s)}
                className={`px-4 py-3 cursor-pointer border-b border-gray-50 transition-colors ${isActive ? 'bg-blue-50 border-l-2 border-l-blue-500' : 'hover:bg-gray-50 border-l-2 border-l-transparent'}`}>
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

function SectionLabel({ text }) {
  return <div className="px-4 py-1.5 text-[9px] font-bold text-gray-300 uppercase tracking-widest bg-gray-50/50">{text}</div>;
}

function TableItem({ t, active, onClick }) {
  return (
    <button onClick={onClick}
      className={`w-full flex items-center justify-between px-4 py-2 text-xs transition-colors border-l-2 ${
        active ? 'bg-blue-50 border-l-blue-500 text-blue-600' : 'border-l-transparent text-gray-600 hover:bg-gray-50'
      }`}>
      <span className="font-medium">{t.table}</span>
      <span className={`text-[10px] px-2 py-0.5 rounded-full font-medium ${t.row_count > 0 ? 'bg-emerald-50 text-emerald-500' : 'bg-gray-50 text-gray-300'}`}>{t.row_count}</span>
    </button>
  );
}
