import { useState, useRef, useEffect, useCallback } from 'react';
import {
  Sparkles, ArrowUp, X, Plus, History, Trash2,
  Pencil, Database, PenLine, FileSearch, FileText,
  Paperclip, PanelRightOpen, PanelRightClose, Upload, RotateCcw,
  ListChecks, AlertTriangle, BookOpen, RefreshCw, Search, BarChart3,
  Brush, ClipboardList, Shield, TrendingUp, CheckCircle2, Zap
} from 'lucide-react';

const STORAGE_KEY = 'bss_agent_unified_history';
function loadSessions() { try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); } catch { return []; } }
function saveSessions(s) { localStorage.setItem(STORAGE_KEY, JSON.stringify(s)); }

const AGENTS = {
  data: {
    id: 'data', name: 'Data Management', command: '/data',
    icon: Database, color: 'blue', colorClass: 'bg-blue-500 shadow-blue-500/20',
    btnClass: 'bg-blue-500 hover:bg-blue-600 shadow-blue-500/20',
    desc: 'Upload CSV/PSV files, sanitize data, run SQL queries, manage database tables.',
    suggestions: [
      { icon: ClipboardList, text: 'List all tables in the database' },
      { icon: Brush, text: 'Sanitize the uploaded file — clean headers and deduplicate' },
      { icon: BarChart3, text: 'Show me the schema of the checklist table' },
      { icon: Upload, text: 'Upload my file to a new PostgreSQL table' },
      { icon: Search, text: 'Run SELECT COUNT(*) FROM phases' },
      { icon: Trash2, text: 'Drop the temp_import table' },
    ],
  },
  crud: {
    id: 'crud', name: 'CRUD Operations', command: '/crud',
    icon: PenLine, color: 'violet', colorClass: 'bg-violet-500 shadow-violet-500/20',
    btnClass: 'bg-violet-500 hover:bg-violet-600 shadow-violet-500/20',
    desc: 'Create, read, update, delete records with AI-generated SQL and diff preview before execution.',
    suggestions: [
      { icon: BookOpen, text: 'Show all open UAT test cases' },
      { icon: PenLine, text: 'Close all ICT test cases' },
      { icon: Plus, text: 'Add a new Broadband product to products table' },
      { icon: RefreshCw, text: 'Update phase 5 status to complete' },
      { icon: Search, text: 'Show products flagged for migration' },
      { icon: BarChart3, text: 'Show transformation activities by LOB' },
    ],
  },
  reconciliation: {
    id: 'reconciliation', name: 'Reconciliation', command: '/recon',
    icon: FileSearch, color: 'cyan', colorClass: 'bg-cyan-500 shadow-cyan-500/20',
    btnClass: 'bg-cyan-500 hover:bg-cyan-600 shadow-cyan-500/20',
    desc: 'Compare CBS vs CLM data, find status mismatches, and generate reconciliation reports.',
    suggestions: [
      { icon: Search, text: 'How many records have mismatched status?' },
      { icon: BarChart3, text: 'Summarise reconciliation by status' },
      { icon: Zap, text: 'CBS active but CLM inactive — find discrepancies' },
      { icon: TrendingUp, text: 'Account codes with most discrepancies' },
      { icon: Shield, text: 'Top 10 service codes by count' },
      { icon: CheckCircle2, text: 'What percentage is fully reconciled?' },
    ],
  },
  ocr: {
    id: 'ocr', name: 'Document Analysis', command: '/doc',
    icon: FileText, color: 'emerald', colorClass: 'bg-emerald-500 shadow-emerald-500/20',
    btnClass: 'bg-emerald-500 hover:bg-emerald-600 shadow-emerald-500/20',
    desc: 'Upload PDFs, extract text, configure export format (CSV/XLSX/TXT), preview and download.',
    suggestions: [
      { icon: ListChecks, text: 'Extract all line items and totals from the PDF' },
      { icon: FileText, text: 'Summarise key details from each document' },
      { icon: AlertTriangle, text: 'Flag discrepancies across uploaded documents' },
    ],
  },
};

const AGENT_LIST = Object.values(AGENTS);
const AGENT_PAGE_MAP = { data: '/agent/data', crud: '/agent/crud', reconciliation: '/agent/reconciliation', ocr: '/agent/ocr' };

export default function AgentChatPage() {
  const [activeAgent, setActiveAgent] = useState(null);
  const [messages, setMessages] = useState([]);
  const [chatHistory, setChatHistory] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [showSlashMenu, setShowSlashMenu] = useState(false);
  const [slashFilter, setSlashFilter] = useState('');
  const bottomRef = useRef(null);
  const inputRef = useRef(null);

  // History
  const [historyOpen, setHistoryOpen] = useState(false);
  const [sessions, setSessions] = useState(loadSessions);
  const [activeSessionId, setActiveSessionId] = useState(null);

  // Data agent specific
  const [fileId, setFileId] = useState(null);
  const [fileInfo, setFileInfo] = useState(null);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef(null);

  // OCR specific
  const [uploadedPdfs, setUploadedPdfs] = useState([]);
  const pdfInputRef = useRef(null);

  // Tables panel (data agent)
  const [tables, setTables] = useState([]);
  const [schemaOpen, setSchemaOpen] = useState(false);
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableSchema, setTableSchema] = useState(null);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const [tableSearch, setTableSearch] = useState('');

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  // Session persistence
  const saveCurrentSession = useCallback(() => {
    if (messages.length === 0) return;
    const title = messages.find(m => m.role === 'user')?.text?.slice(0, 50) || 'New conversation';
    const updated = loadSessions();
    if (activeSessionId) {
      const idx = updated.findIndex(s => s.id === activeSessionId);
      if (idx >= 0) { updated[idx].messages = messages; updated[idx].title = title; updated[idx].agent = activeAgent; updated[idx].updatedAt = new Date().toISOString(); }
    } else {
      const id = Date.now().toString();
      updated.unshift({ id, title, messages, agent: activeAgent, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });
      setActiveSessionId(id);
    }
    saveSessions(updated); setSessions(updated);
  }, [messages, activeSessionId, activeAgent]);
  useEffect(() => { if (messages.length > 0) saveCurrentSession(); }, [messages.length]);

  const loadSession = (s) => { setMessages(s.messages || []); setActiveSessionId(s.id); setActiveAgent(s.agent || null); setChatHistory([]); };
  const deleteSession = (id) => { const u = loadSessions().filter(s => s.id !== id); saveSessions(u); setSessions(u); if (activeSessionId === id) startNewChat(); };
  const startNewChat = () => { setMessages([]); setChatHistory([]); setActiveSessionId(null); setActiveAgent(null); setFileId(null); setFileInfo(null); setUploadedPdfs([]); setSchemaOpen(false); };

  const addMsg = (role, text, extra = {}) => { setMessages(prev => [...prev, { role, text, ts: new Date().toISOString(), ...extra }]); };

  // Fetch tables for data agent
  const fetchTables = () => {
    fetch('/api/data-agent/tables').then(r => r.json()).then(data => setTables(Array.isArray(data) ? data : [])).catch(() => {});
  };
  useEffect(() => { fetchTables(); }, []);

  const handleTableClick = (t) => {
    setSelectedTable(t); setLoadingSchema(true);
    fetch(`/api/data-agent/tables/${t.table}/schema?schema=${t.schema}`).then(r => r.json()).then(data => { setTableSchema(data); setLoadingSchema(false); }).catch(() => setLoadingSchema(false));
  };

  // File upload (data agent)
  const handleFileUpload = async (e) => {
    const file = e.target.files[0]; if (!file) return;
    setUploading(true);
    if (!activeAgent) setActiveAgent('data');
    addMsg('user', `Uploading: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`);
    const formData = new FormData(); formData.append('file', file);
    try {
      const res = await fetch('/api/data-agent/upload', { method: 'POST', body: formData });
      const data = await res.json();
      if (data.error) addMsg('agent', `Upload error: ${data.error}`, { isError: true });
      else {
        setFileId(data.file_id); setFileInfo(data);
        addMsg('agent', `File ready!\n**${data.original_name}** — ${data.row_count?.toLocaleString()} rows, ${data.num_columns} columns, ${data.file_size_human}\n\nTry:\n• "Sanitize the file"\n• "Upload to table_name"\n• "Show me existing tables"`, { filePreview: data });
      }
    } catch (err) { addMsg('agent', `Network error: ${err.message}`, { isError: true }); }
    setUploading(false); e.target.value = '';
  };

  // PDF upload (ocr agent)
  const handlePdfUpload = async (e) => {
    const files = Array.from(e.target.files); if (!files.length) return;
    if (!activeAgent) setActiveAgent('ocr');
    setUploading(true);
    const uploaded = [];
    for (const file of files) {
      const formData = new FormData(); formData.append('file', file);
      try { const res = await fetch('/api/agent/upload-pdf', { method: 'POST', body: formData }); const data = await res.json(); if (data.file_path) uploaded.push({ name: file.name, path: data.file_path }); }
      catch { addMsg('agent', `Failed to upload ${file.name}`, { isError: true }); }
    }
    if (uploaded.length > 0) { setUploadedPdfs(prev => [...prev, ...uploaded]); addMsg('agent', `Uploaded ${uploaded.length} PDF(s): ${uploaded.map(f => f.name).join(', ')}.`); }
    setUploading(false); e.target.value = '';
  };

  // Slash command handling
  const handleInputChange = (val) => {
    setInput(val);
    if (val.startsWith('/')) {
      setShowSlashMenu(true);
      setSlashFilter(val.slice(1).toLowerCase());
    } else {
      setShowSlashMenu(false);
    }
  };

  const selectAgent = (agentId) => {
    setActiveAgent(agentId);
    setInput('');
    setShowSlashMenu(false);
    const a = AGENTS[agentId];
    addMsg('system', `Switched to **${a.name}** agent. ${a.desc}`);
    inputRef.current?.focus();
  };

  // CRUD approve/reject
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

  const send = async (text) => {
    const msg = (text || input).trim(); if (!msg || loading) return;
    setInput(''); setShowSlashMenu(false);

    // Handle slash commands in message
    const slashMatch = msg.match(/^\/(data|crud|recon|doc)\b/i);
    if (slashMatch) {
      const cmd = slashMatch[1].toLowerCase();
      const map = { data: 'data', crud: 'crud', recon: 'reconciliation', doc: 'ocr' };
      const agentId = map[cmd];
      const remainder = msg.slice(slashMatch[0].length).trim();
      setActiveAgent(agentId);
      if (!remainder) {
        addMsg('system', `Switched to **${AGENTS[agentId].name}** agent. ${AGENTS[agentId].desc}`);
        return;
      }
      addMsg('user', remainder);
      await sendToAgent(agentId, remainder);
      return;
    }

    if (!activeAgent) {
      addMsg('user', msg);
      addMsg('system', 'Please select an agent first. Type **/** to see available agents, or click one of the cards below.');
      return;
    }

    addMsg('user', msg);
    await sendToAgent(activeAgent, msg);
  };

  const sendToAgent = async (agentId, msg) => {
    const newHistory = [...chatHistory, { role: 'user', content: msg }]; setChatHistory(newHistory);
    setLoading(true);
    try {
      let res, data;
      if (agentId === 'data') {
        res = await fetch('/api/data-agent/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: msg, history: newHistory, file_id: fileId }) });
        data = await res.json();
        if (data.error) addMsg('agent', `Error: ${data.error}`, { isError: true });
        else {
          const extra = {};
          if (data.query_result) extra.queryResult = data.query_result;
          if (data.tables_refreshed) fetchTables();
          addMsg('agent', data.reply || 'Done.', extra);
          setChatHistory(prev => [...prev, { role: 'assistant', content: data.reply || '' }]);
        }
      } else if (agentId === 'crud') {
        res = await fetch('/api/agent/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: msg, history: newHistory, current_page: '/agent/crud' }) });
        data = await res.json();
        if (data.error) addMsg('agent', `Error: ${data.error}`, { isError: true });
        else if (data.reply) { addMsg('agent', data.reply, { pending: data.pending_confirmation }); setChatHistory(prev => [...prev, { role: 'assistant', content: data.reply }]); }
      } else if (agentId === 'reconciliation') {
        res = await fetch('/api/agent/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: `Use the reconciliation_tool to answer: ${msg}`, history: newHistory, current_page: '/agent/reconciliation' }) });
        data = await res.json();
        if (data.error) addMsg('agent', `Error: ${data.error}`, { isError: true });
        else if (data.reply) { addMsg('agent', data.reply, { toolUsed: data.tool_used }); setChatHistory(prev => [...prev, { role: 'assistant', content: data.reply }]); }
      } else if (agentId === 'ocr') {
        if (uploadedPdfs.length === 0) { addMsg('agent', 'Please upload at least one PDF file first. Use the upload button in the top bar.'); setLoading(false); return; }
        res = await fetch('/api/agent/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: `Use the ocr_tool with file_paths ${JSON.stringify(uploadedPdfs.map(f => f.path))} to answer: ${msg}`, history: newHistory, current_page: '/agent/ocr' }) });
        data = await res.json();
        if (data.error) addMsg('agent', `Error: ${data.error}`, { isError: true });
        else if (data.reply) { addMsg('agent', data.reply, { toolUsed: data.tool_used }); setChatHistory(prev => [...prev, { role: 'assistant', content: data.reply }]); }
      }
    } catch (err) { addMsg('agent', `Network error: ${err.message}`, { isError: true }); }
    setLoading(false);
  };

  const agent = activeAgent ? AGENTS[activeAgent] : null;
  const hasMessages = messages.length > 0;
  const filteredAgents = AGENT_LIST.filter(a => !slashFilter || a.command.slice(1).includes(slashFilter) || a.name.toLowerCase().includes(slashFilter));

  const filteredTables = tables.filter(t => t.table.toLowerCase().includes(tableSearch.toLowerCase()));
  const userTables = filteredTables.filter(t => !['users','pending_writes','agent_audit_log','stages'].includes(t.table));
  const systemTables = filteredTables.filter(t => ['users','pending_writes','agent_audit_log','stages'].includes(t.table));

  return (
    <div className="flex flex-col h-screen bg-[#f5f7fb] font-sans">
      <div className="flex-1 flex overflow-hidden">
        {/* History panel */}
        {historyOpen && (
          <div className="w-64 flex-shrink-0 bg-white border-r border-gray-100 flex flex-col overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
              <div className="text-xs font-semibold text-gray-700 uppercase tracking-wider">History</div>
              <div className="flex items-center gap-1">
                <button onClick={startNewChat} className="p-1.5 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600"><Plus size={14} /></button>
                <button onClick={() => setHistoryOpen(false)} className="p-1.5 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600"><X size={14} /></button>
              </div>
            </div>
            {sessions.length === 0 ? <div className="p-6 text-center text-xs text-gray-400">No conversations yet</div> : (
              <div className="flex-1 overflow-y-auto">
                {sessions.map(s => {
                  const isActive = s.id === activeSessionId;
                  const count = s.messages?.length || 0;
                  const date = s.updatedAt ? new Date(s.updatedAt) : null;
                  const sAgent = s.agent ? AGENTS[s.agent] : null;
                  return (
                    <div key={s.id} onClick={() => loadSession(s)}
                      className={`px-4 py-3 cursor-pointer border-b border-gray-50 transition-colors ${isActive ? 'bg-blue-50 border-l-2 border-l-blue-500' : 'hover:bg-gray-50 border-l-2 border-l-transparent'}`}>
                      <div className="flex items-center gap-1.5 mb-1">
                        {sAgent && <span className={`w-2 h-2 rounded-full ${sAgent.colorClass.split(' ')[0]}`} />}
                        <div className="text-xs font-medium text-gray-700 truncate">{s.title || 'Untitled'}</div>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-[10px] text-gray-400">{sAgent?.name || 'General'} · {count} msg{count !== 1 ? 's' : ''}{date ? ` · ${date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })}` : ''}</span>
                        <button onClick={e => { e.stopPropagation(); deleteSession(s.id); }} className="text-gray-300 hover:text-red-400 transition-colors"><Trash2 size={12} /></button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}

        {/* Main area */}
        <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
          {/* Top bar */}
          <div className="flex items-center justify-between px-6 py-3 bg-white border-b border-gray-100">
            <div className="flex items-center gap-2">
              <button onClick={() => setHistoryOpen(!historyOpen)} className="p-2 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600 transition-colors"><History size={18} /></button>
              {hasMessages && <button onClick={startNewChat} className="p-2 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600 transition-colors"><Plus size={18} /></button>}
              {agent && (
                <div className="flex items-center gap-2 ml-2 px-3 py-1 rounded-full bg-gray-50 border border-gray-100">
                  <span className={`w-2.5 h-2.5 rounded-full ${agent.colorClass.split(' ')[0]}`} />
                  <span className="text-xs font-semibold text-gray-600">{agent.name}</span>
                  <button onClick={() => { setActiveAgent(null); }} className="text-gray-300 hover:text-gray-500"><X size={12} /></button>
                </div>
              )}
            </div>
            <div className="flex items-center gap-2">
              {/* Upload buttons */}
              <button onClick={() => fileInputRef.current?.click()} disabled={uploading}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-gray-500 hover:bg-gray-50 hover:text-gray-700 transition-colors border border-gray-200">
                <Paperclip size={14} /> CSV/PSV
              </button>
              <input ref={fileInputRef} type="file" accept=".csv,.psv,.txt" onChange={handleFileUpload} className="hidden" />
              <button onClick={() => pdfInputRef.current?.click()} disabled={uploading}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-gray-500 hover:bg-gray-50 hover:text-gray-700 transition-colors border border-gray-200">
                <FileText size={14} /> PDF
              </button>
              <input ref={pdfInputRef} type="file" accept=".pdf" multiple onChange={handlePdfUpload} className="hidden" />
              {activeAgent === 'data' && (
                <button onClick={() => setSchemaOpen(!schemaOpen)}
                  className={`p-2 rounded-lg transition-colors ${schemaOpen ? 'bg-blue-50 text-blue-600' : 'text-gray-400 hover:bg-gray-50 hover:text-gray-600'}`}>
                  {schemaOpen ? <PanelRightClose size={18} /> : <PanelRightOpen size={18} />}
                </button>
              )}
            </div>
          </div>

          {/* File info bars */}
          {fileInfo && (
            <div className="mx-6 mt-3 px-4 py-2.5 bg-white rounded-xl border border-gray-100 shadow-sm flex items-center gap-3 text-xs">
              <div className="w-8 h-8 rounded-lg bg-blue-50 flex items-center justify-center"><Upload size={16} className="text-blue-500" /></div>
              <div className="flex-1 min-w-0">
                <div className="font-semibold text-gray-900 truncate">{fileInfo.original_name}</div>
                <div className="text-gray-500 mt-0.5">{fileInfo.row_count?.toLocaleString()} rows · {fileInfo.headers?.length || fileInfo.num_columns} cols · {fileInfo.file_size_human}</div>
              </div>
              <button onClick={() => { setFileId(null); setFileInfo(null); }} className="p-1 rounded hover:bg-gray-100 text-gray-300 hover:text-gray-500"><X size={14} /></button>
            </div>
          )}
          {uploadedPdfs.length > 0 && (
            <div className="px-6 py-2.5 bg-white border-b border-gray-50 flex items-center gap-2 flex-wrap">
              {uploadedPdfs.map((f, i) => (
                <span key={i} className="inline-flex items-center gap-1.5 px-3 py-1 bg-emerald-50 rounded-lg text-[11px] font-medium text-emerald-700 border border-emerald-100">
                  <FileText size={12} /> {f.name}
                  <button onClick={() => setUploadedPdfs(p => p.filter((_, j) => j !== i))} className="text-emerald-300 hover:text-red-400"><X size={12} /></button>
                </span>
              ))}
            </div>
          )}

          {/* Welcome state — no messages */}
          {!hasMessages ? (
            <div className="flex-1 flex flex-col items-center justify-center px-6 py-8 overflow-y-auto">
              {/* Chat input in center */}
              <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-blue-500 to-violet-500 flex items-center justify-center mb-5 shadow-lg shadow-blue-500/20">
                <Sparkles size={28} className="text-white" />
              </div>
              <h1 className="text-2xl font-bold text-gray-900 mb-2 text-center">AI Agent Hub</h1>
              <p className="text-sm text-gray-500 text-center max-w-lg mb-6">
                Type <kbd className="px-1.5 py-0.5 bg-gray-100 rounded text-xs font-mono text-gray-600">/</kbd> to select an agent, or click a suggestion below to get started.
              </p>

              <div className="w-full max-w-2xl mb-8 relative">
                <ChatInput input={input} setInput={handleInputChange} loading={loading} onSend={send} inputRef={inputRef}
                  placeholder="Type / to select an agent, then ask anything…" accent="blue" />
                {showSlashMenu && (
                  <SlashMenu agents={filteredAgents} onSelect={selectAgent} onClose={() => setShowSlashMenu(false)} />
                )}
              </div>

              {/* Agent cards as suggestion groups */}
              <div className="w-full max-w-4xl space-y-6">
                {AGENT_LIST.map(a => {
                  const Icon = a.icon;
                  return (
                    <div key={a.id}>
                      <button onClick={() => selectAgent(a.id)} className="flex items-center gap-2 mb-3 group">
                        <span className={`w-7 h-7 rounded-lg ${a.colorClass} flex items-center justify-center`}>
                          <Icon size={14} className="text-white" />
                        </span>
                        <span className="text-sm font-bold text-gray-700 group-hover:text-gray-900">{a.name}</span>
                        <span className="text-xs text-gray-400 font-mono">{a.command}</span>
                      </button>
                      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                        {a.suggestions.map((s, i) => {
                          const SIcon = s.icon;
                          return (
                            <button key={i} onClick={() => { setActiveAgent(a.id); send(s.text); }}
                              className="bg-white rounded-xl px-4 py-3 text-left shadow-sm hover:shadow-md transition-all border border-gray-50 group/card cursor-pointer">
                              <div className="flex items-start gap-2.5">
                                <SIcon size={16} className={`text-gray-300 group-hover/card:text-${a.color}-500 transition-colors mt-0.5 flex-shrink-0`} />
                                <span className="text-xs text-gray-600 leading-relaxed">{s.text}</span>
                              </div>
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          ) : (
            /* Chat view */
            <>
              <div className="flex-1 overflow-y-auto px-6 py-5 space-y-4">
                {messages.map((m, i) => (
                  <div key={i}>
                    {m.role === 'system' ? (
                      <div className="flex justify-center">
                        <div className="px-4 py-2 bg-gray-100 rounded-full text-xs text-gray-500 font-medium whitespace-pre-wrap">{m.text}</div>
                      </div>
                    ) : (
                      <div className={`flex items-start gap-3 ${m.role === 'user' ? 'justify-end' : ''}`}>
                        {m.role === 'agent' && agent && (
                          <div className={`w-7 h-7 rounded-lg ${agent.colorClass} flex items-center justify-center flex-shrink-0`}>
                            <Sparkles size={14} className="text-white" />
                          </div>
                        )}
                        <div className={`max-w-[72%] px-4 py-2.5 rounded-2xl text-sm leading-relaxed whitespace-pre-wrap ${
                          m.role === 'user' ? 'bg-gray-900 text-white rounded-br-md'
                            : m.isError ? 'bg-red-50 text-red-600 border border-red-100 rounded-bl-md'
                            : m.isSuccess ? 'bg-emerald-50 text-emerald-700 border border-emerald-100 rounded-bl-md'
                            : 'bg-white text-gray-700 border border-gray-100 shadow-sm rounded-bl-md'
                        }`}>
                          {m.text}
                          {m.toolUsed && <div className="mt-1.5 pt-1.5 border-t border-gray-50 text-[10px] text-gray-400">Tool: {m.toolUsed}</div>}
                        </div>
                      </div>
                    )}
                    {m.pending && <ConfirmInline pending={m.pending} onApprove={approve} onReject={reject} loading={loading} />}

                    {/* Query result table */}
                    {m.role === 'agent' && m.queryResult?.length > 0 && (
                      <div className="mt-2 ml-10 overflow-x-auto max-h-64 overflow-y-auto rounded-xl border border-gray-100 shadow-sm">
                        <table className="text-[10px] w-full">
                          <thead><tr className="bg-gray-50 sticky top-0">
                            {Object.keys(m.queryResult[0]).map(k => <th key={k} className="px-3 py-1.5 text-left font-semibold text-gray-600 whitespace-nowrap">{k}</th>)}
                          </tr></thead>
                          <tbody>{m.queryResult.map((row, ri) => (
                            <tr key={ri} className="border-t border-gray-50">{Object.values(row).map((v, ci) => <td key={ci} className="px-3 py-1 text-gray-500">{v != null ? String(v) : <span className="text-gray-300">NULL</span>}</td>)}</tr>
                          ))}</tbody>
                        </table>
                      </div>
                    )}
                  </div>
                ))}
                {loading && agent && (
                  <div className="flex items-center gap-3">
                    <div className={`w-7 h-7 rounded-lg ${agent.colorClass} flex items-center justify-center flex-shrink-0`}>
                      <Sparkles size={14} className="text-white" />
                    </div>
                    <div className="bg-white border border-gray-100 rounded-2xl rounded-bl-md px-4 py-3 shadow-sm flex gap-1.5">
                      {[0,1,2].map(i => <div key={i} className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-pulse" style={{ animationDelay: `${i*200}ms` }} />)}
                    </div>
                  </div>
                )}
                <div ref={bottomRef} />
              </div>

              {/* Suggestions bar when agent selected but few messages */}
              {agent && messages.filter(m => m.role === 'user').length < 2 && (
                <div className="px-6 pb-2 flex flex-wrap gap-2">
                  {agent.suggestions.slice(0, 3).map((s, i) => (
                    <button key={i} onClick={() => send(s.text)}
                      className="px-3 py-1.5 rounded-full bg-white border border-gray-100 text-xs text-gray-500 hover:border-gray-300 hover:bg-gray-50 transition-colors">
                      {s.text}
                    </button>
                  ))}
                </div>
              )}

              <div className="px-6 pb-4 pt-2 bg-[#f5f7fb] relative">
                <div className="max-w-3xl mx-auto relative">
                  <ChatInput input={input} setInput={handleInputChange} loading={loading} onSend={send} inputRef={inputRef}
                    placeholder={agent ? `Ask ${agent.name} anything… or type / to switch` : 'Type / to select an agent…'} accent={agent?.color || 'blue'} />
                  {showSlashMenu && (
                    <SlashMenu agents={filteredAgents} onSelect={selectAgent} onClose={() => setShowSlashMenu(false)} />
                  )}
                </div>
              </div>
            </>
          )}
        </div>

        {/* Tables panel */}
        {schemaOpen && activeAgent === 'data' && (
          <div className="w-72 flex-shrink-0 bg-white border-l border-gray-100 flex flex-col overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
              <div>
                <div className="text-xs font-semibold text-gray-800 uppercase tracking-wider">Tables</div>
                <div className="text-[10px] text-gray-400 mt-0.5">{tables.length} total</div>
              </div>
              <button onClick={fetchTables} className="p-1.5 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600"><RotateCcw size={14} /></button>
            </div>
            <div className="px-3 py-2 border-b border-gray-50">
              <input value={tableSearch} onChange={e => setTableSearch(e.target.value)} placeholder="Search tables..."
                className="w-full px-3 py-1.5 rounded-lg bg-gray-50 border-0 text-xs text-gray-700 placeholder-gray-400 outline-none focus:ring-1 focus:ring-blue-200" />
            </div>
            <div className={`overflow-y-auto ${selectedTable ? 'max-h-48' : 'flex-1'}`}>
              {userTables.length > 0 && <div className="px-4 py-1.5 text-[9px] font-bold text-gray-400 uppercase tracking-widest bg-gray-50/50">Project Tables</div>}
              {userTables.map((t, i) => (
                <button key={i} onClick={() => handleTableClick(t)}
                  className={`w-full flex items-center justify-between px-4 py-2 text-xs transition-colors border-l-2 ${
                    selectedTable?.table === t.table ? 'bg-blue-50 border-l-blue-500 text-blue-600' : 'border-l-transparent text-gray-600 hover:bg-gray-50'
                  }`}>
                  <span className="font-medium">{t.table}</span>
                  <span className={`text-[10px] px-2 py-0.5 rounded-full font-medium ${t.row_count > 0 ? 'bg-emerald-50 text-emerald-500' : 'bg-gray-50 text-gray-300'}`}>{t.row_count}</span>
                </button>
              ))}
              {systemTables.length > 0 && <div className="px-4 py-1.5 text-[9px] font-bold text-gray-400 uppercase tracking-widest bg-gray-50/50">System Tables</div>}
              {systemTables.map((t, i) => (
                <button key={i} onClick={() => handleTableClick(t)}
                  className={`w-full flex items-center justify-between px-4 py-2 text-xs transition-colors border-l-2 ${
                    selectedTable?.table === t.table ? 'bg-blue-50 border-l-blue-500 text-blue-600' : 'border-l-transparent text-gray-600 hover:bg-gray-50'
                  }`}>
                  <span className="font-medium">{t.table}</span>
                  <span className={`text-[10px] px-2 py-0.5 rounded-full font-medium ${t.row_count > 0 ? 'bg-emerald-50 text-emerald-500' : 'bg-gray-50 text-gray-300'}`}>{t.row_count}</span>
                </button>
              ))}
            </div>
            {selectedTable && (
              <div className="flex-1 overflow-y-auto p-4 border-t border-gray-100">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-sm font-bold text-gray-800">{selectedTable.table}</span>
                  <button onClick={() => { setSelectedTable(null); setTableSchema(null); }} className="text-gray-300 hover:text-gray-500"><X size={14} /></button>
                </div>
                {loadingSchema ? <div className="text-xs text-gray-400">Loading...</div> : tableSchema && (
                  <>
                    <div className="text-[10px] text-gray-500 mb-2">{tableSchema.row_count} rows · {tableSchema.columns?.length} cols</div>
                    <table className="w-full text-[10px]">
                      <thead><tr className="bg-gray-50">
                        <th className="px-2 py-1 text-left font-semibold text-gray-500">Column</th>
                        <th className="px-2 py-1 text-left font-semibold text-gray-500">Type</th>
                      </tr></thead>
                      <tbody>{(tableSchema.columns || []).map((c, i) => (
                        <tr key={i} className="border-t border-gray-50">
                          <td className="px-2 py-1 font-medium text-gray-700">{c.column_name}</td>
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

/* ─── Slash Command Menu ─── */
function SlashMenu({ agents, onSelect, onClose }) {
  useEffect(() => {
    const handler = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  return (
    <div className="absolute bottom-full left-0 right-0 mb-2 bg-white rounded-xl border border-gray-200 shadow-lg overflow-hidden z-50">
      <div className="px-4 py-2 border-b border-gray-100">
        <span className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">Select an Agent</span>
      </div>
      {agents.map(a => {
        const Icon = a.icon;
        return (
          <button key={a.id} onClick={() => onSelect(a.id)}
            className="w-full flex items-center gap-3 px-4 py-3 hover:bg-gray-50 transition-colors text-left">
            <span className={`w-8 h-8 rounded-lg ${a.colorClass} flex items-center justify-center flex-shrink-0`}>
              <Icon size={16} className="text-white" />
            </span>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <span className="text-sm font-semibold text-gray-800">{a.name}</span>
                <span className="text-xs text-gray-400 font-mono">{a.command}</span>
              </div>
              <div className="text-xs text-gray-500 mt-0.5">{a.desc}</div>
            </div>
          </button>
        );
      })}
      {agents.length === 0 && <div className="px-4 py-3 text-xs text-gray-400 text-center">No matching agents</div>}
    </div>
  );
}

/* ─── Chat Input ─── */
function ChatInput({ input, setInput, loading, onSend, inputRef, placeholder, accent = 'blue' }) {
  const colors = {
    blue: 'bg-blue-500 hover:bg-blue-600 shadow-blue-500/20',
    violet: 'bg-violet-500 hover:bg-violet-600 shadow-violet-500/20',
    cyan: 'bg-cyan-500 hover:bg-cyan-600 shadow-cyan-500/20',
    emerald: 'bg-emerald-500 hover:bg-emerald-600 shadow-emerald-500/20',
  };
  return (
    <div className="flex items-center gap-3 bg-white rounded-full px-5 py-3.5 shadow-md border border-gray-200">
      <Pencil size={16} className="text-gray-400 flex-shrink-0" />
      <input ref={inputRef} value={input} onChange={e => setInput(e.target.value)}
        onKeyDown={e => e.key === 'Enter' && !e.shiftKey && onSend()}
        placeholder={placeholder} disabled={loading}
        className="flex-1 bg-transparent outline-none text-sm text-gray-800 placeholder-gray-400 disabled:opacity-70" />
      <button onClick={() => onSend()} disabled={loading || !input.trim()}
        className={`w-9 h-9 rounded-full flex items-center justify-center flex-shrink-0 transition-all ${
          input.trim() ? `${colors[accent]} text-white shadow-sm` : 'bg-gray-100 text-gray-300 cursor-not-allowed'
        }`}>
        <ArrowUp size={16} />
      </button>
    </div>
  );
}

/* ─── Confirm Inline (CRUD) ─── */
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
        <span className="text-gray-400">{diff.rows_affected || '?'} row(s)</span>
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
      <button onClick={() => setShowSql(!showSql)} className="text-violet-500 font-semibold text-[10px] mb-2 hover:underline">{showSql ? 'Hide SQL' : 'Show SQL'}</button>
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
