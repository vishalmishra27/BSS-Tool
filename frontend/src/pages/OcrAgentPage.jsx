import { useState, useRef, useEffect, useCallback } from 'react';
import {
  Sparkles, FileText, ListChecks, AlertTriangle, Trash2,
  Pencil, ArrowUp, X, Plus, History, PanelRightOpen, PanelRightClose, Upload
} from 'lucide-react';

const STORAGE_KEY = 'bss_ocr_agent_history';
function loadSessions() { try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); } catch { return []; } }
function saveSessions(s) { localStorage.setItem(STORAGE_KEY, JSON.stringify(s)); }

export default function OcrAgentPage() {
  const [messages, setMessages] = useState([]);
  const [chatHistory, setChatHistory] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [extractedResults, setExtractedResults] = useState(null);
  const [extracting, setExtracting] = useState(false);

  const [showConfig, setShowConfig] = useState(false);
  const [selectedFiles, setSelectedFiles] = useState({});
  const [selectedPages, setSelectedPages] = useState({});
  const [exportFormat, setExportFormat] = useState('xlsx');
  const [includeFileCol, setIncludeFileCol] = useState(true);
  const [includePageCol, setIncludePageCol] = useState(true);
  const [includeTextCol, setIncludeTextCol] = useState(true);
  const [includePageBreaks, setIncludePageBreaks] = useState(true);
  const [textMode, setTextMode] = useState('full');
  const [showPreview, setShowPreview] = useState(false);

  const bottomRef = useRef(null);
  const fileRef = useRef(null);

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
  const startNewChat = () => { setMessages([]); setChatHistory([]); setActiveSessionId(null); setUploadedFiles([]); setExtractedResults(null); setShowConfig(false); setShowPreview(false); };

  const addMsg = (role, text, extra = {}) => { setMessages(prev => [...prev, { role, text, ts: new Date().toISOString(), ...extra }]); };

  const handleUpload = async (e) => {
    const files = Array.from(e.target.files); if (!files.length) return;
    setUploading(true);
    const uploaded = [];
    for (const file of files) {
      const formData = new FormData(); formData.append('file', file);
      try { const res = await fetch('/api/agent/upload-pdf', { method: 'POST', body: formData }); const data = await res.json(); if (data.file_path) uploaded.push({ name: file.name, path: data.file_path }); }
      catch { addMsg('agent', `Failed to upload ${file.name}`, { isError: true }); }
    }
    if (uploaded.length > 0) { setUploadedFiles(prev => [...prev, ...uploaded]); addMsg('agent', `Uploaded ${uploaded.length} file(s): ${uploaded.map(f => f.name).join(', ')}.\n\nClick "Extract Text" to scan the documents.`); }
    setUploading(false); e.target.value = '';
  };

  const handleExtract = async () => {
    if (!uploadedFiles.length) return;
    setExtracting(true); addMsg('user', 'Extract text from uploaded documents');
    try {
      const res = await fetch('/api/agent/extract-pdf', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ file_paths: uploadedFiles.map(f => f.path) }) });
      const data = await res.json();
      if (data.error) addMsg('agent', `Error: ${data.error}`, { isError: true });
      else {
        setExtractedResults(data.results);
        const fSel = {}, pSel = {};
        data.results.forEach(r => { if (!r.error) { fSel[r.file] = true; r.pages.forEach(pg => { pSel[`${r.file}|${pg.page}`] = true; }); } });
        setSelectedFiles(fSel); setSelectedPages(pSel); setShowConfig(true);
        const summary = data.results.map(r => r.error ? `${r.file}: Error` : `${r.file}: ${r.page_count} page(s)`).join('\n');
        addMsg('agent', `Text extracted!\n\n${summary}\n\nConfigure your export in the right panel.`);
      }
    } catch (err) { addMsg('agent', `Network error: ${err.message}`, { isError: true }); }
    setExtracting(false);
  };

  const toggleFile = (file) => { const v = !selectedFiles[file]; setSelectedFiles(p => ({ ...p, [file]: v })); setSelectedPages(p => { const u = { ...p }; Object.keys(u).forEach(k => { if (k.startsWith(`${file}|`)) u[k] = v; }); return u; }); };
  const togglePage = (file, page) => { setSelectedPages(p => ({ ...p, [`${file}|${page}`]: !p[`${file}|${page}`] })); };

  const buildPreviewRows = () => {
    if (!extractedResults) return [];
    const rows = [];
    extractedResults.forEach(r => { if (r.error || !selectedFiles[r.file]) return; r.pages.forEach(pg => { if (!selectedPages[`${r.file}|${pg.page}`]) return; if (textMode === 'lines') { pg.text.split('\n').filter(l => l.trim()).forEach(line => { const row = {}; if (includeFileCol) row['File'] = r.file; if (includePageCol) row['Page'] = pg.page; if (includeTextCol) row['Text'] = line.trim(); rows.push(row); }); } else { const row = {}; if (includeFileCol) row['File'] = r.file; if (includePageCol) row['Page'] = pg.page; if (includeTextCol) row['Text'] = pg.text.trim(); rows.push(row); } }); });
    return rows;
  };
  const buildTxtPreview = () => { if (!extractedResults) return ''; let t = ''; extractedResults.forEach(r => { if (r.error || !selectedFiles[r.file]) return; r.pages.forEach(pg => { if (!selectedPages[`${r.file}|${pg.page}`]) return; if (includePageBreaks) t += `=== ${r.file} — Page ${pg.page} ===\n`; t += pg.text.trim() + '\n\n'; }); }); return t.trim(); };

  const handleDownload = async () => {
    const sel = []; extractedResults.forEach(r => { if (r.error || !selectedFiles[r.file]) return; const pages = r.pages.filter(pg => selectedPages[`${r.file}|${pg.page}`]).map(pg => pg.page); if (pages.length > 0) { const f = uploadedFiles.find(u => u.name === r.file || u.path.endsWith(r.file)); if (f) sel.push({ file_path: f.path, pages }); } });
    try {
      const res = await fetch('/api/agent/extract-pdf/download', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ file_paths: sel.map(s => s.file_path), selected_pages: sel.reduce((a, s) => { a[s.file_path] = s.pages; return a; }, {}), format: exportFormat, include_file_col: includeFileCol, include_page_col: includePageCol, include_text_col: includeTextCol, text_mode: textMode, include_page_breaks: includePageBreaks }) });
      if (!res.ok) { const err = await res.json(); addMsg('agent', `Download error: ${err.error}`, { isError: true }); return; }
      const blob = await res.blob(); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = `extracted_text.${exportFormat}`; document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
      addMsg('agent', `Downloaded extracted_text.${exportFormat} successfully!`);
    } catch (err) { addMsg('agent', `Download error: ${err.message}`, { isError: true }); }
  };

  const send = async (text) => {
    const msg = (text || input).trim(); if (!msg || loading) return;
    setInput(''); addMsg('user', msg);
    if (uploadedFiles.length === 0) { addMsg('agent', 'Please upload at least one PDF file first.'); return; }
    const newHistory = [...chatHistory, { role: 'user', content: msg }]; setChatHistory(newHistory);
    setLoading(true);
    try {
      const res = await fetch('/api/agent/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ message: `Use the ocr_tool with file_paths ${JSON.stringify(uploadedFiles.map(f => f.path))} to answer: ${msg}`, history: newHistory, current_page: '/agent/ocr' }) });
      const data = await res.json();
      if (data.error) addMsg('agent', `Error: ${data.error}`, { isError: true });
      else if (data.reply) { addMsg('agent', data.reply, { toolUsed: data.tool_used }); setChatHistory(prev => [...prev, { role: 'assistant', content: data.reply }]); }
    } catch { addMsg('agent', 'Network error.', { isError: true }); }
    setLoading(false);
  };

  const CARDS = [
    { icon: ListChecks, title: 'Extract all line items and totals', desc: 'Pull structured data from invoices' },
    { icon: FileText, title: 'Summarise key details', desc: 'Get a brief overview of each document' },
    { icon: AlertTriangle, title: 'Flag discrepancies', desc: 'Find inconsistencies across documents' },
  ];

  const hasMessages = messages.length > 0;
  const previewRows = showPreview && exportFormat !== 'txt' ? buildPreviewRows() : [];
  const previewTxt = showPreview && exportFormat === 'txt' ? buildTxtPreview() : '';
  const previewCols = previewRows.length > 0 ? Object.keys(previewRows[0]) : [];
  const selectedPageCount = Object.values(selectedPages).filter(Boolean).length;
  const selectedFileCount = Object.values(selectedFiles).filter(Boolean).length;

  return (
    <div className="flex flex-col h-screen bg-[#f5f7fb] font-sans">
      <div className="flex-1 flex overflow-hidden">
        {historyOpen && <HistoryPanel sessions={sessions} activeSessionId={activeSessionId} onLoad={loadSession} onDelete={deleteSession} onNew={startNewChat} onClose={() => setHistoryOpen(false)} />}

        <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
          {/* Top bar */}
          <div className="flex items-center justify-between px-6 py-3 bg-white border-b border-gray-100">
            <div className="flex items-center gap-2">
              <button onClick={() => setHistoryOpen(!historyOpen)} className="p-2 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600 transition-colors"><History size={18} /></button>
              {hasMessages && <button onClick={startNewChat} className="p-2 rounded-lg hover:bg-gray-50 text-gray-400 hover:text-gray-600 transition-colors"><Plus size={18} /></button>}
            </div>
            <div className="flex items-center gap-2">
              {extractedResults && (
                <button onClick={() => setShowConfig(!showConfig)}
                  className={`p-2 rounded-lg transition-colors ${showConfig ? 'bg-emerald-50 text-emerald-600' : 'text-gray-400 hover:bg-gray-50 hover:text-gray-600'}`}>
                  {showConfig ? <PanelRightClose size={18} /> : <PanelRightOpen size={18} />}
                </button>
              )}
            </div>
          </div>

          {/* File bar when files uploaded */}
          {uploadedFiles.length > 0 && (
            <div className="px-6 py-2.5 bg-white border-b border-gray-50 flex items-center gap-2 flex-wrap">
              <input ref={fileRef} type="file" accept=".pdf" multiple onChange={handleUpload} className="hidden" />
              {uploadedFiles.map((f, i) => (
                <span key={i} className="inline-flex items-center gap-1.5 px-3 py-1 bg-emerald-50 rounded-lg text-[11px] font-medium text-emerald-700 border border-emerald-100">
                  <FileText size={12} /> {f.name}
                  <button onClick={() => { setUploadedFiles(p => p.filter((_, j) => j !== i)); if (uploadedFiles.length <= 1) { setExtractedResults(null); setShowConfig(false); } }} className="text-emerald-300 hover:text-red-400"><X size={12} /></button>
                </span>
              ))}
              <button onClick={() => fileRef.current?.click()} className="px-2.5 py-1 rounded-lg text-[11px] text-gray-400 hover:bg-gray-50 border border-gray-200">+ Add</button>
              {!extractedResults && (
                <button onClick={handleExtract} disabled={extracting}
                  className="px-3 py-1 rounded-lg text-[11px] font-semibold bg-emerald-500 text-white hover:bg-emerald-600 shadow-sm shadow-emerald-500/20">
                  {extracting ? 'Extracting...' : 'Extract Text'}
                </button>
              )}
            </div>
          )}

          {/* Preview */}
          {showPreview && extractedResults && (
            <div className="mx-6 mt-3 bg-white rounded-xl border border-gray-100 shadow-sm p-4">
              <div className="flex items-center justify-between mb-3">
                <span className="text-xs font-bold text-gray-700">Preview — {exportFormat.toUpperCase()} ({selectedFileCount} file(s), {selectedPageCount} page(s))</span>
                <div className="flex gap-2">
                  <button onClick={handleDownload} className="px-3 py-1 rounded-lg text-[11px] font-bold bg-emerald-500 text-white hover:bg-emerald-600">Download</button>
                  <button onClick={() => setShowPreview(false)} className="px-3 py-1 rounded-lg text-[11px] text-gray-400 hover:bg-gray-50 border border-gray-200">Close</button>
                </div>
              </div>
              {exportFormat === 'txt' ? (
                <pre className="bg-gray-50 rounded-lg p-3 text-[11px] whitespace-pre-wrap max-h-64 overflow-y-auto text-gray-600 leading-relaxed">{previewTxt || 'No content selected.'}</pre>
              ) : (
                <div className="overflow-x-auto max-h-64 overflow-y-auto rounded-lg border border-gray-50">
                  <table className="w-full text-[11px]">
                    <thead><tr className="bg-gray-50 sticky top-0">{previewCols.map(c => <th key={c} className="px-3 py-1.5 text-left font-bold text-gray-600">{c}</th>)}</tr></thead>
                    <tbody>{previewRows.slice(0, 50).map((row, i) => <tr key={i} className="border-t border-gray-50">{previewCols.map(c => <td key={c} className="px-3 py-1 text-gray-500 max-w-[300px] truncate">{row[c] ?? ''}</td>)}</tr>)}</tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {/* Welcome / Chat */}
          {!hasMessages && uploadedFiles.length === 0 ? (
            <div className="flex-1 flex flex-col items-center justify-center px-6 py-12">
              <div className="w-14 h-14 rounded-2xl bg-emerald-500 flex items-center justify-center mb-6 shadow-lg shadow-emerald-500/20">
                <Sparkles size={28} className="text-white" />
              </div>
              <h1 className="text-2xl font-bold text-gray-900 mb-2 text-center">What documents shall I analyse?</h1>
              <p className="text-sm text-gray-500 text-center max-w-lg mb-10">
                Upload PDF files to extract text, configure your export, preview the output, and download as TXT, CSV, or XLSX.
              </p>

              {/* Upload zone */}
              <input ref={fileRef} type="file" accept=".pdf" multiple onChange={handleUpload} className="hidden" />
              <button onClick={() => fileRef.current?.click()} disabled={uploading}
                className="max-w-md w-full bg-white rounded-2xl border-2 border-dashed border-gray-200 hover:border-emerald-300 hover:bg-emerald-50/30 p-10 text-center transition-all cursor-pointer mb-10 group">
                <Upload size={32} className="mx-auto mb-3 text-gray-300 group-hover:text-emerald-400 transition-colors" />
                <div className="text-sm font-semibold text-gray-600 group-hover:text-emerald-700">{uploading ? 'Uploading...' : 'Click to select PDF files'}</div>
                <div className="text-xs text-gray-300 mt-1">or drag and drop here</div>
              </button>

              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 max-w-2xl w-full mb-10">
                {CARDS.map((card, i) => {
                  const Icon = card.icon;
                  return (
                    <button key={i} onClick={() => send(card.title)}
                      className="bg-white rounded-xl p-5 text-left shadow-sm hover:shadow-md transition-shadow border border-gray-50 group cursor-pointer">
                      <Icon size={20} className="text-gray-400 group-hover:text-emerald-500 transition-colors mb-3" />
                      <div className="text-sm font-semibold text-gray-800 mb-1">{card.title}</div>
                      <div className="text-xs text-gray-500 leading-relaxed">{card.desc}</div>
                    </button>
                  );
                })}
              </div>

              <div className="w-full max-w-2xl">
                <ChatInput input={input} setInput={setInput} loading={loading} onSend={send}
                  placeholder="Upload PDFs first, then ask questions…" accent="emerald" />
              </div>
            </div>
          ) : (
            <>
              <div className="flex-1 overflow-y-auto px-6 py-5 space-y-4">
                {messages.map((m, i) => (
                  <div key={i} className={`flex items-start gap-3 ${m.role === 'user' ? 'justify-end' : ''}`}>
                    {m.role === 'agent' && <div className="w-7 h-7 rounded-lg bg-emerald-500 flex items-center justify-center flex-shrink-0 shadow-sm shadow-emerald-500/20"><Sparkles size={14} className="text-white" /></div>}
                    <div className={`max-w-[72%] px-4 py-2.5 rounded-2xl text-sm leading-relaxed whitespace-pre-wrap ${
                      m.role === 'user' ? 'bg-gray-900 text-white rounded-br-md' : m.isError ? 'bg-red-50 text-red-600 border border-red-100 rounded-bl-md' : 'bg-white text-gray-700 border border-gray-100 shadow-sm rounded-bl-md'
                    }`}>
                      {m.text}
                      {m.toolUsed && <div className="mt-1.5 pt-1.5 border-t border-gray-50 text-[10px] text-gray-300">Tool: {m.toolUsed}</div>}
                    </div>
                  </div>
                ))}
                {(loading || extracting) && (
                  <div className="flex items-center gap-3">
                    <div className="w-7 h-7 rounded-lg bg-emerald-500 flex items-center justify-center flex-shrink-0 shadow-sm"><Sparkles size={14} className="text-white" /></div>
                    <div className="bg-white border border-gray-100 rounded-2xl rounded-bl-md px-4 py-3 shadow-sm flex gap-1.5">
                      {[0,1,2].map(i => <div key={i} className="w-1.5 h-1.5 rounded-full bg-gray-300 animate-pulse" style={{ animationDelay: `${i*200}ms` }} />)}
                    </div>
                  </div>
                )}
                <div ref={bottomRef} />
              </div>
              {uploadedFiles.length > 0 && messages.length <= 4 && (
                <div className="px-6 pb-2 flex flex-wrap gap-2">
                  {CARDS.map((s, i) => (
                    <button key={i} onClick={() => send(s.title)} className="px-3 py-1.5 rounded-full bg-white border border-gray-100 text-xs text-gray-500 hover:border-emerald-300 hover:bg-emerald-50 transition-colors">
                      {s.title}
                    </button>
                  ))}
                </div>
              )}
              <div className="px-6 pb-4 pt-2 bg-[#f5f7fb]">
                <div className="max-w-3xl mx-auto">
                  <ChatInput input={input} setInput={setInput} loading={loading} onSend={send}
                    placeholder="Ask about the uploaded documents…" accent="emerald" />
                </div>
              </div>
            </>
          )}
        </div>

        {/* Export config panel */}
        {showConfig && extractedResults && (
          <div className="w-72 flex-shrink-0 bg-white border-l border-gray-100 flex flex-col overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-100">
              <div className="text-xs font-semibold text-gray-700 uppercase tracking-wider">Export Config</div>
              <div className="text-[10px] text-gray-300 mt-0.5">{selectedFileCount} file(s), {selectedPageCount} page(s)</div>
            </div>

            <div className="px-4 py-3 border-b border-gray-50">
              <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2">Format</div>
              <div className="flex gap-2">
                {['txt','csv','xlsx'].map(f => (
                  <button key={f} onClick={() => setExportFormat(f)}
                    className={`flex-1 py-1.5 rounded-lg text-[11px] font-bold uppercase transition-colors ${exportFormat === f ? 'bg-emerald-50 text-emerald-600 border border-emerald-200' : 'bg-gray-50 text-gray-400 border border-gray-100'}`}>{f}</button>
                ))}
              </div>
            </div>

            <div className="px-4 py-3 border-b border-gray-50 flex-1 overflow-y-auto">
              <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2">Files & Pages</div>
              {extractedResults.filter(r => !r.error).map((r, fi) => (
                <div key={fi} className="mb-2">
                  <label className="flex items-center gap-1.5 cursor-pointer text-xs font-semibold text-gray-700">
                    <input type="checkbox" checked={!!selectedFiles[r.file]} onChange={() => toggleFile(r.file)} className="accent-emerald-500 rounded" /> {r.file}
                  </label>
                  {selectedFiles[r.file] && <div className="pl-5 mt-1 space-y-0.5">
                    {r.pages.map(pg => (
                      <label key={pg.page} className="flex items-center gap-1.5 cursor-pointer text-[11px] text-gray-500">
                        <input type="checkbox" checked={!!selectedPages[`${r.file}|${pg.page}`]} onChange={() => togglePage(r.file, pg.page)} className="accent-emerald-500 rounded" />
                        Page {pg.page} <span className="text-gray-300 text-[10px]">({pg.text.length})</span>
                      </label>
                    ))}
                  </div>}
                </div>
              ))}
            </div>

            {exportFormat !== 'txt' ? (
              <div className="px-4 py-3 border-b border-gray-50">
                <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-2">Columns</div>
                {[['File Name',includeFileCol,()=>setIncludeFileCol(v=>!v)],['Page Number',includePageCol,()=>setIncludePageCol(v=>!v)],['Extracted Text',includeTextCol,()=>setIncludeTextCol(v=>!v)]].map(([l,v,t])=>(
                  <label key={l} className="flex items-center gap-1.5 cursor-pointer text-xs text-gray-600 mb-1"><input type="checkbox" checked={v} onChange={t} className="accent-emerald-500 rounded" />{l}</label>
                ))}
                <div className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mt-3 mb-2">Structure</div>
                {[['Full page (1 row/page)','full'],['Split lines (1 row/line)','lines']].map(([l,v])=>(
                  <label key={v} className="flex items-center gap-1.5 cursor-pointer text-xs text-gray-600 mb-1"><input type="radio" name="tm" checked={textMode===v} onChange={()=>setTextMode(v)} className="accent-emerald-500" />{l}</label>
                ))}
              </div>
            ) : (
              <div className="px-4 py-3 border-b border-gray-50">
                <label className="flex items-center gap-1.5 cursor-pointer text-xs text-gray-600">
                  <input type="checkbox" checked={includePageBreaks} onChange={()=>setIncludePageBreaks(v=>!v)} className="accent-emerald-500 rounded" /> Include page separators
                </label>
              </div>
            )}

            <div className="px-4 py-3 space-y-2">
              <button onClick={() => setShowPreview(true)} className="w-full py-2 rounded-lg border-2 border-emerald-500 text-emerald-600 text-xs font-bold hover:bg-emerald-50 transition-colors">Preview</button>
              <button onClick={handleDownload} className="w-full py-2 rounded-lg bg-emerald-500 text-white text-xs font-bold hover:bg-emerald-600 shadow-sm shadow-emerald-500/20 transition-colors">Download {exportFormat.toUpperCase()}</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function ChatInput({ input, setInput, loading, onSend, placeholder, accent = 'blue' }) {
  const colors = { blue: 'bg-blue-500 hover:bg-blue-600 shadow-blue-500/20', violet: 'bg-violet-500 hover:bg-violet-600 shadow-violet-500/20', cyan: 'bg-cyan-500 hover:bg-cyan-600 shadow-cyan-500/20', emerald: 'bg-emerald-500 hover:bg-emerald-600 shadow-emerald-500/20' };
  return (
    <div className="flex items-center gap-3 bg-white rounded-full px-4 py-3 shadow-sm border border-gray-100">
      <Pencil size={16} className="text-gray-400 flex-shrink-0" />
      <input value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && onSend()} placeholder={placeholder} disabled={loading}
        className="flex-1 bg-transparent outline-none text-sm text-gray-700 placeholder-gray-400 disabled:opacity-70" />
      <button onClick={() => onSend()} disabled={loading || !input.trim()}
        className={`w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 transition-all ${input.trim() ? `${colors[accent]} text-white shadow-sm` : 'bg-gray-100 text-gray-300 cursor-not-allowed'}`}>
        <ArrowUp size={16} />
      </button>
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
                className={`px-4 py-3 cursor-pointer border-b border-gray-50 transition-colors ${isActive ? 'bg-emerald-50 border-l-2 border-l-emerald-500' : 'hover:bg-gray-50 border-l-2 border-l-transparent'}`}>
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

function Dot({ delay }) {
  return (
    <span style={{ width: 7, height: 7, borderRadius: '50%', background: '#001F5B', display: 'inline-block', animation: `pulse 1.2s ${delay}s infinite` }} />
  );
}

function ExportButton({ label, icon, onClick }) {
  const [busy, setBusy] = useState(false);
  const handle = async () => { setBusy(true); try { await onClick(); } finally { setBusy(false); } };
  return (
    <button onClick={handle} disabled={busy} style={{
      padding: '5px 12px', borderRadius: 6, border: '1px solid #c8d4f0',
      background: '#f4f7ff', color: '#001F5B', fontSize: 12, cursor: 'pointer',
      display: 'flex', alignItems: 'center', gap: 5, fontWeight: 500,
    }}>
      {icon} {busy ? 'Preparing…' : label}
    </button>
  );
}
