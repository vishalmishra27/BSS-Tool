import { useState, useEffect, useRef } from 'react';

export default function DataManagementAgentPage() {
  // Tables panel
  const [tables, setTables] = useState([]);
  const [loadingTables, setLoadingTables] = useState(true);
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableSchema, setTableSchema] = useState(null);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const [schemaOpen, setSchemaOpen] = useState(false);

  // File state
  const [fileId, setFileId] = useState(null);
  const [fileInfo, setFileInfo] = useState(null);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef(null);

  // Chat state
  const [messages, setMessages] = useState([
    { role: 'agent', text: "Hi! I'm the Data Management assistant. Upload a CSV/PSV file, or ask me to list tables, show a schema, sanitize data, upload to DB, drop/alter tables, or run SQL queries." },
  ]);
  const [history, setHistory] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  const fetchTables = () => {
    setLoadingTables(true);
    fetch('/api/data-agent/tables')
      .then(r => r.json())
      .then(data => { setTables(Array.isArray(data) ? data : []); setLoadingTables(false); })
      .catch(() => setLoadingTables(false));
  };
  useEffect(() => { fetchTables(); }, []);

  const handleTableClick = (t) => {
    setSelectedTable(t);
    setSchemaOpen(true);
    setLoadingSchema(true);
    fetch(`/api/data-agent/tables/${t.table}/schema?schema=${t.schema}`)
      .then(r => r.json())
      .then(data => { setTableSchema(data); setLoadingSchema(false); })
      .catch(() => setLoadingSchema(false));
  };

  // File upload
  const handleFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    addMsg('user', `Uploading: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`);
    const formData = new FormData();
    formData.append('file', file);
    try {
      const res = await fetch('/api/data-agent/upload', { method: 'POST', body: formData });
      const data = await res.json();
      if (data.error) {
        addMsg('agent', `Upload error: ${data.error}`, { isError: true });
      } else {
        setFileId(data.file_id);
        setFileInfo(data);
        addMsg('agent',
          `File ready!\n` +
          `**${data.original_name}** — ${data.row_count.toLocaleString()} rows, ${data.num_columns} columns, ${data.file_size_human}\n` +
          `Est. upload time: ${data.estimated_time}\n\n` +
          `What would you like to do?\n• "Sanitize the file"\n• "Upload to table_name"\n• "Show me existing tables"`,
          { filePreview: data }
        );
      }
    } catch { addMsg('agent', 'Network error.', { isError: true }); }
    setUploading(false);
    e.target.value = '';
  };

  const addMsg = (role, text, extra = {}) => {
    setMessages(prev => [...prev, { role, text, ts: new Date(), ...extra }]);
  };

  // Chat send
  const send = async (text) => {
    const msg = (text || input).trim();
    if (!msg || loading) return;
    setInput('');
    addMsg('user', msg);
    const newHistory = [...history, { role: 'user', content: msg }];
    setHistory(newHistory);
    setLoading(true);
    try {
      const res = await fetch('/api/data-agent/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg, history: newHistory, file_id: fileId }),
      });
      const data = await res.json();
      if (data.error) {
        addMsg('agent', `Error: ${data.error}`, { isError: true });
      } else {
        const extra = {};
        if (data.query_result) extra.queryResult = data.query_result;
        if (data.sanitize_result) {
          setFileInfo(prev => prev ? { ...prev, headers: data.sanitize_result.headers, row_count: data.sanitize_result.rows_after } : prev);
        }
        if (data.tables_refreshed) fetchTables();
        addMsg('agent', data.reply || 'Done.', extra);
        setHistory(prev => [...prev, { role: 'assistant', content: data.reply || '' }]);
      }
    } catch { addMsg('agent', 'Network error.', { isError: true }); }
    setLoading(false);
  };

  const SUGGESTIONS = [
    'List all tables',
    'Sanitize the uploaded file',
    'Show schema of checklist',
    'Upload file to a new table called my_data',
    'Run: SELECT COUNT(*) FROM phases',
  ];

  return (
    <div style={{ display: 'flex', minHeight: '100vh', fontFamily: 'system-ui, sans-serif', background: '#f8faff' }}>

      {/* ─── Main: Chat area ──────────────────────────────────────────────── */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', padding: 24, minWidth: 0 }}>
        <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div>
            <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: '#001F5B' }}>Data Management</h1>
            <p style={{ margin: '2px 0 0', color: '#666', fontSize: 13 }}>Manage your database through conversation</p>
          </div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {/* File upload button */}
            <button
              onClick={() => fileInputRef.current?.click()}
              disabled={uploading}
              style={{ padding: '7px 16px', borderRadius: 6, border: 'none', background: '#001F5B', color: '#fff', fontWeight: 600, fontSize: 12, cursor: 'pointer' }}
            >
              {uploading ? 'Uploading...' : '📁 Upload CSV/PSV'}
            </button>
            <input ref={fileInputRef} type="file" accept=".csv,.psv,.txt" onChange={handleFileUpload} style={{ display: 'none' }} />
            {/* Toggle schema panel */}
            <button
              onClick={() => setSchemaOpen(!schemaOpen)}
              style={{ padding: '7px 16px', borderRadius: 6, border: '1px solid #d0d8f0', background: schemaOpen ? '#f0f4ff' : '#fff', color: '#001F5B', fontWeight: 600, fontSize: 12, cursor: 'pointer' }}
            >
              {schemaOpen ? 'Hide Tables ▶' : '◀ Show Tables'}
            </button>
          </div>
        </div>

        {/* File info bar */}
        {fileInfo && (
          <div style={{ padding: '8px 14px', background: '#fff', borderRadius: 6, border: '1px solid #e0e8f0', marginBottom: 12, fontSize: 12, color: '#444', display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontWeight: 700, color: '#001F5B' }}>📄 {fileInfo.original_name}</span>
            <span>{fileInfo.row_count?.toLocaleString()} rows</span>
            <span>{fileInfo.headers?.length} cols</span>
            <span>{fileInfo.file_size_human}</span>
            {fileInfo.sanitized !== false && <span style={{ color: '#16a34a', fontWeight: 600 }}>✓ Sanitized</span>}
            <button onClick={() => { setFileId(null); setFileInfo(null); }} style={{ marginLeft: 'auto', background: 'none', border: 'none', color: '#999', cursor: 'pointer', fontSize: 14 }}>×</button>
          </div>
        )}

        {/* Messages */}
        <div style={{ flex: 1, background: '#fff', borderRadius: 10, border: '1px solid #e0e8f0', padding: 20, overflowY: 'auto', minHeight: 0, maxHeight: 'calc(100vh - 260px)' }}>
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
                </div>
              </div>
              {/* File preview */}
              {m.filePreview && m.filePreview.sample_rows?.length > 0 && (
                <div style={{ margin: '6px 0', overflowX: 'auto' }}>
                  <table style={{ fontSize: 10, borderCollapse: 'collapse', border: '1px solid #e0e8f0' }}>
                    <thead><tr style={{ background: '#f0f4ff' }}>
                      {m.filePreview.headers.map((h, j) => <th key={j} style={{ padding: '4px 8px', borderBottom: '1px solid #d0d8f0', whiteSpace: 'nowrap', fontWeight: 600, color: '#001F5B' }}>{h}</th>)}
                    </tr></thead>
                    <tbody>
                      {m.filePreview.sample_rows.slice(0, 3).map((row, ri) => (
                        <tr key={ri}>{row.map((c, ci) => <td key={ci} style={{ padding: '3px 8px', borderBottom: '1px solid #f0f0f0', maxWidth: 150, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: '#444' }}>{c ?? '—'}</td>)}</tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {/* Query result */}
              {m.queryResult && m.queryResult.length > 0 && (
                <div style={{ margin: '6px 0', overflowX: 'auto', maxHeight: 300, overflowY: 'auto' }}>
                  <table style={{ fontSize: 10, borderCollapse: 'collapse', border: '1px solid #e0e8f0', width: '100%' }}>
                    <thead><tr style={{ background: '#f0f4ff' }}>
                      {Object.keys(m.queryResult[0]).map(k => <th key={k} style={{ padding: '4px 8px', borderBottom: '1px solid #d0d8f0', whiteSpace: 'nowrap', fontWeight: 600, color: '#001F5B', textAlign: 'left' }}>{k}</th>)}
                    </tr></thead>
                    <tbody>
                      {m.queryResult.map((row, ri) => (
                        <tr key={ri}>{Object.values(row).map((v, ci) => <td key={ci} style={{ padding: '3px 8px', borderBottom: '1px solid #f0f0f0', color: '#444' }}>{v != null ? String(v) : <span style={{ color: '#ccc' }}>NULL</span>}</td>)}</tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ))}
          {loading && <div style={{ color: '#888', fontSize: 13 }}>Thinking...</div>}
          <div ref={bottomRef} />
        </div>

        {/* Suggestions */}
        {messages.length <= 2 && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 10 }}>
            {SUGGESTIONS.map((s, i) => (
              <button key={i} onClick={() => send(s)} style={{ background: '#f0f4ff', border: '1px solid #c8d4f0', borderRadius: 16, padding: '5px 12px', fontSize: 11, color: '#001F5B', cursor: 'pointer' }}>
                {s}
              </button>
            ))}
          </div>
        )}

        {/* Input */}
        <div style={{ display: 'flex', gap: 10, marginTop: 12 }}>
          <input value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && send()}
            placeholder="Ask me to list tables, sanitize, upload, drop, alter, run SQL..." disabled={loading}
            style={{ flex: 1, padding: '12px 16px', borderRadius: 8, border: '1px solid #d0d8f0', fontSize: 14, outline: 'none' }} />
          <button onClick={() => send()} disabled={loading || !input.trim()}
            style={{ padding: '12px 24px', borderRadius: 8, border: 'none', background: input.trim() ? '#001F5B' : '#ccc', color: '#fff', fontWeight: 600, fontSize: 14, cursor: input.trim() ? 'pointer' : 'not-allowed' }}>
            Send
          </button>
        </div>
      </div>

      {/* ─── Right: Tables & Schema panel (collapsible) ───────────────────── */}
      {schemaOpen && (
        <div style={{ width: 300, flexShrink: 0, background: '#fff', borderLeft: '1px solid #e0e8f0', overflowY: 'auto', display: 'flex', flexDirection: 'column' }}>
          <div style={{ padding: '12px 14px', background: '#001F5B', color: '#fff', fontSize: 12, fontWeight: 700, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span>DATABASE TABLES</span>
            <button onClick={fetchTables} style={{ background: 'rgba(255,255,255,0.15)', border: 'none', color: '#fff', fontSize: 10, padding: '3px 8px', borderRadius: 4, cursor: 'pointer' }}>Refresh</button>
          </div>

          {/* Table list */}
          <div style={{ flex: selectedTable ? 'none' : 1, maxHeight: selectedTable ? 200 : undefined, overflowY: 'auto', borderBottom: selectedTable ? '1px solid #e0e8f0' : 'none' }}>
            {loadingTables ? (
              <div style={{ padding: 14, color: '#aaa', fontSize: 12 }}>Loading...</div>
            ) : tables.map((t, i) => (
              <button
                key={i}
                onClick={() => handleTableClick(t)}
                style={{
                  display: 'flex', justifyContent: 'space-between', width: '100%',
                  padding: '8px 14px', textAlign: 'left', fontSize: 11, color: '#222',
                  background: selectedTable?.table === t.table ? '#f0f4ff' : '#fff',
                  borderLeft: selectedTable?.table === t.table ? '3px solid #0070c0' : '3px solid transparent',
                  border: 'none', borderBottom: '1px solid #f5f5f5', cursor: 'pointer',
                }}
              >
                <span style={{ fontWeight: 600 }}>{t.table}</span>
                <span style={{ color: '#888', fontSize: 10 }}>{t.row_count}</span>
              </button>
            ))}
          </div>

          {/* Schema detail */}
          {selectedTable && (
            <div style={{ flex: 1, overflowY: 'auto', padding: 14 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: '#001F5B' }}>{selectedTable.table}</div>
                <button onClick={() => { setSelectedTable(null); setTableSchema(null); }} style={{ background: 'none', border: 'none', color: '#888', cursor: 'pointer', fontSize: 14 }}>×</button>
              </div>
              {loadingSchema ? <div style={{ color: '#aaa', fontSize: 12 }}>Loading...</div> : tableSchema && (
                <>
                  <div style={{ fontSize: 10, color: '#888', marginBottom: 8 }}>{tableSchema.row_count} rows · {tableSchema.columns?.length} columns</div>
                  <table style={{ width: '100%', fontSize: 10, borderCollapse: 'collapse', marginBottom: 12 }}>
                    <thead><tr style={{ background: '#f0f4ff' }}>
                      <th style={thS}>Column</th><th style={thS}>Type</th><th style={thS}>PK</th>
                    </tr></thead>
                    <tbody>
                      {(tableSchema.columns || []).map((c, i) => (
                        <tr key={i}>
                          <td style={{ ...tdS, fontWeight: 600 }}>{c.column_name}</td>
                          <td style={tdS}>{c.data_type}</td>
                          <td style={tdS}>{tableSchema.primary_keys?.includes(c.column_name) ? '🔑' : ''}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {tableSchema.sample_rows?.length > 0 && (
                    <>
                      <div style={{ fontSize: 10, fontWeight: 600, color: '#001F5B', marginBottom: 4 }}>Sample ({tableSchema.sample_rows.length} rows)</div>
                      <div style={{ overflowX: 'auto' }}>
                        <table style={{ fontSize: 9, borderCollapse: 'collapse', width: '100%' }}>
                          <thead><tr style={{ background: '#f0f4ff' }}>
                            {Object.keys(tableSchema.sample_rows[0]).map(k => <th key={k} style={{ ...thS, whiteSpace: 'nowrap' }}>{k}</th>)}
                          </tr></thead>
                          <tbody>
                            {tableSchema.sample_rows.map((row, ri) => (
                              <tr key={ri}>{Object.values(row).map((v, ci) => (
                                <td key={ci} style={{ ...tdS, maxWidth: 100, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{v != null ? String(v) : <span style={{ color: '#ccc' }}>NULL</span>}</td>
                              ))}</tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </>
                  )}
                </>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

const thS = { padding: '3px 6px', textAlign: 'left', borderBottom: '1px solid #e0e8f0', fontSize: 10, fontWeight: 600, color: '#001F5B' };
const tdS = { padding: '3px 6px', borderBottom: '1px solid #f0f0f0', color: '#444' };
