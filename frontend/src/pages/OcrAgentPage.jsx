import { useState, useRef, useEffect } from 'react';

const SUGGESTIONS = [
  { label: 'Extract all line items and totals', icon: '≡', desc: 'Pull structured data from invoices' },
  { label: 'Summarise key details', icon: '📄', desc: 'Get a brief overview of each document' },
  { label: 'Flag discrepancies', icon: '⚠', desc: 'Find inconsistencies across documents' },
];

async function readJsonSafely(res) {
  const contentType = res.headers.get('content-type') || '';
  if (!contentType.includes('application/json')) return null;
  try {
    return await res.json();
  } catch {
    return null;
  }
}

function buildHttpErrorMessage(res, data, fallback) {
  if (data?.error) return data.error;
  if (data?.message) return data.message;
  return `${fallback} (${res.status} ${res.statusText || 'Request failed'})`;
}

// ── Markdown table renderer ─────────────────────────────────────────────────
function MarkdownTable({ raw }) {
  const lines = raw.trim().split('\n').filter(l => l.includes('|'));
  if (lines.length < 2) return <span style={{ whiteSpace: 'pre-wrap' }}>{raw}</span>;

  const parse = line =>
    line.split('|').map(c => c.trim()).filter((_, i, a) => i > 0 && i < a.length - 1);
  const headers = parse(lines[0]);
  const rows = lines.slice(2).map(parse);

  return (
    <div style={{ overflowX: 'auto', margin: '10px 0' }}>
      <table style={{ borderCollapse: 'collapse', fontSize: 13, width: '100%', minWidth: 400 }}>
        <thead>
          <tr>
            {headers.map((h, i) => (
              <th key={i} style={{
                padding: '8px 14px', background: '#001F5B', color: '#fff',
                border: '1px solid #001F5B', fontWeight: 600, textAlign: 'left', whiteSpace: 'nowrap',
              }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri} style={{ background: ri % 2 === 0 ? '#fff' : '#f4f7ff' }}>
              {row.map((cell, ci) => (
                <td key={ci} style={{ padding: '7px 14px', border: '1px solid #dde6f5', color: '#222', verticalAlign: 'top' }}>
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderInline(text) {
  const segments = [];
  const rx = /(\*\*(.+?)\*\*|\*(.+?)\*)/g;
  let last = 0, m;
  while ((m = rx.exec(text)) !== null) {
    if (m.index > last) segments.push(text.slice(last, m.index));
    if (m[2]) segments.push(<strong key={m.index}>{m[2]}</strong>);
    else if (m[3]) segments.push(<em key={m.index}>{m[3]}</em>);
    last = m.index + m[0].length;
  }
  if (last < text.length) segments.push(text.slice(last));
  return segments;
}

function MessageBody({ text }) {
  if (!text) return null;
  const parts = [];
  const tableRx = /(\|.+\|[ \t]*\n\|[-| :]+\|\n(?:\|.+\|[ \t]*\n?)*)/g;
  let last = 0, m;
  while ((m = tableRx.exec(text)) !== null) {
    if (m.index > last) parts.push({ type: 'text', content: text.slice(last, m.index) });
    parts.push({ type: 'table', content: m[0] });
    last = m.index + m[0].length;
  }
  if (last < text.length) parts.push({ type: 'text', content: text.slice(last) });

  return (
    <>
      {parts.map((p, i) =>
        p.type === 'table'
          ? <MarkdownTable key={i} raw={p.content} />
          : <span key={i} style={{ whiteSpace: 'pre-wrap' }}>{renderInline(p.content)}</span>
      )}
    </>
  );
}

// ── Export helpers ─────────────────────────────────────────────────────────
async function exportExcel(answer, filename) {
  const res = await fetch('/api/ocr/export-excel', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ answer, filename }),
  });
  if (!res.ok) throw new Error('Export failed');
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename || 'ocr_output.xlsx';
  a.click();
  URL.revokeObjectURL(url);
}

function exportTxt(answer, filename) {
  const blob = new Blob([answer], { type: 'text/plain' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename || 'ocr_output.txt';
  a.click();
  URL.revokeObjectURL(url);
}

// ── Main component ─────────────────────────────────────────────────────────
export default function OcrAgentPage() {
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [batchItems, setBatchItems] = useState('');
  const [showBatch, setShowBatch] = useState(false);
  const fileRef = useRef(null);
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  const addMsg = (role, text, extra = {}) =>
    setMessages(prev => [...prev, { role, text, ts: new Date(), ...extra }]);

  // ── Upload ────────────────────────────────────────────────────────────────
  const uploadFiles = async (files) => {
    if (!files.length) return;
    setUploading(true);
    const formData = new FormData();
    for (const f of files) formData.append('files', f);

    try {
      const res = await fetch('/api/ocr/upload', { method: 'POST', body: formData });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Upload failed');

      const added = data.files || [];
      setUploadedFiles(prev => {
        const existing = new Set(prev.map(f => f.path));
        return [...prev, ...added.filter(f => !existing.has(f.path))];
      });

      const readable = added.filter(f => f.readable);
      const scanned = added.filter(f => !f.readable);

      if (readable.length) {
        const details = readable.map(f =>
          `• ${f.name} — ${(f.chars / 1000).toFixed(1)}k chars, ${f.chunks} chunks`
        ).join('\n');
        addMsg('agent',
          `Loaded ${readable.length} readable PDF${readable.length > 1 ? 's' : ''}:\n${details}\n\nReady — ask me anything about them.`,
          { isSuccess: true }
        );
      }
      if (scanned.length) {
        addMsg('agent',
          `⚠️ ${scanned.map(f => f.name).join(', ')} appear to be scanned/image-based PDFs (no text layer detected). Document Intelligence support for scanned PDFs is coming soon.`,
          { isWarning: true }
        );
      }
    } catch (err) {
      addMsg('agent', `Upload failed: ${err.message}`, { isError: true });
    }
    setUploading(false);
  };

  const handleFileInput = (e) => { uploadFiles(Array.from(e.target.files)); e.target.value = ''; };
  const handleDrop = (e) => {
    e.preventDefault(); setDragOver(false);
    const files = Array.from(e.dataTransfer.files).filter(f => f.type === 'application/pdf');
    if (files.length) uploadFiles(files);
  };

  // ── Send question ──────────────────────────────────────────────────────────
  const send = async (text) => {
    const msg = (text || input).trim();
    if (!msg || loading) return;
    setInput('');
    addMsg('user', msg);

    const readableFiles = uploadedFiles.filter(f => f.readable !== false);
    if (!readableFiles.length) {
      addMsg('agent', 'Please upload at least one readable (text-layer) PDF first.');
      return;
    }

    setLoading(true);
    try {
      const res = await fetch('/api/ocr/analyse', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: msg,
          file_paths: readableFiles.map(f => ({ name: f.name, path: f.path })),
        }),
      });
      const data = await readJsonSafely(res);
      if (!res.ok) {
        addMsg('agent', buildHttpErrorMessage(res, data, 'Analysis failed'), { isError: true });
      } else {
        addMsg('agent', data.answer, {
          docs: data.docs_processed,
          unreadable: data.unreadable_docs,
          exportable: true,
          answer: data.answer,
        });
      }
    } catch (err) {
      addMsg('agent', `Network error — ${err.message || 'could not reach the server.'}`, { isError: true });
    } finally {
      setLoading(false);
    }
  };

  // ── Batch extract ──────────────────────────────────────────────────────────
  const runBatch = async () => {
    const items = batchItems.split('\n').map(s => s.trim()).filter(Boolean);
    if (!items.length) return;
    addMsg('user', `Extract these fields:\n${items.map(i => `• ${i}`).join('\n')}`);
    setShowBatch(false);
    setBatchItems('');

    const readableFiles = uploadedFiles.filter(f => f.readable !== false);
    if (!readableFiles.length) {
      addMsg('agent', 'No readable PDFs loaded.', { isError: true });
      return;
    }

    setLoading(true);
    try {
      const res = await fetch('/api/ocr/batch-extract', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          items,
          file_paths: readableFiles.map(f => ({ name: f.name, path: f.path })),
        }),
      });
      const data = await readJsonSafely(res);
      if (!res.ok) {
        addMsg('agent', buildHttpErrorMessage(res, data, 'Batch extract failed'), { isError: true });
      } else {
        // Build a markdown table from the result
        const cols = data.columns || [];
        const rows = data.table || [];
        let md = cols.join(' | ') + '\n' + cols.map(() => '---').join(' | ') + '\n';
        for (const row of rows) md += cols.map(c => row[c] ?? '').join(' | ') + '\n';
        addMsg('agent', md, {
          docs: data.docs_processed,
          exportable: true,
          answer: md,
          excelB64: data.excel_b64,
          batchFilename: `batch_extract_${Date.now()}.xlsx`,
        });
      }
    } catch (err) {
      addMsg('agent', `Network error — ${err.message || 'request failed.'}`, { isError: true });
    } finally {
      setLoading(false);
    }
  };

  // ── Reset ─────────────────────────────────────────────────────────────────
  const reset = async () => {
    await fetch('/api/ocr/reset', { method: 'DELETE' });
    setUploadedFiles([]);
    setMessages([]);
  };

  const removeFile = path => setUploadedFiles(prev => prev.filter(f => f.path !== path));
  const hasReadable = uploadedFiles.some(f => f.readable !== false);
  const showEmpty = messages.length === 0;

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', background: '#f8faff', fontFamily: 'system-ui, sans-serif' }}>

      {/* Header */}
      <div style={{ padding: '14px 24px', borderBottom: '1px solid #e8eef8', background: '#fff', flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ width: 32, height: 32, background: 'linear-gradient(135deg,#00C4A7,#001F5B)', borderRadius: 8, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#fff', fontSize: 16 }}>✦</div>
          <div>
            <div style={{ fontWeight: 700, fontSize: 15, color: '#001F5B' }}>Document Analysis Agent</div>
            <div style={{ fontSize: 12, color: '#888' }}>Upload readable PDFs — extract text, flag discrepancies, export to Excel</div>
          </div>
        </div>
        {uploadedFiles.length > 0 && (
          <button onClick={reset} title="Clear all documents and start fresh" style={{ padding: '6px 14px', borderRadius: 6, border: '1px solid #dde', background: '#fff', color: '#666', fontSize: 12, cursor: 'pointer' }}>
            Reset session
          </button>
        )}
      </div>

      {/* Scroll area */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '24px 24px 0' }}>

        {/* ── Empty state ── */}
        {showEmpty && (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', paddingTop: 32, gap: 24 }}>
            <div style={{ width: 60, height: 60, background: 'linear-gradient(135deg,#00C4A7,#001F5B)', borderRadius: 18, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 28, color: '#fff' }}>✦</div>
            <div style={{ textAlign: 'center' }}>
              <h2 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: '#111' }}>What documents shall I analyse?</h2>
              <p style={{ margin: '8px 0 0', color: '#666', fontSize: 14 }}>
                Upload PDF files to extract text, preview the output,<br />and download as TXT or XLSX.
              </p>
            </div>

            {/* Drop zone */}
            <div
              onDragOver={e => { e.preventDefault(); setDragOver(true); }}
              onDragLeave={() => setDragOver(false)}
              onDrop={handleDrop}
              onClick={() => !uploading && fileRef.current?.click()}
              style={{
                width: '100%', maxWidth: 540,
                border: `2px dashed ${dragOver ? '#001F5B' : '#c8d4f0'}`,
                borderRadius: 14, padding: '40px 20px', textAlign: 'center', cursor: 'pointer',
                background: dragOver ? '#eef2ff' : '#fff', transition: 'all 0.15s',
              }}
            >
              <div style={{ fontSize: 32, color: '#aab', marginBottom: 10 }}>⬆</div>
              <div style={{ fontWeight: 600, fontSize: 14, color: '#333' }}>
                {uploading ? 'Uploading…' : 'Click to select PDF files'}
              </div>
              <div style={{ fontSize: 12, color: '#999', marginTop: 4 }}>or drag and drop here</div>
            </div>

            {/* Feature cards */}
            <div style={{ display: 'flex', gap: 14, width: '100%', maxWidth: 700, flexWrap: 'wrap' }}>
              {SUGGESTIONS.map((s, i) => (
                <div key={i} onClick={() => hasReadable && send(s.label)}
                  style={{
                    flex: '1 1 180px', background: '#fff', border: '1px solid #e0e8f0',
                    borderRadius: 10, padding: 16, cursor: hasReadable ? 'pointer' : 'default',
                    opacity: hasReadable ? 1 : 0.65, transition: 'box-shadow 0.15s',
                  }}
                  onMouseEnter={e => hasReadable && (e.currentTarget.style.boxShadow = '0 3px 14px rgba(0,31,91,0.12)')}
                  onMouseLeave={e => (e.currentTarget.style.boxShadow = 'none')}
                >
                  <div style={{ fontSize: 22, marginBottom: 8 }}>{s.icon}</div>
                  <div style={{ fontWeight: 600, fontSize: 13, color: '#111', marginBottom: 4 }}>{s.label}</div>
                  <div style={{ fontSize: 12, color: '#666' }}>{s.desc}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ── Conversation ── */}
        {!showEmpty && (
          <div style={{ maxWidth: 820, margin: '0 auto' }}>

            {/* Uploaded file pills */}
            {uploadedFiles.length > 0 && (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 16 }}>
                {uploadedFiles.map((f, i) => (
                  <span key={i} style={{
                    display: 'flex', alignItems: 'center', gap: 6, padding: '4px 12px',
                    background: f.readable === false ? '#fff5f5' : '#f0f4ff',
                    borderRadius: 20, fontSize: 12,
                    color: f.readable === false ? '#c00' : '#001F5B',
                    border: `1px solid ${f.readable === false ? '#ffd0d0' : '#c8d4f0'}`,
                  }}>
                    {f.readable === false ? '⚠️' : '📄'} {f.name}
                    {f.chunks ? <span style={{ opacity: 0.5 }}>({f.chunks} chunks)</span> : null}
                    <span onClick={() => removeFile(f.path)} style={{ cursor: 'pointer', opacity: 0.45, fontSize: 15, lineHeight: 1 }}>×</span>
                  </span>
                ))}
                <button onClick={() => fileRef.current?.click()} style={{
                  padding: '4px 12px', borderRadius: 20, border: '1px dashed #c8d4f0',
                  background: 'transparent', color: '#555', fontSize: 12, cursor: 'pointer',
                }}>+ Add PDFs</button>
              </div>
            )}

            {/* Messages */}
            {messages.map((m, i) => (
              <div key={i} style={{ marginBottom: 18 }}>
                {m.role === 'user' ? (
                  <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                    <div style={{
                      maxWidth: '72%', padding: '10px 16px',
                      borderRadius: '14px 14px 3px 14px',
                      background: '#001F5B', color: '#fff', fontSize: 14, lineHeight: 1.6,
                      whiteSpace: 'pre-wrap',
                    }}>{m.text}</div>
                  </div>
                ) : (
                  <div style={{ display: 'flex', gap: 10, alignItems: 'flex-start' }}>
                    <div style={{
                      width: 30, height: 30, borderRadius: 8, flexShrink: 0,
                      background: 'linear-gradient(135deg,#00C4A7,#001F5B)',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                      color: '#fff', fontSize: 14, marginTop: 2,
                    }}>✦</div>
                    <div style={{ flex: 1 }}>
                      <div style={{
                        padding: '12px 16px',
                        borderRadius: '3px 14px 14px 14px',
                        background: m.isError ? '#fff5f5' : m.isWarning ? '#fffbf0' : '#fff',
                        border: `1px solid ${m.isError ? '#ffd0d0' : m.isWarning ? '#ffe4a0' : '#e4eaf6'}`,
                        color: m.isError ? '#b00' : '#222',
                        fontSize: 14, lineHeight: 1.75,
                      }}>
                        <MessageBody text={m.text} />

                        {/* Source docs footer */}
                        {m.docs?.length > 0 && (
                          <div style={{ marginTop: 10, fontSize: 11, color: '#888', borderTop: '1px solid #eef', paddingTop: 6 }}>
                            Analysed: {m.docs.join(' · ')}
                          </div>
                        )}
                        {m.unreadable?.length > 0 && (
                          <div style={{ marginTop: 4, fontSize: 11, color: '#b08000' }}>
                            ⚠️ Skipped (scanned): {m.unreadable.map(u => u.name).join(', ')}
                          </div>
                        )}
                      </div>

                      {/* Export buttons */}
                      {m.exportable && (
                        <div style={{ display: 'flex', gap: 8, marginTop: 8 }}>
                          <ExportButton
                            label="Export to Excel"
                            icon="📊"
                            onClick={async () => {
                              if (m.excelB64) {
                                // Batch extract — use pre-built Excel
                                const bytes = Uint8Array.from(atob(m.excelB64), c => c.charCodeAt(0));
                                const blob = new Blob([bytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
                                const url = URL.createObjectURL(blob);
                                const a = document.createElement('a');
                                a.href = url; a.download = m.batchFilename || 'batch_extract.xlsx'; a.click();
                                URL.revokeObjectURL(url);
                              } else {
                                await exportExcel(m.answer, `ocr_output_${i}.xlsx`);
                              }
                            }}
                          />
                          <ExportButton
                            label="Export to TXT"
                            icon="📄"
                            onClick={() => exportTxt(m.answer, `ocr_output_${i}.txt`)}
                          />
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            ))}

            {loading && (
              <div style={{ display: 'flex', gap: 10, alignItems: 'flex-start', marginBottom: 18 }}>
                <div style={{ width: 30, height: 30, borderRadius: 8, background: 'linear-gradient(135deg,#00C4A7,#001F5B)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#fff', fontSize: 14, flexShrink: 0, marginTop: 2 }}>✦</div>
                <div style={{ padding: '12px 16px', background: '#fff', border: '1px solid #e4eaf6', borderRadius: '3px 14px 14px 14px', fontSize: 14, color: '#888', display: 'flex', gap: 5, alignItems: 'center' }}>
                  <Dot delay={0} /><Dot delay={0.2} /><Dot delay={0.4} />
                  &nbsp; Analysing documents…
                </div>
              </div>
            )}
            <div ref={bottomRef} />
          </div>
        )}
      </div>

      {/* ── Batch extract panel ── */}
      {showBatch && (
        <div style={{ flexShrink: 0, borderTop: '1px solid #e8eef8', background: '#f4f7ff', padding: '14px 24px' }}>
          <div style={{ maxWidth: 820, margin: '0 auto' }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: '#001F5B', marginBottom: 6 }}>
              Batch Extract — enter one field per line (e.g. "Invoice Number", "Total Amount", "Supplier Name")
            </div>
            <textarea
              value={batchItems}
              onChange={e => setBatchItems(e.target.value)}
              placeholder={'Invoice Number\nTotal Amount\nDue Date\nSupplier Name'}
              rows={4}
              style={{ width: '100%', padding: 10, borderRadius: 8, border: '1px solid #c8d4f0', fontSize: 13, resize: 'vertical', boxSizing: 'border-box' }}
            />
            <div style={{ display: 'flex', gap: 8, marginTop: 8 }}>
              <button onClick={runBatch} disabled={!batchItems.trim() || loading} style={{ padding: '8px 20px', background: '#001F5B', color: '#fff', border: 'none', borderRadius: 7, fontWeight: 600, fontSize: 13, cursor: 'pointer' }}>
                Extract &amp; Build Table
              </button>
              <button onClick={() => setShowBatch(false)} style={{ padding: '8px 14px', background: '#fff', color: '#555', border: '1px solid #dde', borderRadius: 7, fontSize: 13, cursor: 'pointer' }}>
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ── Bottom bar ── */}
      <div style={{ flexShrink: 0, borderTop: '1px solid #e8eef8', background: '#fff', padding: '12px 24px 16px' }}>

        {/* Suggestion chips */}
        {hasReadable && messages.length <= 2 && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 10 }}>
            {SUGGESTIONS.map((s, i) => (
              <button key={i} onClick={() => send(s.label)} style={{
                background: '#f0f4ff', border: '1px solid #c8d4f0', borderRadius: 20,
                padding: '6px 14px', fontSize: 12, color: '#001F5B', cursor: 'pointer',
              }}>{s.label}</button>
            ))}
            <button onClick={() => setShowBatch(b => !b)} style={{
              background: '#fff8f0', border: '1px solid #f0d4b0', borderRadius: 20,
              padding: '6px 14px', fontSize: 12, color: '#885500', cursor: 'pointer',
            }}>📊 Batch Extract to Table</button>
          </div>
        )}

        <div style={{ maxWidth: 820, margin: '0 auto', display: 'flex', gap: 10, alignItems: 'center' }}>
          {/* Upload button */}
          <button onClick={() => fileRef.current?.click()} disabled={uploading} title="Upload PDF files"
            style={{ width: 40, height: 40, borderRadius: 8, border: '1px solid #e0e8f0', background: '#f8faff', color: '#555', fontSize: 18, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
            ⬆
          </button>

          {/* Batch extract button */}
          <button onClick={() => setShowBatch(b => !b)} title="Batch extract specific fields"
            style={{ width: 40, height: 40, borderRadius: 8, border: '1px solid #e0e8f0', background: showBatch ? '#f0f4ff' : '#f8faff', color: '#555', fontSize: 16, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
            📊
          </button>

          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && !e.shiftKey && send()}
            placeholder={hasReadable ? 'Ask about the uploaded documents…' : 'Upload PDFs first, then ask questions…'}
            disabled={loading}
            style={{ flex: 1, padding: '10px 14px', borderRadius: 8, border: '1px solid #d0d8f0', fontSize: 14, outline: 'none' }}
          />

          <button onClick={() => send()} disabled={loading || !input.trim()}
            style={{
              width: 40, height: 40, borderRadius: 8, border: 'none',
              background: input.trim() && !loading ? '#001F5B' : '#ddd',
              color: '#fff', fontSize: 18, cursor: input.trim() && !loading ? 'pointer' : 'not-allowed',
              display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, transition: 'background 0.15s',
            }}>➤</button>
        </div>
      </div>

      <input ref={fileRef} type="file" accept=".pdf" multiple onChange={handleFileInput} style={{ display: 'none' }} />

      <style>{`
        @keyframes pulse { 0%,100%{opacity:.25} 50%{opacity:1} }
      `}</style>
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
