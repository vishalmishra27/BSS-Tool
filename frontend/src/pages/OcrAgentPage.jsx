import { useState, useRef, useEffect } from 'react';

export default function OcrAgentPage() {
  const [messages, setMessages] = useState([
    { role: 'agent', text: "I'm the Document Analysis Agent. Upload PDF files and ask me questions — I'll extract text and analyse them for discrepancies, matching, or any question you have.", ts: new Date() },
  ]);
  const [history, setHistory] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [uploading, setUploading] = useState(false);
  const bottomRef = useRef(null);
  const fileRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const addMsg = (role, text, extra = {}) => {
    setMessages(prev => [...prev, { role, text, ts: new Date(), ...extra }]);
  };

  const handleUpload = async (e) => {
    const files = Array.from(e.target.files);
    if (!files.length) return;
    setUploading(true);

    const uploaded = [];
    for (const file of files) {
      const formData = new FormData();
      formData.append('file', file);
      try {
        const res = await fetch('/api/agent/upload-pdf', {
          method: 'POST',
          body: formData,
        });
        const data = await res.json();
        if (data.file_path) {
          uploaded.push({ name: file.name, path: data.file_path });
        }
      } catch {
        addMsg('agent', `Failed to upload ${file.name}`, { isError: true });
      }
    }

    if (uploaded.length > 0) {
      setUploadedFiles(prev => [...prev, ...uploaded]);
      addMsg('agent', `Uploaded ${uploaded.length} file(s): ${uploaded.map(f => f.name).join(', ')}. Now ask me a question about them.`);
    }
    setUploading(false);
    e.target.value = '';
  };

  const send = async (text) => {
    const msg = (text || input).trim();
    if (!msg || loading) return;
    setInput('');
    addMsg('user', msg);

    if (uploadedFiles.length === 0) {
      addMsg('agent', 'Please upload at least one PDF file first, then ask your question.');
      return;
    }

    const filePaths = uploadedFiles.map(f => f.path);
    const newHistory = [...history, { role: 'user', content: msg }];
    setHistory(newHistory);
    setLoading(true);

    try {
      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: `Use the ocr_tool with file_paths ${JSON.stringify(filePaths)} to answer: ${msg}`,
          history: newHistory,
          current_page: '/agent/ocr',
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
    'Do these invoices match CBS records?',
    'Flag discrepancies across these documents',
    'Extract all line items and totals',
    'Summarise the key details from the uploaded documents',
  ];

  return (
    <div style={{ padding: 24, fontFamily: 'system-ui, sans-serif', background: '#f8faff', minHeight: '100vh' }}>
      <div style={{ marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: '#001F5B' }}>Document Analysis Agent</h1>
        <p style={{ margin: '4px 0 0', color: '#666', fontSize: 13 }}>Upload PDFs and ask questions — extract text, flag discrepancies, match invoices</p>
      </div>

      {/* How it works */}
      <div style={{ maxWidth: 800, margin: '0 auto 16px', background: '#fff', borderRadius: 10, border: '1px solid #e0e8f0', padding: 16 }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: '#001F5B', marginBottom: 8 }}>How this agent works</div>
        <div style={{ fontSize: 12, color: '#555', lineHeight: 1.7 }}>
          Upload one or more PDF documents (invoices, contracts, reports), then ask any question.
          The agent extracts text from each PDF using OCR, sends all content to GPT-4o, and returns
          an answer with flagged items if discrepancies are found.
        </div>
        <div style={{ fontSize: 12, fontWeight: 600, color: '#001F5B', marginTop: 10, marginBottom: 4 }}>Sample queries you can try:</div>
        <ul style={{ fontSize: 12, color: '#444', margin: 0, paddingLeft: 18, lineHeight: 1.8 }}>
          <li><i>"Do these invoices match CBS records?"</i> — cross-references extracted data</li>
          <li><i>"Flag discrepancies across these invoices"</i> — compares amounts, dates, accounts</li>
          <li><i>"Extract all line items and totals"</i> — structured extraction from PDFs</li>
          <li><i>"Summarise the key details from each document"</i> — overview of all uploads</li>
          <li><i>"Are there any duplicate charges across these invoices?"</i></li>
          <li><i>"Compare the supplier details between document 1 and 2"</i></li>
          <li><i>"What is the total amount across all uploaded invoices?"</i></li>
          <li><i>"Check if VAT calculations are correct in these invoices"</i></li>
        </ul>
      </div>

      <div style={{ maxWidth: 800, margin: '0 auto' }}>
        {/* Upload area */}
        <div style={{ background: '#fff', borderRadius: 10, border: '2px dashed #c8d4f0', padding: 20, marginBottom: 16, textAlign: 'center' }}>
          <input ref={fileRef} type="file" accept=".pdf" multiple onChange={handleUpload} style={{ display: 'none' }} />
          <button onClick={() => fileRef.current?.click()} disabled={uploading}
            style={{ padding: '10px 24px', borderRadius: 8, border: 'none', background: '#001F5B', color: '#fff', fontWeight: 600, fontSize: 13, cursor: 'pointer' }}>
            {uploading ? 'Uploading...' : '+ Upload PDF Files'}
          </button>
          {uploadedFiles.length > 0 && (
            <div style={{ marginTop: 12, display: 'flex', flexWrap: 'wrap', gap: 8, justifyContent: 'center' }}>
              {uploadedFiles.map((f, i) => (
                <span key={i} style={{ padding: '4px 12px', background: '#f0f4ff', borderRadius: 12, fontSize: 12, color: '#001F5B', border: '1px solid #c8d4f0' }}>
                  📄 {f.name}
                </span>
              ))}
            </div>
          )}
        </div>

        {/* Messages */}
        <div style={{ background: '#fff', borderRadius: 10, border: '1px solid #e0e8f0', padding: 20, minHeight: 350, maxHeight: 450, overflowY: 'auto', marginBottom: 16 }}>
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
          {loading && <div style={{ color: '#888', fontSize: 13 }}>Analysing documents...</div>}
          <div ref={bottomRef} />
        </div>

        {/* Suggestions */}
        {uploadedFiles.length > 0 && messages.length <= 3 && (
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
            placeholder={uploadedFiles.length > 0 ? "Ask about the uploaded documents..." : "Upload PDFs first, then ask..."}
            disabled={loading}
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
