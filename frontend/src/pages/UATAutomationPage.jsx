import { useEffect, useRef, useState } from 'react';

const API = '/api/uat/automation';

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmt(iso) {
  if (!iso) return '—';
  try { return new Date(iso.endsWith('Z') ? iso : iso + 'Z').toLocaleString(); }
  catch { return iso; }
}

// ── Status badge ──────────────────────────────────────────────────────────────
function StatusBadge({ status }) {
  const map = {
    passed:    { bg: '#dcfce7', color: '#16a34a', border: '#bbf7d0' },
    failed:    { bg: '#fee2e2', color: '#dc2626', border: '#fecaca' },
    pending:   { bg: '#dbeafe', color: '#2563eb', border: '#bfdbfe' },
    running:   { bg: '#fef3c7', color: '#d97706', border: '#fde68a' },
    completed: { bg: '#dcfce7', color: '#16a34a', border: '#bbf7d0' },
  };
  const s = map[status] || map.pending;
  return (
    <span style={{
      background: s.bg, color: s.color, border: `1px solid ${s.border}`,
      borderRadius: 4, padding: '2px 8px', fontSize: 11, fontWeight: 600,
      textTransform: 'capitalize', whiteSpace: 'nowrap',
    }}>
      {status || 'pending'}
    </span>
  );
}

// ── Slim progress bar ─────────────────────────────────────────────────────────
function ProgressBar({ value, max, color = '#0066CC' }) {
  const pct = max > 0 ? Math.min(Math.round((value / max) * 100), 100) : 0;
  return (
    <div style={{ background: '#E5E7EB', borderRadius: 4, height: 6, overflow: 'hidden', flex: 1 }}>
      <div style={{ width: `${pct}%`, height: '100%', background: color, transition: 'width 0.3s ease', borderRadius: 4 }} />
    </div>
  );
}

// ── Screenshot lightbox ───────────────────────────────────────────────────────
function Lightbox({ src, onClose }) {
  if (!src) return null;
  return (
    <div onClick={onClose} style={{
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.88)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      zIndex: 9999, cursor: 'zoom-out',
    }}>
      <div onClick={e => e.stopPropagation()} style={{ position: 'relative', maxWidth: '92vw', maxHeight: '92vh' }}>
        <img src={src} alt="step screenshot" style={{ maxWidth: '100%', maxHeight: '88vh', borderRadius: 8, boxShadow: '0 8px 40px rgba(0,0,0,0.7)', display: 'block' }} />
        <button onClick={onClose} style={{
          position: 'absolute', top: -14, right: -14,
          background: '#dc2626', border: 'none', borderRadius: '50%',
          width: 28, height: 28, cursor: 'pointer', color: '#fff',
          fontSize: 16, lineHeight: '28px', textAlign: 'center', padding: 0,
        }}>×</button>
      </div>
    </div>
  );
}

// ── Results table grouped by test case ────────────────────────────────────────
function ResultsTable({ results }) {
  const [lightbox, setLightbox] = useState(null);

  // group by test_case_id preserving order
  const groups = [];
  const seen = {};
  for (const r of results) {
    if (!seen[r.test_case_id]) {
      seen[r.test_case_id] = [];
      groups.push({ id: r.test_case_id, steps: seen[r.test_case_id] });
    }
    seen[r.test_case_id].push(r);
  }

  return (
    <>
      <Lightbox src={lightbox} onClose={() => setLightbox(null)} />
      {groups.map(({ id: tcId, steps }) => {
        const passed  = steps.filter(s => s.status === 'passed').length;
        const failed  = steps.filter(s => s.status === 'failed').length;
        const pending = steps.filter(s => s.status === 'pending').length;
        const tcStatus = failed > 0 ? 'failed' : pending > 0 ? 'pending' : 'passed';

        return (
          <div key={tcId} style={{
            marginBottom: 14,
            border: '1px solid #E0E8F0',
            borderRadius: 8, overflow: 'hidden',
            background: '#FFFFFF',
          }}>
            {/* Test-case header */}
            <div style={{
              background: tcStatus === 'failed'  ? '#fee2e2'
                        : tcStatus === 'passed'  ? '#dcfce7'
                        : '#dbeafe',
              padding: '9px 14px', display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap',
            }}>
              <span style={{ fontWeight: 700, fontSize: 13, color: '#001F5B' }}>{tcId}</span>
              <StatusBadge status={tcStatus} />
              <span style={{ marginLeft: 'auto', color: '#6B7280', fontSize: 11 }}>
                {passed}✓ {failed}✗ {pending > 0 ? `${pending} pending` : ''} / {steps.length} steps
              </span>
            </div>

            {/* Steps */}
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                <thead>
                  <tr style={{ background: '#F4F6FA' }}>
                    {['Step', 'Action', 'Selector', 'Input', 'Expected', 'Status', 'Error', 'Screenshot'].map(h => (
                      <th key={h} style={{
                        padding: '7px 12px', textAlign: 'left',
                        color: '#6B7280', fontWeight: 600,
                        whiteSpace: 'nowrap', borderBottom: '1px solid #E0E8F0',
                      }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {steps.map((s, i) => (
                    <tr key={i} style={{
                      background: i % 2 === 0 ? '#FFFFFF' : '#F8FAFF',
                      borderBottom: '1px solid #F0F0F0',
                    }}>
                      <td style={{ padding: '7px 12px', color: '#6B7280', whiteSpace: 'nowrap' }}>{s.step_id}</td>
                      <td style={{ padding: '7px 12px', color: '#0066CC', fontWeight: 600, whiteSpace: 'nowrap' }}>{s.action}</td>
                      <td style={{ padding: '7px 12px', color: '#222222', fontFamily: 'monospace', maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={s.selector || ''}>{s.selector || '—'}</td>
                      <td style={{ padding: '7px 12px', color: '#222222', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={s.input_value || ''}>{s.input_value || '—'}</td>
                      <td style={{ padding: '7px 12px', color: '#222222', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={s.expected_result || ''}>{s.expected_result || '—'}</td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap' }}><StatusBadge status={s.status} /></td>
                      <td style={{ padding: '7px 12px', color: '#dc2626', maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={s.error_message || ''}>{s.error_message || '—'}</td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap' }}>
                        {s.screenshot_path ? (
                          <button
                            onClick={() => setLightbox(`${API}/screenshot/${s.screenshot_path}`)}
                            style={{
                              background: '#EFF6FF', border: '1px solid #BFDBFE',
                              borderRadius: 4, padding: '3px 10px', cursor: 'pointer',
                              color: '#0066CC', fontSize: 11, fontWeight: 600,
                            }}
                          >
                            View
                          </button>
                        ) : <span style={{ color: '#9CA3AF' }}>—</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}
    </>
  );
}

// ── Run card in the sidebar list ──────────────────────────────────────────────
function RunCard({ run, isActive, onClick }) {
  const total     = run.total_steps     || 0;
  const completed = run.completed_steps || 0;
  const passed    = run.passed          || 0;
  const failed    = run.failed          || 0;
  const barColor  = failed > 0 ? '#dc2626' : '#16a34a';

  return (
    <div
      onClick={onClick}
      style={{
        background: isActive ? '#EFF6FF' : '#FFFFFF',
        border: `1px solid ${isActive ? '#93C5FD' : '#E0E8F0'}`,
        borderRadius: 8, padding: '10px 14px', cursor: 'pointer',
        transition: 'all 0.15s', marginBottom: 8,
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6, flexWrap: 'wrap' }}>
        <span style={{ fontWeight: 600, color: '#001F5B', fontSize: 12, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          #{run.id} — {run.filename}
        </span>
        <StatusBadge status={run.status} />
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
        <ProgressBar value={completed} max={total || 1} color={barColor} />
        <span style={{ color: '#6B7280', fontSize: 10, whiteSpace: 'nowrap' }}>
          {completed}/{total > 0 ? total : '?'}
        </span>
      </div>
      <div style={{ display: 'flex', gap: 10, fontSize: 10 }}>
        <span style={{ color: '#16a34a' }}>{passed}✓</span>
        <span style={{ color: '#dc2626' }}>{failed}✗</span>
        <span style={{ color: '#9CA3AF', marginLeft: 'auto' }}>{fmt(run.created_at)}</span>
      </div>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function UATAutomationPage() {
  const [runs,         setRuns]         = useState([]);
  const [selectedId,   setSelectedId]   = useState(null);
  const [statusData,   setStatusData]   = useState(null);
  const [resultsData,  setResultsData]  = useState(null);
  const [uploading,    setUploading]    = useState(false);
  const [uploadError,  setUploadError]  = useState('');
  const [runError,     setRunError]     = useState('');
  // headless toggle — default false to match original (headed browser avoids BSS portal detection)
  const [headless,     setHeadless]     = useState(false);
  const fileRef  = useRef();
  const pollRef  = useRef(null);

  // ── Helpers ────────────────────────────────────────────────────────────────
  const stopPolling = () => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
  };

  const refreshRunList = async () => {
    try {
      const r = await fetch(`${API}/runs`);
      if (r.ok) setRuns(await r.json());
    } catch (_) {}
  };

  // Fetch status + results for one run; updates both states and the run list entry.
  const fetchRunData = async (id) => {
    try {
      const [sRes, rRes] = await Promise.all([
        fetch(`${API}/status/${id}`),
        fetch(`${API}/results/${id}`),
      ]);
      if (sRes.ok) {
        const s = await sRes.json();
        setStatusData(s);
        // Merge counts into the sidebar list so RunCards stay up to date
        setRuns(prev => prev.map(r => r.id === id ? { ...r, ...s } : r));
        return s;
      }
    } catch (_) {}
    return null;
  };

  const startPolling = (id) => {
    stopPolling();
    // Poll every 1 s — matches the original Streamlit app's time.sleep(1) cadence
    pollRef.current = setInterval(async () => {
      const s = await fetchRunData(id);
      if (!s || s.status !== 'running') stopPolling();
      // Also refresh results list on every tick
      try {
        const rRes = await fetch(`${API}/results/${id}`);
        if (rRes.ok) setResultsData(await rRes.json());
      } catch (_) {}
    }, 1000);
  };

  // ── Lifecycle ──────────────────────────────────────────────────────────────
  useEffect(() => { refreshRunList(); }, []);
  useEffect(() => () => stopPolling(), []);

  // ── Select a run from the sidebar ─────────────────────────────────────────
  const selectRun = async (id) => {
    stopPolling();
    setSelectedId(id);
    setResultsData(null);
    setRunError('');

    const s = await fetchRunData(id);
    if (s?.status === 'running') startPolling(id);

    try {
      const rRes = await fetch(`${API}/results/${id}`);
      if (rRes.ok) setResultsData(await rRes.json());
    } catch (_) {}
  };

  // ── Upload handler ─────────────────────────────────────────────────────────
  const handleUpload = async (e) => {
    e.preventDefault();
    const file = fileRef.current?.files?.[0];
    if (!file) { setUploadError('Please select an .xlsx or .xls file.'); return; }
    setUploadError('');
    setUploading(true);
    try {
      const fd = new FormData();
      fd.append('file', file);
      const res  = await fetch(`${API}/upload`, { method: 'POST', body: fd });
      const data = await res.json();
      if (!res.ok) { setUploadError(data.error || 'Upload failed'); return; }
      if (fileRef.current) fileRef.current.value = '';
      await refreshRunList();
      await selectRun(data.test_run_id);
    } catch (err) {
      setUploadError(`Upload error: ${err.message}`);
    } finally {
      setUploading(false);
    }
  };

  // ── Run handler ────────────────────────────────────────────────────────────
  const handleRun = async () => {
    if (!selectedId) return;
    setRunError('');
    // Clear stale results display immediately so the user sees the fresh run
    setResultsData(null);
    try {
      const res  = await fetch(
        `${API}/run/${selectedId}?headless=${headless ? 'true' : 'false'}`,
        { method: 'POST' }
      );
      const data = await res.json();
      if (!res.ok) { setRunError(data.error || 'Failed to start run'); return; }
      setStatusData(prev => ({ ...prev, status: 'running', completed_steps: 0, passed: 0, failed: 0 }));
      setRuns(prev => prev.map(r => r.id === selectedId ? { ...r, status: 'running', completed_steps: 0, passed: 0, failed: 0 } : r));
      startPolling(selectedId);
    } catch (err) {
      setRunError(`Error: ${err.message}`);
    }
  };

  const canRun = statusData && statusData.status !== 'running';

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div style={{
      minHeight: '100vh',
      background: '#F4F6FA',
      color: '#222222', fontFamily: "'Helvetica Neue', Arial, 'Segoe UI', sans-serif",
      display: 'flex', flexDirection: 'column',
    }}>

      {/* ── Header ── */}
      <div style={{ padding: '22px 28px 18px', borderBottom: '1px solid #E0E8F0', background: '#FFFFFF' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 4 }}>
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#003087" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="9 11 12 14 22 4"/>
            <path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/>
          </svg>
          <h1 style={{ fontSize: 22, fontWeight: 700, margin: 0, color: '#001F5B' }}>UAT Automation</h1>
        </div>
        <p style={{ margin: 0, color: '#6B7280', fontSize: 12 }}>
          Upload an Excel file of test cases, execute them against a live website, and review step-by-step results with screenshots.
        </p>
      </div>

      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>

        {/* ── Left panel: upload form + run list ── */}
        <div style={{
          width: 300, flexShrink: 0,
          borderRight: '1px solid #E0E8F0',
          background: '#FFFFFF',
          display: 'flex', flexDirection: 'column', overflow: 'hidden',
        }}>

          {/* Upload form */}
          <div style={{ padding: '18px 16px', borderBottom: '1px solid #F0F0F0' }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#6B7280', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 10 }}>
              Upload & Run
            </div>
            <form onSubmit={handleUpload}>
              {/* File picker */}
              <div style={{
                border: '2px dashed #93C5FD', borderRadius: 6,
                padding: '12px 10px', textAlign: 'center', background: '#F8FAFF',
                marginBottom: 8, cursor: 'pointer',
              }}
                onClick={() => fileRef.current?.click()}
              >
                <input ref={fileRef} type="file" accept=".xlsx,.xls" style={{ display: 'none' }} id="uat-file" />
                <div style={{ color: '#0066CC', fontSize: 12 }}>
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ verticalAlign: 'middle', marginRight: 4 }}>
                    <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
                    <polyline points="17 8 12 3 7 8"/>
                    <line x1="12" y1="3" x2="12" y2="15"/>
                  </svg>
                  Select .xlsx / .xls file
                </div>
                <div style={{ color: '#9CA3AF', fontSize: 10, marginTop: 3 }}>
                  Columns: test_case_id · step_id · action · selector · input_value · expected_result
                </div>
              </div>

              {uploadError && (
                <div style={{ background: '#FEE2E2', border: '1px solid #FECACA', borderRadius: 5, padding: '6px 10px', fontSize: 11, color: '#dc2626', marginBottom: 8 }}>
                  {uploadError}
                </div>
              )}

              <button type="submit" disabled={uploading} style={{
                width: '100%', padding: '8px 0',
                background: uploading ? '#93C5FD' : '#003087',
                border: 'none', borderRadius: 5, color: '#fff',
                fontWeight: 700, fontSize: 12, cursor: uploading ? 'not-allowed' : 'pointer',
              }}>
                {uploading ? 'Uploading…' : 'Upload & Parse'}
              </button>
            </form>

            {/* Headless toggle — matches the Streamlit sidebar checkbox */}
            <label style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 10, cursor: 'pointer', userSelect: 'none' }}>
              <input
                type="checkbox"
                checked={headless}
                onChange={e => setHeadless(e.target.checked)}
                style={{ accentColor: '#0066CC', width: 14, height: 14, cursor: 'pointer' }}
              />
              <span style={{ fontSize: 12, color: '#4A4A4A' }}>
                Headless browser
              </span>
              <span style={{ fontSize: 10, color: '#9CA3AF', marginLeft: 2 }}>
                (default: off — headed mode for BSS portals)
              </span>
            </label>
          </div>

          {/* Run list */}
          <div style={{ flex: 1, overflowY: 'auto', padding: '12px 14px' }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#6B7280', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 8 }}>
              Test Runs ({runs.length})
            </div>
            {runs.length === 0 ? (
              <div style={{ color: '#9CA3AF', fontSize: 12, textAlign: 'center', marginTop: 20 }}>
                No runs yet. Upload a file to begin.
              </div>
            ) : (
              runs.map(run => (
                <RunCard
                  key={run.id}
                  run={run}
                  isActive={run.id === selectedId}
                  onClick={() => selectRun(run.id)}
                />
              ))
            )}
          </div>
        </div>

        {/* ── Right panel: status dashboard + results ── */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '20px 24px' }}>
          {!selectedId ? (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '55vh', gap: 14, color: '#9CA3AF' }}>
              <svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="9 11 12 14 22 4"/>
                <path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/>
              </svg>
              <p style={{ margin: 0, fontSize: 13 }}>Upload a test case file or select a run from the list</p>
            </div>
          ) : (
            <>
              {/* Status summary card */}
              {statusData && (
                <div style={{
                  background: '#FFFFFF', border: '1px solid #E0E8F0',
                  borderRadius: 10, padding: '16px 20px', marginBottom: 20,
                }}>
                  {/* Title row */}
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14, flexWrap: 'wrap' }}>
                    <span style={{ fontWeight: 700, fontSize: 14, color: '#001F5B' }}>
                      Run #{statusData.test_run_id} — {statusData.filename}
                    </span>
                    <StatusBadge status={statusData.status} />
                    {statusData.status === 'running' && (
                      <span style={{ color: '#d97706', fontSize: 12, animation: 'blink 1.2s step-start infinite' }}>● Executing…</span>
                    )}
                    <div style={{ marginLeft: 'auto', display: 'flex', gap: 8, alignItems: 'center' }}>
                      {canRun && (
                        <button onClick={handleRun} style={{
                          background: '#16a34a', border: 'none', borderRadius: 6,
                          padding: '7px 16px', color: '#fff', fontWeight: 700,
                          fontSize: 12, cursor: 'pointer',
                        }}>
                          ▶ Run Tests
                        </button>
                      )}
                    </div>
                  </div>

                  {runError && (
                    <div style={{ background: '#FEE2E2', border: '1px solid #FECACA', borderRadius: 6, padding: '7px 12px', fontSize: 12, color: '#dc2626', marginBottom: 12 }}>
                      {runError}
                    </div>
                  )}

                  {/* Counters */}
                  <div style={{ display: 'flex', gap: 28, flexWrap: 'wrap', marginBottom: 12 }}>
                    {[
                      { label: 'Total',     val: statusData.total_steps     || 0, color: '#6B7280' },
                      { label: 'Completed', val: statusData.completed_steps || 0, color: '#0066CC' },
                      { label: 'Passed',    val: statusData.passed          || 0, color: '#16a34a' },
                      { label: 'Failed',    val: statusData.failed          || 0, color: '#dc2626' },
                    ].map(({ label, val, color }) => (
                      <div key={label}>
                        <div style={{ fontSize: 22, fontWeight: 700, color }}>{val}</div>
                        <div style={{ fontSize: 10, color: '#9CA3AF' }}>{label}</div>
                      </div>
                    ))}
                  </div>

                  {/* Progress bar */}
                  <ProgressBar
                    value={statusData.completed_steps || 0}
                    max={statusData.total_steps || 1}
                    color={(statusData.failed || 0) > 0 ? '#dc2626' : '#16a34a'}
                  />

                  {/* Timestamps */}
                  <div style={{ display: 'flex', gap: 20, marginTop: 10, flexWrap: 'wrap' }}>
                    {[
                      { label: 'Created',  ts: statusData.created_at },
                      { label: 'Started',  ts: statusData.started_at },
                      { label: 'Finished', ts: statusData.finished_at },
                    ].map(({ label, ts }) => ts && (
                      <div key={label} style={{ fontSize: 10, color: '#9CA3AF' }}>
                        <span style={{ color: '#6B7280' }}>{label}: </span>{fmt(ts)}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Results */}
              {resultsData?.results?.length > 0 ? (
                <>
                  <div style={{ fontSize: 11, fontWeight: 700, color: '#6B7280', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 12 }}>
                    Results by Test Case
                  </div>
                  <ResultsTable results={resultsData.results} />
                </>
              ) : statusData?.status === 'pending' ? (
                <div style={{ textAlign: 'center', color: '#9CA3AF', fontSize: 13, marginTop: 32 }}>
                  Run is ready. Click <strong style={{ color: '#16a34a' }}>▶ Run Tests</strong> to execute.
                </div>
              ) : statusData?.status === 'running' ? (
                <div style={{ textAlign: 'center', color: '#d97706', fontSize: 13, marginTop: 32 }}>
                  Executing… results will appear here automatically.
                </div>
              ) : null}
            </>
          )}
        </div>
      </div>

      <style>{`
        @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.3} }
      `}</style>
    </div>
  );
}
