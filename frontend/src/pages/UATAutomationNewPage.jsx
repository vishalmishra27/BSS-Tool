import { useEffect, useRef, useState } from 'react';
import {
  Upload,
  FileText,
  Play,
  CheckCircle,
  XCircle,
  Clock,
  AlertCircle,
  Image,
  X,
  Loader2,
  RotateCcw,
  Monitor,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';

const API = '/api/uat/automation';

function fmt(iso) {
  if (!iso) return '—';
  try { return new Date(iso.endsWith('Z') ? iso : iso + 'Z').toLocaleString(); }
  catch { return iso; }
}

// ── Status badge (light theme) ───────────────────────────────────────────────
function StatusBadge({ status }) {
  const map = {
    passed:    'bg-green-100 text-green-800 border-green-300',
    failed:    'bg-red-100 text-red-800 border-red-300',
    pending:   'bg-blue-100 text-blue-800 border-blue-300',
    running:   'bg-yellow-100 text-yellow-800 border-yellow-300',
    completed: 'bg-green-100 text-green-800 border-green-300',
  };
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold border capitalize ${map[status] || map.pending}`}>
      {status || 'pending'}
    </span>
  );
}

// ── Screenshot lightbox ──────────────────────────────────────────────────────
function Lightbox({ src, onClose }) {
  if (!src) return null;
  return (
    <div onClick={onClose} className="fixed inset-0 bg-black bg-opacity-70 flex items-center justify-center z-50 cursor-zoom-out">
      <div onClick={e => e.stopPropagation()} className="relative max-w-[92vw] max-h-[92vh]">
        <img src={src} alt="screenshot" className="max-w-full max-h-[88vh] rounded-lg shadow-2xl block" />
        <button onClick={onClose} className="absolute -top-3 -right-3 bg-red-600 text-white rounded-full w-7 h-7 flex items-center justify-center hover:bg-red-700">
          <X size={14} />
        </button>
      </div>
    </div>
  );
}

// ── Results table grouped by test case ───────────────────────────────────────
function ResultsTable({ results }) {
  const [lightbox, setLightbox] = useState(null);
  const [collapsed, setCollapsed] = useState({});

  const groups = [];
  const seen = {};
  for (const r of results) {
    if (!seen[r.test_case_id]) {
      seen[r.test_case_id] = [];
      groups.push({ id: r.test_case_id, steps: seen[r.test_case_id] });
    }
    seen[r.test_case_id].push(r);
  }

  const toggleGroup = (id) => setCollapsed(p => ({ ...p, [id]: !p[id] }));

  return (
    <>
      <Lightbox src={lightbox} onClose={() => setLightbox(null)} />
      <div className="space-y-4">
        {groups.map(({ id: tcId, steps }) => {
          const passed = steps.filter(s => s.status === 'passed').length;
          const failed = steps.filter(s => s.status === 'failed').length;
          const pending = steps.filter(s => s.status === 'pending').length;
          const tcStatus = failed > 0 ? 'failed' : pending > 0 ? 'pending' : 'passed';
          const isCollapsed = collapsed[tcId];

          return (
            <div key={tcId} className="bg-white rounded-lg shadow-md border border-gray-200 overflow-hidden">
              {/* Test case header */}
              <button
                onClick={() => toggleGroup(tcId)}
                className={`w-full flex items-center justify-between px-5 py-3 ${
                  tcStatus === 'failed' ? 'bg-red-50' : tcStatus === 'passed' ? 'bg-green-50' : 'bg-blue-50'
                } hover:opacity-90 transition-opacity`}
              >
                <div className="flex items-center space-x-3">
                  {tcStatus === 'passed' ? <CheckCircle size={18} className="text-green-600" /> :
                   tcStatus === 'failed' ? <XCircle size={18} className="text-red-600" /> :
                   <Clock size={18} className="text-blue-600" />}
                  <span className="font-bold text-gray-800">{tcId}</span>
                  <StatusBadge status={tcStatus} />
                </div>
                <div className="flex items-center space-x-4">
                  <span className="text-xs text-gray-500">
                    <span className="text-green-600 font-semibold">{passed}</span> passed
                    {' / '}
                    <span className="text-red-600 font-semibold">{failed}</span> failed
                    {pending > 0 && <> / <span className="text-blue-600 font-semibold">{pending}</span> pending</>}
                    {' / '}{steps.length} steps
                  </span>
                  {isCollapsed ? <ChevronDown size={16} className="text-gray-400" /> : <ChevronUp size={16} className="text-gray-400" />}
                </div>
              </button>

              {/* Steps table */}
              {!isCollapsed && (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm text-left text-gray-600">
                    <thead className="text-xs text-white uppercase bg-[#00338d]">
                      <tr>
                        {['Step', 'Action', 'Selector', 'Input', 'Expected', 'Status', 'Error', 'Screenshot'].map(h => (
                          <th key={h} className="px-4 py-3 font-semibold whitespace-nowrap">{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {steps.map((s, i) => (
                        <tr key={i} className={`border-b ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50'} hover:bg-blue-50 transition-colors`}>
                          <td className="px-4 py-3 text-gray-500 whitespace-nowrap font-medium">{s.step_id}</td>
                          <td className="px-4 py-3 text-blue-700 font-semibold whitespace-nowrap">{s.action}</td>
                          <td className="px-4 py-3 font-mono text-xs max-w-[160px] overflow-hidden text-ellipsis whitespace-nowrap" title={s.selector || ''}>{s.selector || '—'}</td>
                          <td className="px-4 py-3 max-w-[180px] overflow-hidden text-ellipsis whitespace-nowrap" title={s.input_value || ''}>{s.input_value || '—'}</td>
                          <td className="px-4 py-3 max-w-[180px] overflow-hidden text-ellipsis whitespace-nowrap" title={s.expected_result || ''}>{s.expected_result || '—'}</td>
                          <td className="px-4 py-3 whitespace-nowrap"><StatusBadge status={s.status} /></td>
                          <td className="px-4 py-3 text-red-600 text-xs max-w-[220px] overflow-hidden text-ellipsis whitespace-nowrap" title={s.error_message || ''}>{s.error_message || '—'}</td>
                          <td className="px-4 py-3 whitespace-nowrap">
                            {s.screenshot_path ? (
                              <button
                                onClick={() => setLightbox(`${API}/screenshot/${s.screenshot_path}`)}
                                className="px-3 py-1 bg-blue-100 text-blue-700 rounded text-xs font-semibold hover:bg-blue-200 transition-colors flex items-center space-x-1"
                              >
                                <Image size={12} />
                                <span>View</span>
                              </button>
                            ) : <span className="text-gray-300">—</span>}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </>
  );
}

// ── Run card for the sidebar list ────────────────────────────────────────────
function RunCard({ run, isActive, onClick }) {
  const total     = run.total_steps     || 0;
  const completed = run.completed_steps || 0;
  const passed    = run.passed          || 0;
  const failed    = run.failed          || 0;
  const pct       = total > 0 ? Math.min(Math.round((completed / total) * 100), 100) : 0;
  const barColor  = failed > 0 ? 'bg-red-500' : 'bg-green-500';

  return (
    <div
      onClick={onClick}
      className={`rounded-lg p-3 cursor-pointer transition-all border mb-2 ${
        isActive ? 'bg-blue-50 border-blue-300 shadow-sm' : 'bg-white border-gray-200 hover:border-gray-300 hover:shadow-sm'
      }`}
    >
      <div className="flex items-center justify-between mb-2">
        <span className="font-semibold text-gray-800 text-xs truncate flex-1 mr-2">
          #{run.id} — {run.filename}
        </span>
        <StatusBadge status={run.status} />
      </div>
      <div className="flex items-center space-x-2 mb-1">
        <div className="flex-1 bg-gray-200 rounded-full h-1.5 overflow-hidden">
          <div className={`h-full ${barColor} rounded-full transition-all`} style={{ width: `${pct}%` }} />
        </div>
        <span className="text-gray-400 text-xs whitespace-nowrap">{completed}/{total || '?'}</span>
      </div>
      <div className="flex items-center space-x-3 text-xs">
        <span className="text-green-600 font-medium">{passed} passed</span>
        <span className="text-red-600 font-medium">{failed} failed</span>
        <span className="text-gray-400 ml-auto">{fmt(run.created_at)}</span>
      </div>
    </div>
  );
}

// ── Main page ────────────────────────────────────────────────────────────────
export default function UATAutomationNewPage() {
  const [runs,        setRuns]        = useState([]);
  const [selectedId,  setSelectedId]  = useState(null);
  const [statusData,  setStatusData]  = useState(null);
  const [resultsData, setResultsData] = useState(null);
  const [uploading,   setUploading]   = useState(false);
  const [uploadError, setUploadError] = useState('');
  const [runError,    setRunError]    = useState('');
  const [headless,    setHeadless]    = useState(false);
  const [isDragging,  setIsDragging]  = useState(false);
  const fileRef  = useRef();
  const pollRef  = useRef(null);

  const stopPolling = () => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
  };

  const refreshRunList = async () => {
    try {
      const r = await fetch(`${API}/runs`);
      if (r.ok) setRuns(await r.json());
    } catch (_) {}
  };

  const fetchRunData = async (id) => {
    try {
      const [sRes, rRes] = await Promise.all([
        fetch(`${API}/status/${id}`),
        fetch(`${API}/results/${id}`),
      ]);
      if (sRes.ok) {
        const s = await sRes.json();
        setStatusData(s);
        setRuns(prev => prev.map(r => r.id === id ? { ...r, ...s } : r));
        if (rRes.ok) setResultsData(await rRes.json());
        return s;
      }
    } catch (_) {}
    return null;
  };

  const startPolling = (id) => {
    stopPolling();
    pollRef.current = setInterval(async () => {
      const s = await fetchRunData(id);
      if (!s || s.status !== 'running') stopPolling();
    }, 1000);
  };

  useEffect(() => { refreshRunList(); }, []);
  useEffect(() => () => stopPolling(), []);

  const selectRun = async (id) => {
    stopPolling();
    setSelectedId(id);
    setResultsData(null);
    setRunError('');
    const s = await fetchRunData(id);
    if (s?.status === 'running') startPolling(id);
  };

  const handleUpload = async (e) => {
    e?.preventDefault();
    const file = fileRef.current?.files?.[0];
    if (!file) { setUploadError('Please select an .xlsx or .xls file.'); return; }
    setUploadError('');
    setUploading(true);
    try {
      const fd = new FormData();
      fd.append('file', file);
      const res = await fetch(`${API}/upload`, { method: 'POST', body: fd });
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

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    const file = e.dataTransfer.files?.[0];
    if (file) {
      const dt = new DataTransfer();
      dt.items.add(file);
      fileRef.current.files = dt.files;
      handleUpload();
    }
  };

  const handleRun = async () => {
    if (!selectedId) return;
    setRunError('');
    setResultsData(null);
    try {
      const res = await fetch(`${API}/run/${selectedId}?headless=${headless ? 'true' : 'false'}`, { method: 'POST' });
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

  return (
    <div className="bg-slate-100 font-sans min-h-screen flex flex-col">

      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-6 py-5">
        <div className="max-w-screen-xl mx-auto">
          <h1 className="text-3xl font-bold text-gray-800">UAT Automation</h1>
          <p className="text-gray-500 text-sm mt-1">
            Upload an Excel file of test cases, execute them against a live website, and review step-by-step results with screenshots.
          </p>
        </div>
      </div>

      <div className="flex flex-1 overflow-hidden max-w-screen-2xl mx-auto w-full">

        {/* Left panel */}
        <div className="w-80 flex-shrink-0 border-r border-gray-200 bg-white flex flex-col overflow-hidden">

          {/* Upload form */}
          <div className="p-4 border-b border-gray-100">
            <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wide mb-3">Upload & Run</h3>
            <form onSubmit={handleUpload}>
              <div
                className={`border-2 border-dashed rounded-lg p-5 text-center mb-3 transition-colors cursor-pointer ${
                  isDragging ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-gray-400'
                }`}
                onClick={() => fileRef.current?.click()}
                onDrop={handleDrop}
                onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
                onDragLeave={(e) => { e.preventDefault(); setIsDragging(false); }}
              >
                <Upload size={28} className="mx-auto text-gray-400 mb-2" />
                <input ref={fileRef} type="file" accept=".xlsx,.xls" className="hidden" />
                <p className="text-sm text-gray-600 font-medium">Drop .xlsx / .xls file here</p>
                <p className="text-xs text-gray-400 mt-1">
                  test_case_id, step_id, action, selector, input_value, expected_result
                </p>
              </div>

              {uploadError && (
                <div className="flex items-center space-x-2 p-2 mb-3 rounded border-l-4 border-l-red-500 bg-red-50">
                  <AlertCircle size={14} className="text-red-600" />
                  <span className="text-red-700 text-xs">{uploadError}</span>
                </div>
              )}

              <button
                type="submit"
                disabled={uploading}
                className="w-full py-2 bg-blue-600 text-white rounded-lg font-semibold text-sm hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors flex items-center justify-center space-x-2"
              >
                {uploading ? (
                  <>
                    <Loader2 size={16} className="animate-spin" />
                    <span>Uploading...</span>
                  </>
                ) : (
                  <>
                    <Upload size={16} />
                    <span>Upload & Parse</span>
                  </>
                )}
              </button>
            </form>

            <label className="flex items-center space-x-2 mt-3 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={headless}
                onChange={e => setHeadless(e.target.checked)}
                className="accent-blue-600 w-3.5 h-3.5 cursor-pointer"
              />
              <span className="text-xs text-gray-600">Headless browser</span>
              <span className="text-xs text-gray-400">(default: off)</span>
            </label>
          </div>

          {/* Run list */}
          <div className="flex-1 overflow-y-auto p-4">
            <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wide mb-3">
              Test Runs ({runs.length})
            </h3>
            {runs.length === 0 ? (
              <div className="text-center text-gray-400 text-sm mt-8">
                <FileText size={28} className="mx-auto mb-2 text-gray-300" />
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

        {/* Right panel */}
        <div className="flex-1 overflow-y-auto p-6">
          {!selectedId ? (
            <div className="flex flex-col items-center justify-center h-[60vh] text-gray-400">
              <Monitor size={48} className="mb-3 text-gray-300" />
              <p className="text-sm">Upload a test case file or select a run from the list</p>
            </div>
          ) : (
            <>
              {/* Status summary cards */}
              {statusData && (
                <>
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
                    <div className="bg-gradient-to-br from-[#004fd9] to-[#00338d] p-5 rounded-lg text-white shadow-lg">
                      <p className="text-xs uppercase opacity-80">Total Steps</p>
                      <p className="text-3xl font-bold">{statusData.total_steps || 0}</p>
                    </div>
                    <div className="bg-gradient-to-br from-[#004fd9] to-[#00338d] p-5 rounded-lg text-white shadow-lg">
                      <p className="text-xs uppercase opacity-80">Completed</p>
                      <p className="text-3xl font-bold">{statusData.completed_steps || 0}</p>
                    </div>
                    <div className="bg-gradient-to-br from-[#004fd9] to-[#00338d] p-5 rounded-lg text-white shadow-lg">
                      <p className="text-xs uppercase opacity-80">Passed</p>
                      <p className="text-3xl font-bold text-green-300">{statusData.passed || 0}</p>
                    </div>
                    <div className="bg-gradient-to-br from-[#004fd9] to-[#00338d] p-5 rounded-lg text-white shadow-lg">
                      <p className="text-xs uppercase opacity-80">Failed</p>
                      <p className="text-3xl font-bold text-red-300">{statusData.failed || 0}</p>
                    </div>
                  </div>

                  {/* Run info card */}
                  <div className="bg-white p-5 rounded-lg shadow-md border border-gray-200 mb-6">
                    <div className="flex items-center justify-between flex-wrap gap-3 mb-4">
                      <div className="flex items-center space-x-3">
                        <h2 className="text-lg font-bold text-gray-800">
                          Run #{statusData.test_run_id}
                          {statusData.filename && <span className="text-gray-500 font-normal"> — {statusData.filename}</span>}
                        </h2>
                        <StatusBadge status={statusData.status} />
                        {statusData.status === 'running' && (
                          <span className="text-yellow-600 text-xs font-medium animate-pulse">Executing...</span>
                        )}
                      </div>
                      {canRun && (
                        <button
                          onClick={handleRun}
                          className="px-5 py-2 bg-green-600 text-white rounded-lg font-semibold text-sm hover:bg-green-700 transition-colors flex items-center space-x-2"
                        >
                          <Play size={16} />
                          <span>Run Tests</span>
                        </button>
                      )}
                    </div>

                    {runError && (
                      <div className="flex items-center space-x-2 p-3 mb-4 rounded-lg border-l-4 border-l-red-500 bg-red-50">
                        <AlertCircle size={16} className="text-red-600" />
                        <span className="text-red-700 text-sm">{runError}</span>
                      </div>
                    )}

                    {/* Progress bar */}
                    <div className="flex items-center space-x-3 mb-3">
                      <div className="flex-1 bg-gray-200 rounded-full h-2.5 overflow-hidden">
                        <div
                          className={`h-full rounded-full transition-all ${(statusData.failed || 0) > 0 ? 'bg-red-500' : 'bg-green-500'}`}
                          style={{ width: `${statusData.total_steps ? Math.min(Math.round(((statusData.completed_steps || 0) / statusData.total_steps) * 100), 100) : 0}%` }}
                        />
                      </div>
                      <span className="text-xs text-gray-500 font-medium">
                        {statusData.completed_steps || 0} / {statusData.total_steps || 0}
                      </span>
                    </div>

                    {/* Timestamps */}
                    <div className="flex flex-wrap gap-4 text-xs text-gray-400">
                      {statusData.created_at && <span><span className="text-gray-500">Created:</span> {fmt(statusData.created_at)}</span>}
                      {statusData.started_at && <span><span className="text-gray-500">Started:</span> {fmt(statusData.started_at)}</span>}
                      {statusData.finished_at && <span><span className="text-gray-500">Finished:</span> {fmt(statusData.finished_at)}</span>}
                    </div>
                  </div>
                </>
              )}

              {/* Results */}
              {resultsData?.results?.length > 0 ? (
                <>
                  <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wide mb-4">
                    Results by Test Case
                  </h3>
                  <ResultsTable results={resultsData.results} />
                </>
              ) : statusData?.status === 'pending' ? (
                <div className="text-center text-gray-400 text-sm mt-8">
                  <Play size={32} className="mx-auto mb-2 text-gray-300" />
                  <p>Run is ready. Click <strong className="text-green-600">Run Tests</strong> to execute.</p>
                </div>
              ) : statusData?.status === 'running' ? (
                <div className="text-center text-yellow-600 text-sm mt-8">
                  <Loader2 size={32} className="mx-auto mb-2 animate-spin" />
                  <p>Executing... results will appear here automatically.</p>
                </div>
              ) : null}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
