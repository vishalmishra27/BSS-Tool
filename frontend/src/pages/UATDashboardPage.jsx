import { useEffect, useState } from 'react';

export default function UATDashboardPage() {
  const [summary, setSummary] = useState(null);
  const [lobDist, setLobDist] = useState([]);
  const [priorityDist, setPriorityDist] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch('/api/uat/summary').then(r => r.json()),
      fetch('/api/uat/lob-distribution').then(r => r.json()),
      fetch('/api/uat/priority-distribution').then(r => r.json()),
    ]).then(([s, l, p]) => {
      setSummary(s);
      setLobDist(Array.isArray(l) ? l : []);
      setPriorityDist(Array.isArray(p) ? p : []);
    }).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#0a1628', color: '#00B0F0' }}>Loading...</div>;
  }

  const total = Number(summary?.total || 0);
  const closed = Number(summary?.closed || 0);
  const open = Number(summary?.open || 0);
  const defects = Number(summary?.defects || 0);
  const lobCount = Number(summary?.lob_count || 0);
  const completionPct = total ? ((closed / total) * 100).toFixed(1) : '0';

  // Status bar widths
  const closedPct = total ? (closed / total) * 100 : 0;
  const openPct = total ? (open / total) * 100 : 0;
  const defectPct = total ? (defects / total) * 100 : 0;

  // Priority counts
  const priMap = {};
  priorityDist.forEach(p => { priMap[p.priority] = Number(p.count || 0); });
  const high = priMap['High'] || 0;
  const medium = priMap['Medium'] || 0;
  const low = priMap['Low'] || 0;

  // LOB totals row
  const lobTotals = {
    open: lobDist.reduce((a, r) => a + Number(r.open || 0), 0),
    reopened: lobDist.reduce((a, r) => a + Number(r.reopened || 0), 0),
    closed: lobDist.reduce((a, r) => a + Number(r.closed || 0), 0),
    cancelled: lobDist.reduce((a, r) => a + Number(r.cancelled || 0), 0),
    ready_for_testing: lobDist.reduce((a, r) => a + Number(r.ready_for_testing || 0), 0),
    needs_fix: lobDist.reduce((a, r) => a + Number(r.needs_fix || 0), 0),
    defect: lobDist.reduce((a, r) => a + Number(r.defect || 0), 0),
  };

  return (
    <div style={{ minHeight: '100vh', background: 'linear-gradient(135deg, #0a1628 0%, #0f2847 50%, #132e4a 100%)', color: '#fff', fontFamily: "'Segoe UI', sans-serif", overflowY: 'auto', padding: '24px 28px' }}>

      {/* Header */}
      <div style={{ textAlign: 'center', marginBottom: 28 }}>
        <h1 style={{ fontSize: 28, fontWeight: 700, margin: 0 }}>UAT Dashboard</h1>
        <p style={{ color: 'rgba(255,255,255,0.5)', fontSize: 13, margin: '4px 0 0' }}>Comprehensive overview of User Acceptance Testing</p>
      </div>

      {/* ──── Summary Cards ───────────────────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 16, marginBottom: 24 }}>
        {/* Total Test Cases */}
        <div style={{ background: 'rgba(255,255,255,0.06)', borderRadius: 10, padding: 20, border: '1px solid rgba(255,255,255,0.08)' }}>
          <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', fontWeight: 600, marginBottom: 4 }}>Total Test Cases</div>
          <div style={{ fontSize: 36, fontWeight: 800, color: '#00B0F0' }}>{total}</div>
          <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)' }}>Test cases in system</div>
        </div>

        {/* Lines of Business */}
        <div style={{ background: 'rgba(255,255,255,0.06)', borderRadius: 10, padding: 20, border: '1px solid rgba(255,255,255,0.08)' }}>
          <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', fontWeight: 600, marginBottom: 4 }}>Lines of Business</div>
          <div style={{ fontSize: 36, fontWeight: 800, color: '#2ecc71' }}>{lobCount}</div>
          <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)' }}>Unique LoBs</div>
        </div>

        {/* Test Case Status */}
        <div style={{ background: 'rgba(255,255,255,0.06)', borderRadius: 10, padding: 20, border: '1px solid rgba(255,255,255,0.08)' }}>
          <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', fontWeight: 600, marginBottom: 8 }}>Test Case Status</div>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 18, fontWeight: 700, color: '#22c55e' }}>{closed}</div>
              <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>Closed</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 18, fontWeight: 700, color: '#f59e0b' }}>{open}</div>
              <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>Open</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 18, fontWeight: 700, color: '#ef4444' }}>{defects}</div>
              <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>Defects</div>
            </div>
          </div>
          {/* Status bar */}
          <div style={{ display: 'flex', borderRadius: 6, overflow: 'hidden', height: 10 }}>
            {closedPct > 0 && <div style={{ width: `${closedPct}%`, background: '#22c55e' }} />}
            {openPct > 0 && <div style={{ width: `${openPct}%`, background: '#00B0F0' }} />}
            {defectPct > 0 && <div style={{ width: `${defectPct}%`, background: '#ef4444' }} />}
          </div>
          {/* Completion badge */}
          <div style={{ marginTop: 10, textAlign: 'center', background: 'rgba(0,176,240,0.15)', borderRadius: 6, padding: '6px 10px' }}>
            <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.5)' }}>Completion:</div>
            <div style={{ fontSize: 14, fontWeight: 700, color: '#00B0F0' }}>{completionPct}% ({closed}/{total})</div>
          </div>
        </div>

        {/* Priority Distribution */}
        <div style={{ background: 'rgba(255,255,255,0.06)', borderRadius: 10, padding: 20, border: '1px solid rgba(255,255,255,0.08)' }}>
          <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)', fontWeight: 600, marginBottom: 8 }}>Priority Distribution</div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 22, fontWeight: 700, color: '#ef4444' }}>{high}</div>
              <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>High</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 22, fontWeight: 700, color: '#f59e0b' }}>{medium}</div>
              <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>Medium</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 22, fontWeight: 700, color: '#22c55e' }}>{low}</div>
              <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>Low</div>
            </div>
          </div>
        </div>
      </div>

      {/* ──── LOB Wise Test Case Distribution ─────────────────────────────────── */}
      <div style={{ background: 'rgba(255,255,255,0.04)', borderRadius: 12, padding: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, margin: '0 0 16px' }}>LOB Wise Test Case Distribution</h2>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: 'rgba(0,176,240,0.15)' }}>
                {['LOB', 'OPEN', 'REOPENED', 'CLOSED', 'CANCELLED', 'READY_FOR_TESTING', 'NEEDS_FIX', 'DEFECT'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 700, fontSize: 11, color: 'rgba(255,255,255,0.7)', borderBottom: '1px solid rgba(255,255,255,0.1)' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {lobDist.map((r, i) => (
                <tr key={i} style={{ borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
                  <td style={{ padding: '10px 14px', fontWeight: 600 }}>{r.lob}</td>
                  <td style={{ padding: '10px 14px', color: '#ef4444', fontWeight: 600 }}>{r.open || 0}</td>
                  <td style={{ padding: '10px 14px' }}>{r.reopened || 0}</td>
                  <td style={{ padding: '10px 14px', color: '#22c55e', fontWeight: 600 }}>{r.closed || 0}</td>
                  <td style={{ padding: '10px 14px' }}>{r.cancelled || 0}</td>
                  <td style={{ padding: '10px 14px' }}>{r.ready_for_testing || 0}</td>
                  <td style={{ padding: '10px 14px' }}>{r.needs_fix || 0}</td>
                  <td style={{ padding: '10px 14px' }}>{r.defect || 0}</td>
                </tr>
              ))}
              {/* Totals row */}
              <tr style={{ borderTop: '2px solid rgba(255,255,255,0.15)', fontWeight: 700 }}>
                <td style={{ padding: '10px 14px' }}>TOTAL</td>
                <td style={{ padding: '10px 14px', color: '#ef4444' }}>{lobTotals.open}</td>
                <td style={{ padding: '10px 14px' }}>{lobTotals.reopened}</td>
                <td style={{ padding: '10px 14px', color: '#22c55e' }}>{lobTotals.closed}</td>
                <td style={{ padding: '10px 14px' }}>{lobTotals.cancelled}</td>
                <td style={{ padding: '10px 14px' }}>{lobTotals.ready_for_testing}</td>
                <td style={{ padding: '10px 14px' }}>{lobTotals.needs_fix}</td>
                <td style={{ padding: '10px 14px' }}>{lobTotals.defect}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
