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
    return <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#F4F6FA', color: '#003087' }}>Loading...</div>;
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
    <div style={{ minHeight: '100vh', background: '#F4F6FA', color: '#222222', fontFamily: "'Helvetica Neue', Arial, 'Segoe UI', sans-serif", overflowY: 'auto', padding: '24px 28px' }}>

      {/* Header */}
      <div style={{ textAlign: 'left', marginBottom: 28 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, margin: 0, color: '#001F5B' }}>UAT Dashboard</h1>
        <p style={{ color: '#666', fontSize: 13, margin: '4px 0 0' }}>Comprehensive overview of User Acceptance Testing</p>
      </div>

      {/* ──── Summary Cards ───────────────────────────────────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 16, marginBottom: 24 }}>
        {/* Total Test Cases */}
        <div style={{ background: '#FFFFFF', borderRadius: 10, padding: 20, border: '1px solid #E0E8F0' }}>
          <div style={{ fontSize: 11, color: '#6B7280', fontWeight: 600, marginBottom: 4 }}>Total Test Cases</div>
          <div style={{ fontSize: 36, fontWeight: 800, color: '#0091DA' }}>{total}</div>
          <div style={{ fontSize: 10, color: '#9CA3AF' }}>Test cases in system</div>
        </div>

        {/* Lines of Business */}
        <div style={{ background: '#FFFFFF', borderRadius: 10, padding: 20, border: '1px solid #E0E8F0' }}>
          <div style={{ fontSize: 11, color: '#6B7280', fontWeight: 600, marginBottom: 4 }}>Lines of Business</div>
          <div style={{ fontSize: 36, fontWeight: 800, color: '#16A34A' }}>{lobCount}</div>
          <div style={{ fontSize: 10, color: '#9CA3AF' }}>Unique LoBs</div>
        </div>

        {/* Test Case Status */}
        <div style={{ background: '#FFFFFF', borderRadius: 10, padding: 20, border: '1px solid #E0E8F0' }}>
          <div style={{ fontSize: 11, color: '#6B7280', fontWeight: 600, marginBottom: 8 }}>Test Case Status</div>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 18, fontWeight: 700, color: '#16A34A' }}>{closed}</div>
              <div style={{ fontSize: 9, color: '#9CA3AF' }}>Closed</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 18, fontWeight: 700, color: '#D97706' }}>{open}</div>
              <div style={{ fontSize: 9, color: '#9CA3AF' }}>Open</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 18, fontWeight: 700, color: '#DC2626' }}>{defects}</div>
              <div style={{ fontSize: 9, color: '#9CA3AF' }}>Defects</div>
            </div>
          </div>
          {/* Status bar */}
          <div style={{ display: 'flex', borderRadius: 6, overflow: 'hidden', height: 10, background: '#E5E7EB' }}>
            {closedPct > 0 && <div style={{ width: `${closedPct}%`, background: '#16A34A' }} />}
            {openPct > 0 && <div style={{ width: `${openPct}%`, background: '#0091DA' }} />}
            {defectPct > 0 && <div style={{ width: `${defectPct}%`, background: '#DC2626' }} />}
          </div>
          {/* Completion badge */}
          <div style={{ marginTop: 10, textAlign: 'center', background: '#E0F2FE', borderRadius: 6, padding: '6px 10px' }}>
            <div style={{ fontSize: 10, color: '#6B7280' }}>Completion:</div>
            <div style={{ fontSize: 14, fontWeight: 700, color: '#0091DA' }}>{completionPct}% ({closed}/{total})</div>
          </div>
        </div>

        {/* Priority Distribution */}
        <div style={{ background: '#FFFFFF', borderRadius: 10, padding: 20, border: '1px solid #E0E8F0' }}>
          <div style={{ fontSize: 11, color: '#6B7280', fontWeight: 600, marginBottom: 8 }}>Priority Distribution</div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 22, fontWeight: 700, color: '#DC2626' }}>{high}</div>
              <div style={{ fontSize: 9, color: '#9CA3AF' }}>High</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 22, fontWeight: 700, color: '#D97706' }}>{medium}</div>
              <div style={{ fontSize: 9, color: '#9CA3AF' }}>Medium</div>
            </div>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 22, fontWeight: 700, color: '#16A34A' }}>{low}</div>
              <div style={{ fontSize: 9, color: '#9CA3AF' }}>Low</div>
            </div>
          </div>
        </div>
      </div>

      {/* ──── LOB Wise Test Case Distribution ─────────────────────────────────── */}
      <div style={{ background: '#FFFFFF', borderRadius: 12, padding: 24, border: '1px solid #E0E8F0' }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, margin: '0 0 16px', color: '#001F5B' }}>LOB Wise Test Case Distribution</h2>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: '#F4F6FA' }}>
                {['LOB', 'OPEN', 'REOPENED', 'CLOSED', 'CANCELLED', 'READY_FOR_TESTING', 'NEEDS_FIX', 'DEFECT'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 700, fontSize: 11, color: '#001F5B', borderBottom: '1px solid #E0E8F0' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {lobDist.map((r, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #F0F0F0' }}>
                  <td style={{ padding: '10px 14px', fontWeight: 600, color: '#222222' }}>{r.lob}</td>
                  <td style={{ padding: '10px 14px', color: '#DC2626', fontWeight: 600 }}>{r.open || 0}</td>
                  <td style={{ padding: '10px 14px', color: '#222222' }}>{r.reopened || 0}</td>
                  <td style={{ padding: '10px 14px', color: '#16A34A', fontWeight: 600 }}>{r.closed || 0}</td>
                  <td style={{ padding: '10px 14px', color: '#222222' }}>{r.cancelled || 0}</td>
                  <td style={{ padding: '10px 14px', color: '#222222' }}>{r.ready_for_testing || 0}</td>
                  <td style={{ padding: '10px 14px', color: '#222222' }}>{r.needs_fix || 0}</td>
                  <td style={{ padding: '10px 14px', color: '#222222' }}>{r.defect || 0}</td>
                </tr>
              ))}
              {/* Totals row */}
              <tr style={{ borderTop: '2px solid #E0E8F0', fontWeight: 700 }}>
                <td style={{ padding: '10px 14px', color: '#222222' }}>TOTAL</td>
                <td style={{ padding: '10px 14px', color: '#DC2626' }}>{lobTotals.open}</td>
                <td style={{ padding: '10px 14px', color: '#222222' }}>{lobTotals.reopened}</td>
                <td style={{ padding: '10px 14px', color: '#16A34A' }}>{lobTotals.closed}</td>
                <td style={{ padding: '10px 14px', color: '#222222' }}>{lobTotals.cancelled}</td>
                <td style={{ padding: '10px 14px', color: '#222222' }}>{lobTotals.ready_for_testing}</td>
                <td style={{ padding: '10px 14px', color: '#222222' }}>{lobTotals.needs_fix}</td>
                <td style={{ padding: '10px 14px', color: '#222222' }}>{lobTotals.defect}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
