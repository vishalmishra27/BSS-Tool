import { useEffect, useState } from 'react';

// ─── Circular gauge component ────────────────────────────────────────────────
const CircularGauge = ({ percent, size = 70, strokeWidth = 6, color = '#f59e0b' }) => {
  const r = (size - strokeWidth) / 2;
  const circ = 2 * Math.PI * r;
  const offset = circ - (percent / 100) * circ;
  return (
    <svg width={size} height={size} style={{ transform: 'rotate(-90deg)' }}>
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="rgba(255,255,255,0.15)" strokeWidth={strokeWidth} />
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={color} strokeWidth={strokeWidth}
        strokeDasharray={circ} strokeDashoffset={offset} strokeLinecap="round" />
      <text x="50%" y="50%" textAnchor="middle" dy="0.35em" fill="#fff" fontSize={14} fontWeight={700}
        style={{ transform: 'rotate(90deg)', transformOrigin: '50% 50%' }}>{percent}%</text>
    </svg>
  );
};

// ─── Donut chart component ───────────────────────────────────────────────────
const DonutChart = ({ total, migrate, purge, size = 180 }) => {
  const r = 65, sw = 28, circ = 2 * Math.PI * r;
  const migPct = total ? migrate / total : 0;
  return (
    <svg width={size} height={size} viewBox="0 0 180 180">
      <circle cx="90" cy="90" r={r} fill="none" stroke="#e74c3c" strokeWidth={sw} />
      <circle cx="90" cy="90" r={r} fill="none" stroke="#2ecc71" strokeWidth={sw}
        strokeDasharray={circ} strokeDashoffset={circ * (1 - migPct)}
        style={{ transform: 'rotate(-90deg)', transformOrigin: '50% 50%' }} />
      <text x="90" y="82" textAnchor="middle" fill="#2ecc71" fontSize="28" fontWeight="800">{total}</text>
      <text x="90" y="104" textAnchor="middle" fill="rgba(255,255,255,0.7)" fontSize="10">TOTAL PRODUCTS</text>
    </svg>
  );
};

// ─── Progress bar ────────────────────────────────────────────────────────────
const ProgressBar = ({ matched, total, label }) => {
  const pct = total ? Math.round((matched / total) * 100) : 0;
  const color = pct >= 80 ? '#22c55e' : pct >= 50 ? '#f59e0b' : '#ef4444';
  return (
    <div style={{ marginTop: 6 }}>
      <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.5)', marginBottom: 2 }}>Progress</div>
      <div style={{ background: 'rgba(255,255,255,0.1)', borderRadius: 4, height: 6, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: `linear-gradient(90deg, ${color}, ${color}dd)`, borderRadius: 4, transition: 'width 0.5s' }} />
      </div>
      {label && <div style={{ fontSize: 9, color: pct >= 80 ? '#22c55e' : '#f59e0b', marginTop: 2 }}>{label}</div>}
    </div>
  );
};

export default function ProductDashboardPage() {
  const [summary, setSummary] = useState([]);
  const [params, setParams] = useState([]);
  const [lobDist, setLobDist] = useState([]);
  const [loading, setLoading] = useState(true);
  const [lobFilter, setLobFilter] = useState('All');
  const [view, setView] = useState('journey'); // journey | list

  useEffect(() => {
    Promise.all([
      fetch('/api/products/summary').then(r => r.json()),
      fetch('/api/parameters').then(r => r.json()),
      fetch('/api/uat/lob-distribution').then(r => r.json()),
    ]).then(([s, p, l]) => {
      setSummary(Array.isArray(s) ? s : []);
      setParams(Array.isArray(p) ? p : []);
      setLobDist(Array.isArray(l) ? l : []);
    }).finally(() => setLoading(false));
  }, []);

  // Aggregates
  const totalProducts = summary.reduce((a, r) => a + Number(r.total || 0), 0);
  const toMigrate = summary.reduce((a, r) => a + Number(r.to_migrate || 0), 0);
  const toPurge = summary.reduce((a, r) => a + Number(r.to_purge || 0), 0);
  const totalConfigured = summary.reduce((a, r) => a + Number(r.configured || 0), 0);
  const configPending = totalProducts - totalConfigured;
  const configPct = totalProducts ? Math.round((totalConfigured / totalProducts) * 100) : 0;
  const rationalized = totalProducts; // all products have a flag
  const ratPending = 0;
  const ratPct = totalProducts ? 100 : 0;

  // Parameter cards — filtered
  const filteredParams = lobFilter === 'All' ? params : params.filter(p => p.lob === lobFilter);
  const uniqueLobs = [...new Set(params.map(p => p.lob).filter(Boolean))];

  // LOB performance
  const lobPerf = uniqueLobs.map(lob => {
    const lobParams = params.filter(p => p.lob === lob);
    const totalMatched = lobParams.reduce((a, p) => a + Number(p.matched || 0), 0);
    const totalAll = lobParams.reduce((a, p) => a + Number(p.total || 0), 0);
    const pct = totalAll ? Math.round((totalMatched / totalAll) * 100) : 0;
    return { lob, products: lobParams.length, pct };
  });

  if (loading) {
    return <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#0a1628', color: '#00B0F0' }}>Loading...</div>;
  }

  return (
    <div style={{ minHeight: '100vh', background: 'linear-gradient(135deg, #0a1628 0%, #0f2847 50%, #132e4a 100%)', color: '#fff', fontFamily: "'Segoe UI', sans-serif", overflowY: 'auto', padding: '24px 28px' }}>

      {/* Header */}
      <div style={{ textAlign: 'center', marginBottom: 28 }}>
        <h1 style={{ fontSize: 28, fontWeight: 700, margin: 0 }}>Product Journey</h1>
        <p style={{ color: 'rgba(255,255,255,0.5)', fontSize: 13, margin: '4px 0 0' }}>Overview of product storyline starting from Rationalization to UAT</p>
      </div>

      {/* ──── Section 1: Rationalization Overview ──────────────────────────────── */}
      <div style={{ background: 'linear-gradient(135deg, #0d3320, #145a32)', borderRadius: 12, padding: 24, marginBottom: 24 }}>
        <h2 style={{ fontSize: 20, fontWeight: 700, color: '#2ecc71', margin: '0 0 20px' }}>Rationalization Overview</h2>
        <p style={{ color: 'rgba(255,255,255,0.5)', fontSize: 12, marginTop: -14, marginBottom: 20 }}>Track migration progress and product optimization status</p>

        <div style={{ display: 'flex', gap: 20, alignItems: 'center', flexWrap: 'wrap' }}>
          {/* Donut */}
          <div style={{ flex: '0 0 auto' }}>
            <DonutChart total={totalProducts} migrate={toMigrate} purge={toPurge} />
          </div>

          {/* Migrate / Purge cards */}
          <div style={{ display: 'flex', gap: 16, flex: 1, flexWrap: 'wrap' }}>
            <div style={{ flex: 1, minWidth: 160, background: 'rgba(255,255,255,0.05)', borderRadius: 10, padding: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                <span style={{ fontSize: 20 }}>📦</span>
                <div>
                  <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)' }}>To be Migrated</div>
                  <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.4)' }}>Ready for migration</div>
                </div>
                <div style={{ marginLeft: 'auto', fontSize: 26, fontWeight: 800, color: '#2ecc71' }}>{toMigrate}</div>
              </div>
              <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)' }}>Out of Total Percentage: <span style={{ color: '#fff' }}>{totalProducts ? ((toMigrate / totalProducts) * 100).toFixed(1) : 0}%</span></div>
            </div>
            <div style={{ flex: 1, minWidth: 160, background: 'rgba(255,255,255,0.05)', borderRadius: 10, padding: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                <span style={{ fontSize: 20 }}>🗑️</span>
                <div>
                  <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.5)' }}>To be Purged</div>
                  <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.4)' }}>Scheduled for removal</div>
                </div>
                <div style={{ marginLeft: 'auto', fontSize: 26, fontWeight: 800, color: '#e74c3c' }}>{toPurge}</div>
              </div>
              <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)' }}>Out of Total Percentage: <span style={{ color: '#fff' }}>{totalProducts ? ((toPurge / totalProducts) * 100).toFixed(1) : 0}%</span></div>
            </div>
          </div>

          {/* Rationalization complete badge */}
          <div style={{ flex: '0 0 200px', background: 'rgba(255,255,255,0.05)', borderRadius: 10, padding: 16, textAlign: 'center' }}>
            <div style={{ fontSize: 32, fontWeight: 900, color: '#2ecc71' }}>{ratPct}%</div>
            <div style={{ fontSize: 13, fontWeight: 700, color: '#2ecc71' }}>Rationalization Complete</div>
            <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)', marginTop: 4 }}>Products successfully rationalized and ready for configuration</div>
            <div style={{ display: 'flex', justifyContent: 'center', gap: 20, marginTop: 10 }}>
              <div><div style={{ fontSize: 18, fontWeight: 700, color: '#2ecc71' }}>{rationalized}</div><div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>Rationalized</div></div>
              <div><div style={{ fontSize: 18, fontWeight: 700, color: '#f59e0b' }}>{ratPending}</div><div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>Pending</div></div>
            </div>
            <button onClick={() => setView(v => v === 'list' ? 'journey' : 'list')}
              style={{ marginTop: 10, padding: '6px 14px', borderRadius: 6, border: '1px solid #2ecc71', background: 'transparent', color: '#2ecc71', fontSize: 11, fontWeight: 600, cursor: 'pointer' }}>
              View Rationalized Product List
            </button>
          </div>
        </div>
      </div>

      {/* ──── Section 2: Configuration Overview ───────────────────────────────── */}
      <div style={{ background: 'rgba(255,255,255,0.04)', borderRadius: 12, padding: 24, marginBottom: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, color: '#00B0F0', margin: '0 0 16px', display: 'flex', alignItems: 'center', gap: 8 }}>
          <span>⚙️</span> Configuration Overview
        </h2>
        {/* Progress bar */}
        <div style={{ marginBottom: 16 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
            <span style={{ color: 'rgba(255,255,255,0.6)' }}>Completion Progress</span>
            <span style={{ color: '#2ecc71', fontWeight: 700 }}>{configPct}%</span>
          </div>
          <div style={{ background: 'rgba(255,255,255,0.1)', borderRadius: 6, height: 10, overflow: 'hidden' }}>
            <div style={{ width: `${configPct}%`, height: '100%', background: 'linear-gradient(90deg, #00B0F0, #2ecc71)', borderRadius: 6, transition: 'width 0.5s' }} />
          </div>
        </div>
        <div style={{ display: 'flex', gap: 16, marginBottom: 20 }}>
          <div style={{ flex: 1, background: 'linear-gradient(135deg, #0d7377, #14b8a6)', borderRadius: 10, padding: '16px 20px', textAlign: 'center' }}>
            <div style={{ fontSize: 28, fontWeight: 800 }}>{totalConfigured}</div>
            <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.7)' }}>Configured</div>
          </div>
          <div style={{ flex: 1, background: 'linear-gradient(135deg, #7c2d12, #dc2626)', borderRadius: 10, padding: '16px 20px', textAlign: 'center' }}>
            <div style={{ fontSize: 28, fontWeight: 800 }}>{configPending}</div>
            <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.7)' }}>Pending</div>
          </div>
        </div>

        {/* Configuration Validation — Top Performing LOBs */}
        <h3 style={{ fontSize: 14, fontWeight: 700, color: '#00B0F0', margin: '0 0 4px' }}>Configuration Validation</h3>
        <p style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)', margin: '0 0 12px' }}>Top Performing LOBs Section - Integrated into Configuration Overview</p>
        <h4 style={{ fontSize: 13, fontWeight: 700, margin: '0 0 10px', display: 'flex', alignItems: 'center', gap: 6 }}>🏆 Top Performing LOBs</h4>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 12 }}>
          {lobPerf.map(l => (
            <div key={l.lob} style={{ flex: '1 1 180px', background: 'rgba(255,255,255,0.06)', borderRadius: 10, padding: 14, display: 'flex', alignItems: 'center', gap: 12 }}>
              <div>
                <div style={{ fontWeight: 700, fontSize: 13 }}>{l.lob}</div>
                <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)' }}>{l.products} products</div>
                <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)', marginTop: 4 }}>Completion Progress</div>
                <div style={{ background: 'rgba(255,255,255,0.1)', borderRadius: 4, height: 5, width: 120, marginTop: 2, overflow: 'hidden' }}>
                  <div style={{ width: `${l.pct}%`, height: '100%', background: '#f59e0b', borderRadius: 4 }} />
                </div>
                <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)', marginTop: 2 }}>{l.pct}% complete</div>
              </div>
              <div style={{ marginLeft: 'auto', fontSize: 20, fontWeight: 800, color: '#2ecc71' }}>{l.pct}%</div>
            </div>
          ))}
        </div>
        <div style={{ textAlign: 'center', fontSize: 12, color: 'rgba(255,255,255,0.5)' }}>
          Average Completion Rate: <strong style={{ color: '#fff' }}>{lobPerf.length ? Math.round(lobPerf.reduce((a, l) => a + l.pct, 0) / lobPerf.length) : 0}%</strong>
        </div>
      </div>

      {/* ──── Section 3: Parameter Status Overview ────────────────────────────── */}
      <div style={{ background: 'rgba(255,255,255,0.04)', borderRadius: 12, padding: 24, marginBottom: 24 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h2 style={{ fontSize: 18, fontWeight: 700, margin: 0 }}>Parameter Status Overview</h2>
          <select value={lobFilter} onChange={e => setLobFilter(e.target.value)}
            style={{ padding: '6px 12px', borderRadius: 6, border: '1px solid rgba(255,255,255,0.2)', background: 'rgba(255,255,255,0.08)', color: '#fff', fontSize: 12 }}>
            <option value="All">All</option>
            {uniqueLobs.map(l => <option key={l} value={l}>{l}</option>)}
          </select>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 14 }}>
          {filteredParams.map(p => {
            const pct = p.total ? Math.round((p.matched / p.total) * 100) : 0;
            const pending = p.total - p.matched;
            return (
              <div key={p.id} style={{ background: 'rgba(255,255,255,0.06)', borderRadius: 10, padding: 14, border: '1px solid rgba(255,255,255,0.08)' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
                  <div style={{ fontSize: 13, fontWeight: 700, lineHeight: 1.3, maxWidth: '70%' }}>{p.param_name}</div>
                  <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)' }}>Total<br /><span style={{ fontWeight: 700, color: '#fff' }}>{p.total}</span></div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <CircularGauge percent={pct} size={60} strokeWidth={5} />
                  <div>
                    <div style={{ display: 'flex', gap: 12 }}>
                      <div><div style={{ fontSize: 16, fontWeight: 700, color: '#2ecc71' }}>{p.matched}</div><div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>Matched</div></div>
                      <div><div style={{ fontSize: 16, fontWeight: 700, color: '#e74c3c' }}>{pending}</div><div style={{ fontSize: 9, color: 'rgba(255,255,255,0.4)' }}>Pending</div></div>
                    </div>
                  </div>
                </div>
                <ProgressBar matched={p.matched} total={p.total} label={pct >= 80 ? 'Good' : pct >= 50 ? 'In Progress' : 'Needs Attention'} />
              </div>
            );
          })}
        </div>
      </div>

      {/* ──── Section 4: Product Test Cases (LOB Wise) ────────────────────────── */}
      <div style={{ background: 'rgba(255,255,255,0.04)', borderRadius: 12, padding: 24, marginBottom: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, margin: '0 0 16px' }}>Product Test Cases (LOB Wise)</h2>
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
            </tbody>
          </table>
        </div>
      </div>

      {/* ──── Quick Actions ───────────────────────────────────────────────────── */}
      <div style={{ background: 'linear-gradient(135deg, rgba(0,176,240,0.15), rgba(0,176,240,0.05))', borderRadius: 12, padding: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, margin: '0 0 16px' }}>Quick Actions</h2>
        <button onClick={() => setView(v => v === 'list' ? 'journey' : 'list')}
          style={{ width: '100%', padding: '14px 20px', borderRadius: 8, border: 'none', background: 'linear-gradient(135deg, #0d7377, #14b8a6)', color: '#fff', fontSize: 14, fontWeight: 700, cursor: 'pointer' }}>
          View Product List
        </button>
      </div>

      {/* ──── Product List Modal ───────────────────────────────────────────────── */}
      {view === 'list' && <ProductListModal onClose={() => setView('journey')} />}
    </div>
  );
}

// ─── Product list overlay ────────────────────────────────────────────────────
function ProductListModal({ onClose }) {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [lobFilter, setLobFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');

  useEffect(() => {
    fetch('/api/legacy-products/raw').then(r => r.json()).then(d => setProducts(d.data || []))
      .finally(() => setLoading(false));
  }, []);

  const lobs = [...new Set(products.map(p => p.lob).filter(Boolean))];
  const statuses = [...new Set(products.map(p => p.rationalization_status).filter(Boolean))];
  const filtered = products.filter(p =>
    (!lobFilter || p.lob === lobFilter) && (!statusFilter || p.rationalization_status === statusFilter)
  );

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)', zIndex: 1000, display: 'flex', alignItems: 'center', justifyContent: 'center' }}
      onClick={onClose}>
      <div style={{ background: '#fff', borderRadius: 12, width: '90%', maxWidth: 900, maxHeight: '80vh', overflow: 'auto', color: '#1e293b', padding: 24 }}
        onClick={e => e.stopPropagation()}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: '#1e40af' }}>Legacy Products Inventory</h2>
          <div style={{ display: 'flex', gap: 8 }}>
            <select value={lobFilter} onChange={e => setLobFilter(e.target.value)} style={{ padding: '6px 10px', borderRadius: 6, border: '1px solid #d1d5db', fontSize: 12 }}>
              <option value="">All LoBs</option>
              {lobs.map(l => <option key={l} value={l}>{l}</option>)}
            </select>
            <select value={statusFilter} onChange={e => setStatusFilter(e.target.value)} style={{ padding: '6px 10px', borderRadius: 6, border: '1px solid #d1d5db', fontSize: 12 }}>
              <option value="">All Statuses</option>
              {statuses.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <button onClick={onClose} style={{ padding: '6px 14px', borderRadius: 6, border: 'none', background: '#1e40af', color: '#fff', fontWeight: 600, cursor: 'pointer', fontSize: 12 }}>Close</button>
          </div>
        </div>
        {loading ? <div>Loading...</div> : (
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: 'linear-gradient(135deg, #1e40af, #3b82f6)', color: '#fff' }}>
                {['Product ID', 'Product Name', 'LOB', 'Status', 'Pending On'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 600 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.map((p, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #e5e7eb' }}>
                  <td style={{ padding: '10px 14px' }}>{p.product_id}</td>
                  <td style={{ padding: '10px 14px' }}>{p.product_name}</td>
                  <td style={{ padding: '10px 14px' }}>{p.lob}</td>
                  <td style={{ padding: '10px 14px' }}>{p.rationalization_status}</td>
                  <td style={{ padding: '10px 14px' }}>{p.pending_on || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
