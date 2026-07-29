import { useEffect, useState } from 'react';

// ─── Circular gauge component ────────────────────────────────────────────────
const CircularGauge = ({ percent, size = 70, strokeWidth = 6, color = '#d97706' }) => {
  const r = (size - strokeWidth) / 2;
  const circ = 2 * Math.PI * r;
  const offset = circ - (percent / 100) * circ;
  return (
    <svg width={size} height={size} style={{ transform: 'rotate(-90deg)' }}>
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="#E0E8F0" strokeWidth={strokeWidth} />
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={color} strokeWidth={strokeWidth}
        strokeDasharray={circ} strokeDashoffset={offset} strokeLinecap="round" />
      <text x="50%" y="50%" textAnchor="middle" dy="0.35em" fill="#222222" fontSize={14} fontWeight={700}
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
      <circle cx="90" cy="90" r={r} fill="none" stroke="#DC2626" strokeWidth={sw} />
      <circle cx="90" cy="90" r={r} fill="none" stroke="#16A34A" strokeWidth={sw}
        strokeDasharray={circ} strokeDashoffset={circ * (1 - migPct)}
        style={{ transform: 'rotate(-90deg)', transformOrigin: '50% 50%' }} />
      <text x="90" y="82" textAnchor="middle" fill="#16A34A" fontSize="28" fontWeight="800">{total}</text>
      <text x="90" y="104" textAnchor="middle" fill="#6B7280" fontSize="10">TOTAL PRODUCTS</text>
    </svg>
  );
};

// ─── Progress bar ────────────────────────────────────────────────────────────
const ProgressBar = ({ matched, total, label }) => {
  const pct = total ? Math.round((matched / total) * 100) : 0;
  const color = pct >= 80 ? '#16A34A' : pct >= 50 ? '#D97706' : '#DC2626';
  return (
    <div style={{ marginTop: 6 }}>
      <div style={{ fontSize: 9, color: '#6B7280', marginBottom: 2 }}>Progress</div>
      <div style={{ background: '#E0E8F0', borderRadius: 4, height: 6, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: `linear-gradient(90deg, ${color}, ${color}dd)`, borderRadius: 4, transition: 'width 0.5s' }} />
      </div>
      {label && <div style={{ fontSize: 9, color: pct >= 80 ? '#16A34A' : '#D97706', marginTop: 2 }}>{label}</div>}
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
    return <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#F4F6FA', color: '#0091DA' }}>Loading...</div>;
  }

  return (
    <div style={{ minHeight: '100vh', background: '#F4F6FA', color: '#222222', fontFamily: "'Helvetica Neue', Arial, 'Segoe UI', sans-serif", overflowY: 'auto', padding: '24px 28px' }}>

      {/* Header */}
      <div style={{ textAlign: 'left', marginBottom: 28 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, margin: 0, color: '#001F5B' }}>Product Journey</h1>
        <p style={{ color: '#666', fontSize: 13, margin: '4px 0 0' }}>Overview of product storyline starting from Rationalization to UAT</p>
      </div>

      {/* ──── Section 1: Rationalization Overview ──────────────────────────────── */}
      <div style={{ background: '#FFFFFF', border: '1px solid #E0E8F0', borderRadius: 12, padding: 24, marginBottom: 24 }}>
        <h2 style={{ fontSize: 20, fontWeight: 700, color: '#16A34A', margin: '0 0 20px' }}>Rationalization Overview</h2>
        <p style={{ color: '#6B7280', fontSize: 12, marginTop: -14, marginBottom: 20 }}>Track migration progress and product optimization status</p>

        <div style={{ display: 'flex', gap: 20, alignItems: 'center', flexWrap: 'wrap' }}>
          {/* Donut */}
          <div style={{ flex: '0 0 auto' }}>
            <DonutChart total={totalProducts} migrate={toMigrate} purge={toPurge} />
          </div>

          {/* Migrate / Purge cards */}
          <div style={{ display: 'flex', gap: 16, flex: 1, flexWrap: 'wrap' }}>
            <div style={{ flex: 1, minWidth: 160, background: '#F4F6FA', border: '1px solid #E0E8F0', borderRadius: 10, padding: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                <span style={{ fontSize: 20 }}>📦</span>
                <div>
                  <div style={{ fontSize: 11, color: '#6B7280' }}>To be Migrated</div>
                  <div style={{ fontSize: 11, color: '#9CA3AF' }}>Ready for migration</div>
                </div>
                <div style={{ marginLeft: 'auto', fontSize: 26, fontWeight: 800, color: '#16A34A' }}>{toMigrate}</div>
              </div>
              <div style={{ fontSize: 10, color: '#9CA3AF' }}>Out of Total Percentage: <span style={{ color: '#222222' }}>{totalProducts ? ((toMigrate / totalProducts) * 100).toFixed(1) : 0}%</span></div>
            </div>
            <div style={{ flex: 1, minWidth: 160, background: '#F4F6FA', border: '1px solid #E0E8F0', borderRadius: 10, padding: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                <span style={{ fontSize: 20 }}>🗑️</span>
                <div>
                  <div style={{ fontSize: 11, color: '#6B7280' }}>To be Purged</div>
                  <div style={{ fontSize: 11, color: '#9CA3AF' }}>Scheduled for removal</div>
                </div>
                <div style={{ marginLeft: 'auto', fontSize: 26, fontWeight: 800, color: '#DC2626' }}>{toPurge}</div>
              </div>
              <div style={{ fontSize: 10, color: '#9CA3AF' }}>Out of Total Percentage: <span style={{ color: '#222222' }}>{totalProducts ? ((toPurge / totalProducts) * 100).toFixed(1) : 0}%</span></div>
            </div>
          </div>

          {/* Rationalization complete badge */}
          <div style={{ flex: '0 0 200px', background: '#F4F6FA', border: '1px solid #E0E8F0', borderRadius: 10, padding: 16, textAlign: 'center' }}>
            <div style={{ fontSize: 32, fontWeight: 900, color: '#16A34A' }}>{ratPct}%</div>
            <div style={{ fontSize: 13, fontWeight: 700, color: '#16A34A' }}>Rationalization Complete</div>
            <div style={{ fontSize: 10, color: '#9CA3AF', marginTop: 4 }}>Products successfully rationalized and ready for configuration</div>
            <div style={{ display: 'flex', justifyContent: 'center', gap: 20, marginTop: 10 }}>
              <div><div style={{ fontSize: 18, fontWeight: 700, color: '#16A34A' }}>{rationalized}</div><div style={{ fontSize: 9, color: '#9CA3AF' }}>Rationalized</div></div>
              <div><div style={{ fontSize: 18, fontWeight: 700, color: '#D97706' }}>{ratPending}</div><div style={{ fontSize: 9, color: '#9CA3AF' }}>Pending</div></div>
            </div>
            <button onClick={() => setView(v => v === 'list' ? 'journey' : 'list')}
              style={{ marginTop: 10, padding: '6px 14px', borderRadius: 6, border: '1px solid #16A34A', background: 'transparent', color: '#16A34A', fontSize: 11, fontWeight: 600, cursor: 'pointer' }}>
              View Rationalized Product List
            </button>
          </div>
        </div>
      </div>

      {/* ──── Section 2: Configuration Overview ───────────────────────────────── */}
      <div style={{ background: '#FFFFFF', border: '1px solid #E0E8F0', borderRadius: 12, padding: 24, marginBottom: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, color: '#0091DA', margin: '0 0 16px', display: 'flex', alignItems: 'center', gap: 8 }}>
          <span>⚙️</span> Configuration Overview
        </h2>
        {/* Progress bar */}
        <div style={{ marginBottom: 16 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
            <span style={{ color: '#6B7280' }}>Completion Progress</span>
            <span style={{ color: '#16A34A', fontWeight: 700 }}>{configPct}%</span>
          </div>
          <div style={{ background: '#E0E8F0', borderRadius: 6, height: 10, overflow: 'hidden' }}>
            <div style={{ width: `${configPct}%`, height: '100%', background: 'linear-gradient(90deg, #0091DA, #16A34A)', borderRadius: 6, transition: 'width 0.5s' }} />
          </div>
        </div>
        <div style={{ display: 'flex', gap: 16, marginBottom: 20 }}>
          <div style={{ flex: 1, background: '#DCFCE7', border: '1px solid #E0E8F0', borderRadius: 10, padding: '16px 20px', textAlign: 'center' }}>
            <div style={{ fontSize: 28, fontWeight: 800, color: '#16A34A' }}>{totalConfigured}</div>
            <div style={{ fontSize: 11, color: '#15803d' }}>Configured</div>
          </div>
          <div style={{ flex: 1, background: '#FEE2E2', border: '1px solid #E0E8F0', borderRadius: 10, padding: '16px 20px', textAlign: 'center' }}>
            <div style={{ fontSize: 28, fontWeight: 800, color: '#DC2626' }}>{configPending}</div>
            <div style={{ fontSize: 11, color: '#b91c1c' }}>Pending</div>
          </div>
        </div>

        {/* Configuration Validation — Top Performing LOBs */}
        <h3 style={{ fontSize: 14, fontWeight: 700, color: '#0091DA', margin: '0 0 4px' }}>Configuration Validation</h3>
        <p style={{ fontSize: 10, color: '#9CA3AF', margin: '0 0 12px' }}>Top Performing LOBs Section - Integrated into Configuration Overview</p>
        <h4 style={{ fontSize: 13, fontWeight: 700, margin: '0 0 10px', color: '#222222', display: 'flex', alignItems: 'center', gap: 6 }}>🏆 Top Performing LOBs</h4>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 12 }}>
          {lobPerf.map(l => (
            <div key={l.lob} style={{ flex: '1 1 180px', background: '#F4F6FA', border: '1px solid #E0E8F0', borderRadius: 10, padding: 14, display: 'flex', alignItems: 'center', gap: 12 }}>
              <div>
                <div style={{ fontWeight: 700, fontSize: 13, color: '#222222' }}>{l.lob}</div>
                <div style={{ fontSize: 10, color: '#9CA3AF' }}>{l.products} products</div>
                <div style={{ fontSize: 10, color: '#9CA3AF', marginTop: 4 }}>Completion Progress</div>
                <div style={{ background: '#E0E8F0', borderRadius: 4, height: 5, width: 120, marginTop: 2, overflow: 'hidden' }}>
                  <div style={{ width: `${l.pct}%`, height: '100%', background: '#D97706', borderRadius: 4 }} />
                </div>
                <div style={{ fontSize: 9, color: '#9CA3AF', marginTop: 2 }}>{l.pct}% complete</div>
              </div>
              <div style={{ marginLeft: 'auto', fontSize: 20, fontWeight: 800, color: '#16A34A' }}>{l.pct}%</div>
            </div>
          ))}
        </div>
        <div style={{ textAlign: 'center', fontSize: 12, color: '#6B7280' }}>
          Average Completion Rate: <strong style={{ color: '#222222' }}>{lobPerf.length ? Math.round(lobPerf.reduce((a, l) => a + l.pct, 0) / lobPerf.length) : 0}%</strong>
        </div>
      </div>

      {/* ──── Section 3: Parameter Status Overview ────────────────────────────── */}
      <div style={{ background: '#FFFFFF', border: '1px solid #E0E8F0', borderRadius: 12, padding: 24, marginBottom: 24 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h2 style={{ fontSize: 18, fontWeight: 700, margin: 0, color: '#001F5B' }}>Parameter Status Overview</h2>
          <select value={lobFilter} onChange={e => setLobFilter(e.target.value)}
            style={{ padding: '6px 12px', borderRadius: 6, border: '1px solid #E0E8F0', background: '#FFFFFF', color: '#222222', fontSize: 12 }}>
            <option value="All">All</option>
            {uniqueLobs.map(l => <option key={l} value={l}>{l}</option>)}
          </select>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 14 }}>
          {filteredParams.map(p => {
            const pct = p.total ? Math.round((p.matched / p.total) * 100) : 0;
            const pending = p.total - p.matched;
            return (
              <div key={p.id} style={{ background: '#F4F6FA', borderRadius: 10, padding: 14, border: '1px solid #E0E8F0' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
                  <div style={{ fontSize: 13, fontWeight: 700, lineHeight: 1.3, maxWidth: '70%', color: '#222222' }}>{p.param_name}</div>
                  <div style={{ fontSize: 10, color: '#9CA3AF' }}>Total<br /><span style={{ fontWeight: 700, color: '#222222' }}>{p.total}</span></div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <CircularGauge percent={pct} size={60} strokeWidth={5} />
                  <div>
                    <div style={{ display: 'flex', gap: 12 }}>
                      <div><div style={{ fontSize: 16, fontWeight: 700, color: '#16A34A' }}>{p.matched}</div><div style={{ fontSize: 9, color: '#9CA3AF' }}>Matched</div></div>
                      <div><div style={{ fontSize: 16, fontWeight: 700, color: '#DC2626' }}>{pending}</div><div style={{ fontSize: 9, color: '#9CA3AF' }}>Pending</div></div>
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
      <div style={{ background: '#FFFFFF', border: '1px solid #E0E8F0', borderRadius: 12, padding: 24, marginBottom: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, margin: '0 0 16px', color: '#001F5B' }}>Product Test Cases (LOB Wise)</h2>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: '#F0F6FF' }}>
                {['LOB', 'OPEN', 'REOPENED', 'CLOSED', 'CANCELLED', 'READY_FOR_TESTING', 'NEEDS_FIX', 'DEFECT'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 700, fontSize: 11, color: '#4A4A4A', borderBottom: '1px solid #E0E8F0' }}>{h}</th>
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
            </tbody>
          </table>
        </div>
      </div>

      {/* ──── Quick Actions ───────────────────────────────────────────────────── */}
      <div style={{ background: '#FFFFFF', border: '1px solid #E0E8F0', borderRadius: 12, padding: 24 }}>
        <h2 style={{ fontSize: 18, fontWeight: 700, margin: '0 0 16px', color: '#001F5B' }}>Quick Actions</h2>
        <button onClick={() => setView(v => v === 'list' ? 'journey' : 'list')}
          style={{ width: '100%', padding: '14px 20px', borderRadius: 8, border: 'none', background: 'linear-gradient(90deg, #00338D 0%, #0091DA 100%)', color: '#fff', fontSize: 14, fontWeight: 700, cursor: 'pointer' }}>
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
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: '#00338D' }}>Legacy Products Inventory</h2>
          <div style={{ display: 'flex', gap: 8 }}>
            <select value={lobFilter} onChange={e => setLobFilter(e.target.value)} style={{ padding: '6px 10px', borderRadius: 6, border: '1px solid #d1d5db', fontSize: 12 }}>
              <option value="">All LoBs</option>
              {lobs.map(l => <option key={l} value={l}>{l}</option>)}
            </select>
            <select value={statusFilter} onChange={e => setStatusFilter(e.target.value)} style={{ padding: '6px 10px', borderRadius: 6, border: '1px solid #d1d5db', fontSize: 12 }}>
              <option value="">All Statuses</option>
              {statuses.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
            <button onClick={onClose} style={{ padding: '6px 14px', borderRadius: 6, border: 'none', background: '#00338D', color: '#fff', fontWeight: 600, cursor: 'pointer', fontSize: 12 }}>Close</button>
          </div>
        </div>
        {loading ? <div>Loading...</div> : (
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: 'linear-gradient(135deg, #00338D, #0091DA)', color: '#fff' }}>
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
