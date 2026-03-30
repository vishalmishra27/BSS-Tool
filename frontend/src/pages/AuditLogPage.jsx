import React, { useEffect, useState } from 'react';

const OP_BADGES = {
  single_row: { label: 'Single Row', color: '#0070c0' },
  bulk:        { label: 'Bulk',       color: '#e07b00' },
  delete:      { label: 'Delete',     color: '#c00000' },
};

export default function AuditLogPage() {
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState({});

  useEffect(() => {
    fetch('/api/agent/audit-log?limit=200')
      .then(r => r.json())
      .then(data => { setLogs(Array.isArray(data) ? data : []); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  const toggleRow = (id) => setExpanded(prev => ({ ...prev, [id]: !prev[id] }));

  return (
    <div style={{ padding: 28, fontFamily: 'system-ui, sans-serif', maxWidth: 1100, margin: '0 auto' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 24 }}>
        <div style={{ background: '#001F5B', color: '#00B0F0', fontWeight: 900, fontSize: 18, letterSpacing: 2, padding: '3px 10px', border: '2px solid #00B0F0' }}>KPMG</div>
        <div>
          <h1 style={{ margin: 0, fontSize: 20, color: '#001F5B', fontWeight: 700 }}>Agent Audit Log</h1>
          <p style={{ margin: 0, fontSize: 13, color: '#666' }}>Immutable record of all AI-assisted data changes — read-only</p>
        </div>
      </div>

      {/* Stats bar */}
      <div style={{ display: 'flex', gap: 14, marginBottom: 20, flexWrap: 'wrap' }}>
        {[
          { label: 'Total Operations', value: logs.length, color: '#001F5B' },
          { label: 'Bulk Operations', value: logs.filter(l => l.operation_type === 'bulk').length, color: '#e07b00' },
          { label: 'Single Row Edits', value: logs.filter(l => l.operation_type === 'single_row').length, color: '#0070c0' },
          { label: 'Rows Affected', value: logs.reduce((s, l) => s + (l.affected_rows || 0), 0), color: '#006600' },
        ].map(stat => (
          <div key={stat.label} style={{
            background: '#fff', border: '1px solid #e0e8f0', borderRadius: 8,
            padding: '12px 20px', minWidth: 140,
            borderTop: `3px solid ${stat.color}`,
          }}>
            <div style={{ fontSize: 22, fontWeight: 700, color: stat.color }}>{stat.value}</div>
            <div style={{ fontSize: 12, color: '#666', marginTop: 2 }}>{stat.label}</div>
          </div>
        ))}
      </div>

      {loading ? (
        <div style={{ padding: 40, textAlign: 'center', color: '#888' }}>Loading audit log…</div>
      ) : logs.length === 0 ? (
        <div style={{
          background: '#f5f7fa', borderRadius: 8, padding: 40,
          textAlign: 'center', color: '#888', border: '1px dashed #ccc',
        }}>
          No agent operations recorded yet. Use the AI chat widget to make your first change.
        </div>
      ) : (
        <div style={{ background: '#fff', border: '1px solid #e0e8f0', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: '#f0f4ff' }}>
                {['#', 'Time', 'User', 'Module', 'Type', 'Table', 'Rows', 'Instruction', ''].map(h => (
                  <th key={h} style={{ padding: '10px 12px', textAlign: 'left', fontWeight: 600, color: '#001F5B', borderBottom: '2px solid #d0d8f0', whiteSpace: 'nowrap' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {logs.map((log, i) => {
                const badge = OP_BADGES[log.operation_type] || OP_BADGES.single_row;
                const isExpanded = expanded[log.id];
                return (
                  <React.Fragment key={log.id}>
                    <tr style={{ background: i % 2 === 0 ? '#fff' : '#fafbff' }}>
                      <td style={td}>{log.id}</td>
                      <td style={{ ...td, whiteSpace: 'nowrap', color: '#555' }}>
                        {new Date(log.triggered_at).toLocaleString('en-GB', { dateStyle: 'short', timeStyle: 'short' })}
                      </td>
                      <td style={td}>{log.username}</td>
                      <td style={{ ...td, color: '#555', fontSize: 12 }}>{log.module_context || '—'}</td>
                      <td style={td}>
                        <span style={{ background: badge.color, color: '#fff', borderRadius: 4, padding: '2px 8px', fontSize: 11, fontWeight: 600 }}>
                          {badge.label}
                        </span>
                      </td>
                      <td style={{ ...td, fontFamily: 'monospace', fontSize: 12, color: '#444' }}>{log.affected_table}</td>
                      <td style={{ ...td, textAlign: 'center', fontWeight: 600, color: log.affected_rows > 10 ? '#e07b00' : '#222' }}>{log.affected_rows}</td>
                      <td style={{ ...td, maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={log.instruction}>
                        {log.instruction}
                      </td>
                      <td style={td}>
                        <button onClick={() => toggleRow(log.id)} style={{ background: 'none', border: '1px solid #ccc', borderRadius: 4, padding: '2px 8px', fontSize: 11, cursor: 'pointer', color: '#555' }}>
                          {isExpanded ? 'Less' : 'Details'}
                        </button>
                      </td>
                    </tr>
                    {isExpanded && (
                      <tr style={{ background: '#f8faff' }}>
                        <td colSpan={9} style={{ padding: '12px 18px', borderBottom: '1px solid #e0e8f0' }}>
                          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
                            {/* SQL */}
                            <div>
                              <div style={{ fontWeight: 600, fontSize: 12, color: '#444', marginBottom: 4 }}>SQL Executed</div>
                              <pre style={{ background: '#1e1e2e', color: '#cdd6f4', borderRadius: 6, padding: '8px 12px', fontSize: 11, margin: 0, whiteSpace: 'pre-wrap', overflowX: 'auto' }}>
                                {log.sql_executed}
                              </pre>
                            </div>
                            {/* Before / After */}
                            <div>
                              <div style={{ fontWeight: 600, fontSize: 12, color: '#444', marginBottom: 4 }}>Data Change</div>
                              <div style={{ display: 'flex', gap: 10 }}>
                                <div style={{ flex: 1 }}>
                                  <div style={{ fontSize: 11, color: '#c00000', fontWeight: 600, marginBottom: 3 }}>BEFORE</div>
                                  <pre style={{ background: '#fff5f5', border: '1px solid #fcc', borderRadius: 4, padding: '6px 10px', fontSize: 10, margin: 0, maxHeight: 120, overflow: 'auto', whiteSpace: 'pre-wrap' }}>
                                    {JSON.stringify(log.before_data, null, 2) || 'N/A'}
                                  </pre>
                                </div>
                                <div style={{ flex: 1 }}>
                                  <div style={{ fontSize: 11, color: '#006600', fontWeight: 600, marginBottom: 3 }}>AFTER</div>
                                  <pre style={{ background: '#f0fff4', border: '1px solid #c8f0c8', borderRadius: 4, padding: '6px 10px', fontSize: 10, margin: 0, maxHeight: 120, overflow: 'auto', whiteSpace: 'pre-wrap' }}>
                                    {JSON.stringify(log.after_data, null, 2) || 'N/A'}
                                  </pre>
                                </div>
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <div style={{ marginTop: 16, fontSize: 12, color: '#999', textAlign: 'center' }}>
        🔒 This log is immutable — entries cannot be edited or deleted by any user
      </div>
    </div>
  );
}

const td = { padding: '9px 12px', borderBottom: '1px solid #eef0f8', verticalAlign: 'middle' };
