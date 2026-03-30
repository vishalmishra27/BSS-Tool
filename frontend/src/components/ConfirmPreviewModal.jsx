import React, { useState } from 'react';

const OPERATION_BADGES = {
  single_row: { label: 'Single Row Edit', color: '#0070c0' },
  bulk:        { label: 'Bulk Operation', color: '#e07b00' },
  delete:      { label: 'Soft Delete',    color: '#c00000' },
};

export default function ConfirmPreviewModal({ proposal, onApprove, onReject, loading }) {
  const [showSQL, setShowSQL] = useState(false);
  const [editedSQL, setEditedSQL] = useState(proposal?.sql || '');
  const [editMode, setEditMode] = useState(false);

  if (!proposal) return null;

  const badge = OPERATION_BADGES[proposal.operation_type] || OPERATION_BADGES.single_row;
  const isBulk = proposal.operation_type === 'bulk';
  const rowCount = proposal.affected_rows_actual ?? proposal.affected_rows_estimate ?? 1;
  const beforeRows = proposal.before_data || [];
  const fields = proposal.fields_changed || [];

  // Build a flat field-level before/after diff from the first before row
  const beforeRow = beforeRows[0] || {};
  const relevantFields = fields.length > 0 ? fields : Object.keys(beforeRow);

  const handleApprove = () => {
    onApprove({ ...proposal, sql: editedSQL });
  };

  return (
    <div style={{
      position: 'fixed', inset: 0, zIndex: 10000,
      background: 'rgba(0,0,0,0.55)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
    }}>
      <div style={{
        background: '#fff', borderRadius: 10, width: 680, maxWidth: '95vw',
        maxHeight: '90vh', overflowY: 'auto',
        boxShadow: '0 8px 40px rgba(0,31,91,0.25)',
        fontFamily: 'system-ui, sans-serif',
      }}>
        {/* Header */}
        <div style={{
          padding: '18px 24px 14px',
          borderBottom: '1px solid #e8e8e8',
          display: 'flex', alignItems: 'center', gap: 12,
        }}>
          <div style={{ background: '#001F5B', color: '#00B0F0', fontWeight: 900, fontSize: 15, letterSpacing: 2, padding: '2px 7px', border: '2px solid #00B0F0' }}>KPMG</div>
          <div style={{ flex: 1 }}>
            <div style={{ fontWeight: 700, fontSize: 15, color: '#001F5B' }}>Confirm Agent Change</div>
            <div style={{ fontSize: 12, color: '#666', marginTop: 2 }}>Review before approving — this will write to the database</div>
          </div>
          <button onClick={onReject} style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 20, color: '#999', lineHeight: 1 }}>×</button>
        </div>

        <div style={{ padding: '20px 24px' }}>
          {/* Instruction recap */}
          <div style={{ background: '#f5f7fa', borderRadius: 6, padding: '10px 14px', marginBottom: 16, fontSize: 13 }}>
            <span style={{ color: '#666', fontWeight: 600 }}>Your instruction: </span>
            <span style={{ color: '#001F5B' }}>{proposal.instruction}</span>
          </div>

          {/* Badges row */}
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: 16, alignItems: 'center' }}>
            <span style={{ background: badge.color, color: '#fff', borderRadius: 4, padding: '3px 10px', fontSize: 12, fontWeight: 600 }}>
              {badge.label}
            </span>
            <span style={{ background: '#f0f4ff', color: '#001F5B', borderRadius: 4, padding: '3px 10px', fontSize: 12 }}>
              Table: <b>{proposal.affected_table}</b>
            </span>
            <span style={{ background: '#f0f4ff', color: '#001F5B', borderRadius: 4, padding: '3px 10px', fontSize: 12 }}>
              Rows: <b>{rowCount}</b>
            </span>
          </div>

          {/* Bulk warning banner */}
          {isBulk && (
            <div style={{
              background: '#fff8e6', border: '1px solid #e07b00', borderRadius: 6,
              padding: '10px 14px', marginBottom: 16, fontSize: 13, color: '#7a4800',
              display: 'flex', gap: 8, alignItems: 'flex-start',
            }}>
              <span style={{ fontSize: 18, marginTop: -1 }}>⚠️</span>
              <div>
                <strong>Bulk operation</strong> — this will affect <strong>{rowCount} rows</strong>.
                Please review carefully before approving.
              </div>
            </div>
          )}

          {/* Human-readable summary */}
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontWeight: 600, fontSize: 13, color: '#444', marginBottom: 6 }}>What will change</div>
            <div style={{ fontSize: 13, color: '#222', lineHeight: 1.5 }}>{proposal.human_summary}</div>
          </div>

          {/* Before / After table */}
          {beforeRows.length > 0 && relevantFields.length > 0 && (
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontWeight: 600, fontSize: 13, color: '#444', marginBottom: 8 }}>
                Before / After Preview {beforeRows.length > 1 ? `(showing ${Math.min(3, beforeRows.length)} of ${beforeRows.length} rows)` : ''}
              </div>
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                  <thead>
                    <tr style={{ background: '#f0f4ff' }}>
                      <th style={thStyle}>Field</th>
                      <th style={thStyle}>Before</th>
                      <th style={thStyle}>After</th>
                    </tr>
                  </thead>
                  <tbody>
                    {relevantFields.map(field => (
                      <tr key={field}>
                        <td style={{ ...tdStyle, fontWeight: 600, color: '#444' }}>{field}</td>
                        <td style={{ ...tdStyle, color: '#c00000', background: '#fff5f5' }}>
                          {String(beforeRow[field] ?? '—')}
                        </td>
                        <td style={{ ...tdStyle, color: '#006600', background: '#f0fff0' }}>
                          {getAfterValue(field, proposal)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {beforeRows.length > 3 && (
                <div style={{ fontSize: 11, color: '#888', marginTop: 4 }}>
                  + {beforeRows.length - 3} more rows will be updated
                </div>
              )}
            </div>
          )}

          {/* SQL toggle */}
          <div style={{ marginBottom: 20 }}>
            <button
              onClick={() => setShowSQL(s => !s)}
              style={{ background: 'none', border: '1px solid #ccc', borderRadius: 4, padding: '4px 10px', fontSize: 12, cursor: 'pointer', color: '#555' }}
            >
              {showSQL ? '▲ Hide SQL' : '▼ View SQL'}
            </button>
            {showSQL && (
              <div style={{ marginTop: 8 }}>
                {editMode ? (
                  <textarea
                    value={editedSQL}
                    onChange={e => setEditedSQL(e.target.value)}
                    style={{
                      width: '100%', minHeight: 80, fontFamily: 'monospace', fontSize: 12,
                      border: '1px solid #0070c0', borderRadius: 4, padding: 8, boxSizing: 'border-box',
                      resize: 'vertical',
                    }}
                  />
                ) : (
                  <pre style={{
                    background: '#1e1e2e', color: '#cdd6f4', borderRadius: 6, padding: 12,
                    fontSize: 11, overflowX: 'auto', margin: 0, whiteSpace: 'pre-wrap',
                  }}>{editedSQL}</pre>
                )}
                <button
                  onClick={() => setEditMode(m => !m)}
                  style={{ marginTop: 6, background: 'none', border: '1px solid #0070c0', borderRadius: 4, padding: '3px 10px', fontSize: 11, cursor: 'pointer', color: '#0070c0' }}
                >
                  {editMode ? 'Lock SQL' : 'Edit SQL'}
                </button>
              </div>
            )}
          </div>

          {/* Action buttons */}
          <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end' }}>
            <button
              onClick={onReject}
              disabled={loading}
              style={{
                padding: '9px 22px', borderRadius: 5, border: '1px solid #ccc',
                background: '#fff', color: '#444', fontSize: 13, cursor: 'pointer', fontWeight: 500,
              }}
            >
              Reject
            </button>
            <button
              onClick={handleApprove}
              disabled={loading}
              style={{
                padding: '9px 22px', borderRadius: 5, border: 'none',
                background: loading ? '#aaa' : '#001F5B', color: '#fff',
                fontSize: 13, cursor: loading ? 'not-allowed' : 'pointer', fontWeight: 600,
              }}
            >
              {loading ? 'Executing…' : isBulk ? `Approve ${rowCount} rows` : 'Approve'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// Extract the "after" value for a field from the SQL string (best-effort parsing)
function getAfterValue(field, proposal) {
  // The agent_service returns human_summary and fields_changed
  // We don't have a structured after object, so show the SQL excerpt or 'Updated'
  const sql = proposal.sql || '';
  // Try to find SET field = 'value' pattern
  const match = sql.match(new RegExp(`\\b${field}\\s*=\\s*'([^']*)'`, 'i'));
  if (match) return match[1];
  const numMatch = sql.match(new RegExp(`\\b${field}\\s*=\\s*([\\d.]+)`, 'i'));
  if (numMatch) return numMatch[1];
  return '(updated)';
}

const thStyle = {
  padding: '7px 12px', textAlign: 'left', fontWeight: 600,
  color: '#001F5B', borderBottom: '2px solid #d0d8f0', fontSize: 12,
};

const tdStyle = {
  padding: '7px 12px', borderBottom: '1px solid #eee', verticalAlign: 'top',
};
