import React, { useState, useCallback, useMemo } from 'react'

const LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

export default function AttributeApprovalModal({ data, onApprove, onCancel }) {
  // Deep-clone schemas into local editable state
  const [schemas, setSchemas] = useState(() =>
    (data.schemas || []).map(s => ({
      ...s,
      attributes: (s.attributes || []).map(a => ({ ...a })),
      worksteps: [...(s.worksteps || [])],
      sample_columns: (s.sample_columns || []).map(c => ({ ...c })),
    }))
  )
  const [expandedIdx, setExpandedIdx] = useState(0)
  const [editingCell, setEditingCell] = useState(null) // { schemaIdx, attrIdx, field }
  const [bulkEditIdx, setBulkEditIdx] = useState(null) // index of schema in bulk-edit mode

  const totalAttrs = useMemo(
    () => schemas.reduce((sum, s) => sum + (s.attributes?.length || 0), 0),
    [schemas]
  )

  const toggleExpand = useCallback((idx) => {
    setExpandedIdx(prev => prev === idx ? -1 : idx)
  }, [])

  // Update a single attribute field
  const updateAttr = useCallback((schemaIdx, attrIdx, field, value) => {
    setSchemas(prev => {
      const next = [...prev]
      next[schemaIdx] = {
        ...next[schemaIdx],
        attributes: next[schemaIdx].attributes.map((a, i) =>
          i === attrIdx ? { ...a, [field]: value } : a
        ),
      }
      return next
    })
  }, [])

  // Add a new attribute to a control
  const addAttribute = useCallback((schemaIdx) => {
    setSchemas(prev => {
      const next = [...prev]
      const existing = next[schemaIdx].attributes || []
      const nextId = LETTERS[existing.length] || `ATTR_${existing.length + 1}`
      next[schemaIdx] = {
        ...next[schemaIdx],
        attributes: [
          ...existing,
          { id: nextId, name: '', description: '' },
        ],
      }
      return next
    })
  }, [])

  // Remove an attribute
  const removeAttribute = useCallback((schemaIdx, attrIdx) => {
    setSchemas(prev => {
      const next = [...prev]
      const filtered = next[schemaIdx].attributes.filter((_, i) => i !== attrIdx)
      // Re-assign IDs (A, B, C, ...)
      const relabeled = filtered.map((a, i) => ({
        ...a,
        id: LETTERS[i] || `ATTR_${i + 1}`,
      }))
      next[schemaIdx] = { ...next[schemaIdx], attributes: relabeled }
      return next
    })
  }, [])

  // Bulk edit — update all descriptions at once via a textarea
  const handleBulkSave = useCallback((schemaIdx, text) => {
    const lines = text.split('\n').filter(l => l.trim())
    setSchemas(prev => {
      const next = [...prev]
      const attrs = [...next[schemaIdx].attributes]
      lines.forEach((line, i) => {
        if (i < attrs.length) {
          // Format: "A: description" or just "description"
          const colonIdx = line.indexOf(':')
          if (colonIdx > 0 && colonIdx <= 3) {
            attrs[i] = { ...attrs[i], description: line.slice(colonIdx + 1).trim() }
          } else {
            attrs[i] = { ...attrs[i], description: line.trim() }
          }
        }
      })
      next[schemaIdx] = { ...next[schemaIdx], attributes: attrs }
      return next
    })
    setBulkEditIdx(null)
  }, [])

  // Build the approval payload: { control_id: { worksteps, attributes, sample_columns } }
  const handleApprove = useCallback(() => {
    const payload = {}
    for (const s of schemas) {
      payload[s.control_id] = {
        worksteps: s.worksteps,
        attributes: s.attributes,
        sample_columns: s.sample_columns,
      }
    }
    onApprove(payload)
  }, [schemas, onApprove])

  return (
    <div className="approval-modal-overlay" onClick={onCancel}>
      <div className="approval-modal" onClick={e => e.stopPropagation()}>
        {/* Header */}
        <div className="approval-modal-header">
          <div className="approval-header-left">
            <h2 className="approval-title">TOE Attribute Review</h2>
            <span className="approval-subtitle">
              {schemas.length} controls &middot; {totalAttrs} attributes
            </span>
          </div>
          <div className="approval-header-right">
            <button className="approval-cancel-btn" onClick={onCancel}>Cancel</button>
            <button className="approval-approve-btn" onClick={handleApprove}>
              Approve &amp; Run TOE
            </button>
          </div>
        </div>

        {/* Control Sections */}
        <div className="approval-body">
          {schemas.map((schema, sIdx) => (
            <div key={schema.control_id} className="approval-control-section">
              {/* Control Header */}
              <div
                className="approval-control-header"
                onClick={() => toggleExpand(sIdx)}
              >
                <span className="approval-expand-icon">
                  {expandedIdx === sIdx ? '▾' : '▸'}
                </span>
                <span className="approval-control-id">{schema.control_id}</span>
                <span className="approval-control-desc">
                  {(schema.control_description || '').slice(0, 80)}
                  {(schema.control_description || '').length > 80 ? '...' : ''}
                </span>
                <span className="approval-attr-count">
                  {schema.attributes?.length || 0} attrs
                </span>
              </div>

              {/* Expanded Content */}
              {expandedIdx === sIdx && (
                <div className="approval-control-content">
                  {/* Source Badges */}
                  <div className="approval-source-badges">
                    {schema.control_type && (
                      <span className="approval-badge">{schema.control_type}</span>
                    )}
                    {schema.nature_of_control && (
                      <span className="approval-badge">{schema.nature_of_control}</span>
                    )}
                    {schema.control_frequency && (
                      <span className="approval-badge">{schema.control_frequency}</span>
                    )}
                    {schema.process && (
                      <span className="approval-badge">{schema.process}</span>
                    )}
                    {schema.sample_count > 0 && (
                      <span className="approval-badge approval-badge-count">
                        {schema.sample_count} samples
                      </span>
                    )}
                  </div>

                  {/* Full description */}
                  {schema.control_description && (
                    <div className="approval-full-desc">
                      {schema.control_description}
                    </div>
                  )}

                  {/* Attributes Table */}
                  <div className="approval-attr-section">
                    <div className="approval-attr-header-row">
                      <h4 className="approval-section-title">Attributes</h4>
                      <div className="approval-attr-actions">
                        <button
                          className="approval-text-btn"
                          onClick={() => setBulkEditIdx(bulkEditIdx === sIdx ? null : sIdx)}
                        >
                          {bulkEditIdx === sIdx ? 'Cancel Bulk Edit' : 'Edit All'}
                        </button>
                        <button
                          className="approval-text-btn approval-add-btn"
                          onClick={() => addAttribute(sIdx)}
                        >
                          + Add
                        </button>
                      </div>
                    </div>

                    {bulkEditIdx === sIdx ? (
                      <BulkEditor
                        attributes={schema.attributes}
                        onSave={(text) => handleBulkSave(sIdx, text)}
                        onCancel={() => setBulkEditIdx(null)}
                      />
                    ) : (
                      <table className="approval-attr-table">
                        <thead>
                          <tr>
                            <th className="attr-col-id">ID</th>
                            <th className="attr-col-name">Name</th>
                            <th className="attr-col-desc">Description</th>
                            <th className="attr-col-actions"></th>
                          </tr>
                        </thead>
                        <tbody>
                          {(schema.attributes || []).map((attr, aIdx) => (
                            <tr key={`${sIdx}-${aIdx}`}>
                              <td className="attr-col-id">{attr.id}</td>
                              <td className="attr-col-name">
                                <EditableCell
                                  value={attr.name || ''}
                                  isEditing={
                                    editingCell?.schemaIdx === sIdx &&
                                    editingCell?.attrIdx === aIdx &&
                                    editingCell?.field === 'name'
                                  }
                                  onStartEdit={() =>
                                    setEditingCell({ schemaIdx: sIdx, attrIdx: aIdx, field: 'name' })
                                  }
                                  onChange={(val) => updateAttr(sIdx, aIdx, 'name', val)}
                                  onDone={() => setEditingCell(null)}
                                />
                              </td>
                              <td className="attr-col-desc">
                                <EditableCell
                                  value={attr.description || ''}
                                  isEditing={
                                    editingCell?.schemaIdx === sIdx &&
                                    editingCell?.attrIdx === aIdx &&
                                    editingCell?.field === 'description'
                                  }
                                  onStartEdit={() =>
                                    setEditingCell({ schemaIdx: sIdx, attrIdx: aIdx, field: 'description' })
                                  }
                                  onChange={(val) => updateAttr(sIdx, aIdx, 'description', val)}
                                  onDone={() => setEditingCell(null)}
                                  multiline
                                />
                              </td>
                              <td className="attr-col-actions">
                                <button
                                  className="approval-remove-btn"
                                  onClick={() => removeAttribute(sIdx, aIdx)}
                                  title="Remove attribute"
                                >
                                  &times;
                                </button>
                              </td>
                            </tr>
                          ))}
                          {(!schema.attributes || schema.attributes.length === 0) && (
                            <tr>
                              <td colSpan={4} className="attr-empty">
                                No attributes. Click "+ Add" to create one.
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    )}
                  </div>

                  {/* Worksteps */}
                  {schema.worksteps && schema.worksteps.length > 0 && (
                    <details className="approval-worksteps">
                      <summary className="approval-section-title">
                        Worksteps ({schema.worksteps.length})
                      </summary>
                      <ol className="approval-worksteps-list">
                        {schema.worksteps.map((ws, i) => (
                          <li key={i}>{ws}</li>
                        ))}
                      </ol>
                    </details>
                  )}

                  {/* Sample Columns */}
                  {schema.sample_columns && schema.sample_columns.length > 0 && (
                    <div className="approval-sample-cols">
                      <span className="approval-section-title">Sample Columns: </span>
                      {schema.sample_columns.map((col, i) => (
                        <span key={i} className="approval-badge approval-badge-sm">
                          {col.header || col.key}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// Inline editable cell — click to edit, blur/Enter to save
function EditableCell({ value, isEditing, onStartEdit, onChange, onDone, multiline }) {
  if (isEditing) {
    if (multiline) {
      return (
        <textarea
          className="approval-attr-input approval-attr-textarea"
          defaultValue={value}
          autoFocus
          onBlur={(e) => { onChange(e.target.value); onDone() }}
          onKeyDown={(e) => {
            if (e.key === 'Escape') onDone()
          }}
          rows={3}
        />
      )
    }
    return (
      <input
        className="approval-attr-input"
        type="text"
        defaultValue={value}
        autoFocus
        onBlur={(e) => { onChange(e.target.value); onDone() }}
        onKeyDown={(e) => {
          if (e.key === 'Enter') { onChange(e.target.value); onDone() }
          if (e.key === 'Escape') onDone()
        }}
      />
    )
  }

  return (
    <span
      className="approval-attr-display"
      onClick={onStartEdit}
      title="Click to edit"
    >
      {value || <em className="approval-placeholder">Click to edit</em>}
    </span>
  )
}

// Bulk editor — textarea with all descriptions, one per line
function BulkEditor({ attributes, onSave, onCancel }) {
  const initial = (attributes || [])
    .map(a => `${a.id}: ${a.description || ''}`)
    .join('\n')
  const [text, setText] = useState(initial)

  return (
    <div className="approval-bulk-editor">
      <p className="approval-bulk-hint">
        Edit all attribute descriptions below. Format: <code>A: description text</code> (one per line)
      </p>
      <textarea
        className="approval-bulk-textarea"
        value={text}
        onChange={e => setText(e.target.value)}
        rows={Math.max(5, (attributes || []).length + 1)}
      />
      <div className="approval-bulk-actions">
        <button className="approval-text-btn" onClick={onCancel}>Cancel</button>
        <button className="approval-approve-btn approval-btn-sm" onClick={() => onSave(text)}>
          Apply Changes
        </button>
      </div>
    </div>
  )
}
