import React, { useState, useMemo, useEffect } from 'react'
import { detectRenderingStrategy } from '../utils/toolResultDetector'
import { getDownloadUrl } from '../api'
import DataTable from './DataTable'
import AuditChart from './AuditChart'

// Tools that produce visual output should auto-expand when completed
const AUTO_EXPAND_TOOLS = new Set([
  'load_rcm', 'inspect_dataframe', 'modify_rcm', 'run_test_of_design',
  'run_test_of_effectiveness', 'run_ai_suggestions', 'run_deduplication',
  'check_duplicates', 'run_sampling_engine', 'run_control_assessment',
  'preview_toe_attributes',
])

export default function ToolCallCard({ name, args, status, success, result, notes, onQuickAction, onRequestApproval }) {
  const [showRaw, setShowRaw] = useState(false)

  const { parsed, strategy } = useMemo(() => {
    let p = null
    try {
      p = typeof result === 'string' ? JSON.parse(result) : result
    } catch {
      p = null
    }
    const s = p
      ? detectRenderingStrategy(name, p)
      : { raw: true, tables: [], charts: [], summary: null }
    return { parsed: p, strategy: s }
  }, [result, name])
  const downloadableFiles = useMemo(() => extractDownloadableFiles(parsed), [parsed])
  const quickActions = useMemo(
    () => buildQuickActions(name, parsed, status, success !== false),
    [name, parsed, status, success]
  )

  const hasVisualContent = strategy.tables.length > 0 || strategy.charts.length > 0
  const shouldAutoExpand = hasVisualContent && AUTO_EXPAND_TOOLS.has(name) && status !== 'running'

  const [expanded, setExpanded] = useState(false)

  // Auto-expand when tool finishes and has tables/charts — useEffect, not useMemo
  useEffect(() => {
    if (shouldAutoExpand) setExpanded(true)
  }, [shouldAutoExpand])

  const statusClass = status === 'running' ? 'running'
    : (success !== false ? 'success' : 'failed')

  const statusIcon = status === 'running' ? '⟳'
    : (success !== false ? '✓' : '✕')

  const statusText = status === 'running' ? 'Running...'
    : (success !== false ? 'Completed' : 'Failed')

  const toolDisplayName = name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, l => l.toUpperCase())

  return (
    <div className={`tool-card ${statusClass}`}>
      <div className="tool-header" onClick={() => setExpanded(e => !e)}>
        <span className="tool-icon">{statusIcon}</span>
        <span className="tool-name">{toolDisplayName}</span>

        {strategy.summary && (
          <div className="tool-summary-badges">
            <span className="tool-summary-label">{strategy.summary.label}</span>
            {strategy.summary.badges?.map((b, i) => (
              <span key={i} className={`badge badge-${b.color}`}>{b.text}</span>
            ))}
          </div>
        )}

        <span className="tool-status">{statusText}</span>
        <span className="tool-expand-icon">{expanded ? '▾' : '▸'}</span>
      </div>

      {expanded && (
        <div className="tool-content">
          {/* Diff summary for modify_rcm */}
          {strategy.summary?.diff && (
            <div className="diff-summary">
              <div className="diff-summary-title">Change Summary</div>
              <div className="diff-summary-row">
                <span className="diff-label">Rows:</span>
                <span className="diff-before">{strategy.summary.diff.rowsBefore ?? '–'}</span>
                <span className="diff-arrow">→</span>
                <span className={`diff-after ${
                  strategy.summary.diff.rowsAfter != null && strategy.summary.diff.rowsBefore != null &&
                  strategy.summary.diff.rowsAfter !== strategy.summary.diff.rowsBefore
                    ? (strategy.summary.diff.rowsAfter > strategy.summary.diff.rowsBefore ? 'diff-added' : 'diff-removed')
                    : ''
                }`}>
                  {strategy.summary.diff.rowsAfter ?? '–'}
                </span>
              </div>
              <div className="diff-summary-row">
                <span className="diff-label">Columns:</span>
                <span className="diff-before">{strategy.summary.diff.colsBefore ?? '–'}</span>
                <span className="diff-arrow">→</span>
                <span className={`diff-after ${
                  strategy.summary.diff.colsAfter != null && strategy.summary.diff.colsBefore != null &&
                  strategy.summary.diff.colsAfter !== strategy.summary.diff.colsBefore
                    ? (strategy.summary.diff.colsAfter > strategy.summary.diff.colsBefore ? 'diff-added' : 'diff-removed')
                    : ''
                }`}>
                  {strategy.summary.diff.colsAfter ?? '–'}
                </span>
              </div>
              {strategy.summary.diff.column && (
                <div className="diff-summary-row">
                  <span className="diff-label">Affected:</span>
                  <span className="diff-column">{strategy.summary.diff.column}</span>
                </div>
              )}
            </div>
          )}

          {/* Version badge for save_excel */}
          {strategy.summary?.versionTag && (
            <div className="version-badge-container">
              <span className="version-badge">{strategy.summary.versionTag}</span>
              {parsed?.path && <span className="version-path">{parsed.path}</span>}
            </div>
          )}

          {downloadableFiles.length > 0 && (
            <div className="tool-downloads">
              {downloadableFiles.map((f) => (
                <a
                  key={f.path}
                  href={getDownloadUrl(f.path)}
                  className="tool-download-btn"
                  download={f.name}
                  onClick={(e) => e.stopPropagation()}
                  title={f.path}
                >
                  Download {f.name}
                </a>
              ))}
            </div>
          )}

          {/* TOE Attribute Approval Button */}
          {strategy.requiresApproval && name === 'preview_toe_attributes' && onRequestApproval && (
            <div className="tool-approval-action">
              <button
                className="approval-review-btn"
                type="button"
                onClick={(e) => {
                  e.stopPropagation()
                  onRequestApproval(parsed)
                }}
              >
                Review &amp; Edit Attributes
              </button>
            </div>
          )}

          {quickActions.length > 0 && (
            <div className="tool-next-actions">
              <div className="tool-next-actions-title">Suggested next steps</div>
              <div className="tool-next-actions-list">
                {quickActions.map((action) => (
                  <button
                    key={`${name}-${action.label}`}
                    className="tool-next-action-btn"
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation()
                      onQuickAction?.(action.prompt)
                    }}
                  >
                    {action.label}
                  </button>
                ))}
              </div>
            </div>
          )}

          {strategy.charts.map((chart, i) => (
            <AuditChart key={i} config={chart} />
          ))}

          {strategy.tables.map((table, i) => (
            <DataTable
              key={i}
              title={table.title}
              data={table.data}
              columns={table.columns}
              toolName={name}
            />
          ))}

          {(strategy.raw && parsed) && (
            <pre className="tool-raw-json">
              {JSON.stringify(parsed, null, 2)}
            </pre>
          )}

          {(!strategy.raw && parsed) && (
            <div className="tool-raw-toggle">
              <button
                onClick={(e) => { e.stopPropagation(); setShowRaw(s => !s) }}
                className="raw-toggle-btn"
              >
                {showRaw ? 'Hide' : 'Show'} Raw JSON
              </button>
              {showRaw && (
                <pre className="tool-raw-json">
                  {JSON.stringify(parsed, null, 2)}
                </pre>
              )}
            </div>
          )}

          {args && Object.keys(args).length > 0 && (
            <details className="tool-args-details">
              <summary>Arguments</summary>
              <pre className="tool-args-json">{JSON.stringify(args, null, 2)}</pre>
            </details>
          )}
        </div>
      )}

      {notes && Array.isArray(notes) && notes.length > 0 && (
        <div className="tool-notes">
          {notes.map((note, i) => (
            <div key={i} className="tool-note-item">&#9888; {note}</div>
          ))}
        </div>
      )}
    </div>
  )
}

function extractDownloadableFiles(parsed) {
  if (!parsed || typeof parsed !== 'object') return []

  const files = []
  const seen = new Set()
  const candidateKeys = new Set([
    'path',
    'file_path',
    'output_excel',
    'output_json',
    'output_text',
    'output_csv',
    'output_pdf',
    'output_file',
    'report_path',
  ])

  const looksLikeFilePath = (value) => {
    if (typeof value !== 'string') return false
    if (!value.includes('/') && !value.includes('\\')) return false
    return /\.[A-Za-z0-9]{1,8}$/.test(value)
  }

  const addFile = (pathValue) => {
    const normalized = String(pathValue || '').trim()
    if (!normalized || seen.has(normalized)) return
    seen.add(normalized)
    const name = normalized.split(/[\\/]/).pop() || 'download'
    files.push({ path: normalized, name })
  }

  const walk = (node, depth = 0) => {
    if (node == null || depth > 3) return
    if (Array.isArray(node)) {
      node.forEach((item) => walk(item, depth + 1))
      return
    }
    if (typeof node !== 'object') return

    Object.entries(node).forEach(([key, value]) => {
      if (typeof value === 'string') {
        if (candidateKeys.has(key) || looksLikeFilePath(value)) {
          addFile(value)
        }
      } else if (Array.isArray(value)) {
        value.forEach((v) => {
          if (typeof v === 'string' && looksLikeFilePath(v)) addFile(v)
          else if (typeof v === 'object') walk(v, depth + 1)
        })
      } else if (value && typeof value === 'object') {
        walk(value, depth + 1)
      }
    })
  }

  walk(parsed)
  return files
}

function buildQuickActions(toolName, parsed, status, isSuccess) {
  if (status === 'running' || !isSuccess) return []

  const suggestions = {
    load_rcm: [
      {
        label: 'Inspect Key Columns',
        prompt: 'Inspect the loaded RCM and show key columns, row count, and any data-quality issues.',
      },
      {
        label: 'Run Gap Analysis',
        prompt: 'Run AI gap analysis for this RCM and prioritize high-risk suggestions first.',
      },
      {
        label: 'Run Deduplication',
        prompt: 'Run deduplication on the current RCM and show duplicate pairs with recommendations.',
      },
    ],
    inspect_dataframe: [
      {
        label: 'Show Data Quality Issues',
        prompt: 'Identify nulls, duplicates, and inconsistent values in important RCM columns.',
      },
      {
        label: 'Run Gap Analysis',
        prompt: 'Run AI gap analysis now and summarize top suggestions.',
      },
    ],
    run_ai_suggestions: [
      {
        label: 'Merge Top Suggestions',
        prompt: 'Merge the top 5 high-priority AI suggestions into the RCM.',
      },
      {
        label: 'Save Checkpoint',
        prompt: 'Save the current RCM as a checkpoint file.',
      },
    ],
    merge_suggestions: [
      {
        label: 'Run Deduplication',
        prompt: 'Run deduplication after merging suggestions and list duplicate candidates.',
      },
      {
        label: 'Save Updated RCM',
        prompt: 'Save the current merged RCM as an Excel checkpoint.',
      },
    ],
    run_deduplication: [
      {
        label: 'Apply Recommended Removals',
        prompt: 'Apply recommended duplicate removals and show updated row count.',
      },
      {
        label: 'Save Deduped RCM',
        prompt: 'Save the deduplicated RCM to a checkpoint file.',
      },
    ],
    run_control_assessment: [
      {
        label: 'Show Weak Controls',
        prompt: 'List controls with weak policy/SOP alignment and explain the biggest gaps.',
      },
      {
        label: 'Save Assessment Output',
        prompt: 'Save the current output and provide download links for the assessment files.',
      },
    ],
    run_test_of_design: [
      {
        label: 'Run Sampling',
        prompt: 'Run the sampling engine to calculate sample sizes for the Test of Effectiveness.',
      },
      {
        label: 'Focus Failed Controls',
        prompt: 'List controls that failed TOD and suggest remediation priorities.',
      },
    ],
    run_test_of_effectiveness: [
      {
        label: 'Compare TOD vs TOE',
        prompt: 'Compare TOD and TOE side by side and identify controls with mismatched outcomes.',
      },
      {
        label: 'Export Final Files',
        prompt: 'Save a final checkpoint and list all generated result files for download.',
      },
    ],
    run_sampling_engine: [
      {
        label: 'Preview TOE Attributes',
        prompt: 'Generate and preview TOE testing attributes for review before running the full Test of Effectiveness. Please ask me for the TOE evidence folder path.',
      },
      {
        label: 'Save Sampling Results',
        prompt: 'Save current RCM and sampling outputs to checkpoint files.',
      },
    ],
    save_excel: [
      {
        label: 'Continue With Next Test',
        prompt: 'Continue the audit workflow from this checkpoint and suggest the next best step.',
      },
      {
        label: 'List Generated Files',
        prompt: 'Show all files generated in the output directory with brief descriptions.',
      },
    ],
    preview_toe_attributes: [
      {
        label: 'Review Attributes',
        prompt: 'Open the attribute review popup so I can review and edit the TOE testing attributes.',
      },
    ],
  }

  let actions = suggestions[toolName] || []

  // If dedup found no pairs, suggest moving forward to testing instead.
  if (toolName === 'run_deduplication' && Number(parsed?.pair_count || 0) === 0) {
    actions = [
      {
        label: 'Proceed to TOD',
        prompt: 'No duplicates found. Proceed with Test of Design and summarize results.',
      },
      {
        label: 'Save Clean RCM',
        prompt: 'Save the clean RCM as a checkpoint file.',
      },
    ]
  }

  return actions.slice(0, 3)
}
