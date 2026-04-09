/**
 * Analyze a tool result and return rendering instructions.
 *
 * @param {string} toolName
 * @param {object} parsed - The parsed JSON result
 * @returns {{ tables: Array, charts: Array, summary: object|null, raw: boolean }}
 */
export function detectRenderingStrategy(toolName, parsed) {
  const strategy = { tables: [], charts: [], summary: null, raw: false }

  if (!parsed || typeof parsed !== 'object') {
    strategy.raw = true
    return strategy
  }

  // Check for error — use 'in' so falsy values like 0 are still caught
  if ('error' in parsed && parsed.error) {
    strategy.raw = true
    strategy.summary = { label: String(parsed.error), badges: [{ text: 'Error', color: 'danger' }] }
    return strategy
  }

  switch (toolName) {
    case 'load_rcm': {
      if (Array.isArray(parsed.preview) && parsed.preview.length > 0 && parsed.preview[0]) {
        strategy.tables.push({
          title: `RCM Preview (${parsed.rows ?? '?'} rows total)`,
          data: parsed.preview,
          columns: Object.keys(parsed.preview[0]),
        })
      }
      strategy.summary = {
        label: `${parsed.rows ?? '?'} rows loaded, ${parsed.columns?.length ?? 0} columns`,
        badges: [
          { text: `${parsed.required_columns_present?.length ?? 0} required cols`, color: 'success' },
          ...(parsed.missing_columns?.length > 0
            ? [{ text: `${parsed.missing_columns.length} missing`, color: 'warning' }]
            : []),
        ],
      }
      break
    }

    case 'modify_rcm': {
      const action = parsed.action || ''
      const badges = []
      if (parsed.rows_before != null && parsed.rows_after != null && parsed.rows_before !== parsed.rows_after) {
        const diff = parsed.rows_after - parsed.rows_before
        badges.push({ text: `${diff > 0 ? '+' : ''}${diff} rows`, color: diff > 0 ? 'success' : 'danger' })
      }
      if (parsed.columns_before != null && parsed.columns_after != null && parsed.columns_before !== parsed.columns_after) {
        const diff = parsed.columns_after - parsed.columns_before
        badges.push({ text: `${diff > 0 ? '+' : ''}${diff} cols`, color: diff > 0 ? 'success' : 'danger' })
      }
      if (parsed.rows_updated != null) {
        badges.push({ text: `${parsed.rows_updated} updated`, color: 'success' })
      }
      if (parsed.rows_deleted != null) {
        badges.push({ text: `${parsed.rows_deleted} deleted`, color: 'danger' })
      }
      strategy.summary = {
        label: `${action}: ${parsed.rows_after ?? '?'} rows, ${parsed.columns_after ?? '?'} cols`,
        badges,
        diff: {
          rowsBefore: parsed.rows_before,
          rowsAfter: parsed.rows_after,
          colsBefore: parsed.columns_before,
          colsAfter: parsed.columns_after,
          column: parsed.column || parsed.deleted_column || parsed.old_name || '',
          action,
        },
      }
      if (action === 'filter_view' && Array.isArray(parsed.data) && parsed.data.length > 0 && parsed.data[0]) {
        strategy.tables.push({
          title: `Filtered View (${parsed.matching_rows ?? parsed.data.length} of ${parsed.total_rows ?? '?'} rows)`,
          data: parsed.data,
          columns: Object.keys(parsed.data[0]),
        })
      }
      break
    }

    case 'save_excel': {
      const badges = []
      if (parsed.version_tag) {
        badges.push({ text: parsed.version_tag, color: 'info' })
      }
      strategy.summary = {
        label: `Saved: ${parsed.rows ?? '?'} rows × ${parsed.columns ?? '?'} cols`,
        badges,
        version: parsed.version,
        versionTag: parsed.version_tag,
      }
      break
    }

    case 'inspect_dataframe': {
      if (['head', 'tail', 'sample', 'query', 'full'].includes(parsed.mode) && Array.isArray(parsed.data) && parsed.data.length > 0) {
        strategy.tables.push({
          title: `DataFrame ${parsed.mode} (${parsed.data.length} of ${parsed.total_rows ?? '?'} rows)`,
          data: parsed.data,
          columns: parsed.columns || (parsed.data[0] ? Object.keys(parsed.data[0]) : []),
        })
      }
      if (parsed.mode === 'value_counts' && parsed.counts && typeof parsed.counts === 'object') {
        const entries = Object.entries(parsed.counts)
        if (entries.length > 0) {
          strategy.charts.push({
            type: 'bar',
            title: `Value Counts: ${parsed.column ?? ''}`,
            labels: entries.map(([k]) => k),
            datasets: [{ label: 'Count', data: entries.map(([, v]) => v) }],
          })
        }
      }
      if (parsed.mode === 'describe' && parsed.statistics && typeof parsed.statistics === 'object') {
        const stats = parsed.statistics
        const cols = Object.keys(stats)
        if (cols.length > 0 && stats[cols[0]] && typeof stats[cols[0]] === 'object') {
          const statKeys = Object.keys(stats[cols[0]])
          const rows = statKeys.map(sk => {
            const row = { statistic: sk }
            cols.forEach(c => { row[c] = stats[c]?.[sk] ?? '–' })
            return row
          })
          if (rows.length > 0) {
            strategy.tables.push({
              title: 'DataFrame Statistics',
              data: rows,
              columns: ['statistic', ...cols],
            })
          }
        }
      }
      if (parsed.mode === 'columns' && Array.isArray(parsed.columns) && parsed.columns.length > 0) {
        strategy.tables.push({
          title: `Column Info (${parsed.total_columns ?? parsed.columns.length} columns)`,
          data: parsed.columns,
          columns: ['name', 'dtype', 'non_null', 'unique'],
        })
      }
      strategy.summary = { label: `inspect (${parsed.mode ?? 'unknown'})` }
      break
    }

    case 'run_test_of_design': {
      strategy.summary = {
        label: `${parsed.controls_evaluated ?? 0} controls evaluated`,
        badges: [
          { text: `${parsed.passed ?? 0} PASS`, color: 'success' },
          { text: `${parsed.failed ?? 0} FAIL`, color: (parsed.failed ?? 0) > 0 ? 'danger' : 'success' },
        ],
      }
      if (parsed.passed != null && parsed.failed != null) {
        strategy.charts.push({
          type: 'doughnut',
          title: 'Test of Design Results',
          labels: ['PASS', 'FAIL'],
          datasets: [{ data: [parsed.passed, parsed.failed] }],
          colors: ['#38A169', '#E53E3E'],
        })
      }
      if (Array.isArray(parsed.results) && parsed.results.length > 0 && parsed.results[0]) {
        strategy.tables.push({
          title: 'Detailed Results',
          data: parsed.results,
          columns: inferColumns(parsed.results[0], ['control_id', 'risk_id', 'result', 'confidence', 'deficiency_type', 'gap_identified']),
        })
      }
      break
    }

    case 'run_test_of_effectiveness': {
      strategy.summary = {
        label: `${parsed.controls_evaluated ?? 0} controls evaluated`,
        badges: [
          { text: `${parsed.effective ?? 0} Effective`, color: 'success' },
          ...((parsed.effective_with_exceptions ?? 0) > 0 ? [{ text: `${parsed.effective_with_exceptions} Exceptions`, color: 'warning' }] : []),
          ...((parsed.not_effective ?? 0) > 0 ? [{ text: `${parsed.not_effective} Not Effective`, color: 'danger' }] : []),
        ],
      }
      strategy.charts.push({
        type: 'bar',
        title: 'Operating Effectiveness',
        labels: ['Effective', 'With Exceptions', 'Not Effective'],
        datasets: [{
          label: 'Controls',
          data: [parsed.effective || 0, parsed.effective_with_exceptions || 0, parsed.not_effective || 0],
        }],
        colors: ['#38A169', '#DD6B20', '#E53E3E'],
      })
      if (Array.isArray(parsed.summary) && parsed.summary.length > 0 && parsed.summary[0]) {
        strategy.tables.push({
          title: 'Per-Control Summary',
          data: parsed.summary,
          columns: inferColumns(parsed.summary[0], ['control_id', 'total_samples', 'passed_samples', 'failed_samples', 'deviation_rate', 'operating_effectiveness', 'deficiency_type']),
        })
      }
      break
    }

    case 'run_ai_suggestions': {
      strategy.summary = { label: `${parsed.suggestion_count ?? 0} suggestions generated` }
      if (Array.isArray(parsed.suggestions) && parsed.suggestions.length > 0 && parsed.suggestions[0]) {
        const priorityCounts = {}
        parsed.suggestions.forEach(s => {
          const p = s.AI_Priority || 'Unknown'
          priorityCounts[p] = (priorityCounts[p] || 0) + 1
        })
        if (Object.keys(priorityCounts).length > 1) {
          strategy.charts.push({
            type: 'doughnut',
            title: 'Suggestions by Priority',
            labels: Object.keys(priorityCounts),
            datasets: [{ data: Object.values(priorityCounts) }],
            colors: ['#E53E3E', '#DD6B20', '#ECC94B', '#38A169'],
          })
        }
        const categoryCounts = {}
        parsed.suggestions.forEach(s => {
          const c = s.AI_Category || 'Unknown'
          categoryCounts[c] = (categoryCounts[c] || 0) + 1
        })
        if (Object.keys(categoryCounts).length > 1) {
          strategy.charts.push({
            type: 'bar',
            title: 'Suggestions by Category',
            labels: Object.keys(categoryCounts),
            datasets: [{ label: 'Count', data: Object.values(categoryCounts) }],
          })
        }
        strategy.tables.push({
          title: `AI Suggestions (${parsed.suggestion_count ?? parsed.suggestions.length})`,
          data: parsed.suggestions,
          columns: inferColumns(parsed.suggestions[0], ['#', 'AI_Suggestion_ID', 'AI_Priority', 'AI_Category', 'Risk Title', 'Control Description', 'AI_Reason']),
        })
      }
      break
    }

    case 'run_deduplication':
    case 'check_duplicates': {
      const pairCount = parsed.pair_count ?? 0
      strategy.summary = {
        label: `${pairCount} duplicate pairs found`,
        badges: pairCount === 0
          ? [{ text: 'Clean', color: 'success' }]
          : [{ text: `${pairCount} pairs`, color: 'warning' }],
      }
      if (Array.isArray(parsed.pairs) && parsed.pairs.length > 0 && parsed.pairs[0]) {
        strategy.tables.push({
          title: `Duplicate Pairs (${pairCount})`,
          data: parsed.pairs,
          columns: inferColumns(parsed.pairs[0], ['#', 'process', 'row_a', 'row_a_risk', 'row_b', 'row_b_risk', 'confidence', 'reasoning', 'recommendation']),
        })
      }
      break
    }

    case 'run_sampling_engine': {
      strategy.summary = {
        label: `${parsed.controls_processed ?? 0} controls, ${parsed.total_samples_required ?? 0} total samples`,
      }
      if (Array.isArray(parsed.sampling_table_used) && parsed.sampling_table_used.length > 0 && parsed.sampling_table_used[0]) {
        strategy.tables.push({
          title: `Sampling Table (${parsed.engine_type || 'Standard'})`,
          data: parsed.sampling_table_used,
          columns: Object.keys(parsed.sampling_table_used[0]),
        })
      }
      if (Array.isArray(parsed.per_control_results) && parsed.per_control_results.length > 0 && parsed.per_control_results[0]) {
        strategy.tables.push({
          title: 'Per-Control Sample Requirements',
          data: parsed.per_control_results,
          columns: inferColumns(parsed.per_control_results[0], ['control_id', 'frequency', 'risk_level', 'matched_frequency', 'sample_count']),
        })
      }
      break
    }

    case 'run_control_assessment': {
      strategy.summary = { label: `${parsed.controls_assessed ?? 0} controls assessed` }
      if (Array.isArray(parsed.results) && parsed.results.length > 0 && parsed.results[0]) {
        strategy.charts.push({
          type: 'bar',
          title: 'Control-to-Policy Match %',
          labels: parsed.results.map(r => r.control_id || '?'),
          datasets: [{
            label: 'Match %',
            data: parsed.results.map(r => parseInt(r.match_pct) || 0),
          }],
          colors: parsed.results.map(r => {
            const pct = parseInt(r.match_pct) || 0
            if (pct >= 80) return '#38A169'
            if (pct >= 50) return '#DD6B20'
            return '#E53E3E'
          }),
        })
        strategy.tables.push({
          title: 'Assessment Results',
          data: parsed.results.map(r => ({
            ...r,
            gaps: Array.isArray(r.gaps) ? r.gaps.join('; ') : (r.gaps || ''),
          })),
          columns: inferColumns(parsed.results[0], ['control_id', 'policy_documented', 'match_pct', 'gaps']),
        })
      }
      break
    }

    case 'preview_toe_attributes': {
      const controlCount = parsed.controls_count ?? 0
      const totalAttrs = parsed.total_attributes ?? 0
      strategy.summary = {
        label: `${controlCount} controls, ${totalAttrs} attributes generated`,
        badges: [
          { text: 'Awaiting Approval', color: 'warning' },
        ],
      }
      // Show overview table of controls and attribute counts
      if (Array.isArray(parsed.schemas) && parsed.schemas.length > 0) {
        strategy.tables.push({
          title: 'TOE Attribute Preview',
          data: parsed.schemas.map(s => ({
            control_id: s.control_id,
            control_type: s.control_type || '',
            process: s.process || '',
            frequency: s.control_frequency || '',
            attributes: (s.attributes || []).length,
            worksteps: (s.worksteps || []).length,
            samples: s.sample_count || 0,
          })),
          columns: ['control_id', 'control_type', 'process', 'frequency', 'attributes', 'worksteps', 'samples'],
        })
      }
      strategy.requiresApproval = true
      break
    }

    default: {
      for (const [key, value] of Object.entries(parsed)) {
        if (key.startsWith('_')) continue
        if (Array.isArray(value) && value.length > 0 && value[0] && typeof value[0] === 'object') {
          strategy.tables.push({
            title: key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
            data: value,
            columns: Object.keys(value[0]),
          })
        }
      }
      const numericEntries = Object.entries(parsed).filter(
        ([k, v]) => typeof v === 'number' && !k.startsWith('_')
      )
      if (numericEntries.length > 0) {
        strategy.summary = {
          label: numericEntries.map(([k, v]) => `${k.replace(/_/g, ' ')}: ${v}`).join(', '),
        }
      }
      if (strategy.tables.length === 0 && !strategy.summary) {
        strategy.raw = true
      }
      break
    }
  }

  return strategy
}

/**
 * Pick columns that exist in the data object, falling back to all keys.
 */
function inferColumns(sampleRow, preferred) {
  if (!sampleRow || typeof sampleRow !== 'object') return preferred || []
  const available = Object.keys(sampleRow)
  if (!preferred || preferred.length === 0) return available
  const matched = preferred.filter(c => available.includes(c))
  return matched.length > 0 ? matched : available
}
