import React, { useState, useMemo, useCallback, useRef, useEffect } from 'react'

const PAGE_SIZES = [25, 50, 100, 250]

export default function DataTable({ title, data, columns, toolName }) {
  const [sortCol, setSortCol] = useState(null)
  const [sortDir, setSortDir] = useState('asc')
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(25)
  const [searchInput, setSearchInput] = useState('')
  const [search, setSearch] = useState('')
  const [expandedCell, setExpandedCell] = useState(null)
  const [isFullWidth, setIsFullWidth] = useState(false)
  const searchTimerRef = useRef(null)

  const displayColumns = useMemo(() => {
    if (columns && columns.length > 0) return columns
    if (data && data.length > 0) return Object.keys(data[0])
    return []
  }, [columns, data])

  // Debounced search — 250ms delay so typing doesn't lag on large datasets
  const handleSearchChange = useCallback((val) => {
    setSearchInput(val)
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current)
    searchTimerRef.current = setTimeout(() => setSearch(val), 250)
  }, [])

  useEffect(() => {
    return () => { if (searchTimerRef.current) clearTimeout(searchTimerRef.current) }
  }, [])

  // Filter data by search query
  const filteredData = useMemo(() => {
    if (!data) return []
    if (!search.trim()) return data
    const q = search.toLowerCase()
    return data.filter(row =>
      displayColumns.some(col => {
        const val = row[col]
        if (val == null) return false
        return String(val).toLowerCase().includes(q)
      })
    )
  }, [data, search, displayColumns])

  const sortedData = useMemo(() => {
    if (!sortCol) return filteredData
    return [...filteredData].sort((a, b) => {
      const aVal = a[sortCol]
      const bVal = b[sortCol]
      if (aVal == null && bVal == null) return 0
      if (aVal == null) return 1
      if (bVal == null) return -1
      const aNum = Number(aVal)
      const bNum = Number(bVal)
      if (!isNaN(aNum) && !isNaN(bNum)) {
        return sortDir === 'asc' ? aNum - bNum : bNum - aNum
      }
      return sortDir === 'asc'
        ? String(aVal).localeCompare(String(bVal))
        : String(bVal).localeCompare(String(aVal))
    })
  }, [filteredData, sortCol, sortDir])

  const totalPages = Math.max(1, Math.ceil((sortedData?.length || 0) / pageSize))

  const pageData = useMemo(() => {
    const start = page * pageSize
    return sortedData.slice(start, start + pageSize)
  }, [sortedData, page, pageSize])

  // Reset page when search or pageSize changes
  useEffect(() => { setPage(0) }, [search, pageSize])

  const handleSort = useCallback((col) => {
    setSortCol(prev => {
      if (prev === col) {
        setSortDir(d => d === 'asc' ? 'desc' : 'asc')
        return col
      }
      setSortDir('asc')
      return col
    })
    setPage(0)
  }, [])

  const toggleCellExpand = useCallback((rowIdx, col) => {
    setExpandedCell(prev =>
      prev && prev.row === rowIdx && prev.col === col ? null : { row: rowIdx, col }
    )
  }, [])

  const exportCSV = useCallback(() => {
    if (!sortedData || sortedData.length === 0) return
    const header = displayColumns.join(',')
    const rows = sortedData.map(row =>
      displayColumns.map(col => {
        const val = String(row[col] ?? '').replace(/"/g, '""')
        return `"${val}"`
      }).join(',')
    )
    const csv = [header, ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${toolName || 'export'}_${Date.now()}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }, [sortedData, displayColumns, toolName])

  const exportExcel = useCallback(async () => {
    try {
      const XLSX = await import('xlsx')
      const ws = XLSX.utils.json_to_sheet(sortedData, { header: displayColumns })
      const wb = XLSX.utils.book_new()
      XLSX.utils.book_append_sheet(wb, ws, 'Results')
      XLSX.writeFile(wb, `${toolName || 'export'}_${Date.now()}.xlsx`)
    } catch (err) {
      console.error('Excel export failed:', err)
    }
  }, [sortedData, displayColumns, toolName])

  if (!data || data.length === 0) return null

  const startRow = page * pageSize

  return (
    <div className={`data-table-container ${isFullWidth ? 'data-table-fullwidth' : ''}`}>
      {/* Toolbar */}
      <div className="data-table-toolbar">
        <span className="data-table-title">{title}</span>
        <span className="data-table-count">
          {search ? `${filteredData.length} of ${data.length}` : data.length} rows
        </span>
        <div className="data-table-search-wrapper">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
          </svg>
          <input
            type="text"
            className="data-table-search"
            placeholder="Search table..."
            value={searchInput}
            onChange={e => handleSearchChange(e.target.value)}
          />
          {searchInput && (
            <button className="data-table-search-clear" onClick={() => { setSearchInput(''); setSearch('') }}>&times;</button>
          )}
        </div>
        <div className="data-table-actions">
          <button
            onClick={() => setIsFullWidth(f => !f)}
            className="export-btn"
            title={isFullWidth ? 'Collapse table' : 'Expand to full width'}
          >
            {isFullWidth ? 'Collapse' : 'Expand'}
          </button>
          <button onClick={exportCSV} className="export-btn" title="Export CSV">CSV</button>
          <button onClick={exportExcel} className="export-btn" title="Export Excel">Excel</button>
        </div>
      </div>

      {/* Table Scroll Area */}
      <div className="data-table-scroll">
        <table className="data-table">
          <thead>
            <tr>
              <th className="row-num-header">#</th>
              {displayColumns.map(col => (
                <th key={col} onClick={() => handleSort(col)}>
                  <span className="th-label">
                    {col.replace(/_/g, ' ')}
                    {sortCol === col && (
                      <span className="sort-indicator">{sortDir === 'asc' ? ' ▲' : ' ▼'}</span>
                    )}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageData.length === 0 ? (
              <tr>
                <td colSpan={displayColumns.length + 1} className="data-table-empty">
                  {search ? 'No rows match your search' : 'No data'}
                </td>
              </tr>
            ) : (
              pageData.map((row, idx) => {
                const globalIdx = startRow + idx
                return (
                  <tr key={globalIdx} className={idx % 2 === 0 ? 'even-row' : 'odd-row'}>
                    <td className="row-num-cell">{globalIdx + 1}</td>
                    {displayColumns.map(col => {
                      const rawVal = row[col]
                      const strVal = formatCellValue(rawVal)
                      const isLong = strVal.length > 50
                      const isExpanded = expandedCell?.row === globalIdx && expandedCell?.col === col
                      return (
                        <td
                          key={col}
                          className={`${isExpanded ? 'cell-expanded' : ''} ${isLong ? 'cell-clickable' : ''}`}
                          onClick={isLong ? () => toggleCellExpand(globalIdx, col) : undefined}
                          title={isLong && !isExpanded ? strVal : undefined}
                        >
                          {isExpanded ? (
                            <div className="cell-expanded-content">{strVal}</div>
                          ) : (
                            strVal
                          )}
                        </td>
                      )
                    })}
                  </tr>
                )
              })
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {sortedData.length > 0 && (
        <div className="data-table-pagination">
          <div className="pagination-left">
            <span className="pagination-info">
              {startRow + 1}–{Math.min(startRow + pageSize, sortedData.length)} of {sortedData.length}
            </span>
          </div>
          <div className="pagination-center">
            <button disabled={page === 0} onClick={() => setPage(0)} title="First page">&laquo;</button>
            <button disabled={page === 0} onClick={() => setPage(p => p - 1)}>Prev</button>
            <span className="pagination-page">{page + 1} / {totalPages}</span>
            <button disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)}>Next</button>
            <button disabled={page >= totalPages - 1} onClick={() => setPage(totalPages - 1)} title="Last page">&raquo;</button>
          </div>
          <div className="pagination-right">
            <select
              className="page-size-select"
              value={pageSize}
              onChange={e => setPageSize(Number(e.target.value))}
            >
              {PAGE_SIZES.map(s => (
                <option key={s} value={s}>{s} / page</option>
              ))}
            </select>
          </div>
        </div>
      )}
    </div>
  )
}

function formatCellValue(value) {
  if (value === null || value === undefined) return '–'
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (Array.isArray(value)) return value.join(', ')
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
