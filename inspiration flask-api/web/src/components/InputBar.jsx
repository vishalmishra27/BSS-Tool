import React, { useState, useRef, useEffect } from 'react'

const ACCEPTED_EXTENSIONS = ['.xlsx', '.xls', '.csv', '.pdf', '.txt', '.json']
const MAX_FILE_SIZE_MB = 50

export default function InputBar({ onSend, onStop, isStreaming, onFileSelect, onFolderSelect }) {
  const [value, setValue] = useState('')
  const [uploadError, setUploadError] = useState(null)
  const textareaRef = useRef(null)
  const fileInputRef = useRef(null)
  const folderInputRef = useRef(null)

  useEffect(() => {
    const ta = textareaRef.current
    if (ta) {
      ta.style.height = 'auto'
      ta.style.height = Math.min(ta.scrollHeight, 200) + 'px'
    }
  }, [value])

  useEffect(() => {
    textareaRef.current?.focus()
  }, [])

  const handleSubmit = () => {
    const trimmed = value.trim()
    if (!trimmed || isStreaming) return
    try {
      onSend(trimmed)
      setValue('')
    } catch (err) {
      console.error('Send failed:', err)
    }
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit()
    }
  }

  const handleFileClick = () => {
    fileInputRef.current?.click()
  }

  const handleFolderClick = () => {
    folderInputRef.current?.click()
  }

  const handleFileChange = (e) => {
    setUploadError(null)
    const file = e.target.files?.[0]
    if (!file) return

    // Validate file size
    if (file.size > MAX_FILE_SIZE_MB * 1024 * 1024) {
      setUploadError(`File too large (max ${MAX_FILE_SIZE_MB}MB)`)
      e.target.value = ''
      return
    }

    onFileSelect?.(file)
    e.target.value = ''
  }

  const handleFolderChange = (e) => {
    setUploadError(null)
    const files = e.target.files
    if (!files || files.length === 0) return
    onFolderSelect?.(files)
    e.target.value = ''
  }

  return (
    <div className="input-bar">
      {uploadError && (
        <div className="upload-error">
          <span>{uploadError}</span>
          <button onClick={() => setUploadError(null)}>&times;</button>
        </div>
      )}
      <div className="input-wrapper">
        <button
          className="attach-btn"
          onClick={handleFileClick}
          title="Upload file"
          disabled={isStreaming}
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M21.44 11.05l-9.19 9.19a6 6 0 01-8.49-8.49l9.19-9.19a4 4 0 015.66 5.66l-9.2 9.19a2 2 0 01-2.83-2.83l8.49-8.48"/>
          </svg>
        </button>
        <button
          className="attach-btn"
          onClick={handleFolderClick}
          title="Upload evidence folder"
          disabled={isStreaming}
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/>
          </svg>
        </button>
        <textarea
          ref={textareaRef}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Message the Control Testing Agent..."
          rows={1}
        />
        {isStreaming ? (
          <button className="stop-btn" onClick={onStop} title="Stop generating">
            <svg width="14" height="14" viewBox="0 0 14 14">
              <rect width="14" height="14" rx="2" fill="currentColor"/>
            </svg>
          </button>
        ) : (
          <button
            className="send-btn"
            onClick={handleSubmit}
            disabled={!value.trim()}
            title="Send"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/>
            </svg>
          </button>
        )}
      </div>
      <input
        ref={fileInputRef}
        type="file"
        accept={ACCEPTED_EXTENSIONS.join(',')}
        onChange={handleFileChange}
        style={{ display: 'none' }}
      />
      <input
        ref={folderInputRef}
        type="file"
        onChange={handleFolderChange}
        style={{ display: 'none' }}
        {...{ webkitdirectory: '', directory: '' }}
      />
    </div>
  )
}
