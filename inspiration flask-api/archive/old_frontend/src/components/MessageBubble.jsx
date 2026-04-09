import React, { useState, useMemo } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { oneLight } from 'react-syntax-highlighter/dist/esm/styles/prism'

const COLLAPSE_THRESHOLD = 2000

export default function MessageBubble({ role, content, timestamp }) {
  const [copied, setCopied] = useState(false)
  const [isExpanded, setIsExpanded] = useState(false)

  if (!content) return null

  const isLong = content.length > COLLAPSE_THRESHOLD
  const displayContent = isLong && !isExpanded
    ? content.substring(0, COLLAPSE_THRESHOLD) + '...'
    : content

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(content)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // Fallback: select text in a temp textarea
      try {
        const ta = document.createElement('textarea')
        ta.value = content
        ta.style.position = 'fixed'
        ta.style.opacity = '0'
        document.body.appendChild(ta)
        ta.select()
        document.execCommand('copy')
        document.body.removeChild(ta)
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
      } catch { /* give up silently */ }
    }
  }

  const timeStr = timestamp
    ? new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    : null

  const CodeBlock = useMemo(() => {
    return function CodeRenderer({ node, inline, className, children, ...props }) {
      const match = /language-(\w+)/.exec(className || '')
      if (!inline && match) {
        const codeString = String(children).replace(/\n$/, '')
        return (
          <div className="code-block-wrapper">
            <div className="code-block-header">
              <span className="code-lang">{match[1]}</span>
              <button className="code-copy-btn" onClick={() => {
                navigator.clipboard.writeText(codeString).catch(() => {})
              }}>Copy</button>
            </div>
            <SyntaxHighlighter
              style={oneLight}
              language={match[1]}
              PreTag="div"
              customStyle={{ margin: 0, borderRadius: '0 0 8px 8px', fontSize: '13px' }}
              {...props}
            >
              {codeString}
            </SyntaxHighlighter>
          </div>
        )
      }
      return <code className={className} {...props}>{children}</code>
    }
  }, [])

  const MarkdownTable = useMemo(() => {
    return function TableRenderer({ children }) {
      return (
        <div className="message-table-wrapper">
          <table className="message-table">{children}</table>
        </div>
      )
    }
  }, [])

  return (
    <div className={`message ${role}-message`}>
      <div className={`message-avatar ${role}`}>
        {role === 'user' ? 'U' : 'K'}
      </div>
      <div className="message-card">
        <div className="message-header">
          <span className="message-sender">{role === 'user' ? 'You' : 'KPMG Agent'}</span>
          {timeStr && <span className="message-time">{timeStr}</span>}
          <button className="message-copy-btn" onClick={handleCopy} title="Copy message">
            {copied ? '✓ Copied' : 'Copy'}
          </button>
        </div>
        <div className="message-body">
          {role === 'user' ? (
            <div style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{content}</div>
          ) : (
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                code: CodeBlock,
                table: MarkdownTable,
              }}
            >
              {displayContent}
            </ReactMarkdown>
          )}
          {isLong && (
            <button
              className="message-expand-btn"
              onClick={() => setIsExpanded(!isExpanded)}
            >
              {isExpanded ? 'Show less' : 'Show more'}
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
