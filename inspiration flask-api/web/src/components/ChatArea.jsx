import React, { useState, useEffect, useRef, useCallback } from 'react'
import MessageBubble from './MessageBubble'
import ToolCallCard from './ToolCallCard'
import NextStepsPanel from './NextStepsPanel'
import InputBar from './InputBar'
import AttributeApprovalModal from './AttributeApprovalModal'
import { fetchMessages, fetchChats, fetchStarterActions, streamMessage, uploadFile, uploadFolder, submitToeApproval } from '../api'

// ---------------------------------------------------------------------------
// Parse the "What you can do next" section from an agent response into
// structured { label, description, prompt } objects for clickable chips.
// ---------------------------------------------------------------------------
function parseNextSteps(content) {
  if (!content) return { cleanContent: content, steps: [] }

  const markers = [
    '**What you can do next:**',
    '**What you can do next**:',
    '**What You Can Do Next:**',
    '**What You Can Do Next**:',
    '**Next steps:**',
    '**Next Steps:**',
  ]

  let markerIdx = -1
  let usedMarker = ''
  for (const marker of markers) {
    const idx = content.lastIndexOf(marker)
    if (idx !== -1 && idx > markerIdx) {
      markerIdx = idx
      usedMarker = marker
    }
  }

  if (markerIdx === -1) return { cleanContent: content, steps: [] }

  const before = content.substring(0, markerIdx).trimEnd()
  const afterSection = content.substring(markerIdx + usedMarker.length)

  const steps = []
  const regex = /(?:[-•]|\d+\.)\s*\*\*(.+?)\*\*\s*[—–\-:]\s*(.+)/g
  let match
  while ((match = regex.exec(afterSection)) !== null) {
    steps.push({
      label: match[1].trim(),
      description: match[2].trim(),
      prompt: match[2].trim(),
    })
  }

  return { cleanContent: before, steps }
}

const WELCOME_MESSAGE = `Welcome to ControlIris — your control testing agent.

I can help you move quickly across your audit workflow. Try one of these:

1. Upload an RCM file and ask for a summary (rows, columns, missing fields).
2. Run AI gap analysis for your industry and review priority suggestions.
3. Run deduplication and get recommended removals.
4. Run Test of Design (TOD) and Test of Effectiveness (TOE) from evidence.
5. Export final outputs and checkpoint files for review.

Start by uploading a file or telling me your audit objective.`

const DEFAULT_WELCOME_ACTIONS = [
  {
    label: 'Summarize my RCM',
    prompt: 'I uploaded an RCM file. Summarize rows, columns, key processes, and any missing required fields.',
  },
  {
    label: 'Run Gap Analysis',
    prompt: 'Run AI gap analysis for my RCM and show high-priority suggestions first.',
  },
  {
    label: 'Run Dedup Check',
    prompt: 'Run deduplication on the RCM and show duplicate pairs with recommended removals.',
  },
  {
    label: 'Run TOD + TOE',
    prompt: 'Run Test of Design and Test of Effectiveness and summarize controls that failed either test.',
  },
]

export default function ChatArea({ chatId, onTitleUpdate, onActivity }) {
  const [messages, setMessages] = useState([])
  const [streamingText, setStreamingText] = useState('')
  const [toolCalls, setToolCalls] = useState([])
  const [isStreaming, setIsStreaming] = useState(false)
  const [loading, setLoading] = useState(true)
  const [isDragOver, setIsDragOver] = useState(false)
  const [showScrollBtn, setShowScrollBtn] = useState(false)
  const [welcomeActions, setWelcomeActions] = useState(DEFAULT_WELCOME_ACTIONS)
  const [nextSteps, setNextSteps] = useState([])
  const [showApprovalModal, setShowApprovalModal] = useState(false)
  const [approvalData, setApprovalData] = useState(null)
  const abortRef = useRef(null)
  const messagesEndRef = useRef(null)
  const messagesContainerRef = useRef(null)
  const isUserScrolledUp = useRef(false)

  useEffect(() => {
    setWelcomeActions(DEFAULT_WELCOME_ACTIONS)
    loadMessages()
    return () => {
      if (abortRef.current) abortRef.current.abort()
    }
  }, [chatId])

  // Track user scroll position
  const handleScroll = useCallback(() => {
    const container = messagesContainerRef.current
    if (!container) return
    const atBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 100
    isUserScrolledUp.current = !atBottom
    // Use state for the button so it re-renders
    setShowScrollBtn(!atBottom && isStreaming)
  }, [isStreaming])

  useEffect(() => {
    const container = messagesContainerRef.current
    if (!container) return
    container.addEventListener('scroll', handleScroll, { passive: true })
    return () => container.removeEventListener('scroll', handleScroll)
  }, [handleScroll, loading])

  // Auto-scroll only when user hasn't scrolled up
  useEffect(() => {
    if (!isUserScrolledUp.current) {
      messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
  }, [messages, streamingText, toolCalls])

  // Hide scroll button when streaming stops
  useEffect(() => {
    if (!isStreaming) {
      setShowScrollBtn(false)
      // Scroll to bottom on stream end if user didn't scroll very far
      const container = messagesContainerRef.current
      if (container) {
        const dist = container.scrollHeight - container.scrollTop - container.clientHeight
        if (dist < 400) {
          messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
        }
      }
    }
  }, [isStreaming])

  const scrollToBottom = useCallback(() => {
    isUserScrolledUp.current = false
    setShowScrollBtn(false)
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [])

  const loadMessages = async () => {
    setLoading(true)
    setNextSteps([])
    try {
      const data = await fetchMessages(chatId)
      const parsed = parseMessages(data.messages || [])
      if (parsed.length > 0) {
        // Extract next steps from the last assistant message
        for (let i = parsed.length - 1; i >= 0; i--) {
          if (parsed[i].type === 'assistant') {
            const { cleanContent, steps } = parseNextSteps(parsed[i].content)
            if (steps.length > 0) {
              parsed[i] = { ...parsed[i], content: cleanContent }
              setNextSteps(steps)
            }
            break
          }
        }
        setMessages(parsed)
      } else {
        setMessages([buildWelcomeMessage()])
        fetchStarterActions(chatId)
          .then(actions => {
            if (Array.isArray(actions) && actions.length > 0) {
              setWelcomeActions(actions.slice(0, 4))
            } else {
              setWelcomeActions(DEFAULT_WELCOME_ACTIONS)
            }
          })
          .catch(() => {
            setWelcomeActions(DEFAULT_WELCOME_ACTIONS)
          })
      }
    } catch (err) {
      console.error('Failed to load messages:', err)
      setMessages([buildWelcomeMessage()])
      setWelcomeActions(DEFAULT_WELCOME_ACTIONS)
    } finally {
      setLoading(false)
      // Scroll to bottom after initial load
      setTimeout(() => messagesEndRef.current?.scrollIntoView({ behavior: 'auto' }), 50)
    }
  }

  const buildWelcomeMessage = () => ({
    type: 'assistant',
    content: WELCOME_MESSAGE,
    timestamp: new Date().toISOString(),
  })

  const parseMessages = (rawMessages) => {
    const display = []
    for (const msg of rawMessages) {
      if (msg.role === 'user') {
        display.push({ type: 'user', content: msg.content, timestamp: msg.created_at })
      } else if (msg.role === 'assistant') {
        if (msg.content) {
          display.push({ type: 'assistant', content: msg.content, timestamp: msg.created_at })
        }
        if (msg.tool_args && Array.isArray(msg.tool_args)) {
          for (const tc of msg.tool_args) {
            display.push({
              type: 'tool_call',
              name: tc.name,
              args: tc.args,
              status: 'completed',
            })
          }
        }
      } else if (msg.role === 'tool') {
        const lastToolCall = [...display].reverse().find(
          d => d.type === 'tool_call' && d.name === msg.tool_name
        )
        if (lastToolCall) {
          try {
            const result = JSON.parse(msg.content || '{}')
            lastToolCall.result = msg.content
            lastToolCall.success = !result.error
            lastToolCall.notes = result._agent_notes
          } catch {
            lastToolCall.result = msg.content
            lastToolCall.success = true
          }
        } else {
          display.push({
            type: 'tool_result',
            name: msg.tool_name,
            result: msg.content,
            success: true,
          })
        }
      }
    }
    return display
  }

  const hasAnyUserMessage = messages.some(msg => msg.type === 'user')
  const showWelcomeActions = !isStreaming && !hasAnyUserMessage && messages.some(
    msg => msg.type === 'assistant' && msg.content === WELCOME_MESSAGE
  )

  const handleSend = useCallback((content) => {
    if (isStreaming) return

    isUserScrolledUp.current = false
    setShowScrollBtn(false)
    setNextSteps([])

    const now = new Date().toISOString()
    setMessages(prev => [...prev, { type: 'user', content, timestamp: now }])
    setStreamingText('')
    setToolCalls([])
    setIsStreaming(true)

    onActivity?.(chatId)

    const controller = streamMessage(chatId, content, {
      onToken: (token) => {
        // Backend sends full accumulated text (snapshot), not deltas
        setStreamingText(token)
      },
      onToolStart: (toolName, args) => {
        setStreamingText(prev => {
          if (prev.trim()) {
            setMessages(msgs => [...msgs, { type: 'assistant', content: prev, timestamp: new Date().toISOString() }])
          }
          return ''
        })
        setToolCalls(prev => [...prev, {
          name: toolName,
          args,
          status: 'running',
        }])
      },
      onToolEnd: (toolName, success, result, notes) => {
        setToolCalls(prev => prev.map(tc =>
          tc.name === toolName && tc.status === 'running'
            ? { ...tc, status: 'completed', success, result, notes }
            : tc
        ))
        // Auto-open approval modal for TOE attribute preview
        if (toolName === 'preview_toe_attributes' && success) {
          try {
            const parsed = typeof result === 'string' ? JSON.parse(result) : result
            if (parsed && parsed.requires_approval && parsed.schemas) {
              setApprovalData(parsed)
              setShowApprovalModal(true)
            }
          } catch { /* ignore parse errors */ }
        }
      },
      onError: (error) => {
        setStreamingText(prev => prev + `\n\n**Error:** ${error}`)
      },
      onDone: () => {
        setStreamingText(prev => {
          if (prev.trim()) {
            const { cleanContent, steps } = parseNextSteps(prev)
            setMessages(msgs => [...msgs, { type: 'assistant', content: cleanContent, timestamp: new Date().toISOString() }])
            if (steps.length > 0) {
              setNextSteps(steps)
            }
          }
          return ''
        })
        setToolCalls(prev => {
          if (prev.length > 0) {
            setMessages(msgs => [...msgs, ...prev.map(tc => ({ type: 'tool_call', ...tc }))])
          }
          return []
        })
        setIsStreaming(false)

        setTimeout(() => {
          onActivity?.(chatId)
          fetchChats().then(chats => {
            const chat = chats.find(c => c.id === chatId)
            if (chat && chat.title !== 'New Chat') {
              onTitleUpdate?.(chatId, chat.title)
            }
          }).catch(() => {})
        }, 1000)
      },
    })

    abortRef.current = controller
  }, [chatId, isStreaming, onActivity, onTitleUpdate])

  const handleStop = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort()
      setIsStreaming(false)
      setStreamingText(prev => {
        if (prev.trim()) {
          setMessages(msgs => [...msgs, { type: 'assistant', content: prev + '\n\n*[Stopped]*', timestamp: new Date().toISOString() }])
        }
        return ''
      })
      setToolCalls(prev => {
        if (prev.length > 0) {
          setMessages(msgs => [...msgs, ...prev.map(tc => ({ ...tc, type: 'tool_call', status: tc.status === 'running' ? 'completed' : tc.status, success: tc.status === 'running' ? false : tc.success }))])
        }
        return []
      })
    }
  }, [])

  const handleApproveAttributes = useCallback(async (editedSchemas) => {
    setShowApprovalModal(false)
    setApprovalData(null)
    try {
      await submitToeApproval(chatId, editedSchemas)
      handleSend("I've reviewed and approved the TOE testing attributes. Please proceed with the full Test of Effectiveness using these approved attributes.")
    } catch (err) {
      console.error('TOE approval failed:', err)
      setMessages(prev => [...prev, {
        type: 'assistant',
        content: `**Error submitting TOE approval:** ${err.message}`,
        timestamp: new Date().toISOString(),
      }])
    }
  }, [chatId, handleSend])

  const handleFileUpload = useCallback(async (file) => {
    try {
      const data = await uploadFile(file)
      handleSend(`I've uploaded a file: **${data.filename}**\nFile path: ${data.path}\nPlease process this file.`)
    } catch (err) {
      setMessages(prev => [...prev, {
        type: 'assistant',
        content: `**Upload Error:** ${err.message}`,
        timestamp: new Date().toISOString(),
      }])
    }
  }, [handleSend])

  const handleFolderUpload = useCallback(async (fileList) => {
    try {
      const data = await uploadFolder(fileList)
      handleSend(`I've uploaded an evidence folder with **${data.file_count} files** (${data.subfolders?.length || 0} control subfolders).\nFolder path: ${data.folder_path}\nPlease process this evidence folder.`)
    } catch (err) {
      setMessages(prev => [...prev, {
        type: 'assistant',
        content: `**Folder Upload Error:** ${err.message}`,
        timestamp: new Date().toISOString(),
      }])
    }
  }, [handleSend])

  const handleDragOver = useCallback((e) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragOver(true)
  }, [])

  const handleDragLeave = useCallback((e) => {
    e.preventDefault()
    e.stopPropagation()
    if (e.currentTarget === e.target || !e.currentTarget.contains(e.relatedTarget)) {
      setIsDragOver(false)
    }
  }, [])

  const handleDrop = useCallback(async (e) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragOver(false)
    const file = e.dataTransfer.files?.[0]
    if (file) await handleFileUpload(file)
  }, [handleFileUpload])

  if (loading) {
    return (
      <div className="chat-area">
        <div className="messages-container">
          <div className="message-wrapper">
            {[1, 2, 3].map(i => (
              <div key={i} className="skeleton-message">
                <div className="skeleton skeleton-avatar" />
                <div className="skeleton-lines">
                  <div className="skeleton skeleton-line" style={{ width: '60%' }} />
                  <div className="skeleton skeleton-line" style={{ width: '80%' }} />
                  <div className="skeleton skeleton-line" style={{ width: '40%' }} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div
      className="chat-area"
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      {isDragOver && (
        <div className="drag-overlay">
          <div className="drag-overlay-content">
            <span className="drag-icon">📎</span>
            <span>Drop file to upload</span>
            <span className="drag-hint">Excel, CSV, PDF, or text files</span>
          </div>
        </div>
      )}

      <div className="messages-container" ref={messagesContainerRef}>
        <div className="message-wrapper">
          {messages.map((msg, idx) => {
            if (msg.type === 'user') {
              return <MessageBubble key={idx} role="user" content={msg.content} timestamp={msg.timestamp} />
            } else if (msg.type === 'assistant') {
              return <MessageBubble key={idx} role="assistant" content={msg.content} timestamp={msg.timestamp} />
            } else if (msg.type === 'tool_call' || msg.type === 'tool_result') {
              return (
                <ToolCallCard
                  key={idx}
                  {...msg}
                  onQuickAction={handleSend}
                  onRequestApproval={(data) => {
                    if (data && data.schemas) {
                      setApprovalData(data)
                      setShowApprovalModal(true)
                    }
                  }}
                />
              )
            }
            return null
          })}

          {showWelcomeActions && (
            <div className="welcome-actions">
              {welcomeActions.map(action => (
                <button
                  key={action.label}
                  className="welcome-action-btn"
                  onClick={() => handleSend(action.prompt)}
                  type="button"
                >
                  {action.label}
                </button>
              ))}
            </div>
          )}

          {toolCalls.map((tc, idx) => (
            <ToolCallCard
              key={`streaming-tc-${idx}`}
              {...tc}
              onQuickAction={handleSend}
              onRequestApproval={(data) => {
                if (data && data.schemas) {
                  setApprovalData(data)
                  setShowApprovalModal(true)
                }
              }}
            />
          ))}

          {streamingText && (
            <MessageBubble role="assistant" content={streamingText} />
          )}

          {isStreaming && !streamingText && toolCalls.length === 0 && (
            <div className="message assistant-message">
              <div className="message-avatar assistant">K</div>
              <div className="message-card">
                <div className="message-body">
                  <div className="typing-dots">
                    <span></span><span></span><span></span>
                  </div>
                </div>
              </div>
            </div>
          )}

          {!isStreaming && nextSteps.length > 0 && (
            <NextStepsPanel steps={nextSteps} onAction={handleSend} />
          )}

          <div ref={messagesEndRef} />
        </div>
      </div>

      {/* Scroll-to-bottom button */}
      {showScrollBtn && (
        <button className="scroll-to-bottom-btn" onClick={scrollToBottom}>
          ↓ Back to latest
        </button>
      )}

      <InputBar
        onSend={handleSend}
        onStop={handleStop}
        isStreaming={isStreaming}
        onFileSelect={handleFileUpload}
        onFolderSelect={handleFolderUpload}
      />

      {/* TOE Attribute Approval Modal */}
      {showApprovalModal && approvalData && (
        <AttributeApprovalModal
          data={approvalData}
          onApprove={handleApproveAttributes}
          onCancel={() => { setShowApprovalModal(false); setApprovalData(null) }}
        />
      )}
    </div>
  )
}
