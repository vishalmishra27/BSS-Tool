import React, { useState } from 'react'

export default function Sidebar({ chats, activeChatId, onSelectChat, onNewChat, onDeleteChat, onRenameChat }) {
  const [renamingId, setRenamingId] = useState(null)
  const [renameValue, setRenameValue] = useState('')

  const startRename = (chat, e) => {
    e.stopPropagation()
    setRenamingId(chat.id)
    setRenameValue(chat.title)
  }

  const submitRename = (chatId) => {
    if (renameValue.trim()) {
      onRenameChat(chatId, renameValue.trim())
    }
    setRenamingId(null)
  }

  const handleRenameKeyDown = (e, chatId) => {
    if (e.key === 'Enter') {
      submitRename(chatId)
    } else if (e.key === 'Escape') {
      setRenamingId(null)
    }
  }

  return (
    <div className="sidebar">
      <div className="sidebar-brand">
        <div className="kpmg-logo-text">Rishit</div>
        <div className="sidebar-brand-sub">SOX Audit Agent</div>
      </div>
      <div className="sidebar-header">
        <button className="new-chat-btn" onClick={onNewChat}>
          <span>+</span>
          <span>New Chat</span>
        </button>
      </div>
      <div className="chat-list">
        {chats.map(chat => (
          <div
            key={chat.id}
            className={`chat-item ${chat.id === activeChatId ? 'active' : ''}`}
            onClick={() => onSelectChat(chat.id)}
          >
            {renamingId === chat.id ? (
              <input
                className="rename-input"
                value={renameValue}
                onChange={e => setRenameValue(e.target.value)}
                onKeyDown={e => handleRenameKeyDown(e, chat.id)}
                onBlur={() => submitRename(chat.id)}
                autoFocus
                onClick={e => e.stopPropagation()}
              />
            ) : (
              <>
                <span className="chat-item-title">{chat.title}</span>
                <div className="chat-item-actions">
                  <button
                    className="chat-action-btn"
                    onClick={(e) => startRename(chat, e)}
                    title="Rename"
                  >
                    ✎
                  </button>
                  <button
                    className="chat-action-btn delete"
                    onClick={(e) => {
                      e.stopPropagation()
                      onDeleteChat(chat.id)
                    }}
                    title="Delete"
                  >
                    ×
                  </button>
                </div>
              </>
            )}
          </div>
        ))}
        {chats.length === 0 && (
          <div style={{ padding: '16px', color: 'var(--text-muted)', fontSize: '13px', textAlign: 'center' }}>
            No chats yet
          </div>
        )}
      </div>
    </div>
  )
}
