import React, { useState, useEffect, useCallback } from 'react'
import Sidebar from './components/Sidebar'
import ChatArea from './components/ChatArea'
import EmptyState from './components/EmptyState'
import { fetchChats, createChat, deleteChat, renameChat } from './api'

export default function App() {
  const [chats, setChats] = useState([])
  const [activeChatId, setActiveChatId] = useState(() => {
    // Restore active chat from localStorage on mount
    return localStorage.getItem('activeChatId') || null
  })
  const [loading, setLoading] = useState(true)

  // Persist activeChatId to localStorage whenever it changes
  useEffect(() => {
    if (activeChatId) {
      localStorage.setItem('activeChatId', activeChatId)
    } else {
      localStorage.removeItem('activeChatId')
    }
  }, [activeChatId])

  // Load chats on mount
  useEffect(() => {
    loadChats()
  }, [])

  const loadChats = async () => {
    try {
      const data = await fetchChats()
      setChats(data)
      // Verify the stored activeChatId still exists in the chat list
      const storedId = localStorage.getItem('activeChatId')
      if (storedId && !data.find(c => c.id === storedId)) {
        // Chat was deleted or archived — clear it
        setActiveChatId(null)
      }
    } catch (err) {
      console.error('Failed to load chats:', err)
    } finally {
      setLoading(false)
    }
  }

  const handleNewChat = async () => {
    try {
      const chat = await createChat()
      setChats(prev => [chat, ...prev])
      setActiveChatId(chat.id)
    } catch (err) {
      console.error('Failed to create chat:', err)
    }
  }

  const handleDeleteChat = async (chatId) => {
    try {
      await deleteChat(chatId)
      setChats(prev => prev.filter(c => c.id !== chatId))
      if (activeChatId === chatId) {
        setActiveChatId(null)
      }
    } catch (err) {
      console.error('Failed to delete chat:', err)
    }
  }

  const handleRenameChat = async (chatId, title) => {
    try {
      const updated = await renameChat(chatId, title)
      setChats(prev => prev.map(c => c.id === chatId ? { ...c, title: updated.title } : c))
    } catch (err) {
      console.error('Failed to rename chat:', err)
    }
  }

  const handleChatTitleUpdate = useCallback((chatId, title) => {
    setChats(prev => prev.map(c => c.id === chatId ? { ...c, title } : c))
  }, [])

  // Bring active chat to top when it gets a new message
  const handleChatActivity = useCallback((chatId) => {
    setChats(prev => {
      const chat = prev.find(c => c.id === chatId)
      if (!chat) return prev
      return [
        { ...chat, updated_at: new Date().toISOString() },
        ...prev.filter(c => c.id !== chatId),
      ]
    })
  }, [])

  return (
    <div className="layout">
      <Sidebar
        chats={chats}
        activeChatId={activeChatId}
        onSelectChat={setActiveChatId}
        onNewChat={handleNewChat}
        onDeleteChat={handleDeleteChat}
        onRenameChat={handleRenameChat}
      />
      <div className="main-content">
        {activeChatId ? (
          <ChatArea
            key={activeChatId}
            chatId={activeChatId}
            onTitleUpdate={handleChatTitleUpdate}
            onActivity={handleChatActivity}
          />
        ) : (
          <EmptyState onNewChat={handleNewChat} />
        )}
      </div>
    </div>
  )
}
