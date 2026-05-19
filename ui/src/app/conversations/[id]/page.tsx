'use client'

import { useEffect, use, useState, useRef } from 'react'
import { useRouter } from 'next/navigation'
import { useConversationStore } from '@/store/conversation'
import { useAuthStore } from '@/store/auth'
import { Shell } from '@/components/layout/shell'
import { Sidebar } from '@/components/layout/sidebar'
import { MessageThread } from '@/components/chat/message-thread'
import { ChatInput } from '@/components/chat/chat-input'
import { ContentPane } from '@/components/content/content-pane'
import { api } from '@/lib/api'
import type { Message } from '@/lib/types'

interface ConversationPageProps {
  params: Promise<{ id: string }>
}

export default function ConversationPage({ params }: ConversationPageProps) {
  const { id } = use(params)
  const router = useRouter()
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated)
  const setActiveConversation = useConversationStore((state) => state.setActiveConversation)
  
  const [chatWidth, setChatWidth] = useState(500)
  const [isResizing, setIsResizing] = useState(false)
  const chatPanelRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!isAuthenticated) {
      router.push('/login')
      return
    }

    setActiveConversation(id)
    
    // Load conversation history
    const loadHistory = async () => {
      try {
        const history = await api.getHistory(id)
        // Add messages to store
        history.messages.forEach((msg: Message) => {
          const { id: msgId, ...msgRest } = msg
          useConversationStore.getState().addMessage({
            id: msgId || crypto.randomUUID(),
            ...msgRest
          })
        })
      } catch (error) {
        console.error('Error loading conversation history:', error)
      }
    }

    loadHistory()
  }, [id])

  const handleMouseDown = (e: React.MouseEvent) => {
    setIsResizing(true)
    e.preventDefault()
  }

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing || !chatPanelRef.current) return
      
      const containerRect = chatPanelRef.current.parentElement?.getBoundingClientRect()
      if (!containerRect) return
      
      const newWidth = e.clientX - containerRect.left - 240 // Subtract sidebar width
      if (newWidth >= 300 && newWidth <= 800) {
        setChatWidth(newWidth)
      }
    }

    const handleMouseUp = () => {
      setIsResizing(false)
    }

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [isResizing])

  return (
    <Shell>
      <div className="w-[240px] flex-shrink-0">
        <Sidebar activeId={id} />
      </div>
      <div
        ref={chatPanelRef}
        className="flex flex-col border-r min-w-0 relative p-4 flex-shrink-0"
        style={{ width: `${chatWidth}px` }}
      >
        <MessageThread />
        <ChatInput />
        <div
          className="absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-primary/50 active:bg-primary/80 transition-colors z-10"
          onMouseDown={handleMouseDown}
        />
      </div>
      <div className="flex-1 min-w-0">
        <ContentPane />
      </div>
    </Shell>
  )
}
