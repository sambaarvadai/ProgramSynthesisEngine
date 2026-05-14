'use client'

import { useState } from 'react'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { Send, Loader2 } from 'lucide-react'
import { useConversationStore } from '@/store/conversation'
import { useExecutionStore } from '@/store/execution'
import { api } from '@/lib/api'

export function ChatInput() {
  const [message, setMessage] = useState('')
  const activeConversationId = useConversationStore((state) => state.activeConversationId)
  const isPlanning = useConversationStore((state) => state.isPlanning)
  const isExecuting = useExecutionStore((state) => state.isExecuting)
  
  const addMessage = useConversationStore((state) => state.addMessage)
  const setIsPlanning = useConversationStore((state) => state.setIsPlanning)
  const setCurrentPlan = useConversationStore((state) => state.setCurrentPlan)

  const handleSend = async () => {
    if (message.trim() && !isPlanning && !isExecuting && activeConversationId) {
      // Add user message to store
      addMessage({
        id: crypto.randomUUID(),
        role: 'user',
        content: message,
        timestamp: new Date().toISOString()
      })

      const messageContent = message
      setMessage('')
      setIsPlanning(true)

      try {
        // Use JSON API instead of SSE for plan generation
        const plan = await api.generatePlan(activeConversationId, messageContent)
        setCurrentPlan(plan)
      } catch (err) {
        console.error('Plan generation error:', err)
      } finally {
        setIsPlanning(false)
      }
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  return (
    <div className="border-t p-4">
      <div className="flex gap-2">
        <Textarea
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Type your message..."
          disabled={isPlanning || isExecuting || !activeConversationId}
          className="min-h-[60px] resize-none"
        />
        <Button
          onClick={handleSend}
          disabled={!message.trim() || isPlanning || isExecuting || !activeConversationId}
          size="icon"
        >
          {isPlanning || isExecuting ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Send className="h-4 w-4" />
          )}
        </Button>
      </div>
      {(isPlanning || isExecuting) && (
        <div className="mt-2 flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          <span>{isPlanning ? 'Generating plan...' : 'Executing query...'}</span>
        </div>
      )}
    </div>
  )
}
