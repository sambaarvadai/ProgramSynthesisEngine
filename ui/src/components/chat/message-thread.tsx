'use client'

import { useEffect, useRef } from 'react'
import { useConversationStore } from '@/store/conversation'
import { MessageBubble } from './message-bubble'
import { ContextStrip } from './context-strip'
import { PlanCard } from './plan-card'
import { ExecutionProgress } from './execution-progress'

export function MessageThread() {
  const messages = useConversationStore((state) => state.messages)
  const currentPlan = useConversationStore((state) => state.currentPlan)
  const isPlanning = useConversationStore((state) => state.isPlanning)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, currentPlan])

  // Scroll to bottom when planning starts
  useEffect(() => {
    if (isPlanning) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
  }, [isPlanning])

  return (
    <div className="flex flex-col h-full">
      <ContextStrip />
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.length === 0 && (
          <div className="text-center text-gray-500 text-sm py-8">
            Start a conversation to begin
          </div>
        )}
        {messages.map((msg) => (
          <MessageBubble key={msg.id} message={msg} />
        ))}
        {isPlanning && (
          <div className="flex items-center justify-center py-4">
            <div className="flex items-center gap-2 text-sm text-gray-600">
              <div className="h-4 w-4 animate-spin rounded-full border-2 border-current border-t-transparent" />
              <span>Planning...</span>
            </div>
          </div>
        )}
        {currentPlan && <PlanCard plan={currentPlan} />}
        <div ref={bottomRef} />
      </div>
      <ExecutionProgress />
    </div>
  )
}
