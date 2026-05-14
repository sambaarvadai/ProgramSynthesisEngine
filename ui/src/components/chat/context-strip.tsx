'use client'

import { useConversationStore } from '@/store/conversation'

export function ContextStrip() {
  const sessionCursor = useConversationStore((state) => state.sessionCursor)

  if (!sessionCursor) return null

  return (
    <div className="h-8 border-b bg-blue-50 flex items-center px-4">
      <span className="text-xs text-blue-700 font-medium">
        {sessionCursor}
      </span>
    </div>
  )
}
