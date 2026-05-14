'use client'

import { useConversationStore } from '@/store/conversation'

export function PlanTab() {
  const currentPlan = useConversationStore((state) => state.currentPlan)

  if (!currentPlan) {
    return (
      <div className="p-4 text-sm text-gray-500">
        No plan loaded
      </div>
    )
  }

  return (
    <div className="p-4">
      <pre className="text-xs bg-gray-100 p-4 rounded overflow-auto">
        {JSON.stringify(currentPlan, null, 2)}
      </pre>
    </div>
  )
}
