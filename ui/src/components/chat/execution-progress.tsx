'use client'

import { useExecutionStore } from '@/store/execution'
import { Check, X, Loader2 } from 'lucide-react'

export function ExecutionProgress() {
  const nodeStates = useExecutionStore((state) => state.nodeStates)
  const isExecuting = useExecutionStore((state) => state.isExecuting)

  if (!isExecuting && Object.keys(nodeStates).length === 0) return null

  return (
    <div className="border-t bg-gray-50 p-3">
      <div className="text-sm font-medium mb-2">Execution Progress</div>
      <div className="space-y-1">
        {Object.entries(nodeStates).map(([nodeId, state]) => (
          <div key={nodeId} className="flex items-center gap-2 text-sm">
            {state.status === 'pending' && (
              <div className="w-4 h-4 rounded-full bg-gray-300" />
            )}
            {state.status === 'running' && (
              <Loader2 className="w-4 h-4 animate-spin text-blue-600" />
            )}
            {state.status === 'complete' && (
              <Check className="w-4 h-4 text-green-600" />
            )}
            {state.status === 'error' && (
              <X className="w-4 h-4 text-red-600" />
            )}
            <span className="flex-1">{state.kind}</span>
            {state.durationMs && (
              <span className="text-xs text-gray-500">
                {state.durationMs}ms
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
