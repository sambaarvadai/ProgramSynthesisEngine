'use client'

import { useState, useEffect } from 'react'
import type { Plan } from '@/lib/types'
import { useConversationStore } from '@/store/conversation'
import { useExecutionStore } from '@/store/execution'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StepBadge } from './step-badge'
import { api } from '@/lib/api'

interface PlanCardProps {
  plan: Plan
}

export function PlanCard({ plan }: PlanCardProps) {
  const setCurrentPlan = useConversationStore((state) => state.setCurrentPlan)
  const addMessage = useConversationStore((state) => state.addMessage)
  const activeConversationId = useConversationStore((state) => state.activeConversationId)
  const startExecution = useExecutionStore((state) => state.startExecution)
  const stopExecution = useExecutionStore((state) => state.stopExecution)
  const setPipelineResult = useExecutionStore((state) => state.setPipelineResult)
  const [isExecuting, setIsExecuting] = useState(false)

  const handleConfirm = async () => {
    if (!activeConversationId) return

    setIsExecuting(true)
    try {
      startExecution()
      const result = await api.executePlan(activeConversationId, plan.planId, {})
      setPipelineResult(result.output)
      
      // Add execution description as a chat message
      if (result.description) {
        addMessage({
          id: crypto.randomUUID(),
          role: 'assistant',
          content: result.description,
          timestamp: new Date().toISOString()
        })
      }
    } catch (error) {
      console.error('Execution error:', error)
    } finally {
      stopExecution()
      setIsExecuting(false)
      setCurrentPlan(null)
    }
  }

  const handleRefine = () => {
    // Send follow-up message logic would go here
    console.log('Refine plan')
  }

  const handleCancel = () => {
    setCurrentPlan(null)
  }

  const hasOptionalFields = plan.steps.some(
    (step) => step.optionalFields && step.optionalFields.length > 0
  )

  return (
    <Card className="border-blue-200 bg-blue-50">
      <CardHeader>
        <CardTitle className="text-base">Generated Plan</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-gray-700">{plan.description}</p>
        
        <div className="space-y-2">
          <h4 className="text-sm font-medium">Steps</h4>
          {plan.steps.map((step) => (
            <div key={step.id} className="flex items-center gap-2 text-sm">
              <StepBadge kind={step.kind} />
              <span>{step.description}</span>
            </div>
          ))}
        </div>

        {plan.compilationErrors.length > 0 && (
          <div className="text-sm text-red-600">
            <p className="font-medium">Compilation Errors:</p>
            {plan.compilationErrors.map((error, i) => (
              <p key={i}>{error.message}</p>
            ))}
          </div>
        )}

        <div className="flex gap-2 pt-2">
          <Button onClick={handleConfirm} size="sm" disabled={isExecuting}>
            {isExecuting && (
              <div className="mr-2 h-4 w-4 animate-spin rounded-full border-2 border-current border-t-transparent" />
            )}
            {isExecuting ? 'Executing...' : hasOptionalFields ? 'Configure' : 'Execute'}
          </Button>
          <Button onClick={handleRefine} variant="outline" size="sm">
            Refine
          </Button>
          <Button onClick={handleCancel} variant="ghost" size="sm">
            Cancel
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
