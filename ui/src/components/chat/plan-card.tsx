'use client'

import { useState, useEffect } from 'react'
import type { Plan } from '@/lib/types'
import { useConversationStore } from '@/store/conversation'
import { useExecutionStore } from '@/store/execution'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StepBadge } from './step-badge'

interface PlanCardProps {
  plan: Plan
}

export function PlanCard({ plan }: PlanCardProps) {
  const setCurrentPlan = useConversationStore((state) => state.setCurrentPlan)
  const setActiveTab = useExecutionStore((state) => state.setActiveTab)

  const handleConfirm = () => {
    useConversationStore.getState().confirmPlan()
    setActiveTab('form')
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
  const hasCompilationErrors = plan.compilationErrors && plan.compilationErrors.length > 0

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

        {hasCompilationErrors && (
          <div className="text-sm text-red-600">
            <p className="font-medium">Compilation Errors:</p>
            {plan.compilationErrors.map((error, i) => (
              <p key={i}>{error.message}</p>
            ))}
          </div>
        )}

        <div className="flex gap-2 pt-2">
          <Button onClick={handleConfirm} size="sm">
            {hasOptionalFields || hasCompilationErrors ? 'Configure' : 'Execute'}
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
