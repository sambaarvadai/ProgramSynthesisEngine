'use client'

import { useConversationStore } from '@/store/conversation'
import { Badge } from '@/components/ui/badge'

export function ResolvedFields() {
  const currentPlan = useConversationStore((state) => state.currentPlan)

  if (!currentPlan) {
    return (
      <div className="p-4 text-sm text-gray-500">
        No plan loaded
      </div>
    )
  }

  const writeSteps = currentPlan.steps.filter((step) => step.resolvedFields && step.resolvedFields.length > 0)

  if (writeSteps.length === 0) {
    return (
      <div className="p-4 text-sm text-gray-500">
        No resolved fields to display
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4">
      <h3 className="font-medium">Resolved Fields</h3>
      {writeSteps.map((step) => (
        <div key={step.id} className="space-y-2">
          <h4 className="text-sm font-medium">{step.description}</h4>
          <div className="space-y-1">
            {step.resolvedFields?.map((field) => (
              <div key={field.column} className="flex items-center gap-2 text-sm">
                <span className="font-medium">{field.column}:</span>
                <span className="text-gray-600">{field.source}</span>
                {field.datasource && (
                  <Badge variant="outline" className="text-xs">
                    {field.datasource}
                  </Badge>
                )}
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}
