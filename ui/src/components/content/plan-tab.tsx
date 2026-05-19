'use client'

import { useConversationStore } from '@/store/conversation'
import { Badge } from '@/components/ui/badge'

const kindColors: Record<string, string> = {
  query: 'bg-emerald-100 text-emerald-800',
  write: 'bg-orange-100 text-orange-800',
  llm: 'bg-purple-100 text-purple-800',
  transform: 'bg-blue-100 text-blue-800',
  http: 'bg-yellow-100 text-yellow-800',
}

export function PlanTab() {
  const currentPlan = useConversationStore((state) => state.currentPlan)
  const lastPlan = useConversationStore((state) => state.lastPlan)

  const plan = currentPlan ?? lastPlan

  if (!plan) {
    return (
      <div className="p-6 text-sm text-muted-foreground">
        No plan yet — send a message to generate one
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4">
      <p className="text-sm font-medium">{plan.description}</p>

      {plan.compilationErrors && plan.compilationErrors.length > 0 && (
        <div className="rounded border border-destructive/30 bg-destructive/5 p-3 space-y-1">
          <p className="text-xs font-medium text-destructive">Compilation warnings</p>
          {plan.compilationErrors.map((err, i) => (
            <p key={i} className="text-xs text-destructive/80">{err.message}</p>
          ))}
        </div>
      )}

      <div className="space-y-2">
        {plan.steps.map((step, i) => (
          <div key={step.id} className="rounded border border-border p-3 space-y-1">
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground font-mono">{i + 1}</span>
              <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${kindColors[step.kind] ?? 'bg-gray-100 text-gray-800'}`}>
                {step.kind}
              </span>
              <span className="text-xs font-medium">{step.id}</span>
              <span className="text-xs text-muted-foreground ml-auto">{step.datasource}</span>
            </div>
            <p className="text-xs text-muted-foreground pl-5">{step.description}</p>
            {step.dependsOn && step.dependsOn.length > 0 && (
              <p className="text-xs text-muted-foreground pl-5">
                depends on: {step.dependsOn.join(', ')}
              </p>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
