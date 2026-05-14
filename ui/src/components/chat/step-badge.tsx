'use client'

import { Badge } from '@/components/ui/badge'

interface StepBadgeProps {
  kind: string
}

export function StepBadge({ kind }: StepBadgeProps) {
  const colors: Record<string, string> = {
    query: 'bg-blue-100 text-blue-800',
    write: 'bg-green-100 text-green-800',
    llm: 'bg-purple-100 text-purple-800',
    transform: 'bg-yellow-100 text-yellow-800',
    http: 'bg-orange-100 text-orange-800'
  }

  return (
    <Badge className={colors[kind] || 'bg-gray-100 text-gray-800'}>
      {kind}
    </Badge>
  )
}
