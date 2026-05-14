'use client'

import { useEffect, useState } from 'react'
import { api } from '@/lib/api'
import type { Workspace } from '@/lib/types'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { ChevronDown } from 'lucide-react'
import { useConversationStore } from '@/store/conversation'

export function WorkspaceSelector() {
  const [workspaces, setWorkspaces] = useState<Workspace[]>([])
  const [selected, setSelected] = useState<Workspace | null>(null)
  const [error, setError] = useState<string | null>(null)
  const setActiveWorkspace = useConversationStore(s => s.setActiveWorkspace)

  useEffect(() => {
    api.getWorkspaces()
      .then(data => {
        setWorkspaces(data)
        if (data.length > 0) {
          setSelected(data[0])
          setActiveWorkspace(data[0].id)
        }
      })
      .catch(() => setError('Could not load workspaces'))
  }, [])

  if (error) {
    return (
      <div className="flex items-center gap-2 px-3 py-1 rounded-full border border-border text-sm text-muted-foreground">
        <span className="w-2 h-2 rounded-full bg-destructive" />
        Workspace unavailable
      </div>
    )
  }

  if (!selected) {
    return (
      <div className="flex items-center gap-2 px-3 py-1 rounded-full border border-border text-sm text-muted-foreground animate-pulse">
        Loading…
      </div>
    )
  }

  // Single workspace — non-interactive pill
  if (workspaces.length === 1) {
    return (
      <div className="flex items-center gap-2 px-3 py-1 rounded-full border border-border text-sm font-medium">
        <span className="w-2 h-2 rounded-full bg-emerald-500" />
        {selected.displayName}
      </div>
    )
  }

  // Multiple workspaces — dropdown
  return (
    <DropdownMenu>
      <DropdownMenuTrigger className="flex items-center gap-2 px-3 py-1 rounded-full border border-border text-sm font-medium hover:bg-accent transition-colors cursor-pointer">
        <span className="w-2 h-2 rounded-full bg-emerald-500" />
        {selected.displayName}
        <ChevronDown className="w-3 h-3 text-muted-foreground" />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start">
        {workspaces.map(ws => (
          <DropdownMenuItem
            key={ws.id}
            onSelect={() => {
              setSelected(ws)
              setActiveWorkspace(ws.id)
            }}
            className={ws.id === selected.id ? 'font-medium' : ''}
          >
            <span className="w-2 h-2 rounded-full bg-emerald-500 mr-2" />
            {ws.displayName}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
