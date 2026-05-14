'use client'

import { useRouter } from 'next/navigation'
import { Button } from '@/components/ui/button'
import { Plus } from 'lucide-react'
import { api } from '@/lib/api'
import { useConversationStore } from '@/store/conversation'
import { useExecutionStore } from '@/store/execution'

interface SidebarProps {
  activeId: string
}

export function Sidebar({ activeId }: SidebarProps) {
  const router = useRouter()
  const { setActiveConversation, activeWorkspaceId } = useConversationStore()

  const handleNewConversation = async () => {
    try {
      const workspaceId = Number(activeWorkspaceId ?? '1')
      const conversation = await api.createConversation(workspaceId)
      setActiveConversation(conversation.conversationId)
      useConversationStore.getState().clearMessages()
      useConversationStore.getState().setCurrentPlan(null)
      useConversationStore.getState().setPendingConfirmation(false)
      useExecutionStore.getState().reset()
      router.push(`/conversations/${conversation.conversationId}`)
    } catch (error) {
      console.error('Failed to create conversation:', error)
    }
  }

  return (
    <div className="border-r bg-gray-50 flex flex-col">
      <div className="p-3 border-b">
        <Button className="w-full justify-start" size="sm" onClick={handleNewConversation}>
          <Plus className="mr-2 h-4 w-4" />
          New Conversation
        </Button>
      </div>
      <div className="flex-1 overflow-y-auto p-2">
        <div className="text-sm text-gray-500 text-center py-4">
          No conversations yet
        </div>
      </div>
    </div>
  )
}
