'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { api } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { useAuthStore } from '@/store/auth'
import { useConversationStore } from '@/store/conversation'

export default function NewConversationPage() {
  const router = useRouter()
  const isAuthenticated = useAuthStore((state) => state.isAuthenticated)
  const activeWorkspaceId = useConversationStore((state) => state.activeWorkspaceId)
  const [error, setError] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)

  useEffect(() => {
    if (!isAuthenticated) {
      router.push('/login')
      return
    }
  }, [isAuthenticated, router])

  const handleCreate = async () => {
    const workspaceId = Number(activeWorkspaceId ?? '1')
    setCreating(true)
    try {
      const conv = await api.createConversation(workspaceId)
      router.push(`/conversations/${conv.conversationId}`)
    } catch (err) {
      setError('Failed to create conversation. Please try again.')
      console.error('Error creating conversation:', err)
    } finally {
      setCreating(false)
    }
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-red-600">{error}</div>
      </div>
    )
  }

  return (
    <div className="flex items-center justify-center h-screen">
      <div className="w-full max-w-md p-6 bg-white rounded-lg shadow-lg">
        <h1 className="text-2xl font-semibold mb-6">New Conversation</h1>
        <Button
          onClick={handleCreate}
          disabled={creating}
          className="w-full"
        >
          {creating ? 'Creating...' : 'Create Conversation'}
        </Button>
      </div>
    </div>
  )
}
