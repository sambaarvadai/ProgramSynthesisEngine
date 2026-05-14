'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { useAuthStore } from '@/store/auth'
import { useConversationStore } from '@/store/conversation'
import { useExecutionStore } from '@/store/execution'

export default function LoginPage() {
  const router = useRouter()
  const setAuth = useAuthStore((state) => state.setAuth)
  const setActiveConversation = useConversationStore((state) => state.setActiveConversation)
  const setActiveWorkspace = useConversationStore((state) => state.setActiveWorkspace)
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    setLoading(true)

    try {
      const result = await api.login(username, password)
      setAuth(result.user, result.token)

      // Auto-create conversation with default workspace
      const workspaceId = 1
      setActiveWorkspace(String(workspaceId))
      const conversation = await api.createConversation(workspaceId)
      setActiveConversation(conversation.conversationId)
      useConversationStore.getState().clearMessages()
      useConversationStore.getState().setCurrentPlan(null)
      useConversationStore.getState().setPendingConfirmation(false)
      useExecutionStore.getState().reset()

      router.push(`/conversations/${conversation.conversationId}`)
    } catch (err) {
      setError('Invalid username or password')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="flex items-center justify-center min-h-screen bg-gray-50">
      <Card className="w-full max-w-md">
        <CardHeader>
          <CardTitle className="text-2xl">PEE Login</CardTitle>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label className="text-sm font-medium">Username</label>
              <Input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="Enter username"
                disabled={loading}
              />
            </div>
            <div>
              <label className="text-sm font-medium">Password</label>
              <Input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Enter password"
                disabled={loading}
              />
            </div>
            {error && (
              <div className="text-sm text-red-600">{error}</div>
            )}
            <Button type="submit" className="w-full" disabled={loading}>
              {loading ? 'Logging in...' : 'Login'}
            </Button>
          </form>
          <div className="mt-4 text-sm text-gray-600">
            <p>Dev credentials:</p>
            <p>Username: admin, Password: dev</p>
            <p>Username: alice, Password: dev</p>
            <p>Username: bob, Password: dev</p>
            <p>Username: viewer, Password: dev</p>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
