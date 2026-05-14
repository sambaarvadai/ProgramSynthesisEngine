import type { Workspace, Conversation, Plan } from './types'
import { useAuthStore } from '../store/auth'

const API_URL = process.env.NEXT_PUBLIC_API_URL

function getAuthHeaders(): HeadersInit {
  const token = useAuthStore.getState().token
  const headers: HeadersInit = {}
  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }
  return headers
}

export const api = {
  // Workspaces
  async getWorkspaces(): Promise<Workspace[]> {
    const res = await fetch(`${API_URL}/api/workspaces`, {
      headers: getAuthHeaders()
    })
    if (!res.ok) throw new Error('Failed to fetch workspaces')
    return res.json()
  },

  // Conversations
  async createConversation(workspaceId: number): Promise<{ conversationId: string; sessionId: string }> {
    const res = await fetch(`${API_URL}/api/conversations`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...getAuthHeaders()
      },
      body: JSON.stringify({ workspaceId })
    })
    if (!res.ok) throw new Error('Failed to create conversation')
    return res.json()
  },

  async getConversation(id: string): Promise<Conversation> {
    const res = await fetch(`${API_URL}/api/conversations/${id}`, {
      headers: getAuthHeaders()
    })
    if (!res.ok) throw new Error('Failed to fetch conversation')
    return res.json()
  },

  async getHistory(id: string) {
    const res = await fetch(`${API_URL}/api/conversations/${id}/history`, {
      headers: getAuthHeaders()
    })
    if (!res.ok) throw new Error('Failed to fetch history')
    return res.json()
  },

  // Plans
  async generatePlan(conversationId: string, message: string): Promise<Plan> {
    const res = await fetch(`${API_URL}/api/execute/${conversationId}/plan`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        ...getAuthHeaders()
      },
      body: JSON.stringify({ message })
    })
    if (!res.ok) throw new Error('Failed to generate plan')
    return res.json()
  },

  async executePlan(conversationId: string, planId: string, optionalValues: Record<string, Record<string, unknown>>) {
    const res = await fetch(`${API_URL}/api/execute/${conversationId}/execute`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        ...getAuthHeaders()
      },
      body: JSON.stringify({ planId, optionalValues })
    })
    if (!res.ok) throw new Error('Failed to execute plan')
    return res.json()
  },

  // Plans
  async updateOptionalFields(
    planId: string,
    stepId: string,
    values: Record<string, unknown>
  ): Promise<{ ok: boolean }> {
    const res = await fetch(`${API_URL}/api/conversations/${planId}/plans/${planId}/optional-fields`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        ...getAuthHeaders()
      },
      body: JSON.stringify({ stepId, values })
    })
    if (!res.ok) throw new Error('Failed to update optional fields')
    return res.json()
  },

  // Auth
  async login(username: string, password: string): Promise<{ token: string; user: { id: string; username: string; role: string } }> {
    const res = await fetch(`${API_URL}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password })
    })
    if (!res.ok) throw new Error('Login failed')
    return res.json()
  }
}
