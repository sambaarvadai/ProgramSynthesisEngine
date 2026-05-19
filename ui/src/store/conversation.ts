import { create } from 'zustand'
import type { Message, Plan } from '../lib/types'

interface ConversationStore {
  activeConversationId: string | null
  activeWorkspaceId: string | null
  messages: Message[]
  currentPlan: Plan | null
  lastPlan: Plan | null
  planningStage: string | null
  isPlanning: boolean
  sessionCursor: string | null
  pendingConfirmation: boolean

  setActiveConversation: (id: string) => void
  setActiveWorkspace: (id: string) => void
  addMessage: (msg: Message) => void
  clearMessages: () => void
  setCurrentPlan: (plan: Plan | null) => void
  setPlanningStage: (stage: string | null) => void
  setIsPlanning: (v: boolean) => void
  setSessionCursor: (cursor: string | null) => void
  confirmPlan: () => void
  setPendingConfirmation: (v: boolean) => void
}

export const useConversationStore = create<ConversationStore>((set) => ({
  activeConversationId: null,
  activeWorkspaceId: null,
  messages: [],
  currentPlan: null,
  lastPlan: null,
  planningStage: null,
  isPlanning: false,
  sessionCursor: null,
  pendingConfirmation: false,

  setActiveConversation: (id) => set({ activeConversationId: id }),
  setActiveWorkspace: (id) => set({ activeWorkspaceId: id }),
  clearMessages: () => set({ messages: [] }),
  addMessage: (msg) => set((state) => ({
    messages: [...state.messages, msg],
    // Clear plan when user sends a new message to prevent showing stale plan from previous turn
    ...(msg.role === 'user' ? { currentPlan: null } : {})
  })),
  setCurrentPlan: (plan) => set((state) => ({ 
    currentPlan: plan,
    ...(plan ? { lastPlan: plan } : {})
  })),
  setPlanningStage: (stage) => set({ planningStage: stage }),
  setIsPlanning: (v) => set({ isPlanning: v }),
  setSessionCursor: (cursor) => set({ sessionCursor: cursor }),
  confirmPlan: () => set({ pendingConfirmation: true }),
  setPendingConfirmation: (v) => set({ pendingConfirmation: v })
}))
