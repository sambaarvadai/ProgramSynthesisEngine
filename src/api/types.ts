import type { PipelineGraph } from '../core/graph/index.js'
import type { PlanResult } from '../pipeline-engine.js'

export interface Workspace {
  id: string           // workspace_id as string e.g. "1"
  displayName: string  // workspace name from DB e.g. "Acme Corp"
  kind: 'workspace'
}

export interface Conversation {
  conversationId: string
  sessionId: string
  workspaceId: number
  userId: string
}

export interface ConversationListItem {
  conversationId: string
  title: string
  updatedAt: string
  messageCount: number
}

export interface Message {
  role: 'user' | 'assistant'
  content: string
  timestamp: string
  planId?: string
}

export interface ConversationHistory {
  messages: Message[]
}

export interface PlanRequest {
  message: string
}

export interface ExecuteRequest {
  planId: string
  optionalValues?: Record<string, Record<string, any>>
  params?: Record<string, any>
}

export interface OptionalFieldsUpdate {
  stepId: string
  values: Record<string, any>
}

export interface PlanStep {
  id: string
  kind: string
  description: string
  datasource: string
  dependsOn: string[]
  optionalFields?: Array<{
    column: string
    type: string
    nullable: boolean
    enumValues?: string[]
  }>
  resolvedFields?: Array<{
    column: string
    source: string
    datasource?: string
  }>
}

export interface PlanResponse {
  planId: string
  description: string
  steps: PlanStep[]
  estimatedLLMCalls: number
  compilationErrors: any[]
  params?: Record<string, string>
}

// In-memory plan store with TTL
const planStore = new Map<string, { graph: PipelineGraph; plan: PlanResult; expiresAt: number }>()

export function storePlan(planId: string, graph: PipelineGraph, plan: PlanResult): void {
  const ttl = 30 * 60 * 1000 // 30 minutes
  planStore.set(planId, { graph, plan, expiresAt: Date.now() + ttl })
  
  // Set timeout to clean up
  setTimeout(() => {
    planStore.delete(planId)
  }, ttl)
}

export function getPlan(planId: string): { graph: PipelineGraph; plan: PlanResult } | null {
  const stored = planStore.get(planId)
  if (!stored) return null
  if (Date.now() > stored.expiresAt) {
    planStore.delete(planId)
    return null
  }
  return { graph: stored.graph, plan: stored.plan }
}

export function updatePlanOptionalFields(planId: string, stepId: string, values: Record<string, any>): boolean {
  const stored = planStore.get(planId)
  if (!stored) return false
  if (Date.now() > stored.expiresAt) {
    planStore.delete(planId)
    return false
  }
  
  // Find the step and update its staticValues
  for (const step of stored.plan.intent.steps) {
    if (step.id === stepId) {
      const stepConfig = step.config as any
      if (!stepConfig.fields) stepConfig.fields = {}
      Object.assign(stepConfig.fields, values)
      
      // Also update the graph node
      const node = stored.graph.nodes.get(stepId)
      if (node?.kind === 'write') {
        const payload = node.payload as any
        if (!payload.staticValues) payload.staticValues = {}
        Object.assign(payload.staticValues, values)
      }
      return true
    }
  }
  
  return false
}

// In-memory conversation store
const conversationStore = new Map<string, Conversation>()

export function createConversation(userId: string, workspaceId: number): Conversation {
  const conversation: Conversation = {
    conversationId: crypto.randomUUID(),
    sessionId: crypto.randomUUID(),
    workspaceId,
    userId,
  }
  conversationStore.set(conversation.conversationId, conversation)
  return conversation
}

export function getConversation(conversationId: string): Conversation | null {
  return conversationStore.get(conversationId) ?? null
}

export function listConversations(): ConversationListItem[] {
  // For now, return empty since we're not persisting to pee_store in the API yet
  // This would be implemented by querying pee_pipelines table
  return []
}
