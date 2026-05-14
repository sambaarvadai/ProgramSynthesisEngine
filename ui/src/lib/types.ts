export type Workspace = {
  id: string           // workspace_id as string
  displayName: string
  kind: 'workspace'
}

export type Conversation = {
  conversationId: string
  title: string
  updatedAt: string
  messageCount: number
}

export type Message = {
  id: string
  role: 'user' | 'assistant'
  content: string
  timestamp: string
  plan?: Plan
}

export type PlanStep = {
  id: string
  kind: 'query' | 'write' | 'llm' | 'transform' | 'http'
  description: string
  datasource: string
  dependsOn: string[]
  optionalFields?: OptionalField[]
  resolvedFields?: ResolvedField[]
}

export type OptionalField = {
  column: string
  type: 'text' | 'number' | 'date' | 'boolean' | 'integer'
  nullable: boolean
}

export type ResolvedField = {
  column: string
  source: string
  datasource?: string
}

export type Plan = {
  planId: string
  description: string
  steps: PlanStep[]
  estimatedLLMCalls: number
  compilationErrors: CompilationError[]
}

export type CompilationError = {
  code: string
  message: string
}

export type ExecutionEvent =
  | { event: 'node_start'; nodeId: string; kind: string }
  | { event: 'node_complete'; nodeId: string; kind: string; rowCount?: number; durationMs: number; result?: NodeResult }
  | { event: 'pipeline_complete'; pipelineId: string; durationMs: number; status: string; output: QueryOutput }
  | { event: 'error'; message: string }

export type NodeResult = {
  mode: string
  table?: string
  rowsAffected?: number
  createdRow?: Record<string, unknown>
}

export type QueryOutput = {
  rows: Record<string, unknown>[]
  schema: { column?: string; name?: string; type: any; nullable?: boolean }[]
}

export type NodeExecutionState = {
  nodeId: string
  kind: string
  status: 'pending' | 'running' | 'complete' | 'error'
  durationMs?: number
  result?: NodeResult
}
