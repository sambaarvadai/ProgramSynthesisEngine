// Defines execution context and context management

import type { Value } from '../types/value.js';
import type { Scope, ScopeKind } from '../scope/scope.js';
import { createScope } from '../scope/scope.js';
import type { ExecutionBudget } from './execution-budget.js';
import { defaultBudget } from './execution-budget.js';

export type { ExecutionBudget };
import type { ExecutionTrace } from './execution-trace.js';

export interface ExecutionContext {
  executionId: string;
  pipelineId: string;
  sessionId: string;
  userId?: string;
  scope: Scope;
  nodeOutputs: Map<string, Value>; // NodeId -> Value
  trace: ExecutionTrace;
  budget: ExecutionBudget;
  params: Record<string, Value>;
}

export function createExecutionContext(opts: {
  pipelineId: string;
  sessionId: string;
  userId?: string;
  params?: Record<string, Value>;
  budget?: Partial<ExecutionBudget>;
}): ExecutionContext {
  const budget = { ...defaultBudget(), ...opts.budget };
  
  return {
    executionId: crypto.randomUUID(),
    pipelineId: opts.pipelineId,
    sessionId: opts.sessionId,
    userId: opts.userId,
    scope: createScope('global', null),
    nodeOutputs: new Map(),
    trace: { events: [] },
    budget,
    params: opts.params || {},
  };
}

export function contextPushScope(
  ctx: ExecutionContext,
  kind: ScopeKind
): ExecutionContext {
  return {
    ...ctx,
    scope: createScope(kind, ctx.scope),
  };
}

export function contextPopScope(ctx: ExecutionContext): ExecutionContext {
  if (!ctx.scope.parent) {
    throw new Error('Cannot pop global scope');
  }
  
  return {
    ...ctx,
    scope: ctx.scope.parent,
  };
}

export function contextSetOutput(
  ctx: ExecutionContext,
  nodeId: string,
  value: Value
): ExecutionContext {
  const newNodeOutputs = new Map(ctx.nodeOutputs);
  newNodeOutputs.set(nodeId, value);
  
  return {
    ...ctx,
    nodeOutputs: newNodeOutputs,
  };
}
