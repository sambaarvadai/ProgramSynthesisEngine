// Defines execution budget management and budget tracking

export interface ExecutionBudget {
  maxLLMCalls: number;
  llmCallsUsed: number;
  maxIterations: number;
  iterationsUsed: number;
  maxMemoryMB: number;
  timeoutMs: number;
  startedAt: number;
  maxRowsPerNode: number;
  maxBatchSize: number;
}

export function defaultBudget(): ExecutionBudget {
  return {
    maxLLMCalls: 100,
    llmCallsUsed: 0,
    maxIterations: 1000,
    iterationsUsed: 0,
    maxMemoryMB: 512,
    timeoutMs: 300000, // 5 minutes
    startedAt: Date.now(),
    maxRowsPerNode: 10000,
    maxBatchSize: 1000,
  };
}

export function isBudgetExceeded(budget: ExecutionBudget): boolean {
  const now = Date.now();
  const elapsedMs = now - budget.startedAt;
  
  return (
    budget.llmCallsUsed >= budget.maxLLMCalls ||
    budget.iterationsUsed >= budget.maxIterations ||
    elapsedMs >= budget.timeoutMs
  );
}

export function budgetRemaining(budget: ExecutionBudget): {
  timeMs: number;
  llmCalls: number;
  iterations: number;
} {
  const now = Date.now();
  const elapsedMs = now - budget.startedAt;
  
  return {
    timeMs: Math.max(0, budget.timeoutMs - elapsedMs),
    llmCalls: Math.max(0, budget.maxLLMCalls - budget.llmCallsUsed),
    iterations: Math.max(0, budget.maxIterations - budget.iterationsUsed),
  };
}
