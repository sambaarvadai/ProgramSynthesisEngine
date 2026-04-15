export type PipelineStepKind =
  | 'query'
  | 'transform'
  | 'llm'
  | 'http'
  | 'write'
  | 'conditional'
  | 'loop'
  | 'merge'
  | 'parallel';

export interface PipelineStepIntent {
  id: string;
  kind: PipelineStepKind;
  description: string;
  dependsOn?: string[];

  // Conditional-specific
  condition?: string;
  trueBranch?: string;
  falseBranch?: string;
  mergeStep?: string;

  // Loop-specific
  loopMode?: 'forEach' | 'while';
  loopOver?: string;
  loopBody?: string[];
  maxIterations?: number;

  // Merge-specific
  mergeFrom?: string[];
  mergeStrategy?: 'union' | 'join' | 'first';

  // Parallel-specific
  parallelBranches?: string[];
  maxConcurrency?: number;

  // LLM-specific
  outputFields?: string[];

  // General config hints
  config?: Record<string, unknown>;
}

export interface PipelineIntent {
  description: string;
  steps: PipelineStepIntent[];
  params?: Record<string, string>;
  budget?: {
    maxLLMCalls?: number;
    maxIterations?: number;
    timeoutMs?: number;
  };
}

export interface PipelineIntentValidationError {
  stepId?: string;
  code: string;
  message: string;
  missingColumns?: Array<{ column: string; nullable: boolean; description: string }>;
}
