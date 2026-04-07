// Defines graph node structures and node operations

export type NodeId = string;

export type ErrorPolicyKind = 'fail' | 'skip' | 'retry' | 'fallback';

export interface ErrorPolicy {
  onError: ErrorPolicyKind;
  maxRetries?: number;
  retryDelayMs?: number;
  fallbackNodeId?: NodeId;
}

export interface PipelineNode {
  id: NodeId;
  kind: string;
  label?: string;
  payload: unknown; // validated by NodeDefinition at runtime
  errorPolicy: ErrorPolicy;
}
