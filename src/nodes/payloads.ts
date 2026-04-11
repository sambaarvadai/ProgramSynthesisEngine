// Pipeline node payload types
// Pure types only, no logic

import type { ExprAST } from '../core/ast/expr-ast.js';
import type { EngineType, RowSchema, Value, RowSet } from '../core/types/index.js';
import type { QueryIntent } from '../compiler/query-ast/query-intent.js';

// ============================================================================
// Template String
// ============================================================================

export type TemplateStringPart =
  | { kind: 'literal'; text: string }
  | { kind: 'expr'; expr: ExprAST };

export type TemplateString = {
  parts: TemplateStringPart[];
};

// ============================================================================
// Transform Operations
// ============================================================================

export type TransformOpKind =
  | 'addField'
  | 'removeField'
  | 'renameField'
  | 'castField'
  | 'filterRows'
  | 'sortRows'
  | 'dedup'
  | 'limit';

export type TransformOp =
  | { kind: 'addField'; name: string; expr: ExprAST }
  | { kind: 'removeField'; name: string }
  | { kind: 'renameField'; from: string; to: string }
  | { kind: 'castField'; name: string; to: EngineType }
  | { kind: 'filterRows'; predicate: ExprAST }
  | { kind: 'sortRows'; keys: Array<{ expr: ExprAST; direction: 'ASC' | 'DESC' }> }
  | { kind: 'dedup'; on: string[] }
  | { kind: 'limit'; count: number };

// ============================================================================
// Node Payloads
// ============================================================================

export type QueryPayload = {
  intent: QueryIntent;
  datasource: string;
};

export type TransformPayload = {
  operations: TransformOp[];
};

export type LLMPayload = {
  model: string;
  systemPrompt?: TemplateString;
  userPrompt: TemplateString;
  outputSchema: RowSchema;
  cacheBy?: string[];
  maxTokens: number;
  temperature?: number;
};

export type HttpPayload = {
  url: TemplateString;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE';
  headers?: Record<string, TemplateString>;
  body?: ExprAST;
  outputSchema: EngineType;
  auth?:
    | { kind: 'bearer'; token: ExprAST }
    | { kind: 'apiKey'; header: string; value: ExprAST };
  retryPolicy?: { maxRetries: number; backoffMs: number };
};

export type InputPayload = {
  schema: RowSchema;
  source:
    | { kind: 'param'; paramKey: string }
    | { kind: 'static'; data: RowSet };
};

export type OutputPayload = {
  outputKey: string;
  transform?: ExprAST;
};

export type ConditionalPayload = {
  predicate: ExprAST;
};

export type AccumulatorKind = 'collect' | 'merge' | 'reduce';

export type AccumulatorDef = {
  kind: AccumulatorKind;
  initial?: Value;
  reducer?: ExprAST;
};

export type LoopPayload = {
  mode: 'forEach' | 'while';
  over?: ExprAST;
  condition?: ExprAST;
  iterVar: string;
  indexVar?: string;
  maxIterations: number;
  accumulator?: AccumulatorDef;
};

export type MergeStrategy = 'union' | 'join' | 'first' | 'last';

export type MergePayload = {
  strategy: MergeStrategy;
  joinOn?: string[];
  waitForAll: boolean;
};

export type ParallelPayload = {
  maxConcurrency: number;
};

export type SubPipelinePayload = {
  pipelineId: string;
  inputMap: Record<string, ExprAST>;
  outputMap: Record<string, string>;
};

export type WriteMode = 'insert' | 'update' | 'upsert' | 'insert_ignore' | 'delete';

export type WritePayload = {
  table: string;
  mode: WriteMode;
  columns: string[];
  staticValues?: Record<string, unknown>;
  staticWhere?: Record<string, unknown>;
  conflictColumns?: string[];
  updateColumns?: string[];
  whereColumns?: string[];
  returning?: string[];
  batchSize?: number;
  datasource: string;
};
