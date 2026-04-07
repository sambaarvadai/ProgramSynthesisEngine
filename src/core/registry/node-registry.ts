// Defines node registry for managing available nodes

import type { EngineType } from '../types/engine-type.js';
import type { ValidationResult } from '../types/validation.js';
import type { ExecutionContext } from '../context/execution-context.js';

export interface PortDefinition {
  key: string;
  label: string;
  type: EngineType | 'infer';
  required: boolean;
}

export interface NodeDefinition<TPayload, TInput, TOutput> {
  kind: string;
  displayName: string;
  icon?: string;
  color?: string;
  inputPorts: PortDefinition[];
  outputPorts: PortDefinition[];
  validate: (payload: unknown) => ValidationResult;
  inferOutputSchema: (payload: TPayload, inputSchema: EngineType) => EngineType;
  execute: (payload: TPayload, input: TInput, ctx: ExecutionContext) => Promise<TOutput>;
}

export class UnknownNodeKindError extends Error {
  constructor(kind: string) {
    super(`Unknown node kind '${kind}' not found in registry`);
    this.name = 'UnknownNodeKindError';
  }
}

export class NodeRegistry {
  private defs: Map<string, NodeDefinition<any, any, any>> = new Map();

  register<P, I, O>(def: NodeDefinition<P, I, O>): void {
    if (this.defs.has(def.kind)) {
      throw new Error(`Node kind '${def.kind}' is already registered`);
    }
    this.defs.set(def.kind, def);
  }

  get(kind: string): NodeDefinition<any, any, any> {
    const def = this.defs.get(kind);
    if (!def) {
      throw new UnknownNodeKindError(kind);
    }
    return def;
  }

  has(kind: string): boolean {
    return this.defs.has(kind);
  }

  all(): NodeDefinition<any, any, any>[] {
    return Array.from(this.defs.values());
  }
}
