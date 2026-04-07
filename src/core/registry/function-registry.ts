// Defines function registry for managing available functions

import type { EngineType } from '../types/engine-type.js';
import type { Value } from '../types/value.js';
import type { ValidationResult } from '../types/validation.js';

export interface FunctionDefinition {
  name: string;
  inferType: (args: EngineType[]) => EngineType;
  validate: (args: EngineType[]) => ValidationResult;
  execute: (args: Value[]) => Value;
}

export class FunctionNotFoundError extends Error {
  constructor(name: string) {
    super(`Function '${name}' not found in registry`);
    this.name = 'FunctionNotFoundError';
  }
}

export class FunctionRegistry {
  private defs: Map<string, FunctionDefinition> = new Map();

  register(def: FunctionDefinition): void {
    if (this.defs.has(def.name)) {
      throw new Error(`Function '${def.name}' is already registered`);
    }
    this.defs.set(def.name, def);
  }

  get(name: string): FunctionDefinition {
    const def = this.defs.get(name);
    if (!def) {
      throw new FunctionNotFoundError(name);
    }
    return def;
  }

  has(name: string): boolean {
    return this.defs.has(name);
  }

  all(): FunctionDefinition[] {
    return Array.from(this.defs.values());
  }
}
