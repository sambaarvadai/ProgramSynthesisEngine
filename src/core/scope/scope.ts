// Defines variable scope management and resolution

import type { Value } from '../types/value.js';
import type { EngineType } from '../types/engine-type.js';

export type ScopeKind = 'global' | 'pipeline' | 'loop' | 'branch' | 'node';

export interface Scope {
  id: string;
  kind: ScopeKind;
  bindings: Map<string, Value>;
  parent: Scope | null;
}

export interface TypeScope {
  id: string;
  kind: ScopeKind;
  bindings: Map<string, EngineType>;
  parent: TypeScope | null;
}

export class UnresolvedVariableError extends Error {
  constructor(name: string, depth: number) {
    super(`Unresolved variable '${name}' after searching ${depth} scope levels`);
    this.name = 'UnresolvedVariableError';
  }
}

export function createScope(kind: ScopeKind, parent: Scope | null): Scope {
  return {
    id: crypto.randomUUID(),
    kind,
    bindings: new Map(),
    parent,
  };
}

export function createTypeScope(kind: ScopeKind, parent: TypeScope | null): TypeScope {
  return {
    id: crypto.randomUUID(),
    kind,
    bindings: new Map(),
    parent,
  };
}

export function scopeSet(scope: Scope, name: string, value: Value): void {
  scope.bindings.set(name, value);
}

export function scopeResolve(name: string, scope: Scope): Value {
  let current: Scope | null = scope;
  let depth = 0;
  
  while (current !== null) {
    if (current.bindings.has(name)) {
      return current.bindings.get(name)!;
    }
    current = current.parent;
    depth++;
  }
  
  throw new UnresolvedVariableError(name, depth);
}

export function typeScopeSet(scope: TypeScope, name: string, type: EngineType): void {
  scope.bindings.set(name, type);
}

export function typeScopeResolve(name: string, scope: TypeScope): EngineType {
  let current: TypeScope | null = scope;
  
  while (current !== null) {
    if (current.bindings.has(name)) {
      return current.bindings.get(name)!;
    }
    current = current.parent;
  }
  
  throw new UnresolvedVariableError(name, -1);
}

export function scopePush(parent: Scope, kind: ScopeKind): Scope {
  return createScope(kind, parent);
}

export function scopePop(scope: Scope): Scope {
  if (scope.parent === null) {
    throw new Error('Cannot pop global scope');
  }
  return scope.parent;
}
