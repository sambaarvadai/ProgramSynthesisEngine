// Scope module exports

export type { ScopeKind, Scope, TypeScope } from './scope.js';
export { 
  UnresolvedVariableError,
  createScope,
  createTypeScope,
  scopeSet,
  scopeResolve,
  typeScopeSet,
  typeScopeResolve,
  scopePush,
  scopePop
} from './scope.js';
