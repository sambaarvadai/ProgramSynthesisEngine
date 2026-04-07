// Expression evaluator for ExprAST with type inference

import type { ExprAST, BinaryOperator, AggFn } from '../core/ast/index.js';
import type { Value, Row, ArrayValue, ObjectValue } from '../core/types/value.js';
import type { EngineType } from '../core/types/engine-type.js';
import type { Scope, TypeScope } from '../core/scope/index.js';
import type { FunctionRegistry } from '../core/registry/index.js';

export class FieldNotFoundError extends Error {
  constructor(fieldName: string, availableFields: string[]) {
    super(`Field '${fieldName}' not found. Available fields: ${availableFields.join(', ')}`);
    this.name = 'FieldNotFoundError';
  }
}

export class TypeMismatchError extends Error {
  constructor(op: string, leftType: string, rightType: string) {
    super(`Type mismatch in ${op}: ${leftType} vs ${rightType}`);
    this.name = 'TypeMismatchError';
  }
}

export class CastError extends Error {
  constructor(fromType: string, toType: string) {
    super(`Cannot cast from ${fromType} to ${toType}`);
    this.name = 'CastError';
  }
}

export class NotImplementedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NotImplementedError';
  }
}

export class ExprEvaluator {
  constructor(private fnRegistry: FunctionRegistry) {}

  evaluate(expr: ExprAST, scope: Scope, row?: Row): Value {
    switch (expr.kind) {
      case 'Literal':
        return expr.value;

      case 'FieldRef': {
        // If table is specified, try table-prefixed field first, then un-prefixed
        if (expr.table) {
          try {
            const tableValue = scopeResolve(expr.table, scope);
            if (typeof tableValue === 'object' && tableValue !== null && !Array.isArray(tableValue)) {
              // tableValue is a Record, access the field from it
              const fieldValue = (tableValue as Record<string, Value>)[expr.field];
              if (fieldValue !== undefined) {
                return fieldValue;
              }
            }
          } catch (e) {
            // table not found in scope, fall through to row lookup
          }
        }

        // Standard row lookup
        if (row) {
          // Try table-prefixed version first if table is specified
          if (expr.table) {
            const prefixedField = `${expr.table}.${expr.field}`;
            const value = row[prefixedField];
            if (value !== undefined) {
              return value;
            }
          }
          
          // Try un-prefixed version
          const value = row[expr.field];
          if (value !== undefined) {
            return value;
          }
          
          // Try right/left prefixes as last resort (for joined rows)
          // This handles cases where the join operator renames fields to left.field or right.field
          for (const prefix of ['left.', 'right.']) {
            const prefixedField = `${prefix}${expr.field}`;
            const prefixedValue = row[prefixedField];
            if (prefixedValue !== undefined) {
              return prefixedValue;
            }
          }
        }

        // Fallback: try scope as VarRef (for loop scope variables that are records)
        try {
          return scopeResolve(expr.field, scope);
        } catch (e) {
          // Not in scope either
        }

        // Build helpful error message with available fields
        const availableFields: string[] = [];
        if (row) {
          availableFields.push(...Object.keys(row));
        }
        // Try to get fields from scope if it's a record
        try {
          const scopeValue = scopeResolve(expr.field, scope);
          if (typeof scopeValue === 'object' && scopeValue !== null && !Array.isArray(scopeValue)) {
            availableFields.push(...Object.keys(scopeValue));
          }
        } catch (e) {
          // Ignore
        }

        if (availableFields.length > 0) {
          throw new FieldNotFoundError(expr.field, availableFields);
        }
        throw new Error(`Field '${expr.field}' not found`);
      }

      case 'VarRef':
        return scopeResolve(expr.name, scope);

      case 'BinaryOp':
        return this.evaluateBinaryOp(expr, scope, row);

      case 'UnaryOp':
        return this.evaluateUnaryOp(expr, scope, row);

      case 'FunctionCall':
        return this.evaluateFunctionCall(expr, scope, row);

      case 'Conditional':
        return this.evaluateConditional(expr, scope, row);

      case 'Cast':
        return this.evaluateCast(expr, scope, row);

      case 'IsNull':
        return this.evaluateIsNull(expr, scope, row);

      case 'In':
        return this.evaluateIn(expr, scope, row);

      case 'Between':
        return this.evaluateBetween(expr, scope, row);

      case 'WindowExpr':
        throw new NotImplementedError('Window functions require SortOperator context');

      case 'SqlExpr':
        throw new NotImplementedError('SqlExpr requires SQL backend evaluation - cannot be evaluated in-memory');

      case 'Wildcard':
        throw new NotImplementedError('Wildcard (*) requires SQL backend evaluation - cannot be evaluated in-memory');

      default:
        throw new Error(`Unknown expression kind: ${(expr as any).kind}`);
    }
  }

  inferType(expr: ExprAST, typeScope: TypeScope): EngineType {
    switch (expr.kind) {
      case 'Literal':
        return expr.type;

      case 'FieldRef':
        // For now, return 'any' - would need schema context
        return { kind: 'any' };

      case 'VarRef':
        return typeScopeResolve(expr.name, typeScope);

      case 'BinaryOp':
        return this.inferBinaryOpType(expr, typeScope);

      case 'UnaryOp':
        return this.inferUnaryOpType(expr, typeScope);

      case 'FunctionCall':
        return this.inferFunctionCallType(expr, typeScope);

      case 'Conditional':
        return this.inferConditionalType(expr, typeScope);

      case 'Cast':
        return expr.to;

      case 'IsNull':
        return { kind: 'boolean' };

      case 'In':
        return { kind: 'boolean' };

      case 'Between':
        return { kind: 'boolean' };

      case 'WindowExpr':
        throw new NotImplementedError('Window functions require SortOperator context');

      case 'SqlExpr':
        throw new NotImplementedError('SqlExpr requires SQL backend evaluation - cannot be evaluated in-memory');

      case 'Wildcard':
        throw new NotImplementedError('Wildcard (*) requires SQL backend evaluation - cannot be evaluated in-memory');

      default:
        throw new Error(`Unknown expression kind: ${(expr as any).kind}`);
    }
  }

  private evaluateBinaryOp(expr: ExprAST & { kind: 'BinaryOp' }, scope: Scope, row?: Row): Value {
    const left = this.evaluate(expr.left, scope, row);
    const right = this.evaluate(expr.right, scope, row);

    switch (expr.op) {
      // Arithmetic operators
      case '+':
      case '-':
      case '*':
      case '/':
      case '%':
        return this.evaluateArithmetic(expr.op, left, right);

      // Comparison operators
      case '=':
      case '!=':
      case '<':
      case '>':
      case '<=':
      case '>=':
        return this.evaluateComparison(expr.op, left, right);

      // Logical operators
      case 'AND':
      case 'OR':
        return this.evaluateLogical(expr.op, left, right);

      // String operators
      case 'LIKE':
        return this.evaluateLike(left, right);
      case 'CONCAT':
        return this.evaluateConcat(left, right);

      default:
        throw new Error(`Unknown binary operator: ${expr.op}`);
    }
  }

  private evaluateArithmetic(op: BinaryOperator, left: Value, right: Value): Value {
    if (typeof left !== 'number' || typeof right !== 'number') {
      throw new TypeMismatchError(op, typeof left, typeof right);
    }

    switch (op) {
      case '+': return left + right;
      case '-': return left - right;
      case '*': return left * right;
      case '/': return left / right;
      case '%': return left % right;
      default: throw new Error(`Unknown arithmetic operator: ${op}`);
    }
  }

  private evaluateComparison(op: BinaryOperator, left: Value, right: Value): Value {
    switch (op) {
      case '=': return left === right;
      case '!=': return left !== right;
      case '<': return (left as any) < (right as any);
      case '>': return (left as any) > (right as any);
      case '<=': return (left as any) <= (right as any);
      case '>=': return (left as any) >= (right as any);
      default: throw new Error(`Unknown comparison operator: ${op}`);
    }
  }

  private evaluateLogical(op: BinaryOperator, left: Value, right: Value): Value {
    if (typeof left !== 'boolean' || typeof right !== 'boolean') {
      throw new TypeMismatchError(op, typeof left, typeof right);
    }

    switch (op) {
      case 'AND': return left && right;
      case 'OR': return left || right;
      default: throw new Error(`Unknown logical operator: ${op}`);
    }
  }

  private evaluateLike(left: Value, right: Value): Value {
    if (typeof left !== 'string' || typeof right !== 'string') {
      throw new TypeMismatchError('LIKE', typeof left, typeof right);
    }

    const pattern = right.replace(/%/g, '.*');
    const regex = new RegExp(`^${pattern}$`);
    return regex.test(left);
  }

  private evaluateConcat(left: Value, right: Value): Value {
    return String(left) + String(right);
  }

  private evaluateUnaryOp(expr: ExprAST & { kind: 'UnaryOp' }, scope: Scope, row?: Row): Value {
    const operand = this.evaluate(expr.operand, scope, row);

    switch (expr.op) {
      case 'NOT':
        if (typeof operand !== 'boolean') {
          throw new TypeMismatchError('NOT', typeof operand, 'boolean');
        }
        return !operand;

      case 'NEG':
        if (typeof operand !== 'number') {
          throw new TypeMismatchError('NEG', typeof operand, 'number');
        }
        return -operand;

      default:
        throw new Error(`Unknown unary operator: ${expr.op}`);
    }
  }

  private evaluateFunctionCall(expr: ExprAST & { kind: 'FunctionCall' }, scope: Scope, row?: Row): Value {
    const func = this.fnRegistry.get(expr.name);
    const args = expr.args.map(arg => this.evaluate(arg, scope, row));
    return func.execute(args);
  }

  private evaluateConditional(expr: ExprAST & { kind: 'Conditional' }, scope: Scope, row?: Row): Value {
    const condition = this.evaluate(expr.condition, scope, row);
    if (condition) {
      return this.evaluate(expr.then, scope, row);
    } else {
      return this.evaluate(expr.else, scope, row);
    }
  }

  private evaluateCast(expr: ExprAST & { kind: 'Cast' }, scope: Scope, row?: Row): Value {
    const value = this.evaluate(expr.expr, scope, row);
    return this.castValue(value, expr.to);
  }

  private evaluateIsNull(expr: ExprAST & { kind: 'IsNull' }, scope: Scope, row?: Row): Value {
    const value = this.evaluate(expr.expr, scope, row);
    return value === null;
  }

  private evaluateIn(expr: ExprAST & { kind: 'In' }, scope: Scope, row?: Row): Value {
    const value = this.evaluate(expr.expr, scope, row);
    const values = expr.values.map(val => this.evaluate(val, scope, row));
    return values.includes(value);
  }

  private evaluateBetween(expr: ExprAST & { kind: 'Between' }, scope: Scope, row?: Row): Value {
    const value = this.evaluate(expr.expr, scope, row);
    const low = this.evaluate(expr.low, scope, row);
    const high = this.evaluate(expr.high, scope, row);
    return (value as any) >= (low as any) && (value as any) <= (high as any);
  }

  private castValue(value: Value, to: EngineType): Value {
    if (value === null) return null;

    switch (to.kind) {
      case 'string':
        return String(value);
      case 'number':
        const num = Number(value);
        if (isNaN(num)) throw new CastError(typeof value, 'number');
        return num;
      case 'boolean':
        return Boolean(value);
      case 'json':
        return value; // Already a JSON-compatible value
      case 'datetime':
        if (value instanceof Date) return value.toISOString() as Value;
        const date = new Date(value as string);
        if (isNaN(date.getTime())) throw new CastError(typeof value, 'datetime');
        return date.toISOString() as Value;
      case 'array':
        if (Array.isArray(value)) return value;
        throw new CastError(typeof value, 'array');
      case 'record':
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) return value;
        throw new CastError(typeof value, 'record');
      case 'any':
        return value;
      default:
        throw new CastError(typeof value, to.kind);
    }
  }

  // Type inference methods (mirroring evaluate structure)
  private inferBinaryOpType(expr: ExprAST & { kind: 'BinaryOp' }, typeScope: TypeScope): EngineType {
    const leftType = this.inferType(expr.left, typeScope);
    const rightType = this.inferType(expr.right, typeScope);

    if (['=', '!=', '<', '>', '<=', '>=', 'AND', 'OR', 'LIKE', 'IN', 'BETWEEN'].includes(expr.op)) {
      return { kind: 'boolean' };
    }

    if (['+', '-', '*', '/', '%'].includes(expr.op)) {
      return { kind: 'number' };
    }

    if (expr.op === 'CONCAT') {
      return { kind: 'string' };
    }

    return { kind: 'any' };
  }

  private inferUnaryOpType(expr: ExprAST & { kind: 'UnaryOp' }, typeScope: TypeScope): EngineType {
    if (expr.op === 'NOT') return { kind: 'boolean' };
    if (expr.op === 'NEG') return { kind: 'number' };
    return { kind: 'any' };
  }

  private inferFunctionCallType(expr: ExprAST & { kind: 'FunctionCall' }, typeScope: TypeScope): EngineType {
    const func = this.fnRegistry.get(expr.name);
    const argTypes = expr.args.map(arg => this.inferType(arg, typeScope));
    return func.inferType(argTypes);
  }

  private inferConditionalType(expr: ExprAST & { kind: 'Conditional' }, typeScope: TypeScope): EngineType {
    // Both branches should have the same type, return the 'then' type for now
    return this.inferType(expr.then, typeScope);
  }
}

// Helper functions (assuming these are exported from scope module)
function scopeResolve(name: string, scope: Scope): Value {
  let current: Scope | null = scope;
  while (current !== null) {
    if (current.bindings.has(name)) {
      return current.bindings.get(name)!;
    }
    current = current.parent;
  }
  throw new Error(`Unresolved variable: ${name}`);
}

function typeScopeResolve(name: string, typeScope: TypeScope): EngineType {
  let current: TypeScope | null = typeScope;
  while (current !== null) {
    if (current.bindings.has(name)) {
      return current.bindings.get(name)!;
    }
    current = current.parent;
  }
  throw new Error(`Unresolved type variable: ${name}`);
}
