// Defines expression abstract syntax tree structures

import type { Value } from '../types/value.js';
import type { EngineType } from '../types/engine-type.js';

export type BinaryOperator =
  // Arithmetic operators
  | '+' | '-' | '*' | '/' | '%'
  // Comparison operators
  | '=' | '!=' | '<' | '>' | '<=' | '>='
  // Logical operators
  | 'AND' | 'OR'
  // String operators
  | 'LIKE' | 'CONCAT';

export type AggFn =
  | 'SUM' | 'AVG' | 'COUNT' | 'COUNT_DISTINCT' | 'MIN' | 'MAX'
  | 'ROW_NUMBER' | 'RANK' | 'LAG' | 'LEAD';

export type ExprAST =
  | { kind: 'Literal'; value: Value; type: EngineType }
  | { kind: 'FieldRef'; table?: string; field: string }
  | { kind: 'VarRef'; name: string }
  | { kind: 'BinaryOp'; op: BinaryOperator; left: ExprAST; right: ExprAST }
  | { kind: 'UnaryOp'; op: 'NOT' | 'NEG'; operand: ExprAST }
  | { kind: 'FunctionCall'; name: string; args: ExprAST[] }
  | { kind: 'Conditional'; condition: ExprAST; then: ExprAST; else: ExprAST }
  | { kind: 'Cast'; expr: ExprAST; to: EngineType }
  | { kind: 'IsNull'; expr: ExprAST }
  | { kind: 'In'; expr: ExprAST; values: ExprAST[] }
  | { kind: 'Between'; expr: ExprAST; low: ExprAST; high: ExprAST }
  | { kind: 'WindowExpr'; fn: AggFn; over: ExprAST; partition?: ExprAST[]; orderBy?: ExprAST[] }
  | { kind: 'SqlExpr'; sql: string } // raw SQL expression for complex values
  | { kind: 'Wildcard' }; // SQL * wildcard for COUNT(*), etc.
