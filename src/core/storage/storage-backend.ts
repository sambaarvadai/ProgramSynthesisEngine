// Defines storage backend interface and base implementations

import type { RowBatch } from '../types/row.js';
import type { RowSchema } from '../types/schema.js';
import type { Row } from '../types/value.js';
import type { ExprAST } from '../ast/expr-ast.js';

export type PhysicalOperatorResult = AsyncIterable<RowBatch>;

export interface StorageBackend {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  transaction<T>(fn: () => Promise<T>): Promise<T>;

  // data operations used by physical operators
  scan(opts: {
    table: string;
    predicate?: ExprAST;
    columns?: string[];
    batchSize: number;
  }): PhysicalOperatorResult;

  insert(table: string, rows: Row[]): Promise<void>;

  createTemp(schema: RowSchema): Promise<string>; // returns temp table name
  dropTemp(table: string): Promise<void>;

  // metadata operations
  tableExists(table: string): Promise<boolean>;
  getSchema(table: string): Promise<RowSchema>;

  // raw SQL query execution
  rawQuery(sql: string, params?: any[]): Promise<{ rows: any[]; rowCount: number }>;
}
