import type { AggFn } from '../../core/ast/expr-ast.js';
import type { Value } from '../../core/types/value.js';

export interface QueryIntentColumn {
  field: string;
  table?: string;
  alias?: string;
  agg?: AggFn;
  expr?: string; // raw expression string for LLM-specified computed columns
}

export interface QueryIntentFilter {
  field: string;
  table?: string;
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'IN' | 'NOT IN' | 'BETWEEN' | 'IS NULL' | 'IS NOT NULL';
  value?: Value | Value[];
  valueRef?: string; // reference to a pipeline variable instead of literal
  expr?: string; // raw SQL expression for complex filter values (e.g., "NOW() - INTERVAL 90 DAY")
}

export interface QueryIntentJoin {
  table: string;
  alias?: string;
  kind?: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL'; // default INNER
  on?: { left: string; right: string }; // explicit override
  // if on is omitted, intentCompiler derives from schema foreignKeys
}

export interface QueryIntentOrderBy {
  field: string;
  table?: string;
  direction?: 'ASC' | 'DESC';
  nulls?: 'FIRST' | 'LAST';
  expr?: string; // SQL expression for complex sorting
}

export interface QueryIntent {
  table: string; // primary table
  columns: QueryIntentColumn[];
  joins?: QueryIntentJoin[];
  filters?: QueryIntentFilter[];
  groupBy?: string[];
  aggregations?: Array<{ fn: AggFn; expr: string; alias: string }>;
  having?: QueryIntentFilter[];
  orderBy?: QueryIntentOrderBy[];
  limit?: number;
  offset?: number;
  distinct?: boolean;
}
