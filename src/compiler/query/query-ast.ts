import type { ExprAST, AggFn } from '../../core/ast/expr-ast.js';
import type { RowSchema } from '../../core/types/schema.js';

export interface OrderByNode {
  expr: ExprAST;
  direction: 'ASC' | 'DESC';
  nulls: 'FIRST' | 'LAST';
}

export interface ProjectionNode {
  expr: ExprAST;
  alias: string;
}

export interface ScanNode {
  table: string;
  alias: string;
  schema: RowSchema;
  predicate?: ExprAST; // optional predicate for pushdown filtering
}

export interface JoinNode {
  kind: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL';
  table: string;
  alias: string;
  on: ExprAST;
  schema: RowSchema;
}

export interface QueryAST {
  kind: 'Select';
  columns: ProjectionNode[];
  from: ScanNode;
  joins: JoinNode[];
  where?: ExprAST;
  groupBy?: ExprAST[];
  aggregations?: Array<{ fn: AggFn; expr: ExprAST; alias: string }>;
  having?: ExprAST;
  orderBy?: OrderByNode[];
  limit?: number;
  offset?: number;
}

export type QueryDAGNodeId = string;

export type QueryDAGNode = 
  | { id: QueryDAGNodeId; kind: 'Scan'; payload: ScanNode }
  | { id: QueryDAGNodeId; kind: 'Filter'; predicate: ExprAST; input: QueryDAGNodeId }
  | { id: QueryDAGNodeId; kind: 'Join'; payload: JoinNode; left: QueryDAGNodeId; right: QueryDAGNodeId }
  | { id: QueryDAGNodeId; kind: 'Agg'; keys: ExprAST[]; aggregations: Array<{ fn: AggFn; expr: ExprAST; alias: string }>; input: QueryDAGNodeId }
  | { id: QueryDAGNodeId; kind: 'Project'; columns: ProjectionNode[]; input: QueryDAGNodeId }
  | { id: QueryDAGNodeId; kind: 'Sort'; keys: OrderByNode[]; input: QueryDAGNodeId }
  | { id: QueryDAGNodeId; kind: 'Limit'; count: number; offset: number; input: QueryDAGNodeId };

export interface QueryPlan {
  nodes: Map<QueryDAGNodeId, QueryDAGNode>;
  root: QueryDAGNodeId;
  optimizations: string[];
}
