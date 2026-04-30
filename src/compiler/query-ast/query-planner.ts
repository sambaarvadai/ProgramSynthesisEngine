import type { SchemaConfig } from '../schema/schema-config.js';
import { getTable } from '../schema/schema-config.js';
import type { QueryAST, ScanNode, JoinNode, ProjectionNode, OrderByNode } from './query-ast.js';
import type { QueryPlan, QueryDAGNode, QueryDAGNodeId } from './query-ast.js';
import type { ExprAST } from '../../core/ast/expr-ast.js';
import { randomUUID } from 'crypto';

interface OptimizationResult {
  nodes: Map<QueryDAGNodeId, QueryDAGNode>;
  root: QueryDAGNodeId;
  applied: string[];
}

export class QueryPlanner {
  constructor(private schema: SchemaConfig) {}

  plan(ast: QueryAST): QueryPlan {
    const { nodes, root } = this.buildDAG(ast);
    const { nodes: optimizedNodes, root: optimizedRoot, applied } = this.applyOptimizations(nodes, root);

    return {
      nodes: optimizedNodes,
      root: optimizedRoot,
      optimizations: applied
    };
  }

  private buildDAG(ast: QueryAST): { nodes: Map<QueryDAGNodeId, QueryDAGNode>; root: QueryDAGNodeId } {
    const nodes = new Map<QueryDAGNodeId, QueryDAGNode>();
    let currentRoot: QueryDAGNodeId;

    // 1. Scan node for primary table
    const scanNodeId = this.generateId();
    const scanNode: QueryDAGNode = {
      id: scanNodeId,
      kind: 'Scan',
      payload: ast.from
    };
    nodes.set(scanNodeId, scanNode);
    currentRoot = scanNodeId;

    // 2. Join nodes
    for (const join of ast.joins) {
      const joinNodeId = this.generateId();
      const rightScanNodeId = this.generateId();
      
      // Create scan node for join table
      const rightScanNode: QueryDAGNode = {
        id: rightScanNodeId,
        kind: 'Scan',
        payload: {
          table: join.table,
          alias: join.alias || join.table,
          schema: join.schema
        }
      };
      nodes.set(rightScanNodeId, rightScanNode);

      // Create join node
      const joinNode: QueryDAGNode = {
        id: joinNodeId,
        kind: 'Join',
        payload: join,
        left: currentRoot,
        right: rightScanNodeId
      };
      nodes.set(joinNodeId, joinNode);
      currentRoot = joinNodeId;
    }

    // 3. Where filter
    console.debug(`[QueryPlanner] Processing where clause:`, ast.where ? 'exists' : 'none');
    if (ast.where) {
      console.debug(`[QueryPlanner] Where predicate:`, JSON.stringify(ast.where, null, 2));
      const filterNodeId = this.generateId();
      const filterNode: QueryDAGNode = {
        id: filterNodeId,
        kind: 'Filter',
        predicate: ast.where,
        input: currentRoot
      };
      nodes.set(filterNodeId, filterNode);
      currentRoot = filterNodeId;
      console.debug(`[QueryPlanner] Filter node created with id: ${filterNodeId}`);
    }

    // 4. Aggregation
    if (ast.groupBy || ast.aggregations) {
      const aggNodeId = this.generateId();
      const aggNode: QueryDAGNode = {
        id: aggNodeId,
        kind: 'Agg',
        keys: ast.groupBy || [],
        aggregations: ast.aggregations || [],
        input: currentRoot
      };
      nodes.set(aggNodeId, aggNode);
      currentRoot = aggNodeId;
    }

    // 5. Having filter
    if (ast.having) {
      const havingFilterNodeId = this.generateId();
      const havingFilterNode: QueryDAGNode = {
        id: havingFilterNodeId,
        kind: 'Filter',
        predicate: ast.having,
        input: currentRoot
      };
      nodes.set(havingFilterNodeId, havingFilterNode);
      currentRoot = havingFilterNodeId;
    }

    // 6. Sort BEFORE project — sort keys must still be available
    if (ast.orderBy && ast.orderBy.length > 0) {
      const sortNodeId = this.generateId();
      const sortNode: QueryDAGNode = {
        id: sortNodeId,
        kind: 'Sort',
        keys: ast.orderBy,
        input: currentRoot
      };
      nodes.set(sortNodeId, sortNode);
      currentRoot = sortNodeId;
    }

    // 7. Project node (always) — comes AFTER Sort
    const projectId = this.generateId();
    
    // Build projections: regular columns + aggregation result columns
    const projectColumns: ProjectionNode[] = [...ast.columns];
    
    // Add projections for aggregation results
    if (ast.aggregations) {
      for (const agg of ast.aggregations) {
        projectColumns.push({
          expr: { kind: 'FieldRef', field: agg.alias },
          alias: agg.alias
        });
      }
    }
    
    const projectNode: QueryDAGNode = {
      id: projectId,
      kind: 'Project',
      columns: projectColumns,
      input: currentRoot
    };
    nodes.set(projectId, projectNode);
    currentRoot = projectId;

    // 8. Limit
    if (ast.limit !== undefined || ast.offset !== undefined) {
      const limitNodeId = this.generateId();
      const limitNode: QueryDAGNode = {
        id: limitNodeId,
        kind: 'Limit',
        count: ast.limit || Number.MAX_SAFE_INTEGER,
        offset: ast.offset || 0,
        input: currentRoot
      };
      nodes.set(limitNodeId, limitNode);
      currentRoot = limitNodeId;
    }

    return { nodes, root: currentRoot };
  }

  private applyOptimizations(
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    root: QueryDAGNodeId
  ): OptimizationResult {
    let currentNodes = new Map(nodes);
    let currentRoot = root;
    const applied: string[] = [];

    // Optimization 1: Predicate Pushdown
    const predicateResult = this.predicatePushdown(currentNodes, currentRoot);
    if (predicateResult.applied) {
      currentNodes = predicateResult.nodes;
      currentRoot = predicateResult.root;
      applied.push('predicate_pushdown');
    }

    // Optimization 2: Projection Pushdown
    const projectionResult = this.projectionPushdown(currentNodes, currentRoot);
    if (projectionResult.applied) {
      currentNodes = projectionResult.nodes;
      currentRoot = projectionResult.root;
      applied.push('projection_pushdown');
    }

    return {
      nodes: currentNodes,
      root: currentRoot,
      applied
    };
  }

  private predicateReferencesOtherTables(
    predicate: ExprAST,
    scanTable: string,
    nodes: Map<QueryDAGNodeId, QueryDAGNode>
  ): boolean {
    // Collect all table names referenced in the predicate
    const referencedTables = new Set<string>();
    
    const collectTables = (expr: ExprAST) => {
      if (expr.kind === 'FieldRef' && expr.table) {
        referencedTables.add(expr.table);
      } else if (expr.kind === 'BinaryOp') {
        collectTables(expr.left);
        collectTables(expr.right);
      }
    };
    
    collectTables(predicate);
    
    // Check if any referenced table is different from the scan table
    for (const table of Array.from(referencedTables)) {
      if (table !== scanTable) {
        return true;
      }
    }
    
    return false;
  }

  private predicatePushdown(
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    root: QueryDAGNodeId
  ): { nodes: Map<QueryDAGNodeId, QueryDAGNode>; root: QueryDAGNodeId; applied: boolean } {
    const newNodes = new Map(nodes);
    let applied = false;

    // Find Filter nodes above Scan nodes
    for (const [nodeId, node] of Array.from(newNodes.entries())) {
      if (node.kind === 'Filter') {
        const inputNode = newNodes.get(node.input);
        if (inputNode && inputNode.kind === 'Scan') {
          // Check if the filter references any tables that are not the scan's table
          // If so, don't push down (it's a filter on a joined table)
          const scanTable = inputNode.payload.table;
          const filterReferencesOtherTables = this.predicateReferencesOtherTables(node.predicate, scanTable, nodes);
          
          if (!filterReferencesOtherTables) {
            // Push filter into Scan node payload
            const updatedScan: QueryDAGNode = {
              ...inputNode,
              payload: {
                ...inputNode.payload,
                predicate: node.predicate
              }
            };
            newNodes.set(inputNode.id, updatedScan);

            // Remove original filter node and update parent references
            newNodes.delete(nodeId);
            this.updateParentReferences(newNodes, nodeId, inputNode.id);

            applied = true;
          }
        }
      }
    }

    // Find Filter nodes above Join nodes
    console.debug(`[QueryPlanner] Starting predicate pushdown, checking ${newNodes.size} nodes`);
    for (const [nodeId, node] of Array.from(newNodes.entries())) {
      if (node.kind === 'Filter') {
        console.log(`[QueryPlanner] Found Filter node ${nodeId}, checking for pushdown`);
        const inputNode = newNodes.get(node.input);
        if (inputNode && inputNode.kind === 'Join') {
          console.log(`[QueryPlanner] Filter ${nodeId} is above Join ${node.input}`);
          // Check if predicate references only one side of the join
          const referencedTables = this.extractReferencedTables(node.predicate);
          console.log(`[QueryPlanner] Referenced tables: ${referencedTables.join(', ')}`);
          
          // Get left and right tables from the join's input nodes
          const leftInput = newNodes.get(inputNode.left);
          const rightInput = newNodes.get(inputNode.right);
          const leftTable = leftInput?.kind === 'Scan' ? leftInput.payload.table : null;
          const rightTable = rightInput?.kind === 'Scan' ? rightInput.payload.table : null;
          
          console.log(`[QueryPlanner] Left table: ${leftTable}, Right table: ${rightTable}`);

          if (leftTable !== null && rightTable !== null && referencedTables.includes(leftTable) && !referencedTables.includes(rightTable)) {
            console.log(`[QueryPlanner] Pushing filter below join on left side`);
            // Push filter below join on left side
            const filterBelowJoinId = this.generateId();
            const filterBelowJoin: QueryDAGNode = {
              id: filterBelowJoinId,
              kind: 'Filter',
              predicate: node.predicate,
              input: inputNode.left
            };
            newNodes.set(filterBelowJoinId, filterBelowJoin);

            // Update join node's left input
            const updatedJoin: QueryDAGNode = {
              ...inputNode,
              left: filterBelowJoinId
            };
            newNodes.set(inputNode.id, updatedJoin);

            // Remove original filter node and update parent references
            newNodes.delete(nodeId);
            this.updateParentReferences(newNodes, nodeId, filterBelowJoinId);

            applied = true;
          } else if (leftTable !== null && rightTable !== null && referencedTables.includes(rightTable) && !referencedTables.includes(leftTable)) {
            // Push filter below join on right side
            const filterBelowJoinId = this.generateId();
            const filterBelowJoin: QueryDAGNode = {
              id: filterBelowJoinId,
              kind: 'Filter',
              predicate: node.predicate,
              input: inputNode.right
            };
            newNodes.set(filterBelowJoinId, filterBelowJoin);

            // Update join node's right input
            const updatedJoin: QueryDAGNode = {
              ...inputNode,
              right: filterBelowJoinId
            };
            newNodes.set(inputNode.id, updatedJoin);

            // Remove original filter node and update parent references
            newNodes.delete(nodeId);
            this.updateParentReferences(newNodes, nodeId, inputNode.id);

            applied = true;
          }
        }
      }
    }

    return { nodes: newNodes, root, applied };
  }

  private projectionPushdown(
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    root: QueryDAGNodeId
  ): { nodes: Map<QueryDAGNodeId, QueryDAGNode>; root: QueryDAGNodeId; applied: boolean } {
    // TODO: projection pushdown disabled pending cycle-free implementation
    // The current implementation creates cycles in the DAG when inserting
    // new Project nodes below existing ones, causing infinite recursion
    // in OperatorTreeBuilder.buildProjectNode.
    // Future implementation should modify Scan node columns directly instead
    // of inserting new Project nodes.
    return { nodes, root, applied: false };
  }

  private generateId(): QueryDAGNodeId {
    return randomUUID().substring(0, 8);
  }

  private extractReferencedTables(expr: ExprAST): string[] {
    const tables = new Set<string>();
    
    const traverse = (e: ExprAST) => {
      switch (e.kind) {
        case 'FieldRef':
          if (e.table) {
            tables.add(e.table);
          }
          break;
        case 'BinaryOp':
          traverse(e.left);
          traverse(e.right);
          break;
        case 'UnaryOp':
          traverse(e.operand);
          break;
        case 'FunctionCall':
          e.args.forEach(traverse);
          break;
        case 'Conditional':
          traverse(e.condition);
          traverse(e.then);
          traverse(e.else);
          break;
        case 'Cast':
        case 'IsNull':
          traverse(e.expr);
          break;
        case 'In':
          traverse(e.expr);
          e.values.forEach(traverse);
          break;
        case 'Between':
          traverse(e.expr);
          traverse(e.low);
          traverse(e.high);
          break;
        case 'SqlExpr':
          // Can't extract tables from raw SQL - assume it references the left table
          // This allows predicatePushdown to move it below the join
          break; // returns empty set -> pushdown applies to left side
      }
    };

    traverse(expr);
    return Array.from(tables);
  }

  private extractRequiredFields(columns: ProjectionNode[]): string[] {
    const fields = new Set<string>();
    
    const traverse = (expr: ExprAST) => {
      switch (expr.kind) {
        case 'FieldRef':
          fields.add(expr.field);
          break;
        case 'BinaryOp':
          traverse(expr.left);
          traverse(expr.right);
          break;
        case 'UnaryOp':
          traverse(expr.operand);
          break;
        case 'FunctionCall':
          expr.args.forEach(traverse);
          break;
        case 'Conditional':
          traverse(expr.condition);
          traverse(expr.then);
          traverse(expr.else);
          break;
        case 'Cast':
        case 'IsNull':
          traverse(expr.expr);
          break;
        case 'In':
          traverse(expr.expr);
          expr.values.forEach(traverse);
          break;
        case 'Between':
          traverse(expr.expr);
          traverse(expr.low);
          traverse(expr.high);
          break;
      }
    };

    columns.forEach(col => traverse(col.expr));
    return Array.from(fields);
  }

  private updateParentReferences(
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    oldNodeId: QueryDAGNodeId,
    newNodeId: QueryDAGNodeId
  ): void {
    for (const node of Array.from(nodes.values())) {
      if ('input' in node && node.input === oldNodeId) {
        (node as any).input = newNodeId;
      }
      if ('left' in node && node.left === oldNodeId) {
        (node as any).left = newNodeId;
      }
      if ('right' in node && node.right === oldNodeId) {
        (node as any).right = newNodeId;
      }
    }
  }

  private findScanNode(
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    nodeId: QueryDAGNodeId
  ): QueryDAGNode & { kind: 'Scan' } | null {
    const node = nodes.get(nodeId);
    if (!node) return null;

    if (node.kind === 'Scan') {
      return node as QueryDAGNode & { kind: 'Scan' };
    }

    // Traverse through simple operators to find scan
    if ('input' in node) {
      return this.findScanNode(nodes, node.input);
    }

    return null;
  }
}
