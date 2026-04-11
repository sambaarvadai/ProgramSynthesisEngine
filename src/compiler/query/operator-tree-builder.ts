import type { StorageBackend } from '../../core/storage/storage-backend.js';
import type { TempStore } from '../../core/storage/temp-store.js';
import type { ExprEvaluator } from '../../executors/expr-evaluator.js';
import type { QueryPlan, QueryDAGNode, QueryDAGNodeId } from '../query-ast/query-ast.js';
import type { PhysicalOperator } from '../../executors/physical-operator.js';
import { ScanOperator } from '../../executors/operators/scan-operator.js';
import { FilterOperator } from '../../executors/operators/filter-operator.js';
import { JoinOperator } from '../../executors/operators/join-operator.js';
import { AggOperator } from '../../executors/operators/agg-operator.js';
import { ProjectOperator } from '../../executors/operators/project-operator.js';
import { SortOperator } from '../../executors/operators/sort-operator.js';
import { LimitOperator } from '../../executors/operators/limit-operator.js';

export class OperatorTreeBuilder {
  constructor(
    private backend: StorageBackend,
    private tempStore: TempStore,
    private evaluator: ExprEvaluator,
    private batchSize: number = 100
  ) {}

  build(plan: QueryPlan): PhysicalOperator {
    return this.buildNode(plan.root, plan.nodes, new Set());
  }

  private buildNode(
    nodeId: QueryDAGNodeId,
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    visited: Set<QueryDAGNodeId>
  ): PhysicalOperator {
    // Cycle detection
    if (visited.has(nodeId)) {
      throw new Error(`Cycle detected in QueryDAG at node ${nodeId}`);
    }
    visited.add(nodeId);

    const node = nodes.get(nodeId);
    if (!node) {
      throw new Error(`Node ${nodeId} not found in query plan`);
    }

    let result: PhysicalOperator;
    switch (node.kind) {
      case 'Scan':
        result = this.buildScanNode(node);
        break;
      
      case 'Filter':
        result = this.buildFilterNode(node, nodes, visited);
        break;
      
      case 'Join':
        result = this.buildJoinNode(node, nodes, visited);
        break;
      
      case 'Agg':
        result = this.buildAggNode(node, nodes, visited);
        break;
      
      case 'Project':
        result = this.buildProjectNode(node, nodes, visited);
        break;
      
      case 'Sort':
        result = this.buildSortNode(node, nodes, visited);
        break;
      
      case 'Limit':
        result = this.buildLimitNode(node, nodes, visited);
        break;
      
      default:
        const _exhaustiveCheck: never = node;
        throw new Error(`Unknown node kind: ${_exhaustiveCheck}`);
    }

    // Remove from visited set after building (allows DAG sharing)
    visited.delete(nodeId);
    return result;
  }

  private buildScanNode(node: QueryDAGNode & { kind: 'Scan' }): PhysicalOperator {
    return new ScanOperator({
      table: node.payload.table,
      alias: node.payload.alias,
      schema: node.payload.schema,
      predicate: node.payload.predicate,
      batchSize: this.batchSize,
      backend: this.backend
    });
  }

  private buildFilterNode(
    node: QueryDAGNode & { kind: 'Filter' },
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    visited: Set<QueryDAGNodeId>
  ): PhysicalOperator {
    const input = this.buildNode(node.input, nodes, visited);
    
    return new FilterOperator({
      input,
      predicate: node.predicate,
      evaluator: this.evaluator
    });
  }

  private buildJoinNode(
    node: QueryDAGNode & { kind: 'Join' },
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    visited: Set<QueryDAGNodeId>
  ): PhysicalOperator {
    const left = this.buildNode(node.left, nodes, visited);
    const right = this.buildNode(node.right, nodes, visited);
    
    return new JoinOperator({
      left,
      right,
      on: node.payload.on,
      kind: node.payload.kind,
      evaluator: this.evaluator,
      tempStore: this.tempStore,
      batchSize: this.batchSize
    });
  }

  private buildAggNode(
    node: QueryDAGNode & { kind: 'Agg' },
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    visited: Set<QueryDAGNodeId>
  ): PhysicalOperator {
    const input = this.buildNode(node.input, nodes, visited);
    
    return new AggOperator({
      input,
      groupBy: node.keys,
      aggregations: node.aggregations,
      evaluator: this.evaluator,
      tempStore: this.tempStore,
      batchSize: this.batchSize
    });
  }

  private buildProjectNode(
    node: QueryDAGNode & { kind: 'Project' },
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    visited: Set<QueryDAGNodeId>
  ): PhysicalOperator {
    const input = this.buildNode(node.input, nodes, visited);
    
    return new ProjectOperator({
      input,
      projections: node.columns,
      evaluator: this.evaluator
    });
  }

  private buildSortNode(
    node: QueryDAGNode & { kind: 'Sort' },
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    visited: Set<QueryDAGNodeId>
  ): PhysicalOperator {
    const input = this.buildNode(node.input, nodes, visited);
    
    return new SortOperator({
      input,
      keys: node.keys,
      evaluator: this.evaluator,
      tempStore: this.tempStore,
      memoryLimitRows: 10000
    });
  }

  private buildLimitNode(
    node: QueryDAGNode & { kind: 'Limit' },
    nodes: Map<QueryDAGNodeId, QueryDAGNode>,
    visited: Set<QueryDAGNodeId>
  ): PhysicalOperator {
    const input = this.buildNode(node.input, nodes, visited);
    
    return new LimitOperator({
      input,
      limit: node.count,
      offset: node.offset
    });
  }
}
