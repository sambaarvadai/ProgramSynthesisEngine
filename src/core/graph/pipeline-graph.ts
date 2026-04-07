// Defines pipeline graph structure and graph operations

import type { ExecutionBudget } from '../context/execution-budget.js';
import type { PipelineNode } from './node.js';
import type { PipelineEdge } from './edge.js';

export interface PipelineMetadata {
  createdAt: number;
  description: string;
  tags: string[];
  budget: Partial<ExecutionBudget>;
}

export interface PipelineGraph {
  id: string;
  version: number;
  nodes: Map<string, PipelineNode>;
  edges: Map<string, PipelineEdge>;
  entryNode: string;
  exitNodes: string[];
  metadata: PipelineMetadata;
}

export interface PipelineGraphJSON {
  id: string;
  version: number;
  nodes: Record<string, PipelineNode>;
  edges: Record<string, PipelineEdge>;
  entryNode: string;
  exitNodes: string[];
  metadata: PipelineMetadata;
}

export function serializeGraph(graph: PipelineGraph): PipelineGraphJSON {
  return {
    id: graph.id,
    version: graph.version,
    nodes: Object.fromEntries(graph.nodes),
    edges: Object.fromEntries(graph.edges),
    entryNode: graph.entryNode,
    exitNodes: graph.exitNodes,
    metadata: graph.metadata,
  };
}

export function deserializeGraph(json: PipelineGraphJSON): PipelineGraph {
  return {
    id: json.id,
    version: json.version,
    nodes: new Map(Object.entries(json.nodes)),
    edges: new Map(Object.entries(json.edges)),
    entryNode: json.entryNode,
    exitNodes: json.exitNodes,
    metadata: json.metadata,
  };
}
