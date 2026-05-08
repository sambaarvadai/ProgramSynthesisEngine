import type { PlanResult } from '../pipeline-engine.js';

export function serializePlan(plan: PlanResult): object {
  
  // graph.nodes is a Map<string, GraphNode>
  // Serialize as a plain object { id: node } for safe JSON roundtrip
  const nodesObj: Record<string, any> = {};
  for (const [id, node] of plan.graph.nodes.entries()) {
    nodesObj[id] = node;
  }

  // Detect edges type from live graph
  let edgesSerialized: any[] = [];

  if (plan.graph.edges instanceof Map) {
    // Serialize Map as array of [key, value] pairs
    edgesSerialized = [...plan.graph.edges.entries()];
    console.log('[PlanSerializer] Edges serialized as Map entries, count:',
      plan.graph.edges.size);
  } else if (Array.isArray(plan.graph.edges)) {
    edgesSerialized = plan.graph.edges;
    console.log('[PlanSerializer] Edges serialized as array, count:',
      (plan.graph.edges as any[]).length);
  } else if (plan.graph.edges && typeof plan.graph.edges === 'object') {
    // Plain object or Set
    edgesSerialized = Object.entries(plan.graph.edges) as any[];
    console.log('[PlanSerializer] Edges serialized as object entries');
  }

  const serialized = {
    intent: plan.intent,
    graph: {
      id:        plan.graph.id,
      version:   plan.graph.version,
      entryNode: plan.graph.entryNode,
      metadata:  plan.graph.metadata,
      exitNodes: plan.graph.exitNodes,
      nodes:     nodesObj,      // plain object, not array
      edges:     edgesSerialized
    },
    compilationErrors: plan.compilationErrors ?? [],
    intentRaw: plan.intentRaw,
    userId: plan.userId
  };

  console.log('[PlanSerializer] Graph node keys:', Object.keys(nodesObj));
  console.log('[PlanSerializer] Edge count:', edgesSerialized.length);

  return serialized;
}

export function deserializePlan(serialized: any): PlanResult {
  
  const nodes = new Map<string, any>();
  
  // nodes is now a plain object { id: node } — iterate entries
  const nodesObj = serialized.graph.nodes ?? {};
  
  for (const [id, node] of Object.entries(nodesObj)) {
    nodes.set(id, node);
  }

  console.log('[PlanSerializer] Deserialized node keys:', [...nodes.keys()]);

  console.log('[PlanSerializer] Edges type after deserialize:',
    typeof serialized.graph.edges);
  console.log('[PlanSerializer] Edges is array:',
    Array.isArray(serialized.graph.edges));
  console.log('[PlanSerializer] Edges sample:',
    JSON.stringify(serialized.graph.edges).slice(0, 200));

  // Reconstruct edges as a Map to match live graph structure
  const edgesMap = new Map<string, any>();
  for (const edge of (serialized.graph.edges ?? [])) {
    // edges were serialized as array of [key, value] pairs from Map.entries()
    // OR as array of edge objects (legacy format)
    if (Array.isArray(edge)) {
      // [key, value] pair from Map.entries()
      edgesMap.set(edge[0], edge[1]);
    } else if (edge.id) {
      // edge object with id field
      edgesMap.set(edge.id, edge);
    } else {
      // fallback: generate key from from/to
      const key = `${edge.from}->${edge.to}`;
      edgesMap.set(key, edge);
    }
  }

  console.log('[PlanSerializer] Reconstructed edges map size:', edgesMap.size);

  // Validate that _input and _output exist
  // If missing, the plan was serialized before system nodes were added
  // The Scheduler adds them — check if they're needed
  if (!nodes.has('_input') || !nodes.has('_output')) {
    console.warn(
      '[PlanSerializer] System nodes _input/_output missing from cache. ' +
      'These will be added by the Scheduler on execute.'
    );
    // Note: if Scheduler requires them upfront, we need to add them here
    // Check scheduler.ts to see if it validates before adding system nodes
  }

  return {
    intent: serialized.intent,
    graph: {
      id:        serialized.graph.id,
      version:   serialized.graph.version ?? 1,
      entryNode: serialized.graph.entryNode ?? '',
      metadata:  serialized.graph.metadata ?? {},
      exitNodes: serialized.graph.exitNodes,
      nodes,
      edges:     edgesMap    // Map instead of array
    },
    compilationErrors: serialized.compilationErrors ?? [],
    intentRaw: serialized.intentRaw ?? '',
    userId: serialized.userId
  };
}
