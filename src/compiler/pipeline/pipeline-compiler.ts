import type {
  PipelineGraph,
  PipelineNode,
  PipelineEdge,
  NodeId,
  EdgeId,
} from '../../core/graph/index.js';
import type {
  PipelineIntent,
  PipelineStepIntent,
  PipelineIntentValidationError,
} from './pipeline-intent.js';
import type { SchemaConfig } from '../schema/schema-config.js';
import type { QueryIntent } from '../query/query-intent.js';
import { MODELS } from '../../config/models.js';
import type {
  QueryPayload,
  TransformPayload,
  LLMPayload,
  ConditionalPayload,
  LoopPayload,
  MergePayload,
  ParallelPayload,
  InputPayload,
  OutputPayload,
} from '../../nodes/payloads.js';
import type { ExprAST } from '../../core/ast/expr-ast.js';
import type { RowSchema } from '../../core/types/schema.js';
import type { EngineType } from '../../core/types/engine-type.js';

export class PipelineCompiler {
  constructor(private schema: SchemaConfig) {}

  compile(intent: PipelineIntent): {
    graph: PipelineGraph;
    errors: PipelineIntentValidationError[];
  } {
    const errors = this.validate(intent);

    if (errors.length > 0) {
      return {
        graph: this.buildEmptyGraph(intent),
        errors,
      };
    }

    const nodes = this.buildNodes(intent);
    const edges = this.buildEdges(intent, nodes);
    const graph = this.buildGraph(nodes, edges, intent);

    return { graph, errors };
  }

  private validate(intent: PipelineIntent): PipelineIntentValidationError[] {
    const errors: PipelineIntentValidationError[] = [];
    const stepIds = new Set(intent.steps.map(s => s.id));

    // Check for duplicate step ids
    const seenIds = new Set<string>();
    for (const step of intent.steps) {
      if (seenIds.has(step.id)) {
        errors.push({
          stepId: step.id,
          code: 'DUPLICATE_STEP_ID',
          message: `Duplicate step id: ${step.id}`,
        });
      }
      seenIds.add(step.id);
    }

    // Check all dependsOn references exist
    for (const step of intent.steps) {
      if (step.dependsOn) {
        for (const dep of step.dependsOn) {
          if (!stepIds.has(dep)) {
            errors.push({
              stepId: step.id,
              code: 'INVALID_DEPENDENCY',
              message: `Dependency '${dep}' does not exist`,
            });
          }
        }
      }
    }

    // Check all branch references exist
    for (const step of intent.steps) {
      if (step.kind === 'conditional') {
        if (!step.trueBranch) {
          errors.push({
            stepId: step.id,
            code: 'MISSING_TRUE_BRANCH',
            message: 'Conditional step missing trueBranch',
          });
        } else if (!stepIds.has(step.trueBranch)) {
          errors.push({
            stepId: step.id,
            code: 'INVALID_TRUE_BRANCH',
            message: `trueBranch '${step.trueBranch}' does not exist`,
          });
        }

        if (!step.falseBranch) {
          errors.push({
            stepId: step.id,
            code: 'MISSING_FALSE_BRANCH',
            message: 'Conditional step missing falseBranch',
          });
        } else if (!stepIds.has(step.falseBranch)) {
          errors.push({
            stepId: step.id,
            code: 'INVALID_FALSE_BRANCH',
            message: `falseBranch '${step.falseBranch}' does not exist`,
          });
        }

        if (step.mergeStep && !stepIds.has(step.mergeStep)) {
          errors.push({
            stepId: step.id,
            code: 'INVALID_MERGE_STEP',
            message: `mergeStep '${step.mergeStep}' does not exist`,
          });
        }
      }
    }

    // Check loop references
    for (const step of intent.steps) {
      if (step.kind === 'loop') {
        if (!step.loopBody || step.loopBody.length === 0) {
          errors.push({
            stepId: step.id,
            code: 'MISSING_LOOP_BODY',
            message: 'Loop step missing loopBody or loopBody is empty',
          });
        } else {
          for (const bodyStepId of step.loopBody) {
            if (!stepIds.has(bodyStepId)) {
              errors.push({
                stepId: step.id,
                code: 'INVALID_LOOP_BODY',
                message: `loopBody step '${bodyStepId}' does not exist`,
              });
            }
          }
        }
      }
    }

    // Check merge references
    for (const step of intent.steps) {
      if (step.kind === 'merge') {
        if (!step.mergeFrom || step.mergeFrom.length < 2) {
          errors.push({
            stepId: step.id,
            code: 'INVALID_MERGE_FROM',
            message: 'Merge step must have at least 2 mergeFrom sources',
          });
        } else {
          for (const fromId of step.mergeFrom) {
            if (!stepIds.has(fromId)) {
              errors.push({
                stepId: step.id,
                code: 'INVALID_MERGE_SOURCE',
                message: `mergeFrom step '${fromId}' does not exist`,
              });
            }
          }
        }
      }
    }

    // Check parallel references
    for (const step of intent.steps) {
      if (step.kind === 'parallel') {
        if (!step.parallelBranches || step.parallelBranches.length === 0) {
          errors.push({
            stepId: step.id,
            code: 'MISSING_PARALLEL_BRANCHES',
            message: 'Parallel step missing parallelBranches',
          });
        } else {
          for (const branchId of step.parallelBranches) {
            if (!stepIds.has(branchId)) {
              errors.push({
                stepId: step.id,
                code: 'INVALID_PARALLEL_BRANCH',
                message: `parallelBranch '${branchId}' does not exist`,
              });
            }
          }
        }
      }
    }

    // Check for cycles in dependsOn graph
    const cycleErrors = this.detectCycles(intent);
    errors.push(...cycleErrors);

    return errors;
  }

  private detectCycles(intent: PipelineIntent): PipelineIntentValidationError[] {
    const errors: PipelineIntentValidationError[] = [];
    const visited = new Set<string>();
    const recursionStack = new Set<string>();

    const stepMap = new Map(intent.steps.map(s => [s.id, s]));

    const visit = (stepId: string): boolean => {
      if (recursionStack.has(stepId)) {
        errors.push({
          stepId,
          code: 'CYCLE_DETECTED',
          message: `Cycle detected in dependency graph involving step '${stepId}'`,
        });
        return true;
      }

      if (visited.has(stepId)) {
        return false;
      }

      visited.add(stepId);
      recursionStack.add(stepId);

      const step = stepMap.get(stepId);
      if (step?.dependsOn) {
        for (const dep of step.dependsOn) {
          visit(dep);
        }
      }

      recursionStack.delete(stepId);
      return false;
    };

    for (const step of intent.steps) {
      visit(step.id);
    }

    return errors;
  }

  private buildNodes(intent: PipelineIntent): Map<NodeId, PipelineNode> {
    const nodes = new Map<NodeId, PipelineNode>();

    // Add input node
    nodes.set('_input', {
      id: '_input',
      kind: 'input',
      label: 'Pipeline Input',
      payload: {
        schema: { columns: [] },
        source: { kind: 'static', data: { schema: { columns: [] }, rows: [{}] } },
      } as InputPayload,
      errorPolicy: { onError: 'fail' as const },
    });

    // Add output node
    nodes.set('_output', {
      id: '_output',
      kind: 'output',
      label: 'Pipeline Output',
      payload: {
        outputKey: 'result',
      } as OutputPayload,
      errorPolicy: { onError: 'fail' as const },
    });

    // Build step nodes
    for (const step of intent.steps) {
      const node = this.buildStepNode(step);
      nodes.set(step.id, node);
    }

    return nodes;
  }

  private buildStepNode(step: PipelineStepIntent): PipelineNode {
    const baseNode = {
      id: step.id,
      kind: step.kind,
      label: step.description,
      errorPolicy: { onError: 'fail' as const },
    };

    let payload: unknown;

    switch (step.kind) {
      case 'query': {
        const placeholderQuery: QueryIntent = {
          table: '',
          columns: [],
        };
        payload = {
          intent: placeholderQuery,
          datasource: 'default',
        } as QueryPayload;
        break;
      }

      case 'transform': {
        payload = {
          operations: [],
        } as TransformPayload;
        break;
      }

      case 'llm': {
        const outputSchema: RowSchema = {
          columns:
            step.outputFields?.map(f => ({
              name: f,
              type: { kind: 'any' } as EngineType,
              nullable: true,
            })) ?? [],
        };
        payload = {
          model: MODELS.PIPELINE_COMPILER,
          userPrompt: {
            parts: [{ kind: 'literal', text: step.description }],
          },
          outputSchema,
          maxTokens: 1000,
        } as LLMPayload;
        break;
      }

      case 'conditional': {
        const placeholderPredicate: ExprAST = {
          kind: 'Literal',
          value: true,
          type: { kind: 'boolean' },
        };
        payload = {
          predicate: placeholderPredicate,
        } as ConditionalPayload;
        break;
      }

      case 'loop': {
        const placeholderOver: ExprAST = {
          kind: 'VarRef',
          name: 'input',
        };
        payload = {
          mode: step.loopMode ?? 'forEach',
          over: placeholderOver,
          iterVar: 'item',
          maxIterations: step.maxIterations ?? 100,
          accumulator: { kind: 'collect' },
        } as LoopPayload;
        break;
      }

      case 'merge': {
        payload = {
          strategy: step.mergeStrategy ?? 'union',
          waitForAll: false,
        } as MergePayload;
        break;
      }

      case 'parallel': {
        payload = {
          maxConcurrency: step.maxConcurrency ?? 3,
        } as ParallelPayload;
        break;
      }

      case 'http': {
        payload = {
          url: { parts: [{ kind: 'literal', text: '' }] },
          method: 'GET',
          outputSchema: { kind: 'any' },
        };
        break;
      }

      default: {
        payload = {};
        break;
      }
    }

    return {
      ...baseNode,
      payload,
    };
  }

  private buildEdges(
    intent: PipelineIntent,
    nodes: Map<NodeId, PipelineNode>,
  ): Map<EdgeId, PipelineEdge> {
    const edges = new Map<EdgeId, PipelineEdge>();
    const stepIds = new Set(intent.steps.map(s => s.id));

    // Track which steps are referenced as branch targets
    const branchTargets = new Set<string>();
    const loopBodySteps = new Set<string>();
    const mergeSources = new Set<string>();
    const parallelBranches = new Set<string>();

    for (const step of intent.steps) {
      if (step.kind === 'conditional') {
        if (step.trueBranch) branchTargets.add(step.trueBranch);
        if (step.falseBranch) branchTargets.add(step.falseBranch);
      }
      if (step.kind === 'loop' && step.loopBody) {
        for (const bodyId of step.loopBody) {
          loopBodySteps.add(bodyId);
        }
      }
      if (step.kind === 'merge' && step.mergeFrom) {
        for (const fromId of step.mergeFrom) {
          mergeSources.add(fromId);
        }
      }
      if (step.kind === 'parallel' && step.parallelBranches) {
        for (const branchId of step.parallelBranches) {
          parallelBranches.add(branchId);
        }
      }
    }

    // Build edges for each step
    const entrySteps = this.findEntrySteps(intent, loopBodySteps);

    for (const step of intent.steps) {
      const isBranchTarget = branchTargets.has(step.id);
      const isLoopBody = loopBodySteps.has(step.id);
      const isMergeSource = mergeSources.has(step.id);
      const isParallelBranch = parallelBranches.has(step.id);
      const isEntryStep = entrySteps.has(step.id);

      // Add edge from _input to entry steps
      if (isEntryStep) {
        const edgeId = `e__input_${step.id}`;
        edges.set(edgeId, {
          id: edgeId,
          from: '_input',
          to: step.id,
          kind: 'data',
        });
      }

      // Add edges for dependencies
      if (step.dependsOn) {
        for (const dep of step.dependsOn) {
          const edgeId = `e_${dep}_${step.id}`;
          edges.set(edgeId, {
            id: edgeId,
            from: dep,
            to: step.id,
            kind: 'data',
          });
        }
      }

      // Add conditional edges
      if (step.kind === 'conditional') {
        if (step.trueBranch) {
          const controlEdgeId = `e_${step.id}_${step.trueBranch}_control_true`;
          edges.set(controlEdgeId, {
            id: controlEdgeId,
            from: step.id,
            to: step.trueBranch,
            kind: 'control',
            condition: 'true',
          });

          const dataEdgeId = `e_${step.id}_${step.trueBranch}_data`;
          edges.set(dataEdgeId, {
            id: dataEdgeId,
            from: step.id,
            to: step.trueBranch,
            kind: 'data',
          });
        }

        if (step.falseBranch) {
          const controlEdgeId = `e_${step.id}_${step.falseBranch}_control_false`;
          edges.set(controlEdgeId, {
            id: controlEdgeId,
            from: step.id,
            to: step.falseBranch,
            kind: 'control',
            condition: 'false',
          });

          const dataEdgeId = `e_${step.id}_${step.falseBranch}_data`;
          edges.set(dataEdgeId, {
            id: dataEdgeId,
            from: step.id,
            to: step.falseBranch,
            kind: 'data',
          });
        }
      }

      // Add loop edges
      if (step.kind === 'loop' && step.loopBody) {
        for (const bodyStepId of step.loopBody) {
          // First body step gets data from loop node
          const edgeId = `e_${step.id}_${bodyStepId}`;
          edges.set(edgeId, {
            id: edgeId,
            from: step.id,
            to: bodyStepId,
            kind: 'data',
          });
        }
      }

      // Add merge edges
      if (step.kind === 'merge' && step.mergeFrom) {
        for (const fromId of step.mergeFrom) {
          const edgeId = `e_${fromId}_${step.id}`;
          edges.set(edgeId, {
            id: edgeId,
            from: fromId,
            to: step.id,
            kind: 'data',
          });
        }
      }

      // Add parallel edges
      if (step.kind === 'parallel' && step.parallelBranches) {
        for (const branchId of step.parallelBranches) {
          const edgeId = `e_${step.id}_${branchId}`;
          edges.set(edgeId, {
            id: edgeId,
            from: step.id,
            to: branchId,
            kind: 'data',
          });
        }
      }
    }

    // Find exit steps and connect to _output
    const exitSteps = this.findExitSteps(intent, loopBodySteps, mergeSources);
    for (const exitStepId of exitSteps) {
      const edgeId = `e_${exitStepId}__output`;
      edges.set(edgeId, {
        id: edgeId,
        from: exitStepId,
        to: '_output',
        kind: 'data',
      });
    }

    return edges;
  }

  private getBodyStepIds(intent: PipelineIntent): Set<string> {
    const bodyIds = new Set<string>();
    for (const step of intent.steps) {
      if (step.loopBody) {
        for (const id of step.loopBody) {
          bodyIds.add(id);
        }
      }
      if (step.parallelBranches) {
        for (const id of step.parallelBranches) {
          bodyIds.add(id);
        }
      }
      // Branch steps of conditionals are NOT excluded — they connect to merge
      // which connects to _output
    }
    return bodyIds;
  }

  private findExitSteps(
    intent: PipelineIntent,
    loopBodySteps: Set<string>,
    mergeSources: Set<string>,
  ): string[] {
    const bodyStepIds = this.getBodyStepIds(intent);

    // Steps that are referenced as a dependency by other non-body steps
    const referencedIds = new Set<string>();
    for (const step of intent.steps) {
      if (bodyStepIds.has(step.id)) continue; // skip body steps
      for (const dep of step.dependsOn ?? []) {
        referencedIds.add(dep);
      }
      // merge steps reference their inputs
      for (const from of step.mergeFrom ?? []) {
        referencedIds.add(from);
      }
    }

    // Exit steps = non-body steps not referenced by any other non-body step
    // Also exclude merge sources (merge handles its own flow)
    return intent.steps
      .filter(
        s =>
          !bodyStepIds.has(s.id) &&
          !referencedIds.has(s.id) &&
          !mergeSources.has(s.id),
      )
      .map(s => s.id);
  }

  private findEntrySteps(intent: PipelineIntent, loopBodySteps: Set<string>): Set<string> {
    const bodyStepIds = this.getBodyStepIds(intent);
    return new Set(
      intent.steps
        .filter(
          s => !bodyStepIds.has(s.id) && (!s.dependsOn || s.dependsOn.length === 0),
        )
        .map(s => s.id),
    );
  }

  private buildGraph(
    nodes: Map<NodeId, PipelineNode>,
    edges: Map<EdgeId, PipelineEdge>,
    intent: PipelineIntent,
  ): PipelineGraph {
    return {
      id: `pipeline_${Date.now()}`,
      version: 1,
      nodes,
      edges,
      entryNode: '_input',
      exitNodes: ['_output'],
      metadata: {
        description: intent.description,
        createdAt: Date.now(),
        tags: [],
        budget: intent.budget ?? {},
      },
    };
  }

  private buildEmptyGraph(intent: PipelineIntent): PipelineGraph {
    return {
      id: `pipeline_${Date.now()}`,
      version: 1,
      nodes: new Map(),
      edges: new Map(),
      entryNode: '_input',
      exitNodes: ['_output'],
      metadata: {
        description: intent.description,
        createdAt: Date.now(),
        tags: [],
        budget: intent.budget ?? {},
      },
    };
  }
}
