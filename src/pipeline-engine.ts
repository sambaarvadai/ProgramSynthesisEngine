import {
  NodeRegistry,
  FunctionRegistry,
  Scheduler,
  registerAllNodes,
  QueryExecutor,
  ExprEvaluator,
  StorageBackend,
  TempStore,
  createExecutionContext,
  ExecutionBudget,
  ExecutionResult,
  Value,
} from './index.js';
import { PostgresBackend, SQLiteTempStore } from './storage/index.js';
import { stripHallucinatedFieldRefs } from './compiler/pipeline/expr-sanitizer.js';
import { PipelineIntentGenerator } from './compiler/pipeline/index.js';
import { PipelineCompiler } from './compiler/pipeline/index.js';
import { QueryIntentGenerator as QueryIntentGeneratorClass, TablePreSelector } from './compiler/query/index.js';
import { registerBuiltinFunctions } from './functions/index.js';
import Anthropic from '@anthropic-ai/sdk';
import type {
  PipelineIntent,
  PipelineStepIntent,
  PipelineIntentValidationError,
} from './compiler/pipeline/index.js';
import type {
  PipelineGraph,
  PipelineNode,
} from './core/graph/index.js';
import type { SchemaConfig } from './compiler/schema/index.js';
import type { QueryPayload, TransformPayload, ConditionalPayload, LLMPayload } from './nodes/payloads.js';
import { MODELS } from './config/models.js';

export type PipelineEngineConfig = {
  anthropicApiKey: string;
  postgresUrl?: string;
  schema?: SchemaConfig;
  storageBackend?: StorageBackend;
  budget?: Partial<ExecutionBudget>;
  maxParallelBranches?: number;
  defaultBatchSize?: number;
};

export type PlanResult = {
  intent: PipelineIntent;
  graph: PipelineGraph;
  compilationErrors: PipelineIntentValidationError[];
  intentRaw: string;
};

export type RunResult = {
  plan: PlanResult;
  execution: ExecutionResult;
  durationMs: number;
};

export class PipelineCompilationError extends Error {
  errors: PipelineIntentValidationError[];

  constructor(errors: PipelineIntentValidationError[]) {
    const messages = errors
      .map(e => `[${e.code}] ${e.stepId ? `${e.stepId}: ` : ''}${e.message}`)
      .join('\n');
    super(`Pipeline compilation failed:\n${messages}`);
    this.name = 'PipelineCompilationError';
    this.errors = errors;
  }
}

export class PipelineEngine {
  public generator: PipelineIntentGenerator;
  public compiler: PipelineCompiler;
  private queryIntentGenerator: QueryIntentGeneratorClass;
  private client: Anthropic;
  private scheduler: Scheduler;
  private nodeRegistry: NodeRegistry;
  private fnRegistry: FunctionRegistry;
  private evaluator: ExprEvaluator;
  private queryExecutor: QueryExecutor;
  private storageBackend: StorageBackend;
  private tempStore: TempStore;

  constructor(private config: PipelineEngineConfig) {
    // Initialize Anthropic client for transform enrichment
    this.client = new Anthropic({ apiKey: config.anthropicApiKey });

    // Initialize generator
    this.generator = new PipelineIntentGenerator({
      anthropicApiKey: config.anthropicApiKey,
      schema: config.schema,
    });

    // Initialize compiler
    this.compiler = new PipelineCompiler(
      config.schema ?? { tables: new Map(), foreignKeys: [], version: '1' },
    );

    // Initialize query intent generator
    const preSelector = new TablePreSelector({
      anthropicApiKey: config.anthropicApiKey,
      maxTables: 5,
    });
    this.queryIntentGenerator = new QueryIntentGeneratorClass({
      anthropicApiKey: config.anthropicApiKey,
      preSelector,
    });

    // Initialize registries
    this.nodeRegistry = new NodeRegistry();
    this.fnRegistry = new FunctionRegistry();
    registerBuiltinFunctions(this.fnRegistry);

    // Initialize evaluator
    this.evaluator = new ExprEvaluator(this.fnRegistry);

    // Register nodes with dependencies
    registerAllNodes(this.nodeRegistry, {
      anthropicApiKey: config.anthropicApiKey,
    });

    // Initialize storage backend
    this.storageBackend =
      config.storageBackend ??
      (config.postgresUrl
        ? new PostgresBackend(config.postgresUrl)
        : new PostgresBackend('postgresql://localhost:5432/default'));

    // Initialize temp store
    this.tempStore = new SQLiteTempStore(':memory:');

    // Initialize query executor
    this.queryExecutor = new QueryExecutor({
      schema: config.schema ?? { tables: new Map(), foreignKeys: [], version: '1' },
      backend: this.storageBackend,
      tempStore: this.tempStore,
      evaluator: this.evaluator,
      batchSize: config.defaultBatchSize || 100,
    });

    // Initialize scheduler
    this.scheduler = new Scheduler({
      nodeRegistry: this.nodeRegistry,
      queryExecutor: this.queryExecutor,
      evaluator: this.evaluator,
      maxParallelBranches: config.maxParallelBranches || 4,
      defaultBatchSize: config.defaultBatchSize || 100,
    });
  }

  async initialize(): Promise<void> {
    await this.storageBackend.connect();
  }

  async dispose(): Promise<void> {
    await this.storageBackend.disconnect();
    await this.tempStore.clear();
    (this.tempStore as any).close();
  }

  async plan(
    description: string,
    params?: Record<string, string>,
  ): Promise<PlanResult> {
    const { intent, raw: intentRaw } = await this.generator.generate(
      description,
      {
        availableParams: params,
      },
    );

    const { graph, errors: compilationErrors } = this.compiler.compile(intent);

    if (compilationErrors.length === 0) {
      await this.enrichNodes(
        graph,
        intent,
        this.config.schema ?? { tables: new Map(), foreignKeys: [], version: '1' },
      );
    }

    return {
      intent,
      graph,
      compilationErrors,
      intentRaw,
    };
  }

  async run(
    description: string,
    params?: Record<string, Value>,
  ): Promise<RunResult> {
    const plan = await this.plan(description);

    if (plan.compilationErrors.length > 0) {
      throw new PipelineCompilationError(plan.compilationErrors);
    }

    return await this.execute(plan, params);
  }

  async planAndConfirm(
    description: string,
    params?: Record<string, string>,
  ): Promise<PlanResult> {
    return await this.plan(description, params);
  }

  async execute(
    plan: PlanResult,
    params?: Record<string, Value>,
  ): Promise<RunResult> {
    if (plan.compilationErrors.length > 0) {
      throw new PipelineCompilationError(plan.compilationErrors);
    }

    // Merge budget with correct precedence: defaults < plan.intent.budget < config.budget (user always wins)
    const budget: Partial<ExecutionBudget> = {
      // Sensible defaults
      maxLLMCalls: 20,
      maxIterations: 1000,
      timeoutMs: 300000,
      maxRowsPerNode: 10000,
      maxMemoryMB: 512,
      maxBatchSize: 100,
      // LLM-generated hints (lower priority than user config)
      ...plan.intent.budget,
      // User config always wins
      ...(this.config.budget ?? {}),
      // These must never be overridden — always reset at execution start
      llmCallsUsed: 0,
      iterationsUsed: 0,
      startedAt: Date.now(),
    };

    // Create execution context
    const ctx = createExecutionContext({
      pipelineId: plan.graph.id,
      sessionId: crypto.randomUUID(),
      params: params || {},
      budget,
    });

    const startTime = Date.now();
    const execution = await this.scheduler.execute(plan.graph, ctx);
    const durationMs = Date.now() - startTime;

    return {
      plan,
      execution,
      durationMs,
    };
  }

  formatPlan(plan: PlanResult): string {
    const lines: string[] = [];

    lines.push(`Pipeline: ${plan.intent.description}`);
    lines.push('');

    if (plan.compilationErrors.length > 0) {
      lines.push('Compilation Errors:');
      for (const error of plan.compilationErrors) {
        lines.push(
          `  [${error.code}] ${error.stepId ? `${error.stepId}: ` : ''}${error.message}`,
        );
      }
      lines.push('');
    }

    lines.push('Steps:');
    for (let i = 0; i < plan.intent.steps.length; i++) {
      const step = plan.intent.steps[i];
      const deps = step.dependsOn?.join(', ') || 'none';
      lines.push(
        `  ${i + 1}. [${step.kind}] ${step.id}: ${step.description}`,
      );
      lines.push(`     Depends on: ${deps}`);

      // Show kind-specific info
      if (step.kind === 'conditional') {
        lines.push(`     True branch: ${step.trueBranch}`);
        lines.push(`     False branch: ${step.falseBranch}`);
        if (step.mergeStep) {
          lines.push(`     Merge step: ${step.mergeStep}`);
        }
      }
      if (step.kind === 'loop') {
        lines.push(`     Mode: ${step.loopMode}`);
        lines.push(`     Body: ${step.loopBody?.join(', ')}`);
        lines.push(`     Max iterations: ${step.maxIterations}`);
      }
      if (step.kind === 'merge') {
        lines.push(`     From: ${step.mergeFrom?.join(', ')}`);
        lines.push(`     Strategy: ${step.mergeStrategy}`);
      }
      if (step.kind === 'parallel') {
        lines.push(`     Branches: ${step.parallelBranches?.join(', ')}`);
        lines.push(`     Max concurrency: ${step.maxConcurrency}`);
      }
      if (step.kind === 'llm') {
        lines.push(`     Output fields: ${step.outputFields?.join(', ')}`);
      }
    }

    lines.push('');

    // Estimate LLM calls
    let estimatedLLMCalls = 0;
    for (const step of plan.intent.steps) {
      if (step.kind === 'llm') {
        estimatedLLMCalls++;
      }
    }
    lines.push(`Estimated LLM calls: ${estimatedLLMCalls}`);

    // Show budget if specified
    if (plan.intent.budget) {
      lines.push('Budget limits:');
      if (plan.intent.budget.maxLLMCalls) {
        lines.push(`  Max LLM calls: ${plan.intent.budget.maxLLMCalls}`);
      }
      if (plan.intent.budget.maxIterations) {
        lines.push(`  Max iterations: ${plan.intent.budget.maxIterations}`);
      }
      if (plan.intent.budget.timeoutMs) {
        lines.push(`  Timeout: ${plan.intent.budget.timeoutMs}ms`);
      }
    }

    return lines.join('\n');
  }

  private async enrichNodes(
    graph: PipelineGraph,
    intent: PipelineIntent,
    schema: SchemaConfig,
  ): Promise<void> {
    // Track available fields for each step to prevent hallucination
    const fieldMap = new Map<string, string[]>(); // stepId -> field names

    for (const [nodeId, node] of graph.nodes) {
      const step = intent.steps.find(s => s.id === nodeId);
      if (!step) continue;

      switch (node.kind) {
        case 'query': {
          // Use QueryIntentGenerator to convert step.description → QueryIntent
          const { intent: queryIntent } =
            await this.queryIntentGenerator.generate(step.description, schema);

          // Update the node payload in-place
          (node.payload as QueryPayload).intent = queryIntent;
          (node.payload as QueryPayload).datasource = 'default';

          // Track field names from query output
          const fieldNames = queryIntent.columns.map((c: any) => c.alias || c.field);
          fieldMap.set(step.id, fieldNames);
          break;
        }

        case 'transform': {
          // Generate transform operations from step.description
          if (step.description) {
            const availableFields = this.getAvailableFields(step.id, intent, fieldMap);
            node.payload = await this.enrichTransformNode(step, node.payload as TransformPayload, availableFields);
            
            // Track output fields from transform operations
            const operations = (node.payload as TransformPayload).operations ?? [];
            const addedFields = operations
              .filter(op => op.kind === 'addField')
              .map(op => (op as any).name);
            
            fieldMap.set(step.id, [...availableFields, ...addedFields]);
          }
          break;
        }

        case 'conditional': {
          // Generate predicate ExprAST from step.condition description
          if (step.condition) {
            const availableFields = this.getAvailableFields(step.id, intent, fieldMap);
            node.payload = await this.enrichConditionalNode(step, node.payload as ConditionalPayload, availableFields);
          }
          break;
        }

        case 'llm': {
          // Generate real prompt template from step.description
          node.payload = await this.enrichLLMNode(step, node.payload as LLMPayload);
          break;
        }

        // loop, merge, parallel: structural nodes — no enrichment needed
        // their config comes directly from PipelineStepIntent fields
        default:
          break;
      }
    }
  }

  private getAvailableFields(
    stepId: string,
    intent: PipelineIntent,
    fieldMap: Map<string, string[]>
  ): string[] {
    const step = intent.steps.find(s => s.id === stepId);
    if (!step?.dependsOn) return [];

    const allFields: string[] = [];
    for (const depId of step.dependsOn) {
      const depFields = fieldMap.get(depId) ?? [];
      allFields.push(...depFields);
    }

    // Remove duplicates
    return [...new Set(allFields)];
  }

  private stripHallucinatedFields(
    operations: any[],
    availableFields: string[]
  ): any[] {
    if (!availableFields?.length) return operations;

    const fieldSet = new Set(availableFields);
    const cleaned: any[] = [];

    for (const op of operations) {
      let isValid = true;

      switch (op.kind) {
        case 'filterRows':
          // Check if predicate references unknown fields
          if (this.referencesUnknownField(op.predicate, fieldSet)) {
            console.warn('[Transform] Stripping filterRows operation with unknown field reference');
            isValid = false;
          }
          break;

        case 'addField':
          // Check if expr references unknown fields
          if (this.referencesUnknownField(op.expr, fieldSet)) {
            console.warn('[Transform] Replacing unknown field references in addField with null');
            op.expr = { kind: 'Literal', value: null, type: { kind: 'any' } };
          }
          break;

        case 'removeField':
        case 'renameField':
        case 'castField':
          // These operations should reference existing fields
          if (!fieldSet.has(op.name)) {
            console.warn(`[Transform] Stripping ${op.kind} operation for unknown field: ${op.name}`);
            isValid = false;
          }
          break;

        case 'sortRows':
          // Check sort keys
          if (op.keys?.some((key: any) => this.referencesUnknownField(key.expr, fieldSet))) {
            console.warn('[Transform] Stripping sortRows operation with unknown field reference');
            isValid = false;
          }
          break;

        case 'dedup':
          // Check dedup fields
          if (op.on?.some((field: string) => !fieldSet.has(field))) {
            console.warn('[Transform] Stripping dedup operation with unknown field');
            isValid = false;
          }
          break;
      }

      if (isValid) {
        cleaned.push(op);
      }
    }

    return cleaned;
  }

  private referencesUnknownField(expr: any, availableFields: Set<string>): boolean {
    if (!expr) return false;

    switch (expr.kind) {
      case 'FieldRef':
        return !availableFields.has(expr.field);
      
      case 'BinaryOp':
        return this.referencesUnknownField(expr.left, availableFields) || 
               this.referencesUnknownField(expr.right, availableFields);
      
      case 'Conditional':
        return this.referencesUnknownField(expr.condition, availableFields) ||
               this.referencesUnknownField(expr.then, availableFields) ||
               this.referencesUnknownField(expr.else, availableFields);
      
      case 'FunctionCall':
        return expr.args?.some((arg: any) => this.referencesUnknownField(arg, availableFields));
      
      default:
        return false;
    }
  }

  private async enrichTransformNode(
    step: PipelineStepIntent,
    payload: TransformPayload,
    availableFields: string[],
  ): Promise<TransformPayload> {
    const fieldsContext = availableFields?.length 
      ? `\n\nAvailable fields in the input data: ${availableFields.join(', ')}\nIMPORTANT: Use ONLY these exact field names in FieldRef expressions. Do not invent field names.`
      : '';

    const prompt = `Convert this data transformation description into a JSON array of transform operations.

Description: "${step.description}"${fieldsContext}

Available operation kinds:
- addField: { kind: "addField", name: "field_name", expr: ExprAST }
- removeField: { kind: "removeField", name: "field_name" }
- renameField: { kind: "renameField", from: "old_name", to: "new_name" }
- castField: { kind: "castField", name: "field_name", to: { kind: "string"|"number"|"boolean" } }
- filterRows: { kind: "filterRows", predicate: ExprAST }
- sortRows: { kind: "sortRows", keys: [{ expr: ExprAST, direction: "ASC"|"DESC" }] }
- dedup: { kind: "dedup", on: ["field1", "field2"] }
- limit: { kind: "limit", count: N }

IMPORTANT: If the description mentions "keep only X, Y, Z fields":
- First add any new fields mentioned (like computed fields)
- Then ONLY remove fields that are NOT in the keep list
- DO NOT remove fields that are explicitly mentioned in the keep list
- Example: "keep only name, segment, arr, and tier" means remove fields like 'id', 'created_at', etc. but KEEP name, segment, arr, tier

For steps that just add a null/default field:
Generate ONLY: [{ kind: 'addField', name: '<fieldName>', 
expr: { kind: 'Literal', value: null, type: { kind: 'string' } } }]
Do NOT generate filterRows or complex conditionals for passthrough steps.
Do NOT reference fields that represent values (like 'enterprise' or 'critical')
as FieldRef - those are values to compare against, not field names.

ExprAST types:
- Literal: { kind: "Literal", value: <value>, type: { kind: "string"|"number"|"boolean" } }
- FieldRef: { kind: "FieldRef", field: "field_name" }
- VarRef: { kind: "VarRef", name: "variable_name" }
- BinaryOp: { kind: "BinaryOp", op: "+"|"-"|"*"|"/"|"="|"!="|"<"|">"|"<="|">="|"AND"|"OR", left: ExprAST, right: ExprAST }
- Conditional: { kind: "Conditional", condition: ExprAST, then: ExprAST, else: ExprAST } // For if-then-else expressions like "price > 200 ? 'premium' : 'standard'"
- FunctionCall: { kind: "FunctionCall", name: "UPPER"|"LOWER"|"TRIM"|"LENGTH"|"ROUND"|"ABS", args: [ExprAST] }

IMPORTANT: If this description involves GROUP BY, aggregation (COUNT/SUM/AVG), or joining tables — return an empty array []. These belong in QueryNode, not TransformNode.

Return ONLY a JSON array of operations, no markdown, no explanation.`;

    const response = await this.client.messages.create({
      model: MODELS.LLM_NODE,
      max_tokens: 500,
      messages: [{ role: 'user', content: prompt }],
    });

    const raw = response.content[0].type === 'text' ? response.content[0].text : '[]';
    console.log('[Transform Enrichment LLM Output]', raw);
    const clean = raw.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();

    try {
      const operations = JSON.parse(clean);
      if (!Array.isArray(operations)) return payload;
      
      // Strip hallucinated field references
      const safeOps = stripHallucinatedFieldRefs(operations, availableFields ?? []);
      
      return { ...payload, operations: safeOps };
    } catch {
      return payload; // if parse fails, leave empty (node passes through)
    }
  }

  private async enrichConditionalNode(
    step: PipelineStepIntent,
    payload: ConditionalPayload,
    availableFields?: string[],
  ): Promise<ConditionalPayload> {
    const fieldsContext = availableFields?.length 
    ? `\nAvailable field names: ${availableFields.join(', ')}\nUse ONLY these exact field names in FieldRef. Never use spaces in field names. Never invent field names.` 
    : '';

  const prompt = `Convert this condition description into an ExprAST JSON object.

Condition: "${step.condition}"${fieldsContext}

CRITICAL: field names must be single words matching exactly the
available fields list. 'customer segment' is NOT a valid field name.
'segment' IS valid. 'priority' IS valid. 'customer name' is NOT valid.
'customer_name' IS valid.

Map natural language to exact field names:
'customer segment' -> segment
'customer name' -> customer_name
'ticket priority' -> priority
'ticket subject' -> subject

ExprAST types:
- Literal: { kind: "Literal", value: <value>, type: { kind: "string"|"number"|"boolean" } }
- FieldRef: { kind: "FieldRef", field: "field_name" }
- VarRef: { kind: "VarRef", name: "variable_name" }
- BinaryOp: { kind: "BinaryOp", op: "="|"!="|"<"|">"|"<="|">="|"AND"|"OR", left: ExprAST, right: ExprAST }
- Conditional: { kind: "Conditional", condition: ExprAST, then: ExprAST, else: ExprAST }
- FunctionCall: { kind: "FunctionCall", name: "...", args: [ExprAST] }
- IsNull: { kind: "IsNull", expr: ExprAST }
- In: { kind: "In", expr: ExprAST, values: [ExprAST] }

IMPORTANT: Do not use table prefixes in FieldRef.
Use { kind: 'FieldRef', field: 'total' } NOT { kind: 'FieldRef', field: 'total', table: 'orders' }.
Field names are always simple names without table qualification.

Examples:
  "total > 1000" →
    { "kind": "BinaryOp", "op": ">", "left": { "kind": "FieldRef", "field": "total" }, "right": { "kind": "Literal", "value": 1000, "type": { "kind": "number" } } }

  "status is completed" →
    { "kind": "BinaryOp", "op": "=", "left": { "kind": "FieldRef", "field": "status" }, "right": { "kind": "Literal", "value": "completed", "type": { "kind": "string" } } }

  "segment is enterprise or smb" →
    { "kind": "In", "expr": { "kind": "FieldRef", "field": "segment" }, "values": [{ "kind": "Literal", "value": "enterprise", "type": { "kind": "string" } }, { "kind": "Literal", "value": "smb", "type": { "kind": "string" } }] }

Return ONLY the ExprAST JSON object, no markdown, no explanation.`;

    const response = await this.client.messages.create({
      model: MODELS.LLM_NODE,
      max_tokens: 300,
      messages: [{ role: 'user', content: prompt }],
    });

    const raw = response.content[0].type === 'text' ? response.content[0].text : '';
    console.log('[Conditional Enrichment LLM Output]', raw);
    const clean = raw.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();

    function stripTablePrefixes(expr: any): any {
      if (!expr || typeof expr !== 'object') return expr;
      if (expr.kind === 'FieldRef' && expr.table) {
        return { kind: 'FieldRef', field: expr.field }; // drop table
      }
      // Recurse into nested expressions
      const result = { ...expr };
      for (const key of Object.keys(result)) {
        if (typeof result[key] === 'object') {
          result[key] = stripTablePrefixes(result[key]);
        }
      }
      return result;
    }

    try {
      const rawPredicate = stripTablePrefixes(JSON.parse(clean));
      
      // Strip hallucinated field references
      const safePredicate = stripHallucinatedFieldRefs(
        [{ kind: 'addField', name: '_', expr: rawPredicate }],
        availableFields ?? []
      )[0]?.expr ?? rawPredicate;
      
      return { ...payload, predicate: safePredicate };
    } catch {
      return payload; // leave as Literal(true) if parse fails
    }
  }

  private async enrichLLMNode(
    step: PipelineStepIntent,
    payload: LLMPayload,
  ): Promise<LLMPayload> {
    // Build prompt template and output schema from step information
    // No LLM call needed - just template construction from known fields
    const outputFields = step.outputFields ?? ['result'];

    return {
      ...payload,
      model: MODELS.LLM_NODE,
      userPrompt: {
        parts: [
          {
            kind: 'literal',
            text: `${step.description}\n\nRespond with JSON only. Fields required: ${outputFields.join(', ')}\n\nInput:\n`,
          },
          { kind: 'expr', expr: { kind: 'VarRef', name: 'input' } },
        ],
      },
      outputSchema: {
        columns: outputFields.map(f => ({
          name: f,
          type: { kind: 'any' },
          nullable: true,
        })),
      },
      maxTokens: 500,
      cacheBy: ['id'], // cache by id field if present to avoid duplicate LLM calls
    };
  }
}
