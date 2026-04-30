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
import type { SessionCursorStore } from './session/SessionCursor.js';
import { CalciteClient } from './compiler/calcite/index.js';
import { PostgresBackend, SQLiteTempStore } from './storage/index.js';
import { stripHallucinatedFieldRefs } from './compiler/pipeline/expr-sanitizer.js';
import { PipelineIntentGenerator } from './compiler/pipeline/index.js';
import { PipelineCompiler } from './compiler/pipeline/index.js';
import { QueryIntentGenerator as QueryIntentGeneratorClass, TablePreSelector } from './compiler/query/index.js';
import { apiRegistryStore } from './config/api-registry-store.js';
import { ApiPreSelector } from './compiler/http/api-pre-selector.js';
import { registerBuiltinFunctions } from './functions/index.js';
import { SchemaValidator } from './core/validation/schema-validator.js';
import { PermissionChecker, type GrantedSchemaResult, type JoinCompletenessResult, type PostIntentValidationResult, type MissingColumnResult } from './auth/permission-checker.js';
import { auditStore, AuditAction } from './auth/audit-store.js';
import { grantStore } from './auth/grant-store.js';
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
import { resolveWriteColumnSource } from './compiler/schema/schema-config.js';
import type { QueryPayload, TransformPayload, ConditionalPayload, LLMPayload, HttpPayload, WritePayload } from './nodes/payloads.js';
import { MODELS } from './config/models.js';
import { getAppConfig } from './config/app-config.js';
import { 
  classifyAllColumns,
  getAutoResolvedValues,
  getUserSuppliedRequired,
  getBlockedOnUpdate,
  buildIntentExclusionList,
  type SessionContext
} from './schema/ColumnClassifier.js';
import { crmSchema } from './schema/crm-schema.js';

export type PipelineEngineConfig = {
  anthropicApiKey: string;
  postgresUrl?: string;
  schema?: SchemaConfig;
  storageBackend?: StorageBackend;
  budget?: Partial<ExecutionBudget>;
  maxParallelBranches?: number;
  defaultBatchSize?: number;
  sessionCursorStore?: SessionCursorStore;
};

export type PlanResult = {
  intent: PipelineIntent;
  graph: PipelineGraph;
  compilationErrors: PipelineIntentValidationError[];
  intentRaw: string;
  userId?: string;
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

function suggestFix(
  column: string,
  targetTable: string,
  upstreamFields: string[],
  foreignKeys: any[]
): string {
  const fk = foreignKeys.find(fk =>
    fk.fromTable === targetTable && fk.fromColumn === column
  )
  if (fk) {
    return (
      `Add '${fk.toColumn}' from table '${fk.toTable}' to the upstream query ` +
      `(it will be mapped to '${column}' via the foreign key relationship)` 
    )
  }
  return `Add '${column}' to the upstream query output or staticValues` 
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
  public calciteClient: CalciteClient;
  private schemaValidator: SchemaValidator;
  private permissionChecker: PermissionChecker;

  constructor(private config: PipelineEngineConfig) {
    // Initialize Anthropic client for transform enrichment
    this.client = new Anthropic({ apiKey: config.anthropicApiKey });

    // Initialize generator
    this.generator = new PipelineIntentGenerator({
      anthropicApiKey: config.anthropicApiKey,
      schema: config.schema,
      sessionCursorStore: config.sessionCursorStore,
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

    // Initialize storage backend
    this.storageBackend =
      config.storageBackend ??
      (config.postgresUrl
        ? new PostgresBackend(config.postgresUrl)
        : new PostgresBackend('postgresql://localhost:5432/default'));

    // Initialize registries
    this.nodeRegistry = new NodeRegistry();
    this.fnRegistry = new FunctionRegistry();
    registerBuiltinFunctions(this.fnRegistry);

    // Initialize evaluator
    this.evaluator = new ExprEvaluator(this.fnRegistry);

    // Initialize temp store
    this.tempStore = new SQLiteTempStore(':memory:');

    // Initialize CalciteClient
    this.calciteClient = new CalciteClient();
    
    // Non-blocking availability check
    this.calciteClient.isAvailable().then(available => {
      if (available) {
        console.log('Calcite compiler: connected at', this.calciteClient['baseUrl'])
      } else {
        console.log('Calcite compiler: not available - using fallback SQL builder')
      }
    });

    // Register nodes with dependencies
    registerAllNodes(this.nodeRegistry, {
      anthropicApiKey: config.anthropicApiKey,
      evaluator: this.evaluator,
      storageBackend: this.storageBackend,
      schema: config.schema,
      calciteClient: this.calciteClient,
      sessionCursorStore: config.sessionCursorStore,
    });

    // Initialize query executor
    this.queryExecutor = new QueryExecutor({
      schema: config.schema ?? { tables: new Map(), foreignKeys: [], version: '1' },
      backend: this.storageBackend,
      tempStore: this.tempStore,
      evaluator: this.evaluator,
      batchSize: config.defaultBatchSize || 100,
      calciteClient: this.calciteClient,
    });

    // Initialize schema validator for pre-flight checks
    this.schemaValidator = new SchemaValidator(config.schema || { 
      tables: new Map(), 
      foreignKeys: [], 
      version: '1.0' 
    });

    // Initialize permission checker
    this.permissionChecker = new PermissionChecker();

    // Initialize scheduler
    this.scheduler = new Scheduler({
      nodeRegistry: this.nodeRegistry,
      queryExecutor: this.queryExecutor,
      evaluator: this.evaluator,
      maxParallelBranches: config.maxParallelBranches || 4,
      defaultBatchSize: config.defaultBatchSize || 100,
      sessionCursorStore: config.sessionCursorStore,
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
    options?: {
      params?: Record<string, string>;
      sessionHistory?: string;
      userId?: string;
    },
  ): Promise<PlanResult> {
    const { params, sessionHistory, userId } = options || {};
    const fullSchema = this.config.schema ?? { 
      tables: new Map(), 
      foreignKeys: [],
      version: '1' 
    };

    // Ensure foreignKeys is never undefined even if schema was constructed 
    // without it (e.g. older code paths)
    if (!fullSchema.foreignKeys) {
      (fullSchema as any).foreignKeys = []
    }

    // If userId is provided, run permission checks
    let grantedSchemaResult: GrantedSchemaResult | undefined;
    
    if (userId) {
      try {
        // 1. Check explicit mentions for permission violations
        const explicitCheck = this.permissionChecker.checkExplicitMentions(description, userId, fullSchema);
        if (!explicitCheck.allowed) {
        return {
          intent: { steps: [], description: '' },
          graph: {
            id: 'error-graph',
            version: 1,
            nodes: new Map(),
            edges: new Map(),
            entryNode: '',
            exitNodes: [],
            metadata: {
              createdAt: Date.now(),
              description: 'Permission denied',
              tags: [],
              budget: {}
            }
          },
          compilationErrors: [{
            code: 'PERMISSION_DENIED',
            message: explicitCheck.message || 'Permission denied',
            stepId: ''
          }],
          intentRaw: description,
        };
      }

      // 2. Get granted schema or return error
      grantedSchemaResult = this.permissionChecker.getGrantedSchemaOrError(userId, fullSchema);
      if (!grantedSchemaResult.ok) {
        return {
          intent: { steps: [], description: '' },
          graph: {
            id: 'error-graph',
            version: 1,
            nodes: new Map(),
            edges: new Map(),
            entryNode: '',
            exitNodes: [],
            metadata: {
              createdAt: Date.now(),
              description: 'No accessible tables',
              tags: [],
              budget: {}
            }
          },
          compilationErrors: [{
            code: 'NO_ACCESSIBLE_TABLES',
            message: grantedSchemaResult.message || 'No accessible tables',
            stepId: ''
          }],
          intentRaw: description,
        };
      }
      } catch (e) {
        console.error('[PipelineEngine] Permission check error:', e);
        console.error('[PipelineEngine] Stack:', (e as Error).stack);
        return {
          intent: { steps: [], description: '' },
          graph: {
            id: 'error-graph',
            version: 1,
            nodes: new Map(),
            edges: new Map(),
            entryNode: '',
            exitNodes: [],
            metadata: {
              createdAt: Date.now(),
              description: 'Permission check failed',
              tags: [],
              budget: {}
            }
          },
          compilationErrors: [{
            code: 'PERMISSION_CHECK_ERROR',
            message: `Permission check failed: ${(e as Error).message}`,
            stepId: ''
          }],
          intentRaw: description,
        };
      }
    }

    // Use granted schema if userId provided, otherwise use full schema
    const schemaToUse = userId && grantedSchemaResult?.ok && grantedSchemaResult.schema 
      ? grantedSchemaResult.schema 
      : fullSchema;

    // Run TablePreSelector first to get selected tables
    const preSelector = new TablePreSelector({
      anthropicApiKey: this.config.anthropicApiKey,
      maxTables: 5,
    });
    
    let preSelectionResult;
    try {
      preSelectionResult = await preSelector.select(description, schemaToUse);
    } catch (e) {
      console.error('[PipelineEngine] TablePreSelector error:', e);
      console.error('[PipelineEngine] Stack:', (e as Error).stack);
      console.error('[PipelineEngine] schemaToUse structure:', {
        hasTables: !!schemaToUse.tables,
        tablesType: typeof schemaToUse.tables,
        isMap: schemaToUse.tables instanceof Map
      });
      return {
        intent: { steps: [], description: '' },
        graph: {
          id: 'error-graph',
          version: 1,
          nodes: new Map(),
          edges: new Map(),
          entryNode: '',
          exitNodes: [],
          metadata: {
            createdAt: Date.now(),
            description: 'Table selection failed',
            tags: [],
            budget: {}
          }
        },
        compilationErrors: [{
          code: 'TABLE_SELECTION_ERROR',
          message: `Table selection failed: ${(e as Error).message}`,
          stepId: ''
        }],
        intentRaw: description,
      };
    }
    
    // If userId is provided, check join completeness
    if (userId) {
      const joinCompletenessResult = this.permissionChecker.checkJoinCompleteness(
        preSelectionResult.selectedTables,
        schemaToUse,
        fullSchema
      );
      
      if (!joinCompletenessResult.ok) {
        return {
          intent: { steps: [], description: '' },
          graph: {
            id: 'error-graph',
            version: 1,
            nodes: new Map(),
            edges: new Map(),
            entryNode: '',
            exitNodes: [],
            metadata: {
              createdAt: Date.now(),
              description: 'Join completeness check failed',
              tags: [],
              budget: {}
            }
          },
          compilationErrors: [{
            code: 'JOIN_INCOMPLETE',
            message: joinCompletenessResult.message || 'Join completeness check failed',
            stepId: ''
          }],
          intentRaw: description,
        };
      }
    }

    // Use the reduced schema from TablePreSelector for intent generation
    const schemaForIntentGeneration = preSelectionResult.reducedSchema;

    const { intent, raw: intentRaw } = await this.generator.generate(
      description,
      {
        availableParams: params,
        sessionHistory,
      },
    );

    const { graph, errors: compilationErrors } = this.compiler.compile(intent);

    let fieldMap: Map<string, string[]> = new Map();
    if (compilationErrors.length === 0) {
      fieldMap = await this.enrichNodes(
        graph,
        intent,
        schemaForIntentGeneration,
        userId
      );
    }

    // Post-intent permission validation (safety net against LLM hallucinations)
    if (userId && compilationErrors.length === 0) {
      const postIntentValidation = this.permissionChecker.validateEnrichedPipeline(
        intent,
        userId,
        schemaToUse,
        graph
      );
      
      if (!postIntentValidation.ok) {
        return {
          intent: { steps: [], description: '' },
          graph: {
            id: 'error-graph',
            version: 1,
            nodes: new Map(),
            edges: new Map(),
            entryNode: '',
            exitNodes: [],
            metadata: {
              createdAt: Date.now(),
              description: 'Post-intent permission validation failed',
              tags: [],
              budget: {}
            }
          },
          compilationErrors: [{
            code: 'POST_INTENT_PERMISSION_DENIED',
            message: postIntentValidation.message || 'Post-intent permission validation failed',
            stepId: ''
          }],
          intentRaw: description,
        };
      }
    }

    // Validate write field resolution using FK-aware column mapping
    if (compilationErrors.length === 0) {
      const fieldResolutionErrors = this.validateWriteFieldResolution(
        graph, intent, fieldMap, fullSchema
      )
      if (fieldResolutionErrors.length > 0) {
        compilationErrors.push(...fieldResolutionErrors)
      }
    }

    // Write completeness check for all WriteNode steps
    if (compilationErrors.length === 0) {
      for (const step of intent.steps) {
        const node = graph.nodes.get(step.id);
        if (node?.kind === 'write') {
          const writePayload = node.payload as WritePayload;
          
          // ColumnClassifier integration: skip auto-resolved columns in completeness check
          // Skip DELETE mode - DELETE only needs WHERE clause, not column values
          if (writePayload.table && (crmSchema as any).parsed.tables.has(writePayload.table) && writePayload.mode !== 'delete') {
            const mode = writePayload.mode === 'update' ? 'update' : 'insert';
            
            // Build session context
            const sessionCtx: SessionContext = {
              userId: userId ? parseInt(userId, 10) : 1,
              anchorIds: { workspaces: Number(1) }
            };
            
            const classifications = classifyAllColumns(
        writePayload.table, 
        crmSchema as any, 
        sessionCtx, 
        mode
      );
            const currentStaticValues = writePayload.staticValues || {};
            const missingRequired = getUserSuppliedRequired(classifications, currentStaticValues);
            
            // Always return WRITE_INCOMPLETE if there are missing columns
            // Let the CLI handle separating required vs optional in the form
            if (missingRequired.length > 0) {
              const tableColumns = (crmSchema as any).parsed.tables.get(writePayload.table)?.columns;
              const missingList = missingRequired.map(c => {
                const colDef = tableColumns?.get(c.column);
                const colType = colDef?.type ?? 'TEXT';
                const isRequired = colDef && !colDef.nullable && colDef.defaultRaw === null;
                return `${c.column} (${colType}, ${isRequired ? 'required' : 'optional'})`;
              }).join('\n  ');
              
              return {
                intent,
                graph,
                compilationErrors: [{
                  code: 'WRITE_INCOMPLETE',
                  message: `The ${writePayload.mode} into ${writePayload.table} is missing values:\n  ${missingList}`,
                  stepId: step.id,
                  missingColumns: missingRequired.map(c => {
                    const colDef = tableColumns?.get(c.column);
                    const isRequired = colDef && !colDef.nullable && colDef.defaultRaw === null;
                    return {
                      column: c.column,
                      nullable: !isRequired,
                      description: isRequired ? 'Required field' : 'Optional field'
                    };
                  })
                }],
                intentRaw: description,
              };
            }
          } else {
            // Fallback to permission checker for non-CRM tables
            const missingColumnsResult = this.permissionChecker.getMissingColumns(
              writePayload.table,
              fullSchema,
              writePayload,
              []
            );
            
            if (!missingColumnsResult.complete) {
              const missing = missingColumnsResult.missing!;
              const missingList = missing.map(m => 
                `${m.column}${m.nullable ? ' (optional)' : ' (required)'} - ${m.description}`
              ).join('\n  ');
              
              return {
                intent,
                graph,
                compilationErrors: [{
                  code: 'WRITE_INCOMPLETE',
                  message: `The ${writePayload.mode} into ${writePayload.table} is missing required values:\n  ${missingList}`,
                  stepId: step.id,
                  missingColumns: missing.map(m => ({
                    column: m.column,
                    nullable: m.nullable,
                    description: m.description
                  }))
                }],
                intentRaw: description,
              };
            }
          }
        }
      }
    }

    // Add audit logging
    if (userId) {
      const user = grantStore.getUserById(userId);
      if (user) {
        auditStore.log({
          userId,
          username: user.username,
          role: user.role,
          action: AuditAction.PIPELINE_PLANNED,
          resourceName: intent.description.slice(0, 80),
          status: compilationErrors.length === 0 ? 'planned' : 'failed',
          error: compilationErrors.length > 0
            ? compilationErrors.map(e => e.message).join('; ')
            : undefined,
          details: {
            stepCount: intent.steps.length,
            steps: intent.steps.map(s => ({ id: s.id, kind: s.kind })),
            compilationErrors: compilationErrors.map(e => e.code),
          }
        });
      }
    }

    return {
      intent,
      graph,
      compilationErrors,
      intentRaw,
      userId,
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

  /**
   * Patch an existing plan with additional static values for a specific write step.
   * This patches the plan in place without re-planning or LLM calls.
   */
  planWithMissingValues(
    existingPlan: PlanResult,
    stepId: string,
    additionalValues: Record<string, any>,
    userId?: string
  ): PlanResult {
    // Find the write step and node in the existing plan
    const writeStep = existingPlan.intent.steps.find(step => step.id === stepId);
    const writeNode = existingPlan.graph.nodes.get(stepId);
    
    if (!writeStep || !writeNode || writeNode.kind !== 'write') {
      return existingPlan;
    }
    
    // Update the WriteNode payload with additional static values
    const writePayload = writeNode.payload as WritePayload;
    writePayload.staticValues = {
      ...writePayload.staticValues,
      ...additionalValues
    };
    
    // Update the step config as well
    writeStep.config = {
      ...writeStep.config,
      fields: {
        ...(writeStep.config?.fields as Record<string, any> || {}),
        ...additionalValues
      }
    };
    
    // Re-run completeness check on the patched payload using crmSchema for accurate default info
    // (not permissionChecker which uses engine SchemaConfig with TEXT fallback)
    const tableColumns = (crmSchema as any).parsed.tables.get(writePayload.table)?.columns;
    const currentStaticValues = writePayload.staticValues || {};
    
    // Build session context for ColumnClassifier
    const sessionCtx: SessionContext = {
      userId: userId ? parseInt(userId, 10) : 1,
      anchorIds: { workspaces: Number(1) }
    };
    
    const classifications = classifyAllColumns(
      writePayload.table,
      crmSchema as any,
      sessionCtx,
      writePayload.mode === 'update' ? 'update' : 'insert'
    );
    
    const missingRequired = getUserSuppliedRequired(classifications, currentStaticValues);
    
    // Filter: skip columns that have a schema default from DDLParser
    const mustPrompt = missingRequired.filter(c => {
      const colDef = tableColumns?.get(c.column);
      if (!colDef) return false;
      
      // Skip nullable columns
      if (colDef.nullable) return false;
      
      // Skip columns with defaults from DDLParser
      if (colDef.defaultRaw !== null) return false;
      
      return true;
    });
    
    // If still incomplete, return plan with remaining WRITE_INCOMPLETE error
    if (mustPrompt.length > 0) {
      const missingList = mustPrompt.map(c => 
        `${c.column} (${c.column}, required) - ${c.column} column`
      ).join('\n  ');
      
      return {
        ...existingPlan,
        compilationErrors: [{
          code: 'WRITE_INCOMPLETE',
          message: `The ${writePayload.mode} into ${writePayload.table} is missing required values:\n  ${missingList}`,
          stepId: stepId,
          missingColumns: mustPrompt.map(c => ({
            column: c.column,
            nullable: false,
            description: 'Required field'
          }))
        }]
      };
    }
    
    // If complete, return plan with empty compilationErrors (ready to execute)
    return {
      ...existingPlan,
      compilationErrors: []
    };
  }

  async execute(
    plan: PlanResult,
    params?: Record<string, Value>,
  ): Promise<RunResult> {
    if (plan.compilationErrors.length > 0) {
      const compilationError = new PipelineCompilationError(plan.compilationErrors);
      throw compilationError;
    }

    // Pre-flight schema validation
    console.log('Performing pre-flight schema validation...');
    const schemaValidation = await this.schemaValidator.validatePipeline(plan.graph);
    
    if (!schemaValidation.isValid) {
      console.log('\nSchema validation failed:');
      console.log(this.schemaValidator.formatForDisplay(schemaValidation));
      
      const validationError = new Error(`Schema validation failed: ${schemaValidation.summary}`);
      (validationError as any).schemaValidation = schemaValidation;
      throw validationError;
    }
    
    console.log('Schema validation passed successfully');

    // Merge budget with correct precedence: defaults < plan.intent.budget < config.budget (user always wins)
    // Get centralized configuration for defaults
    const appConfig = getAppConfig();
    const budget: Partial<ExecutionBudget> = {
      // Use centralized defaults
      maxLLMCalls: appConfig.execution.maxLLMCalls,
      maxIterations: appConfig.execution.maxIterations,
      timeoutMs: appConfig.execution.timeoutMs,
      maxRowsPerNode: appConfig.execution.maxRowsPerNode,
      maxMemoryMB: appConfig.execution.maxMemoryMB,
      maxBatchSize: appConfig.execution.maxBatchSize,
      // LLM-generated hints (lower priority than user config)
      ...plan.intent.budget,
      // User config always wins
      ...(this.config.budget ?? {}),
      // These must never be overridden â always reset at execution start
      llmCallsUsed: 0,
      iterationsUsed: 0,
      startedAt: Date.now(),
    };

    // Create execution context
    const ctx = createExecutionContext({
      pipelineId: plan.graph.id,
      sessionId: crypto.randomUUID(),
      userId: plan.userId,
      params: params || {},
      budget,
    });

    const startTime = Date.now();
    const auditStart = Date.now();
    
    try {
      const execution = await this.scheduler.execute(plan.graph, ctx);
      const durationMs = Date.now() - startTime;

      // Add audit logging for successful execution
      if (plan.userId) {
        const user = grantStore.getUserById(plan.userId);
        if (user) {
          auditStore.log({
            userId: plan.userId,
            username: user.username,
            role: user.role,
            action: AuditAction.PIPELINE_EXECUTED,
            resourceName: plan.intent.description.slice(0, 80),
            status: execution.status === 'success' ? 'success' : 'failed',
            durationMs: Date.now() - auditStart,
            error: execution.status !== 'success' ? `Execution ${execution.status}` : undefined,
            details: {
              nodeCount: plan.graph.nodes.size,
              nodeStatuses: Object.fromEntries(
                [...execution.nodeStates.entries()].map(([k, v]) => [k, v.status])
              ),
              // For write nodes: capture row counts from outputs
              writes: [...execution.outputs.entries()]
                .filter(([, dv]) => dv.kind === 'tabular' || dv.kind === 'void')
                .map(([key]) => key),
            }
          });
        }
      }

      // Integrate execution trace with error monitoring
      // this.errorMonitoring.integrateWithTrace(ctx.trace);

      return {
        plan,
        execution,
        durationMs,
      };
    } catch (error) {
      // Add audit logging for execution failure
      if (plan.userId) {
        const user = grantStore.getUserById(plan.userId);
        if (user) {
          auditStore.log({
            userId: plan.userId,
            username: user.username,
            role: user.role,
            action: AuditAction.PIPELINE_EXECUTED,
            resourceName: plan.intent.description.slice(0, 80),
            status: 'failed',
            durationMs: Date.now() - auditStart,
            error: error instanceof Error ? error.message : String(error),
            details: {
              nodeCount: plan.graph.nodes.size,
            }
          });
        }
      }

      // Capture execution errors
      // const executionError = ErrorUtils.wrapError(error as Error, {
      //   pipelineId: plan.graph.id,
      //   executionId: ctx.executionId,
      //   operation: 'pipeline_execution',
      //   stage: 'scheduler_execution',
      //   duration: Date.now() - startTime
      // });
      
      // this.errorMonitoring.captureError(executionError);
      // this.errorMonitoring.integrateWithTrace(ctx.trace);
      
      throw error;
    }
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
    userId?: string
  ): Promise<Map<string, string[]>> {
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

        case 'http': {
          const availableFields = this.getAvailableFields(step.id, intent, fieldMap);
          
          // Try registry lookup first (fast, deterministic)
          const urlFromStep = step.config?.url as string ?? 
                              step.description.match(/https?:\/\/[^\s"']+/)?.[0] ?? ''
          
          let matchedEndpoint = urlFromStep 
            ? apiRegistryStore.findByUrl(urlFromStep) 
            : null
          
          // If no URL match, use ApiPreSelector to find relevant endpoint from description
          if (!matchedEndpoint) {
            const allEndpoints = apiRegistryStore.listAll()
            if (allEndpoints.length > 0) {
              const apiSelector = new ApiPreSelector({ 
                anthropicApiKey: this.config.anthropicApiKey 
              })
              const { selectedEndpoints } = await apiSelector.select(
                step.description, 
                allEndpoints
              )
              matchedEndpoint = selectedEndpoints[0] ?? null
            }
          }
          
          node.payload = await this.enrichHttpNode(step, availableFields, matchedEndpoint);
          
          const responseFieldNames = matchedEndpoint
            ? matchedEndpoint.responseFields.map((f: any) => f.name)
            : (node.payload as HttpPayload).outputFields ?? ['http_response']
          
          // Array-mode HTTP nodes expand rows - fieldMap gets response fields only
          // (not combined with input fields, since each output row is a new entity)
          if (matchedEndpoint?.responseMode === 'array') {
            fieldMap.set(step.id, [...responseFieldNames, 'http_status'])
          } else {
            // Object mode - input fields flow through with response fields appended
            fieldMap.set(step.id, [...availableFields, ...responseFieldNames, 'http_status'])
          }
          break;
        }

        case 'write': {
          const writeConfig = await this.enrichWriteNode(step, fieldMap, intent, graph, userId, 1) // Default workspaceId to 1 for demo
          node.payload = writeConfig
          
          // WriteNodes pass through the fields they received from upstream.
          // This allows downstream nodes (e.g. another WriteNode) to see 
          // the same fields that were available to this WriteNode.
          const passedThroughFields = this.getAvailableFields(step.id, intent, fieldMap)
          fieldMap.set(step.id, passedThroughFields)
          break
        }

        // loop, merge, parallel: structural nodes — no enrichment needed
        // their config comes directly from PipelineStepIntent fields
        default:
          break;
      }
    }

    return fieldMap;
  }

  private getUpstreamFields(
    stepId: string,
    intent: PipelineIntent,
    fieldMap: Map<string, string[]>
  ): string[] {
    const step = intent.steps.find(s => s.id === stepId)
    if (!step?.dependsOn?.length) return []

    const allFields: string[] = []
    
    // BFS through dependsOn chain to collect all reachable fields
    const visited = new Set<string>()
    const queue = [...step.dependsOn]
    
    while (queue.length > 0) {
      const depId = queue.shift()!
      if (visited.has(depId)) continue
      visited.add(depId)
      
      const fields = fieldMap.get(depId)
      if (fields && fields.length > 0) {
        // Found fields at this level - add them and stop going deeper for this branch
        allFields.push(...fields)
      } else {
        // No fields at this level - go deeper (this dep may be a write node 
        // that didn't populate fieldMap yet, or a structural node)
        const depStep = intent.steps.find(s => s.id === depId)
        if (depStep?.dependsOn?.length) {
          queue.push(...depStep.dependsOn)
        }
      }
    }
    
    return [...new Set(allFields)]
  }

  private getUpstreamTablesTransitive(
    stepId: string,
    intent: PipelineIntent,
    graph: PipelineGraph
  ): string[] {
    const tables: string[] = []
    const visited = new Set<string>()
    const queue = [stepId]
    
    while (queue.length > 0) {
      const id = queue.shift()!
      if (visited.has(id)) continue
      visited.add(id)
      
      const node = graph.nodes.get(id)
      if (node?.kind === 'query') {
        const qp = node.payload as any
        console.log(`[upstreamTables] QueryNode '${id}': table=${qp.intent?.table}, ` +
          `joins=${JSON.stringify(qp.intent?.joins?.map((j:any) => j.table))}`)
        if (qp.intent?.table) tables.push(qp.intent.table)
        qp.intent?.joins?.forEach((j: any) => {
          if (j.table) tables.push(j.table)
        })
      }
      
      const step = intent.steps.find(s => s.id === id)
      if (step?.dependsOn?.length) {
        queue.push(...step.dependsOn)
      }
    }
    
    return [...new Set(tables)]
  }

  public validateWriteFieldResolution(
    graph: PipelineGraph,
    intent: PipelineIntent,
    fieldMap: Map<string, string[]>,
    schema: SchemaConfig
  ): PipelineIntentValidationError[] {
    const errors: PipelineIntentValidationError[] = []

    for (const [nodeId, node] of graph.nodes) {
      if (node.kind !== 'write') continue

      const payload = node.payload as WritePayload
      const step = intent.steps.find(s => s.id === nodeId)
      const upstreamFields = this.getUpstreamFields(nodeId, intent, fieldMap)
      const staticKeys = new Set(Object.keys(payload.staticValues ?? {}))
      // Extract upstream tables from dependency nodes if not already set
      let upstreamTables = payload.upstreamTables ?? []
      if (upstreamTables.length === 0 && step?.dependsOn) {
        upstreamTables = []
        for (const depId of step.dependsOn) {
          const depNode = graph.nodes.get(depId)
          if (depNode?.kind === 'query' && depNode.payload) {
            const queryPayload = depNode.payload as any
            if (queryPayload.table) {
              upstreamTables.push(queryPayload.table)
            }
          }
        }
        // Update the payload with extracted upstream tables for future use
        if (!payload.upstreamTables) {
          payload.upstreamTables = upstreamTables
        }
      }

      console.log(
        `[Plan] WriteNode '${nodeId}': upstreamTables=${JSON.stringify(upstreamTables)}, ` +
        `upstreamFields=${JSON.stringify(upstreamFields)}, ` +
        `columns=${JSON.stringify(payload.columns)}` 
      )

      // Build a simulated row from upstream field names (values don't matter for validation)
      const simulatedRow = Object.fromEntries(upstreamFields.map(f => [f, '__present__']))

      const unresolvable: string[] = []
      const remapped: Array<{ writeCol: string; sourceField: string; via: string }> = []

      for (const col of payload.columns) {
        // Case 1: in staticValues AND value is not self-referential
        if (staticKeys.has(col)) {
          const staticVal = payload.staticValues?.[col]
          if (staticVal !== col) continue  // genuine static value, skip FK check
          // else: self-referential, fall through to FK resolution
        }

        // Case 2: direct match in upstream fields â OK
        if (col in simulatedRow) continue

        // Case 3: resolvable via FK
        const foreignKeys = schema.foreignKeys ?? []
        const sourceField = resolveWriteColumnSource(
          payload.table, col, upstreamTables, foreignKeys, simulatedRow
        )
        if (sourceField !== col && sourceField in simulatedRow) {
          // FK remapping found â auto-fix silently
          remapped.push({
            writeCol: col,
            sourceField,
            via: `${payload.table}.${col} â FK â ${sourceField}` 
          })
          // Inject columnAliases into payload so WriteNode uses it at runtime
          if (!payload.columnAliases) payload.columnAliases = {}
          payload.columnAliases[col] = sourceField
          continue
        }

        // Case 4: unresolvable
        unresolvable.push(col)
      }

      // Log auto-remappings (not an error, just informational)
      if (remapped.length > 0) {
        console.log(
          `[Plan] WriteNode '${nodeId}': auto-remapped fields via FK:\n` +
          remapped.map(r => `  ${r.writeCol} â ${r.sourceField} (${r.via})`).join('\n')
        )
      }

      // Return errors for unresolvable columns
      if (unresolvable.length > 0) {
        const available = [
          ...upstreamFields,
          ...Object.keys(payload.staticValues ?? {})
        ].join(', ')

        errors.push({
          code: 'WRITE_FIELD_UNRESOLVABLE',
          stepId: nodeId,
          message:
            `Write to '${payload.table}' needs column(s) [${unresolvable.join(', ')}] ` +
            `but they are not in the upstream output or staticValues.\n` +
            `Available fields: ${available || 'none'}\n` +
            `Fix: ensure the upstream query selects a field that maps to ` +
            `[${unresolvable.join(', ')}], or add it to staticValues.`,
          // Structured data for CLI display and AI analyzer
          missingColumns: unresolvable.map(col => ({
            column: col,
            table: payload.table,
            availableFields: upstreamFields,
            suggestion: suggestFix(col, payload.table, upstreamFields, schema.foreignKeys ?? [])
          }))
        })
      }
    }

    return errors
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
      temperature: 0,
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
    temperature: 0,
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

  private async enrichWriteNode(
    step: PipelineStepIntent,
    fieldMap: Map<string, string[]>,
    intent: PipelineIntent,
    graph: PipelineGraph,
    userId?: string,
    workspaceId?: number
  ): Promise<WritePayload> {
    const availableFields = this.getAvailableFields(step.id, { description: '', steps: [step], budget: {} }, fieldMap);
    
    // Extract existing fields from the original step configuration
    const originalFields = step.config?.fields as Record<string, any> || {};
    console.log('[WriteEnrichment] Raw step.config.fields:', step.config?.fields);
    console.log('[WriteEnrichment] originalFields type:', Array.isArray(originalFields) ? 'array' : 'object');
    
    // ColumnClassifier integration: auto-inject session-scoped values
    const targetTable = step.config?.table as string;
    let autoValues: Record<string, any> = {};
    let exclusions: string[] = [];
    
    if (targetTable && (crmSchema as any).parsed.tables.has(targetTable)) {
      const operation = step.config?.operation as string | undefined;
      const mode = operation?.toLowerCase() === 'update' ? 'update' : 'insert';
      
      // Build session context
      // Map auth user UUID to CRM user integer ID
      let crmUserId = 1;  // Default
      if (userId) {
        // Try to find the user in CRM database by matching with auth user
        // For now, use a simple lookup by username/email if available
        // TODO: Add proper UUID-to-int mapping table
        const user = grantStore.getUserById(userId);
        if (user) {
          // Query CRM database for user by email to get integer ID
          try {
            const result = await (this.storageBackend as any).pool.query(
              'SELECT id FROM users WHERE email = $1 LIMIT 1',
              [user.username]  // username is email in grant store
            );
            if (result.rows.length > 0) {
              crmUserId = result.rows[0].id;
              console.log(`[WriteEnrichment] Mapped auth user ${userId} to CRM user ID ${crmUserId}`);
            } else {
              console.debug('[WriteEnrichment] CRM user not found, using default userId=1');
            }
          } catch (err) {
            console.warn(`[WriteEnrichment] Failed to lookup CRM user: ${(err as Error).message}, using default ID 1`);
          }
        }
      }
      
      const sessionCtx: SessionContext = {
        userId: crmUserId,
        anchorIds: { workspaces: Number(workspaceId ?? 1) }
      };
      
      const classifications = classifyAllColumns(
        targetTable, 
        crmSchema as any, 
        sessionCtx, 
        mode
      );
      
      autoValues = getAutoResolvedValues(classifications);
      exclusions = buildIntentExclusionList(classifications);
      
      console.log(`[ColumnClassifier] Auto-injected for ${targetTable} (${mode}):`, autoValues);
      console.log('[ColumnClassifier] Excluded from LLM:', exclusions);
    }
    
    const prompt = `Extract database write configuration from this step description:
"${step.description}"

Existing fields to preserve: ${JSON.stringify(originalFields)}

Identify:
1. 'columns': field names that come FROM input data rows (dynamic SET values)
2. 'staticValues': literal SET values mentioned in the description
3. 'whereColumns': field names from input rows used in WHERE
4. 'staticWhere': literal WHERE values mentioned in the description
                  (e.g., 'ID=2', 'where id is 5', 'ticket 3')

IMPORTANT: You MUST preserve all existing fields from the original configuration!
- Template fields like "{{step.field}}" should go in 'columns'
- Literal values should go in 'staticValues'
- Do NOT lose any fields from the original configuration

Examples:
Original: {"customer_id": "{{step.id}}", "subject": "Test"}
Description: "update the email"
-> columns: ['customer_id']
-> staticValues: { 'subject': 'Test' }

Available tables: ${Array.from((this.config.schema as any)?.tables?.keys() ?? []).join(', ')}
Available input fields: ${availableFields.join(', ')}

Return JSON:
{
  'table': 'table_name',
  'mode': 'update',
  'columns': [],
  'staticValues': { 'field': 'value' },
  'staticWhere': { 'field': value },
  'whereColumns': [],
  'conflictColumns': []
}

IMPORTANT:
- Only put field names in "columns" if they exist in availableFields
- Put hardcoded/literal values in "staticValues"
- For NOW() timestamps, use the string "NOW()"
- PRESERVE all original fields - merge with existing configuration
- NEVER include 'id' in columns if the table has a SERIAL or auto-increment primary key
- Postgres assigns auto-increment IDs automatically - omit them from INSERT
- Only include 'id' if the user explicitly provides a specific ID value
- For email_log, use order_id (from orders table), not a computed max+1
- FIELD MAPPING: When inserting into email_log from an orders query:
  * Map 'id' from orders query to 'order_id' column
  * Map 'customer_id' from orders query to 'customer_id' column
  * Map 'email' from customers query to 'email' column
- If availableFields contains 'id' and table is 'email_log', include 'order_id' in columns
${exclusions.length > 0 ? `Do not include these columns — auto-resolved: ${exclusions.join(', ')}` : ''}
${(() => {
  // Layer 2: Add enum hints for columns the LLM can set
  const enumHints: string[] = [];
  if (targetTable && (crmSchema as any).traits?.has(targetTable)) {
    const tableTraits = (crmSchema as any).traits.get(targetTable);
    for (const [col, traits] of tableTraits.entries()) {
      if (exclusions.includes(col)) continue;  // already excluded
      if (!traits.enumValues?.length) continue;
      enumHints.push(
        `${col}: must be one of [${traits.enumValues.join(', ')}]`
      );
    }
  }
  return enumHints.length > 0
    ? `\nValid enum values:\n${enumHints.map(h => `  ${h}`).join('\n')}`
    : '';
})()}
Return ONLY raw JSON.`;

    const response = await this.client.messages.create({
      model: MODELS.LLM_NODE,
      max_tokens: 300,
      temperature: 0,     // deterministic output for structural calls
      messages: [{ role: 'user', content: prompt }],
    });

    const raw = response.content[0].type === 'text' ? response.content[0].text : '{}';
    console.log('[Write Enrichment LLM Output]', raw);
    const clean = raw.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();

    try {
      const config = JSON.parse(clean);
      console.debug('[WriteEnrichment] Parsed config:', JSON.stringify(config, null, 2));
      
      // Sanitize staticValues: remove entries where value === key (LLM hallucination)
      if (config.staticValues) {
        for (const [key, val] of Object.entries(config.staticValues)) {
          if (val === key) {
            console.warn(
              `[WriteEnrichment] Dropping staticValue '${key}': value equals key name ` +
              `(likely LLM hallucination). Will be left for user to supply or default.` 
            )
            delete config.staticValues[key]
          }
        }
      }
      
      // Convert values based on DDLParser's accurate type info from crmSchema.parsed.tables
      // (not from this.config.schema which has TEXT fallback types)
      const convertIfNeeded = (obj: Record<string, any>) => {
        for (const [key, val] of Object.entries(obj)) {
          // Always use crmSchema.parsed.tables for type info
          // this.config.schema has TEXT fallback types, not accurate INT types
          const colDef = (crmSchema as any).parsed.tables
            .get(config.table)?.columns.get(key);
          const colType = colDef?.type?.toUpperCase() ?? 'TEXT';
          
          if (typeof val === 'number') {
            if (colType === 'TEXT' || colType === 'VARCHAR' || colType === 'CHARACTER VARYING') {
              obj[key] = String(val);
              console.log(`[WriteEnrichment] Converted ${key} from number to string for ${colType} column`);
            }
          }
        }
      };
      
      if (config.staticValues) {
        convertIfNeeded(config.staticValues);
      }
      convertIfNeeded(originalFields);
      
      // ColumnClassifier: Ensure LLM didn't override auto-resolved columns
      // by re-applying autoValues on top of LLM output
      for (const [col, val] of Object.entries(autoValues)) {
        if (config.staticValues?.[col] !== undefined && 
            config.staticValues[col] !== val) {
          console.warn(
            `[ColumnClassifier] LLM tried to override auto-resolved ` +
            `column ${col}. Reverting to system value: ${val}` 
          );
        }
        if (!config.staticValues) config.staticValues = {};
        config.staticValues[col] = val;
      }
      
      // Preserve original table name from step.config
      const tableName = step.config?.table as string ?? config.table ?? 'output';
      console.log('[WriteEnrichment] Table name:', tableName);
      
      // Merge original fields with LLM-generated configuration
      const mergedColumns: string[] = [];
      // Original static values take precedence over LLM-generated ones
      const mergedStaticValues: Record<string, any> = { ...config.staticValues };
      console.log('[WriteEnrichment] Original fields:', JSON.stringify(originalFields, null, 2));
      console.log('[WriteEnrichment] LLM config columns:', config.columns);
      console.log('[WriteEnrichment] LLM config staticValues:', config.staticValues);
      
      // Process original fields
      if (Array.isArray(originalFields)) {
        // Handle array case - just use the field names directly as columns
        console.log('[WriteEnrichment] Processing originalFields as array');
        for (const fieldName of originalFields) {
          console.log(`[WriteEnrichment] Processing array field: '${fieldName}'`);
          if (!mergedColumns.includes(fieldName)) {
            mergedColumns.push(fieldName);
          }
        }
      } else {
        // Handle object case - process field/value pairs
        console.log('[WriteEnrichment] Processing originalFields as object');
        for (const [fieldName, fieldValue] of Object.entries(originalFields)) {
          const fieldValueStr = String(fieldValue);
          
          // Check if it's a template field (contains {{}})
          if (fieldValueStr.includes('{{') && fieldValueStr.includes('}}')) {
            // Extract the field name from template (e.g., "{{step.id}}" -> "id")
            const templateMatch = fieldValueStr.match(/\{\{([^}]+)\.([^}]+)\}\}/);
            if (templateMatch) {
              const sourceStep = templateMatch[1]; // e.g., "get_globex_latest_order"
              const templateField = templateMatch[2]; // e.g., "id"
              
              // Field mapping logic: map common field names to target column names
              let mappedField = templateField;
              
              // Map id -> order_id when the target table is email_log and field comes from orders-like query
              if (tableName === 'email_log' && templateField === 'id') {
                mappedField = 'order_id';
              }
              
              // Map id -> customer_id when the target table needs customer_id and field comes from customers-like query  
              if (fieldName === 'customer_id' && templateField === 'id') {
                mappedField = 'customer_id';
              }
              
              if (!mergedColumns.includes(mappedField)) {
                mergedColumns.push(mappedField);
              }
            } else {
              // If it's a complex template, add the whole field as a column
              if (!mergedColumns.includes(fieldName)) {
                mergedColumns.push(fieldName);
              }
            }
          } else if (fieldValue === fieldName) {
            // Field name used as its own value - LLM hallucination
            // Treat as a dynamic column reference, not a static value
            console.warn(`[WriteEnrichment] Dropping self-referential value: '${fieldName}' = '${fieldValue}' - treating as dynamic column`)
            if (!mergedColumns.includes(fieldName)) {
              mergedColumns.push(fieldName)
            }
          } else {
            // It's a literal value
            mergedStaticValues[fieldName] = fieldValue;
          }
        }
      }
      
      // Add any additional columns from LLM config
      console.log('[WriteEnrichment] Adding LLM config columns to merged columns');
      for (const column of config.columns || []) {
        console.log(`[WriteEnrichment] Processing LLM column: '${column}'`);
        if (!mergedColumns.includes(column)) {
          mergedColumns.push(column);
          console.log(`[WriteEnrichment] Added column '${column}' to merged columns`);
        } else {
          console.log(`[WriteEnrichment] Column '${column}' already exists in merged columns`);
        }
      }
      console.log('[WriteEnrichment] Final merged columns:', mergedColumns);
      
      // Re-apply auto-injected values (session_scoped, etc.) to ensure they're always preserved
      // even if not in originalFields or LLM output
      for (const [col, val] of Object.entries(autoValues)) {
        mergedStaticValues[col] = val;
        console.log(`[WriteEnrichment] Re-applied auto-injected value: ${col} = ${val}`);
      }

      // AUDIT_TABLES: auto-inject timestamp = NOW() for audit/history tables
      const AUDIT_TABLES = new Set([
        'assignments_history', 'audit_logs',
        'opportunity_stage_history', 'lead_score_events'
      ]);

      if (AUDIT_TABLES.has(tableName)) {
        // Find the correct timestamp column for this audit table
        const tableColumns = (crmSchema as any).parsed.tables
          .get(tableName)?.columns;
        
        // Use changed_at if it exists, else fall back to null
        // (created_at is server_generated, don't inject it)
        const timestampCol = tableColumns?.has('changed_at') 
          ? 'changed_at' 
          : null;
        
        if (timestampCol) {
          if (!mergedStaticValues[timestampCol]) {
            mergedStaticValues[timestampCol] = 'NOW()';
            console.log(`[WriteEnrichment] Auto-injected ${timestampCol} = NOW() for audit table: ${tableName}`);
          }
          
          // Remove timestamp column from dynamic columns (it's not coming from upstream, it's always NOW())
          const timestampIndex = mergedColumns.indexOf(timestampCol);
          if (timestampIndex !== -1) {
            mergedColumns.splice(timestampIndex, 1);
            console.log(`[WriteEnrichment] Removed ${timestampCol} from dynamic columns for audit table: ${tableName}`);
          }
        }

        // Auto-derive action from write mode for audit_logs
        if (targetTable === 'audit_logs' && !mergedStaticValues['action']) {
          const modeToAction: Record<string, string> = {
            'insert':        'create',
            'update':        'update',
            'delete':        'delete',
            'upsert':        'update',
            'insert_ignore': 'create'
          };
          // Derive from the upstream write node's mode if available
          // For now, default to 'update' since most audit logs are updates
          const writeMode = config.mode ?? (step.config?.mode as string ?? 'insert');
          mergedStaticValues['action'] = modeToAction[writeMode] ?? 'system';
          console.log(`[WriteEnrichment] Auto-derived action=${mergedStaticValues['action']} for audit_logs`);
        }
      }

      // Special handling: if we're inserting into email_log and have 'id' in availableFields,
      // ensure 'order_id' is included in columns
      if (tableName === 'email_log' && availableFields.includes('id')) {
        if (!mergedColumns.includes('order_id')) {
          mergedColumns.push('order_id');
        }
      }
      const isLogTable = tableName.includes('_log') || 
                         tableName.includes('_audit') ||
                         tableName.includes('notification');
      
      let mode = config.mode ?? (step.config?.mode as string ?? 'insert') as 'insert' | 'update' | 'upsert' | 'insert_ignore' | 'delete';
      if (isLogTable && mode === 'insert') {
        mode = 'insert_ignore';
      }
      
      // Find upstream tables transitively (not just direct dependsOn)
      const extractedUpstreamTables = this.getUpstreamTablesTransitive(step.id, intent, graph)

      // Build columnAliases from FK relationships
      // This maps write column names to upstream row field names
      const columnAliases: Record<string, string> = {};
      for (const col of mergedColumns) {
        // Find FK for this column in the target table
        const fkEdges = (crmSchema as any).parsed.fkGraph
          .getOutbound(tableName);

        if (fkEdges) {
          const fk = fkEdges.find((e: any) => e.fromColumn === col);
          if (fk && extractedUpstreamTables.includes(fk.toTable)) {
            // This column's value comes from upstream table's PK
            columnAliases[col] = fk.toColumn;  // e.g. owner_user_id → id
            console.log(
              `[WriteEnrichment] FK alias: ${col} ← ${fk.toTable}.${fk.toColumn}`
            );
          }
        }
      }

      // Strip unresolved param references (values matching /^\$/) from mergedStaticValues
      // These are template variables that weren't resolved and should be left for user to supply
      for (const [key, val] of Object.entries(mergedStaticValues)) {
        if (typeof val === 'string' && /^\$/.test(val)) {
          console.log(`[WriteEnrichment] Stripping unresolved param reference: ${key} = ${val}`);
          delete mergedStaticValues[key];
        }
      }

      // Type-based validity check: remove invalid string values for INT columns
      // (e.g., hallucinated placeholders like "Karthik" for owner_user_id)
      for (const [col, val] of Object.entries(mergedStaticValues)) {
        if (val === null || val === undefined) continue;

        const colDef = (crmSchema as any).parsed.tables
          .get(tableName)?.columns.get(col);

        if (!colDef) continue;

        const colType = colDef.type?.toUpperCase() ?? 'TEXT';
        const isIntType = colType === 'INT' || colType === 'INTEGER' ||
                          colType === 'SERIAL' || colType === 'BIGINT';

        if (isIntType && typeof val === 'string' && isNaN(Number(val))) {
          // String value for INT column that isn't a number
          // This is a hallucinated placeholder (e.g. "Karthik" for owner_user_id)
          // Check if this column appears in upstreamTables — if so,
          // it should be resolved from upstream, not staticValues
          console.warn(
            `[WriteEnrichment] Removing invalid INT value for ${col}: ` +
            `"${val}" — will be resolved from upstream row`
          );
          delete mergedStaticValues[col];
        }
      }

      // Layer 1: Validate staticValues against enum constraints
      const tableTraits = (crmSchema as any).traits?.get(tableName);
      if (tableTraits) {
        for (const [col, val] of Object.entries(mergedStaticValues)) {
          if (val === null || val === undefined) continue;
          
          const colTraits = tableTraits.get(col);
          const enumValues: string[] | undefined = colTraits?.enumValues;
          
          if (!enumValues || enumValues.length === 0) continue;
          
          const strVal = String(val);
          
          // Case-insensitive match against valid enum values
          const match = enumValues.find(
            v => v.toLowerCase() === strVal.toLowerCase()
          );
          
          if (match && match !== strVal) {
            // Correct casing silently
            console.log(
              `[WriteEnrichment] Corrected enum casing: ` +
              `${col} = "${strVal}" → "${match}"`
            );
            mergedStaticValues[col] = match;
          } else if (!match) {
            // Invalid enum value — check if column has a default
            const colDef = (crmSchema as any).parsed.tables
              .get(tableName)?.columns.get(col);
            
            if (colDef?.defaultRaw) {
              // Strip SQL quotes from default e.g. "'mql'" → "mql"
              const defaultVal = colDef.defaultRaw.replace(/^'(.*)'$/, '$1');
              
              console.warn(
                `[WriteEnrichment] Invalid enum value for ${col}: ` +
                `"${strVal}" not in [${enumValues.join(', ')}]. ` +
                `Using schema default: "${defaultVal}"`
              );
              mergedStaticValues[col] = defaultVal;
            } else if (!match) {
              const colDef = (crmSchema as any).parsed.tables
                .get(tableName)?.columns.get(col);
              
              if (colDef?.defaultRaw) {
                // Use schema default
                const defaultVal = colDef.defaultRaw.replace(/^'(.*)'$/, '$1');
                mergedStaticValues[col] = defaultVal;
                console.warn(`[WriteEnrichment] Invalid enum: ${col}="${strVal}" → using default "${defaultVal}"`);
              } else if (!colDef?.nullable) {
                // NOT NULL, no default — substitute closest valid value
                // For action column: 'resolved' maps to 'update' (status change)
                // Log the substitution clearly
                const fallback = findClosestEnumValue(strVal, enumValues);
                mergedStaticValues[col] = fallback;
                console.warn(
                  `[WriteEnrichment] Invalid enum: ${col}="${strVal}" has no default. ` +
                  `Substituting closest valid value: "${fallback}"` 
                );
              } else {
                // Nullable — safe to remove
                delete mergedStaticValues[col];
                console.warn(`[WriteEnrichment] Invalid enum: ${col}="${strVal}" removed (nullable)`);
              }
            }
          }
          // else: valid match with correct casing — leave as-is
        }
      }

      const finalPayload = {
        table: tableName,
        mode,
        columns: mergedColumns,
        staticValues: mergedStaticValues,
        staticWhere: config.staticWhere ?? {},
        conflictColumns: config.conflictColumns as string[],
        updateColumns: config.updateColumns as string[],
        whereColumns: config.whereColumns as string[],
        upstreamTables: extractedUpstreamTables,
        columnAliases: Object.keys(columnAliases).length > 0 ? columnAliases : undefined,
        datasource: 'default'
      };
      
      console.log('[WriteEnrichment] Final payload:', JSON.stringify(finalPayload, null, 2));

      // Auto-refresh updated_at on every UPDATE
      if (finalPayload.mode === 'update') {
        const hasUpdatedAt = (crmSchema as any).parsed.tables
          .get(tableName)?.columns.has('updated_at');
        
        if (hasUpdatedAt) {
          finalPayload.staticValues = finalPayload.staticValues ?? {};
          
          // Only set if not already explicitly provided
          if (!finalPayload.staticValues['updated_at']) {
            finalPayload.staticValues['updated_at'] = 'NOW()';
            console.log(
              `[WriteEnrichment] Auto-added updated_at = NOW() for UPDATE on ${tableName}` 
            );
          }
        }
      }

      return finalPayload;
    } catch {
      // Fallback to basic configuration with original fields
      const fallbackColumns: string[] = [];
      const fallbackStaticValues: Record<string, any> = {};
      
      // Process original fields for fallback
      for (const [fieldName, fieldValue] of Object.entries(originalFields)) {
        const fieldValueStr = String(fieldValue);
        if (fieldValueStr.includes('{{')) {
          fallbackColumns.push(fieldName);
        } else {
          fallbackStaticValues[fieldName] = fieldValue;
        }
      }
      
      return {
        table: step.config?.table as string ?? 'output',
        mode: (step.config?.mode as string ?? 'insert') as 'insert' | 'update' | 'upsert' | 'insert_ignore' | 'delete',
        columns: fallbackColumns.length > 0 ? fallbackColumns : availableFields,
        staticValues: fallbackStaticValues,
        datasource: 'default'
      };
    }
  }

  private async enrichHttpNode(
    step: PipelineStepIntent,
    availableFields: string[],
    matchedEndpoint: any // ApiEndpointRow | null
  ): Promise<HttpPayload> {
    // Extract URL using registry endpoint as authoritative source
    const urlFromDescription = matchedEndpoint?.baseUrl
      ?? step.description.match(/https?:\/\/[^\s"']+/)?.[0]
      ?? step.config?.url as string
      ?? ''

    // Extract method using registry endpoint as authoritative source
    const methodFromDescription = matchedEndpoint?.method
      ?? (step.description.match(/\b(GET|POST|PUT|PATCH|DELETE)\b/i)?.[1]
      ?? step.config?.method as string
      ?? 'POST').toUpperCase()

    // Extract batchMode from registry endpoint
    const batchModeFromRegistry = matchedEndpoint?.batchMode ?? null

    // Get schema context for LLM prompt (endpoint is now passed as parameter)
    const schemaContext = matchedEndpoint 
      ? apiRegistryStore.getSchemaContext(matchedEndpoint)
      : ''

    const userPrompt = `
Extract HTTP request config from this step description:
"${step.description}"

Available fields for body construction: ${availableFields.join(', ')}

IMPORTANT: The URL has already been extracted and is: ${urlFromDescription}
The method has already been determined as: ${methodFromDescription}
Do NOT change or reinterpret these values.
Your job is ONLY to determine: bodyFields, batchMode, outputFields, and auth.

${schemaContext ? `API Schema Context:\n${schemaContext}\n` : ''}
Return JSON with these fields:
{
  "headers": {"Content-Type": "application/json"},
  "auth": {"kind": "bearer|apiKey", "envVar": "ENV_VAR_NAME"},
  "bodyFields": ["field1", "field2"] | [],
  "bodyTemplate": "JSON template with {field} placeholders" | null,
  "outputFields": ["responseField1", "responseField2"],
  "batchMode": true/false
}

Rules:
- batchMode: true if description mentions 'batch', 'all at once', 'send all', 'array of', or the URL path suggests bulk operation (e.g. /enrich/customers plural endpoint)
- If bodyFields is empty array [], send entire row as JSON object
- If bodyTemplate is provided, use that instead of bodyFields
- If method is GET, omit bodyFields and bodyTemplate
- Always include Content-Type: application/json in headers
- For auth, use $ENV_VAR_NAME format for environment variables
- outputFields defaults to ["http_response"] if not specified
`

    // Fast path: if matchedEndpoint is found, skip LLM entirely
    if (matchedEndpoint) {
      // Registry is authoritative - only need to map availableFields to requestFields
      const requestFieldNames = matchedEndpoint.requestFields.map((f: any) => f.name)
      const responseFieldNames = matchedEndpoint.responseFields.map((f: any) => f.name)
      
      // Map: which availableFields exist in the API's requestSchema
      const bodyFields = availableFields.filter(f => requestFieldNames.includes(f))
      
      // Resolve auth from registry
      let auth
      if (matchedEndpoint.auth.kind === 'bearer' && matchedEndpoint.auth.envVar) {
        const token = process.env[matchedEndpoint.auth.envVar] ?? ''
        auth = {
          kind: 'bearer' as const,
          token: { kind: 'Literal' as const, value: token, type: { kind: 'string' as const } }
        }
      } else if (matchedEndpoint.auth.kind === 'apiKey' && matchedEndpoint.auth.envVar) {
        const key = process.env[matchedEndpoint.auth.envVar] ?? ''
        auth = {
          kind: 'apiKey' as const,
          header: matchedEndpoint.auth.header ?? 'X-API-Key',
          value: { kind: 'Literal' as const, value: key, type: { kind: 'string' as const } }
        }
      }

      return {
        url: this.buildUrlTemplate(urlFromDescription, availableFields),
        method: methodFromDescription as 'GET'|'POST'|'PUT'|'PATCH'|'DELETE',
        headers: { 'Content-Type': { parts: [{ kind: 'literal', text: 'application/json' }] } },
        body: { kind: 'VarRef' as const, name: 'input' },
        bodyFields,                          // intersection of availableFields and requestFields
        outputFields: responseFieldNames,    // authoritative from registry
        outputSchema: { kind: 'any' as const },
        auth,
        retryPolicy: { maxRetries: getAppConfig().http.defaultMaxRetries, backoffMs: getAppConfig().http.defaultBackoffMs },
        batchMode: matchedEndpoint.batchMode,
        endpointId: matchedEndpoint.id,
        responseMode: matchedEndpoint.responseMode,
        responseRoot: matchedEndpoint.responseRoot,
        concurrency: matchedEndpoint.defaultConcurrency,
        rateLimitPerSecond: matchedEndpoint.defaultRateLimit,
        chunkSize: matchedEndpoint.defaultChunkSize
      }
    }
    // Fall through to LLM-based extraction for unknown APIs

    try {
      const response = await this.client.messages.create({
        model: MODELS.LLM_NODE,
        max_tokens: 400,
        temperature: 0,
        system: "You extract HTTP request configuration from natural language step descriptions. Return only raw JSON, no markdown.",
        messages: [{ role: 'user', content: userPrompt }]
      })

      const content = response.content[0]
      if (content.type !== 'text') {
        throw new Error('Unexpected response type from LLM')
      }

      // Clean markdown wrapping if present
      const cleanText = content.text.trim()
        .replace(/```json\n?/g, '')
        .replace(/```\n?/g, '')

      const config = JSON.parse(cleanText)

      // Build URL template from extracted URL with field validation
      const urlParts = this.buildUrlTemplate(urlFromDescription, availableFields)

      // Parse auth
      let auth
      if (config.auth && config.auth.kind) {
        const tokenValue = process.env[config.auth.envVar] ?? ''
        if (config.auth.kind === 'bearer') {
          auth = {
            kind: 'bearer' as const,
            token: { kind: 'Literal' as const, value: tokenValue, type: { kind: 'string' as const } }
          }
        } else if (config.auth.kind === 'apiKey') {
          auth = {
            kind: 'apiKey' as const,
            header: 'X-API-Key',
            value: { kind: 'Literal' as const, value: tokenValue, type: { kind: 'string' as const } }
          }
        }
      }

      // Parse body - validate against matched endpoint schema
      let body
      let validatedBodyFields: string[] = []
      if (config.method !== 'GET' && config.bodyFields !== null) {
        if (config.bodyTemplate) {
          // For body templates, use VarRef to reference the input row
          // The template processing will be handled by the HTTP node during execution
          body = { kind: 'VarRef' as const, name: 'input' }
        } else if (config.bodyFields && config.bodyFields.length > 0) {
          // When no matched endpoint, don't filter against empty validRequestFields
          // - accept all bodyFields the LLM returned that exist in availableFields
          const validatedBodyFields = matchedEndpoint
            ? config.bodyFields.filter((f: string) => 
                matchedEndpoint.requestFields.map((rf: any) => rf.name).includes(f)
              )
            : config.bodyFields.filter((f: string) => availableFields.includes(f))
          
          // bodyFields case - set fallback body expression
          body = { kind: 'VarRef' as const, name: 'input' }
        } else {
          // Empty array = send entire row
          body = { kind: 'VarRef' as const, name: 'input' }
        }
      }

      // Parse output schema - validate against matched endpoint schema
      let validatedOutputFields: string[] = []
      if (config.outputFields) {
        const validResponseFields = matchedEndpoint?.responseFields.map((f: any) => f.name) || []
        validatedOutputFields = config.outputFields.filter((field: string) => 
          validResponseFields.includes(field)
        )
      }
      
      const outputFields = validatedOutputFields.length > 0 ? validatedOutputFields : ['http_response']
      const outputSchema = {
        columns: outputFields.map((f: string) => ({
          name: f,
          type: { kind: 'any' as const },
          nullable: true
        }))
      }

      // Convert headers to TemplateString format
      const headers: Record<string, any> = {}
      if (config.headers) {
        for (const [key, value] of Object.entries(config.headers)) {
          headers[key] = { parts: [{ kind: 'literal', text: String(value) }] }
        }
      }

      return {
        url: urlParts,
        method: methodFromDescription as 'GET'|'POST'|'PUT'|'PATCH'|'DELETE',
        headers,
        body,
        bodyFields: validatedBodyFields,
        outputFields: outputFields,
        outputSchema: { kind: 'any' },
        auth,
        retryPolicy: { maxRetries: getAppConfig().http.defaultMaxRetries, backoffMs: getAppConfig().http.defaultBackoffMs },
        batchMode: config.batchMode ?? false,
        endpointId: matchedEndpoint?.id
      }
    } catch (error) {
      console.warn('Failed to parse LLM response for HTTP node:', error)
      
      // Fallback: minimal valid payload
      const fallbackUrl = String(step.config?.url ?? '')
      return {
        url: { parts: [{ kind: 'literal', text: fallbackUrl }] },
        method: 'POST',
        headers: {},
        body: { kind: 'VarRef' as const, name: 'input' },
        bodyFields: [],
        outputFields: ['http_response'],
        outputSchema: { kind: 'any' },
        retryPolicy: { maxRetries: getAppConfig().http.defaultMaxRetries, backoffMs: getAppConfig().http.defaultBackoffMs },
        batchMode: false,
        endpointId: matchedEndpoint?.id
      }
    }
  }

  private buildUrlTemplate(url: string, availableFields: string[]): { parts: any[] } {
    const fieldSet = new Set(availableFields)
    const parts: any[] = []
    const regex = /\{(\w+)\}/g
    let lastIndex = 0
    let match: RegExpExecArray | null

    while ((match = regex.exec(url)) !== null) {
      // Add literal part before the template variable
      if (match.index > lastIndex) {
        parts.push({ kind: 'literal', text: url.slice(lastIndex, match.index) })
      }
      
      const fieldName = match[1]
      
      if (fieldSet.has(fieldName)) {
        // Valid field - create FieldRef
        parts.push({ kind: 'expr', expr: { kind: 'FieldRef', field: fieldName } })
      } else {
        // Field not in available fields - try common name mappings or keep as literal
        const mapped = fieldName === 'customerId' ? 'id'
                     : fieldName === 'customer_id' ? 'id'  
                     : fieldName === 'orderId' ? 'id'
                     : null
        if (mapped && fieldSet.has(mapped)) {
          parts.push({ kind: 'expr', expr: { kind: 'FieldRef', field: mapped } })
        } else {
          // Unknown field - keep as literal text to avoid runtime crash
          parts.push({ kind: 'literal', text: match[0] })
          console.warn(`[enrichHttpNode] URL template field {${fieldName}} not found in availableFields: ${availableFields.join(', ')} - keeping as literal`)
        }
      }
      
      lastIndex = regex.lastIndex
    }

    // Add remaining literal text
    if (lastIndex < url.length) {
      parts.push({ kind: 'literal', text: url.slice(lastIndex) })
    }

    return { parts: parts.length > 0 ? parts : [{ kind: 'literal', text: url }] }
  }
}

// Helper: find closest valid enum value by string similarity
function findClosestEnumValue(input: string, validValues: string[]): string {
  // Simple approach: return the first value that shares a prefix,
  // or the first value as fallback
  const lower = input.toLowerCase();
  const match = validValues.find(v => 
    v.toLowerCase().startsWith(lower[0]) 
  );
  return match ?? validValues[0];
}
