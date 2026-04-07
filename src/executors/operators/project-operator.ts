// Project operator for transforming rows with projection expressions

import { BasePhysicalOperator } from '../physical-operator.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import type { RowBatch, Row } from '../../core/types/row.js';
import type { RowSchema } from '../../core/types/schema.js';
import type { ExprAST } from '../../core/ast/expr-ast.js';
import type { PhysicalOperator } from '../physical-operator.js';
import type { ExprEvaluator } from '../expr-evaluator.js';
import type { EngineType } from '../../core/types/engine-type.js';
import type { TypeScope } from '../../core/scope/scope.js';

export interface ProjectionSpec {
  expr: ExprAST;
  alias: string;
}

interface ProjectOptions {
  input: PhysicalOperator;
  projections: ProjectionSpec[];
  evaluator: ExprEvaluator;
}

export class ProjectOperator extends BasePhysicalOperator {
  readonly kind = 'Project';
  
  private ctx: ExecutionContext | null = null;
  private outputSchema: RowSchema | null = null;

  constructor(private opts: ProjectOptions) {
    super();
  }

  protected async doOpen(ctx: ExecutionContext): Promise<void> {
    // Open input operator
    await this.opts.input.open(ctx);
    
    // Create a TypeScope from the current Scope for type inference
    // We need to convert Value bindings to EngineType bindings
    const typeScope: TypeScope = {
      id: crypto.randomUUID(),
      kind: ctx.scope.kind,
      bindings: new Map(), // Empty for now - would need proper conversion
      parent: null
    };

    // Infer outputSchema by calling evaluator.inferType on each projection expr
    const columns = await Promise.all(
      this.opts.projections.map(async (spec) => {
        try {
          const inferredType = this.opts.evaluator.inferType(spec.expr, typeScope);
          return {
            name: spec.alias,
            type: inferredType,
            nullable: true, // Assume nullable for projections
          };
        } catch (error) {
          console.error(`Error inferring type for projection '${spec.alias}': ${error}`);
          return {
            name: spec.alias,
            type: { kind: 'any' } as EngineType,
            nullable: true,
          };
        }
      })
    );

    this.outputSchema = { columns };
    
    // Store ctx
    this.ctx = ctx;
  }

  protected async doNextBatch(size: number): Promise<RowBatch> {
    if (!this.ctx || !this.outputSchema) {
      throw new Error('ProjectOperator not opened');
    }

    // Pull batch from input
    const inputBatch = await this.opts.input.nextBatch(size);
    
    if (inputBatch.rows.length === 0) {
      return {
        rows: [],
        schema: this.outputSchema
      };
    }

    // For each row: evaluate each ProjectionSpec expr against the row
    const projectedRows = inputBatch.rows.map(row => {
      const projectedRow: Row = {};
      
      for (const spec of this.opts.projections) {
        try {
          const result = this.opts.evaluator.evaluate(
            spec.expr,
            this.ctx!.scope,
            row
          );
          projectedRow[spec.alias] = result;
        } catch (error) {
          console.error(`Error evaluating projection '${spec.alias}': ${error}`);
          projectedRow[spec.alias] = null;
        }
      }
      
      return projectedRow;
    });

    return {
      rows: projectedRows,
      schema: this.outputSchema
    };
  }

  protected async doClose(): Promise<void> {
    // Close input operator
    await this.opts.input.close();
    
    // Clean up state
    this.ctx = null;
    this.outputSchema = null;
  }
}
