// Transform node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { TransformPayload, TransformOp } from '../payloads.js';
import type { RowSet, Row, EngineType, Value } from '../../core/types/index.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';
import { ExprEvaluator } from '../../executors/expr-evaluator.js';
import { FunctionRegistry } from '../../core/registry/function-registry.js';
import { TypeMismatchError, CastError } from '../../executors/expr-evaluator.js';
import { normalizeToRowSet } from '../../core/types/row.js';

export const transformNodeDefinition: NodeDefinition<TransformPayload, RowSet, RowSet> = {
  kind: 'transform',
  displayName: 'Transform',
  inputPorts: [{ key: 'input', label: 'Input', type: 'infer', required: true }],
  outputPorts: [{ key: 'output', label: 'Output', type: 'infer', required: true }],

  validate(payload: unknown) {
    const p = payload as TransformPayload;
    const errors: string[] = [];

    if (!p.operations || !Array.isArray(p.operations) || p.operations.length === 0) {
      errors.push('Operations must be a non-empty array');
      return validationFail(errors.map(msg => ({ code: 'INVALID_PAYLOAD', message: msg })));
    }

    for (const op of p.operations) {
      switch (op.kind) {
        case 'addField':
          if (!op.name || !op.expr) {
            errors.push('addField operation requires name and expr');
          }
          break;
        case 'removeField':
          if (!op.name) {
            errors.push('removeField operation requires name');
          }
          break;
        case 'renameField':
          if (!op.from || !op.to) {
            errors.push('renameField operation requires from and to');
          }
          break;
        case 'castField':
          if (!op.name || !op.to) {
            errors.push('castField operation requires name and to');
          }
          break;
        case 'filterRows':
          if (!op.predicate) {
            errors.push('filterRows operation requires predicate');
          }
          break;
        case 'sortRows':
          if (!op.keys || !Array.isArray(op.keys) || op.keys.length === 0) {
            errors.push('sortRows operation requires non-empty keys array');
          }
          break;
        case 'dedup':
          if (!op.on || !Array.isArray(op.on) || op.on.length === 0) {
            errors.push('dedup operation requires non-empty on array');
          }
          break;
        case 'limit':
          if (typeof op.count !== 'number' || op.count < 0) {
            errors.push('limit operation requires non-negative count');
          }
          break;
        default:
          errors.push(`Unknown transform operation kind: ${(op as any).kind}`);
      }
    }

    if (errors.length > 0) {
      return validationFail(errors.map(msg => ({ code: 'INVALID_PAYLOAD', message: msg })));
    }

    return validationOk();
  },

  inferOutputSchema(payload: TransformPayload, inputSchema: EngineType): EngineType {
    // For simplicity, return inputSchema unchanged
    // A full implementation would walk through operations and derive the schema
    return inputSchema;
  },

  async execute(payload: TransformPayload, input: RowSet, ctx: ExecutionContext): Promise<RowSet> {
    const fnRegistry = new FunctionRegistry();
    const evaluator = new ExprEvaluator(fnRegistry);
    
    // Normalize input to RowSet in case it's not already (e.g., loop accumulator output)
    const normalizedInput = normalizeToRowSet(input);
    
    let rows: Row[] = [...normalizedInput.rows];
    let schema = normalizedInput.schema;

    for (const op of payload.operations) {
      switch (op.kind) {
        case 'addField':
          rows = rows.map(row => {
            const value = evaluator.evaluate(op.expr, ctx.scope, row);
            return { ...row, [op.name]: value };
          });
          // Add column to schema
          schema = {
            ...schema,
            columns: [...schema.columns, { name: op.name, type: { kind: 'any' }, nullable: true }]
          };
          break;

        case 'removeField':
          rows = rows.map(row => {
            const newRow = { ...row };
            delete newRow[op.name];
            return newRow;
          });
          // Remove column from schema
          schema = {
            ...schema,
            columns: schema.columns.filter(col => col.name !== op.name)
          };
          break;

        case 'renameField':
          rows = rows.map(row => {
            if (op.from in row) {
              const newRow = { ...row };
              newRow[op.to] = newRow[op.from];
              delete newRow[op.from];
              return newRow;
            }
            return row;
          });
          // Rename column in schema
          schema = {
            ...schema,
            columns: schema.columns.map(col => 
              col.name === op.from ? { ...col, name: op.to } : col
            )
          };
          break;

        case 'castField':
          rows = rows.map(row => {
            if (op.name in row) {
              const value = row[op.name];
              let castedValue: Value = value;
              
              // Simple type conversion
              switch (op.to.kind) {
                case 'string':
                  castedValue = String(value);
                  break;
                case 'number':
                  castedValue = Number(value);
                  break;
                case 'boolean':
                  castedValue = Boolean(value);
                  break;
                default:
                  castedValue = value;
              }
              
              return { ...row, [op.name]: castedValue };
            }
            return row;
          });
          // Update column type in schema
          schema = {
            ...schema,
            columns: schema.columns.map(col => 
              col.name === op.name ? { ...col, type: op.to } : col
            )
          };
          break;

        case 'filterRows':
          rows = rows.filter(row => {
            const result = evaluator.evaluate(op.predicate, ctx.scope, row);
            return Boolean(result);
          });
          break;

        case 'sortRows':
          rows.sort((a, b) => {
            for (const key of op.keys) {
              const aVal = evaluator.evaluate(key.expr, ctx.scope, a);
              const bVal = evaluator.evaluate(key.expr, ctx.scope, b);
              
              // Handle null values - they sort last
              if (aVal === null && bVal === null) continue;
              if (aVal === null) return key.direction === 'ASC' ? 1 : -1;
              if (bVal === null) return key.direction === 'ASC' ? -1 : 1;
              
              if (aVal < bVal) return key.direction === 'ASC' ? -1 : 1;
              if (aVal > bVal) return key.direction === 'ASC' ? 1 : -1;
            }
            return 0;
          });
          break;

        case 'dedup':
          const seen = new Set<string>();
          rows = rows.filter(row => {
            const key = op.on.map(field => String(row[field])).join('|');
            if (seen.has(key)) {
              return false;
            }
            seen.add(key);
            return true;
          });
          break;

        case 'limit':
          rows = rows.slice(0, op.count);
          break;
      }
    }

    return { schema, rows };
  }
};
