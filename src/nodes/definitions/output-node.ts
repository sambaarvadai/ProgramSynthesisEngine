// Output node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { OutputPayload } from '../payloads.js';
import type { Value, EngineType } from '../../core/types/index.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { contextSetOutput } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';
import { ExprEvaluator } from '../../executors/expr-evaluator.js';
import { FunctionRegistry } from '../../core/registry/function-registry.js';
import { normalizeToRowSet } from '../../core/types/row.js';

export const outputNodeDefinition: NodeDefinition<OutputPayload, Value, Value> = {
  kind: 'output',
  displayName: 'Output',
  inputPorts: [{ key: 'input', label: 'Input', type: 'infer', required: true }],
  outputPorts: [{ key: 'output', label: 'Output', type: 'infer', required: true }],

  validate(payload: unknown) {
    const p = payload as OutputPayload;
    const errors: string[] = [];

    if (!p.outputKey || typeof p.outputKey !== 'string') {
      errors.push('outputKey must be a non-empty string');
    }

    if (errors.length > 0) {
      return validationFail(errors.map(msg => ({ code: 'INVALID_PAYLOAD', message: msg })));
    }

    return validationOk();
  },

  inferOutputSchema(payload: OutputPayload, inputSchema: EngineType): EngineType {
    return inputSchema;
  },

  async execute(payload: OutputPayload, input: Value, ctx: ExecutionContext): Promise<Value> {
    const fnRegistry = new FunctionRegistry();
    const evaluator = new ExprEvaluator(fnRegistry);

    const value = payload.transform
      ? evaluator.evaluate(payload.transform, ctx.scope)
      : input;

    // Normalize to RowSet before setting output
    // This handles loop accumulator outputs (arrays, records) and other Value types
    const normalizedValue = normalizeToRowSet(value);

    const updatedCtx = contextSetOutput(ctx, payload.outputKey, normalizedValue);
    // Note: We can't actually update ctx since it's immutable, but this is the pattern
    // In a real scheduler, the scheduler would handle this

    return normalizedValue;
  }
};
