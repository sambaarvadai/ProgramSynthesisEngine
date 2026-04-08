// Conditional node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { ConditionalPayload } from '../payloads.js';
import type { Value } from '../../core/types/index.js';
import type { DataType, DataValue } from '../../core/types/data-value.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';
import { ExprEvaluator } from '../../executors/expr-evaluator.js';
import { FunctionRegistry } from '../../core/registry/function-registry.js';
import { TypeMismatchError } from '../../executors/expr-evaluator.js';

export const conditionalNodeDefinition: NodeDefinition<ConditionalPayload, DataValue, DataValue> = {
  kind: 'conditional',
  displayName: 'Conditional',
  inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'any' }, required: true }],
  outputPorts: [
    { key: 'true', label: 'True Branch', dataType: { kind: 'any' }, required: false },
    { key: 'false', label: 'False Branch', dataType: { kind: 'any' }, required: false },
  ],

  validate(payload: unknown) {
    const p = payload as ConditionalPayload;
    const errors: string[] = [];

    if (!p.predicate) {
      errors.push('Predicate must be a valid ExprAST');
    }

    if (errors.length > 0) {
      return validationFail(errors.map(msg => ({ code: 'INVALID_PAYLOAD', message: msg })));
    }

    return validationOk();
  },

  inferOutputType(payload: ConditionalPayload, inputType: DataType): DataType {
    return inputType;
  },

  async execute(payload: ConditionalPayload, input: DataValue, ctx: ExecutionContext): Promise<DataValue> {
    const fnRegistry = new FunctionRegistry();
    const evaluator = new ExprEvaluator(fnRegistry);

    const result = evaluator.evaluate(payload.predicate, ctx.scope);
    
    if (typeof result !== 'boolean') {
      throw new TypeMismatchError('=', typeof result, 'boolean');
    }

    // Wrap result as a scalar DataValue
    return { kind: 'scalar', data: result, type: { kind: 'boolean' } };
  }
};
