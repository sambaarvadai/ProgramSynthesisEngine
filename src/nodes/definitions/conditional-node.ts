// Conditional node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { ConditionalPayload } from '../payloads.js';
import type { Value, EngineType } from '../../core/types/index.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';
import { ExprEvaluator } from '../../executors/expr-evaluator.js';
import { FunctionRegistry } from '../../core/registry/function-registry.js';
import { TypeMismatchError } from '../../executors/expr-evaluator.js';

export const conditionalNodeDefinition: NodeDefinition<ConditionalPayload, Value, { result: boolean; input: Value }> = {
  kind: 'conditional',
  displayName: 'Conditional',
  inputPorts: [{ key: 'input', label: 'Input', type: 'infer', required: true }],
  outputPorts: [
    { key: 'true', label: 'True Branch', type: 'infer', required: false },
    { key: 'false', label: 'False Branch', type: 'infer', required: false }
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

  inferOutputSchema(payload: ConditionalPayload, inputSchema: EngineType): EngineType {
    return inputSchema;
  },

  async execute(payload: ConditionalPayload, input: Value, ctx: ExecutionContext): Promise<{ result: boolean; input: Value }> {
    const fnRegistry = new FunctionRegistry();
    const evaluator = new ExprEvaluator(fnRegistry);

    const result = evaluator.evaluate(payload.predicate, ctx.scope);
    
    if (typeof result !== 'boolean') {
      throw new TypeMismatchError('=', typeof result, 'boolean');
    }

    return { result, input };
  }
};
