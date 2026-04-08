// Loop node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { LoopPayload } from '../payloads.js';
import type { DataType, DataValue, DataValueKind } from '../../core/types/data-value.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';

export const loopNodeDefinition: NodeDefinition<LoopPayload, DataValue, DataValue> = {
  kind: 'loop',
  displayName: 'Loop',
  inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'any' }, required: true }],
  outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'collection', itemKind: 'tabular' as DataValueKind }, required: true }],

  validate(payload: unknown) {
    const p = payload as LoopPayload;
    const errors: string[] = [];

    if (typeof p.maxIterations !== 'number' || p.maxIterations <= 0 || p.maxIterations > 10000) {
      errors.push('maxIterations must be > 0 and <= 10000');
    }

    if (p.mode === 'forEach' && !p.over) {
      errors.push('over must be present when mode is forEach');
    }

    if (p.mode === 'while' && !p.condition) {
      errors.push('condition must be present when mode is while');
    }

    if (!p.iterVar || typeof p.iterVar !== 'string') {
      errors.push('iterVar must be a non-empty string');
    }

    if (errors.length > 0) {
      return validationFail(errors.map(msg => ({ code: 'INVALID_PAYLOAD', message: msg })));
    }

    return validationOk();
  },

  inferOutputType(payload: LoopPayload, inputType: DataType): DataType {
    return inputType;
  },

  async execute(payload: LoopPayload, input: DataValue, ctx: ExecutionContext): Promise<DataValue> {
    // LoopNode execution is handled by Scheduler
    // Like ParallelNode, Scheduler owns loop execution entirely
    throw new Error('LoopNode execution is handled by Scheduler');
  }
};
