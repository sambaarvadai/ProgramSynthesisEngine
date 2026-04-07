// Parallel node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { ParallelPayload } from '../payloads.js';
import type { RowSet, EngineType } from '../../core/types/index.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';

export const parallelNodeDefinition: NodeDefinition<ParallelPayload, RowSet, RowSet> = {
  kind: 'parallel',
  displayName: 'Parallel',
  inputPorts: [{ key: 'input', label: 'Input', type: 'infer', required: true }],
  outputPorts: [{ key: 'output', label: 'Output', type: 'infer', required: true }],

  validate(payload: unknown) {
    const p = payload as ParallelPayload;
    const errors: string[] = [];

    if (typeof p.maxConcurrency !== 'number' || p.maxConcurrency < 1) {
      errors.push('maxConcurrency must be a number >= 1');
    }

    if (errors.length > 0) {
      return validationFail(errors.map(msg => ({ code: 'INVALID_PAYLOAD', message: msg })));
    }

    return validationOk();
  },

  inferOutputSchema(payload: ParallelPayload, inputSchema: EngineType): EngineType {
    return inputSchema;
  },

  async execute(payload: ParallelPayload, input: RowSet, ctx: ExecutionContext): Promise<RowSet> {
    // ParallelNode execution is handled entirely by Scheduler
    // The node itself is a marker — execute just returns input unchanged
    // Scheduler sees ParallelNode and forks branches
    return input;
  }
};
