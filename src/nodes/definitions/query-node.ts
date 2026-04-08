import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { QueryPayload } from '../payloads.js';
import type { DataType, DataValue } from '../../core/types/data-value.js';
import { validationOk, validationFail } from '../../core/types/validation.js';

export const queryNodeDefinition: NodeDefinition<QueryPayload, DataValue, DataValue> = {
  kind: 'query',
  displayName: 'Query',
  icon: '🔍',
  color: '#4A90E2',
  inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'any' }, required: false }],
  outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'tabular' }, required: true }],

  validate(payload: unknown): any {
    const p = payload as QueryPayload;
    if (!p?.intent) {
      return validationFail([
        { code: 'MISSING_INTENT', message: 'QueryNode requires an intent' },
      ]);
    }
    if (!p?.datasource) {
      return validationFail([
        { code: 'MISSING_DATASOURCE', message: 'QueryNode requires a datasource' },
      ]);
    }
    return validationOk();
  },

  inferOutputType(payload: QueryPayload, inputType: DataType): DataType {
    return { kind: 'tabular' };
  },

  // Execution is handled by Scheduler's QueryExecutor special case
  // This execute is never called directly — Scheduler intercepts 'query' nodes
  async execute(payload: QueryPayload, input: DataValue, ctx: any): Promise<DataValue> {
    throw new Error(
      'QueryNode execution must be handled by Scheduler via QueryExecutor',
    );
  },
};
