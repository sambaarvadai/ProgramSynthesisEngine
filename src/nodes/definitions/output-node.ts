// Output node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { OutputPayload } from '../payloads.js';
import type { DataType, DataValue } from '../../core/types/data-value.js';
import { tabular, toTabular } from '../../core/types/data-value.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { contextSetOutput } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';

export const outputNodeDefinition: NodeDefinition<OutputPayload, DataValue, DataValue> = {
  kind: 'output',
  displayName: 'Output',
  inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'any' }, required: true }],
  outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'tabular' }, required: true }],

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

  inferOutputType(payload: OutputPayload, inputType: DataType): DataType {
    return inputType;
  },

  async execute(payload: OutputPayload, input: DataValue, ctx: ExecutionContext): Promise<DataValue> {
    // Convert input to tabular for storage and display
    // toTabular handles all DataValue kinds:
    //   - tabular: pass through
    //   - record: single-row RowSet
    //   - collection: flatten all items into one RowSet
    //   - scalar: single-cell RowSet
    //   - void: empty RowSet
    const asTabular = toTabular(input);

    // Store in context output (for pipeline result collection)
    contextSetOutput(ctx, payload.outputKey ?? 'result', asTabular);

    // Return as tabular DataValue
    return tabular(asTabular, asTabular.schema);
  }
};
