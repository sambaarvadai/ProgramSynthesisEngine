// Input node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { InputPayload } from '../payloads.js';
import type { RowSet } from '../../core/types/index.js';
import type { DataType, DataValue } from '../../core/types/data-value.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';

class MissingParamError extends Error {
  constructor(paramKey: string) {
    super(`Missing required parameter: ${paramKey}`);
    this.name = 'MissingParamError';
  }
}

export const inputNodeDefinition: NodeDefinition<InputPayload, DataValue, DataValue> = {
  kind: 'input',
  displayName: 'Input',
  inputPorts: [],
  outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'tabular' }, required: true }],

  validate(payload: unknown) {
    const p = payload as InputPayload;
    const errors: string[] = [];

    if (!p.schema || !p.schema.columns || p.schema.columns.length === 0) {
      errors.push('Schema must have at least one column');
    }

    if (!p.source) {
      errors.push('Source must be specified');
    } else if (p.source.kind !== 'param' && p.source.kind !== 'static') {
      errors.push('Source kind must be either "param" or "static"');
    }

    if (p.source.kind === 'param' && !p.source.paramKey) {
      errors.push('paramKey is required when source kind is "param"');
    }

    if (errors.length > 0) {
      return validationFail(errors.map(msg => ({ code: 'INVALID_PAYLOAD', message: msg })));
    }

    return validationOk();
  },

  inferOutputType(payload: InputPayload, _inputType: DataType): DataType {
    return { kind: 'tabular' };
  },

  async execute(payload: InputPayload, _input: DataValue, ctx: ExecutionContext): Promise<DataValue> {
    if (payload.source.kind === 'param') {
      const value = ctx.params[payload.source.paramKey];
      if (value === undefined) {
        throw new MissingParamError(payload.source.paramKey);
      }

      if (typeof value !== 'object' || value === null || !('rows' in value) || !('schema' in value)) {
        throw new TypeError(`Parameter ${payload.source.paramKey} must be a RowSet`);
      }

      // Wrap RowSet as tabular DataValue
      const rs = value as RowSet;
      return { kind: 'tabular', data: rs, schema: rs.schema };
    } else {
      // static source
      const rs = payload.source.data;
      return { kind: 'tabular', data: rs, schema: rs.schema };
    }
  }
};
