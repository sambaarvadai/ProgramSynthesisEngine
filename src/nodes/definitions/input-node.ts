// Input node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { InputPayload } from '../payloads.js';
import type { RowSet, EngineType } from '../../core/types/index.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';

class MissingParamError extends Error {
  constructor(paramKey: string) {
    super(`Missing required parameter: ${paramKey}`);
    this.name = 'MissingParamError';
  }
}

export const inputNodeDefinition: NodeDefinition<InputPayload, never, RowSet> = {
  kind: 'input',
  displayName: 'Input',
  inputPorts: [],
  outputPorts: [{ key: 'output', label: 'Output', type: 'infer', required: true }],

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

  inferOutputSchema(payload: InputPayload, _inputSchema: EngineType): EngineType {
    return { kind: 'rowset', schema: payload.schema };
  },

  async execute(payload: InputPayload, _input: never, ctx: ExecutionContext): Promise<RowSet> {
    if (payload.source.kind === 'param') {
      const value = ctx.params[payload.source.paramKey];
      if (value === undefined) {
        throw new MissingParamError(payload.source.paramKey);
      }

      if (typeof value !== 'object' || value === null || !('rows' in value) || !('schema' in value)) {
        throw new TypeError(`Parameter ${payload.source.paramKey} must be a RowSet`);
      }

      return value as RowSet;
    } else {
      // static source
      return payload.source.data;
    }
  }
};
