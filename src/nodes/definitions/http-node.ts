import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { HttpPayload } from '../payloads.js';
import type { DataType, DataValue } from '../../core/types/data-value.js';
import { validationOk, validationFail } from '../../core/types/validation.js';

export const httpNodeDefinition: NodeDefinition<HttpPayload, DataValue, DataValue> = {
  kind: 'http',
  displayName: 'HTTP',
  icon: '🌐',
  color: '#E67E22',
  inputPorts: [
    { key: 'input', label: 'Input', dataType: { kind: 'any' }, required: false },
  ],
  outputPorts: [
    { key: 'output', label: 'Output', dataType: { kind: 'any' }, required: true },
  ],

  validate(payload: unknown): any {
    const p = payload as HttpPayload;
    if (!p?.url) {
      return validationFail([
        { code: 'MISSING_URL', message: 'HTTPNode requires a url' },
      ]);
    }
    if (!p?.method) {
      return validationFail([
        { code: 'MISSING_METHOD', message: 'HTTPNode requires a method' },
      ]);
    }
    return validationOk();
  },

  inferOutputType(payload: HttpPayload, inputType: DataType): DataType {
    return { kind: 'tabular' };
  },

  async execute(payload: HttpPayload, input: DataValue, ctx: any): Promise<DataValue> {
    // HTTP execution would be implemented here with fetch/axios
    // Placeholder - return void for now
    return { kind: 'void' };
  },
};
