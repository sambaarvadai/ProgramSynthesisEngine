import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { HttpPayload } from '../payloads.js';
import { validationOk, validationFail } from '../../core/types/validation.js';

export const httpNodeDefinition: NodeDefinition<HttpPayload, any, any> = {
  kind: 'http',
  displayName: 'HTTP',
  icon: '🌐',
  color: '#E67E22',
  inputPorts: [
    { key: 'input', label: 'Input', type: { kind: 'any' }, required: false },
  ],
  outputPorts: [
    { key: 'output', label: 'Output', type: { kind: 'any' }, required: true },
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

  inferOutputSchema(payload: HttpPayload, inputSchema: any): any {
    return payload.outputSchema;
  },

  async execute(payload: HttpPayload, input: any, ctx: any): Promise<any> {
    // HTTP execution would be implemented here with fetch/axios
    throw new Error('HTTPNode execution not yet implemented');
  },
};
