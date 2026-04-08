// Nodes module exports

import type { NodeRegistry } from '../core/registry/node-registry.js';
import Anthropic from '@anthropic-ai/sdk';

export * from './payloads.js';

export { inputNodeDefinition } from './definitions/input-node.js';
export { outputNodeDefinition } from './definitions/output-node.js';
export { transformNodeDefinition } from './definitions/transform-node.js';
export { queryNodeDefinition } from './definitions/query-node.js';
export { createLLMNodeDefinition } from './definitions/llm-node.js';
export { httpNodeDefinition } from './definitions/http-node.js';
export { conditionalNodeDefinition } from './definitions/conditional-node.js';
export { mergeNodeDefinition } from './definitions/merge-node.js';
export { parallelNodeDefinition } from './definitions/parallel-node.js';
export { loopNodeDefinition } from './definitions/loop-node.js';

import { inputNodeDefinition } from './definitions/input-node.js';
import { outputNodeDefinition } from './definitions/output-node.js';
import { transformNodeDefinition } from './definitions/transform-node.js';
import { queryNodeDefinition } from './definitions/query-node.js';
import { createLLMNodeDefinition } from './definitions/llm-node.js';
import { httpNodeDefinition } from './definitions/http-node.js';
import { conditionalNodeDefinition } from './definitions/conditional-node.js';
import { mergeNodeDefinition } from './definitions/merge-node.js';
import { parallelNodeDefinition } from './definitions/parallel-node.js';
import { loopNodeDefinition } from './definitions/loop-node.js';
import { validationFail } from '../core/types/validation.js';

export function registerAllNodes(
  registry: NodeRegistry,
  deps?: { anthropicApiKey?: string },
): void {
  registry.register(inputNodeDefinition);
  registry.register(outputNodeDefinition);
  registry.register(transformNodeDefinition);
  registry.register(queryNodeDefinition);
  registry.register(httpNodeDefinition);
  registry.register(conditionalNodeDefinition);
  registry.register(mergeNodeDefinition);
  registry.register(parallelNodeDefinition);
  registry.register(loopNodeDefinition);

  // LLM node requires Anthropic client
  if (deps?.anthropicApiKey) {
    const client = new Anthropic({ apiKey: deps.anthropicApiKey });
    registry.register(createLLMNodeDefinition(client));
  } else {
    // Register a stub LLM node that throws an error
    registry.register({
      kind: 'llm',
      displayName: 'LLM',
      icon: '🤖',
      color: '#7C3AED',
      inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'any' }, required: true }],
      outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'any' }, required: true }],
      validate: () => validationFail([{ code: 'MISSING_API_KEY', message: 'LLMNode requires anthropicApiKey' }]),
      inferOutputType: () => ({ kind: 'any' }),
      execute: async () => {
        throw new Error('LLMNode requires anthropicApiKey to be provided');
      },
    });
  }
}
