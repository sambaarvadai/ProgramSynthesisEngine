import Anthropic from '@anthropic-ai/sdk';
import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { LLMPayload } from '../payloads.js';
import type { RowSet, Row } from '../../core/types/value.js';
import type { RowSchema } from '../../core/types/schema.js';
import type { DataType, DataValue } from '../../core/types/data-value.js';
import { validationOk, validationFail } from '../../core/types/validation.js';
import { MODELS } from '../../config/models.js';

export function createLLMNodeDefinition(
  client: Anthropic,
): NodeDefinition<LLMPayload, DataValue, DataValue> {
  return {
    kind: 'llm',
    displayName: 'LLM',
    icon: '🤖',
    color: '#7C3AED',
    inputPorts: [{ key: 'input', label: 'Input', dataType: { kind: 'tabular' }, required: true }],
    outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'tabular' }, required: true }],

    validate(payload: unknown) {
      const p = payload as LLMPayload;
      if (!p?.userPrompt) {
        return validationFail([
          { code: 'MISSING_PROMPT', message: 'LLMNode requires userPrompt' },
        ]);
      }
      if (!p?.outputSchema) {
        return validationFail([
          { code: 'MISSING_SCHEMA', message: 'LLMNode requires outputSchema' },
        ]);
      }
      return validationOk();
    },

    inferOutputType(payload: LLMPayload, inputType: DataType): DataType {
      return { kind: 'tabular' };
    },

    async execute(payload, input: DataValue, ctx): Promise<DataValue> {
      // Unwrap DataValue to access RowSet
      // Tabular: use data directly
      // Record: wrap as single-row RowSet
      // Others: empty RowSet
      let inputRowSet: RowSet;
      if (input.kind === 'tabular') {
        inputRowSet = input.data;
      } else if (input.kind === 'record') {
        inputRowSet = { schema: input.schema, rows: [input.data] };
      } else {
        inputRowSet = { schema: { columns: [] }, rows: [] } as RowSet;
      }

      if (!inputRowSet.rows?.length) {
        return { kind: 'tabular', schema: payload.outputSchema, data: { schema: payload.outputSchema, rows: [] } };
      }

      const outputRows: Row[] = [];

      for (const row of inputRowSet.rows) {
        // Build user prompt from template parts
        // For literal parts: use the text
        // For expr parts: serialize the row as JSON
        const userContent = payload.userPrompt.parts.map(part => {
          if (part.kind === 'literal') return part.text;
          // Serialize row as JSON context
          return JSON.stringify(row, null, 2);
        }).join('');

        // Build system prompt from template parts
        const systemContent = payload.systemPrompt
          ? payload.systemPrompt.parts.map(p => p.kind === 'literal' ? p.text : '').join('')
          : `You are a data processing assistant. Respond with JSON only containing fields: ${payload.outputSchema.columns.map(c => c.name).join(', ')}`;

        // Append row data to user prompt if not already included (no JSON in prompt)
        const fullUserContent = userContent.includes('{')
          ? userContent
          : userContent + '\n\n' + JSON.stringify(row, null, 2);

        const response = await client.messages.create({
          model: payload.model ?? MODELS.LLM_NODE,
          max_tokens: payload.maxTokens ?? 500,
          system: systemContent,
          messages: [{ role: 'user', content: fullUserContent }],
        });

        const raw =
          response.content[0].type === 'text' ? response.content[0].text : '{}';
        console.log('[LLM Node Output]', raw);
        const clean = raw.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();

        try {
          const parsed = JSON.parse(clean);
          // Merge original row fields with LLM output
          outputRows.push({ ...row, ...parsed });
        } catch {
          // If parse fails, add error field
          outputRows.push({
            ...row,
            llm_error: 'parse_failed',
            llm_raw: raw.slice(0, 200),
          });
        }
      }

      // Output schema: original row columns + declared output fields (avoid duplicates)
      const outputSchema: RowSchema = {
        columns: [
          ...(inputRowSet.schema?.columns ?? []),
          ...payload.outputSchema.columns.filter(c =>
            !inputRowSet.schema?.columns?.some((ic: {name: string}) => ic.name === c.name),
          ),
        ],
      };

      const outputRowSet: RowSet = { schema: outputSchema, rows: outputRows };
      return { kind: 'tabular', data: outputRowSet, schema: outputSchema };
    },
  };
}
