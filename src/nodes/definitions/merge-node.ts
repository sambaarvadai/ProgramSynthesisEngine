// Merge node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { MergePayload, MergeStrategy } from '../payloads.js';
import type { RowSet, EngineType } from '../../core/types/index.js';
import type { ExecutionContext } from '../../core/context/execution-context.js';
import { validationOk, validationFail } from '../../core/types/validation.js';

const VALID_STRATEGIES: MergeStrategy[] = ['union', 'join', 'first', 'last'];

function hashJoin(inputs: RowSet[], joinOn: string[]): RowSet {
  // Build hash table from first input
  const hashTable = new Map<string, any[]>();
  const key = (row: any) => joinOn.map(field => row[field]).join('|');

  for (const row of inputs[0].rows) {
    const k = key(row);
    if (!hashTable.has(k)) {
      hashTable.set(k, []);
    }
    hashTable.get(k)!.push(row);
  }

  // Join with other inputs
  const joinedRows: any[] = [];
  for (let i = 1; i < inputs.length; i++) {
    const currentInput = inputs[i];
    const newJoinedRows: any[] = [];

    for (const row of currentInput.rows) {
      const k = key(row);
      const matches = hashTable.get(k);
      
      if (matches) {
        for (const match of matches) {
          newJoinedRows.push({ ...match, ...row });
        }
      }
    }

    // Update hash table for next iteration
    hashTable.clear();
    for (const row of newJoinedRows) {
      const k = key(row);
      if (!hashTable.has(k)) {
        hashTable.set(k, []);
      }
      hashTable.get(k)!.push(row);
    }

    joinedRows.push(...newJoinedRows);
  }

  // Infer schema from joined rows
  const sampleRow = joinedRows[0];
  const columns = sampleRow ? Object.keys(sampleRow).map(name => ({
    name,
    type: { kind: 'any' as const },
    nullable: true
  })) : [];

  return { schema: { columns }, rows: joinedRows };
}

export const mergeNodeDefinition: NodeDefinition<MergePayload, RowSet[], RowSet> = {
  kind: 'merge',
  displayName: 'Merge',
  inputPorts: [{ key: 'inputs', label: 'Inputs', type: 'infer', required: true }],
  outputPorts: [{ key: 'output', label: 'Output', type: 'infer', required: true }],

  validate(payload: unknown) {
    const p = payload as MergePayload;
    const errors: string[] = [];

    if (!p.strategy || !VALID_STRATEGIES.includes(p.strategy)) {
      errors.push(`Strategy must be one of: ${VALID_STRATEGIES.join(', ')}`);
    }

    if (errors.length > 0) {
      return validationFail(errors.map(msg => ({ code: 'INVALID_PAYLOAD', message: msg })));
    }

    return validationOk();
  },

  inferOutputSchema(payload: MergePayload, inputSchema: EngineType): EngineType {
    return inputSchema;
  },

  async execute(payload: MergePayload, inputs: RowSet[], ctx: ExecutionContext): Promise<RowSet> {
    const nonNullInputs = inputs.filter(i => i !== null && i !== undefined);

    switch (payload.strategy) {
      case 'union':
        if (nonNullInputs.length === 0) {
          return { schema: { columns: [] }, rows: [] };
        }
        // Concatenate all rows, use schema from first non-empty input
        const firstInput = nonNullInputs[0];
        const allRows = nonNullInputs.flatMap(i => i.rows);
        return { schema: firstInput.schema, rows: allRows };

      case 'first':
        return nonNullInputs[0] || { schema: { columns: [] }, rows: [] };

      case 'last':
        return nonNullInputs[nonNullInputs.length - 1] || { schema: { columns: [] }, rows: [] };

      case 'join':
        if (!payload.joinOn || payload.joinOn.length === 0) {
          throw new Error('joinOn fields required for join strategy');
        }
        
        if (nonNullInputs.length < 2) {
          throw new Error('At least 2 inputs required for join');
        }

        // Simple hash join implementation
        return hashJoin(nonNullInputs, payload.joinOn);

      default:
        throw new Error(`Unknown merge strategy: ${payload.strategy}`);
    }
  }
};
