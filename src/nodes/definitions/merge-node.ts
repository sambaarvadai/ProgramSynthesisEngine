// Merge node definition

import type { NodeDefinition } from '../../core/registry/node-registry.js';
import type { MergePayload, MergeStrategy } from '../payloads.js';
import type { RowSet } from '../../core/types/value.js';
import type { DataType, DataValue } from '../../core/types/data-value.js';
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

export const mergeNodeDefinition: NodeDefinition<MergePayload, DataValue, DataValue> = {
  kind: 'merge',
  displayName: 'Merge',
  inputPorts: [{ key: 'inputs', label: 'Inputs', dataType: { kind: 'collection', itemKind: 'tabular' }, required: true }],
  outputPorts: [{ key: 'output', label: 'Output', dataType: { kind: 'any' }, required: true }],

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

  inferOutputType(payload: MergePayload, inputType: DataType): DataType {
    return { kind: 'tabular' };
  },

  async execute(payload: MergePayload, input: DataValue, ctx: ExecutionContext): Promise<DataValue> {
    // Extract RowSet array from collection DataValue
    const inputs: RowSet[] = [];
    if (input.kind === 'collection') {
      for (const item of input.data) {
        if (item.kind === 'tabular') {
          inputs.push(item.data);
        }
      }
    } else if (input.kind === 'tabular') {
      inputs.push(input.data);
    }
    const nonNullInputs = inputs.filter(i => i !== null && i !== undefined);

    let result: RowSet;
    switch (payload.strategy) {
      case 'union':
        if (nonNullInputs.length === 0) {
          result = { schema: { columns: [] }, rows: [] };
        } else {
          // Concatenate all rows, use schema from first non-empty input
          const firstInput = nonNullInputs[0];
          const allRows = nonNullInputs.flatMap(i => i.rows);
          result = { schema: firstInput.schema, rows: allRows };
        }
        break;

      case 'first':
        result = nonNullInputs[0] || { schema: { columns: [] }, rows: [] };
        break;

      case 'last':
        result = nonNullInputs[nonNullInputs.length - 1] || { schema: { columns: [] }, rows: [] };
        break;

      case 'join':
        if (!payload.joinOn || payload.joinOn.length === 0) {
          throw new Error('joinOn fields required for join strategy');
        }
        
        if (nonNullInputs.length < 2) {
          throw new Error('At least 2 inputs required for join');
        }

        // Simple hash join implementation
        result = hashJoin(nonNullInputs, payload.joinOn);
        break;

      default:
        throw new Error(`Unknown merge strategy: ${payload.strategy}`);
    }
    
    // Wrap result as tabular DataValue
    return { kind: 'tabular', data: result, schema: result.schema };
  }
};
