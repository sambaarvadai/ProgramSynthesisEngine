import type { EngineType } from '../../core/types/engine-type.js';
import type { RowSchema } from '../../core/types/schema.js';
import type { Value } from '../../core/types/value.js';

export interface ColumnConfig {
  name: string;
  type: EngineType;
  nullable: boolean;
  description?: string; // semantic metadata, used in LLM prompts
  examples?: Value[]; // sample values, used for llmTransform nodes
}

export interface ForeignKeyConfig {
  fromTable: string;
  fromColumn: string;
  toTable: string;
  toColumn: string;
  description?: string;
}

export interface TableConfig {
  name: string;
  alias?: string;
  description?: string;
  columns: ColumnConfig[];
  primaryKey: string[];
}

export interface SchemaConfig {
  tables: Map<string, TableConfig>;
  foreignKeys: ForeignKeyConfig[];
  version: string;
  description?: string;
}

export class UnknownTableError extends Error {
  constructor(tableName: string) {
    super(`Unknown table: ${tableName}`);
    this.name = 'UnknownTableError';
  }
}

export class NoJoinPathError extends Error {
  constructor(from: string, to: string) {
    super(`No join path found from ${from} to ${to}`);
    this.name = 'NoJoinPathError';
    this.from = from;
    this.to = to;
  }

  public readonly from: string;
  public readonly to: string;
}

export function getTable(schema: SchemaConfig, name: string): TableConfig {
  const table = schema.tables.get(name);
  if (!table) {
    throw new UnknownTableError(name);
  }
  return table;
}

export function getRowSchema(schema: SchemaConfig, table: string): RowSchema {
  const tableConfig = getTable(schema, table);
  return {
    columns: tableConfig.columns.map(col => ({
      name: col.name,
      type: col.type,
      nullable: col.nullable
    }))
  };
}

export function findJoinPath(schema: SchemaConfig, from: string, to: string): ForeignKeyConfig[] {
  if (from === to) {
    return []; // Same table, no join needed
  }

  // Build adjacency map for BFS
  const adjacency = new Map<string, ForeignKeyConfig[]>();
  
  // Initialize adjacency map
  for (const table of schema.tables.keys()) {
    adjacency.set(table, []);
  }

  // Add foreign key relationships (bidirectional)
  for (const fk of schema.foreignKeys) {
    // Forward direction: fromTable -> toTable
    if (!adjacency.has(fk.fromTable)) {
      adjacency.set(fk.fromTable, []);
    }
    adjacency.get(fk.fromTable)!.push(fk);

    // Reverse direction: toTable -> fromTable (for reverse joins)
    if (!adjacency.has(fk.toTable)) {
      adjacency.set(fk.toTable, []);
    }
    adjacency.get(fk.toTable)!.push(fk);
  }

  // BFS to find path
  const queue: Array<{ table: string; path: ForeignKeyConfig[] }> = [{ table: from, path: [] }];
  const visited = new Set<string>();

  while (queue.length > 0) {
    const { table: current, path } = queue.shift()!;

    if (current === to) {
      return path;
    }

    if (visited.has(current)) {
      continue;
    }

    visited.add(current);

    const neighbors = adjacency.get(current) || [];
    for (const fk of neighbors) {
      if (visited.has(fk.fromTable) && visited.has(fk.toTable)) {
        continue;
      }

      let nextTable: string;
      if (fk.fromTable === current) {
        nextTable = fk.toTable;
      } else {
        nextTable = fk.fromTable;
      }

      if (!visited.has(nextTable)) {
        queue.push({ table: nextTable, path: [...path, fk] });
      }
    }
  }

  throw new NoJoinPathError(from, to);
}

export function getRelatedTables(schema: SchemaConfig, table: string): string[] {
  const related = new Set<string>();

  for (const fk of schema.foreignKeys) {
    if (fk.fromTable === table) {
      related.add(fk.toTable);
    } else if (fk.toTable === table) {
      related.add(fk.fromTable);
    }
  }

  return Array.from(related);
}

export function tableExists(schema: SchemaConfig, name: string): boolean {
  return schema.tables.has(name);
}
