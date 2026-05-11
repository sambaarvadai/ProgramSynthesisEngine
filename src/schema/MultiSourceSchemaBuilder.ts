import { dataSourceRegistry, type DataSourceConfig, DataSourceRegistry } from '../storage/DataSourceRegistry.js';
import { buildSchemaFromSQL } from './SchemaBuilder.js';
import type { SchemaConfig } from '../compiler/schema/schema-config.js';

// Helper function to strip CREATE TYPE ... AS ENUM (...) statements
// These cause DDLParser issues, so we remove them before parsing
export function stripCreateTypes(ddl: string): string {
  return ddl.replace(
    /CREATE TYPE\s+\w+\s+AS ENUM\s*\([^)]+\)\s*;/gis,
    ''
  );
}

export interface MultiSourceSchema {
  schemas:      Map<string, any>
  tableRouting: Map<string, string>
}

export function buildMultiSourceSchema(
  sources: DataSourceConfig[]
): MultiSourceSchema {
  
  const schemas      = new Map<string, any>();
  const tableRouting = new Map<string, string>();

  for (const source of sources) {
    if (!source.schema) continue;  // skip non-postgres sources

    schemas.set(source.name, source.schema);

    for (const tableName of source.schema.parsed.tables.keys()) {
      tableRouting.set(tableName, source.name);
    }

    console.log(
      `[MultiSourceSchema] ${source.name}: ` +
      `${source.schema.parsed.tables.size} tables` 
    );
  }

  return { schemas, tableRouting };
}

export function buildCombinedSchemaConfig(
  multiSchema: MultiSourceSchema,
  registry:    DataSourceRegistry
): SchemaConfig {

  const combinedTables = new Map<string, any>();
  const combinedFKs:   any[] = [];

  for (const [datasource, schema] of multiSchema.schemas.entries()) {
    const dsConfig = registry.get(datasource);

    for (const [tableName, rawTable] of schema.parsed.tables.entries()) {
      // Convert columns Map → Array in SchemaConfig format
      const columns: any[] = [];
      for (const [colName, colDef] of rawTable.columns.entries() as any) {
        columns.push({
          name:        colName,
          type:        { kind: colDef.type },
          nullable:    colDef.nullable,
          primaryKey:  colDef.primaryKey,
          unique:      colDef.unique,
          description: colDef.description,
          examples:    colDef.examples,
        });
      }

      // Extract primary key column names
      const primaryKey = columns
        .filter((c: any) => c.primaryKey)
        .map((c: any) => c.name);

      combinedTables.set(tableName, {
        name:        tableName,
        columns,
        primaryKey,
        description: rawTable.description,
        alias:       rawTable.alias,
        // Annotate with datasource for pre-selector display
        _datasource:   datasource,
        _displayName:  dsConfig?.displayName ?? datasource
      });
    }

    // Add intra-datasource FKs
    for (const fk of schema.parsed.foreignKeys ?? []) {
      combinedFKs.push(fk);
    }
  }

  return {
    tables:      combinedTables,
    foreignKeys: combinedFKs,
    version:     '1.0',
    description: `Combined: ${[...multiSchema.schemas.keys()].join(', ')}` 
  };
}

// Given a table name, return its datasource
export function routeTable(
  tableName:    string,
  multiSchema:  MultiSourceSchema
): string {
  const datasource = multiSchema.tableRouting.get(tableName);
  if (!datasource) {
    console.warn(
      `[MultiSourceSchema] Table "${tableName}" not found in any datasource. ` +
      `Defaulting to "default"` 
    );
    return 'default';
  }
  return datasource;
}
