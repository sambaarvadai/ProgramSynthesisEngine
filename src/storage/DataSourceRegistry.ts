import type { StorageBackend } from '../core/storage/storage-backend.js';
import type { Pool } from 'pg';
import type { BuiltSchema } from '../schema/SchemaBuilder.js';

export interface DataSourceConfig {
  name:        string;
  displayName: string;
  kind:        'postgres' | 'mcp_tool' | 'rest_api' | 'file';
  description: string;

  // Postgres-specific
  pool?:    Pool;
  backend?: StorageBackend;
  schema?:  BuiltSchema;
  ddlPath?: string;  // Path to DDL file for schema change detection

  // MCP-specific (Phase 3)
  mcpUrl?:  string;

  // REST-specific (Phase 4)
  openApiUrl?: string;
}

export type CrossDatasourceFK = {
  fromDatasource: string   // datasource where the FK column lives
  fromTable:      string
  fromColumn:     string
  toDatasource:   string   // datasource being referenced
  toTable:        string
  toColumn:       string
}

export class DataSourceRegistry {
  private datasources: Map<string, DataSourceConfig> = new Map();
  private crossDatasourceFKs: Map<string, CrossDatasourceFK> = new Map();

  register(config: DataSourceConfig): void {
    if (this.datasources.has(config.name)) {
      throw new Error(`DataSourceRegistry: datasource "${config.name}" already registered`);
    }
    this.datasources.set(config.name, config);
    console.log(`[DataSourceRegistry] Registered datasource: ${config.name} (${config.displayName})`);
  }

  get(name: string): DataSourceConfig | undefined {
    return this.datasources.get(name);
  }

  getBackend(name: string): StorageBackend | undefined {
    const ds = this.datasources.get(name);
    return ds?.backend;
  }

  getPool(name: string): Pool | undefined {
    const ds = this.datasources.get(name);
    return ds?.pool;
  }

  getSchema(name: string): BuiltSchema | undefined {
    const ds = this.datasources.get(name);
    return ds?.schema;
  }

  all(): DataSourceConfig[] {
    return Array.from(this.datasources.values());
  }

  has(name: string): boolean {
    return this.datasources.has(name);
  }

  // Declare one or more cross-datasource FK relationships
  declareCrossDatasourceFKs(fks: CrossDatasourceFK[]): void {
    for (const fk of fks) {
      const key = `${fk.fromTable}.${fk.fromColumn}`;
      this.crossDatasourceFKs.set(key, fk);
      console.log(
        `[DataSourceRegistry] Declared cross-datasource FK: ${key} ` +
        `→ ${fk.toDatasource}.${fk.toTable}.${fk.toColumn}`
      );
    }
  }

  // Resolve: given a table+column, return the FK definition if one exists
  resolveCrossDatasourceFK(fromTable: string, fromColumn: string): CrossDatasourceFK | undefined {
    const key = `${fromTable}.${fromColumn}`;
    return this.crossDatasourceFKs.get(key);
  }

  // Get all declared FKs for a given table
  getCrossDatasourceFKsForTable(fromTable: string): CrossDatasourceFK[] {
    const results: CrossDatasourceFK[] = [];
    for (const [key, fk] of this.crossDatasourceFKs.entries()) {
      if (fk.fromTable === fromTable) {
        results.push(fk);
      }
    }
    return results;
  }
}

export const dataSourceRegistry = new DataSourceRegistry();
