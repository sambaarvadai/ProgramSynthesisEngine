import Database from 'better-sqlite3';
import { randomUUID } from 'crypto';
import { getDatabaseConfig } from './database-config.js';

export type ApiEndpointRow = {
  id: string
  displayName: string
  baseUrl: string
  method: 'GET'|'POST'|'PUT'|'PATCH'|'DELETE'
  batchMode: boolean
  responseMode: 'object' | 'array'
  responseRoot?: string
  auth: { kind: 'none'|'bearer'|'apiKey', envVar?: string, header?: string }
  urlPattern?: RegExp
  description: string
  requestFields: ApiFieldRow[]
  responseFields: ApiFieldRow[]
  defaultConcurrency: number
  defaultRateLimit?: number
  defaultChunkSize?: number
}

export type ApiFieldRow = {
  id: string
  endpointId: string
  name: string
  type: 'string'|'number'|'boolean'|'object'|'array'
  required?: boolean
  description?: string
  sortOrder: number
  apiFieldName?: string
}

export class ApiRegistryStore {
  private db: Database.Database;

  constructor(dbPath?: string) {
    const config = getDatabaseConfig();
    this.db = new Database(dbPath || config.apiRegistryPath);
    this.initializeSchema();
  }

  private initializeSchema(): void {
    // Create api_endpoints table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS api_endpoints (
        id           TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        base_url     TEXT NOT NULL,
        method       TEXT NOT NULL,
        batch_mode   INTEGER NOT NULL DEFAULT 0,
        response_mode TEXT NOT NULL DEFAULT 'object',
        response_root TEXT,
        auth_kind    TEXT NOT NULL DEFAULT 'none',
        auth_env_var TEXT,
        auth_header  TEXT,
        url_pattern  TEXT,
        description  TEXT NOT NULL,
        default_concurrency INTEGER NOT NULL DEFAULT 1,
        default_rate_limit INTEGER,
        default_chunk_size INTEGER,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL
      )
    `);

    // Create api_request_fields table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS api_request_fields (
        id           TEXT PRIMARY KEY,
        endpoint_id  TEXT NOT NULL REFERENCES api_endpoints(id) ON DELETE CASCADE,
        name         TEXT NOT NULL,
        type         TEXT NOT NULL,
        required     INTEGER NOT NULL DEFAULT 0,
        description  TEXT,
        sort_order   INTEGER NOT NULL DEFAULT 0
      )
    `);

    // Create api_response_fields table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS api_response_fields (
        id           TEXT PRIMARY KEY,
        endpoint_id  TEXT NOT NULL REFERENCES api_endpoints(id) ON DELETE CASCADE,
        name         TEXT NOT NULL,
        type         TEXT NOT NULL,
        description  TEXT,
        sort_order   INTEGER NOT NULL DEFAULT 0,
        api_field_name TEXT
      )
    `);

    // Create indexes
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_req_fields_endpoint  ON api_request_fields(endpoint_id);
      CREATE INDEX IF NOT EXISTS idx_resp_fields_endpoint ON api_response_fields(endpoint_id);
    `);
  }

  register(endpoint: Omit<ApiEndpointRow, 'requestFields'|'responseFields'> & {
    requestFields: Omit<ApiFieldRow, 'id'|'endpointId'>[]
    responseFields: Omit<ApiFieldRow, 'id'|'endpointId'>[]
  }): void {
    const now = Date.now();
    
    // Insert or replace endpoint
    const insertEndpoint = this.db.prepare(`
      INSERT OR REPLACE INTO api_endpoints (
        id, display_name, base_url, method, batch_mode, response_mode, response_root,
        auth_kind, auth_env_var, auth_header, url_pattern, description, default_concurrency,
        default_rate_limit, default_chunk_size, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    insertEndpoint.run(
      endpoint.id,
      endpoint.displayName,
      endpoint.baseUrl,
      endpoint.method,
      endpoint.batchMode ? 1 : 0,
      endpoint.responseMode ?? 'object',
      endpoint.responseRoot || null,
      endpoint.auth.kind,
      endpoint.auth.envVar || null,
      endpoint.auth.header || null,
      endpoint.urlPattern?.source || null,
      endpoint.description,
      endpoint.defaultConcurrency ?? 1,
      endpoint.defaultRateLimit || null,
      endpoint.defaultChunkSize || null,
      now,
      now
    );

    // Delete existing fields for this endpoint
    const deleteRequestFields = this.db.prepare('DELETE FROM api_request_fields WHERE endpoint_id = ?');
    const deleteResponseFields = this.db.prepare('DELETE FROM api_response_fields WHERE endpoint_id = ?');
    
    deleteRequestFields.run(endpoint.id);
    deleteResponseFields.run(endpoint.id);

    // Insert request fields
    const insertRequestField = this.db.prepare(`
      INSERT INTO api_request_fields (id, endpoint_id, name, type, required, description, sort_order)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);

    for (const field of endpoint.requestFields) {
      insertRequestField.run(
        randomUUID(),
        endpoint.id,
        field.name,
        field.type,
        field.required ? 1 : 0,
        field.description || null,
        field.sortOrder
      );
    }

    // Insert response fields
    const insertResponseField = this.db.prepare(`
      INSERT INTO api_response_fields (id, endpoint_id, name, type, description, sort_order, api_field_name)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);

    for (const field of endpoint.responseFields) {
      insertResponseField.run(
        randomUUID(),
        endpoint.id,
        field.name,
        field.type,
        field.description || null,
        field.sortOrder,
        field.apiFieldName || null
      );
    }
  }

  private mapRowToEndpoint(row: any, requestFields: ApiFieldRow[], responseFields: ApiFieldRow[]): ApiEndpointRow {
    return {
      id: row.id,
      displayName: row.display_name,
      baseUrl: row.base_url,
      method: row.method as 'GET'|'POST'|'PUT'|'PATCH'|'DELETE',
      batchMode: row.batch_mode === 1,
      responseMode: (row.response_mode ?? 'object') as 'object' | 'array',
      responseRoot: row.response_root || undefined,
      auth: {
        kind: row.auth_kind as 'none'|'bearer'|'apiKey',
        envVar: row.auth_env_var || undefined,
        header: row.auth_header || undefined
      },
      urlPattern: row.url_pattern ? new RegExp(row.url_pattern) : undefined,
      description: row.description,
      requestFields,
      responseFields,
      defaultConcurrency: row.default_concurrency ?? 1,
      defaultRateLimit: row.default_rate_limit || undefined,
      defaultChunkSize: row.default_chunk_size || undefined
    };
  }

  private mapRowToRequestField(row: any): ApiFieldRow {
    return {
      id: row.id,
      endpointId: row.endpoint_id,
      name: row.name,
      type: row.type as 'string'|'number'|'boolean'|'object'|'array',
      required: row.required === 1,
      description: row.description || undefined,
      sortOrder: row.sort_order
    };
  }

  private mapRowToResponseField(row: any): ApiFieldRow {
    return {
      id: row.id,
      endpointId: row.endpoint_id,
      name: row.name,
      type: row.type as 'string'|'number'|'boolean'|'object'|'array',
      description: row.description || undefined,
      sortOrder: row.sort_order,
      apiFieldName: row.api_field_name || undefined
    };
  }

  findByUrl(url: string): ApiEndpointRow | null {
    // Get all endpoints
    const endpoints = this.db.prepare('SELECT * FROM api_endpoints').all() as any[];
    
    for (const endpointRow of endpoints) {
      // Check if URL matches baseUrl prefix
      if (url.startsWith(endpointRow.base_url)) {
        // Get fields for this endpoint
        const requestFields = this.db.prepare('SELECT * FROM api_request_fields WHERE endpoint_id = ? ORDER BY sort_order')
          .all(endpointRow.id) as any[];
        const responseFields = this.db.prepare('SELECT * FROM api_response_fields WHERE endpoint_id = ? ORDER BY sort_order')
          .all(endpointRow.id) as any[];
        
        return this.mapRowToEndpoint(
          endpointRow,
          requestFields.map(f => this.mapRowToRequestField(f)),
          responseFields.map(f => this.mapRowToResponseField(f))
        );
      }
      
      // Check if URL matches urlPattern regex
      if (endpointRow.url_pattern) {
        try {
          const regex = new RegExp(endpointRow.url_pattern);
          if (regex.test(url)) {
            // Get fields for this endpoint
            const requestFields = this.db.prepare('SELECT * FROM api_request_fields WHERE endpoint_id = ? ORDER BY sort_order')
              .all(endpointRow.id) as any[];
            const responseFields = this.db.prepare('SELECT * FROM api_response_fields WHERE endpoint_id = ? ORDER BY sort_order')
              .all(endpointRow.id) as any[];
            
            return this.mapRowToEndpoint(
              endpointRow,
              requestFields.map(f => this.mapRowToRequestField(f)),
              responseFields.map(f => this.mapRowToResponseField(f))
            );
          }
        } catch (e) {
          // Invalid regex, skip
        }
      }
    }
    
    return null;
  }

  findByInputField(fieldName: string): ApiEndpointRow[] {
    const endpoints = this.db.prepare(`
      SELECT DISTINCT e.* FROM api_endpoints e
      JOIN api_request_fields rf ON e.id = rf.endpoint_id
      WHERE rf.name = ?
    `).all(fieldName) as any[];
    
    return endpoints.map(endpointRow => {
      const requestFields = this.db.prepare('SELECT * FROM api_request_fields WHERE endpoint_id = ? ORDER BY sort_order')
        .all(endpointRow.id) as any[];
      const responseFields = this.db.prepare('SELECT * FROM api_response_fields WHERE endpoint_id = ? ORDER BY sort_order')
        .all(endpointRow.id) as any[];
      
      return this.mapRowToEndpoint(
        endpointRow,
        requestFields.map(f => this.mapRowToRequestField(f)),
        responseFields.map(f => this.mapRowToResponseField(f))
      );
    });
  }

  findByOutputField(fieldName: string): ApiEndpointRow[] {
    const endpoints = this.db.prepare(`
      SELECT DISTINCT e.* FROM api_endpoints e
      JOIN api_response_fields rf ON e.id = rf.endpoint_id
      WHERE rf.name = ?
    `).all(fieldName) as any[];
    
    return endpoints.map(endpointRow => {
      const requestFields = this.db.prepare('SELECT * FROM api_request_fields WHERE endpoint_id = ? ORDER BY sort_order')
        .all(endpointRow.id) as any[];
      const responseFields = this.db.prepare('SELECT * FROM api_response_fields WHERE endpoint_id = ? ORDER BY sort_order')
        .all(endpointRow.id) as any[];
      
      return this.mapRowToEndpoint(
        endpointRow,
        requestFields.map(f => this.mapRowToRequestField(f)),
        responseFields.map(f => this.mapRowToResponseField(f))
      );
    });
  }

  listAll(): ApiEndpointRow[] {
    const endpoints = this.db.prepare('SELECT * FROM api_endpoints ORDER BY id').all() as any[];
    
    return endpoints.map(endpointRow => {
      const requestFields = this.db.prepare('SELECT * FROM api_request_fields WHERE endpoint_id = ? ORDER BY sort_order')
        .all(endpointRow.id) as any[];
      const responseFields = this.db.prepare('SELECT * FROM api_response_fields WHERE endpoint_id = ? ORDER BY sort_order')
        .all(endpointRow.id) as any[];
      
      return this.mapRowToEndpoint(
        endpointRow,
        requestFields.map(f => this.mapRowToRequestField(f)),
        responseFields.map(f => this.mapRowToResponseField(f))
      );
    });
  }

  getById(id: string): ApiEndpointRow | null {
    const endpointRow = this.db.prepare('SELECT * FROM api_endpoints WHERE id = ?').get(id) as any;
    
    if (!endpointRow) {
      return null;
    }
    
    const requestFields = this.db.prepare('SELECT * FROM api_request_fields WHERE endpoint_id = ? ORDER BY sort_order')
      .all(endpointRow.id) as any[];
    const responseFields = this.db.prepare('SELECT * FROM api_response_fields WHERE endpoint_id = ? ORDER BY sort_order')
      .all(endpointRow.id) as any[];
    
    return this.mapRowToEndpoint(
      endpointRow,
      requestFields.map(f => this.mapRowToRequestField(f)),
      responseFields.map(f => this.mapRowToResponseField(f))
    );
  }

  delete(id: string): void {
    const deleteEndpoint = this.db.prepare('DELETE FROM api_endpoints WHERE id = ?');
    deleteEndpoint.run(id);
  }

  getSchemaContext(endpoint: ApiEndpointRow): string {
    const accepts = endpoint.requestFields
      .map(f => `${f.name} (${f.type}${f.required ? ', required' : ''})${f.description ? ` - ${f.description}` : ''}`)
      .join(', ');
    
    const returns = endpoint.responseFields
      .map(f => `${f.name} (${f.type})${f.description ? ` - ${f.description}` : ''}`)
      .join(', ');

    const responseModeInfo = endpoint.responseMode === 'array' 
      ? `Response mode: array${endpoint.responseRoot ? ` (root: ${endpoint.responseRoot})` : ''}`
      : 'Response mode: object';

    return `API: ${endpoint.displayName} [${endpoint.method} ${endpoint.baseUrl}]
Description: ${endpoint.description}
${responseModeInfo}
Accepts: ${accepts || 'none'}
Returns: ${returns || 'none'}`;
  }

  getSummaryList(): string {
    const endpoints = this.listAll();
    
    return endpoints
      .map(endpoint => {
        const accepts = endpoint.requestFields.map(f => f.name).join(',');
        const returns = endpoint.responseFields.map(f => f.name).join(',');
        return `${endpoint.id} | ${endpoint.method} ${endpoint.baseUrl} | accepts: ${accepts || 'none'} | returns: ${returns || 'none'}`;
      })
      .join('\n');
  }

  close(): void {
    this.db.close();
  }
}

// Export singleton
export const apiRegistryStore = new ApiRegistryStore();
