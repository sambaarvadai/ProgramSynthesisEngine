import { PipelineEngine } from './pipeline-engine.js';
import { PostgresBackend } from './storage/index.js';
import { SessionManager } from './session/session-manager.js';
import { SessionCursorStore } from './session/SessionCursor.js';
import { VoyageClient } from './cache/VoyageClient.js';
import { SemanticCache } from './cache/SemanticCache.js';
import { SchemaStateManager } from './cache/SchemaStateManager.js';
import { dataSourceRegistry } from './storage/DataSourceRegistry.js';
import { buildMultiSourceSchema, type MultiSourceSchema, buildCombinedSchemaConfig, stripCreateTypes } from './schema/MultiSourceSchemaBuilder.js';
import { buildSchemaFromSQL } from './schema/SchemaBuilder.js';
import { connectPeeStore, getPeeStorePool } from './storage/PeeStoreBackend.js';
import { initPeeStore } from './storage/initPeeStore.js';
import { getDatabaseConfig } from './config/database-config.js';
import { Pool } from 'pg';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

function createPoolWithRawQuery(pool: Pool): Pool & { rawQuery: (sql: string, values?: any[]) => Promise<{ rows: any[] }> } {
  (pool as any).rawQuery = async (sql: string, values?: any[]) => {
    return await pool.query(sql, values);
  };
  return pool as Pool & { rawQuery: (sql: string, values?: any[]) => Promise<{ rows: any[] }> };
}

export interface BootstrappedServices {
  pipelineEngine: PipelineEngine;
  sessionManager: SessionManager;
  sessionCursorStore: SessionCursorStore;
  dataSourceRegistry: typeof dataSourceRegistry;
  semanticCache: SemanticCache | null;
  crmPool: Pool & { rawQuery: (sql: string, values?: any[]) => Promise<{ rows: any[] }> };
  pmPool: Pool | null;
  peeStoreAvailable: boolean;
}

export async function bootstrap(userId?: string): Promise<BootstrappedServices> {
  const dbConfig = getDatabaseConfig();
  const backend = new PostgresBackend(dbConfig.crmPostgresUrl!);
  await backend.connect();
  const crmPool = createPoolWithRawQuery((backend as any).pool);

  const crmSchema = buildSchemaFromSQL(
    readFileSync(join(__dirname, '../crm_postgres.sql'), 'utf-8'),
    { sessionAnchorTables: ['workspaces'] }
  );

  dataSourceRegistry.register({
    name:        'default',
    displayName: 'CRM',
    kind:        'postgres',
    pool:        crmPool,
    backend:     backend,
    schema:      crmSchema,
    ddlPath:     join(__dirname, '../crm_postgres.sql'),
    description: 'Customer relationship management — accounts, contacts, leads, opportunities, tickets, activities, pipelines'
  });

  let pmPool: Pool | null = null;
  const pmUrl = process.env.PM_DATABASE_URL;
  if (pmUrl) {
    try {
      pmPool = new Pool({ connectionString: pmUrl, max: 10 });
      const pmBackend = new PostgresBackend(pmUrl);
      await pmBackend.connect();
      
      const pmDDLRaw = readFileSync(join(__dirname, '../pee_pm_schema.sql'), 'utf-8');
      const pmDDL = stripCreateTypes(pmDDLRaw);
      const pmSchema = buildSchemaFromSQL(
        pmDDL,
        { sessionAnchorTables: ['workspaces'] }
      );
      
      console.log('[PM Schema] Tables after preprocessing:', 
        [...pmSchema.parsed.tables.keys()]);
      
      dataSourceRegistry.register({
        name:        'pm',
        displayName: 'Project Management',
        kind:        'postgres',
        pool:        pmPool,
        backend:     pmBackend,
        schema:      pmSchema,
        ddlPath:     join(__dirname, '../pee_pm_schema.sql'),
        description: 'Project execution and delivery — projects, milestones, tasks, time logs, team members, project comments'
      });
      
      console.log('[DataSourceRegistry] PM database connected: pee_pm');
    } catch (e) {
      console.warn('[DataSourceRegistry] PM database unavailable — continuing without it:', e);
    }
  }

  dataSourceRegistry.declareCrossDatasourceFKs([
    { fromDatasource: 'pm', fromTable: 'projects', fromColumn: 'crm_account_id',
      toDatasource: 'default', toTable: 'accounts', toColumn: 'id' },
    { fromDatasource: 'pm', fromTable: 'projects', fromColumn: 'crm_opportunity_id',
      toDatasource: 'default', toTable: 'opportunities', toColumn: 'id' },
    { fromDatasource: 'pm', fromTable: 'projects', fromColumn: 'crm_contact_id',
      toDatasource: 'default', toTable: 'contacts', toColumn: 'id' },
    { fromDatasource: 'pm', fromTable: 'projects', fromColumn: 'owner_user_id',
      toDatasource: 'default', toTable: 'users', toColumn: 'id' },
    { fromDatasource: 'pm', fromTable: 'project_members', fromColumn: 'user_id',
      toDatasource: 'default', toTable: 'users', toColumn: 'id' },
    { fromDatasource: 'pm', fromTable: 'tasks', fromColumn: 'assigned_to_user_id',
      toDatasource: 'default', toTable: 'users', toColumn: 'id' },
    { fromDatasource: 'pm', fromTable: 'time_logs', fromColumn: 'user_id',
      toDatasource: 'default', toTable: 'users', toColumn: 'id' },
    { fromDatasource: 'pm', fromTable: 'comments', fromColumn: 'user_id',
      toDatasource: 'default', toTable: 'users', toColumn: 'id' },
    { fromDatasource: 'pm', fromTable: 'project_activity', fromColumn: 'user_id',
      toDatasource: 'default', toTable: 'users', toColumn: 'id' },
  ]);

  const multiSchema  = buildMultiSourceSchema(dataSourceRegistry.all());
  
  console.log('[MultiSourceSchema] All routed tables:', 
    [...multiSchema.tableRouting.keys()].join(', '));
  console.log('[MultiSourceSchema] Table routing entries:', multiSchema.tableRouting.size);
  const combinedSchema = buildCombinedSchemaConfig(multiSchema, dataSourceRegistry);

  let peeStoreAvailable = false;
  try {
    peeStoreAvailable = await connectPeeStore();
    if (peeStoreAvailable) {
      await initPeeStore();
    }
  } catch {
    console.warn('[PeeStore] Persistence unavailable — continuing without it');
  }

  let semanticCache: SemanticCache | null = null;
  if (peeStoreAvailable && process.env.VOYAGE_API_KEY) {
    try {
      const voyage = new VoyageClient(process.env.VOYAGE_API_KEY);

      const cacheConfig = {
        threshold: parseFloat(process.env.SEMANTIC_CACHE_THRESHOLD ?? '0.96'),
        enabled: process.env.SEMANTIC_CACHE_ENABLED !== 'false',
        workspaceId: 1,
        sourceType: 'crm'
      };

      semanticCache = new SemanticCache(
        voyage,
        getPeeStorePool(),
        cacheConfig
      );

      const schemaManager = new SchemaStateManager(getPeeStorePool());
      await schemaManager.checkAndHandleSchemaChange(semanticCache);

      console.log(
        `[SemanticCache] Ready — threshold: ${cacheConfig.threshold}, ` +
        `model: voyage-3, source_type: ${cacheConfig.sourceType}`
      );
    } catch (e) {
      console.warn('[SemanticCache] Init failed — cache disabled:', e);
      semanticCache = null;
    }
  }

  const sessionManager = new SessionManager(
    process.env.ANTHROPIC_API_KEY!,
    userId ?? '1'
  );

  const sessionCursorStore = new SessionCursorStore();

  const engine = new PipelineEngine({
    anthropicApiKey: process.env.ANTHROPIC_API_KEY!,
    schema: combinedSchema,
    storageBackend: backend,
    sessionCursorStore,
    multiSchema,
    budget: {
      maxLLMCalls: 20,
      maxIterations: 100,
      timeoutMs: 60000,
    },
  });

  return {
    pipelineEngine: engine,
    sessionManager,
    sessionCursorStore,
    dataSourceRegistry,
    semanticCache,
    crmPool,
    pmPool,
    peeStoreAvailable,
  };
}
