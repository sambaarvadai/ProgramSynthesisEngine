import readline from 'node:readline';
import { PipelineEngine, type PipelineEngineConfig } from './pipeline-engine.js';
import type { WritePayload } from './nodes/payloads.js';
import { crmSchema } from './schema/crm-schema.js';
import { PostgresBackend } from './storage/index.js';
import { isTabular, isRecord, isScalar, isCollection, isVoid, toTabular } from './core/types/data-value.js';
import type { DataValue } from './core/types/data-value.js';
import { SessionManager } from './session/session-manager.js';
import { SessionCursorStore, extractCursor, buildWhereFromCursor } from './session/SessionCursor.js';
// Import error analyzer for detailed LLM-based error analysis
import { ErrorAnalyzer } from './core/llm/error-analyzer.js';
import { grantStore } from './auth/grant-store.js';
import { auditStore, AuditAction } from './auth/audit-store.js';
import { apiRegistryStore } from './config/api-registry-store.js';
import { getDatabaseConfig } from './config/database-config.js';
import { coerceColumnValue } from './write/coerceColumnValue.js';
import * as dotenv from 'dotenv';
dotenv.config();

/**
 * Check if write intent is complete (has both WHERE and WHAT to update)
 */
function isWriteIntentComplete(payload: WritePayload): boolean {
  if (payload.mode !== 'update') return false;

  // Must know WHERE (what rows to update)
  const hasWhere =
    Object.keys(payload.staticWhere ?? {}).length > 0 ||
    payload.wherePredicate != null ||
    (payload.whereColumns && payload.whereColumns.length > 0) ||
    (payload.upstreamTables && payload.upstreamTables.length > 0);  // upstream query provides WHERE

  if (!hasWhere) return false;

  // Must know WHAT to update (non-system fields)
  const systemFields = [
    'workspace_id', 'created_at', 'updated_at', 'deleted_at'
  ];
  const intentFields = Object.keys(payload.staticValues ?? {})
    .filter(k => !systemFields.includes(k));

  return intentFields.length > 0;
}

async function main() {
  const dbConfig = getDatabaseConfig();
  const backend = new PostgresBackend(dbConfig.crmPostgresUrl!);
  await backend.connect();

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const ask = (q: string): Promise<string> =>
    new Promise(resolve => rl.question(q, resolve));

  // User authentication
  let currentUser: { id: string; username: string; role: string; workspaceId?: number } | null = null;

  async function login() {
    console.log('\n\uFE0F ProgramExecutionEngine CLI');
    console.log('Please log in to continue.\n');
    
    while (true) {
      const username = await ask('Username: ');
      if (!username.trim()) continue;
      
      const password = await ask('Password: ');
      if (!password.trim()) continue;
      
      const user = grantStore.getUserByUsername(username.trim());
      if (!user) {
        // Add audit logging for failed login
        auditStore.log({
          userId: 'unknown',
          username: username.trim(),
          role: 'unknown',
          action: AuditAction.LOGIN,
          status: 'failed',
          error: 'Invalid credentials',
        });
        
        console.log('\n\u274c Invalid username or password. Please try again.\n');
        continue;
      }
      
      // In a real implementation, you'd verify the password hash
      // For now, we'll just accept any password for demo purposes
      currentUser = { id: user.id, username: user.username, role: user.role, workspaceId: user.workspaceId ?? 1 };
      grantStore.updateLastLogin(user.id);
      
      // Add audit logging for successful login
      auditStore.log({
        userId: currentUser.id,
        username: currentUser.username,
        role: currentUser.role,
        action: AuditAction.LOGIN,
        status: 'success',
        details: { loginAt: new Date().toISOString() }
      });
      
      console.log(`\n\u2709 Logged in as ${user.username} (${user.role})\n`);
      return;
    }
  }

  await login();

  // Field collection interface and helpers
  interface FieldPrompt {
    column:       string;
    type:         string;
    required:     boolean;
    defaultValue: string | null;
    enumValues:   string[];
    fkTarget:     { table: string; column: string } | null;
  }

  function buildFieldHint(field: FieldPrompt): string {
    const parts: string[] = [];
    
    // Type label — simplified for readability
    const typeLabel = field.type.startsWith('INT') || field.type === 'SERIAL'
      ? '(integer)'
      : field.type === 'BOOLEAN'
      ? '(true/false)'
      : field.type === 'TIMESTAMPTZ' || field.type === 'DATE'
      ? '(date)'
      : field.type.startsWith('NUMERIC')
      ? '(number)'
      : '(text)';
    
    parts.push(typeLabel.padEnd(12));
    
    // Enum values
    if (field.enumValues.length > 0) {
      parts.push(`[${field.enumValues.join(', ')}]`);
    }
    
    // Default value — strip surrounding quotes from SQL literal
    if (field.defaultValue !== null) {
      const rawDefault = field.defaultValue;
      const displayDefault = typeof rawDefault === 'object' && rawDefault !== null
        ? ((rawDefault as any).expr ?? String(rawDefault))
        : String(rawDefault ?? '').replace(/^'(.*)'$/, '$1');
      parts.push(`default: ${displayDefault}`);
    }
    
    // FK hint
    if (field.fkTarget) {
      parts.push(`FK → ${field.fkTarget.table}.${field.fkTarget.column}`);
    }
    
    return parts.join('  ');
  }

  async function fetchCurrentRow(
    table: string,
    where: Record<string, any>
  ): Promise<Record<string, any> | null> {
    try {
      // Replace scalar WHERE builder with array-aware version:
      const conditions: string[] = [];
      const values: any[] = [];
      let paramIndex = 1;

      for (const [col, val] of Object.entries(where)) {
        if (Array.isArray(val)) {
          if (val.length === 1) {
            conditions.push(`"${col}" = $${paramIndex}`);
            values.push(val[0]);
          } else {
            // Multi-row — skip pre-fetch, it's not a single row
            return null;
          }
        } else {
          conditions.push(`"${col}" = $${paramIndex}`);
          values.push(val);
        }
        paramIndex++;
      }

      const sql = `SELECT * FROM "${table}" WHERE ${conditions.join(' AND ')} LIMIT 1`;

      const result = await backend.rawQuery(sql, values);
      return result.rows[0] ?? null;
    } catch (e) {
      console.warn(`[PreFetch] Could not fetch current row: ${e}`);
      return null;
    }
  }

  function displayCurrentRow(currentRow: Record<string, any>): void {
    console.log('\n📋 Current record:');
    console.log('─'.repeat(60));
    
    for (const [col, val] of Object.entries(currentRow)) {
      if (val === null || val === undefined) continue;
      if (['created_at', 'updated_at', 'deleted_at'].includes(col)) continue;
      if (col === 'id') continue;
      
      const displayVal = typeof val === 'object' 
        ? JSON.stringify(val)
        : String(val);
      
      console.log(`  ${col.padEnd(25)} ${displayVal}`);
    }
    console.log('─'.repeat(60));
    console.log('');
  }

  async function collectFieldsFromUser(
    required: FieldPrompt[],
    optional: FieldPrompt[],
    currentRow: Record<string, any> | null = null
  ): Promise<Record<string, any>> {
    
    const collected: Record<string, any> = {};
    
    if (required.length === 0 && optional.length === 0) {
      return collected;
    }

    if (currentRow) {
      console.log('\n✏️  Edit fields below. Press Enter to keep current value.\n');
    } else {
      console.log('\n📝 To complete this operation, please fill in the following:');
      console.log('   Press Enter to skip optional fields.\n');
    }

    // ── REQUIRED ──────────────────────────────────────
    if (required.length > 0) {
      console.log('REQUIRED');
      for (const field of required) {
        const hint = buildFieldHint(field);
        const currentVal = currentRow?.[field.column];
        const currentDisplay = currentVal !== null && currentVal !== undefined
          ? String(currentVal)
          : null;
        const currentHint = currentDisplay
          ? ` (current: ${currentDisplay})` 
          : '';
        
        let value: string | undefined;
        
        // Loop until user provides a non-empty value
        while (!value?.trim()) {
          value = await ask(
            `  ${field.column.padEnd(20)} ${hint}${currentHint}: ` 
          );
          if (!value?.trim()) {
            if (currentRow && currentDisplay !== null) {
              // Keep current value
              collected[field.column] = currentVal;
              break;
            }
            console.log(`  ⚠ ${field.column} is required. Please enter a value.`);
          }
        }
        
        if (value?.trim()) {
          try {
            const coerced = coerceColumnValue(
              value.trim(), 
              field.type, 
              field.enumValues.length > 0 ? field.enumValues : undefined
            );
            if (coerced !== null) collected[field.column] = coerced;
          } catch (e) {
            console.log(`  ✗ ${(e as Error).message}`);
            // Re-add to required and retry — simplest approach
            // for now: just store raw and let validator catch it
            collected[field.column] = value.trim();
          }
        }
      }
      console.log('');
    }

    // ── OPTIONAL ──────────────────────────────────────
    if (optional.length > 0) {
      console.log(currentRow ? 'FIELDS  (press Enter to keep current value)' : 'OPTIONAL  (press Enter to skip)');
      for (const field of optional) {
        const hint = buildFieldHint(field);
        const currentVal = currentRow?.[field.column];
        const currentDisplay = currentVal !== null && currentVal !== undefined
          ? String(currentVal)
          : null;
        const currentHint = currentDisplay
          ? ` (current: ${currentDisplay})` 
          : '';
        
        const value = await ask(
          `  ${field.column.padEnd(20)} ${hint}${currentHint}: ` 
        );
        
        if (!value?.trim()) {
          if (currentRow && currentDisplay !== null) {
            // Keep current value
            collected[field.column] = currentVal;
          }
          // else: User skipped — do not add to collected
          // DB default or NULL will be used
          continue;
        }

        try {
          const coerced = coerceColumnValue(
            value.trim(),
            field.type,
            field.enumValues.length > 0 ? field.enumValues : undefined
          );
          if (coerced !== null) collected[field.column] = coerced;
        } catch (e) {
          console.log(`  ✗ Invalid: ${(e as Error).message}. Field skipped.`);
          // Skip invalid optional values silently
        }
      }
    }

    console.log('');
    return collected;
  }

  // Now create session manager with authenticated user
  const sessionManager = new SessionManager(process.env.ANTHROPIC_API_KEY!, currentUser!.id);

  // Create session cursor store for lightweight result pagination
  const sessionCursorStore = new SessionCursorStore();

  // Initialize error analyzer for detailed LLM-based error analysis
  const errorAnalyzer = new ErrorAnalyzer({
    anthropicApiKey: process.env.ANTHROPIC_API_KEY!
  });

  // Build SchemaConfig from BuiltSchema for the engine
  // Convert DDLParser RawTableMap → SchemaConfig tables Map
  // SchemaConfig table shape: { columns: Array, primaryKey: string[], ... }
  // DDLParser table shape:    { columns: Map, foreignKeys: [], ... }
  
  const schemaConfigTables = new Map<string, any>();
  
  for (const [tableName, rawTable] of crmSchema.parsed.tables) {
    // Convert columns Map → Array in SchemaConfig format
    const columns = Array.from(rawTable.columns.entries()).map(
      ([colName, colDef]) => ({
        name:        colName,
        type:        { kind: colDef.type },   // SchemaConfig wraps type in { kind }
        nullable:    colDef.nullable,
        primaryKey:  colDef.primaryKey,
        unique:      colDef.unique,
        description: undefined,
        examples:    undefined,
      })
    );
    
    // Extract primary key column names
    const primaryKey = columns
      .filter(c => c.primaryKey)
      .map(c => c.name);
    
    schemaConfigTables.set(tableName, {
      columns,
      primaryKey,
      description: undefined,
      alias:       undefined,
    });
  }

  // Convert FKGraph edges → flat FK array
  const foreignKeys: any[] = [];
  for (const edges of crmSchema.parsed.fkGraph.outbound.values()) {
    for (const edge of edges) {
      foreignKeys.push({
        fromTable:  edge.fromTable,
        fromColumn: edge.fromColumn,
        toTable:    edge.toTable,
        toColumn:   edge.toColumn,
        onDelete:   edge.onDelete,
      });
    }
  }

  const engineSchema = {
    tables:      schemaConfigTables,
    foreignKeys,
    version:     '1',
  };

  const engine = new PipelineEngine({
    anthropicApiKey: process.env.ANTHROPIC_API_KEY!,
    schema: engineSchema as any,
    storageBackend: backend,
    sessionCursorStore,
    budget: {
      maxLLMCalls: 20,
      maxIterations: 100,
      timeoutMs: 60000,
    },
  });

  // API registry command parsing
  async function handleApiCommand(input: string): Promise<boolean> {
    const trimmed = input.trim().toLowerCase();
    
    // List all APIs
    if (trimmed === 'list apis') {
      const endpoints = apiRegistryStore.listAll();
      
      console.log('\n\ud83d\udccb Registered API Endpoints:');
      console.log('ID                     | Method | URL                           | Description');
      console.log('---------------------- | ------ | ----------------------------- | -----------');
      
      for (const endpoint of endpoints) {
        const id = endpoint.id.padEnd(22);
        const method = endpoint.method.padEnd(6);
        const url = endpoint.baseUrl.padEnd(29);
        const description = endpoint.description.length > 30 
          ? endpoint.description.substring(0, 27) + '...'
          : endpoint.description;
        
        console.log(`${id} | ${method} | ${url} | ${description}`);
        console.log(`  Accepts: ${endpoint.requestFields.length} fields, Returns: ${endpoint.responseFields.length} fields`);
      }
      
      if (endpoints.length === 0) {
        console.log('No API endpoints registered. Use "api:seed" to register default endpoints.');
      }
      console.log();
      return true;
    }
    
    // Show specific API details
    if (trimmed.startsWith('show api ')) {
      const apiId = input.slice('show api '.length).trim();
      if (!apiId) {
        console.log('\u274c Usage: "show api [id]"');
        return true;
      }
      
      const endpoint = apiRegistryStore.getById(apiId);
      if (!endpoint) {
        console.log(`\u274c API '${apiId}' not found.`);
        return true;
      }
      
      console.log('\n\ud83d\udccb API Endpoint Details:');
      console.log(apiRegistryStore.getSchemaContext(endpoint));
      console.log();
      return true;
    }
    
    // Find APIs that accept a specific field
    if (trimmed.startsWith('apis for field ')) {
      const fieldName = input.slice('apis for field '.length).trim();
      if (!fieldName) {
        console.log('\u274c Usage: "apis for field [fieldName]"');
        return true;
      }
      
      const endpoints = apiRegistryStore.findByInputField(fieldName);
      
      console.log(`\n\ud83d\udd0d APIs that accept field '${fieldName}':`);
      
      if (endpoints.length === 0) {
        console.log(`No APIs accept field '${fieldName}'.`);
      } else {
        console.log('ID                     | Method | URL                           | Description');
        console.log('---------------------- | ------ | ----------------------------- | -----------');
        
        for (const endpoint of endpoints) {
          const id = endpoint.id.padEnd(22);
          const method = endpoint.method.padEnd(6);
          const url = endpoint.baseUrl.padEnd(29);
          const description = endpoint.description.length > 30 
            ? endpoint.description.substring(0, 27) + '...'
            : endpoint.description;
          
          console.log(`${id} | ${method} | ${url} | ${description}`);
        }
      }
      console.log();
      return true;
    }
    
    return false;
  }

  // Access management command parsing
  async function handleAccessCommand(input: string): Promise<boolean> {
    const trimmed = input.trim().toLowerCase();
    
    // User commands
    if (trimmed.startsWith('request access to ')) {
      const target = input.slice('request access to '.length).trim();
      if (!target) {
        console.log('\u274c Usage: "request access to [table]" or "request access to [table].[column]"');
        return true;
      }
      
      const [table, column] = target.split('.');
      if (!table) {
        console.log('\u274c Invalid table name');
        return true;
      }
      
      const requestId = grantStore.requestAccess(currentUser!.id, table, column);
      const targetDesc = column ? `${table}.${column}` : table;
      console.log(`\u2709 Access request submitted for ${targetDesc}. An admin will review it.`);
      console.log(`Request ID: ${requestId}`);
      return true;
    }
    
    if (trimmed === 'my access' || trimmed === 'what can i access') {
      console.log('\n\ud83d\udccb Your Current Access:');
      console.log('Table          | Read | Write');
      console.log('-------------- | ---- | -----');
      
      // Get all tables in the schema
      const schema = crmSchema;
      let hasAnyAccess = false;
      
      for (const [tableName] of schema.parsed.tables) {
        const canRead = grantStore.checkTableAccess(currentUser!.id, tableName, 'read');
        const canWrite = grantStore.checkTableAccess(currentUser!.id, tableName, 'write');
        
        if (canRead || canWrite) {
          hasAnyAccess = true;
          const readSymbol = canRead ? '\u2713' : '\u2717';
          const writeSymbol = canWrite ? '\u2713' : '\u2717';
          console.log(`${tableName.padEnd(14)} | ${readSymbol}   | ${writeSymbol}`);
        }
      }
      
      if (!hasAnyAccess) {
        console.log('No table access granted. Use "request access to [table]" to request access.');
      }
      console.log();
      return true;
    }
    
    // Audit log commands
    if (trimmed === 'audit log' || trimmed === 'my audit log') {
      const entries = auditStore.getForUser(currentUser!.id, 50);
      if (entries.length === 0) {
        console.log('\nNo audit history yet.');
      } else {
        console.log('\n\ud83d\udcdc Your Audit History:');
        console.log(auditStore.formatTable(entries));
      }
      console.log();
      return true;
    }
    
    if (trimmed.startsWith('audit log ')) {
      const target = input.slice('audit log '.length).trim();
      
      if (target === 'all') {
        // Admin only command
        if (currentUser!.role !== 'admin') {
          console.log('\u274c Only admins can view all audit logs.');
          return true;
        }
        
        const entries = auditStore.getAll(100);
        console.log('\n\ud83d\udcdc All Audit Logs:');
        console.log(auditStore.formatTable(entries));
        console.log();
        return true;
      } else {
        // Audit log for specific username (admin only)
        if (currentUser!.role !== 'admin') {
          console.log('\u274c Only admins can view other users\' audit logs.');
          return true;
        }
        
        const targetUser = grantStore.getUserByUsername(target);
        if (!targetUser) {
          console.log(`\u274c User '${target}' not found.`);
          return true;
        }
        
        const entries = auditStore.getForUser(targetUser.id, 100);
        console.log(`\n\ud83d\udcdc Audit History for ${target}:`);
        console.log(auditStore.formatTable(entries));
        console.log();
        return true;
      }
    }
    
    // Admin commands
    if (currentUser!.role !== 'admin') {
      if (trimmed.startsWith('pending requests') || 
          trimmed.startsWith('approve request') || 
          trimmed.startsWith('deny request')) {
        console.log('\u274c Only admins can manage access requests.');
        return true;
      }
    }
    
    if (currentUser!.role === 'admin') {
      if (trimmed === 'pending requests') {
        const pending = grantStore.listPendingRequests();
        if (pending.length === 0) {
          console.log('\n\ud83d\udccb No pending access requests.');
        } else {
          console.log('\n\ud83d\udccb Pending Access Requests:');
          console.log('ID              | User    | Table/Column     | Requested At');
          console.log('---------------- | ------- | ---------------- | ------------');
          
          for (const request of pending) {
            const user = grantStore.getUserById(request.userId);
            const username = user?.username || 'Unknown';
            const target = request.columnName ? `${request.tableName}.${request.columnName}` : request.tableName;
            const requestedAt = new Date(request.requestedAt).toLocaleString();
            
            console.log(`${request.id.slice(0, 14).padEnd(14)} | ${username.padEnd(7)} | ${target.padEnd(14)} | ${requestedAt}`);
          }
        }
        console.log();
        return true;
      }
      
      if (trimmed.startsWith('approve request ')) {
        const requestId = input.slice('approve request '.length).trim();
        if (!requestId) {
          console.log('\u274c Usage: "approve request [requestId]"');
          return true;
        }
        
        try {
          grantStore.approveAccess(requestId, currentUser!.id);
          console.log(`\u2709 Request ${requestId} approved and access granted.`);
        } catch (error) {
          console.log(`\u274c Error approving request: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
        return true;
      }
      
      if (trimmed.startsWith('deny request ')) {
        const requestId = input.slice('deny request '.length).trim();
        if (!requestId) {
          console.log('\u274c Usage: "deny request [requestId]"');
          return true;
        }
        
        try {
          grantStore.denyAccess(requestId, currentUser!.id);
          console.log(`\u2709 Request ${requestId} denied.`);
        } catch (error) {
          console.log(`\u274c Error denying request: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
        return true;
      }
    }
    
    return false; // Not an access command
  }

  console.log('Describe a workflow and the engine will plan and execute it.');
  console.log('Type "exit" to quit.\n');

  console.log('\ud83d\udd52 Connected to Postgres:', process.env.DATABASE_URL?.replace(/:\/\/.*@/, '://***@'));
  console.log('\ud83d\udcca Schema:', [...crmSchema.parsed.tables.keys()].join(', '));
  console.log(`\ud83d\udd11 Session ID: ${sessionManager.getSessionId()}`);
  console.log('\ud83d\udce7 Email: Resend API', process.env.RESEND_API_KEY ? '??' : '?? (set RESEND_API_KEY)');
  console.log();

  process.on('SIGINT', async () => {
    // Add logout audit logging on SIGINT
    if (currentUser) {
      auditStore.log({
        userId: currentUser.id,
        username: currentUser.username,
        role: currentUser.role,
        action: AuditAction.LOGOUT,
        status: 'success',
      });
    }
    
    await backend.disconnect();
    rl.close();
    process.exit(0);
  });

  while (true) {
    const description = await ask(`${currentUser!.username}> `);
    if (description.trim() === 'exit') break;
    if (!description.trim()) continue;

    try {
      // Check for API registry commands first
      const isApiCommand = await handleApiCommand(description);
      if (isApiCommand) {
        continue; // Skip NL pipeline for API commands
      }

      // Check for access management commands next
      const isAccessCommand = await handleAccessCommand(description);
      if (isAccessCommand) {
        continue; // Skip NL pipeline for access commands
      }

      // Plan phase
      console.log('\n\ud83d\udccb Planning...');
      const sessionHistory = sessionManager.getHistory();
      const plan = await engine.plan(description, { sessionHistory, userId: currentUser!.id });

      // Check if this is conversational (no execution steps)
      const isConversational = plan.intent.steps.length === 0;

      // Show plan only for non-conversational inputs
      if (!isConversational) {
        console.log('\n' + engine.formatPlan(plan));
      }

      if (plan.compilationErrors.length > 0) {
        // Check if this is a write completeness error that we can handle interactively
        const writeIncompleteError = plan.compilationErrors.find(err => err.code === 'WRITE_INCOMPLETE');
        const writeFieldUnresolvableError = plan.compilationErrors.find(err => err.code === 'WRITE_FIELD_UNRESOLVABLE');
        
        if (writeIncompleteError && writeIncompleteError.missingColumns) {
          // Get the write payload to determine table and column types
          const writeNode = plan.graph.nodes.get(writeIncompleteError.stepId!);
          const writePayload = writeNode?.kind === 'write' ? writeNode.payload as WritePayload : null;
          const tableColumns = writePayload?.table ? (crmSchema as any).parsed?.tables.get(writePayload.table)?.columns : undefined;

          // Detect full-table UPDATE (no WHERE clause specified)
          // Show a confirmation gate instead of a field collection form
          const isNoWhereUpdate =
            writePayload?.mode === 'update' &&
            Object.keys(writePayload.staticWhere ?? {}).length === 0 &&
            !writePayload.wherePredicate &&
            (writePayload.whereColumns ?? []).length === 0;

          if (isNoWhereUpdate) {
            const tableRowCount = await backend
              .rawQuery(`SELECT COUNT(*) FROM "${writePayload!.table}"`)
              .then((r: { rows: any[] }) => r.rows[0].count)
              .catch(() => 'unknown');

            console.warn(
              `\n⚠️  WARNING: No filter specified.`
            );
            console.warn(
              `   This will update ALL ${tableRowCount} rows ` +
              `in '${writePayload!.table}'.`
            );
            console.warn(`   Type "yes" to confirm, anything else cancels.\n`);

            const confirmAll = await ask('> ');
            if (confirmAll.trim().toLowerCase() !== 'yes') {
              console.log('Cancelled.');
              sessionCursorStore.clear();
              continue;
            }

            // User confirmed — inject updated_at and proceed to execution
            // without showing any field form
            if (writePayload!.staticValues) {
              const hasUpdatedAt = (crmSchema as any).parsed.tables
                .get(writePayload!.table)?.columns.has('updated_at');
              if (hasUpdatedAt) {
                writePayload!.staticValues['updated_at'] = 'NOW()';
              }
            }
            // Clear compilationErrors and skip form logic
            plan.compilationErrors = [];
            // Fall through to "Execute this plan?" prompt
          }

          // Check if write intent is complete (has both WHERE and WHAT to update)
          // Do this FIRST — before printing anything to avoid unnecessary warnings
          if (writePayload && isWriteIntentComplete(writePayload)) {
            // Silent — no warning shown to user at all
            if (writePayload.staticValues) {
              const hasUpdatedAt = (crmSchema as any).parsed.tables
                .get(writePayload.table)?.columns.has('updated_at');
              if (hasUpdatedAt) {
                writePayload.staticValues['updated_at'] = 'NOW()';
              }
            }
            // Clear compilationErrors since intent is complete
            plan.compilationErrors = [];
            // Skip form entirely - continue to execution
          } else {
            // Only reach here when there is genuinely something missing
            // Now safe to show the warning
            console.log('\u26A0\ufe0f  Write completeness check:');
            console.log(writeIncompleteError.message);
            // Show form — genuinely missing intent
          
            // Detect single-row UPDATE
          const isSingleRowUpdate = 
            writePayload?.mode === 'update' &&
            (
              Object.keys(writePayload.staticWhere ?? {}).length === 1 ||
              (writePayload.whereColumns?.length === 1 && 
               Object.keys(writePayload.staticValues ?? {}).some(k => 
                 k === 'id' || k.endsWith('_id')
               ))
            );

          // Pre-fetch current row for single-row UPDATE
          const whereClause = writePayload?.staticWhere ?? {};
          const currentRow = isSingleRowUpdate && writePayload?.table
            ? await fetchCurrentRow(writePayload.table, whereClause)
            : null;

          // Display current row if found
          if (currentRow) {
            displayCurrentRow(currentRow);
          } else if (isSingleRowUpdate) {
            console.warn(
              '⚠️  Multi-row update detected. ' +
              'Changes will apply to all matching rows.'
            );
          }
          
          // Build field manifest
          const requiredFields: FieldPrompt[] = [];
          const optionalFields: FieldPrompt[] = [];

          for (const col of writeIncompleteError.missingColumns) {
            // Skip auto-managed columns
            if (['id', 'created_at', 'updated_at', 'deleted_at', 'converted_at'].includes(col.column)) {
              continue;
            }

            const colDef = tableColumns?.get(col.column);
            const colTraits = writePayload?.table
              ? (crmSchema as any).traits.get(writePayload.table)?.get(col.column)
              : undefined;

            const field: FieldPrompt = {
              column:       col.column,
              type:         colDef?.type ?? 'TEXT',
              required:     writePayload?.mode === 'insert' 
                ? !('nullable' in col) || !col.nullable  // INSERT: required = NOT NULL
                : false,  // UPDATE: no required fields (partial updates valid)
              defaultValue: colDef?.defaultRaw ?? null,
              enumValues:   colTraits?.enumValues ?? [],
              fkTarget:     colTraits?.foreignKey
                ? { table: colTraits.foreignKey.references, 
                    column: colTraits.foreignKey.column }
                : null,
            };

            if (field.required) {
              requiredFields.push(field);
            } else {
              optionalFields.push(field);
            }
          }

          // Collect values from user using structured form
          const collectedValues = await collectFieldsFromUser(
            requiredFields, 
            optionalFields,
            currentRow
          );
          
          // For UPDATE: diff against current row and only include changed fields
          let finalValues = collectedValues;
          if (isSingleRowUpdate && currentRow) {
            const changedFields: Record<string, any> = {};
            
            for (const [col, newVal] of Object.entries(collectedValues)) {
              const oldVal = currentRow[col];
              
              // Include if value changed or was not in current row
              const changed = String(newVal) !== String(oldVal ?? '');
              if (changed) {
                changedFields[col] = newVal;
              } else {
                // For required fields, include unchanged values to preserve them
                const isRequired = writeIncompleteError.missingColumns.some(
                  mc => 'description' in mc && mc.column === col && !mc.nullable
                );
                if (isRequired) {
                  changedFields[col] = newVal;
                }
              }
            }
            
            // Always include the intent's explicit changes
            const intentFields = writePayload?.staticValues ?? {};
            for (const [col, val] of Object.entries(intentFields)) {
              // Skip auto-injected system columns
              if (['workspace_id', 'created_at', 'updated_at'].includes(col)) continue;
              changedFields[col] = val;
            }
            
            console.log('\n📝 Changes to apply:');
            for (const [col, val] of Object.entries(changedFields)) {
              if (col === 'updated_at') continue;
              const oldVal = currentRow[col];
              console.log(
                `  ${col.padEnd(25)} ${String(oldVal ?? 'null')} → ${String(val)}` 
              );
            }
            console.log('');
            
            finalValues = changedFields;
          }
          
          // Patch the existing plan in place - no re-planning
          const enrichedPlan = engine.planWithMissingValues(
            plan,                                    // pass full plan
            writeIncompleteError.stepId!,
            finalValues,
            currentUser!.id
          );
          
          plan.intent = enrichedPlan.intent;
          plan.graph = enrichedPlan.graph;
          plan.compilationErrors = enrichedPlan.compilationErrors;
          
          if (plan.compilationErrors.length > 0) {
            // Still missing something - show remaining gaps
            console.log('\n\u26A0\ufe0f  Still missing required values:');
            for (const err of plan.compilationErrors) {
              console.log(`  - ${err.message}`);
            }
            continue;
          }
          
          // Plan is now complete - show it and ask for confirmation
          console.log('\n' + engine.formatPlan(plan));
          console.log('\n\u2705 All required values provided!');
          const confirmAfterFill = await ask('\nExecute this plan? (y/n) ');
          if (confirmAfterFill.trim() !== 'y') continue;
          // fall through to execution
          }  // Close else block for form display
        } else if (writeFieldUnresolvableError && writeFieldUnresolvableError.missingColumns && 'table' in writeFieldUnresolvableError.missingColumns[0]) {
          console.log(`\n\u26A0\ufe0f  Write field resolution failed for step '${writeFieldUnresolvableError.stepId}':`);
          console.log(writeFieldUnresolvableError.message);
          
          // Resolve table column definitions once for filtering
          const _writeNode = plan.graph.nodes.get(writeFieldUnresolvableError.stepId!);
          const _writePayload = _writeNode?.kind === 'write'
            ? _writeNode.payload as WritePayload
            : null;
          const _tableColumns = _writePayload?.table
            ? (crmSchema as any).parsed?.tables.get(_writePayload.table)?.columns
            : undefined;

          // Detect single-row UPDATE
          const isSingleRowUpdate = 
            _writePayload?.mode === 'update' &&
            (
              Object.keys(_writePayload.staticWhere ?? {}).length === 1 ||
              (_writePayload.whereColumns?.length === 1 && 
               Object.keys(_writePayload.staticValues ?? {}).some(k => 
                 k === 'id' || k.endsWith('_id')
               ))
            );

          // Pre-fetch current row for single-row UPDATE
          const whereClause = _writePayload?.staticWhere ?? {};
          const currentRow = isSingleRowUpdate && _writePayload?.table
            ? await fetchCurrentRow(_writePayload.table, whereClause)
            : null;

          // Display current row if found
          if (currentRow) {
            displayCurrentRow(currentRow);
          } else if (isSingleRowUpdate) {
            console.warn(
              '⚠️  Multi-row update detected. ' +
              'Changes will apply to all matching rows.'
            );
          }

          // Build field manifest from ALL missing columns
          const requiredFields: FieldPrompt[] = [];
          const optionalFields: FieldPrompt[] = [];

          for (const col of writeFieldUnresolvableError.missingColumns) {
            if (!('table' in col)) continue;

            // Skip auto-managed columns
            if (['id', 'created_at', 'updated_at', 'deleted_at', 'converted_at'].includes(col.column)) {
              continue;
            }

            const colDef = _tableColumns?.get(col.column);
            const colTraits = _writePayload?.table
              ? (crmSchema as any).traits?.get(_writePayload.table)?.get(col.column)
              : undefined;

            const field: FieldPrompt = {
              column:       col.column,
              type:         colDef?.type ?? 'TEXT',
              required:     _writePayload?.mode === 'insert' 
                ? !!(colDef && !colDef.nullable && colDef.defaultRaw === null)  // INSERT: required = NOT NULL
                : false,  // UPDATE: no required fields (partial updates valid)
              defaultValue: colDef?.defaultRaw ?? null,
              enumValues:   colTraits?.enumValues ?? [],
              fkTarget:     colTraits?.foreignKey
                ? { table: colTraits.foreignKey.references, 
                    column: colTraits.foreignKey.column }
                : null,
            };

            if (field.required) {
              requiredFields.push(field);
            } else {
              optionalFields.push(field);
            }
          }

          // Collect values from user using structured form
          const collectedValues = await collectFieldsFromUser(
            requiredFields, 
            optionalFields,
            currentRow
          );
          
          // For UPDATE: diff against current row and only include changed fields
          let finalValues = collectedValues;
          if (isSingleRowUpdate && currentRow) {
            const changedFields: Record<string, any> = {};
            
            for (const [col, newVal] of Object.entries(collectedValues)) {
              const oldVal = currentRow[col];
              
              // Include if value changed or was not in current row
              const changed = String(newVal) !== String(oldVal ?? '');
              if (changed) {
                changedFields[col] = newVal;
              } else {
                // For required fields, include unchanged values to preserve them
                const isRequired = writeFieldUnresolvableError.missingColumns.some(
                  mc => 'description' in mc && mc.column === col && !mc.nullable
                );
                if (isRequired) {
                  changedFields[col] = newVal;
                }
              }
            }
            
            // Always include the intent's explicit changes
            const intentFields = _writePayload?.staticValues ?? {};
            for (const [col, val] of Object.entries(intentFields)) {
              // Skip auto-injected system columns
              if (['workspace_id', 'created_at', 'updated_at'].includes(col)) continue;
              changedFields[col] = val;
            }
            
            console.log('\n📝 Changes to apply:');
            for (const [col, val] of Object.entries(changedFields)) {
              if (col === 'updated_at') continue;
              const oldVal = currentRow[col];
              console.log(
                `  ${col.padEnd(25)} ${String(oldVal ?? 'null')} → ${String(val)}` 
              );
            }
            console.log('');
            
            finalValues = changedFields;
          }
          
          // Patch the existing plan in place - add missing values to staticValues
          const writeNode = plan.graph.nodes.get(writeFieldUnresolvableError.stepId!);
          if (writeNode && writeNode.kind === 'write') {
            const payload = writeNode.payload as WritePayload;
            payload.staticValues = { ...payload.staticValues, ...finalValues };
            
            // Clear the compilation errors since we've resolved the missing fields
            plan.compilationErrors = [];
          }
          
          // Plan is now complete - show it and ask for confirmation
          console.log('\n' + engine.formatPlan(plan));
          console.log('\n\u2705 All required values provided!');
          const confirmAfterFill = await ask('\nExecute this plan? (y/n) ');
          if (confirmAfterFill.trim() !== 'y') continue;
          // fall through to execution
        } else {
          // Handle other compilation errors normally
          console.log('\n\u26A0\ufe0f  Compilation errors:');
          for (const err of plan.compilationErrors) {
            console.log(`  - ${err.message}`);
          }
          console.log('Please refine your description.\n');
          continue;
        }
      }

      if (isConversational) {
        // Just show the conversational response
        console.log(`\n${plan.intent.description}`);
        console.log();
        
        // Add turn to session history
        sessionManager.addTurn(description, plan.intent, plan, true);
        continue;
      }

      // Confirm for non-conversational inputs
      const hasWriteNode = plan.intent.steps.some(s => s.kind === 'write');
      const cursor = sessionCursorStore.get();

      if (hasWriteNode && cursor) {
        // Build the where result to show accurate count and determine if bulk
        let affectedDesc = `~${cursor.rowCount} rows`;
        let isBulk = cursor.rowCount > 50;

        try {
          const whereResult = buildWhereFromCursor(cursor, 1);
          isBulk = whereResult.isBulk;
        } catch (e) {
          // If buildWhereFromCursor fails, fall back to rowCount heuristic
          isBulk = cursor.rowCount > 50;
        }

        if (isBulk) {
          console.warn(
            `\n⚠️  This will affect ${affectedDesc} in ` +
            `'${cursor.table}' matching: ${cursor.description}`
          );
          console.warn(
            `   This cannot be undone. Type "yes" to confirm.\n`
          );

          const confirmBulk = await ask('> ');
          if (confirmBulk.trim().toLowerCase() !== 'yes') {
            console.log('Cancelled.');
            sessionCursorStore.clear();
            continue;
          }
        }
      }

      // Guard against no-WHERE update
      const writeStep = plan.intent.steps.find(s => s.kind === 'write');
      if (writeStep && writeStep.config) {
        const writePayload = writeStep.config as WritePayload;
        if (writePayload?.mode === 'update') {
          const hasWhere =
            Object.keys(writePayload.staticWhere ?? {}).length > 0 ||
            writePayload.wherePredicate != null ||
            (writePayload.whereColumns && writePayload.whereColumns.length > 0);

          if (!hasWhere) {
            console.warn(
              '\n⚠️  WARNING: This UPDATE has no WHERE clause.\n' +
              `   It will modify ALL rows in "${writePayload.table}".\n` +
              '   Are you sure? (type "yes" to confirm, anything else cancels)\n'
            );
            const confirmAllRows = await ask('> ');
            if (confirmAllRows.trim().toLowerCase() !== 'yes') {
              console.log('Cancelled.');
              sessionCursorStore.clear();
              continue;
            }
          }
        }
      }

      const confirm = await ask('\nExecute this plan? (y/n/refine) ');

      if (confirm.trim() === 'n') {
        sessionCursorStore.clear();
        continue;
      }

      if (confirm.trim() === 'refine') {
        const feedback = await ask('What should be changed? ');
        const refined = await engine.generator.refine(plan.intent, feedback);
        console.log('\nRefined intent:');
        console.log(JSON.stringify(refined.intent, null, 2));
        const confirm2 = await ask('\nExecute refined plan? (y/n) ');
        if (confirm2.trim() !== 'y') continue;
        // recompile and execute refined intent
        const recompiled = engine.compiler.compile(refined.intent);
        plan.graph = recompiled.graph;
        plan.intent = refined.intent;
      }

      if (confirm.trim() !== 'y' && confirm.trim() !== 'refine') continue;

      // Execute phase
      console.log('\n⚡ Executing...');
      const startMs = Date.now();
      const result = await engine.execute(plan);
      const durationMs = Date.now() - startMs;

      // Show result
      console.log(`\n✅ Completed in ${durationMs}ms`);
      console.log(`Status: ${result.execution.status}`);

      if (result.execution.outputs.size > 0) {
        console.log('\nOutputs:');
        for (const [key, dv] of result.execution.outputs) {
          if (isCollection(dv)) {
            // Display collection as a unified table
            console.log(`\n  ${key} (${dv.data.length} items):`);
            
            // Convert collection to tabular for display
            const rs = toTabular(dv);
            if (rs.rows.length === 0) {
              console.log('  (empty)');
            } else {
              // Print column headers
              const cols = rs.schema.columns.map((c: any) => c.name);
              const colWidths = cols.map((c: string) =>
                Math.max(
                  c.length,
                  ...rs.rows
                    .slice(0, 20)
                    .map((r: any) => String(r[c] ?? '').length),
                ),
              );
              const header = cols
                .map((c: string, i: number) => c.padEnd(colWidths[i]))
                .join(' | ');
              const divider = colWidths.map((w: number) => '-'.repeat(w)).join('-+-');
              console.log('  ' + header);
              console.log('  ' + divider);
              // Print up to 20 rows
              for (const row of rs.rows.slice(0, 20)) {
                const line = cols
                  .map((c: string, i: number) =>
                    String(row[c] ?? '').padEnd(colWidths[i]),
                  )
                  .join(' | ');
                console.log('  ' + line);
              }
              if (rs.rows.length > 20) {
                console.log(`  ... and ${rs.rows.length - 20} more rows`);
              }
            }
          } else if (isTabular(dv) || isRecord(dv)) {
            // Convert to tabular for display
            const rs = toTabular(dv);
            console.log(`\n  ${key} (${rs.rows.length} rows):`);
            if (rs.rows.length === 0) {
              console.log('  (empty)');
            } else {
              // Print column headers
              const cols = rs.schema.columns.map((c: any) => c.name);
              const colWidths = cols.map((c: string) =>
                Math.max(
                  c.length,
                  ...rs.rows
                    .slice(0, 20)
                    .map((r: any) => String(r[c] ?? '').length),
                ),
              );
              const header = cols
                .map((c: string, i: number) => c.padEnd(colWidths[i]))
                .join(' | ');
              const divider = colWidths.map((w: number) => '-'.repeat(w)).join('-+-');
              console.log('  ' + header);
              console.log('  ' + divider);
              // Print up to 20 rows
              for (const row of rs.rows.slice(0, 20)) {
                const line = cols
                  .map((c: string, i: number) =>
                    String(row[c] ?? '').padEnd(colWidths[i]),
                  )
                  .join(' | ');
                console.log('  ' + line);
              }
              if (rs.rows.length > 20) {
                console.log(`  ... and ${rs.rows.length - 20} more rows`);
              }
            }
          } else if (isScalar(dv)) {
            console.log(`${key}: ${dv.data}`);
          } else if (isVoid(dv)) {
            console.log(`${key}: (no output)`);
          } else {
            console.log(`${key}:`, JSON.stringify(dv, null, 2).slice(0, 300));
          }
        }
      }

      console.log('\nNode summary:');
      for (const [nodeId, nodeState] of result.execution.nodeStates) {
        if (nodeId.startsWith('_')) continue; // skip internal nodes
        const icon =
          nodeState.status === 'completed'
            ? '✔'
            : nodeState.status === 'skipped'
              ? '⊘'
              : nodeState.status === 'failed'
                ? '✖'
                : '?';
        const ms =
          nodeState.completedAt && nodeState.startedAt
            ? `${nodeState.completedAt - nodeState.startedAt}ms`
            : '';
        console.log(`  ${icon} ${nodeId} (${ms})`);
      }

      // Add turn to session history for workflow execution
      sessionManager.addTurn(description, plan.intent, plan, false);

      console.log();
    } catch (error) {
      console.log();
      
      // Check for FK validation errors - display cleanly without AI analysis
      if ((error as Error).message.startsWith('FK validation failed')) {
        console.log('\n❌ Cannot complete this operation:\n');
        console.log((error as Error).message);
        console.log(
          '\nCheck that all referenced records exist before retrying.'
        );
        // Do NOT show the AI error analysis for FK violations
        // They are self-explanatory
        continue;
      }
      
      console.log('\nError:', (error as Error).message);
      console.log('\nStack trace:');
      console.log((error as Error).stack);
      
      // Get detailed error analysis from LLM
      console.log('\nAnalyzing error with AI...');
      try {
        const analysis = await errorAnalyzer.analyzeError(error as Error, {
          operation: 'pipeline_execution',
          additionalInfo: 'Error occurred during pipeline execution in Program Synthesis Engine'
        });
        
        console.log(errorAnalyzer.formatForDisplay(analysis));
      } catch (analysisError) {
        console.log('\nFailed to get AI analysis:', analysisError instanceof Error ? analysisError.message : String(analysisError));
        console.log('\nBasic error information:');
      }
      
      console.log();
    }
  }

  // Add logout audit logging before closing
  if (currentUser) {
    const user = currentUser as { id: string; username: string; role: string };
    auditStore.log({
      userId: user.id,
      username: user.username,
      role: user.role,
      action: AuditAction.LOGOUT,
      status: 'success',
    });
  }
  
  rl.close();
  await backend.disconnect();
  console.log('Bye!');
}

main().catch(console.error);
