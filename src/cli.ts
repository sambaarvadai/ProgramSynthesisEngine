import readline from 'node:readline';
import { PipelineEngine } from './pipeline-engine.js';
import { crmSchema } from './config/index.js';
import { PostgresBackend } from './storage/index.js';
import { isTabular, isRecord, isScalar, isCollection, isVoid, toTabular } from './core/types/data-value.js';
import type { DataValue } from './core/types/data-value.js';
import { SessionManager } from './session/session-manager.js';
// Import error analyzer for detailed LLM-based error analysis
import { ErrorAnalyzer } from './core/llm/error-analyzer.js';
import { grantStore } from './auth/grant-store.js';
import { auditStore, AuditAction } from './auth/audit-store.js';
import * as dotenv from 'dotenv';
dotenv.config();

async function main() {
  const backend = new PostgresBackend(process.env.DATABASE_URL!);
  await backend.connect();

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const ask = (q: string): Promise<string> =>
    new Promise(resolve => rl.question(q, resolve));

  // User authentication
  let currentUser: { id: string; username: string; role: string } | null = null;

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
      currentUser = { id: user.id, username: user.username, role: user.role };
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

  // Now create session manager with authenticated user
  const sessionManager = new SessionManager(process.env.ANTHROPIC_API_KEY!, currentUser!.id);

  // Initialize error analyzer for detailed LLM-based error analysis
  const errorAnalyzer = new ErrorAnalyzer({
    anthropicApiKey: process.env.ANTHROPIC_API_KEY!
  });

  const engine = new PipelineEngine({
    anthropicApiKey: process.env.ANTHROPIC_API_KEY!,
    schema: crmSchema,
    storageBackend: backend,
    budget: {
      maxLLMCalls: 20,
      maxIterations: 100,
      timeoutMs: 60000,
    },
  });

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
      
      for (const [tableName] of schema.tables) {
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
  console.log('\ud83d\udcca Schema:', [...crmSchema.tables.keys()].join(', '));
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
      // Check for access management commands first
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
        if (writeIncompleteError && writeIncompleteError.missingColumns) {
          console.log('\n\u26A0\ufe0f  Write completeness check:');
          console.log(writeIncompleteError.message);
          
          console.log('\nThe write operation needs a few more values:');
          
          const collectedValues: Record<string, string> = {};
          let cancelled = false;
          
          for (const col of writeIncompleteError.missingColumns) {
            if (cancelled) break;
            const label = col.nullable ? '(optional)' : '(required)';
            
            while (true) {
              const value = await ask(`  ${col.column} ${label} - ${col.description}: `);
              
              if (value.trim().toLowerCase() === 'cancel') {
                console.log('\nWrite cancelled.');
                cancelled = true;
                break;
              }
              if (!value.trim() && !col.nullable) {
                console.log('  This field is required, please enter a value.');
                continue;
              }
              if (value.trim()) collectedValues[col.column] = value.trim();
              break;
            }
          }
          
          if (cancelled) continue;
          
          // Patch the existing plan in place - no re-planning
          const enrichedPlan = engine.planWithMissingValues(
            plan,                                    // pass full plan
            writeIncompleteError.stepId!,
            collectedValues
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
      const confirm = await ask('\nExecute this plan? (y/n/refine) ');

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
      console.log('\nError:', (error as Error).message);
      
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
