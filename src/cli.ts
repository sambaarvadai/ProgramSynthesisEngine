import readline from 'node:readline';
import { PipelineEngine } from './pipeline-engine.js';
import { crmSchema } from './config/index.js';
import { PostgresBackend } from './storage/index.js';
import * as dotenv from 'dotenv';
dotenv.config();

async function main() {
  const backend = new PostgresBackend(process.env.DATABASE_URL!);
  await backend.connect();

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

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const ask = (q: string): Promise<string> =>
    new Promise(resolve => rl.question(q, resolve));

  console.log('\n🔧 ProgramExecutionEngine CLI');
  console.log('Describe a workflow and the engine will plan and execute it.');
  console.log('Type "exit" to quit.\n');

  console.log('🗄️  Connected to Postgres:', process.env.DATABASE_URL?.replace(/:\/\/.*@/, '://***@'));
  console.log('📊 Schema:', [...crmSchema.tables.keys()].join(', '));
  console.log();

  process.on('SIGINT', async () => {
    await backend.disconnect();
    rl.close();
    process.exit(0);
  });

  while (true) {
    const description = await ask('PEE> ');
    if (description.trim() === 'exit') break;
    if (!description.trim()) continue;

    try {
      // Plan phase
      console.log('\n📋 Planning...');
      const plan = await engine.plan(description);

      // Show plan
      console.log('\n' + engine.formatPlan(plan));

      if (plan.compilationErrors.length > 0) {
        console.log('\n⚠️  Compilation errors:');
        for (const err of plan.compilationErrors) {
          console.log(`  - ${err.message}`);
        }
        console.log('Please refine your description.\n');
        continue;
      }

      // Confirm
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
        for (const [key, value] of result.execution.outputs) {
          if (
            value &&
            typeof value === 'object' &&
            'rows' in value &&
            'schema' in value
          ) {
            // It's a RowSet — print as table
            const rs = value as any;
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
          } else if (Array.isArray(value)) {
            console.log(`\n  ${key} (${value.length} items):`);
            console.log(
              '  ' +
                JSON.stringify(value.slice(0, 5), null, 2).slice(0, 300),
            );
          } else {
            console.log(
              `  ${key}:`,
              JSON.stringify(value, null, 2).slice(0, 300),
            );
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

      console.log();
    } catch (error) {
      console.error('\n❌ Error:', (error as Error).message);
      if (process.env.DEBUG) console.error((error as Error).stack);
      console.log();
    }
  }

  rl.close();
  await backend.disconnect();
  console.log('Bye!');
}

main().catch(console.error);
