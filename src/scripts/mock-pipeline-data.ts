import { getPeeStorePool } from '../storage/PeeStoreBackend.js';
import { initPeeStore } from '../storage/initPeeStore.js';
import { randomUUID } from 'crypto';
import * as dotenv from 'dotenv';
dotenv.config();

/**
 * Mock test script to populate pee_store with dummy pipeline data
 * Does not require LLM access - creates synthetic data for testing
 */

async function generateMockPipelineData(count: number = 10) {
  const pool = getPeeStorePool();
  
  // Initialize schema
  await initPeeStore();
  console.log('[Mock] Schema initialized');

  const nlInputs = [
    'show me all accounts',
    'find tickets from last week',
    'update account status to active',
    'list all users in workspace 1',
    'count tickets by priority',
    'get recent audit logs',
    'show accounts with balance > 1000',
    'delete expired sessions',
    'update ticket status to closed',
    'find high-value customers'
  ];

  const tables = ['accounts', 'tickets', 'users', 'audit_logs', 'sessions'];
  const kinds = ['query', 'write', 'input', 'output'];
  const writeModes = ['insert', 'update', 'delete'];

  for (let i = 0; i < count; i++) {
    const pipelineId = randomUUID();
    const startedAt = new Date(Date.now() - Math.random() * 7 * 24 * 60 * 60 * 1000); // Last 7 days
    const durationMs = Math.floor(Math.random() * 5000) + 10; // 10-5010ms
    const completedAt = new Date(startedAt.getTime() + durationMs);
    
    const nlInput = nlInputs[Math.floor(Math.random() * nlInputs.length)];
    const nodeCount = Math.floor(Math.random() * 5) + 1; // 1-5 nodes
    const sourcesTouched = tables.slice(0, Math.floor(Math.random() * 3) + 1);
    const totalRowsAffected = Math.floor(Math.random() * 100);
    
    // Insert pipeline record
    await pool.query(
      `INSERT INTO pee_pipelines (
        id, workspace_id, user_id, session_id,
        nl_input, description, intent_json,
        status, started_at, completed_at, duration_ms,
        total_rows_affected, node_count, sources_touched,
        calcite_used, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'success', $8, $9, $10, $11, $12, $13, $14, NOW())`,
      [
        pipelineId,
        Math.floor(Math.random() * 5) + 1, // workspace_id 1-5
        Math.floor(Math.random() * 10) + 1, // user_id 1-10
        randomUUID(),
        nlInput,
        `Mock pipeline for: ${nlInput}`,
        JSON.stringify({ description: `Mock pipeline for: ${nlInput}`, steps: [] }),
        startedAt,
        completedAt,
        durationMs,
        totalRowsAffected,
        nodeCount,
        sourcesTouched,
        Math.random() > 0.5 // random calcite usage
      ]
    );

    // Insert node records
    for (let j = 0; j < nodeCount; j++) {
      const nodeKind = kinds[Math.floor(Math.random() * kinds.length)];
      const nodeDurationMs = Math.floor(durationMs / nodeCount);
      
      const record: any = {
        pipeline_id: pipelineId,
        node_id: `node_${j}`,
        node_kind: nodeKind,
        step_order: j + 1,
        table_name: sourcesTouched[0] || null,
        sql_executed: nodeKind === 'query' ? `SELECT * FROM ${sourcesTouched[0]}` : null,
        rows_returned: nodeKind === 'query' ? Math.floor(Math.random() * 50) : null,
        write_mode: nodeKind === 'write' ? writeModes[Math.floor(Math.random() * writeModes.length)] : null,
        rows_affected: nodeKind === 'write' ? Math.floor(Math.random() * 20) : null,
        static_where: nodeKind === 'write' ? JSON.stringify({ id: Math.floor(Math.random() * 100) }) : null,
        static_values: nodeKind === 'write' ? JSON.stringify({ status: 'active' }) : null,
        duration_ms: nodeDurationMs,
        status: 'completed'
      };

      await pool.query(
        `INSERT INTO pee_pipeline_nodes (
          pipeline_id, node_id, node_kind, step_order,
          table_name, sql_executed, rows_returned,
          write_mode, rows_affected, static_where, static_values,
          duration_ms, status, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())`,
        [
          record.pipeline_id,
          record.node_id,
          record.node_kind,
          record.step_order,
          record.table_name,
          record.sql_executed,
          record.rows_returned,
          record.write_mode,
          record.rows_affected,
          record.static_where,
          record.static_values,
          record.duration_ms,
          record.status
        ]
      );
    }

    console.log(`[Mock] Created pipeline ${i + 1}/${count}: ${nlInput} (${durationMs}ms)`);
  }

  console.log(`\n[Mock] ✅ Created ${count} mock pipelines with node records`);
  
  // Verify the data
  const result = await pool.query('SELECT COUNT(*) as count FROM pee_pipelines');
  console.log(`[Mock] Total pipelines in database: ${result.rows[0].count}`);
}

// Run if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const count = parseInt(process.argv[2]) || 10;
  generateMockPipelineData(count).catch(console.error);
}

export { generateMockPipelineData };
