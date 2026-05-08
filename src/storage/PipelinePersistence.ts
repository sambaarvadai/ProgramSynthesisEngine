import { getPeeStorePool } from './PeeStoreBackend.js';
import type { PlanResult, RunResult } from '../pipeline-engine.js';

export interface PersistenceContext {
  nlInput:     string      // raw NL typed by user
  sessionId:   string
  workspaceId: number
  userId:      number
  startedAt:   Date
}

export async function persistPipeline(
  plan:    PlanResult,
  result:  RunResult,
  ctx:     PersistenceContext
): Promise<string | null> {
  
  // Only persist successful executions
  if (result.execution.status !== 'success') return null;
  
  try {
    const pool = getPeeStorePool();
    const completedAt = new Date();
    
    // Debug logging to check ctx.startedAt
    console.log('[PeeStore] startedAt:', ctx.startedAt, typeof ctx.startedAt);
    console.log('[PeeStore] completedAt:', completedAt);
    
    // Defensive duration calculation
    const startTime = ctx.startedAt instanceof Date 
      ? ctx.startedAt.getTime()
      : typeof ctx.startedAt === 'number'
        ? ctx.startedAt
        : Date.now();
    
    const durationMs = Math.max(0, Math.min(
      completedAt.getTime() - startTime,
      86_400_000   // cap at 24 hours — sanity check
    ));
    
    console.debug(`[PeeStore] duration: ${durationMs}ms (started: ${startTime}, completed: ${completedAt.getTime()})`);
    
    // ── Collect node-level data ──────────────────────────────
    
    const nodeRecords: any[] = [];
    let totalRowsAffected = 0;
    const sourcesTouched = new Set<string>();
    let calciteUsed = false;
    let stepOrder = 0;
    
    for (const [nodeId, nodeState] of result.execution.nodeStates) {
      if (nodeId === '_input' || nodeId === '_output') continue;
      if (nodeState.status !== 'completed') continue;
      
      stepOrder++;
      const node = plan.graph.nodes.get(nodeId);
      const nodeDurationMs = (nodeState.completedAt ?? 0) - 
                             (nodeState.startedAt ?? 0);
      
      const record: any = {
        pipeline_id: plan.graph.id,
        node_id:     nodeId,
        node_kind:   node?.kind ?? 'unknown',
        step_order:  stepOrder,
        duration_ms: nodeDurationMs,
        status:      'completed'
      };
      
      if (node?.kind === 'query') {
        const qp = node.payload as any;
        record.table_name    = qp?.intent?.table ?? null;
        // Access tabular data via .data property
        record.rows_returned = (nodeState.output as any)?.data?.length ?? 0;
        
        // Extract SQL from node state if available (optional property)
        record.sql_executed = (nodeState as any).sqlExecuted ?? null;
        
        if (record.table_name) sourcesTouched.add(record.table_name);
      }
      
      if (node?.kind === 'write') {
        const wp = node.payload as any;
        record.table_name   = wp?.table ?? null;
        record.write_mode   = wp?.mode ?? null;
        record.static_where = wp?.staticWhere ?? null;
        record.static_values = wp?.staticValues ?? null;
        
        // rows_affected from output summary
        const outputRows = (nodeState.output as any)?.data ?? [];
        const rowsAff = outputRows[0]?.rows_affected ?? 0;
        record.rows_affected = rowsAff;
        totalRowsAffected += rowsAff;
        
        // Detect Calcite usage from SQL
        const sqlExecuted = (nodeState as any).sqlExecuted;
        if (sqlExecuted && !sqlExecuted.includes('fallback')) {
          calciteUsed = true;
        }
        
        if (record.table_name) sourcesTouched.add(record.table_name);
      }
      
      nodeRecords.push(record);
    }
    
    // ── Insert pipeline record ───────────────────────────────
    
    await pool.query(
      `INSERT INTO pee_pipelines (
        id, workspace_id, user_id, session_id,
        nl_input, description, intent_json,
        status, started_at, completed_at, duration_ms,
        total_rows_affected, node_count, sources_touched,
        calcite_used, created_at
      ) VALUES (
        $1, $2, $3, $4,
        $5, $6, $7,
        'success', $8, $9, $10,
        $11, $12, $13,
        $14, NOW()
      )`,
      [
        plan.graph.id,
        ctx.workspaceId,
        ctx.userId,
        ctx.sessionId,
        ctx.nlInput,
        plan.intent.description,
        JSON.stringify(plan.intent),
        ctx.startedAt,
        completedAt,
        durationMs,
        totalRowsAffected,
        nodeRecords.length,
        [...sourcesTouched],
        calciteUsed
      ]
    );
    
    // ── Insert node records ──────────────────────────────────
    
    if (nodeRecords.length > 0) {
      const values = nodeRecords.map((r, i) => {
        const base = i * 14;
        return `($${base+1}, $${base+2}, $${base+3}, $${base+4}, ` +
               `$${base+5}, $${base+6}, $${base+7}, $${base+8}, ` +
               `$${base+9}, $${base+10}, $${base+11}, $${base+12}, ` +
               `$${base+13}, $${base+14})`;
      }).join(', ');
      
      const params = nodeRecords.flatMap(r => [
        r.pipeline_id,
        r.node_id,
        r.node_kind,
        r.step_order,
        r.table_name    ?? null,
        r.sql_executed  ?? null,
        r.rows_returned ?? null,
        r.write_mode    ?? null,
        r.rows_affected ?? null,
        r.static_where  ? JSON.stringify(r.static_where)  : null,
        r.static_values ? JSON.stringify(r.static_values) : null,
        r.duration_ms,
        r.status,
        'NOW()'
      ]);
      
      await pool.query(
        `INSERT INTO pee_pipeline_nodes (
          pipeline_id, node_id, node_kind, step_order,
          table_name, sql_executed, rows_returned,
          write_mode, rows_affected, static_where, static_values,
          duration_ms, status, created_at
        ) VALUES ${values}`,
        params
      );
    }
    
    console.log(
      `[PeeStore] Persisted pipeline ${plan.graph.id} ` +
      `(${durationMs}ms, ${nodeRecords.length} nodes, ` +
      `${totalRowsAffected} rows affected)` 
    );
    
    return plan.graph.id;
    
  } catch (e) {
    // Non-fatal — log and continue
    console.warn('[PeeStore] Failed to persist pipeline:', e);
    return null;
  }
}
