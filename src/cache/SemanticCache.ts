import { Pool } from 'pg';
import { VoyageClient } from './VoyageClient.js';
import { serializePlan, deserializePlan } from './PlanSerializer.js';
import type { PipelineIntent } from '../compiler/pipeline/pipeline-intent.js';
import type { PlanResult } from '../pipeline-engine.js';

export interface CacheHit {
  intent: PipelineIntent;
  plan: PlanResult;
  pipelineId: string | null;
  hitCount: number;
  cachedAt: Date;
  similarity: number;
}

export interface CacheConfig {
  threshold: number;    // similarity threshold e.g. 0.92
  enabled: boolean;
  workspaceId: number;
  sourceType: string;   // e.g. 'crm', 'api', 'file'
}

export class SemanticCache {
  private voyage: VoyageClient;
  private pool: Pool;
  private config: CacheConfig;

  constructor(voyage: VoyageClient, pool: Pool, config: CacheConfig) {
    this.voyage = voyage;
    this.pool = pool;
    this.config = config;
  }

  // ── LOOKUP ─────────────────────────────────────────────────

  async lookup(nlInput: string): Promise<CacheHit | null> {
    console.debug('[SemanticCache] Lookup called for:', nlInput);
    if (!this.config.enabled) {
      console.debug('[SemanticCache] Cache disabled');
      return null;
    }

    try {
      // Embed the query
      const embedding = await this.voyage.embed(nlInput);
      const vectorStr = `[${embedding.join(',')}]`;
      console.debug('[SemanticCache] Embedding generated, searching...');

      // Cosine similarity search — pgvector operator <=>
      // Filter by source_type to prevent cross-source contamination
      const result = await this.pool.query(
        `SELECT
            id,
            intent_json,
            plan_json,
            pipeline_id,
            hit_count,
            created_at,
            1 - (nl_embedding <=> $1::vector) AS similarity
           FROM pee_semantic_cache
           WHERE is_valid = TRUE
             AND workspace_id = $2
             AND source_type = $3
           ORDER BY nl_embedding <=> $1::vector
           LIMIT 1`,
        [vectorStr, this.config.workspaceId, this.config.sourceType]
      );

      if (result.rows.length === 0) {
        console.debug('[SemanticCache] No results found');
        return null;
      }

      const row = result.rows[0];
      const similarity = parseFloat(row.similarity);
      console.debug('[SemanticCache] Found result, similarity:', similarity);

      console.debug(
        `[SemanticCache] Best match similarity: ${similarity.toFixed(4)} ` +
        `(threshold: ${this.config.threshold})`
      );

      if (similarity < this.config.threshold) return null;

      const planJson = row.plan_json;
      if (!planJson) {
        // Old cache entry without plan_json — treat as miss
        console.debug('[SemanticCache] Entry has no plan_json — treating as miss');
        return null;
      }

      const plan = deserializePlan(planJson);

      // Update hit count and last_hit_at
      await this.pool.query(
        `UPDATE pee_semantic_cache
           SET hit_count = hit_count + 1,
               last_hit_at = NOW(),
               updated_at = NOW()
           WHERE id = $1`,
        [row.id]
      );

      console.log(
        `[SemanticCache] Cache HIT — similarity: ${similarity.toFixed(4)}, ` +
        `hits: ${row.hit_count + 1}`
      );

      return {
        intent: row.intent_json as PipelineIntent,
        plan,
        pipelineId: row.pipeline_id,
        hitCount: row.hit_count + 1,
        cachedAt: new Date(row.created_at),
        similarity
      };

    } catch (e) {
      // Non-fatal — cache miss on error
      console.warn('[SemanticCache] Lookup failed:', e);
      return null;
    }
  }

  // ── STORE ──────────────────────────────────────────────────

  async store(
    nlInput: string,
    intent: PipelineIntent,
    plan: PlanResult,
    sourcesTouched: string[],
    pipelineId?: string
  ): Promise<void> {
    if (!this.config.enabled) return;

    try {
      const embedding = await this.voyage.embed(nlInput);
      const vectorStr = `[${embedding.join(',')}]`;
      const serialized = serializePlan(plan);

      console.debug('[SemanticCache] Serialized plan size:', JSON.stringify(serialized).length);
      console.debug('[SemanticCache] plan_json will be stored');

      const result = await this.pool.query(
        `INSERT INTO pee_semantic_cache (
            nl_input, nl_embedding, intent_json, plan_json,
            pipeline_id, workspace_id, sources_touched, source_type
          ) VALUES ($1, $2::vector, $3, $4, $5, $6, $7, $8)`,
        [
          nlInput,
          vectorStr,
          JSON.stringify(intent),
          JSON.stringify(serialized),
          pipelineId ?? null,
          this.config.workspaceId,
          sourcesTouched,
          this.config.sourceType
        ]
      );

      console.log(
        `[SemanticCache] Stored: "${nlInput.slice(0, 50)}" ` +
        `→ sources [${sourcesTouched.join(', ')}] (row count: ${result.rowCount})`
      );

    } catch (e) {
      console.warn('[SemanticCache] Store failed:', e);
      throw e;
    }
  }

  // ── INVALIDATION ───────────────────────────────────────────

  async invalidateBySources(
    sources: string[],
    reason: string
  ): Promise<number> {
    if (sources.length === 0) return 0;

    try {
      const result = await this.pool.query(
        `UPDATE pee_semantic_cache
           SET is_valid = FALSE,
               invalidated_at = NOW(),
               invalidated_reason = $1,
               updated_at = NOW()
           WHERE is_valid = TRUE
             AND workspace_id = $2
             AND source_type = $3
             AND sources_touched && $4::text[]
           RETURNING id`,
        [reason, this.config.workspaceId, this.config.sourceType, sources]
      );

      const count = result.rowCount ?? 0;
      if (count > 0) {
        console.log(
          `[SemanticCache] Invalidated ${count} entries ` +
          `touching sources: [${sources.join(', ')}] — ${reason}`
        );
      }
      return count;

    } catch (e) {
      console.warn('[SemanticCache] Invalidation failed:', e);
      return 0;
    }
  }

  async invalidateAll(reason: string): Promise<number> {
    try {
      const result = await this.pool.query(
        `UPDATE pee_semantic_cache
           SET is_valid = FALSE,
               invalidated_at = NOW(),
               invalidated_reason = $1,
               updated_at = NOW()
           WHERE is_valid = TRUE
             AND workspace_id = $2
             AND source_type = $3`,
        [reason, this.config.workspaceId, this.config.sourceType]
      );
      const count = result.rowCount ?? 0;
      console.log(
        `[SemanticCache] Invalidated ALL ${count} cache entries — ${reason}`
      );
      return count;
    } catch (e) {
      console.warn('[SemanticCache] Full invalidation failed:', e);
      return 0;
    }
  }

  // ── STATS ──────────────────────────────────────────────────

  async getStats(): Promise<{
    total: number;
    valid: number;
    totalHits: number;
    topEntries: any[];
  }> {
    const result = await this.pool.query(`
        SELECT
          COUNT(*)                          AS total,
          COUNT(*) FILTER (WHERE is_valid)  AS valid,
          COALESCE(SUM(hit_count), 0)       AS total_hits
        FROM pee_semantic_cache
        WHERE workspace_id = $1
          AND source_type = $2
      `, [this.config.workspaceId, this.config.sourceType]);

    const top = await this.pool.query(`
        SELECT nl_input, hit_count, created_at
        FROM pee_semantic_cache
        WHERE is_valid = TRUE
          AND workspace_id = $1
          AND source_type = $2
        ORDER BY hit_count DESC
        LIMIT 5
      `, [this.config.workspaceId, this.config.sourceType]);

    return {
      total: parseInt(result.rows[0].total),
      valid: parseInt(result.rows[0].valid),
      totalHits: parseInt(result.rows[0].total_hits),
      topEntries: top.rows
    };
  }
}
