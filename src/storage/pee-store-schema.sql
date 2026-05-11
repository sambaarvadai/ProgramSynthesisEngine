-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Pipeline execution records
CREATE TABLE IF NOT EXISTS pee_pipelines (
  id               TEXT        PRIMARY KEY,   -- UUID from plan.graph.id
  workspace_id     INT         NOT NULL,
  user_id          INT         NOT NULL,
  session_id       TEXT,                      -- CLI session ID
  
  -- Input
  nl_input         TEXT        NOT NULL,      -- raw NL description typed by user
  description      TEXT        NOT NULL,      -- LLM-generated description
  intent_json      JSONB       NOT NULL,      -- full QueryIntent object
  
  -- Execution
  status           TEXT        NOT NULL       -- 'success' (only successes stored)
                     CHECK (status IN ('success')),
  started_at       TIMESTAMPTZ NOT NULL,
  completed_at     TIMESTAMPTZ NOT NULL,
  duration_ms      INT         NOT NULL,
  
  -- Output summary
  total_rows_affected  INT     NOT NULL DEFAULT 0,
  node_count           INT     NOT NULL DEFAULT 0,
  sources_touched      TEXT[], -- e.g. ['tickets', 'audit_logs']
  
  -- Metadata
  model_used       TEXT,                      -- LLM model
  calcite_used     BOOLEAN     NOT NULL DEFAULT FALSE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Individual node execution records
CREATE TABLE IF NOT EXISTS pee_pipeline_nodes (
  id               SERIAL      PRIMARY KEY,
  pipeline_id      TEXT        NOT NULL REFERENCES pee_pipelines(id),
  
  -- Node identity
  node_id          TEXT        NOT NULL,      -- e.g. 'fetch_ticket'
  node_kind        TEXT        NOT NULL,      -- 'query', 'write', 'input', 'output'
  step_order       INT         NOT NULL,      -- execution order
  
  -- For query nodes
  table_name       TEXT,                      -- primary table queried
  sql_executed     TEXT,                      -- full SQL string
  rows_returned    INT,
  
  -- For write nodes
  write_mode       TEXT,                      -- 'insert', 'update', 'delete'
  rows_affected    INT,
  static_where     JSONB,                     -- WHERE clause values
  static_values    JSONB,                     -- SET clause values
  
  -- Timing
  duration_ms      BIGINT      NOT NULL DEFAULT 0,
  status           TEXT        NOT NULL
                     CHECK (status IN ('completed', 'skipped')),
  
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_pee_pipelines_workspace 
  ON pee_pipelines(workspace_id);
CREATE INDEX IF NOT EXISTS idx_pee_pipelines_created 
  ON pee_pipelines(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pee_pipelines_sources 
  ON pee_pipelines USING GIN(sources_touched);
CREATE INDEX IF NOT EXISTS idx_pee_pipeline_nodes_pipeline
  ON pee_pipeline_nodes(pipeline_id);

-- DDL state tracking for cache invalidation
CREATE TABLE IF NOT EXISTS pee_schema_state (
  id              SERIAL PRIMARY KEY,
  datasource      TEXT        NOT NULL DEFAULT 'default',
  ddl_hash        TEXT        NOT NULL,   -- SHA256 of DDL file
  table_count     INT         NOT NULL,
  column_count    INT         NOT NULL,
  recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pee_schema_state_datasource
  ON pee_schema_state(datasource);

-- Semantic cache entries
CREATE TABLE IF NOT EXISTS pee_semantic_cache (
  id              SERIAL      PRIMARY KEY,

  -- Input
  nl_input        TEXT        NOT NULL,   -- canonical NL input
  nl_embedding    vector(1024),           -- Voyage voyage-3 dimension

  -- Cached output
  intent_json     JSONB       NOT NULL,   -- QueryIntent to replay
  plan_json       JSONB,                  -- full serialized enriched plan
  pipeline_id     TEXT,                   -- reference to pee_pipelines

  -- Cache metadata
  workspace_id    INT         NOT NULL DEFAULT 1,
  source_type     TEXT        NOT NULL DEFAULT 'crm', -- e.g. 'crm', 'api', 'file'
  sources_touched TEXT[]      NOT NULL,   -- for targeted invalidation
  hit_count       INT         NOT NULL DEFAULT 0,
  last_hit_at     TIMESTAMPTZ,

  -- Validity
  is_valid        BOOLEAN     NOT NULL DEFAULT TRUE,
  invalidated_at  TIMESTAMPTZ,
  invalidated_reason TEXT,

  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast similarity search
-- Using HNSW for approximate nearest neighbor (fast)
CREATE INDEX IF NOT EXISTS idx_pee_semantic_cache_embedding
  ON pee_semantic_cache
  USING hnsw (nl_embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);

-- Index for targeted invalidation by source
CREATE INDEX IF NOT EXISTS idx_pee_semantic_cache_source
  ON pee_semantic_cache(source_type, workspace_id);

-- Index for targeted invalidation by sources
CREATE INDEX IF NOT EXISTS idx_pee_semantic_cache_sources
  ON pee_semantic_cache USING GIN(sources_touched);

-- Index for valid entries only
CREATE INDEX IF NOT EXISTS idx_pee_semantic_cache_valid
  ON pee_semantic_cache(is_valid, workspace_id)
  WHERE is_valid = TRUE;
