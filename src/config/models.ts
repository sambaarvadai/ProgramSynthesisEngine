// Model configuration for LLM calls
// Can be overridden with environment variables

export const MODELS = {
  // Table pre-selection (Haiku for fast schema analysis)
  TABLE_PRE_SELECTOR:
    process.env.ANTHROPIC_TABLE_PRE_SELECTOR_MODEL ||
    'claude-haiku-4-5-20251001',

  // Query intent generation (Sonnet for complex query understanding)
  QUERY_INTENT_GENERATOR:
    process.env.ANTHROPIC_QUERY_INTENT_MODEL ||
    'claude-sonnet-4-6',

  // Pipeline intent generation (Sonnet for workflow planning)
  PIPELINE_INTENT_GENERATOR:
    process.env.ANTHROPIC_PIPELINE_INTENT_MODEL ||
    'claude-sonnet-4-6',

  // Pipeline compiler placeholder (Haiku for lightweight placeholders)
  PIPELINE_COMPILER:
    process.env.ANTHROPIC_PIPELINE_COMPILER_MODEL ||
    'claude-haiku-4-5-20251001',

  // LLM node execution and transform enrichment (Haiku for per-row processing)
  LLM_NODE:
    process.env.ANTHROPIC_LLM_NODE_MODEL ||
    'claude-haiku-4-5-20251001',
} as const;

export type ModelName = keyof typeof MODELS;
