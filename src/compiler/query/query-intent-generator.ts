import type { SchemaConfig } from '../schema/schema-config.js';
import type { QueryIntent } from '../query-ast/query-intent.js'
import { QueryASTBuilder } from '../query-ast/query-ast-builder.js'
import type { ValidationResult } from '../query-ast/query-ast-builder.js';
import type { RowSchema } from '../../core/types/schema.js';
import { TablePreSelector, type TablePreSelectorConfig, type PreSelectionResult } from './table-pre-selector.js';
import { z } from 'zod';
import { MODELS } from '../../config/models.js';
import { callLLM, LLMMessage } from '../../core/llm/llm-client.js';
import { isTextColumn, normalizeStringParam } from '../../storage/column-type-helper.js';
import { crmSchema } from '../../schema/crm-schema.js';

function parseJsonResponse(raw: string): any {
  // Strip markdown fences — model sometimes wraps response despite instructions
  const clean = raw
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```\s*$/i, '')
    .trim();
  return JSON.parse(clean);
}

function normalizeIntentFilters(
  intent: QueryIntent,
  schema: any
): QueryIntent {
  if (!intent.filters) return intent;

  return {
    ...intent,
    filters: intent.filters.map(filter => {
      const tableName = filter.table || intent.table;
      if (!isTextColumn(tableName, filter.field)) {
        return filter;
      }

      // Normalize string values to lowercase
      // The DB comparison will also use LOWER() so this is redundant but explicit
      return {
        ...filter,
        caseInsensitive: true,
        value: filter.value !== undefined
          ? normalizeStringParam(filter.value)
          : filter.value
      };
    })
  };
}

export interface QueryIntentGeneratorConfig {
  anthropicApiKey: string;
  model?: string; // default from MODELS.QUERY_INTENT_GENERATOR
  preSelector?: TablePreSelector;
}

export class QueryIntentGenerator {
  private queryIntentZodSchema: z.ZodSchema<QueryIntent>;

  constructor(private config: QueryIntentGeneratorConfig) {
    this.config.model = config.model || MODELS.QUERY_INTENT_GENERATOR;
    this.queryIntentZodSchema = this.buildQueryIntentZodSchema();
  }

  async generate(
    naturalLanguageQuery: string,
    schema: SchemaConfig,
    sessionContext?: string
  ): Promise<{ intent: QueryIntent; validation: ValidationResult }> {
    let workingSchema = schema;

    // 1. If preSelector configured, reduce schema
    if (this.config.preSelector) {
      const preSelectionResult = await this.config.preSelector.select(naturalLanguageQuery, schema);
      workingSchema = preSelectionResult.reducedSchema;
    }

    // Create ASTBuilder with the schema for validation
    const dummyEvaluator = null as any;
    const astBuilder = new QueryASTBuilder(schema, dummyEvaluator);

    // 2. Build schema prompt section
    const schemaSection = this.buildSchemaSection(workingSchema);

    // 3. Build system prompt
    const systemPrompt = `You are a query intent analyzer. Convert natural language queries into a structured QueryIntent JSON object. Return ONLY valid JSON matching this exact schema:

{
  "table": string (primary table name),
  "columns": Array<{
    "field": string,
    "table?": string,
    "alias?": string,
    "agg?": "SUM" | "AVG" | "COUNT" | "COUNT_DISTINCT" | "MIN" | "MAX",
    "expr?": string (raw expression for computed columns)
  }>,
  "joins?": Array<{
    "table": string,
    "alias?": string,
    "kind?": "INNER" | "LEFT" | "RIGHT" | "FULL",
    "on?": { "left": string, "right": string }
  }>,
  "filters?": Array<{
    "field": string,
    "table?": string,
    "operator": "=" | "!=" | "<" | ">" | "<=" | ">=" | "LIKE" | "IN" | "NOT IN" | "BETWEEN" | "IS NULL" | "IS NOT NULL",
    "value?": (primitive value or array for IN/BETWEEN),
    "valueRef?": string (reference to pipeline variable),
    "caseInsensitive?": boolean (enable case-insensitive matching for text columns)
  }>,
  "groupBy?": string[],
  "aggregations?": Array<{
    "fn": "SUM" | "AVG" | "COUNT" | "COUNT_DISTINCT" | "MIN" | "MAX",
    "expr": string,
    "alias": string
  }>,
  "having?": Array<(same as filters)>,
  "orderBy?": Array<{
    "field": string,
    "table?": string,
    "direction?": "ASC" | "DESC",
    "nulls?": "FIRST" | "LAST"
  }>,
  "limit?": number,
  "offset?": number,
  "distinct?": boolean
}

Rules:
- Use exact table and column names from the schema
- Derive joins from foreign keys, do not invent join conditions
- Use filters array for WHERE conditions
- Use groupBy + aggregations together, never one without the other
- Never add columns not in the schema
- For IN and BETWEEN operators, use array values
- For NOT IN with subqueries, use the value field for the subquery string
- For IS NULL and IS NOT NULL, omit the value field
- For text/string column filters (city names, names, categories, statuses, emails, descriptions), always set caseInsensitive: true. Do not use LOWER() in the expr field — set caseInsensitive: true instead and let the backend handle normalization.
- ORDER BY rules:
  - Always include orderBy when the user asks for sorted or ranked results
  - Use exact column names from the schema
  - For aggregated results: use the alias defined in the columns array
  - Direction: DESC for 'top', 'highest', 'most'; ASC for 'lowest', 'oldest', 'first'
- Return ONLY the JSON object, no markdown formatting
- Return ONLY raw JSON with no markdown formatting, no backticks, no explanation.`;

    // 4. Build user prompt
    let userPrompt = 'Respond with raw JSON only. No markdown. No backticks. No explanation.\n\n';
    if (sessionContext) {
      userPrompt += `Session Context:\n${sessionContext}\n\n`;
    }
    userPrompt += `Query: ${naturalLanguageQuery}\n\nSchema:\n${schemaSection}`;

    // 5. Call Sonnet and parse JSON response
    const messages: LLMMessage[] = [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userPrompt }
    ];
    const response = await callLLM('anthropic', {
      apiKey: this.config.anthropicApiKey,
      model: this.config.model,
      maxTokens: 4096
    }, messages);
    const intent = parseJsonResponse(response);

    // 6. Validate against QueryIntent shape with Zod
    const zodResult = this.queryIntentZodSchema.safeParse(intent);
    if (!zodResult.success) {
      const errors = zodResult.error.errors.map(e => `${e.path.join('.')}: ${e.message}`).join(', ');
      return {
        intent,
        validation: { isValid: false, errors: [`Zod validation failed: ${errors}`] }
      };
    }

    const validatedIntent = zodResult.data;

    // 7. Normalize intent filters for case-insensitive matching
    const normalizedIntent = normalizeIntentFilters(validatedIntent, crmSchema);

    // 8. Run QueryASTBuilder.validateIntent for semantic validation
    // Note: We need to use the full schema for validation, not the reduced one
    const semanticValidation = astBuilder.validateIntent(normalizedIntent);

    return {
      intent: normalizedIntent,
      validation: semanticValidation
    };
  }

  
  private buildSchemaSection(schema: SchemaConfig): string {
    const lines: string[] = [];

    lines.push('Database Schema:');
    lines.push('');

    // Tables with detailed information
    for (const [tableName, tableConfig] of schema.tables.entries()) {
      lines.push(`Table: ${tableName}`);
      if (tableConfig.description) {
        lines.push(`  Description: ${tableConfig.description}`);
      }
      if (tableConfig.alias) {
        lines.push(`  Alias: ${tableConfig.alias}`);
      }
      
      // Columns with types and descriptions
      lines.push('  Columns:');
      for (const column of tableConfig.columns) {
        const colInfo = `    ${column.name}: ${column.type.kind}`;
        if (column.nullable) {
          lines.push(`${colInfo} (nullable)`);
        } else {
          lines.push(colInfo);
        }
        if (column.description) {
          lines.push(`      Description: ${column.description}`);
        }
        if (column.examples && column.examples.length > 0) {
          const examples = column.examples.slice(0, 3).map(e => JSON.stringify(e)).join(', ');
          lines.push(`      Examples: ${examples}`);
        }
      }
      
      if (tableConfig.primaryKey.length > 0) {
        lines.push(`  Primary Key: ${tableConfig.primaryKey.join(', ')}`);
      }
      lines.push('');
    }

    // Foreign key relationships
    if (schema.foreignKeys.length > 0) {
      lines.push('Foreign Keys:');
      for (const fk of schema.foreignKeys) {
        lines.push(`  ${fk.fromTable}.${fk.fromColumn} → ${fk.toTable}.${fk.toColumn}`);
        if (fk.description) {
          lines.push(`    (${fk.description})`);
        }
      }
      lines.push('');
    }

    // Schema metadata
    lines.push(`Schema Version: ${schema.version}`);
    if (schema.description) {
      lines.push(`Description: ${schema.description}`);
    }

    return lines.join('\n');
  }

  private buildQueryIntentZodSchema(): z.ZodSchema<QueryIntent> {
    const aggFnSchema = z.enum(['SUM', 'AVG', 'COUNT', 'COUNT_DISTINCT', 'MIN', 'MAX']);
    
    const queryIntentColumnSchema = z.object({
      field: z.string(),
      table: z.string().optional(),
      alias: z.string().optional(),
      agg: aggFnSchema.optional(),
      expr: z.string().optional()
    });

    const queryIntentFilterSchema = z.object({
      field: z.string(),
      table: z.string().optional(),
      operator: z.enum(['=', '!=', '<', '>', '<=', '>=', 'LIKE', 'IN', 'NOT IN', 'BETWEEN', 'IS NULL', 'IS NOT NULL']),
      value: z.union([z.string(), z.number(), z.boolean(), z.array(z.any())]).optional(),
      valueRef: z.string().optional(),
      expr: z.string().optional(),
      caseInsensitive: z.boolean().optional()
    });

    const queryIntentJoinSchema = z.object({
      table: z.string(),
      alias: z.string().optional(),
      kind: z.enum(['INNER', 'LEFT', 'RIGHT', 'FULL']).optional(),
      on: z.object({
        left: z.string(),
        right: z.string()
      }).optional()
    });

    const queryIntentOrderBySchema = z.object({
      field: z.string(),
      table: z.string().optional(),
      direction: z.enum(['ASC', 'DESC']).optional(),
      nulls: z.enum(['FIRST', 'LAST']).optional()
    });

    const aggregationSchema = z.object({
      fn: aggFnSchema,
      expr: z.string(),
      alias: z.string()
    });

    return z.object({
      table: z.string(),
      columns: z.array(queryIntentColumnSchema),
      joins: z.array(queryIntentJoinSchema).optional(),
      filters: z.array(queryIntentFilterSchema).optional(),
      groupBy: z.array(z.string()).optional(),
      aggregations: z.array(aggregationSchema).optional(),
      having: z.array(queryIntentFilterSchema).optional(),
      orderBy: z.array(queryIntentOrderBySchema).optional(),
      limit: z.number().optional(),
      offset: z.number().optional(),
      distinct: z.boolean().optional()
    });
  }
}
