import type { SchemaConfig } from '../schema/schema-config.js';
import { MODELS } from '../../config/models.js';
import { callLLM, LLMMessage } from '../../core/llm/llm-client.js';
import type { MultiSourceSchema } from '../../schema/MultiSourceSchemaBuilder.js';

function parseJsonResponse(raw: string): any {
  const clean = raw
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```\s*$/i, '')
    .trim();
  return JSON.parse(clean);
}

export interface TablePreSelectorConfig {
  anthropicApiKey: string;
  maxTables?:  number;
  model?:      string;
  multiSchema?: MultiSourceSchema;
}

export interface PreSelectionResult {
  selectedTables: string[];
  reasoning:      string;
  reducedSchema:  SchemaConfig;
  tableDataSources: Record<string, string>;
}

export class TablePreSelector {
  constructor(private config: TablePreSelectorConfig) {
    this.config.maxTables = config.maxTables || 5;
    this.config.model     = config.model || MODELS.TABLE_PRE_SELECTOR;
  }

  async select(
    naturalLanguageQuery: string,
    fullSchema:           SchemaConfig
  ): Promise<PreSelectionResult> {

    // 1. Build schema summary with dynamic datasource labels
    const schemaSummary = this.buildSchemaSummary(
      fullSchema,
      this.config.multiSchema
    );

    // 2. Build datasource context block dynamically from schema annotations
    const datasourceContext = this.buildDatasourceContext(fullSchema);

    // 3. System prompt — generic, no datasource names hardcoded
    const systemPrompt = [
      'You are a database schema analyzer.',
      'Given a natural language query and a multi-system database schema,',
      'identify which tables are needed to answer the query.',
      '',
      'Return ONLY a JSON object with:',
      '{ "selectedTables": string[], "reasoning": string }',
      '',
      'Rules:',
      '- Select the minimum tables needed to answer the query',
      '- Include tables required for joins',
      '- For cross-system queries, include tables from multiple systems',
      '- Be conservative — include an extra related table rather than miss one',
      '- selectedTables must be exact table names from the schema',
      '- Return ONLY raw JSON — no markdown, no backticks, no explanation',
      '',
      datasourceContext
    ].filter(Boolean).join('\n');

    // 4. User prompt
    const userPrompt = [
      'Respond with raw JSON only. No markdown. No backticks.',
      '',
      `Query: ${naturalLanguageQuery}`,
      '',
      'Schema:',
      schemaSummary
    ].join('\n');

    // 5. Call LLM
    const messages: LLMMessage[] = [
      { role: 'system', content: systemPrompt },
      { role: 'user',   content: userPrompt }
    ];

    const response = await callLLM('anthropic', {
      apiKey:    this.config.anthropicApiKey,
      model:     this.config.model,
      maxTokens: 512
    }, messages);

    // 6. Parse response
    let selectedTables: string[];
    let reasoning: string;

    try {
      const parsed  = parseJsonResponse(response);
      selectedTables = parsed.selectedTables || [];
      reasoning      = parsed.reasoning || 'No reasoning provided';
    } catch (error) {
      console.warn('[TablePreSelector] Failed to parse response:', error);
      selectedTables = Array.from(fullSchema.tables.keys());
      reasoning      = 'Parse failed — using full schema';
    }

    // 7. Validate — only keep real table names
    selectedTables = selectedTables.filter(t => fullSchema.tables.has(t));

    if (selectedTables.length === 0) {
      selectedTables = Array.from(fullSchema.tables.keys());
      reasoning      = 'No valid tables selected — using full schema';
    }

    // 8. Expand to include directly related tables
    const expandedTables = new Set<string>(selectedTables);
    for (const table of selectedTables) {
      for (const related of this.getRelatedTables(fullSchema, table)) {
        expandedTables.add(related);
      }
    }

    const finalTables = Array.from(expandedTables)
      .slice(0, this.config.maxTables! * 2);

    // 9. Build tableDataSources map — which DB each table belongs to
    const tableDataSources: Record<string, string> = {};
    if (this.config.multiSchema) {
      for (const table of finalTables) {
        tableDataSources[table] =
          this.config.multiSchema.tableRouting.get(table) ?? 'default';
      }
    }

    // 10. Build reduced schema
    const reducedSchema = this.buildReducedSchema(fullSchema, finalTables);

    return {
      selectedTables: finalTables,
      reasoning,
      reducedSchema,
      tableDataSources
    };
  }

  // ── PRIVATE ────────────────────────────────────────────────

  private buildDatasourceContext(schema: SchemaConfig): string {
    // Build context block from schema annotations — fully dynamic
    // Works for any number of datasources without code changes
    
    const datasources = new Map<string, { displayName: string; description: string }>();
    
    // Collect unique datasources from schema annotations
    for (const [tableName, tableConfig] of schema.tables.entries()) {
      if (tableConfig._datasource && tableConfig._displayName) {
        if (!datasources.has(tableConfig._datasource)) {
          // Get description from first table of this datasource
          datasources.set(tableConfig._datasource, {
            displayName: tableConfig._displayName,
            description: tableConfig.description || ''
          });
        }
      }
    }
    
    if (datasources.size <= 1) return '';  // single DB — no context needed

    const lines: string[] = [
      'Connected systems and their data:',
    ];

    for (const [dsName, dsInfo] of datasources.entries()) {
      lines.push(`- ${dsInfo.displayName} (tables labeled "${dsInfo.displayName}"): ${dsInfo.description}`);
    }

    lines.push('');
    lines.push('Use the system labels in the schema to route your table selection.');
    lines.push('For queries spanning multiple systems, select tables from each relevant system.');

    return lines.join('\n');
  }

  private buildSchemaSummary(
    schema:      SchemaConfig,
    multiSchema?: MultiSourceSchema
  ): string {
    
    const lines: string[] = ['Database Schema:', ''];

    for (const [tableName, tableConfig] of schema.tables.entries()) {
      // Use _displayName annotation from combined schema
      let label = '';
      if (tableConfig._displayName) {
        label = ` [${tableConfig._displayName}]`;
      }

      lines.push(`Table: ${tableName}${label}`);

      if (tableConfig.description) {
        lines.push(`  Description: ${tableConfig.description}`);
      }

      const columnNames = tableConfig.columns.map(c => c.name).join(', ');
      lines.push(`  Columns: ${columnNames}`);

      if (tableConfig.primaryKey?.length) {
        lines.push(`  PK: ${tableConfig.primaryKey.join(', ')}`);
      }

      lines.push('');
    }

    if (schema.foreignKeys.length > 0) {
      lines.push('Foreign Keys:');
      for (const fk of schema.foreignKeys) {
        lines.push(`  ${fk.fromTable}.${fk.fromColumn} → ${fk.toTable}.${fk.toColumn}`);
      }
      lines.push('');
    }

    const summary = lines.join('\n');
    return summary.length > 8000
      ? summary.substring(0, 8000) + '\n... (truncated)'
      : summary;
  }

  private getRelatedTables(schema: SchemaConfig, tableName: string): string[] {
    const related = new Set<string>();
    for (const fk of schema.foreignKeys) {
      if (fk.fromTable === tableName) related.add(fk.toTable);
      if (fk.toTable   === tableName) related.add(fk.fromTable);
    }
    return [...related];
  }

  private buildReducedSchema(
    fullSchema: SchemaConfig,
    tables:     string[]
  ): SchemaConfig {
    
    const reducedTables = new Map<string, any>();
    for (const t of tables) {
      const config = fullSchema.tables.get(t);
      if (config) reducedTables.set(t, config);
    }

    return {
      tables:      reducedTables,
      foreignKeys: fullSchema.foreignKeys.filter(
        fk => tables.includes(fk.fromTable) && tables.includes(fk.toTable)
      ),
      version:     fullSchema.version,
      description: `Reduced schema for: ${tables.join(', ')}` 
    };
  }
}
