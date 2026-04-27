import type { SchemaConfig } from '../schema/schema-config.js';
import { MODELS } from '../../config/models.js';
import { callLLM, LLMMessage } from '../../core/llm/llm-client.js';

function parseJsonResponse(raw: string): any {
  // Strip markdown fences — model sometimes wraps response despite instructions
  const clean = raw
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```\s*$/i, '')
    .trim();
  return JSON.parse(clean);
}

export interface TablePreSelectorConfig {
  anthropicApiKey: string;
  maxTables?: number; // default 5
  model?: string; // default from MODELS.TABLE_PRE_SELECTOR
}

export interface PreSelectionResult {
  selectedTables: string[];
  reasoning: string;
  reducedSchema: SchemaConfig; // SchemaConfig containing only selected tables + their directly related tables
}

export class TablePreSelector {
  constructor(private config: TablePreSelectorConfig) {
    this.config.maxTables = config.maxTables || 5;
    this.config.model = config.model || MODELS.TABLE_PRE_SELECTOR;
  }

  async select(
    naturalLanguageQuery: string,
    fullSchema: SchemaConfig
  ): Promise<PreSelectionResult> {
    // 1. Build compact schema summary
    const schemaSummary = this.buildSchemaSummary(fullSchema);

    // 2. Call Haiku with system prompt
    const systemPrompt = `You are a database schema analyzer. Given a natural language query and a database schema, identify which tables are needed to answer the query. Return ONLY a JSON object with:
{ "selectedTables": string[], "reasoning": string }
Select the minimum tables needed. Include tables required for joins. Be conservative - it's better to include an extra table than miss a required one.
Return ONLY raw JSON with no markdown formatting, no backticks, no explanation.`;

    const userPrompt = `Respond with raw JSON only. No markdown. No backticks. No explanation.\n\nQuery: ${naturalLanguageQuery}\n\nSchema:\n${schemaSummary}`;

    // 3. Call Anthropic API
    const messages: LLMMessage[] = [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userPrompt }
    ];
    const response = await callLLM('anthropic', {
      apiKey: this.config.anthropicApiKey,
      model: this.config.model,
      maxTokens: 4096
    }, messages);

    // 4. Parse response JSON
    let selectedTables: string[];
    let reasoning: string;

    try {
      const parsed = parseJsonResponse(response);
      selectedTables = parsed.selectedTables || [];
      reasoning = parsed.reasoning || 'No reasoning provided';
    } catch (error) {
      console.error('Failed to parse Haiku response:', error);
      // Fall back to all tables if parsing fails
      selectedTables = Array.from(fullSchema.tables.keys());
      reasoning = 'Failed to parse AI response, using full schema';
    }

    // 5. Validate selected tables are real table names
    selectedTables = selectedTables.filter(tableName => fullSchema.tables.has(tableName));

    if (selectedTables.length === 0) {
      // Fall back to full schema if no valid tables selected
      selectedTables = Array.from(fullSchema.tables.keys());
      reasoning = 'No valid tables selected, using full schema';
    }

    // 6. Expand selection to include directly related tables
    const expandedTables = new Set<string>(selectedTables);
    for (const table of selectedTables) {
      const related = this.getRelatedTables(fullSchema, table);
      for (const t of related) {
        expandedTables.add(t);
      }
    }

    // Deduplicate and cap at maxTables * 2
    const finalTables = Array.from(expandedTables)
      .slice(0, this.config.maxTables! * 2);

    // 7. Build reduced schema
    const reducedSchema = this.buildReducedSchema(fullSchema, finalTables);

    return {
      selectedTables: finalTables,
      reasoning,
      reducedSchema
    };
  }

  
  private buildSchemaSummary(schema: SchemaConfig): string {
    const lines: string[] = [];

    lines.push('Database Schema:');
    lines.push('');

    // Tables with their columns
    for (const [tableName, tableConfig] of schema.tables.entries()) {
      lines.push(`Table: ${tableName}`);
      if (tableConfig.description) {
        lines.push(`  Description: ${tableConfig.description}`);
      }
      if (tableConfig.alias) {
        lines.push(`  Alias: ${tableConfig.alias}`);
      }
      
      // Column names only (not types) to keep it compact
      const columnNames = tableConfig.columns.map(col => col.name).join(', ');
      lines.push(`  Columns: ${columnNames}`);
      
      if (tableConfig.primaryKey && tableConfig.primaryKey.length > 0) {
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

    const summary = lines.join('\n');

    // Ensure summary is under 2000 tokens (roughly 8000 characters)
    if (summary.length > 8000) {
      // Truncate if too long, keeping the structure
      return summary.substring(0, 8000) + '\n... (truncated)';
    }

    return summary;
  }

  private getRelatedTables(schema: SchemaConfig, tableName: string): string[] {
    const related = new Set<string>();
    
    // Find tables related through foreign keys
    for (const fk of schema.foreignKeys) {
      if (fk.fromTable === tableName) {
        related.add(fk.toTable);
      } else if (fk.toTable === tableName) {
        related.add(fk.fromTable);
      }
    }
    
    return Array.from(related);
  }

  private buildReducedSchema(fullSchema: SchemaConfig, tables: string[]): SchemaConfig {
    const reducedTables = new Map<string, any>();

    // Copy selected tables
    for (const tableName of tables) {
      const tableConfig = fullSchema.tables.get(tableName);
      if (tableConfig) {
        reducedTables.set(tableName, tableConfig);
      }
    }

    // Filter foreign keys to only those between selected tables
    const reducedForeignKeys = fullSchema.foreignKeys.filter(fk =>
      tables.includes(fk.fromTable) && tables.includes(fk.toTable)
    );

    return {
      tables: reducedTables,
      foreignKeys: reducedForeignKeys,
      version: fullSchema.version,
      description: `Reduced schema for tables: ${tables.join(', ')}`
    };
  }
}
