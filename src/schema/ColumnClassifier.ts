import type { BuiltSchema } from './SchemaBuilder.js';

export type ColumnTrait =
  | 'server_generated'
  | 'session_scoped'
  | 'system_audited'
  | 'soft_delete_marker'
  | 'immutable_after_insert'
  | 'conditional_required'
  | 'computed_from_children'
  | 'user_supplied';

export interface ColumnClassification {
  column:         string;
  trait:          ColumnTrait;
  resolvedValue?: any;
  immutable?:     boolean;
  confidence?:    'high' | 'low';
  enumValues?:    string[];
  range?:         { min: number; max: number };
  foreignKey?:    { references: string; column: string; onDelete: string };
  condition?:     { whenColumn: string; whenValue: string };
}

export interface SessionContext {
  userId:    number;
  anchorIds: Record<string, number>;  // e.g. { workspaces: 1 }
}

export interface LiveColumnSchema {
  name?: string;
  type: string;
  nullable?: boolean;
  hasDefault?: boolean;
}

export interface LiveTableSchema {
  columns: Record<string, LiveColumnSchema>;
}

export function classifyColumn(
  table:   string,
  column:  string,
  schema:  BuiltSchema,
  session: SessionContext,
  mode:    'insert' | 'update' = 'insert'
): ColumnClassification | null {
  
  const tableTraits = schema.traits.get(table);
  if (!tableTraits) return null;
  
  const raw = tableTraits.get(column);
  if (!raw) return null;

  const result: ColumnClassification = {
    column,
    trait:      raw.trait ?? 'user_supplied',
    immutable:  raw.immutable ?? false,
    confidence: raw.confidence ?? 'high',
  };

  // Copy optional metadata
  if (raw.enumValues)  result.enumValues  = raw.enumValues;
  if (raw.range)       result.range       = raw.range;
  if (raw.foreignKey)  result.foreignKey  = raw.foreignKey;
  if (raw.condition)   result.condition   = raw.condition;

  // Resolve runtime values from session
  if (raw.trait === 'session_scoped' && raw.sessionAnchorTable) {
    result.resolvedValue = session.anchorIds[raw.sessionAnchorTable];
  } else if (raw.trait === 'system_audited') {
    result.resolvedValue = session.userId;
    console.log(`[ColumnClassifier] Resolved system_audited column ${column} to userId: ${session.userId}`);
  } else if (raw.resolvedValue !== undefined) {
    result.resolvedValue = raw.resolvedValue;
  }

  // Convert resolved value to match column type from DDLParser's accurate type info
  // (always use schema.parsed.tables, not live schema which may have TEXT fallbacks)
  if (result.resolvedValue !== undefined) {
    const tableDef = schema.parsed.tables.get(table);
    if (tableDef) {
      const colDef = tableDef.columns.get(column);
      if (colDef) {
        const colType = colDef.type?.toUpperCase() || 'TEXT';
        // If column is TEXT/VARCHAR but value is number, convert to string
        if ((colType === 'TEXT' || colType === 'VARCHAR' || colType === 'CHARACTER VARYING') && 
            typeof result.resolvedValue === 'number') {
          result.resolvedValue = String(result.resolvedValue);
        }
      }
    }
  }

  // Immutable columns are blocked on UPDATE regardless
  if (mode === 'update' && result.immutable) {
    result.trait = 'immutable_after_insert';
  }

  return result;
}

export function classifyAllColumns(
  table:   string,
  schema:  BuiltSchema,
  session: SessionContext,
  mode:    'insert' | 'update' = 'insert'
): Map<string, ColumnClassification> {
  const result = new Map<string, ColumnClassification>();
  const tableTraits = schema.traits.get(table);
  if (!tableTraits) return result;
  
  for (const [column, raw] of tableTraits) {
    const classification = classifyColumn(table, column, schema, session, mode);
    if (classification) {
      result.set(column, classification);
    }
  }
  return result;
}

export function getAutoResolvedValues(
  classifications: Map<string, ColumnClassification>
): Record<string, any> {
  const result: Record<string, any> = {};
  for (const [col, c] of classifications) {
    if (c.trait === 'user_supplied') continue;
    if (c.trait === 'soft_delete_marker') continue;   // omit on INSERT/UPDATE
    if (c.trait === 'computed_from_children') continue; // no value to inject
    if (c.trait === 'server_generated' && c.resolvedValue === undefined) continue; // PK, DB handles
    if (c.resolvedValue !== undefined && 
        !(typeof c.resolvedValue === 'number' && isNaN(c.resolvedValue))) {
      result[col] = c.resolvedValue;
      // Debug: log lifecycle_stage
      if (col === 'lifecycle_stage') {
        console.log('[getAutoResolvedValues] lifecycle_stage:', c.resolvedValue, 'trait:', c.trait);
      }
    }
  }
  console.log('[getAutoResolvedValues] Final result:', result);
  return result;
}

export function getUserSuppliedRequired(
  classifications: Map<string, ColumnClassification>,
  existingValues:  Record<string, any>
): ColumnClassification[] {
  const required: ColumnClassification[] = [];
  for (const [col, c] of classifications) {
    if (c.trait !== 'user_supplied') continue;
    if (existingValues[col] !== undefined) continue;
    // Only flag if no default and not nullable
    // (caller must check schema for nullable/default — 
    //  ColumnClassifier doesn't re-read raw schema here)
    required.push(c);
  }
  return required;
}

export function getBlockedOnUpdate(
  classifications: Map<string, ColumnClassification>
): string[] {
  return [...classifications.entries()]
    .filter(([_, c]) => c.immutable === true)
    .map(([col]) => col);
}

export function buildIntentExclusionList(
  classifications: Map<string, ColumnClassification>
): string[] {
  return [...classifications.entries()]
    .filter(([_, c]) => c.trait !== 'user_supplied')
    .map(([col]) => col);
}
