import type { BuiltSchema } from '../schema/SchemaBuilder.js';
import type { WritePayload } from '../nodes/payloads.js';
import type { Pool } from 'pg';

export interface FKViolation {
  column:    string
  value:     any
  refTable:  string
  refColumn: string
  message:   string
}

export async function validateForeignKeys(
  payload:  WritePayload,
  schema:   BuiltSchema,
  pool:     Pool
): Promise<FKViolation[]> {
  
  const violations: FKViolation[] = [];
  
  // Skip FK validation for DELETE mode
  // Deleting a non-existent row is a no-op, no need to validate references
  if (payload.mode === 'delete') return violations;
  
  const tableTraits = schema.traits.get(payload.table);
  if (!tableTraits) return violations;
  
  // Collect all column values being written
  // (both staticValues and dynamic columns resolved from upstream)
  const writtenValues: Record<string, any> = {
    ...payload.staticValues
  };
  
  // Group FK columns by their referenced table for batched checks
  const byRefTable = new Map<string, Array<{col: string, val: any, refCol: string}>>();
  
  for (const [col, val] of Object.entries(writtenValues)) {
    if (val === null || val === undefined) continue;
    if (typeof val === 'string' && 
        ['NOW()', 'CURRENT_TIMESTAMP'].includes(val)) continue;
    
    // Get FK info from traits
    const colTraits = tableTraits.get(col);
    if (!colTraits?.foreignKey) continue;
    
    const { references: refTable, column: refCol } = colTraits.foreignKey;
    
    // Skip self-references
    if (refTable === payload.table) continue;
    
    // Skip session_scoped columns — they're always valid
    // (workspace_id always points to the current workspace)
    const trait = colTraits?.trait;
    if (trait === 'session_scoped' || trait === 'server_generated') {
      continue;
    }
    
    // Skip system_audited columns — session.userId is always valid
    if (trait === 'system_audited') {
      continue;
    }
    
    if (!byRefTable.has(refTable)) byRefTable.set(refTable, []);
    byRefTable.get(refTable)!.push({ col, val, refCol });
  }
  
  // One query per referenced table
  for (const [refTable, refs] of byRefTable) {
    // If all refs use the same refCol (usually 'id'), 
    // use a single IN query:
    const sameRefCol = refs.every(r => r.refCol === refs[0].refCol);
    
    if (sameRefCol && refs.length > 1) {
      const vals = refs.map(r => r.val);
      try {
        const result = await pool.query(
          `SELECT "${refs[0].refCol}" FROM "${refTable}" 
           WHERE "${refs[0].refCol}" = ANY($1)`,
          [vals]
        );
        
        const foundVals = new Set(
          result.rows.map(r => String(r[refs[0].refCol]))
        );
        
        for (const ref of refs) {
          if (!foundVals.has(String(ref.val))) {
            violations.push({
              column:    ref.col,
              value:     ref.val,
              refTable,
              refColumn: ref.refCol,
              message:   `${ref.col}: ${refTable} with ${ref.refCol} = ${ref.val} does not exist` 
            });
          }
        }
      } catch (e) {
        // Non-fatal — log and skip this FK check
        console.warn(
          `[FKValidator] Could not validate batch FK check for ${refTable}: ${e}` 
        );
      }
    } else {
      // Different refCols or single ref — check individually
      for (const ref of refs) {
        try {
          const result = await pool.query(
            `SELECT 1 FROM "${refTable}" WHERE "${ref.refCol}" = $1 LIMIT 1`,
            [ref.val]
          );
          if (result.rowCount === 0) {
            violations.push({
              column:    ref.col,
              value:     ref.val,
              refTable,
              refColumn: ref.refCol,
              message:   `${ref.col}: ${refTable} with ${ref.refCol} = ${ref.val} does not exist` 
            });
          }
        } catch (e) {
          // Non-fatal — log and skip this FK check
          console.warn(
            `[FKValidator] Could not validate ${ref.col} → ` +
            `${refTable}.${ref.refCol}: ${e}` 
          );
        }
      }
    }
  }
  
  return violations;
}
