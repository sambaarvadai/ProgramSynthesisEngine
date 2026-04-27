import type { ParsedSchema, RawTableMap } from './DDLParser.js';
import { isChildAggregateColumn, getConditionalDependency } from './DDLParser.js';

export interface TraitInferencerConfig {
  sessionAnchorTables: string[];    // e.g. ['workspaces']
}

export function inferTraits(
  parsed: ParsedSchema,
  config: TraitInferencerConfig = { sessionAnchorTables: [] }
): Map<string, Map<string, any>> {

  const traits = new Map<string, Map<string, any>>();

  for (const [tableName, tableDef] of parsed.tables) {
    const tableTraits = new Map<string, any>();

    for (const [columnName, colDef] of tableDef.columns) {
      const colTraits: any = {};

      // --- server_generated ---
      // SERIAL / primary key → DB owns entirely
      if (colDef.primaryKey || colDef.type === 'SERIAL') {
        colTraits.trait = 'server_generated';
        tableTraits.set(columnName, colTraits);
        continue;  // skip remaining rules
      }
      // SQL expression default (NOW(), etc.)
      if (colDef.defaultRaw && isSqlExpression(colDef.defaultRaw)) {
        colTraits.trait = 'server_generated';
        colTraits.resolvedValue = colDef.defaultRaw;
        // created_at and updated_at are also immutable
        if (/^(created_at|updated_at)$/.test(columnName)) {
          colTraits.immutable = true;
        }
        tableTraits.set(columnName, colTraits);
        continue;
      }

      // --- immutable_after_insert (standalone) ---
      if (/^created_/.test(columnName)) {
        colTraits.trait = 'immutable_after_insert';
        colTraits.immutable = true;
      }

      // --- session_scoped ---
      const fk = tableDef.foreignKeys.find(f => f.column === columnName);
      if (fk && config.sessionAnchorTables.includes(fk.refTable.toLowerCase())) {
        colTraits.trait = 'session_scoped';
        colTraits.sessionAnchorTable = fk.refTable.toLowerCase();
        // resolvedValue is set at runtime from session.anchorIds
      }

      // --- system_audited ---
      else if (
        fk?.refTable.toLowerCase() === 'users' &&
        /^(created_by|updated_by)_user_id$/.test(columnName)
      ) {
        colTraits.trait = 'system_audited';
        // resolvedValue = session.userId at runtime
      }

      // --- soft_delete_marker ---
      else if (
        /^(deleted_at|archived_at|deactivated_at|removed_at)$/.test(columnName) &&
        colDef.nullable &&
        colDef.type === 'TIMESTAMPTZ'
      ) {
        colTraits.trait = 'soft_delete_marker';
        colTraits.resolvedValue = null;
      }

      // --- computed_from_children ---
      else if (isChildAggregateColumn(tableName, columnName, parsed.fkGraph)) {
        colTraits.trait = 'computed_from_children';
      }

      // --- conditional_required ---
      else {
        const conditional = getConditionalDependency(
          tableName, columnName, parsed.tables, parsed.constraints
        );
        if (conditional) {
          colTraits.trait = 'conditional_required';
          colTraits.condition = {
            whenColumn: conditional.whenColumn,
            whenValue: conditional.whenValue
          };
        }
      }

      // --- additional metadata (non-exclusive) ---
      if (!colTraits.trait) {
        colTraits.trait = 'user_supplied';
        // flag non-nullable, no-default, no-FK columns for UI review
        if (!colDef.nullable && colDef.defaultRaw === null && !fk) {
          colTraits.confidence = 'low';
        } else {
          colTraits.confidence = 'high';
        }
      } else {
        colTraits.confidence = 'high';
      }

      // enum/range from constraints (for all traits)
      const constraintKey = `${tableName}.${columnName}`;
      const constraint = parsed.constraints.get(constraintKey);
      if (constraint?.typed.kind === 'enum') {
        colTraits.enumValues = constraint.typed.values;
      }
      if (constraint?.typed.kind === 'range') {
        colTraits.range = { min: constraint.typed.min, max: constraint.typed.max };
      }

      // foreign key metadata
      if (fk) {
        colTraits.foreignKey = {
          references: fk.refTable,
          column: fk.refColumn,
          onDelete: fk.onDelete
        };
      }

      if (colDef.unique) colTraits.unique = true;
      if (colDef.defaultRaw !== null) colTraits.default = colDef.defaultRaw;

      tableTraits.set(columnName, colTraits);
    }

    traits.set(tableName, tableTraits);
  }

  return traits;
}

// Helper — is this default a SQL expression (not a literal)?
function isSqlExpression(raw: string): boolean {
  return /^(NOW\(\)|CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_USER|gen_random_uuid\(\))/i.test(raw.trim());
}
