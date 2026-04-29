import type { WritePredicate } from '../payloads.js';

/**
 * Build a WritePredicate AST from staticWhere record
 */
export function buildWritePredicate(
  staticWhere: Record<string, any>
): WritePredicate | null {
  const entries = Object.entries(staticWhere);
  if (entries.length === 0) return null;

  const predicates: WritePredicate[] = entries.map(([col, val]) => {
    if (Array.isArray(val)) {
      if (val.length === 0) return null;
      if (val.length === 1) return { kind: 'eq', column: col, value: val[0] };
      return { kind: 'any', column: col, values: val };
    }
    if (val === null) return { kind: 'isNull', column: col };
    return { kind: 'eq', column: col, value: val };
  }).filter(Boolean) as WritePredicate[];

  if (predicates.length === 0) return null;
  if (predicates.length === 1) return predicates[0];

  // Combine multiple conditions with AND
  return predicates.reduce((acc, pred) => ({
    kind: 'and', left: acc, right: pred
  }));
}

/**
 * Convert WritePredicate AST to SQL with parameterized placeholders
 */
export function predicateToSQL(
  predicate: WritePredicate,
  startIndex: number = 1
): { sql: string; params: any[]; nextIndex: number } {

  switch (predicate.kind) {
    case 'eq':
      return {
        sql: `"${predicate.column}" = $${startIndex}`,
        params: [predicate.value],
        nextIndex: startIndex + 1
      };

    case 'any':
      return {
        sql: `"${predicate.column}" = ANY($${startIndex})`,
        params: [predicate.values],
        nextIndex: startIndex + 1
      };

    case 'in':
      const placeholders = predicate.values
        .map((_, i) => `$${startIndex + i}`)
        .join(', ');
      return {
        sql: `"${predicate.column}" IN (${placeholders})`,
        params: predicate.values,
        nextIndex: startIndex + predicate.values.length
      };

    case 'range':
      return {
        sql: `"${predicate.column}" BETWEEN $${startIndex} AND $${startIndex + 1}`,
        params: [predicate.min, predicate.max],
        nextIndex: startIndex + 2
      };

    case 'isNull':
      return {
        sql: `"${predicate.column}" IS NULL`,
        params: [],
        nextIndex: startIndex
      };

    case 'notNull':
      return {
        sql: `"${predicate.column}" IS NOT NULL`,
        params: [],
        nextIndex: startIndex
      };

    case 'and': {
      const left = predicateToSQL(predicate.left, startIndex);
      const right = predicateToSQL(predicate.right, left.nextIndex);
      return {
        sql: `(${left.sql} AND ${right.sql})`,
        params: [...left.params, ...right.params],
        nextIndex: right.nextIndex
      };
    }

    case 'or': {
      const left = predicateToSQL(predicate.left, startIndex);
      const right = predicateToSQL(predicate.right, left.nextIndex);
      return {
        sql: `(${left.sql} OR ${right.sql})`,
        params: [...left.params, ...right.params],
        nextIndex: right.nextIndex
      };
    }

    case 'raw':
      return {
        sql: predicate.sql,
        params: predicate.params,
        nextIndex: startIndex + predicate.params.length
      };
  }
}
