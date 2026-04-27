/**
 * Session Cursor for lightweight result pagination in PEE
 * Stores either explicit IDs (for small result sets) or filter AST (for large sets)
 */

export interface SessionCursor {
  table: string;
  primaryKeys: string[];
  ids: (string | number)[] | null;
  sourceFilter: object | null;
  rowCount: number;
  pipelineId: string;
  description: string;
  expiresAt: Date;
}

export class SessionCursorStore {
  private cursor: SessionCursor | null = null;

  set(cursor: SessionCursor): void {
    this.cursor = cursor;
  }

  get(): SessionCursor | null {
    if (this.cursor && this.isExpired()) {
      this.cursor = null;
      return null;
    }
    return this.cursor;
  }

  clear(): void {
    this.cursor = null;
  }

  isExpired(): boolean {
    if (!this.cursor) {
      return true;
    }
    return new Date() > this.cursor.expiresAt;
  }
}

export function extractCursor(
  rows: any[],
  primaryKey: string,
  filter: object | null,
  pipelineId: string,
  description: string
): SessionCursor {
  const rowCount = rows.length;
  const expiresAt = new Date(Date.now() + 300 * 1000); // now + 300 seconds

  let ids: (string | number)[] | null;
  let sourceFilter: object | null;

  if (rowCount <= 50) {
    ids = rows.map(row => row[primaryKey]);
    sourceFilter = null;
  } else {
    ids = null;
    sourceFilter = filter;
  }

  // Infer table name from filter or use a default
  const table = (filter as any)?.table || 'unknown';

  return {
    table,
    primaryKeys: [primaryKey],
    ids,
    sourceFilter,
    rowCount,
    pipelineId,
    description,
    expiresAt,
  };
}

export function buildWhereFromCursor(cursor: SessionCursor): { clause: string; params: any[] } {
  if (cursor.ids && cursor.ids.length > 0) {
    const primaryKey = cursor.primaryKeys[0] || 'id';
    return {
      clause: `"${primaryKey}" = ANY($1)`,
      params: [cursor.ids],
    };
  }

  if (cursor.sourceFilter) {
    return serializeFilterToWhere(cursor.sourceFilter);
  }

  throw new Error('Cursor has no resolvable WHERE target');
}

function serializeFilterToWhere(filter: object): { clause: string; params: any[] } {
  const ast = filter as any;
  const params: any[] = [];

  if (!ast || typeof ast !== 'object') {
    throw new Error('Invalid filter AST');
  }

  const clause = serializeNode(ast, params);
  return { clause, params };
}

function serializeNode(node: any, params: any[]): string {
  if (!node || typeof node !== 'object') {
    throw new Error('Invalid AST node');
  }

  // Handle comparison operators
  if (node.op) {
    return serializeComparison(node, params);
  }

  // Handle logical operators (AND, OR, NOT)
  if (node.type === 'and' || node.type === 'or') {
    return serializeLogical(node, params);
  }

  if (node.type === 'not') {
    return serializeNot(node, params);
  }

  // Handle simple field-value pairs (implicit equality)
  if (node.field !== undefined && node.value !== undefined) {
    params.push(node.value);
    return `"${node.field}" = $${params.length}`;
  }

  throw new Error(`Unsupported AST node structure: ${JSON.stringify(node)}`);
}

function serializeComparison(node: any, params: any[]): string {
  const { op, field, value } = node;
  const paramIndex = params.length + 1;

  switch (op) {
    case '=':
    case '==':
      params.push(value);
      return `"${field}" = $${paramIndex}`;
    case '!=':
    case '<>':
      params.push(value);
      return `"${field}" != $${paramIndex}`;
    case '>':
      params.push(value);
      return `"${field}" > $${paramIndex}`;
    case '>=':
      params.push(value);
      return `"${field}" >= $${paramIndex}`;
    case '<':
      params.push(value);
      return `"${field}" < $${paramIndex}`;
    case '<=':
      params.push(value);
      return `"${field}" <= $${paramIndex}`;
    case 'like':
    case 'ILIKE':
      params.push(value);
      return `"${field}" ${op.toUpperCase()} $${paramIndex}`;
    case 'in':
      if (!Array.isArray(value) || value.length === 0) {
        throw new Error('IN operator requires a non-empty array');
      }
      params.push(value);
      return `"${field}" = ANY($${paramIndex})`;
    case 'not in':
      if (!Array.isArray(value) || value.length === 0) {
        throw new Error('NOT IN operator requires a non-empty array');
      }
      params.push(value);
      return `"${field}" != ALL($${paramIndex})`;
    case 'is null':
      return `"${field}" IS NULL`;
    case 'is not null':
      return `"${field}" IS NOT NULL`;
    default:
      throw new Error(`Unsupported comparison operator: ${op}`);
  }
}

function serializeLogical(node: any, params: any[]): string {
  const { type, conditions } = node;

  if (!Array.isArray(conditions) || conditions.length === 0) {
    throw new Error(`${type.toUpperCase()} operator requires an array of conditions`);
  }

  const serializedConditions = conditions.map((cond: any) => serializeNode(cond, params));
  const operator = type === 'and' ? 'AND' : 'OR';

  return `(${serializedConditions.join(` ${operator} `)})`;
}

function serializeNot(node: any, params: any[]): string {
  const { condition } = node;

  if (!condition) {
    throw new Error('NOT operator requires a condition');
  }

  const serialized = serializeNode(condition, params);
  return `NOT (${serialized})`;
}
