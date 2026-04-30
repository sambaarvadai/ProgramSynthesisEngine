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
  description: string,
  table: string = 'unknown'
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

export interface CursorWhereResult {
  clause: string;    // SQL WHERE clause fragment
  params: any[];     // parameterized values
  isBulk: boolean;   // true if affects many rows — trigger confirm
}

export function buildWhereFromCursor(
  cursor: SessionCursor,
  startIndex: number = 1
): CursorWhereResult {
  if (cursor.ids && cursor.ids.length > 0) {
    // Small result path — use IN list
    if (cursor.ids.length === 1) {
      return {
        clause: `"${cursor.primaryKeys[0]}" = $${startIndex}`,
        params: [cursor.ids[0]],
        isBulk: false
      };
    }
    // Multiple IDs — use ANY()
    return {
      clause: `"${cursor.primaryKeys[0]}" = ANY($${startIndex})`,
      params: [cursor.ids],
      isBulk: cursor.ids.length > 1
    };
  }

  if (cursor.sourceFilter) {
    // Large result path — reconstruct WHERE from stored filter
    // sourceFilter is the QueryIntent filters array:
    // [{ field, operator, value, table?, caseInsensitive? }, ...]

    const filters = Array.isArray(cursor.sourceFilter)
      ? cursor.sourceFilter
      : [cursor.sourceFilter];

    const clauses: string[] = [];
    const params: any[] = [];
    let paramIndex = startIndex;

    for (const f of filters as any[]) {
      const col = `"${f.field}"`;

      switch (f.operator) {
        case '=':
          if (f.caseInsensitive && typeof f.value === 'string') {
            clauses.push(`LOWER(${col}) = LOWER($${paramIndex})`);
          } else {
            clauses.push(`${col} = $${paramIndex}`);
          }
          params.push(f.value);
          paramIndex++;
          break;

        case 'IN':
          const placeholders = (f.value as any[])
            .map((_, i) => `$${paramIndex + i}`)
            .join(', ');
          clauses.push(`${col} IN (${placeholders})`);
          params.push(...f.value);
          paramIndex += f.value.length;
          break;

        case 'NOT IN':
          const notInPlaceholders = (f.value as any[])
            .map((_, i) => `$${paramIndex + i}`)
            .join(', ');
          clauses.push(`${col} NOT IN (${notInPlaceholders})`);
          params.push(...f.value);
          paramIndex += f.value.length;
          break;

        case '>':
        case '<':
        case '>=':
        case '<=':
        case '!=':
          clauses.push(`${col} ${f.operator} $${paramIndex}`);
          params.push(f.value);
          paramIndex++;
          break;

        case 'IS NULL':
          clauses.push(`${col} IS NULL`);
          break;

        case 'IS NOT NULL':
          clauses.push(`${col} IS NOT NULL`);
          break;

        case 'BETWEEN':
          clauses.push(
            `${col} BETWEEN $${paramIndex} AND $${paramIndex + 1}`
          );
          params.push(f.value[0], f.value[1]);
          paramIndex += 2;
          break;

        case 'LIKE':
          clauses.push(`${col} ILIKE $${paramIndex}`);
          params.push(f.value);
          paramIndex++;
          break;

        default:
          // Skip unknown operators — don't crash
          console.warn(
            `[SessionCursor] buildWhereFromCursor: ` +
            `unknown operator "${f.operator}" for field "${f.field}" — skipped`
          );
      }
    }

    if (clauses.length === 0) {
      throw new Error(
        '[SessionCursor] buildWhereFromCursor: ' +
        'sourceFilter produced no WHERE clauses'
      );
    }

    return {
      clause: clauses.join(' AND '),
      params,
      isBulk: true   // large result → always bulk
    };
  }

  throw new Error(
    '[SessionCursor] buildWhereFromCursor: ' +
    'cursor has neither ids nor sourceFilter'
  );
}
