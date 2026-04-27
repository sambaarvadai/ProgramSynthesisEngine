import pkg from 'node-sql-parser';
const { Parser } = pkg;

// ============================================================
// INTERNAL TYPES
// ============================================================

interface RawColumnDef {
  name: string;
  type: string;
  nullable: boolean;
  primaryKey: boolean;
  unique: boolean;
  defaultRaw: string | null;
  checkRaw: string | null;
}

interface RawFKDef {
  column: string;
  refTable: string;
  refColumn: string;
  onDelete: string;
}

interface RawTableDef {
  columns: Map<string, RawColumnDef>;
  foreignKeys: RawFKDef[];
  uniqueConstraints: string[][];
}

export type RawTableMap = Map<string, RawTableDef>;

// ============================================================
// EXPORTED TYPES
// ============================================================

export type CheckConstraintKind =
  | { kind: 'enum'; values: string[] }
  | { kind: 'range'; min: number; max: number }
  | { kind: 'boolean'; values: [0, 1] }
  | { kind: 'raw'; expr: string };

export interface TypedConstraint {
  table: string;
  column: string;
  raw: string;
  typed: CheckConstraintKind;
}

export interface ParsedIndex {
  name: string;
  table: string;
  columns: string[];
  unique: boolean;
}

export interface FKEdge {
  fromTable: string;
  fromColumn: string;
  toTable: string;
  toColumn: string;
  onDelete: string;
}

export interface FKGraph {
  outbound: Map<string, FKEdge[]>;
  inbound: Map<string, FKEdge[]>;
  getOutbound: (table: string) => FKEdge[];
  getInbound: (table: string) => FKEdge[];
  getReferencedBy: (table: string) => string[];
  getReferences: (table: string) => string[];
  isReachable: (from: string, to: string, maxHops?: number) => boolean;
}

export interface ParsedSchema {
  tables: RawTableMap;
  indexes: Map<string, ParsedIndex>;
  constraints: Map<string, TypedConstraint>;
  fkGraph: FKGraph;
}

// ============================================================
// MAIN FUNCTION
// ============================================================

const parser = new Parser();
const pgOpt = { database: 'Postgresql' };

export function parseSchema(ddl: string): ParsedSchema {
  const tables: RawTableMap = new Map();
  const indexes = new Map<string, ParsedIndex>();
  const constraints = new Map<string, TypedConstraint>();
  const outbound = new Map<string, FKEdge[]>();
  const inbound = new Map<string, FKEdge[]>();

  // Step 1: Split and parse DDL
  const statements = ddl
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0);

  const tableNodes: any[] = [];
  const indexNodes: any[] = [];

  for (const statement of statements) {
    try {
      const ast = parser.astify(statement, pgOpt);
      if (Array.isArray(ast)) {
        for (const node of ast) {
          collectNode(node, tableNodes, indexNodes);
        }
      } else {
        collectNode(ast, tableNodes, indexNodes);
      }
    } catch (e) {
      const preview = statement.substring(0, 60);
      console.log(`[DDLParser] Skipped unparseable statement: ${preview}...`);
    }
  }

  // Step 2: Extract tables into RawTableMap
  for (const node of tableNodes) {
    const tableName = node.table[0].table.toLowerCase();
    const columns = new Map<string, RawColumnDef>();
    const foreignKeys: RawFKDef[] = [];
    const uniqueConstraints: string[][] = [];

    for (const item of node.create_definitions || []) {
      if (item.resource === 'column') {
        const col = extractColumn(item);
        columns.set(col.name, col);
      } else if (item.resource === 'constraint') {
        if (item.constraint_type === 'FOREIGN KEY') {
          const fk = extractForeignKey(item);
          foreignKeys.push(fk);
        } else if (item.constraint_type === 'UNIQUE') {
          const cols = item.index_columns.map((c: any) => c.column);
          uniqueConstraints.push(cols);
        } else if (item.constraint_type === 'PRIMARY KEY') {
          const pkCol = item.index_columns[0].column;
          if (columns.has(pkCol)) {
            columns.get(pkCol)!.primaryKey = true;
          }
        }
      }
    }

    tables.set(tableName, { columns, foreignKeys, uniqueConstraints });
  }

  // Step 3: Extract indexes
  for (const node of indexNodes) {
    try {
      // node-sql-parser CREATE INDEX shape:
      // node.index = index name string
      // node.table = array [{ table: 'tablename' }]  (same as CREATE TABLE)
      // node.index_columns = [{ column: 'col', order: 'ASC' }]
      // node.index_type may be 'UNIQUE' or node.index_option may signal unique
      
      const name = node.index ?? node.index_name ?? '';
      if (!name) continue;
      
      const tableRef = Array.isArray(node.table)
        ? node.table[0]?.table
        : node.table?.table;
      if (!tableRef) continue;
      
      const table = tableRef.toLowerCase();
      
      const columns = (node.index_columns ?? []).map((c: any) => {
        if (typeof c.column === 'string') return c.column;
        if (c.column?.expr?.value) return c.column.expr.value;
        if (c.column?.column) return c.column.column;
        return String(c.column ?? '');
      });
      
      const unique = 
        node.index_type?.toUpperCase() === 'UNIQUE' ||
        node.keyword?.toLowerCase() === 'unique' ||
        node.index_option?.some((o: any) => 
          o?.type?.toUpperCase() === 'UNIQUE'
        ) === true;
      
      indexes.set(name, { name, table, columns, unique });
    } catch (e) {
      console.log(`[DDLParser] Skipped malformed index node`);
    }
  }

  // Step 4: Build TypedConstraintMap
  for (const [tableName, tableDef] of tables) {
    for (const [columnName, colDef] of tableDef.columns) {
      if (colDef.checkRaw !== null) {
        const typed = classifyCheckConstraint(colDef.checkRaw);
        const key = `${tableName}.${columnName}`;
        constraints.set(key, {
          table: tableName,
          column: columnName,
          raw: colDef.checkRaw,
          typed
        });
      }
    }
  }

  // Step 5: Build FKGraph
  let totalFKs = 0;
  for (const [tableName, tableDef] of tables) {
    for (const fk of tableDef.foreignKeys) {
      totalFKs++;
      const edge: FKEdge = {
        fromTable: tableName,
        fromColumn: fk.column,
        toTable: fk.refTable.toLowerCase(),
        toColumn: fk.refColumn,
        onDelete: fk.onDelete
      };

      if (!outbound.has(tableName)) {
        outbound.set(tableName, []);
      }
      outbound.get(tableName)!.push(edge);

      if (!inbound.has(fk.refTable.toLowerCase())) {
        inbound.set(fk.refTable.toLowerCase(), []);
      }
      inbound.get(fk.refTable.toLowerCase())!.push(edge);
    }
  }

  const fkGraph: FKGraph = {
    outbound,
    inbound,
    getOutbound: (table: string) => outbound.get(table) ?? [],
    getInbound: (table: string) => inbound.get(table) ?? [],
    getReferencedBy: (table: string) => {
      return (inbound.get(table) ?? []).map(e => e.fromTable);
    },
    getReferences: (table: string) => {
      return (outbound.get(table) ?? []).map(e => e.toTable);
    },
    isReachable: (from: string, to: string, maxHops = 3) => {
      if (from === to) return true;
      const queue: { table: string; hops: number }[] = [{ table: from, hops: 0 }];
      const visited = new Set<string>();

      while (queue.length > 0) {
        const { table: current, hops } = queue.shift()!;
        if (current === to) return true;
        if (hops >= maxHops) continue;
        if (visited.has(current)) continue;
        visited.add(current);

        for (const edge of outbound.get(current) ?? []) {
          if (!visited.has(edge.toTable)) {
            queue.push({ table: edge.toTable, hops: hops + 1 });
          }
        }
      }
      return false;
    }
  };

  // Step 6: Log summary
  console.log(
    `[DDLParser] Parsed: ${tables.size} tables, ${indexes.size} indexes, ${constraints.size} check constraints, ${totalFKs} foreign keys`
  );

  return { tables, indexes, constraints, fkGraph };
}

// ============================================================
// HELPER FUNCTIONS
// ============================================================

function collectNode(node: any, tableNodes: any[], indexNodes: any[]): void {
  if (node.type === 'create') {
    if (node.keyword === 'table') {
      tableNodes.push(node);
    } else if (node.keyword === 'index') {
      indexNodes.push(node);
    }
  }
}

function extractColumn(item: any): RawColumnDef {
  // Handle different AST structures for column names
  let name = '';
  if (item.column && item.column.column) {
    const col = item.column.column;
    if (col.expr && col.expr.value) {
      name = col.expr.value;
    } else if (typeof col === 'string') {
      name = col;
    } else {
      name = col;
    }
  } else if (item.column) {
    name = item.column;
  }
  
  const def = item.definition;

  // Normalize type
  let type = (def?.dataType ?? 'TEXT').toUpperCase();
  
  if (def.length) {
    if (Array.isArray(def.length)) {
      type += `(${def.length.join(',')})`;
    } else {
      type += `(${def.length})`;
    }
  }

  // Map common aliases
  const typeMap: Record<string, string> = {
    'CHARACTER VARYING': 'TEXT',
    'INT4':     'INT',
    'INTEGER':  'INT',
    'INT2':     'INT',
    'INT8':     'BIGINT',
    'INT':      'INT',
    'BOOL':     'BOOLEAN',
    'BIGSERIAL':'SERIAL'
  };
  if (typeMap[type]) {
    type = typeMap[type];
  }

  // Nullable
  const nullable = item.nullable?.value !== 'not null';

  // Primary key
  let primaryKey = item.primary_key === true;
  if (type === 'SERIAL' || def.dataType?.toUpperCase() === 'BIGSERIAL') {
    primaryKey = true;
  }

  // Unique
  const unique = item.unique === true || item.unique_or_primary === 'unique';

  // Default value
  let defaultRaw: string | null = null;
  if (item.default_val) {
    defaultRaw = stringifyDefault(item.default_val);
  }

  // Check constraint
  let checkRaw: string | null = null;
  if (item.check) {
    checkRaw = stringifyCheck(item.check);
  }

  return { name, type, nullable, primaryKey, unique, defaultRaw, checkRaw };
}

function extractForeignKey(item: any): RawFKDef {
  // Handle different AST structures from node-sql-parser
  let column = '';
  
  // Try index_columns first (older format)
  if (item.index_columns && item.index_columns.length > 0) {
    column = item.index_columns[0].column;
  }
  // Try definition array (newer format)
  else if (item.definition && item.definition.length > 0) {
    const def = item.definition[0];
    if (def.column && def.column.expr && def.column.expr.value) {
      column = def.column.expr.value;
    } else if (def.column) {
      column = def.column;
    }
  }
  
  if (!column) {
    console.log('[DDLParser] Warning: FK constraint missing column', JSON.stringify(item));
    return { column: '', refTable: '', refColumn: '', onDelete: 'NO ACTION' };
  }
  
  if (!item.reference_definition) {
    console.log('[DDLParser] Warning: FK constraint missing reference_definition', JSON.stringify(item));
    return { column, refTable: '', refColumn: '', onDelete: 'NO ACTION' };
  }
  
  const refTable = item.reference_definition.table?.[0]?.table || '';
  
  let refColumn = '';
  if (item.reference_definition.definition && item.reference_definition.definition.length > 0) {
    const def = item.reference_definition.definition[0];
    if (def.column && def.column.expr && def.column.expr.value) {
      refColumn = def.column.expr.value;
    } else if (def.column) {
      refColumn = def.column;
    }
  }
  
  let onDelete = 'NO ACTION';
  if (item.reference_definition?.on_action) {
    const deleteAction = item.reference_definition.on_action.find(
      (a: any) => a.type === 'on delete'
    );
    if (deleteAction) {
      const v = deleteAction.value;
      if (Array.isArray(v)) {
        // e.g. [{ type:'origin', value:'SET' }, { type:'origin', value:'NULL' }]
        onDelete = v.map((x: any) => x.value ?? x).join(' ').toUpperCase();
      } else if (v?.type === 'origin') {
        onDelete = String(v.value ?? '').toUpperCase();
      } else if (typeof v === 'string') {
        onDelete = v.toUpperCase();
      } else if (v?.value) {
        onDelete = String(v.value).toUpperCase();
      }
      // Normalize common variants
      onDelete = onDelete
        .replace(/\s+/g, ' ')
        .replace('SET NULL', 'SET NULL')
        .trim();
    }
  }

  return { column, refTable, refColumn, onDelete };
}

function stringifyDefault(node: any): string {
  if (!node) return '';
  
  // node-sql-parser wraps defaults in a value object
  const inner = node.value ?? node;
  
  if (inner.type === 'function') {
    // function name is nested: { name: [{ value: 'NOW' }] }
    const nameParts = Array.isArray(inner.name)
      ? inner.name.map((n: any) => n.value ?? n).join('')
      : (inner.name ?? '');
    const args = inner.args?.value
      ? inner.args.value.map((a: any) => stringifyDefault(a)).join(', ')
      : '';
    return `${nameParts}(${args})`;
  }
  if (inner.type === 'single_quote_string' || inner.type === 'string') {
    return `'${inner.value}'`;
  }
  if (inner.type === 'number' || inner.type === 'integer') {
    return String(inner.value);
  }
  if (inner.type === 'boolean') {
    return String(inner.value);
  }
  if (inner.type === 'null') return 'NULL';
  if (inner.type === 'origin') return String(inner.value ?? '');
  
  // Fallback
  return String(inner.value ?? inner ?? '');
}

function stringifyCheck(node: any): string {
  // Convert check constraint AST to SQL string
  if (!node) return '';
  
  // Handle different AST structures
  if (node.definition && node.definition.length > 0) {
    return stringifyCheckExpr(node.definition[0]);
  }
  
  if (node.expr) {
    return stringifyCheckExpr(node.expr);
  }
  
  return JSON.stringify(node);
}

function stringifyCheckExpr(expr: any): string {
  if (!expr) return '';
  
  // BETWEEN expression
  if (expr.type === 'binary_expr' && expr.operator === 'BETWEEN') {
    const col   = stringifyCheckExpr(expr.left);
    let lower = stringifyCheckExpr(expr.right?.left ?? expr.right);
    let upper = stringifyCheckExpr(expr.right?.right ?? expr.right);
    
    // Handle wrapped values like (0, 100) - extract first element
    if (lower.startsWith('(') && lower.includes(',')) {
      lower = lower.match(/\(([^,]+)/)?.[1] || lower;
    }
    if (upper.startsWith('(') && upper.includes(',')) {
      upper = upper.match(/,\s*([^)]+)\)/)?.[1] || upper;
    }
    
    return `${col} BETWEEN ${lower} AND ${upper}`;
  }
  
  // Binary expression (e.g., column IN (values))
  if (expr.type === 'binary_expr' && expr.operator) {
    const left = stringifyCheckExpr(expr.left);
    const right = stringifyCheckExpr(expr.right);
    return `${left} ${expr.operator} ${right}`;
  }
  
  // Column reference
  if (expr.type === 'column_ref') {
    if (expr.column && expr.column.expr && expr.column.expr.value) {
      return expr.column.expr.value;
    }
    return expr.column || '';
  }
  
  // Expression list (e.g., (value1, value2))
  if (expr.type === 'expr_list' && expr.value) {
    const values = expr.value.map((v: any) => stringifyCheckExpr(v)).join(', ');
    return `(${values})`;
  }
  
  // String literal
  if (expr.type === 'single_quote_string') {
    return `'${expr.value}'`;
  }
  
  // Default value wrapper
  if (expr.type === 'default' && expr.value) {
    return expr.value;
  }
  
  // Fallback for other types
  if (expr.value !== undefined) {
    return String(expr.value);
  }
  
  return '';
}

function classifyCheckConstraint(checkRaw: string): CheckConstraintKind {
  // Enum: IN (value1, value2, ...)
  const enumMatch = checkRaw.match(/IN\s*\(([^)]+)\)/i);
  if (enumMatch) {
    const values = enumMatch[1]
      .split(',')
      .map(v => v.trim().replace(/^['"]|['"]$/g, ''));
    if (values.length === 2 && (values.includes('0') || values.includes('1'))) {
      // Check for boolean pattern
      if ((values[0] === '0' && values[1] === '1') || (values[0] === '1' && values[1] === '0')) {
        return { kind: 'boolean', values: [0, 1] };
      }
    }
    return { kind: 'enum', values };
  }

  // Range: BETWEEN X AND Y
  const rangeMatch = checkRaw.match(/BETWEEN\s+(\d+)\s+AND\s+(\d+)/i);
  if (rangeMatch) {
    return { kind: 'range', min: parseInt(rangeMatch[1], 10), max: parseInt(rangeMatch[2], 10) };
  }

  // Boolean: IN (0, 1) or IN (1, 0)
  const boolMatch = checkRaw.match(/IN\s*\(\s*0\s*,\s*1\s*\)/i) || checkRaw.match(/IN\s*\(\s*1\s*,\s*0\s*\)/i);
  if (boolMatch) {
    return { kind: 'boolean', values: [0, 1] };
  }

  // Default: raw expression
  return { kind: 'raw', expr: checkRaw };
}

// ============================================================
// STRUCTURAL HELPERS (exported)
// ============================================================

export function isChildAggregateColumn(
  table: string,
  column: string,
  fkGraph: FKGraph
): boolean {
  const aggregatePattern = /^(subtotal|tax_total|discount_total|grand_total|line_total|total_amount|total_price|total_rows|success_rows|failed_rows)$/;
  return aggregatePattern.test(column) && fkGraph.getInbound(table).length > 0;
}

export function getEnumValues(
  table: string,
  column: string,
  constraints: Map<string, TypedConstraint>
): string[] | null {
  const key = `${table}.${column}`;
  const constraint = constraints.get(key);
  if (constraint && constraint.typed.kind === 'enum') {
    return constraint.typed.values;
  }
  return null;
}

export function getConditionalDependency(
  table: string,
  column: string,
  rawMap: RawTableMap,
  constraints: Map<string, TypedConstraint>
): { whenColumn: string; whenValue: string } | null {
  const tableDef = rawMap.get(table);
  if (!tableDef) return null;

  // Check for sibling 'status' column with enum constraint
  const statusCol = tableDef.columns.get('status');
  if (!statusCol) return null;

  const statusEnum = getEnumValues(table, 'status', constraints);
  if (!statusEnum) return null;

  // Pattern matching for conditional columns
  if (column.match(/^resolved(_|_at$)/) && statusEnum.includes('resolved')) {
    return { whenColumn: 'status', whenValue: 'resolved' };
  }
  if (column.match(/^closed(_|_at$)/) && statusEnum.includes('closed')) {
    return { whenColumn: 'status', whenValue: 'closed' };
  }
  if (column === 'actual_close_date' && statusEnum.includes('won')) {
    return { whenColumn: 'status', whenValue: 'won' };
  }
  if (column === 'loss_reason' && statusEnum.includes('lost')) {
    return { whenColumn: 'status', whenValue: 'lost' };
  }
  if (column === 'converted_at') {
    // Check for sibling converted_* FK columns
    for (const [colName, colDef] of tableDef.columns) {
      if (colName.match(/^converted_/) && colDef.type === 'INT') {
        return { whenColumn: 'status', whenValue: 'converted' };
      }
    }
  }

  return null;
}
