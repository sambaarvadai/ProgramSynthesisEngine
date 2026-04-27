import { parseSchema, type ParsedSchema, type RawTableMap } from './DDLParser.js';
import { inferTraits, type TraitInferencerConfig } from './TraitInferencer.js';

export interface BuiltSchema {
  parsed: ParsedSchema;
  traits: Map<string, Map<string, any>>;
}

export interface SchemaBuilderConfig {
  sessionAnchorTables: string[];
}

export function buildSchemaFromSQL(
  ddl: string,
  config: SchemaBuilderConfig = { sessionAnchorTables: [] }
): BuiltSchema {
  const parsed = parseSchema(ddl);
  const traits = inferTraits(parsed, config);
  return { parsed, traits };
}

export function toTypeScript(schema: BuiltSchema): string {
  const lines: string[] = [];
  
  lines.push('// Generated schema types');
  lines.push('');
  
  for (const [tableName, tableDef] of schema.parsed.tables) {
    lines.push(`export interface ${toPascalCase(tableName)} {`);
    for (const [colName, colDef] of tableDef.columns) {
      const tsType = mapToTsType(colDef.type);
      const nullable = colDef.nullable ? ' | null' : '';
      lines.push(`  ${colName}: ${tsType}${nullable};`);
    }
    lines.push('}');
    lines.push('');
  }
  
  return lines.join('\n');
}

export function toJSON(schema: BuiltSchema): string {
  const obj: any = {
    tables: {},
    indexes: [],
    constraints: {},
    foreignKeys: []
  };
  
  for (const [tableName, tableDef] of schema.parsed.tables) {
    obj.tables[tableName] = {
      columns: Object.fromEntries(tableDef.columns),
      foreignKeys: tableDef.foreignKeys,
      uniqueConstraints: tableDef.uniqueConstraints
    };
  }
  
  for (const [indexName, indexDef] of schema.parsed.indexes) {
    obj.indexes.push(indexDef);
  }
  
  for (const [key, constraint] of schema.parsed.constraints) {
    obj.constraints[key] = constraint;
  }
  
  for (const [tableName, edges] of schema.parsed.fkGraph.outbound) {
    for (const edge of edges) {
      obj.foreignKeys.push(edge);
    }
  }
  
  return JSON.stringify(obj, null, 2);
}

function toPascalCase(str: string): string {
  return str
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join('');
}

function mapToTsType(sqlType: string): string {
  const upper = sqlType.toUpperCase();
  
  if (upper.startsWith('INT') || upper === 'SERIAL') return 'number';
  if (upper.startsWith('NUMERIC') || upper.startsWith('DECIMAL') || upper === 'REAL' || upper === 'DOUBLE') return 'number';
  if (upper === 'BOOLEAN' || upper === 'BOOL') return 'boolean';
  if (upper === 'TEXT' || upper.startsWith('VARCHAR') || upper === 'CHAR') return 'string';
  if (upper === 'TIMESTAMPTZ' || upper === 'TIMESTAMP') return 'Date';
  if (upper === 'DATE') return 'Date';
  if (upper === 'JSONB' || upper === 'JSON') return 'any';
  if (upper === 'INET') return 'string';
  
  return 'any';
}
