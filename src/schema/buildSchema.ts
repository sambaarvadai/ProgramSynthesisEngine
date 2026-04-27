#!/usr/bin/env ts-node
import { readFileSync } from 'fs';
import { parseSchema } from './DDLParser.js';
import { inferTraits } from './TraitInferencer.js';

// Parse command line arguments
const args = process.argv.slice(2);
let sqlPath = './crm.sql';
let anchorTables: string[] = [];

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--sql' && i + 1 < args.length) {
    sqlPath = args[i + 1];
  } else if (args[i] === '--anchor-tables' && i + 1 < args.length) {
    anchorTables = args[i + 1].split(',');
  }
}

// Read SQL file
const ddl = readFileSync(sqlPath, 'utf-8');

// Parse schema
const parsed = parseSchema(ddl);

console.log('\n[Constraints by table]:');
const byTable = new Map<string, string[]>();
for (const [key, c] of parsed.constraints) {
  const t = c.table;
  if (!byTable.has(t)) byTable.set(t, []);
  byTable.get(t)!.push(`${c.column}(${c.typed.kind})`);
}
for (const [table, cols] of byTable) {
  console.log(`  ${table}: ${cols.join(', ')}`);
}

// Count foreign keys
let totalFKs = 0;
for (const [tableName, tableDef] of parsed.tables) {
  totalFKs += tableDef.foreignKeys.length;
}

// Infer traits
const traits = inferTraits(parsed, { sessionAnchorTables: anchorTables });

// Check FK count
if (totalFKs !== 91) {
  console.error(`\n[ERROR] Expected 91 foreign keys, but found ${totalFKs}`);
  console.error('FK warnings may indicate AST shape issues with node-sql-parser version\n');
  process.exit(1);
}

console.log('\n[SUCCESS] Schema parsed successfully with 91 foreign keys');
