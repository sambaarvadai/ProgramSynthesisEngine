import type { QueryIntent } from '../query-ast/query-intent.js';
import type { MultiSourceSchema } from '../../schema/MultiSourceSchemaBuilder.js';
import type { WritePayload } from '../../nodes/payloads.js';
import type { FKEdge } from '../../schema/DDLParser.js';
import type { BuiltSchema } from '../../schema/SchemaBuilder.js';

export interface DecomposedQuery {
  isCrossDB: boolean;
  steps: DecomposedStep[];
}

export interface DecomposedStep {
  nodeId: string;      // e.g. 'lookup_accounts', 'get_technova_projects'
  datasource: string;  // 'default' or 'pm'
  intent: QueryIntent;
  dependsOn: string[];  // upstream step nodeIds
}

export interface CrossDBFKReference {
  column: string;      // FK column in target table (e.g., 'crm_account_id')
  refTable: string;    // Referenced table (e.g., 'accounts')
  refColumn: string;   // Referenced column (e.g., 'id')
  refDatasource: string; // Datasource of referenced table (e.g., 'default')
  targetDatasource: string; // Datasource of target table (e.g., 'pm')
}

export interface DecomposedWrite {
  isCrossDB: boolean;
  lookupSteps: DecomposedStep[];
  fkReferences: CrossDBFKReference[];
  modifiedPayload: WritePayload;
}

export function detectCrossDBJoins(
  intent: QueryIntent,
  multiSchema: MultiSourceSchema
): boolean {
  if (!intent.joins?.length) return false;
  
  const primaryDS = multiSchema.tableRouting.get(intent.table) ?? 'default';
  
  return intent.joins.some(join => {
    const joinDS = multiSchema.tableRouting.get(join.table) ?? 'default';
    return joinDS !== primaryDS;
  });
}

export function decomposeCrossDBQuery(
  originalNodeId: string,
  intent: QueryIntent,
  multiSchema: MultiSourceSchema
): DecomposedQuery {
  
  const primaryDS = multiSchema.tableRouting.get(intent.table) ?? 'default';
  
  // Separate joins into local (same DB) and foreign (different DB)
  const localJoins = (intent.joins ?? []).filter(
    j => (multiSchema.tableRouting.get(j.table) ?? 'default') === primaryDS
  );
  const foreignJoins = (intent.joins ?? []).filter(
    j => (multiSchema.tableRouting.get(j.table) ?? 'default') !== primaryDS
  );

  if (foreignJoins.length === 0) {
    return { isCrossDB: false, steps: [] };
  }

  console.log(
    `[CrossDBDecomposer] Decomposing "${originalNodeId}": ` +
    `primary=${primaryDS}, foreign joins=[${foreignJoins.map(j => j.table).join(', ')}]`
  );

  const steps: DecomposedStep[] = [];
  const lookupNodeIds: string[] = [];

  // Step A: For each foreign join, create a lookup step in the foreign DB
  for (const join of foreignJoins) {
    const foreignDS = multiSchema.tableRouting.get(join.table) ?? 'default';
    const lookupId = `lookup_${join.table}`;

    // Parse join.on: "projects.crm_account_id" → "accounts.id"
    // We need the foreign table's column (right side of ON)
    const onRight = join.on?.right ?? '';
    const foreignCol = onRight.includes('.')
      ? onRight.split('.')[1]
      : onRight;

    const onLeft = join.on?.left ?? '';
    const primaryLinkCol = onLeft.includes('.')
      ? onLeft.split('.')[1]
      : onLeft;

    // Filters that reference the foreign table
    const foreignFilters = (intent.filters ?? []).filter(
      f => f.table === join.table
    );

    // Build lookup intent: SELECT foreignCol FROM foreignTable WHERE ...
    const lookupIntent: QueryIntent = {
      table: join.table,
      columns: [{ field: foreignCol, table: join.table }],
      filters: foreignFilters,
      joins: []  // lookup steps don't have joins themselves
    };

    steps.push({
      nodeId: lookupId,
      datasource: foreignDS,
      intent: lookupIntent,
      dependsOn: []
    });

    lookupNodeIds.push(lookupId);

    // Store mapping: which primary column links to this lookup's result
    // Used to build the IN filter on the primary step
    (join as any)._lookupNodeId = lookupId;
    (join as any)._foreignCol = foreignCol;
    (join as any)._primaryLinkCol = primaryLinkCol;
  }

  // Step B: Build the primary step — no foreign joins
  // Replace foreign join filters with IN ($lookupNodeId.foreignCol)
  const primaryFilters = (intent.filters ?? []).filter(
    f => !f.table || (multiSchema.tableRouting.get(f.table) ?? 'default') === primaryDS
  );

  // For each foreign join, add a valueRef filter on the primary table
  for (const join of foreignJoins) {
    primaryFilters.push({
      field: (join as any)._primaryLinkCol,
      table: intent.table,
      operator: 'IN',
      // valueRef tells the scheduler to use the upstream step's output
      valueRef: `$${(join as any)._lookupNodeId}.${(join as any)._foreignCol}`
    } as any);
  }

  steps.push({
    nodeId: originalNodeId,
    datasource: primaryDS,
    intent: {
      ...intent,
      joins: localJoins,    // only same-DB joins remain
      filters: primaryFilters
    },
    dependsOn: lookupNodeIds
  });

  return { isCrossDB: true, steps };
}

// ───────────────────────────────────────────────────────────────────
// Cross-Datasource Write Decomposition
// ───────────────────────────────────────────────────────────────────

export function detectCrossDBFK(
  payload: WritePayload,
  multiSchema: MultiSourceSchema,
  schema: BuiltSchema
): CrossDBFKReference[] {
  const targetDS = multiSchema.tableRouting.get(payload.table) ?? 'default';
  const references: CrossDBFKReference[] = [];
  
  // Get FK graph from the target table's schema
  const tableTraits = schema.traits.get(payload.table);
  if (!tableTraits) return references;
  
  // Check each column for FK traits
  for (const [col, traits] of tableTraits) {
    if (!traits.foreignKey) continue;
    
    const { references: refTable, column: refCol } = traits.foreignKey;
    const refDS = multiSchema.tableRouting.get(refTable) ?? 'default';
    
    // Check if this FK crosses datasource boundaries
    if (refDS !== targetDS) {
      references.push({
        column: col,
        refTable,
        refColumn: refCol,
        refDatasource: refDS,
        targetDatasource: targetDS
      });
    }
  }
  
  return references;
}

export function decomposeCrossDBWrite(
  nodeId: string,
  payload: WritePayload,
  fkReferences: CrossDBFKReference[],
  schema: BuiltSchema
): DecomposedWrite {
  if (fkReferences.length === 0) {
    return { isCrossDB: false, lookupSteps: [], fkReferences: [], modifiedPayload: payload };
  }
  
  console.log(
    `[CrossDBDecomposer] Decomposing write "${nodeId}": ` +
    `cross-datasource FKs=[${fkReferences.map(fk => `${fk.column}->${fk.refTable}`).join(', ')}]`
  );
  
  const lookupSteps: DecomposedStep[] = [];
  const modifiedPayload = { ...payload, staticValues: { ...payload.staticValues } };
  const lookupNodeIds: string[] = [];
  
  // For each cross-datasource FK, create a lookup step
  for (const fk of fkReferences) {
    const fkValue = payload.staticValues?.[fk.column];
    
    // Skip if FK column value is not provided (will be resolved from input rows)
    if (fkValue === undefined || fkValue === null) {
      console.log(`[CrossDBDecomposer] Skipping FK ${fk.column}: value not provided (will resolve from input)`);
      continue;
    }
    
    // Skip if value is already an explicit ID (numeric)
    if (typeof fkValue === 'number' && fkValue > 0) {
      console.log(`[CrossDBDecomposer] Skipping FK ${fk.column}: explicit ID provided (${fkValue})`);
      continue;
    }
    
    // If value is a string (e.g., "TechNova Solutions"), create lookup
    if (typeof fkValue === 'string') {
      const lookupId = `lookup_${fk.refTable}_${fk.column}`;
      
      // Determine which field to filter on
      // Try to find a 'name' field in the referenced table, otherwise use the first text field
      const refTableTraits = schema.traits.get(fk.refTable);
      let filterField = 'name'; // default to 'name'
      
      if (refTableTraits) {
        for (const [col, traits] of refTableTraits) {
          if (traits.type === 'TEXT' && (col === 'name' || col.includes('name'))) {
            filterField = col;
            break;
          }
        }
      }
      
      // Build lookup intent: SELECT refColumn FROM refTable WHERE filterField = fkValue
      const lookupIntent: QueryIntent = {
        table: fk.refTable,
        columns: [{ field: fk.refColumn, table: fk.refTable }],
        filters: [
          {
            field: filterField,
            table: fk.refTable,
            operator: '=',
            value: fkValue
          }
        ],
        joins: []
      };
      
      lookupSteps.push({
        nodeId: lookupId,
        datasource: fk.refDatasource,
        intent: lookupIntent,
        dependsOn: []
      });
      
      lookupNodeIds.push(lookupId);
      
      // Replace FK column value with valueRef
      modifiedPayload.staticValues![fk.column] = `$${lookupId}.${fk.refColumn}` as any;
      
      console.log(
        `[CrossDBDecomposer] Added lookup node: ${lookupId} (${fk.refDatasource}) → ${nodeId}`
      );
    }
  }
  
  return {
    isCrossDB: lookupSteps.length > 0,
    lookupSteps,
    fkReferences,
    modifiedPayload
  };
}
