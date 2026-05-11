import type { QueryIntent } from '../query-ast/query-intent.js';
import type { MultiSourceSchema } from '../../schema/MultiSourceSchemaBuilder.js';

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
