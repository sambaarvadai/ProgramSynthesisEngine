import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { parseSchema, isChildAggregateColumn, getEnumValues, getConditionalDependency } from '../../src/schema/DDLParser.js';

// Simple test runner for Node.js
function describe(name: string, fn: () => void) {
  console.log(`\n${name}`);
  fn();
}

function it(name: string, fn: () => void) {
  try {
    fn();
    console.log(`  ✓ ${name}`);
  } catch (error: any) {
    console.error(`  ✗ ${name}`);
    console.error(`    ${error.message}`);
    throw error;
  }
}

function expect<T>(actual: T) {
  const not = {
    toBeNull: () => {
      if (actual === null) {
        throw new Error(`Expected not null, but got null`);
      }
    },
    toBeUndefined: () => {
      if (actual === undefined) {
        throw new Error(`Expected not undefined, but got undefined`);
      }
    }
  };

  return {
    toBe: (expected: T) => {
      if (actual !== expected) {
        throw new Error(`Expected ${expected}, but got ${actual}`);
      }
    },
    toBeDefined: () => {
      if (actual === undefined || actual === null) {
        throw new Error(`Expected value to be defined, but got ${actual}`);
      }
    },
    toBeGreaterThan: (expected: number) => {
      if (typeof actual !== 'number' || actual <= expected) {
        throw new Error(`Expected ${actual} to be greater than ${expected}`);
      }
    },
    toBeLessThan: (expected: number) => {
      if (typeof actual !== 'number' || actual >= expected) {
        throw new Error(`Expected ${actual} to be less than ${expected}`);
      }
    },
    toContain: (expected: any) => {
      if (!Array.isArray(actual) || !actual.includes(expected)) {
        throw new Error(`Expected ${JSON.stringify(actual)} to contain ${expected}`);
      }
    },
    toMatch: (expected: RegExp) => {
      if (typeof actual !== 'string' || !expected.test(actual)) {
        throw new Error(`Expected "${actual}" to match ${expected}`);
      }
    },
    toEqual: (expected: T) => {
      if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(`Expected ${JSON.stringify(expected)}, but got ${JSON.stringify(actual)}`);
      }
    },
    toBeNull: () => {
      if (actual !== null) {
        throw new Error(`Expected null, but got ${actual}`);
      }
    },
    toBeUndefined: () => {
      if (actual !== undefined) {
        throw new Error(`Expected undefined, but got ${actual}`);
      }
    },
    not
  };
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const ddlPath = join(__dirname, '../../crm.sql');
const ddl = readFileSync(ddlPath, 'utf-8');

describe('DDLParser', () => {
  it('parseSchema integration test', () => {
    const startTime = performance.now();
    const schema = parseSchema(ddl);
    const endTime = performance.now();
    const duration = endTime - startTime;

    // Verify counts
    expect(schema.tables.size).toBe(40);
    expect(schema.indexes.size).toBe(17);
    
    // Count total foreign keys
    let totalFKs = 0;
    for (const tableDef of schema.tables.values()) {
      totalFKs += tableDef.foreignKeys.length;
    }
    expect(totalFKs).toBe(91);

    // Verify constraints have typed entries for CHECK constraints
    expect(schema.constraints.size).toBeGreaterThan(0);

    // Verify performance (< 500ms)
    expect(duration).toBeLessThan(500);
  });

  it('tickets table spot checks', () => {
    const schema = parseSchema(ddl);
    const tickets = schema.tables.get('tickets');
    expect(tickets).toBeDefined();
    expect(tickets!.columns.size).toBe(17);

    const workspaceId = tickets!.columns.get('workspace_id');
    expect(workspaceId).toBeDefined();
    expect(workspaceId!.type).toBe('INT');
    expect(workspaceId!.nullable).toBe(false);
    expect(workspaceId!.primaryKey).toBe(false);

    const status = tickets!.columns.get('status');
    expect(status).toBeDefined();
    expect(status!.checkRaw).toMatch(/open/);
    expect(status!.checkRaw).toMatch(/pending/);
    expect(status!.checkRaw).toMatch(/resolved/);
    expect(status!.checkRaw).toMatch(/closed/);

    const deletedAt = tickets!.columns.get('deleted_at');
    expect(deletedAt).toBeUndefined();

    // Verify foreign keys
    const workspaceFK = tickets!.foreignKeys.find(fk => fk.column === 'workspace_id');
    expect(workspaceFK).toBeDefined();
    expect(workspaceFK!.refTable).toBe('workspaces');
    expect(workspaceFK!.refColumn).toBe('id');
    expect(workspaceFK!.onDelete).toBe('CASCADE');

    const ownerFK = tickets!.foreignKeys.find(fk => fk.column === 'owner_user_id');
    expect(ownerFK).toBeDefined();
    expect(ownerFK!.refTable).toBe('users');
    expect(ownerFK!.refColumn).toBe('id');
    expect(ownerFK!.onDelete).toBe('SET NULL');
  });

  it('TypedConstraintMap tests', () => {
    const schema = parseSchema(ddl);

    // tickets.status → enum
    const statusConstraint = schema.constraints.get('tickets.status');
    expect(statusConstraint).toBeDefined();
    if (statusConstraint!.typed.kind === 'enum') {
      expect(statusConstraint!.typed.values).toContain('open');
      expect(statusConstraint!.typed.values).toContain('pending');
      expect(statusConstraint!.typed.values).toContain('resolved');
      expect(statusConstraint!.typed.values).toContain('closed');
    }

    // tickets.priority → enum
    const priorityConstraint = schema.constraints.get('tickets.priority');
    expect(priorityConstraint).toBeDefined();
    if (priorityConstraint!.typed.kind === 'enum') {
      expect(priorityConstraint!.typed.values).toContain('low');
      expect(priorityConstraint!.typed.values).toContain('medium');
      expect(priorityConstraint!.typed.values).toContain('high');
      expect(priorityConstraint!.typed.values).toContain('urgent');
    }

    // pipeline_stages.probability_percent → range
    const probConstraint = schema.constraints.get('pipeline_stages.probability_percent');
    expect(probConstraint).toBeDefined();
    if (probConstraint!.typed.kind === 'range') {
      expect(probConstraint!.typed.min).toBe(0);
      expect(probConstraint!.typed.max).toBe(100);
    }
  });

  it('FKGraph tests', () => {
    const schema = parseSchema(ddl);

    // workspaces inbound.length > 10
    const workspacesInbound = schema.fkGraph.getInbound('workspaces');
    expect(workspacesInbound.length).toBeGreaterThan(10);

    // isReachable('tickets', 'workspaces', 1) === true
    expect(schema.fkGraph.isReachable('tickets', 'workspaces', 1)).toBe(true);

    // isReachable('quote_items', 'workspaces', 3) === true
    expect(schema.fkGraph.isReachable('quote_items', 'workspaces', 3)).toBe(true);

    // getReferencedBy('workspaces') includes 'tickets', 'users', 'accounts'
    const referencedBy = schema.fkGraph.getReferencedBy('workspaces');
    expect(referencedBy).toContain('tickets');
    expect(referencedBy).toContain('users');
    expect(referencedBy).toContain('accounts');
  });

  it('isChildAggregateColumn tests', () => {
    const schema = parseSchema(ddl);

    expect(isChildAggregateColumn('quotes', 'grand_total', schema.fkGraph)).toBe(true);
    expect(isChildAggregateColumn('tickets', 'subject', schema.fkGraph)).toBe(false);
  });

  it('getEnumValues tests', () => {
    const schema = parseSchema(ddl);

    const statusValues = getEnumValues('tickets', 'status', schema.constraints);
    expect(statusValues).not.toBeNull();
    expect(statusValues).toContain('open');
    expect(statusValues).toContain('pending');
    expect(statusValues).toContain('resolved');
    expect(statusValues).toContain('closed');
  });

  it('getConditionalDependency tests', () => {
    const schema = parseSchema(ddl);

    const resolvedAtDep = getConditionalDependency('tickets', 'resolved_at', schema.tables, schema.constraints);
    expect(resolvedAtDep).toEqual({ whenColumn: 'status', whenValue: 'resolved' });

    const lossReasonDep = getConditionalDependency('opportunities', 'loss_reason', schema.tables, schema.constraints);
    expect(lossReasonDep).toEqual({ whenColumn: 'status', whenValue: 'lost' });

    const subjectDep = getConditionalDependency('tickets', 'subject', schema.tables, schema.constraints);
    expect(subjectDep).toBeNull();
  });
});
