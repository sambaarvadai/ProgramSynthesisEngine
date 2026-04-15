import Database from 'better-sqlite3';
import { v4 as uuidv4 } from 'uuid';
import type { SchemaConfig, TableConfig, ColumnConfig } from '../compiler/schema/schema-config.js';
import { auditStore, AuditAction } from './audit-store.js';

export interface AccessRequest {
  id: string;
  userId: string;
  tableName: string;
  columnName?: string;
  status: 'pending' | 'approved' | 'denied';
  requestedAt: number;
  reviewedAt?: number;
  reviewedBy?: string;
}

export interface User {
  id: string;
  username: string;
  passwordHash: string;
  role: string;
  createdAt: number;
  lastLogin?: number;
}

export interface TableGrant {
  id: string;
  userId: string;
  tableName: string;
  canRead: number;
  canWrite: number;
}

export interface ColumnGrant {
  id: string;
  userId: string;
  tableName: string;
  columnName: string;
  canRead: number;
  canWrite: number;
}

export class GrantStore {
  private db: Database.Database;

  constructor(dbPath: string = './pipelines.db') {
    this.db = new Database(dbPath);
    this.initializeSchema();
  }

  private initializeSchema(): void {
    // Create users table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        last_login INTEGER
      )
    `);

    // Create table_grants table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS table_grants (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        table_name TEXT NOT NULL,
        can_read INTEGER NOT NULL DEFAULT 0,
        can_write INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        UNIQUE(user_id, table_name)
      )
    `);

    // Create column_grants table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS column_grants (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        can_read INTEGER NOT NULL DEFAULT 0,
        can_write INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        UNIQUE(user_id, table_name, column_name)
      )
    `);

    // Create access_requests table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS access_requests (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        table_name TEXT NOT NULL,
        column_name TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        requested_at INTEGER NOT NULL,
        reviewed_at INTEGER,
        reviewed_by TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
      )
    `);

    // Create indexes for better performance
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_table_grants_user_id ON table_grants(user_id);
      CREATE INDEX IF NOT EXISTS idx_table_grants_table_name ON table_grants(table_name);
      CREATE INDEX IF NOT EXISTS idx_column_grants_user_id ON column_grants(user_id);
      CREATE INDEX IF NOT EXISTS idx_column_grants_table_name ON column_grants(table_name);
      CREATE INDEX IF NOT EXISTS idx_access_requests_user_id ON access_requests(user_id);
      CREATE INDEX IF NOT EXISTS idx_access_requests_status ON access_requests(status);
    `);
  }

  getGrantedSchema(userId: string, fullSchema: SchemaConfig): SchemaConfig {
    const grantedTables = new Map<string, TableConfig>();
    const grantedForeignKeys: typeof fullSchema.foreignKeys = [];

    // Get all table grants for the user
    const tableGrants = this.db.prepare(`
      SELECT table_name, can_read, can_write
      FROM table_grants
      WHERE user_id = ? AND can_read = 1
    `).all(userId) as { table_name: string; can_read: number; can_write: number }[];

    const grantedTableNames = new Set(tableGrants.map(g => g.table_name));

    // Filter tables based on grants
    for (const [tableName, tableConfig] of fullSchema.tables) {
      if (!grantedTableNames.has(tableName)) {
        continue; // Skip tables without read access
      }

      // Get column grants for this table
      const columnGrants = this.db.prepare(`
        SELECT column_name, can_read
        FROM column_grants
        WHERE user_id = ? AND table_name = ? AND can_read = 1
      `).all(userId, tableName) as { column_name: string; can_read: number }[];

      const grantedColumnNames = new Set(columnGrants.map(g => g.column_name));

      // Filter columns based on grants (if no column grants exist, all columns are granted)
      let grantedColumns: ColumnConfig[];
      if (columnGrants.length > 0) {
        grantedColumns = tableConfig.columns.filter(col => grantedColumnNames.has(col.name));
      } else {
        // No column-specific grants, so all columns are granted
        grantedColumns = tableConfig.columns;
      }

      // Create the filtered table config
      const filteredTableConfig: TableConfig = {
        ...tableConfig,
        columns: grantedColumns
      };

      grantedTables.set(tableName, filteredTableConfig);
    }

    // Filter foreign keys to only include relationships between granted tables
    for (const fk of fullSchema.foreignKeys) {
      if (grantedTables.has(fk.fromTable) && grantedTables.has(fk.toTable)) {
        grantedForeignKeys.push(fk);
      }
    }

    return {
      version: fullSchema.version,
      description: fullSchema.description,
      tables: grantedTables,
      foreignKeys: grantedForeignKeys
    };
  }

  checkTableAccess(userId: string, tableName: string, mode: 'read' | 'write'): boolean {
    const grant = this.db.prepare(`
      SELECT can_read, can_write
      FROM table_grants
      WHERE user_id = ? AND table_name = ?
    `).get(userId, tableName) as { can_read: number; can_write: number } | undefined;

    if (!grant) {
      return false; // No grant exists
    }

    return mode === 'read' ? grant.can_read === 1 : grant.can_write === 1;
  }

  checkColumnAccess(userId: string, tableName: string, columnName: string, mode: 'read' | 'write'): boolean {
    // First check if user has table-level access
    if (!this.checkTableAccess(userId, tableName, mode)) {
      return false;
    }

    // Check column-specific grants
    const columnGrant = this.db.prepare(`
      SELECT can_read, can_write
      FROM column_grants
      WHERE user_id = ? AND table_name = ? AND column_name = ?
    `).get(userId, tableName, columnName) as { can_read: number; can_write: number } | undefined;

    if (!columnGrant) {
      // No column-specific grant, inherit from table-level access
      return true;
    }

    return mode === 'read' ? columnGrant.can_read === 1 : columnGrant.can_write === 1;
  }

  requestAccess(userId: string, tableName: string, columnName?: string): string {
    const requestId = uuidv4();
    const now = Date.now();

    this.db.prepare(`
      INSERT INTO access_requests (id, user_id, table_name, column_name, status, requested_at)
      VALUES (?, ?, ?, ?, 'pending', ?)
    `).run(requestId, userId, tableName, columnName || null, now);

    // Add audit logging
    const user = this.getUserById(userId);
    if (user) {
      auditStore.log({
        userId,
        username: user.username,
        role: user.role,
        action: AuditAction.ACCESS_REQUESTED,
        resourceId: requestId,
        resourceName: columnName ? `${tableName}.${columnName}` : tableName,
        status: 'success',
        details: { tableName, columnName }
      });
    }

    return requestId;
  }

  approveAccess(requestId: string, adminUserId: string): void {
    const now = Date.now();

    // Get the request details
    const request = this.db.prepare(`
      SELECT user_id, table_name, column_name
      FROM access_requests
      WHERE id = ? AND status = 'pending'
    `).get(requestId) as { user_id: string; table_name: string; column_name: string | null } | undefined;

    if (!request) {
      throw new Error('Access request not found or already processed');
    }

    // Update request status
    this.db.prepare(`
      UPDATE access_requests
      SET status = 'approved', reviewed_at = ?, reviewed_by = ?
      WHERE id = ?
    `).run(now, adminUserId, requestId);

    // Grant the access
    if (request.column_name) {
      // Column-specific grant
      this.db.prepare(`
        INSERT OR REPLACE INTO column_grants (id, user_id, table_name, column_name, can_read, can_write)
        VALUES (?, ?, ?, ?, 1, 1)
      `).run(uuidv4(), request.user_id, request.table_name, request.column_name);
    } else {
      // Table-level grant
      this.db.prepare(`
        INSERT OR REPLACE INTO table_grants (id, user_id, table_name, can_read, can_write)
        VALUES (?, ?, ?, 1, 1)
      `).run(uuidv4(), request.user_id, request.table_name);
    }

    // Add audit logging
    const adminUser = this.getUserById(adminUserId);
    if (adminUser) {
      const target = request.column_name ? `${request.table_name}.${request.column_name}` : request.table_name;
      auditStore.log({
        userId: adminUserId,
        username: adminUser.username,
        role: adminUser.role,
        action: AuditAction.ACCESS_APPROVED,
        resourceId: requestId,
        resourceName: target,
        status: 'success',
        details: { grantedTo: request.user_id, tableName: request.table_name, columnName: request.column_name }
      });
    }
  }

  denyAccess(requestId: string, adminUserId: string): void {
    const now = Date.now();

    // Get the request details to ensure it exists
    const request = this.db.prepare(`
      SELECT user_id, table_name, column_name
      FROM access_requests
      WHERE id = ? AND status = 'pending'
    `).get(requestId) as { user_id: string; table_name: string; column_name: string | null } | undefined;

    if (!request) {
      throw new Error('Access request not found or already processed');
    }

    // Update request status to denied
    this.db.prepare(`
      UPDATE access_requests
      SET status = 'denied', reviewed_at = ?, reviewed_by = ?
      WHERE id = ?
    `).run(now, adminUserId, requestId);

    // Add audit logging
    const adminUser = this.getUserById(adminUserId);
    if (adminUser) {
      const target = request.column_name ? `${request.table_name}.${request.column_name}` : request.table_name;
      auditStore.log({
        userId: adminUserId,
        username: adminUser.username,
        role: adminUser.role,
        action: AuditAction.ACCESS_DENIED,
        resourceId: requestId,
        resourceName: target,
        status: 'success',
        details: { deniedTo: request.user_id, tableName: request.table_name, columnName: request.column_name }
      });
    }
  }

  listPendingRequests(): AccessRequest[] {
    return this.db.prepare(`
      SELECT id, user_id, table_name, column_name, status, requested_at, reviewed_at, reviewed_by
      FROM access_requests
      WHERE status = 'pending'
      ORDER BY requested_at ASC
    `).all() as AccessRequest[];
  }

  // Helper methods for user management
  createUser(username: string, passwordHash: string, role: string): string {
    const userId = uuidv4();
    const now = Date.now();

    this.db.prepare(`
      INSERT INTO users (id, username, password_hash, role, created_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(userId, username, passwordHash, role, now);

    return userId;
  }

  getUserByUsername(username: string): User | undefined {
    return this.db.prepare(`
      SELECT id, username, password_hash, role, created_at, last_login
      FROM users
      WHERE username = ?
    `).get(username) as User | undefined;
  }

  getUserById(userId: string): User | undefined {
    return this.db.prepare(`
      SELECT id, username, password_hash, role, created_at, last_login
      FROM users
      WHERE id = ?
    `).get(userId) as User | undefined;
  }

  updateLastLogin(userId: string): void {
    this.db.prepare(`
      UPDATE users
      SET last_login = ?
      WHERE id = ?
    `).run(Date.now(), userId);
  }

  // Helper methods for grant management
  grantTableAccess(userId: string, tableName: string, canRead: boolean, canWrite: boolean): void {
    this.db.prepare(`
      INSERT OR REPLACE INTO table_grants (id, user_id, table_name, can_read, can_write)
      VALUES (?, ?, ?, ?, ?)
    `).run(uuidv4(), userId, tableName, canRead ? 1 : 0, canWrite ? 1 : 0);
  }

  grantColumnAccess(userId: string, tableName: string, columnName: string, canRead: boolean, canWrite: boolean): void {
    this.db.prepare(`
      INSERT OR REPLACE INTO column_grants (id, user_id, table_name, column_name, can_read, can_write)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(uuidv4(), userId, tableName, columnName, canRead ? 1 : 0, canWrite ? 1 : 0);
  }

  revokeTableAccess(userId: string, tableName: string): void {
    this.db.prepare(`
      DELETE FROM table_grants
      WHERE user_id = ? AND table_name = ?
    `).run(userId, tableName);

    // Also revoke column-level access for this table
    this.db.prepare(`
      DELETE FROM column_grants
      WHERE user_id = ? AND table_name = ?
    `).run(userId, tableName);
  }

  revokeColumnAccess(userId: string, tableName: string, columnName: string): void {
    this.db.prepare(`
      DELETE FROM column_grants
      WHERE user_id = ? AND table_name = ? AND column_name = ?
    `).run(userId, tableName, columnName);
  }

  close(): void {
    this.db.close();
  }
}

// Export singleton instance
export const grantStore = new GrantStore();
