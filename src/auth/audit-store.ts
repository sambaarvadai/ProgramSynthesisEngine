import Database from 'better-sqlite3';
import { randomUUID } from 'crypto';

export enum AuditAction {
  PIPELINE_PLANNED = 'pipeline.planned',
  PIPELINE_EXECUTED = 'pipeline.executed',
  LOGIN = 'auth.login',
  LOGOUT = 'auth.logout',
  ACCESS_REQUESTED = 'access.requested',
  ACCESS_APPROVED = 'access.approved',
  ACCESS_DENIED = 'access.denied'
}

export interface AuditEntry {
  id: string;
  userId: string;
  username: string;
  role: string;
  action: AuditAction;
  resourceId?: string;
  resourceName?: string;
  status: 'success' | 'failed' | 'cancelled' | 'planned';
  details?: Record<string, any>;
  error?: string;
  durationMs?: number;
  timestamp: number;
}

export class AuditStore {
  private db: Database.Database;

  constructor(dbPath: string = './pipelines.db') {
    this.db = new Database(dbPath);
    this.initializeSchema();
  }

  private initializeSchema(): void {
    // Create audit_log table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS audit_log (
        id           TEXT PRIMARY KEY,
        user_id      TEXT NOT NULL,
        username     TEXT NOT NULL,
        role         TEXT NOT NULL,
        action       TEXT NOT NULL,
        resource_id  TEXT,
        resource_name TEXT,
        status       TEXT NOT NULL,
        details      TEXT,
        error        TEXT,
        duration_ms  INTEGER,
        timestamp    INTEGER NOT NULL
      )
    `);

    // Create indexes for better performance
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_audit_user      ON audit_log(user_id, timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_audit_action    ON audit_log(action, timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_audit_resource  ON audit_log(resource_id, timestamp DESC);
    `);
  }

  log(entry: Omit<AuditEntry, 'id' | 'timestamp'>): string {
    const id = randomUUID();
    const timestamp = Date.now();

    this.db.prepare(`
      INSERT INTO audit_log (
        id, user_id, username, role, action, resource_id, resource_name,
        status, details, error, duration_ms, timestamp
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      entry.userId,
      entry.username,
      entry.role,
      entry.action,
      entry.resourceId || null,
      entry.resourceName || null,
      entry.status,
      entry.details ? JSON.stringify(entry.details) : null,
      entry.error || null,
      entry.durationMs || null,
      timestamp
    );

    return id;
  }

  getForUser(userId: string, limit = 50): AuditEntry[] {
    const rows = this.db.prepare(`
      SELECT id, user_id, username, role, action, resource_id, resource_name,
             status, details, error, duration_ms, timestamp
      FROM audit_log
      WHERE user_id = ?
      ORDER BY timestamp DESC
      LIMIT ?
    `).all(userId, limit) as any[];

    return this.mapRowsToEntries(rows);
  }

  getAll(limit = 100): AuditEntry[] {
    const rows = this.db.prepare(`
      SELECT id, user_id, username, role, action, resource_id, resource_name,
             status, details, error, duration_ms, timestamp
      FROM audit_log
      ORDER BY timestamp DESC
      LIMIT ?
    `).all(limit) as any[];

    return this.mapRowsToEntries(rows);
  }

  getForResource(resourceId: string): AuditEntry[] {
    const rows = this.db.prepare(`
      SELECT id, user_id, username, role, action, resource_id, resource_name,
             status, details, error, duration_ms, timestamp
      FROM audit_log
      WHERE resource_id = ?
      ORDER BY timestamp DESC
    `).all(resourceId) as any[];

    return this.mapRowsToEntries(rows);
  }

  formatTable(entries: AuditEntry[]): string {
    if (entries.length === 0) {
      return 'No audit entries found.';
    }

    const header = 'Timestamp            | User    | Action              | Status   | Resource / Details';
    const separator = '-------------------- | ------- | ------------------- | -------- | -------------------';

    const rows = entries.map(entry => {
      const timestamp = new Date(entry.timestamp).toLocaleString();
      const user = entry.username.padEnd(7);
      const action = entry.action.padEnd(19);
      const status = entry.status.padEnd(8);

      let resourceDetails = '';
      if (entry.resourceName) {
        resourceDetails = entry.resourceName;
      }
      
      if (entry.durationMs) {
        const duration = `${entry.durationMs}ms`;
        if (entry.details?.rows) {
          resourceDetails += ` (${duration}, ${entry.details.rows} row${entry.details.rows !== 1 ? 's' : ''})`;
        } else {
          resourceDetails += ` (${duration})`;
        }
      } else if (entry.details && Object.keys(entry.details).length > 0) {
        const detailsStr = JSON.stringify(entry.details);
        resourceDetails = detailsStr.length > 50 ? detailsStr.substring(0, 47) + '...' : detailsStr;
      }

      resourceDetails = resourceDetails.padEnd(50);

      return `${timestamp} | ${user} | ${action} | ${status} | ${resourceDetails}`;
    });

    return [header, separator, ...rows].join('\n');
  }

  private mapRowsToEntries(rows: any[]): AuditEntry[] {
    return rows.map(row => ({
      id: row.id,
      userId: row.user_id,
      username: row.username,
      role: row.role,
      action: row.action as AuditAction,
      resourceId: row.resource_id,
      resourceName: row.resource_name,
      status: row.status as AuditEntry['status'],
      details: row.details ? JSON.parse(row.details) : undefined,
      error: row.error,
      durationMs: row.duration_ms,
      timestamp: row.timestamp
    }));
  }

  close(): void {
    this.db.close();
  }
}

// Export singleton instance
export const auditStore = new AuditStore();
