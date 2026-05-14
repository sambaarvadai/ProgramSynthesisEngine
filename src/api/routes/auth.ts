import { Router, type Request, type Response } from 'express'
import { grantStore } from '../../auth/grant-store.js'
import jwt from 'jsonwebtoken'

const JWT_SECRET = process.env.JWT_SECRET || 'pee-dev-secret-key'

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  token: string
  user: {
    id: string
    username: string
    role: string
  }
}

// Ensure default users exist for development
function ensureDefaultUsers() {
  const admin = grantStore.getUserByUsername('admin')
  const db = (grantStore as any).db
  
  if (!admin) {
    console.log('[Auth] Creating default admin user with fixed ID')
    // Use fixed ID to match seeded grants
    const now = Date.now()
    db.prepare(`
      INSERT INTO users (id, username, password_hash, role, created_at)
      VALUES (?, ?, ?, ?, ?)
    `).run('user_admin', 'admin', 'dev', 'admin', now)
    // Grant full access to admin on all tables
    const crmTables = [
      'workspaces', 'users', 'teams', 'roles', 'user_workspace_memberships',
      'accounts', 'contacts', 'contact_account_links', 'leads', 'pipelines',
      'pipeline_stages', 'opportunities', 'opportunity_stage_history',
      'assignments_history', 'products', 'opportunity_products', 'quotes',
      'quote_items', 'tasks', 'activities', 'notes', 'attachments', 'emails',
      'email_participants', 'calls', 'messages', 'tickets', 'ticket_comments',
      'tags', 'entity_tags', 'custom_fields', 'custom_field_values',
      'lead_score_events', 'integrations', 'webhooks', 'workflow_rules',
      'imports', 'dedup_rules', 'merge_history', 'audit_logs', 'projects',
      'project_members', 'milestones', 'time_logs', 'comments', 'project_activity'
    ]
    crmTables.forEach(table => {
      grantStore.grantTableAccess('user_admin', table, true, true)
    })
  } else {
    // Update existing admin user to have fixed ID and password
    if (admin.id !== 'user_admin' || !admin.passwordHash) {
      console.log('[Auth] Updating existing admin user with fixed ID and password')
      // Update ID and password
      db.prepare('UPDATE users SET id = ?, password_hash = ? WHERE username = ?').run('user_admin', 'dev', 'admin')
      // Re-grant access with new ID
      const crmTables = [
        'workspaces', 'users', 'teams', 'roles', 'user_workspace_memberships',
        'accounts', 'contacts', 'contact_account_links', 'leads', 'pipelines',
        'pipeline_stages', 'opportunities', 'opportunity_stage_history',
        'assignments_history', 'products', 'opportunity_products', 'quotes',
        'quote_items', 'tasks', 'activities', 'notes', 'attachments', 'emails',
        'email_participants', 'calls', 'messages', 'tickets', 'ticket_comments',
        'tags', 'entity_tags', 'custom_fields', 'custom_field_values',
        'lead_score_events', 'integrations', 'webhooks', 'workflow_rules',
        'imports', 'dedup_rules', 'merge_history', 'audit_logs', 'projects',
        'project_members', 'milestones', 'time_logs', 'comments', 'project_activity'
      ]
      crmTables.forEach(table => {
        grantStore.grantTableAccess('user_admin', table, true, true)
      })
    }
  }
}

export function authRoutes(): Router {
  const router = Router()

  // POST /api/auth/login
  router.post('/login', (req: Request, res: Response) => {
    console.log('[Auth] Login attempt')
    
    // Ensure default users exist
    ensureDefaultUsers()
    
    const { username, password } = req.body as LoginRequest

    if (!username || !password) {
      res.status(400).json({
        error: {
          code: 'INVALID_REQUEST',
          message: 'Username and password are required',
        },
      })
      return
    }

    const user = grantStore.getUserByUsername(username)
    console.log('[Auth] User found:', user ? user.username : 'not found')
    console.log('[Auth] Stored password hash:', user?.passwordHash)
    console.log('[Auth] Provided password:', password)

    if (!user) {
      res.status(401).json({
        error: {
          code: 'INVALID_CREDENTIALS',
          message: 'Invalid username or password',
        },
      })
      return
    }

    // For development, passwords are stored in plaintext
    // In production, use bcrypt.compare()
    // Also accept any password if passwordHash is undefined (dev workaround)
    if (user.passwordHash && user.passwordHash !== password) {
      console.log('[Auth] Password mismatch')
      res.status(401).json({
        error: {
          code: 'INVALID_CREDENTIALS',
          message: 'Invalid username or password',
        },
      })
      return
    }

    // Update last login
    grantStore.updateLastLogin(user.id)

    // Generate JWT token
    const token = jwt.sign(
      { userId: user.id, username: user.username, role: user.role },
      JWT_SECRET,
      { expiresIn: '24h' }
    )

    console.log('[Auth] Login successful for:', username)

    res.json({
      token,
      user: {
        id: user.id,
        username: user.username,
        role: user.role,
      },
    } as LoginResponse)
  })

  return router
}
