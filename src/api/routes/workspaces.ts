import { Router, type Request, type Response } from 'express'
import type { BootstrappedServices } from '../../bootstrap.js'

let services: BootstrappedServices | null = null

export function setWorkspaceServices(s: BootstrappedServices | null): void {
  services = s
}

export function workspaceRoutes(): Router {
  const router = Router()

  router.get('/', async (req: Request, res: Response) => {
    if (!services) {
      res.status(500).json({
        error: { code: 'SERVICES_NOT_INITIALIZED', message: 'Services not initialized' }
      })
      return
    }

    const result = await services.crmPool.query(
      `SELECT id, name FROM workspaces WHERE deleted_at IS NULL ORDER BY id`
    )

    res.json(result.rows.map(row => ({
      id: String(row.id),
      displayName: row.name,
      kind: 'workspace'
    })))
  })

  return router
}
