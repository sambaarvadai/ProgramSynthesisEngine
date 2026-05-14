import type { Request, Response, NextFunction } from 'express'

export interface ApiError {
  error: {
    code: string
    message: string
  }
}

export function errorMiddleware(
  err: Error,
  req: Request,
  res: Response,
  next: NextFunction
): void {
  console.error('[API Error]', err)
  
  const errorResponse: ApiError = {
    error: {
      code: 'INTERNAL_ERROR',
      message: err.message || 'An unexpected error occurred',
    },
  }
  
  res.status(500).json(errorResponse)
}

export function createError(code: string, message: string): ApiError {
  return {
    error: { code, message },
  }
}
