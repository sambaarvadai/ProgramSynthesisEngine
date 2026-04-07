// Defines validation rules and validation utilities

export interface ValidationError {
  code: string;
  message: string;
  nodeId?: string;
  path?: string[];
}

export type ValidationResult =
  | { ok: true }
  | { ok: false; errors: ValidationError[] };

export function validationOk(): ValidationResult {
  return { ok: true };
}

export function validationFail(errors: ValidationError[]): ValidationResult {
  return { ok: false, errors };
}
