/**
 * Expression sanitizer utilities for removing hallucinated field references
 */

export function stripHallucinatedFieldRefs(
  operations: any[],
  availableFields: string[]
): any[] {
  if (!availableFields.length) return operations;
  
  function fixExpr(expr: any): any {
    if (!expr || typeof expr !== 'object') return expr;
    
    if (expr.kind === 'FieldRef') {
      if (!availableFields.includes(expr.field)) {
        // Hallucinated field - replace with Literal(null)
        console.warn(`Stripping hallucinated FieldRef: '${expr.field}', available: ${availableFields.join(', ')}`);
        return { kind: 'Literal', value: null, type: { kind: 'string' } };
      }
      return expr;
    }
    
    // Recurse into all object properties
    const result: any = {};
    for (const [key, val] of Object.entries(expr)) {
      if (typeof val === 'object' && val !== null) {
        result[key] = Array.isArray(val) ? val.map(fixExpr) : fixExpr(val);
      } else {
        result[key] = val;
      }
    }
    return result;
  }
  
  return operations.map(op => ({
    ...op,
    expr: op.expr ? fixExpr(op.expr) : op.expr,
    predicate: op.predicate ? fixExpr(op.predicate) : op.predicate
  }));
}

export function fixExprFieldRefs(expr: any, availableFields: string[]): any {
  if (!availableFields.length) return expr;
  
  if (!expr || typeof expr !== 'object') return expr;
  
  if (expr.kind === 'FieldRef') {
    if (!availableFields.includes(expr.field)) {
      // Hallucinated field - replace with Literal(null)
      console.warn(`Stripping hallucinated FieldRef: '${expr.field}', available: ${availableFields.join(', ')}`);
      return { kind: 'Literal', value: null, type: { kind: 'string' } };
    }
    return expr;
  }
  
  // Recurse into all object properties
  const result: any = {};
  for (const [key, val] of Object.entries(expr)) {
    if (typeof val === 'object' && val !== null) {
      result[key] = Array.isArray(val) ? val.map(val => fixExprFieldRefs(val, availableFields)) : fixExprFieldRefs(val, availableFields);
    } else {
      result[key] = val;
    }
  }
  return result;
}
