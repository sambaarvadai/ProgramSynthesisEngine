export function flattenLeaves(
  obj: any,
  prefix = '',
  maxDepth = 6
): Record<string, any> {
  if (maxDepth === 0 || obj === null || obj === undefined) {
    return prefix ? { [prefix]: obj } : {}
  }
  
  if (typeof obj !== 'object' || obj instanceof Date) {
    return prefix ? { [prefix]: obj } : {}
  }
  
  if (Array.isArray(obj)) {
    // For arrays: if all elements are scalars, join them
    // If elements are objects, flatten each with index prefix
    if (obj.every(el => typeof el !== 'object' || el === null)) {
      return prefix ? { [prefix]: obj.join(',') } : {}
    }
    const result: Record<string, any> = {}
    obj.forEach((el, i) => {
      Object.assign(result, flattenLeaves(el, prefix ? `${prefix}[${i}]` : `[${i}]`, maxDepth - 1))
    })
    return result
  }
  
  // Regular object: recurse into each key
  const result: Record<string, any> = {}
  for (const [key, value] of Object.entries(obj)) {
    const newPrefix = prefix ? `${prefix}.${key}` : key
    Object.assign(result, flattenLeaves(value, newPrefix, maxDepth - 1))
  }
  return result
}

export function applyAliases(
  flattened: Record<string, any>,
  aliases: Array<{ name: string; apiFieldName?: string; jsonPath?: string }>
): Record<string, any> {
  const result = { ...flattened }
  
  for (const alias of aliases) {
    const sourceKey = alias.jsonPath     // explicit path takes priority
      ?? alias.apiFieldName              // then api field name
      ?? alias.name                      // then canonical name (no alias needed)
    
    if (sourceKey !== alias.name && sourceKey in flattened) {
      result[alias.name] = flattened[sourceKey]  // add canonical name
      delete result[sourceKey]                    // remove original key
    }
  }
  
  return result
}

export function extractArrayRoot(
  response: any,
  responseRoot?: string
): any[] {
  if (!responseRoot) {
    return Array.isArray(response) ? response : [response]
  }
  // Support dot-path: 'data.items'
  const value = responseRoot.split('.').reduce((obj, key) => obj?.[key], response)
  return Array.isArray(value) ? value : value ? [value] : []
}
