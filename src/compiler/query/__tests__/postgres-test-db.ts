import type { Row } from '../../../core/types/row.js'

export class PostgresTestDB {
  private data: Record<string, Row[]> = {}

  constructor() {
    // In-memory database for testing
  }

  async loadData(tables: Record<string, Row[]>): Promise<void> {
    this.data = { ...tables }
  }

  async query(sql: string, params?: any[]): Promise<Row[]> {
    // Simple SQL executor for testing
    // This is a mock implementation - in a real scenario you'd use actual Postgres
    
    // Parse basic SQL for demonstration
    const normalizedSQL = sql.trim().toLowerCase()
    
    if (normalizedSQL.startsWith('select')) {
      return this.executeSelect(sql, params)
    } else if (normalizedSQL.startsWith('insert')) {
      return this.executeInsert(sql, params)
    } else if (normalizedSQL.startsWith('create')) {
      return this.executeCreate(sql, params)
    }
    
    throw new Error(`Unsupported SQL: ${sql}`)
  }

  private executeSelect(sql: string, params?: any[]): Row[] {
    // Very basic SQL parser - just for demonstration
    // In a real implementation, you'd use a proper SQL parser
    
    const normalizedSQL = sql.toLowerCase()
    
    // Handle simple SELECT * FROM table queries
    const tableMatch = normalizedSQL.match(/select \* from (\w+)/)
    if (tableMatch) {
      const tableName = tableMatch[1]
      const tableData = this.data[tableName] || []
      
      // Apply WHERE clause if present
      const whereMatch = normalizedSQL.match(/where (.+?)(?:\s+(group by|order by|limit|offset|$))/)
      if (whereMatch) {
        const whereClause = whereMatch[1]
        return tableData.filter(row => this.evaluateWhere(row, whereClause))
      }
      
      return tableData
    }
    
    // Handle more complex queries with subqueries
    if (normalizedSQL.includes('from (')) {
      // For now, return empty results for complex queries
      // In a real implementation, you'd parse and execute subqueries
      return []
    }
    
    return []
  }

  private executeInsert(sql: string, params?: any[]): Row[] {
    // Mock implementation
    return []
  }

  private executeCreate(sql: string, params?: any[]): Row[] {
    // Mock implementation
    return []
  }

  private evaluateWhere(row: Row, whereClause: string): boolean {
    // Very basic WHERE clause evaluation
    // In a real implementation, you'd use a proper expression evaluator
    
    // Handle simple equality: col = 'value'
    const equalityMatch = whereClause.match(/(\w+)\s*=\s*'([^']*)'/)
    if (equalityMatch) {
      const col = equalityMatch[1]
      const value = equalityMatch[2]
      return String(row[col]) === value
    }
    
    // Handle numeric equality: col = 123
    const numEqualityMatch = whereClause.match(/(\w+)\s*=\s*(\d+)/)
    if (numEqualityMatch) {
      const col = numEqualityMatch[1]
      const value = parseInt(numEqualityMatch[2])
      return Number(row[col]) === value
    }
    
    return true
  }

  async close(): Promise<void> {
    // Mock cleanup
  }
}
