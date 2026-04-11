import duckdb from 'duckdb'
import type { QueryPlan } from '../../query-ast/query-ast.js'
import type { Row } from '../../../core/types/row.js'

export class SubstraitTestHarness {
  private db: duckdb.Database
  private conn: duckdb.Connection

  constructor() {
    this.db = new duckdb.Database(':memory:')
    this.conn = this.db.connect()
    
    // Install and load Substrait extension
    this.conn.exec('INSTALL substrait')
    this.conn.exec('LOAD substrait')
    
    // Check what Substrait functions are available
    try {
      const result = this.conn.all("SELECT function_name FROM duckdb_functions() WHERE function_name LIKE '%substrait%'")
      console.log('Available Substrait functions:', result)
      
      // Also check for table functions
      const tableFunctions = this.conn.all("SELECT function_name FROM duckdb_table_functions() WHERE function_name LIKE '%substrait%'")
      console.log('Available Substrait table functions:', tableFunctions)
      
      // Check if there's a substrait function without substrait in name
      const allFunctions = this.conn.all("SELECT function_name FROM duckdb_functions() WHERE function_name LIKE '%scan%'")
      console.log('Available scan functions:', allFunctions)
    } catch (e) {
      console.log('Error checking Substrait functions:', e)
    }
  }

  async loadData(tables: Record<string, Row[]>): Promise<void> {
    for (const [tableName, rows] of Object.entries(tables)) {
      if (rows.length === 0) continue

      // Infer schema from first row
      const sampleRow = rows[0]
      const columns = Object.keys(sampleRow)
      const columnDefs = columns.map(col => {
        const value = sampleRow[col]
        let duckdbType: string
        if (value === null) {
          duckdbType = 'VARCHAR'
        } else if (typeof value === 'string') {
          duckdbType = 'VARCHAR'
        } else if (typeof value === 'number') {
          duckdbType = Number.isInteger(value) ? 'INTEGER' : 'DOUBLE'
        } else if (typeof value === 'boolean') {
          duckdbType = 'BOOLEAN'
        } else {
          duckdbType = 'VARCHAR' // fallback
        }
        return `${col} ${duckdbType}`
      }).join(', ')

      // Create table
      await this.execSQL(`CREATE TABLE ${tableName} (${columnDefs})`)

      // Insert data using VALUES clause
      if (rows.length > 0) {
        const valuesList = rows.map(row => {
          const values = columns.map(col => {
            const val = row[col]
            if (val === null) return 'NULL'
            if (typeof val === 'string') return `'${val.replace(/'/g, "''")}'`
            if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE'
            return String(val)
          }).join(', ')
          return `(${values})`
        }).join(', ')

        await this.execSQL(`INSERT INTO ${tableName} VALUES ${valuesList}`)
      }
    }
  }

  async executeSubstrait(plan: Uint8Array): Promise<Row[]> {
    return new Promise((resolve, reject) => {
      // Convert Uint8Array to Buffer for DuckDB
      const buffer = Buffer.from(plan)
      
      // Try different approaches to execute Substrait plan
      const approaches = [
        (callback: (err: any, rows?: Row[]) => void) => 
          this.conn.all('SELECT * FROM substrait_scan($1)', [buffer], callback),
        (callback: (err: any, rows?: Row[]) => void) => 
          this.conn.all('SELECT * FROM substrait($1)', [buffer], callback),
        (callback: (err: any, rows?: Row[]) => void) => 
          this.conn.all('SELECT substrait($1)', [buffer], callback),
        (callback: (err: any, rows?: Row[]) => void) => 
          this.conn.all('SELECT * FROM substrait_scan(?)', [buffer], callback),
        (callback: (err: any, rows?: Row[]) => void) => 
          this.conn.all('SELECT substrait(?)', [buffer], callback)
      ]
      
      // Try each approach
      let attempt = 0
      const tryNext = () => {
        if (attempt >= approaches.length) {
          reject(new Error('All Substrait execution approaches failed'))
          return
        }
        
        try {
          approaches[attempt]((err: any, rows?: Row[]) => {
            if (err) {
              attempt++
              if (attempt < approaches.length) {
                tryNext()
              } else {
                reject(err)
              }
            } else {
              resolve(rows || [])
            }
          })
        } catch (err) {
          attempt++
          if (attempt < approaches.length) {
            tryNext()
          } else {
            reject(err)
          }
        }
      }
      
      tryNext()
    })
  }

  async executeSQL(sql: string, params?: any[]): Promise<Row[]> {
    return this.execSQL(sql, params)
  }

  private async execSQL(sql: string, params?: any[]): Promise<Row[]> {
    return new Promise((resolve, reject) => {
      if (params && params.length > 0) {
        this.conn.all(sql, params, (err, rows) => {
          if (err) {
            reject(err)
          } else {
            resolve(rows as Row[])
          }
        })
      } else {
        this.conn.all(sql, (err, rows) => {
          if (err) {
            reject(err)
          } else {
            resolve(rows as Row[])
          }
        })
      }
    })
  }

  async close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.db.close((err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }
}

// Helper function to load CRM test data
export async function loadCRMTestData(harness: SubstraitTestHarness): Promise<void> {
  // Generate the same test data as in query-pipeline.test.ts
  const segments = ['enterprise', 'smb']
  const statuses = ['pending', 'completed', 'cancelled']
  const categories = ['electronics', 'clothing', 'food']

  // Generate 20 customers
  const customers = Array.from({ length: 20 }, (_, i) => ({
    id: i + 1,
    name: `Customer${i + 1}`,
    email: `customer${i + 1}@example.com`,
    segment: segments[i % 2],
    created_at: new Date(2023, 0, 1 + i).toISOString()
  }))

  // Generate 100 orders
  const orders = Array.from({ length: 100 }, (_, i) => ({
    id: i + 1,
    customer_id: (i % 20) + 1,
    status: statuses[i % 3],
    total: (i + 1) * 50,
    created_at: new Date(2023, 1, 1 + (i % 28)).toISOString()
  }))

  // Generate 300 order_items
  const orderItems = Array.from({ length: 300 }, (_, i) => ({
    id: i + 1,
    order_id: (i % 100) + 1,
    product_id: (i % 10) + 1,
    quantity: (i % 5) + 1,
    unit_price: ((i % 10) + 1) * 10
  }))

  // Generate 10 products
  const products = Array.from({ length: 10 }, (_, i) => ({
    id: i + 1,
    name: `Product${i + 1}`,
    category: categories[i % 3],
    price: (i + 1) * 10
  }))

  // Load all data into the harness
  await harness.loadData({
    customers,
    orders,
    order_items: orderItems,
    products
  })
}
