'use client'

import { useState } from 'react'
import { useExecutionStore } from '@/store/execution'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'

export function PreviewTab() {
  const pipelineResult = useExecutionStore((state) => state.pipelineResult)
  const lastCreatedRow = useExecutionStore((state) => state.lastCreatedRow)
  const [showAllColumns, setShowAllColumns] = useState(false)

  // Show query results if available
  if (pipelineResult && pipelineResult.rows && pipelineResult.rows.length > 0) {
    const columns = pipelineResult.schema || []
    const getColumnName = (col: any) => col.name || col.column || String(col)

    // Filter out columns where ALL rows have null values
    const nonNullColumns = columns.filter(col => {
      const colName = getColumnName(col)
      return pipelineResult.rows.some(row => row[colName] !== null && row[colName] !== undefined)
    })

    // Priority columns to show first
    const PRIORITY_COLS = ['id', 'name', 'status', 'email', 'amount', 'created_at']

    // Sort: priority cols first, then others, max 12 columns visible initially
    const sortedColumns = showAllColumns
      ? nonNullColumns
      : [
          ...nonNullColumns.filter(c => PRIORITY_COLS.includes(getColumnName(c))),
          ...nonNullColumns.filter(c => !PRIORITY_COLS.includes(getColumnName(c)))
        ].slice(0, 12)

    const displayValue = (val: unknown) => {
      if (val === null || val === undefined) return <span className="text-muted-foreground">—</span>
      if (typeof val === 'boolean') return val ? 'true' : 'false'
      return String(val)
    }

    return (
      <div className="p-4 flex flex-col h-full">
        <h3 className="font-medium mb-4">Query Results ({pipelineResult.rows.length} rows)</h3>
        <Card className="flex-1 flex flex-col overflow-hidden">
          <CardContent className="p-0 flex-1 overflow-auto">
            <div className="min-w-full inline-block">
              <table className="w-full text-sm border-collapse">
                <thead className="sticky top-0 bg-white z-10">
                  <tr className="border-b bg-gray-50">
                    {sortedColumns.map((col, colIdx) => (
                      <th key={colIdx} className="text-left p-2 font-medium whitespace-nowrap border-r last:border-r-0">
                        {getColumnName(col)}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pipelineResult.rows.slice(0, 100).map((row, rowIdx) => (
                    <tr key={rowIdx} className="border-b hover:bg-gray-50">
                      {sortedColumns.map((col, colIdx) => {
                        const colName = getColumnName(col)
                        const value = row[colName]
                        return (
                          <td key={`${rowIdx}-${colIdx}`} className="p-2 whitespace-nowrap border-r last:border-r-0 max-w-xs truncate">
                            {displayValue(value)}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {pipelineResult.rows.length > 100 && (
              <div className="p-2 text-sm text-gray-500 text-center border-t">
                Showing first 100 of {pipelineResult.rows.length} rows
              </div>
            )}
            {nonNullColumns.length > 12 && (
              <div className="p-2 text-center border-t">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setShowAllColumns(!showAllColumns)}
                >
                  {showAllColumns ? 'Show fewer columns' : `Show all ${nonNullColumns.length} columns`}
                </Button>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    )
  }

  // Show created row for write operations
  if (lastCreatedRow) {
    const displayValue = (val: unknown) => {
      if (val === null || val === undefined) return <span className="text-muted-foreground">—</span>
      if (typeof val === 'boolean') return val ? 'true' : 'false'
      return String(val)
    }

    return (
      <div className="p-4">
        <h3 className="font-medium mb-4">Created Row</h3>
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Record</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {Object.entries(lastCreatedRow).map(([key, value]) => (
                <div key={key} className="flex justify-between text-sm">
                  <span className="font-medium">{key}:</span>
                  <span className="text-gray-600">
                    {displayValue(value)}
                  </span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="p-4 text-sm text-gray-500">
      No results yet
    </div>
  )
}
