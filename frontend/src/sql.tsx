"use client"

import { useEffect, useState } from "react"
import { Database, Play, Table2, CheckCircle, AlertCircle } from "lucide-react"
import { Button } from "./components/button"
import { Card } from "./components/card"
import { ScrollArea } from "./components/scroll"
import { loadTables, execQuery } from "./data/data"

const SQLQueryInterface = () => {
  const [query, setQuery] = useState("")
  const [results, setResults] = useState<any[]>([])
  const [isExecuting, setIsExecuting] = useState(false)
  const [tables, set_tables] = useState([])

  const [success, setsuccess] = useState<boolean>(false)
  const [error, seterror] = useState<boolean>(false)
  const [message, setmessage] = useState<string>("")

  const executeQuery = async () => {
    setIsExecuting(true)
    seterror(false)
    setsuccess(false)
    const response = await execQuery({ content: query })
    const tables = await loadTables()
    if (response.success && tables.success) {
      if (response.data.ok) {
        setResults(response.data.results[0]["data"])
        setmessage(`Executed: ${response.data.results[0]["meta"]["time_ms"]} ms`)
        setsuccess(true)
        set_tables(tables.data)
      } else {
        setmessage(`Executed: ${response.data["stats"]["time_ms"]}ms\n
            Error: ${response.data.results[0]["error"]["message"]}`)
        seterror(true)
      }
    } else {
      seterror(true)
      setmessage("Ocurrió un error, inténtelo de nuevo")
    }
    setIsExecuting(false)
  }

  const fetchTables = async () => {
    const response = await loadTables()
    if (response.success) {
      set_tables(response.data)
    } else {
      seterror(true)
      setmessage("Ocurrió un error, inténtelo de nuevo")
    }
  }

  useEffect(() => {
    fetchTables()
  }, [])

  return (
    <div className="flex h-screen bg-background">
      <aside className="w-64 border-r border-border bg-sidebar flex flex-col">
        <div className="p-4 border-b border-sidebar-border">
          <div className="flex items-center gap-2 text-sidebar-foreground">
            <Database className="h-5 w-5" />
            <h2 className="font-semibold text-sm">Tables</h2>
          </div>
        </div>

        <ScrollArea className="flex-1">
          <div className="p-2 space-y-1">
            {tables.map((table, index) => (
              <div key={index} className={`flex items-center gap-2 px-3 py-2 rounded-md text-sm transition-colors`}>
                <Table2 className="h-4 w-4 shrink-0" />
                <span className="font-mono">{table}</span>
              </div>
            ))}
          </div>
        </ScrollArea>
      </aside>

      <main className="flex-1 flex flex-col overflow-hidden">
        <div className="border-b border-border bg-card">
          <div className="p-4">
            <div className="flex items-start gap-3">
              <div className="flex-1">
                <textarea
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Enter your SQL statement..."
                  className="w-full min-h-[120px] bg-background border border-input rounded-lg px-4 py-3 font-mono text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring resize-none"
                />
                {(success || error) && (
                  <div
                    className={`mt-3 flex items-start gap-3 rounded-lg border px-4 py-3 animate-in fade-in slide-in-from-top-2 duration-300 ${
                      success
                        ? "bg-emerald-500/10 border-emerald-500/20 text-emerald-400"
                        : "bg-red-500/10 border-red-500/20 text-red-400"
                    }`}
                  >
                    {success ? (
                      <CheckCircle className="h-5 w-5 shrink-0 mt-0.5" />
                    ) : (
                      <AlertCircle className="h-5 w-5 shrink-0 mt-0.5" />
                    )}
                    <div className="flex-1 space-y-1">
                      <p className="text-sm font-medium leading-relaxed whitespace-pre-line">{message}</p>
                    </div>
                  </div>
                )}
              </div>
              <Button onClick={executeQuery} disabled={isExecuting} className="shrink-0" size="lg">
                <Play className="h-4 w-4 mr-2" />
                {isExecuting ? "Executing..." : "Execute"}
              </Button>
            </div>
          </div>
        </div>

        <div className="flex-1 overflow-hidden p-4">
          {results.length > 0 ? (
            <Card className="h-full overflow-hidden flex flex-col">
              <div className="border-b border-border px-4 py-3">
                <h3 className="text-sm font-semibold text-foreground">Results ({results.length} rows)</h3>
              </div>

              <ScrollArea className="flex-1">
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead className="sticky top-0 bg-muted/50 backdrop-blur-sm">
                      <tr>
                        {Object.keys(results[0]).map((key) => (
                          <th
                            key={key}
                            className="text-left px-4 py-3 text-xs font-semibold text-muted-foreground uppercase tracking-wider border-b border-border"
                          >
                            {key}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {results.map((row, rowIndex) => (
                        <tr key={rowIndex} className="border-b border-border hover:bg-accent/50 transition-colors">
                          {Object.values(row).map((value, colIndex) => (
                            <td key={colIndex} className="px-4 py-3 text-sm text-foreground font-mono">
                              {String(value)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </ScrollArea>
            </Card>
          ) : (
            <div className="h-full flex items-center justify-center">
              <div className="text-center">
                <Database className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-semibold text-foreground mb-2">No data</h3>
                <p className="text-sm text-muted-foreground">Execute a query to see the results here</p>
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  )
}

export default SQLQueryInterface
