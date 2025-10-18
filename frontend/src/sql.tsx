import { useEffect, useState } from "react"
import { Database, Play, Table2, CheckCircle, AlertCircle, ChevronRight, ChevronDown, X } from "lucide-react"
import { Button } from "../src/components/button.tsx"
import { Card } from "../src/components/card.tsx"
import { ScrollArea } from "../src/components/scroll.tsx"
import { loadTables, execQuery } from "./data/data"

const SQLQueryInterface = () => {
  const [query, setQuery] = useState("")
  const [results, setResults] = useState<any[]>([])
  const [isExecuting, setIsExecuting] = useState(false)
  const [tables, set_tables] = useState<any>({})
  const [selectedTable, setSelectedTable] = useState<string>("")
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  const [success, setsuccess] = useState<boolean>(false)
  const [error, seterror] = useState<boolean>(false)
  const [message, setmessage] = useState<string>("")
  const [lastIO, setLastIO] = useState<any | null>(null)
  const [lastPlan, setLastPlan] = useState<any | null>(null)
  const [showIO, setShowIO] = useState(false)
  const [showPlan, setShowPlan] = useState(false)

  const executeQuery = async () => {
    setIsExecuting(true)
    seterror(false)
    setsuccess(false)
    const response = await execQuery({ content: query })
    const tables = await loadTables()
    if (response.success && tables.success) {
      if (response.data.ok) {
        const first = response.data.results?.[0] || {}
        setResults(first?.data || [])
        setLastIO(first?.meta?.io || null)
        setLastPlan(first?.plan || null)
        setmessage(`Executed: ${first?.meta?.time_ms ?? response.data?.stats?.time_ms ?? ""} ms`)
        setsuccess(true)
        set_tables(tables.data)
      } else {
        setmessage(`Executed: ${response.data["stats"]["time_ms"]} ms
            Error: ${response.data.results[0]["error"]["message"]}`)
        seterror(true)
        setLastIO(null)
        setLastPlan(null)
      }
    } else {
      seterror(true)
      setmessage("Ocurrió un error, inténtelo de nuevo")
      setLastIO(null)
      setLastPlan(null)
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

  useEffect(() => {
    if (tables && typeof tables === "object" && !selectedTable) {
      const names = Object.keys(tables)
      if (names.length > 0) setSelectedTable(names[0])
    }
  }, [tables, selectedTable])

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
            {Object.keys(tables || {}).map((tableName) => {
              const isActive = tableName === selectedTable
              const isOpen = !!expanded[tableName]
              const meta = (tables as any)[tableName] || {}
              const rel = meta.relation || {}
              const idxs = meta.indexes || {}
              const miniRows = Object.keys(rel).map((col) => {
                const spec = rel[col] || {}
                const type = spec.type + (spec.length ? `(${spec.length})` : "")
                let ix = idxs[col]
                if (!ix && spec.key === "primary") ix = idxs["primary"]
                const isPK = spec.key === "primary"
                const ixType = (ix?.index as string | undefined) || undefined
                const ixFile = (ix?.filename as string | undefined) || undefined
                return { name: col, type, isPK, ixType, ixFile }
              })
              return (
                <div key={tableName} className="rounded-md">
                  <div
                    className={`flex items-center justify-between px-2 py-1 rounded-md ${
                      isActive ? "bg-accent/40" : ""
                    }`}
                  >
                    <button
                      onClick={() => setExpanded((prev) => ({ ...prev, [tableName]: !isOpen }))}
                      className="p-1 rounded hover:bg-accent/40 text-sidebar-foreground"
                      aria-label={isOpen ? "Collapse" : "Expand"}
                    >
                      {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                    </button>
                    <button
                      onClick={() => setSelectedTable(tableName)}
                      className={`flex-1 flex items-center gap-2 px-2 py-1 rounded-md text-sm transition-colors hover:bg-accent/30 ${
                        isActive ? "text-foreground" : "text-sidebar-foreground"
                      }`}
                    >
                      <Table2 className="h-4 w-4 shrink-0" />
                      <span className="font-mono truncate">{tableName}</span>
                    </button>
                  </div>
                  {isOpen && miniRows.length > 0 && (
                    <div className="pl-7 pr-2 pb-2 space-y-1 mr-6">
                      {miniRows.map((r) => {
                        const badgeBase = "inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-medium"
                        const ixColor =
                          r.ixType === "hash"
                            ? "bg-blue-500/15 text-blue-400 border border-blue-500/30"
                            : r.ixType === "bplus"
                              ? "bg-purple-500/15 text-purple-400 border border-purple-500/30"
                              : r.ixType === "rtree"
                                ? "bg-emerald-500/15 text-emerald-400 border border-emerald-500/30"
                                : r.ixType === "heap"
                                  ? "bg-amber-500/15 text-amber-400 border border-amber-500/30"
                                  : r.ixType === "sequential"
                                    ? "bg-indigo-500/15 text-indigo-400 border border-indigo-500/30"
                                    : r.ixType === "isam"
                                      ? "bg-rose-500/15 text-rose-400 border border-rose-500/30"
                                      : "bg-muted text-muted-foreground border border-border"
                        return (
                          <div
                            key={r.name}
                            className="flex items-center justify-between gap-2 text-xs text-muted-foreground"
                          >
                            <span className="font-mono truncate">{r.name}</span>
                            <span className="truncate flex items-center gap-1">
                              <span className="truncate">{r.type}</span>
                              {r.isPK && (
                                <span
                                  className={`${badgeBase} bg-sky-500/15 text-sky-400 border border-sky-500/30`}
                                  title="Primary Key"
                                >
                                  PK
                                </span>
                              )}
                              {r.ixType && (
                                <span className={`${badgeBase} ${ixColor}`} title={r.ixFile || "Index file"}>
                                  {r.ixType}
                                </span>
                              )}
                            </span>
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>
              )
            })}
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
            {(lastIO || lastPlan) && (
              <div className="mt-3 flex items-center gap-2">
                {lastIO && (
                  <Button onClick={() => setShowIO(true)} variant="outline" size="sm">
                    IO
                  </Button>
                )}
                {lastPlan && (
                  <Button onClick={() => setShowPlan(true)} variant="outline" size="sm">
                    Plan
                  </Button>
                )}
              </div>
            )}
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

      {showIO && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center backdrop-blur-sm bg-black/20 p-4"
          onClick={() => setShowIO(false)}
        >
          <Card
            className="w-full max-w-2xl max-h-[80vh] flex flex-col shadow-2xl bg-white"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="border-b border-border px-6 py-4 flex items-center justify-between shrink-0">
              <h3 className="text-lg font-semibold text-card-foreground">IO Stats</h3>
              <Button
                onClick={() => setShowIO(false)}
                variant="ghost"
                size="sm"
                className="h-8 w-8 p-0 hover:bg-accent"
              >
                <X className="h-4 w-4" />
                <span className="sr-only">Close</span>
              </Button>
            </div>
            <ScrollArea className="flex-1">
              <div className="p-6">
                {lastIO ? (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border">
                          <th className="text-left px-3 py-3 text-xs font-semibold text-muted-foreground uppercase">
                            Tipo
                          </th>
                          <th className="text-left px-3 py-3 text-xs font-semibold text-muted-foreground uppercase">
                            Read
                          </th>
                          <th className="text-left px-3 py-3 text-xs font-semibold text-muted-foreground uppercase">
                            Write
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(lastIO).map(([k, v]: any) => {
                          const rc =
                            typeof v === "object" && v ? ((v as any).read_count ?? (v as any).readCount ?? 0) : 0
                          const wc =
                            typeof v === "object" && v ? ((v as any).write_count ?? (v as any).writeCount ?? 0) : 0
                          return (
                            <tr key={k as string} className="border-b border-border hover:bg-accent/50">
                              <td className="px-3 py-3 font-mono text-card-foreground">{k as string}</td>
                              <td className="px-3 py-3 text-card-foreground">{rc}</td>
                              <td className="px-3 py-3 text-card-foreground">{wc}</td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No IO data.</p>
                )}
              </div>
            </ScrollArea>
          </Card>
        </div>
      )}

      {showPlan && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center backdrop-blur-sm bg-black/20 p-4"
          onClick={() => setShowPlan(false)}
        >
          <Card
            className="w-full max-w-2xl max-h-[80vh] flex flex-col shadow-2xl bg-white"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="border-b border-border px-6 py-4 flex items-center justify-between shrink-0">
              <h3 className="text-lg font-semibold text-card-foreground">Execution Plan</h3>
              <Button
                onClick={() => setShowPlan(false)}
                variant="ghost"
                size="sm"
                className="h-8 w-8 p-0 hover:bg-accent"
              >
                <X className="h-4 w-4" />
                <span className="sr-only">Close</span>
              </Button>
            </div>
            <ScrollArea className="flex-1">
              <div className="p-6 space-y-4">
                {lastPlan ? (
                  <>
                    <div className="grid grid-cols-2 gap-3 text-sm">
                      {lastPlan?.action && (
                        <div className="space-y-1">
                          <span className="text-xs text-muted-foreground uppercase font-semibold">Action</span>
                          <div className="font-mono text-card-foreground">{String(lastPlan.action)}</div>
                        </div>
                      )}
                      {lastPlan?.table && (
                        <div className="space-y-1">
                          <span className="text-xs text-muted-foreground uppercase font-semibold">Table</span>
                          <div className="font-mono text-card-foreground">{String(lastPlan.table)}</div>
                        </div>
                      )}
                      {typeof lastPlan?.columns !== "undefined" && (
                        <div className="col-span-2 space-y-1">
                          <span className="text-xs text-muted-foreground uppercase font-semibold">Columns</span>
                          <div className="font-mono text-card-foreground">
                            {lastPlan.columns === null
                              ? "null"
                              : Array.isArray(lastPlan.columns)
                                ? lastPlan.columns.join(", ")
                                : String(lastPlan.columns)}
                          </div>
                        </div>
                      )}
                      {typeof lastPlan?.where !== "undefined" && (
                        <div className="col-span-2 space-y-1">
                          <span className="text-xs text-muted-foreground uppercase font-semibold">Where</span>
                          <div className="font-mono text-card-foreground">
                            {lastPlan.where === null
                              ? "null"
                              : typeof lastPlan.where === "object"
                                ? JSON.stringify(lastPlan.where)
                                : String(lastPlan.where)}
                          </div>
                        </div>
                      )}
                    </div>
                    <div className="space-y-2">
                      <div className="text-xs text-muted-foreground uppercase font-semibold">Raw Data</div>
                      <pre className="text-xs bg-muted/40 border border-border rounded-lg p-4 overflow-auto max-h-64 font-mono text-card-foreground">
                        {JSON.stringify(lastPlan, null, 2)}
                      </pre>
                    </div>
                  </>
                ) : (
                  <p className="text-sm text-muted-foreground">No plan available.</p>
                )}
              </div>
            </ScrollArea>
          </Card>
        </div>
      )}
    </div>
  )
}

export default SQLQueryInterface
