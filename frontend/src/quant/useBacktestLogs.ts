import { useCallback, useEffect, useRef, useState } from 'react'
import * as api from './api'

export interface LogRow {
  rowid?: number
  ts: string
  level: string
  message: string
}

const PAGE = 200

// 回测日志增量加载：先拉尾部（最新在底），向上滚动加载更早；运行期 SSE 新日志从底部追加。
export function useBacktestLogs(runId: string | null) {
  const [logs, setLogs] = useState<LogRow[]>([])
  const [minRowid, setMinRowid] = useState<number | null>(null)
  const [hasMore, setHasMore] = useState(false)
  const [total, setTotal] = useState<number | null>(null)
  const [loading, setLoading] = useState(false)
  const [loadingMore, setLoadingMore] = useState(false)
  const reqId = useRef(0)

  useEffect(() => {
    if (!runId) {
      setLogs([]); setMinRowid(null); setHasMore(false); setTotal(null)
      return
    }
    const my = ++reqId.current
    setLoading(true)
    api.getBacktestLogs(runId, { limit: PAGE })
      .then((res: any) => {
        if (my !== reqId.current) return
        setLogs(res.data ?? [])
        setMinRowid(res.min_rowid ?? null)
        setHasMore(!!res.has_more)
        setTotal(res.total ?? null)
      })
      .catch(() => { if (my === reqId.current) setLogs([]) })
      .finally(() => { if (my === reqId.current) setLoading(false) })
  }, [runId])

  const loadEarlier = useCallback(() => {
    if (!runId || loadingMore || !hasMore || minRowid == null) return
    const my = reqId.current
    setLoadingMore(true)
    api.getBacktestLogs(runId, { before: minRowid, limit: PAGE })
      .then((res: any) => {
        if (my !== reqId.current) return
        const rows: LogRow[] = res.data ?? []
        if (rows.length) {
          setLogs((prev) => [...rows, ...prev])
          setMinRowid(res.min_rowid ?? null)
        }
        setHasMore(!!res.has_more && rows.length > 0)
      })
      .catch(() => {})
      .finally(() => { if (my === reqId.current) setLoadingMore(false) })
  }, [runId, loadingMore, hasMore, minRowid])

  const appendLog = useCallback((l: LogRow) => {
    setLogs((prev) => {
      const maxRid = prev.reduce((m, r) => Math.max(m, r.rowid ?? 0), 0)
      if (l.rowid != null && l.rowid <= maxRid) return prev
      const sig = (r: LogRow) => `${r.ts}|${r.level}|${r.message}`
      if (prev.some((r) => sig(r) === sig(l))) return prev
      return [...prev, l]
    })
    if (total != null) setTotal(total + 1)
  }, [total])

  return { logs, hasMore, total, loading, loadingMore, loadEarlier, appendLog }
}
