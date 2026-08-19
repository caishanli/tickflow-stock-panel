import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { HardDrive, Wrench } from 'lucide-react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { Skeleton } from '@/components/data/Skeleton'
import { toast } from '@/components/Toast'
import { api, type LocalMarketStatsRow } from '@/lib/api'
import { QK } from '@/lib/queryKeys'

const PAGE_SIZE = 15

type CountKey = Exclude<keyof LocalMarketStatsRow, 'date'>

const COLUMNS: { key: CountKey; label: string }[] = [
  { key: 'stock_daily', label: '股市日线' },
  { key: 'stock_minute', label: '股市分钟线' },
  { key: 'etf_daily', label: 'ETF日线' },
  { key: 'etf_minute', label: 'ETF分钟线' },
  { key: 'index_daily', label: '指数日线' },
  { key: 'index_minute', label: '指数分钟线' },
]

function fmtCount(n: number): string {
  return n.toLocaleString('zh-CN')
}

export function LocalData() {
  const [page, setPage] = useState(1)
  const qc = useQueryClient()
  const { data, isLoading, isError } = useQuery({
    queryKey: QK.localMarketStats(page, PAGE_SIZE),
    queryFn: () => api.localMarketStats(page, PAGE_SIZE),
  })

  const refreshStats = () => {
    qc.invalidateQueries({ queryKey: ['local-market-stats'] })
  }

  const checkDayMut = useMutation({
    mutationFn: (date: string) => api.checkDay(date),
    onSuccess: (_data, date) => {
      toast(`已触发 ${date} 检验补齐`, 'success', 'top')
      setTimeout(refreshStats, 3000)
    },
  })

  const checkFullMut = useMutation({
    mutationFn: () => api.checkFull(),
    onSuccess: () => {
      toast('已触发全量检验补齐', 'success', 'top')
      setTimeout(refreshStats, 3000)
    },
  })

  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const safePage = Math.min(page, totalPages)
  const rows = data?.rows ?? []

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="本地股市数据"
        subtitle={total > 0 ? `本地 Parquet 各日期去重标的数 · 共 ${total} 天` : '本地 Parquet 各日期去重标的数'}
      />
      <div className="flex-1 p-4 overflow-auto space-y-3">
        {!isLoading && !isError && total > 0 && (
          <div className="flex items-center justify-end">
            <button
              onClick={() => checkFullMut.mutate()}
              disabled={checkFullMut.isPending}
              className="px-3 py-1.5 rounded-btn border border-border bg-elevated text-secondary hover:text-foreground disabled:opacity-40 transition-colors flex items-center gap-1.5"
            >
              <Wrench className="h-3 w-3" />
              {checkFullMut.isPending ? '校验中...' : '全量检验补齐'}
            </button>
          </div>
        )}
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 8 }).map((_, i) => (
              <Skeleton key={i} className="h-9 w-full" />
            ))}
          </div>
        ) : isError ? (
          <EmptyState title="加载失败" hint="无法获取本地数据统计，请稍后重试或检查后端服务。" />
        ) : total === 0 ? (
          <EmptyState
            icon={HardDrive}
            title="暂无本地数据"
            hint="本地尚无任何行情数据，数据同步完成后会在此展示各日期的标的覆盖情况。"
          />
        ) : (
          <>
            <div className="rounded-card border border-border bg-surface overflow-hidden">
              <table className="w-full text-xs">
                <thead className="text-muted bg-elevated/40">
                  <tr className="text-left">
                    <th className="px-3 py-2 font-normal">日期</th>
                    {COLUMNS.map(c => (
                      <th key={c.key} className="px-3 py-2 font-normal text-right">{c.label}</th>
                    ))}
                    <th className="px-3 py-2 font-normal text-right">操作</th>
                  </tr>
                </thead>
                <tbody className="text-foreground">
                  {rows.map(row => (
                    <tr key={row.date} className="border-t border-border/60 hover:bg-elevated/60 transition-colors">
                      <td className="px-3 py-2 font-mono num">{row.date}</td>
                      {COLUMNS.map(c => (
                        <td key={c.key} className="px-3 py-2 text-right num text-muted">
                          {fmtCount(row[c.key])}
                        </td>
                      ))}
                      <td className="px-3 py-2 text-right">
                        <button
                          onClick={() => checkDayMut.mutate(row.date)}
                          disabled={checkDayMut.isPending}
                          className="px-2 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                        >
                          检验
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between mt-3 text-xs text-muted">
              <span>共 {total} 天 · 第 {safePage}/{totalPages} 页</span>
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={safePage <= 1}
                  className="px-2.5 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                >
                  上一页
                </button>
                <button
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={safePage >= totalPages}
                  className="px-2.5 py-1 rounded-btn border border-border text-secondary hover:text-foreground disabled:opacity-40 transition-colors"
                >
                  下一页
                </button>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
