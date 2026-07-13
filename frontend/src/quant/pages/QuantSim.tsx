import { useState } from 'react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import * as api from '../api'
import { AccountDialog } from './AccountDialog'

export function QuantSim() {
  const qc = useQueryClient()
  const { data: accounts } = useQuery({ queryKey: ['quant', 'sim', 'accounts'], queryFn: api.listAccounts })
  const [sel, setSel] = useState<string | null>(null)
  const [dialog, setDialog] = useState(false)

  const startMut = useMutation({ mutationFn: () => api.startAccount(sel!), onSuccess: () => qc.invalidateQueries({ queryKey: ['quant', 'sim', 'accounts'] }) })
  const pauseMut = useMutation({ mutationFn: () => api.pauseAccount(sel!), onSuccess: () => qc.invalidateQueries({ queryKey: ['quant', 'sim', 'accounts'] }) })
  const resetMut = useMutation({ mutationFn: () => api.resetAccount(sel!), onSuccess: () => qc.invalidateQueries({ queryKey: ['quant', 'sim', 'accounts'] }) })
  const createMut = useMutation({ mutationFn: (b: any) => api.createAccount(b), onSuccess: () => { setDialog(false); qc.invalidateQueries({ queryKey: ['quant', 'sim', 'accounts'] }) } })

  // 实时轮询（模拟盘 4s）
  const { data: st } = useQuery({ queryKey: ['quant', 'sim', sel, 'status'], queryFn: () => api.getSimStatus(sel!), enabled: !!sel, refetchInterval: 4000 })
  const { data: eq } = useQuery({ queryKey: ['quant', 'sim', sel, 'equity'], queryFn: () => api.getSimEquity(sel!), enabled: !!sel, refetchInterval: 4000 })
  const { data: tr } = useQuery({ queryKey: ['quant', 'sim', sel, 'trades'], queryFn: () => api.getSimTrades(sel!), enabled: !!sel, refetchInterval: 4000 })

  const positions = st?.state?.positions ?? {}
  const posEntries = Object.entries(positions)

  return (
    <div className="flex flex-col h-full">
      <PageHeader title="量化模拟盘" subtitle="实时盘 / 离线回放" right={
        <button onClick={() => setDialog(true)} className="px-3 h-9 rounded-lg bg-accent text-white text-xs">新建账户</button>
      } />
      <div className="flex-1 grid grid-cols-[320px_1fr] overflow-hidden">
        <aside className="border-r border-border p-3 space-y-2 overflow-auto">
          {(accounts ?? []).length === 0 && (
            <div className="text-xs text-muted px-1 py-2">暂无账户，点击右上角新建</div>
          )}
          {(accounts ?? []).map((a: any) => (
            <button key={a.id} onClick={() => setSel(a.id)}
              className={`w-full flex items-center justify-between rounded-card border px-3 h-10 text-xs ${sel === a.id ? 'border-accent' : 'border-border bg-surface'}`}>
              <span className="text-foreground truncate">{a.name}</span>
              <span className="text-muted">{a.status}</span>
            </button>
          ))}
        </aside>
        <section className="p-4 space-y-4 overflow-auto">
          {!sel ? <EmptyState title="选择一个账户" hint="或新建模拟盘账户" /> : (
            <>
              <div className="flex gap-2">
                <button onClick={() => startMut.mutate()} disabled={startMut.isPending} className="px-3 h-9 rounded-lg bg-accent text-white text-xs disabled:opacity-50">启动</button>
                <button onClick={() => pauseMut.mutate()} disabled={pauseMut.isPending} className="px-3 h-9 rounded-lg bg-elevated text-foreground text-xs disabled:opacity-50">暂停</button>
                <button onClick={() => resetMut.mutate()} disabled={resetMut.isPending} className="px-3 h-9 rounded-lg bg-elevated text-foreground text-xs disabled:opacity-50">重置</button>
              </div>

              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                <div className="rounded-card border border-border bg-surface px-3 py-2">
                  <div className="text-[10px] text-muted">净值</div>
                  <div className="text-sm font-medium text-foreground">{st?.state?.net_value ?? '—'}</div>
                </div>
                <div className="rounded-card border border-border bg-surface px-3 py-2">
                  <div className="text-[10px] text-muted">现金</div>
                  <div className="text-sm font-medium text-foreground">{st?.state?.cash ?? '—'}</div>
                </div>
                <div className="rounded-card border border-border bg-surface px-3 py-2">
                  <div className="text-[10px] text-muted">盈亏</div>
                  <div className={`text-sm font-medium ${typeof st?.state?.pnl === 'number' && st.state.pnl < 0 ? 'text-bear' : 'text-bull'}`}>{st?.state?.pnl ?? '—'}</div>
                </div>
                <div className="rounded-card border border-border bg-surface px-3 py-2">
                  <div className="text-[10px] text-muted">持仓数</div>
                  <div className="text-sm font-medium text-foreground">{posEntries.length}</div>
                </div>
              </div>

              <div className="rounded-card border border-border bg-surface overflow-hidden">
                <div className="px-4 pt-3 pb-2 text-xs text-foreground font-medium">持仓</div>
                {posEntries.length > 0 ? (
                  <div className="overflow-auto max-h-60">
                    <table className="w-full text-xs">
                      <thead className="text-muted sticky top-0 bg-surface">
                        <tr className="text-left">
                          <th className="px-3 py-1.5 font-normal">标的</th>
                          <th className="px-3 py-1.5 font-normal text-right">数量</th>
                          <th className="px-3 py-1.5 font-normal text-right">成本</th>
                          <th className="px-3 py-1.5 font-normal text-right">市值</th>
                        </tr>
                      </thead>
                      <tbody className="text-foreground">
                        {posEntries.map(([sym, p]: any) => (
                          <tr key={sym} className="border-t border-border/60">
                            <td className="px-3 py-1.5">{sym}</td>
                            <td className="px-3 py-1.5 text-right">{p.quantity ?? p.qty ?? ''}</td>
                            <td className="px-3 py-1.5 text-right">{p.cost ?? ''}</td>
                            <td className="px-3 py-1.5 text-right">{p.value ?? ''}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="px-4 pb-4 text-xs text-muted">暂无持仓</div>
                )}
              </div>

              <div className="rounded-card border border-border bg-base p-3">
                <div className="text-xs text-foreground font-medium mb-2">止损日志 ({st?.stop_loss?.length ?? 0})</div>
                <div className="max-h-48 overflow-auto space-y-0.5 text-[11px] text-muted font-mono">
                  {(st?.stop_loss ?? []).length > 0 ? (st.stop_loss as any[]).map((l, i) => (
                    <div key={i}>{typeof l === 'string' ? l : JSON.stringify(l)}</div>
                  )) : <div className="text-muted">暂无触发</div>}
                </div>
              </div>

              <div className="rounded-card border border-border bg-surface overflow-hidden">
                <div className="px-4 pt-3 pb-2 text-xs text-foreground font-medium">成交记录</div>
                {Array.isArray(tr) && tr.length > 0 ? (
                  <div className="overflow-auto max-h-60">
                    <table className="w-full text-xs">
                      <thead className="text-muted sticky top-0 bg-surface">
                        <tr className="text-left">
                          <th className="px-3 py-1.5 font-normal">时间</th>
                          <th className="px-3 py-1.5 font-normal">标的</th>
                          <th className="px-3 py-1.5 font-normal">方向</th>
                          <th className="px-3 py-1.5 font-normal text-right">价格</th>
                          <th className="px-3 py-1.5 font-normal text-right">数量</th>
                        </tr>
                      </thead>
                      <tbody className="text-foreground">
                        {tr.map((t: any, i: number) => (
                          <tr key={i} className="border-t border-border/60">
                            <td className="px-3 py-1.5 text-muted">{String(t.datetime ?? t.time ?? t.date ?? '')}</td>
                            <td className="px-3 py-1.5">{t.symbol ?? t.code ?? ''}</td>
                            <td className={`px-3 py-1.5 ${t.side === 'buy' || t.side === 'BUY' ? 'text-bull' : 'text-bear'}`}>{t.side ?? ''}</td>
                            <td className="px-3 py-1.5 text-right">{t.price ?? ''}</td>
                            <td className="px-3 py-1.5 text-right">{t.quantity ?? t.qty ?? ''}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="px-4 pb-4 text-xs text-muted">暂无成交</div>
                )}
              </div>

              {Array.isArray(eq) && eq.length > 0 && (
                <div className="rounded-card border border-border bg-surface">
                  <div className="px-4 pt-3 text-xs text-foreground font-medium">净值曲线</div>
                  <div className="h-[300px] grid place-items-center text-xs text-muted">
                    数据点 {eq.length}（详见离线回放图形）
                  </div>
                </div>
              )}
            </>
          )}
        </section>
      </div>
      <AccountDialog open={dialog} onClose={() => setDialog(false)} onSave={(b) => createMut.mutate(b)} />
    </div>
  )
}
