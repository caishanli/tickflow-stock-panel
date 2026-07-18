import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Modal } from '@/components/Modal'
import { DatePicker } from '@/components/DatePicker'
import * as api from '../api'

function todayStr() {
  const d = new Date()
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
}

const INPUT_CLS = 'w-full h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40'

export interface AccountForm {
  name: string
  strategy_id: string
  capital: number
  stop_loss: number
  start_date: string
  frequency: string
}

export function AccountDialog({ open, onClose, onSave, saving }: {
  open: boolean
  onClose: () => void
  onSave: (b: AccountForm) => void
  saving?: boolean
}) {
  const { data: strategies } = useQuery({ queryKey: ['quant', 'strategies'], queryFn: api.listStrategies })
  const [name, setName] = useState('')
  const [strategyId, setStrategyId] = useState('')
  const [capital, setCapital] = useState(100000)
  const [stop_loss, setStopLoss] = useState(0.1)
  const [start_date, setStartDate] = useState(todayStr())
  const [frequency, setFrequency] = useState('minute')

  if (!open) return null
  const valid = !!name && !!strategyId

  return (
    <Modal onClose={onClose} panelClassName="w-[92vw] max-w-md bg-surface border border-border rounded-card">
      <div className="p-5 space-y-4">
        <h2 className="text-base font-medium text-foreground">新建模拟盘账户</h2>
        <div className="space-y-3">
          <div className="space-y-1">
            <label className="text-xs text-muted">交易名称</label>
            <input value={name} onChange={(e) => setName(e.target.value)}
              placeholder="如: 动量组合A" className={INPUT_CLS} />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted">使用策略（与量化回测同一策略库）</label>
            <select value={strategyId} onChange={(e) => setStrategyId(e.target.value)} className={INPUT_CLS}>
              <option value="" disabled>选择策略</option>
              {(strategies ?? []).map((s: any) => (
                <option key={s.id} value={s.id}>{s.name || s.id}</option>
              ))}
            </select>
            {(strategies ?? []).length === 0 && (
              <p className="text-[11px] text-warning">暂无策略，请先在量化回测页新建策略</p>
            )}
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted">初始资金</label>
            <input type="number" value={capital} onChange={(e) => setCapital(+e.target.value)}
              placeholder="初始资金" className={INPUT_CLS} />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted">运行频率</label>
            <select value={frequency} onChange={(e) => setFrequency(e.target.value)} className={INPUT_CLS}>
              <option value="minute">分钟级（交易时段逐分钟驱动策略）</option>
              <option value="daily">日频（每日开盘首次驱动，全量触发当日任务）</option>
            </select>
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted">开始模拟日期</label>
            <DatePicker value={start_date} onChange={setStartDate} className="w-full" />
            <p className="text-[11px] text-muted/70">
              默认今天（立即开始）；选过去日期将从该日起按历史行情补跑至今，再接入实时
            </p>
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted">止损比例 (0–1)</label>
            <input type="number" step="0.01" value={stop_loss} onChange={(e) => setStopLoss(+e.target.value)}
              placeholder="如 0.1 表示 10%" className={INPUT_CLS} />
          </div>
        </div>
        <div className="flex justify-end gap-2">
          <button onClick={onClose} className="px-3 h-9 rounded-lg bg-elevated text-foreground text-xs">取消</button>
          <button
            onClick={() => valid && onSave({ name, strategy_id: strategyId, capital, stop_loss, start_date, frequency })}
            disabled={!valid || saving}
            className="px-3 h-9 rounded-lg bg-accent text-white text-xs disabled:opacity-50">创建</button>
        </div>
      </div>
    </Modal>
  )
}
