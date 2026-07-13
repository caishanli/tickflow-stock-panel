import { useState } from 'react'
import { Modal } from '@/components/Modal'

export function AccountDialog({ open, onClose, onSave }: {
  open: boolean
  onClose: () => void
  onSave: (b: { name: string; capital: number; stop_loss: number }) => void
}) {
  if (!open) return null
  const [name, setName] = useState('')
  const [capital, setCapital] = useState(100000)
  const [stop_loss, setStopLoss] = useState(0.1)

  return (
    <Modal onClose={onClose} panelClassName="w-[92vw] max-w-md bg-surface border border-border rounded-card">
      <div className="p-5 space-y-4">
        <h2 className="text-base font-medium text-foreground">新建模拟盘账户</h2>
        <div className="space-y-3">
          <div className="space-y-1">
            <label className="text-xs text-muted">账户名称</label>
            <input value={name} onChange={(e) => setName(e.target.value)}
              placeholder="如: 动量组合A"
              className="w-full h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted">初始资金</label>
            <input type="number" value={capital} onChange={(e) => setCapital(+e.target.value)}
              placeholder="初始资金"
              className="w-full h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted">止损比例 (0–1)</label>
            <input type="number" step="0.01" value={stop_loss} onChange={(e) => setStopLoss(+e.target.value)}
              placeholder="如 0.1 表示 10%"
              className="w-full h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
          </div>
        </div>
        <div className="flex justify-end gap-2">
          <button onClick={onClose} className="px-3 h-9 rounded-lg bg-elevated text-foreground text-xs">取消</button>
          <button onClick={() => name && onSave({ name, capital, stop_loss })}
            disabled={!name}
            className="px-3 h-9 rounded-lg bg-accent text-white text-xs disabled:opacity-50">创建</button>
        </div>
      </div>
    </Modal>
  )
}
