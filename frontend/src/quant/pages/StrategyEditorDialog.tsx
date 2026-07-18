import { useState } from 'react'
import { Modal } from '@/components/Modal'
import { CodeEditor } from '../components/CodeEditor'

export function StrategyEditorDialog({ open, initial, onClose, onSave }: {
  open: boolean; initial?: { id: string | null; name: string; code: string }
  onClose: () => void; onSave: (name: string, code: string) => void
}) {
  if (!open) return null
  const [name, setName] = useState(initial?.name ?? '')
  const [code, setCode] = useState(initial?.code ?? '')

  return (
    <Modal onClose={onClose} panelClassName="w-[92vw] max-w-3xl bg-surface border border-border rounded-card">
      <div className="p-5 space-y-4">
        <input value={name} onChange={(e) => setName(e.target.value)}
          placeholder="策略名称" className="w-full h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
        <CodeEditor value={code} onChange={setCode} />
        <div className="flex justify-end gap-2">
          <button onClick={onClose} className="px-3 h-9 rounded-lg bg-elevated text-foreground text-xs">取消</button>
          <button onClick={() => onSave(name, code)} className="px-3 h-9 rounded-lg bg-accent text-white text-xs">保存</button>
        </div>
      </div>
    </Modal>
  )
}
