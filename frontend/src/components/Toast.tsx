import { useCallback, useEffect, useState } from 'react'

// ===== 全局 toast 状态 =====
type ToastItem = { id: number; msg: string; kind: 'error' | 'success'; position: 'top' | 'bottom' }
let _id = 0
const _listeners: Set<(items: ToastItem[]) => void> = new Set()
let _queue: ToastItem[] = []

function _emit() { _listeners.forEach(fn => fn([..._queue])) }

function toast(msg: string, kind: 'error' | 'success' = 'error', position: 'top' | 'bottom' = 'bottom') {
  const item = { id: ++_id, msg, kind, position }
  _queue = [..._queue, item]
  _emit()
  setTimeout(() => { _queue = _queue.filter(t => t.id !== item.id); _emit() }, 4000)
}

export { toast }

// ===== Toast 容器 — 挂在 Layout 最顶层 =====
export function ToastContainer() {
  const [items, setItems] = useState<ToastItem[]>([])

  const sub = useCallback(() => {
    _listeners.add(setItems)
    return () => { _listeners.delete(setItems) }
  }, [])

  useEffect(sub, [sub])

  if (!items.length) return null

  const top = items.filter(t => t.position === 'top')
  const bottom = items.filter(t => t.position === 'bottom')

  const renderGroup = (group: ToastItem[], pos: 'top' | 'bottom') => (
    <div
      key={pos}
      role="status"
      aria-live="polite"
      aria-atomic="false"
      className={`fixed left-1/2 -translate-x-1/2 z-[9999] flex flex-col items-center gap-2 pointer-events-none ${
        pos === 'top' ? 'top-4' : 'bottom-4'
      }`}
    >
      {group.map(t => (
        <div
          key={t.id}
          className={`pointer-events-auto px-4 py-2.5 rounded-lg shadow-lg text-sm font-medium animate-in fade-in duration-200 ${
            t.kind === 'error'
              ? 'bg-red-500/90 text-white'
              : 'bg-emerald-500/90 text-white'
          }`}
        >
          {t.msg}
        </div>
      ))}
    </div>
  )

  return (
    <>
      {top.length > 0 && renderGroup(top, 'top')}
      {bottom.length > 0 && renderGroup(bottom, 'bottom')}
    </>
  )
}
