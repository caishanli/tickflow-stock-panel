import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as api from '../api'

export function DingtalkConfigDialog({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const { data: cfg } = useQuery({
    queryKey: ['quant', 'dingtalk', 'config'],
    queryFn: api.getDingtalkConfig,
  })
  const [webhookUrl, setWebhookUrl] = useState(cfg?.webhook_url ?? '')
  const [secret, setSecret] = useState(cfg?.secret ?? '')
  const [testResult, setTestResult] = useState<string>('')

  useEffect(() => {
    if (cfg) {
      setWebhookUrl(cfg.webhook_url ?? '')
      setSecret(cfg.secret ?? '')
    }
  }, [cfg])

  const saveMut = useMutation({
    mutationFn: () => api.saveDingtalkConfig(webhookUrl, secret),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['quant', 'dingtalk'] })
      onClose()
    },
  })
  const testMut = useMutation({
    mutationFn: () => api.testDingtalk(webhookUrl, secret),
    onSuccess: (r: any) => setTestResult(r?.success ? '发送成功' : `失败: ${r?.message ?? ''}`),
    onError: (e: any) => setTestResult(`请求失败: ${e?.message ?? ''}`),
  })

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onClick={onClose}>
      <div className="w-96 rounded-card border border-border bg-surface p-5 space-y-4" onClick={e => e.stopPropagation()}>
        <h2 className="text-sm font-medium text-foreground">钉钉推送配置</h2>
        <div className="space-y-3">
          <div>
            <label className="text-xs text-muted">Webhook URL</label>
            <input value={webhookUrl} onChange={e => setWebhookUrl(e.target.value)}
              placeholder="https://oapi.dingtalk.com/robot/send?access_token=..."
              className="w-full mt-1 px-3 h-9 rounded-lg bg-elevated border border-border text-xs text-foreground" />
          </div>
          <div>
            <label className="text-xs text-muted">加签密钥（可选）</label>
            <input value={secret} onChange={e => setSecret(e.target.value)}
              placeholder="SEC..."
              className="w-full mt-1 px-3 h-9 rounded-lg bg-elevated border border-border text-xs text-foreground" />
          </div>
        </div>
        {testResult && <div className="text-xs text-muted">{testResult}</div>}
        <div className="flex justify-end gap-2">
          <button onClick={() => testMut.mutate()} disabled={testMut.isPending}
            className="px-3 h-9 rounded-lg bg-elevated text-foreground text-xs">测试发送</button>
          <button onClick={() => saveMut.mutate()} disabled={saveMut.isPending}
            className="px-3 h-9 rounded-lg bg-accent text-white text-xs">保存</button>
        </div>
      </div>
    </div>
  )
}
