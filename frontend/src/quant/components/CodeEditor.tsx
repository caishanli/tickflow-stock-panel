import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { githubDark, githubLight } from '@uiw/codemirror-theme-github'
import { useTheme } from '@/lib/theme'

export function CodeEditor({ value, onChange, readOnly, height = '100%' }: {
  value: string
  onChange?: (v: string) => void
  readOnly?: boolean
  height?: string | number
}) {
  const theme = useTheme()
  const dark = theme === 'dark'
  return (
    <CodeMirror
      value={value}
      height={height as string}
      theme={dark ? githubDark : githubLight}
      extensions={[python()]}
      readOnly={readOnly}
      onChange={onChange}
      className="rounded-card border border-border overflow-hidden text-xs"
    />
  )
}
