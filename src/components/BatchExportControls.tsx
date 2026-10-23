import { useEffect, useRef, useState } from 'react'
import type { KafkaMessageView } from '../types/kafdesk'
import type { useLocale } from '../hooks/useLocale'
import {
  defaultExportFilename,
  saveBatchExport,
  serializeBatch,
  type BatchExportFormat,
} from '../lib/exportBatch'

type Tr = ReturnType<typeof useLocale>['tr']

export default function BatchExportControls({
  tr,
  messages,
  topic,
}: {
  tr: Tr
  messages: KafkaMessageView[]
  topic: string | null
}) {
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState(false)
  const rootRef = useRef<HTMLDivElement>(null)
  const empty = messages.length === 0

  useEffect(() => {
    if (empty) setOpen(false)
  }, [empty])

  useEffect(() => {
    if (!open) return
    function onDoc(e: MouseEvent) {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false)
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onDoc)
    window.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDoc)
      window.removeEventListener('keydown', onKey)
    }
  }, [open])

  async function run(format: BatchExportFormat) {
    if (empty || busy) return
    setOpen(false)
    setBusy(true)
    try {
      const contents = serializeBatch(messages, format)
      await saveBatchExport(contents, defaultExportFilename(topic, format), format)
    } catch (e) {
      console.error(e)
      window.alert(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className={`batch-export-menu${open ? ' open' : ''}`} ref={rootRef}>
      <button
        type="button"
        className="batch-export-btn"
        disabled={empty || busy}
        aria-expanded={open}
        aria-haspopup="menu"
        title={tr('exportBatchTitle')}
        onClick={() => setOpen((v) => !v)}
      >
        {tr('exportBatch')}
        <span aria-hidden>{open ? '▴' : '▾'}</span>
      </button>
      {open && (
        <div className="batch-export-popover" role="menu" aria-label={tr('exportBatch')}>
          <button
            type="button"
            className="batch-export-item"
            role="menuitem"
            disabled={busy}
            onClick={() => void run('json')}
          >
            {tr('exportBatchJson')}
          </button>
          <button
            type="button"
            className="batch-export-item"
            role="menuitem"
            disabled={busy}
            onClick={() => void run('csv')}
          >
            {tr('exportBatchCsv')}
          </button>
        </div>
      )}
    </div>
  )
}
