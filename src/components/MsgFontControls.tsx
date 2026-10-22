import { useMsgFontScale } from '../hooks/useMsgFontScale'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

export default function MsgFontControls({ tr }: { tr: Tr }) {
  const { smaller, larger, canSmaller, canLarger } = useMsgFontScale()

  return (
    <div className="msg-font-controls" role="group" aria-label={tr('msgFontSize')}>
      <button
        type="button"
        className="msg-font-btn"
        disabled={!canSmaller}
        onClick={smaller}
        title={tr('msgFontSmaller')}
        aria-label={tr('msgFontSmaller')}
      >
        A−
      </button>
      <button
        type="button"
        className="msg-font-btn"
        disabled={!canLarger}
        onClick={larger}
        title={tr('msgFontLarger')}
        aria-label={tr('msgFontLarger')}
      >
        A+
      </button>
    </div>
  )
}
