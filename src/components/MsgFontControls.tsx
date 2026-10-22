import { useMsgFontScale } from '../hooks/useMsgFontScale'
import type { useLocale } from '../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

export default function MsgFontControls({ tr }: { tr: Tr }) {
  const { msgFontPx, setMsgFontPx, min, max } = useMsgFontScale()

  return (
    <div className="msg-font-controls" role="group" aria-label={tr('msgFontSize')}>
      <span className="msg-font-label" aria-hidden="true">
        A
      </span>
      <input
        type="range"
        className="msg-font-range"
        min={min}
        max={max}
        step={1}
        value={msgFontPx}
        onChange={(e) => setMsgFontPx(Number(e.target.value))}
        aria-label={tr('msgFontSize')}
        title={`${msgFontPx}px`}
      />
      <span className="msg-font-readout" aria-hidden="true">
        {msgFontPx}px
      </span>
    </div>
  )
}
