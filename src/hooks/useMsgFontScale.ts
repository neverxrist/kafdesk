import { useEffect, useState } from 'react'
import { applyMsgFontPx, loadMsgFontPx, MSG_FONT_MAX, MSG_FONT_MIN } from '../lib/msgFontScale'

export function useMsgFontScale() {
  const [px, setPx] = useState(loadMsgFontPx)

  useEffect(() => {
    applyMsgFontPx(px)
  }, [px])

  function setMsgFontPx(n: number) {
    setPx(applyMsgFontPx(n))
  }

  return {
    msgFontPx: px,
    setMsgFontPx,
    min: MSG_FONT_MIN,
    max: MSG_FONT_MAX,
  }
}
