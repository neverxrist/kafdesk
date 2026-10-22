import { useEffect, useState } from 'react'
import { applyMsgFontPx, loadMsgFontPx, MSG_FONT_MAX, MSG_FONT_MIN } from '../lib/msgFontScale'

export function useMsgFontScale() {
  const [px, setPx] = useState(loadMsgFontPx)

  useEffect(() => {
    applyMsgFontPx(px)
  }, [px])

  function smaller() {
    setPx((prev) => applyMsgFontPx(prev - 1))
  }

  function larger() {
    setPx((prev) => applyMsgFontPx(prev + 1))
  }

  return {
    msgFontPx: px,
    smaller,
    larger,
    canSmaller: px > MSG_FONT_MIN,
    canLarger: px < MSG_FONT_MAX,
  }
}
