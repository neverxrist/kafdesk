/** Click-drag (LMB) to scroll overflow panes — scrollbars are hidden. */

/** Don't start a drag from these — keep caret / text selection / native controls. */
const SKIP =
  'input, textarea, select, a, [contenteditable="true"], .no-drag-scroll, .allow-select, .msg-body, pre, code, .schema-preview, .modal-message'

function canScroll(el: HTMLElement) {
  const s = getComputedStyle(el)
  const oy = s.overflowY
  const ox = s.overflowX
  const y =
    (oy === 'auto' || oy === 'scroll' || oy === 'overlay') && el.scrollHeight > el.clientHeight + 1
  const x =
    (ox === 'auto' || ox === 'scroll' || ox === 'overlay') && el.scrollWidth > el.clientWidth + 1
  return { x, y }
}

function nearestScrollable(start: Element | null): HTMLElement | null {
  let el: Element | null = start
  while (el && el instanceof HTMLElement) {
    if (el === document.documentElement || el === document.body) break
    const { x, y } = canScroll(el)
    if (x || y) return el
    el = el.parentElement
  }
  return null
}

export function installDragScroll(): () => void {
  let pane: HTMLElement | null = null
  let lastX = 0
  let lastY = 0
  let active = false
  let moved = false
  let suppressClick = false

  const onMove = (e: MouseEvent) => {
    if (!pane || !active) return
    const dx = e.clientX - lastX
    const dy = e.clientY - lastY
    if (!moved && Math.abs(dx) + Math.abs(dy) < 3) return
    moved = true
    suppressClick = true
    document.body.classList.add('drag-scrolling')
    window.getSelection()?.removeAllRanges()
    e.preventDefault()
    const { x, y } = canScroll(pane)
    if (y) pane.scrollTop -= dy
    if (x) pane.scrollLeft -= dx
    lastX = e.clientX
    lastY = e.clientY
  }

  const onSelectStart = (e: Event) => {
    // Block native highlight while a list/pane drag may be in progress
    if (active) e.preventDefault()
  }

  const onUp = () => {
    active = false
    pane = null
    moved = false
    document.body.classList.remove('drag-scrolling')
    window.removeEventListener('mousemove', onMove, true)
    window.removeEventListener('mouseup', onUp, true)
    window.removeEventListener('selectstart', onSelectStart, true)
  }

  const onDown = (e: MouseEvent) => {
    if (e.button !== 0) return
    if (!(e.target instanceof Element)) return
    if (e.target.closest(SKIP)) return
    const scroll = nearestScrollable(e.target)
    if (!scroll) return
    pane = scroll
    lastX = e.clientX
    lastY = e.clientY
    active = true
    moved = false
    window.addEventListener('mousemove', onMove, true)
    window.addEventListener('mouseup', onUp, true)
    window.addEventListener('selectstart', onSelectStart, true)
  }

  // After a drag, kill the synthetic click on topic/conn buttons
  const onClickCapture = (e: MouseEvent) => {
    if (!suppressClick) return
    suppressClick = false
    e.preventDefault()
    e.stopPropagation()
  }

  document.addEventListener('mousedown', onDown, true)
  document.addEventListener('click', onClickCapture, true)
  return () => {
    document.removeEventListener('mousedown', onDown, true)
    document.removeEventListener('click', onClickCapture, true)
    onUp()
  }
}
