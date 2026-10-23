/** Click-drag (LMB) to scroll overflow panes — scrollbars are hidden. */

/** Don't start a drag from these — keep caret / text selection / native controls. */
const SKIP =
  'input, textarea, select, a, [contenteditable="true"], .no-drag-scroll, .allow-select, .msg-body, pre, code, .schema-preview, .modal-message, .conn-drag-handle, [draggable="true"]'

function canScroll(el: HTMLElement): { x: boolean; y: boolean } {
  const s = getComputedStyle(el)
  const oy = s.overflowY
  const ox = s.overflowX
  const y =
    (oy === 'auto' || oy === 'scroll' || oy === 'overlay') && el.scrollHeight > el.clientHeight + 1
  const x =
    (ox === 'auto' || ox === 'scroll' || ox === 'overlay') && el.scrollWidth > el.clientWidth + 1
  return { x, y }
}

/** Nearest `.drag-scroll` that currently overflows. */
function nearestDragScrollPane(start: Element | null): HTMLElement | null {
  let el: Element | null = start
  while (el && el instanceof HTMLElement) {
    if (el === document.documentElement || el === document.body) break
    if (el.classList.contains('drag-scroll')) {
      const { x, y } = canScroll(el)
      if (x || y) return el
    }
    el = el.parentElement
  }
  return null
}

function pointerInsidePane(pane: HTMLElement, e: MouseEvent, pad = 8): boolean {
  const r = pane.getBoundingClientRect()
  return (
    e.clientX >= r.left - pad &&
    e.clientX <= r.right + pad &&
    e.clientY >= r.top - pad &&
    e.clientY <= r.bottom + pad
  )
}

export function installDragScroll(): () => void {
  let pane: HTMLElement | null = null
  let lastX = 0
  let lastY = 0
  let active = false
  let moved = false
  let suppressClick = false

  const endListeners = () => {
    window.removeEventListener('mousemove', onMove, true)
    window.removeEventListener('mouseup', onUp, true)
    window.removeEventListener('selectstart', onSelectStart, true)
    window.removeEventListener('dragstart', onDragStart, true)
  }

  const onUp = () => {
    active = false
    pane = null
    moved = false
    document.body.classList.remove('drag-scrolling')
    endListeners()
  }

  const onDragStart = (e: Event) => {
    if (active) e.preventDefault()
  }

  const onSelectStart = (e: Event) => {
    if (active) e.preventDefault()
  }

  const onMove = (e: MouseEvent) => {
    if (!pane || !active) return

    const dx = e.clientX - lastX
    const dy = e.clientY - lastY
    if (!moved && Math.abs(dx) + Math.abs(dy) < 3) return

    // Own the gesture until mouseup. Ending early lets macOS drag the app window.
    moved = true
    suppressClick = true
    document.body.classList.add('drag-scrolling')
    window.getSelection()?.removeAllRanges()
    e.preventDefault()

    if (pointerInsidePane(pane, e)) {
      const { x, y } = canScroll(pane)
      if (y) pane.scrollTop -= dy
      if (x) pane.scrollLeft -= dx
    }

    lastX = e.clientX
    lastY = e.clientY
  }

  const onDown = (e: MouseEvent) => {
    if (e.button !== 0) return
    if (!(e.target instanceof Element)) return
    if (e.target.closest(SKIP)) return
    if (e.target.closest('.topics-resize, .groups-resize, .schemas-resize, .conn-drag-handle')) return
    if (document.body.classList.contains('conn-reordering')) return
    const scroll = nearestDragScrollPane(e.target)
    if (!scroll) return
    pane = scroll
    lastX = e.clientX
    lastY = e.clientY
    active = true
    moved = false
    window.addEventListener('mousemove', onMove, true)
    window.addEventListener('mouseup', onUp, true)
    window.addEventListener('selectstart', onSelectStart, true)
    window.addEventListener('dragstart', onDragStart, true)
  }

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
