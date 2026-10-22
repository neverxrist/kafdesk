import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type ReactNode,
} from 'react'

type VirtualListProps<T> = {
  items: T[]
  /** Fixed row estimate (px), including gap. */
  estimateSize: number
  /** Space after each row (px) — applied as padding so it is included in measure. */
  gap?: number
  overscan?: number
  getKey: (item: T, index: number) => string
  children: (item: T, index: number) => ReactNode
  className?: string
}

/**
 * Windowed list — only ~viewport rows in the DOM.
 * Own scroll container (flex:1). No extra deps.
 */
export default function VirtualList<T>({
  items,
  estimateSize,
  gap = 0,
  overscan = 8,
  getKey,
  children,
  className,
}: VirtualListProps<T>) {
  const parentRef = useRef<HTMLDivElement | null>(null)
  const [scrollTop, setScrollTop] = useState(0)
  const [viewportH, setViewportH] = useState(400)
  const measured = useRef<Map<number, number>>(new Map())
  const [measureEpoch, setMeasureEpoch] = useState(0)

  useEffect(() => {
    measured.current.clear()
    setMeasureEpoch((n) => n + 1)
  }, [items.length, estimateSize, gap])

  useEffect(() => {
    const el = parentRef.current
    if (!el) return
    const onScroll = () => setScrollTop(el.scrollTop)
    const ro = new ResizeObserver(() => {
      setViewportH(el.clientHeight)
    })
    setViewportH(el.clientHeight)
    el.addEventListener('scroll', onScroll, { passive: true })
    ro.observe(el)
    return () => {
      el.removeEventListener('scroll', onScroll)
      ro.disconnect()
    }
  }, [])

  const offsets = useMemo(() => {
    const off: number[] = new Array(items.length + 1)
    off[0] = 0
    for (let i = 0; i < items.length; i++) {
      off[i + 1] = off[i]! + (measured.current.get(i) ?? estimateSize)
    }
    return off
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items.length, estimateSize, measureEpoch])

  const totalH = offsets[items.length] ?? 0

  let start = 0
  while (start < items.length && (offsets[start + 1] ?? 0) < scrollTop) start++
  let end = start
  const limit = scrollTop + viewportH
  while (end < items.length && (offsets[end] ?? 0) < limit) end++
  start = Math.max(0, start - overscan)
  end = Math.min(items.length, end + overscan)

  const measureRef = useCallback(
    (index: number, node: HTMLElement | null) => {
      if (!node) return
      // offsetHeight includes padding (gap) and borders; avoids margin-collapse misses.
      const h = node.offsetHeight
      if (h <= 0) return
      const prev = measured.current.get(index)
      if (prev == null || Math.abs(prev - h) > 1) {
        measured.current.set(index, h)
        setMeasureEpoch((n) => n + 1)
      }
    },
    [],
  )

  return (
    <div
      ref={parentRef}
      className={`virtual-list drag-scroll${className ? ` ${className}` : ''}`}
    >
      <div className="virtual-list-spacer" style={{ height: totalH }}>
        {items.slice(start, end).map((item, i) => {
          const index = start + i
          const top = offsets[index] ?? index * estimateSize
          const style: CSSProperties = {
            position: 'absolute',
            top,
            left: 0,
            right: 0,
            width: '100%',
            paddingBottom: gap > 0 ? gap : undefined,
            boxSizing: 'border-box',
          }
          return (
            <div
              key={getKey(item, index)}
              className="virtual-list-row"
              style={style}
              ref={(node) => measureRef(index, node)}
            >
              {children(item, index)}
            </div>
          )
        })}
      </div>
    </div>
  )
}
