type Kind = 'topics' | 'events'

/** Same viewBox / stroke weight so both badges match visually. */
function TopicsIcon() {
  return (
    <svg viewBox="0 0 28 28" fill="none" aria-hidden>
      <rect x="4" y="5" width="20" height="4.5" rx="1.5" stroke="currentColor" strokeWidth="1.6" />
      <rect x="4" y="12" width="20" height="4.5" rx="1.5" stroke="currentColor" strokeWidth="1.6" />
      <rect x="4" y="19" width="14" height="4.5" rx="1.5" stroke="currentColor" strokeWidth="1.6" />
    </svg>
  )
}

function EventsIcon() {
  return (
    <svg viewBox="0 0 28 28" fill="none" aria-hidden>
      <rect x="5" y="4.5" width="18" height="19" rx="2.5" stroke="currentColor" strokeWidth="1.6" />
      <path
        d="M9.5 10.5h9M9.5 14.5h9M9.5 18.5h6"
        stroke="currentColor"
        strokeWidth="1.6"
        strokeLinecap="round"
      />
    </svg>
  )
}

export default function EmptyState({
  kind,
  title,
  hint,
}: {
  kind: Kind
  title: string
  hint?: string
}) {
  return (
    <div className="empty-state" role="status">
      <div className="empty-state-icon">{kind === 'topics' ? <TopicsIcon /> : <EventsIcon />}</div>
      <div className="empty-state-title">{title}</div>
      {hint ? <div className="empty-state-hint">{hint}</div> : null}
    </div>
  )
}
