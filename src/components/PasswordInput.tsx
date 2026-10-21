import { useState } from 'react'

export default function PasswordInput({
  value,
  onChange,
  autoComplete,
  showLabel,
  hideLabel,
}: {
  value: string
  onChange: (value: string) => void
  autoComplete?: string
  showLabel: string
  hideLabel: string
}) {
  const [visible, setVisible] = useState(false)

  return (
    <div className="password-input">
      <input
        type={visible ? 'text' : 'password'}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        autoComplete={autoComplete}
      />
      <button
        type="button"
        className="password-toggle"
        onClick={() => setVisible((v) => !v)}
        aria-label={visible ? hideLabel : showLabel}
        title={visible ? hideLabel : showLabel}
      >
        {visible ? (
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <path
              d="M3 3l18 18M10.6 10.6a2 2 0 002.8 2.8M9.9 5.1A10.4 10.4 0 0112 5c5 0 9.3 3.1 11 7.5a11.7 11.7 0 01-4.2 5.1M6.1 6.1A11.7 11.7 0 001 12.5C2.7 16.9 7 20 12 20c1.5 0 2.9-.3 4.2-.8"
              stroke="currentColor"
              strokeWidth="1.8"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        ) : (
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <path
              d="M1 12.5C2.7 8.1 7 5 12 5s9.3 3.1 11 7.5c-1.7 4.4-6 7.5-11 7.5S2.7 16.9 1 12.5z"
              stroke="currentColor"
              strokeWidth="1.8"
              strokeLinejoin="round"
            />
            <circle cx="12" cy="12.5" r="2.5" stroke="currentColor" strokeWidth="1.8" />
          </svg>
        )}
      </button>
    </div>
  )
}
