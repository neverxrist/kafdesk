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
      {/*
        Always type=text: WKWebView + custom fonts break type=password bullets (tofu).
        Masking via -webkit-text-security on .is-masked instead.
      */}
      <input
        type="text"
        className={visible ? undefined : 'is-masked'}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        autoComplete={autoComplete}
        spellCheck={false}
        autoCapitalize="none"
        autoCorrect="off"
        lang="en"
      />
      <button
        type="button"
        className="password-toggle"
        onClick={() => setVisible((v) => !v)}
        aria-label={visible ? hideLabel : showLabel}
        title={visible ? hideLabel : showLabel}
        aria-pressed={visible}
      >
        {visible ? (
          /* open eye — password visible */
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <path
              d="M1 12.5C2.7 8.1 7 5 12 5s9.3 3.1 11 7.5c-1.7 4.4-6 7.5-11 7.5S2.7 16.9 1 12.5z"
              stroke="currentColor"
              strokeWidth="1.8"
              strokeLinejoin="round"
            />
            <circle cx="12" cy="12.5" r="2.5" stroke="currentColor" strokeWidth="1.8" />
          </svg>
        ) : (
          /* closed eye — password hidden */
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <path
              d="M3 12.5c1.8 2.8 5 4.5 9 4.5s7.2-1.7 9-4.5"
              stroke="currentColor"
              strokeWidth="1.8"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
            <path
              d="M8.5 14.2L7 16.5M12 14.8v2.7M15.5 14.2L17 16.5"
              stroke="currentColor"
              strokeWidth="1.8"
              strokeLinecap="round"
            />
          </svg>
        )}
      </button>
    </div>
  )
}
