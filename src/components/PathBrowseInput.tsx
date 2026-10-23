/** PEM / file path — browse only (no typing caret). Value set via dialog. */
export default function PathBrowseInput({
  value,
  onChange,
  onBrowse,
  placeholder,
  browseLabel,
}: {
  value: string
  onChange: (value: string) => void
  onBrowse: () => void | Promise<void>
  placeholder?: string
  browseLabel: string
}) {
  const fileName = value
    ? value.split(/[/\\]/).filter(Boolean).pop() || value
    : ''

  return (
    <div className={`path-browse-input${value ? ' has-path' : ''}`}>
      <input
        readOnly
        value={fileName}
        placeholder={placeholder}
        spellCheck={false}
        autoCapitalize="off"
        autoCorrect="off"
        tabIndex={0}
        title={value || placeholder}
        aria-label={value || placeholder}
        onChange={() => {
          /* readOnly — path only from Browse */
        }}
        onKeyDown={(e) => {
          // Allow clearing with Backspace/Delete when focused.
          if (e.key === 'Backspace' || e.key === 'Delete') {
            e.preventDefault()
            if (value) onChange('')
          }
        }}
      />
      <button type="button" className="path-browse-btn" onClick={() => void onBrowse()}>
        {browseLabel}
      </button>
    </div>
  )
}
