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
  return (
    <div className="path-browse-input">
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        spellCheck={false}
        autoCapitalize="off"
        autoCorrect="off"
      />
      <button type="button" className="path-browse-btn" onClick={() => void onBrowse()}>
        {browseLabel}
      </button>
    </div>
  )
}
