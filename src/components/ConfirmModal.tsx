type Props = {
  title: string
  message: string
  detail?: string
  confirmLabel: string
  cancelLabel: string
  langSwitchLabel?: string
  onLangSwitch?: () => void
  onConfirm: () => void
  onCancel: () => void
}

export default function ConfirmModal({
  title,
  message,
  detail,
  confirmLabel,
  cancelLabel,
  langSwitchLabel,
  onLangSwitch,
  onConfirm,
  onCancel,
}: Props) {
  return (
    <div className="modal-backdrop" role="presentation" onClick={onCancel}>
      <div
        className="modal-dialog modal-dialog-info"
        role="alertdialog"
        aria-modal="true"
        aria-labelledby="confirm-modal-title"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="modal-title-row">
          <h3 id="confirm-modal-title">{title}</h3>
          {langSwitchLabel && onLangSwitch ? (
            <button type="button" className="modal-lang-btn" onClick={onLangSwitch}>
              {langSwitchLabel}
            </button>
          ) : null}
        </div>
        <p className="modal-message modal-message-plain">{message}</p>
        {detail ? <pre className="modal-message modal-detail">{detail}</pre> : null}
        <div className="modal-actions modal-actions-split">
          <button type="button" onClick={onCancel}>
            {cancelLabel}
          </button>
          <button type="button" className="primary" onClick={onConfirm}>
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
