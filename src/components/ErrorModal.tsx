import errorBg from '../assets/error-bg.png'

type Props = {
  title: string
  message: string
  closeLabel: string
  onClose: () => void
}

export default function ErrorModal({ title, message, closeLabel, onClose }: Props) {
  return (
    <div className="modal-backdrop modal-backdrop-error" role="presentation" onClick={onClose}>
      <img className="modal-error-bg" src={errorBg} alt="" aria-hidden="true" draggable={false} />
      <div
        className="modal-dialog"
        role="alertdialog"
        aria-modal="true"
        aria-labelledby="error-modal-title"
        onClick={(e) => e.stopPropagation()}
      >
        <h3 id="error-modal-title">{title}</h3>
        <pre className="modal-message">{message}</pre>
        <div className="modal-actions">
          <button type="button" className="primary" onClick={onClose}>
            {closeLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
