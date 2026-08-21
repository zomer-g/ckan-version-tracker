import { useEffect, useId, useRef } from "react";

const FOCUSABLE =
  'a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),' +
  'textarea:not([disabled]),summary,[tabindex]:not([tabindex="-1"])';

/**
 * The one modal shell in the app.
 *
 * Before this, every overlay was a bare pair of divs: no role, no accessible
 * name, nothing to stop Tab walking straight out into the page behind it, no
 * Escape, and no way back to the control that opened it. A screen reader saw
 * content that had simply appeared somewhere in the document.
 *
 * What it guarantees:
 *   · role="dialog" + aria-modal, named by its own heading (WCAG 4.1.2)
 *   · focus enters on open and is restored to the opener on close (2.4.3)
 *   · Tab and Shift+Tab cycle inside; Escape closes (2.1.2)
 *   · the rest of the page is inert while it is up, so a screen reader cannot
 *     wander into content the sighted user cannot reach
 *
 * The backdrop is NOT a click target with a handler and no role — closing by
 * clicking outside is a mouse affordance, so it is offered as a genuine button
 * that screen readers see as "close", alongside the visible ✕.
 */
export default function Modal(props: {
  title: string;
  onClose: () => void;
  children: React.ReactNode;
  footer?: React.ReactNode;
  /** Defaults to 32rem. */
  width?: string;
  /** Skip the backdrop-click close where a stray click would lose work. */
  closeOnBackdrop?: boolean;
}) {
  const { title, onClose, children, footer, width = "32rem" } = props;
  const closeOnBackdrop = props.closeOnBackdrop !== false;
  const dialogRef = useRef<HTMLDivElement>(null);
  const openerRef = useRef<Element | null>(null);
  const titleId = useId();

  useEffect(() => {
    openerRef.current = document.activeElement;

    // Everything outside the dialog goes inert. Tracked so we only clear the
    // flag on nodes we actually set it on — another modal may own the rest.
    const marked: HTMLElement[] = [];
    const root = dialogRef.current?.parentElement;
    Array.from(document.body.children).forEach((el) => {
      if (el === root || el.contains(dialogRef.current)) return;
      const node = el as HTMLElement;
      if (node.hasAttribute("aria-hidden")) return;
      node.setAttribute("aria-hidden", "true");
      marked.push(node);
    });

    // First stop is the dialog itself, so the name and role are announced
    // before its contents rather than dropping the user onto a stray control.
    dialogRef.current?.focus();

    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
        return;
      }
      if (e.key !== "Tab") return;
      const box = dialogRef.current;
      if (!box) return;
      const items = Array.from(box.querySelectorAll<HTMLElement>(FOCUSABLE)).filter(
        (el) => el.offsetParent !== null || el === document.activeElement
      );
      if (!items.length) {
        e.preventDefault();
        box.focus();
        return;
      }
      const first = items[0];
      const last = items[items.length - 1];
      const active = document.activeElement;
      if (e.shiftKey && (active === first || active === box)) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && active === last) {
        e.preventDefault();
        first.focus();
      }
    };

    document.addEventListener("keydown", onKey, true);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    return () => {
      document.removeEventListener("keydown", onKey, true);
      document.body.style.overflow = prevOverflow;
      marked.forEach((n) => n.removeAttribute("aria-hidden"));
      (openerRef.current as HTMLElement | null)?.focus?.();
    };
  }, [onClose]);

  return (
    <div className="modal-overlay">
      {closeOnBackdrop && (
        // A real control, not a div with onClick: it is reachable, it is
        // announced, and it sits behind the dialog in the stacking order.
        <button
          type="button"
          onClick={onClose}
          aria-label="סגירת החלון"
          tabIndex={-1}
          style={{
            position: "absolute",
            inset: 0,
            width: "100%",
            height: "100%",
            background: "transparent",
            border: 0,
            minHeight: 0,
            cursor: "default",
          }}
        />
      )}
      <div
        ref={dialogRef}
        className="modal-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        style={{ maxWidth: width, position: "relative" }}
      >
        <div className="modal-head">
          <h2 id={titleId}>{title}</h2>
          <button
            type="button"
            onClick={onClose}
            aria-label="סגירת החלון"
            style={{
              background: "none",
              border: "none",
              fontSize: "1.4rem",
              lineHeight: 1,
              color: "var(--text-muted)",
              minWidth: "var(--target-min)",
            }}
          >
            <span aria-hidden="true">&times;</span>
          </button>
        </div>
        <div className="modal-body">{children}</div>
        {footer && <div className="modal-foot">{footer}</div>}
      </div>
    </div>
  );
}
