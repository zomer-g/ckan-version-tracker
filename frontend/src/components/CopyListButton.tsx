/**
 * "העתק" button for list screens (scrape-queue failures, activity log).
 *
 * Copies a caller-built plain-text digest to the clipboard so the operator
 * can paste error lists straight into a debugging chat. `getText` is lazy —
 * the digest is built from the CURRENT (possibly filtered) list only when
 * clicked. Flashes "הועתק ✓" on success; falls back to a hidden textarea
 * when the async Clipboard API is unavailable (non-HTTPS / older browsers).
 */
import { useRef, useState } from "react";

async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    /* fall through to the textarea fallback */
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}

export default function CopyListButton({
  getText,
  label = "העתק",
  title = "העתק את הרשימה ללוח",
}: {
  getText: () => string;
  label?: string;
  title?: string;
}) {
  const [state, setState] = useState<"idle" | "done" | "fail">("idle");
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const onClick = async () => {
    const ok = await copyText(getText());
    setState(ok ? "done" : "fail");
    clearTimeout(timer.current);
    timer.current = setTimeout(() => setState("idle"), 2000);
  };

  return (
    <button
      type="button"
      onClick={onClick}
      title={title}
      style={{
        background: "none",
        border: "1px solid var(--border, #cbd5e1)",
        color: state === "done" ? "#166534" : state === "fail" ? "#991b1b" : "var(--text-muted)",
        cursor: "pointer",
        fontSize: "0.7rem",
        padding: "0.15rem 0.5rem",
        borderRadius: "4px",
        whiteSpace: "nowrap",
      }}
    >
      {state === "done" ? "הועתק ✓" : state === "fail" ? "ההעתקה נכשלה" : `⧉ ${label}`}
    </button>
  );
}
