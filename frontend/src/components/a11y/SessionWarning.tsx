import { useState } from "react";
import { useAuth } from "../../auth/AuthContext";

/**
 * Warns before the session runs out, and offers to extend it in place
 * (WCAG 2.2.1 Timing Adjustable, 2.2.5 Re-authenticating).
 *
 * The token lives about two hours and refreshes silently every 45 minutes.
 * When that refresh failed, nothing was said — the next request came back 401
 * and the user was bounced, taking an unsaved SQL query or a half-filled
 * request form with it. Renewing from here keeps the page, and everything
 * typed into it, exactly where it was.
 *
 * role="alertdialog" rather than a quiet status line: this is time-limited and
 * needs to interrupt, which is the one case the criteria ask for it.
 */
export default function SessionWarning() {
  const { user, sessionWarning, renewSession, logout } = useAuth();
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);

  if (!user || !sessionWarning) return null;

  const renew = async () => {
    setBusy(true);
    setFailed(false);
    try {
      await renewSession();
    } catch {
      setFailed(true);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      role="alertdialog"
      aria-live="assertive"
      aria-label="החיבור עומד לפוג"
      style={{
        position: "fixed",
        insetInlineStart: "1rem",
        insetBlockEnd: "1rem",
        zIndex: 300,
        maxWidth: "26rem",
        background: "var(--tint-warn-bg)",
        color: "var(--tint-warn-fg)",
        border: "1px solid var(--tint-warn-bd)",
        borderRadius: "var(--radius)",
        padding: "0.9rem 1.1rem",
        boxShadow: "var(--shadow-md)",
      }}
    >
      <p style={{ margin: "0 0 0.5rem", fontWeight: 600 }}>החיבור שלך עומד לפוג</p>
      <p style={{ margin: "0 0 0.7rem", fontSize: "0.88rem", lineHeight: 1.6 }}>
        חידוש החיבור ישאיר את העמוד וכל מה שהוקלד בו במקומם. בלי חידוש, הפעולה
        הבאה תדרוש התחברות מחדש.
      </p>
      {failed && (
        <p role="alert" style={{ margin: "0 0 0.6rem", fontSize: "0.85rem", fontWeight: 600 }}>
          החידוש נכשל. שמרו את מה שהקלדתם והתחברו מחדש.
        </p>
      )}
      <div style={{ display: "flex", gap: "0.5rem" }}>
        <button className="btn-primary" onClick={renew} disabled={busy}>
          {busy ? "מחדש…" : "חידוש החיבור"}
        </button>
        <button className="btn-secondary" onClick={logout}>
          התנתקות
        </button>
      </div>
    </div>
  );
}
