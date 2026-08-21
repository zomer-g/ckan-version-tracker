import { useEffect, useState } from "react";
import { drive } from "../api/client";

// Admin-level Google Drive connection management: connect / reconnect /
// disconnect the refresh token used by "export a version to Drive". The
// per-version export button also offers connect, but there was no place to
// SEE the connection state or drop a stale token — a token goes stale every
// 7 days while the OAuth consent screen sits in "Testing", and the only fix
// is to disconnect + reconnect.
export default function DriveConnectionPanel() {
  const [connected, setConnected] = useState<boolean | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Result of a just-completed consent round-trip (?drive=... on the hash).
  const [returnMsg, setReturnMsg] = useState<{ ok: boolean; text: string } | null>(null);

  const refresh = async () => {
    try {
      const s = await drive.status();
      setConnected(s.connected);
    } catch (e: any) {
      setError(e?.message || "שגיאה בטעינת סטטוס");
    }
  };

  useEffect(() => {
    refresh();
    // The Drive consent callback returns to /admin#drive and appends
    // ?drive=connected|denied|error|norefresh onto the hash. Read it once,
    // show a message, then scrub it back to a clean #drive.
    const hash = window.location.hash || "";
    const q = hash.includes("?") ? hash.slice(hash.indexOf("?") + 1) : "";
    const status = new URLSearchParams(q).get("drive");
    if (status) {
      const map: Record<string, { ok: boolean; text: string }> = {
        connected: { ok: true, text: "Drive חובר בהצלחה." },
        denied: { ok: false, text: "החיבור בוטל במסך ההסכמה של Google." },
        error: { ok: false, text: "שגיאה בהחלפת ה-token מול Google." },
        norefresh: { ok: false, text: "Google לא החזירה refresh token — נסה שוב." },
      };
      setReturnMsg(map[status] || null);
      window.history.replaceState(null, "", window.location.pathname + "#drive");
    }
  }, []);

  const connect = async () => {
    setBusy(true);
    setError(null);
    try {
      // Return to this tab after consent so the admin lands back here.
      const { authorize_url } = await drive.connect("/admin#drive");
      window.location.href = authorize_url;
    } catch (e: any) {
      setError(e?.message || "לא ניתן להתחיל את חיבור ה-Drive");
      setBusy(false);
    }
  };

  const disconnect = async () => {
    if (!window.confirm("לנתק את חיבור ה-Drive? תצטרך לחבר מחדש כדי לייצא.")) return;
    setBusy(true);
    setError(null);
    try {
      await drive.disconnect();
      setReturnMsg(null);
      await refresh();
    } catch (e: any) {
      setError(e?.message || "שגיאה בניתוק");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section style={{ maxWidth: 720 }}>
      <h2 style={{ fontSize: "1.15rem", marginBottom: "0.75rem" }}>
        חיבור Google Drive
      </h2>
      <p style={{ fontSize: "0.85rem", color: "var(--text-muted)", marginBottom: "1rem", lineHeight: 1.6 }}>
        משמש לייצוא כל הקבצים של גרסה ישירות לתיקיית Drive (הכפתור "ייצוא לדרייב"
        בעמוד גרסה). החיבור שמור על חשבון האדמין המחובר.
      </p>

      <div style={{
        border: "1px solid var(--border)", borderRadius: 8, padding: "1rem",
        background: "var(--surface-2)", display: "flex", alignItems: "center",
        gap: "0.75rem", flexWrap: "wrap",
      }}>
        <span style={{
          display: "inline-flex", alignItems: "center", gap: "0.4rem",
          fontSize: "0.9rem", fontWeight: 600,
          color: connected ? "var(--success)" : "#941A1A",
        }}>
          <span style={{
            width: 10, height: 10, borderRadius: "50%",
            background: connected ? "var(--success)" : "var(--danger)", display: "inline-block",
          }} />
          {connected === null ? "טוען..." : connected ? "מחובר" : "לא מחובר"}
        </span>

        <div style={{ flex: 1 }} />

        <button className="btn-primary" onClick={connect} disabled={busy}
          style={{ fontSize: "0.85rem", padding: "0.4rem 0.9rem" }}>
          {connected ? "התחבר מחדש" : "התחבר ל-Drive"}
        </button>
        {connected && (
          <button className="btn-secondary" onClick={disconnect} disabled={busy}
            style={{ fontSize: "0.85rem", padding: "0.4rem 0.9rem" }}>
            נתק
          </button>
        )}
      </div>

      {returnMsg && (
        <div style={{
          marginTop: "0.75rem", fontSize: "0.85rem", padding: "0.5rem 0.75rem",
          borderRadius: 6,
          background: returnMsg.ok ? "var(--tint-good-bg)" : "var(--tint-bad-bg)",
          color: returnMsg.ok ? "var(--success)" : "#941A1A",
        }}>{returnMsg.text}</div>
      )}
      {error && (
        <div style={{
          marginTop: "0.75rem", fontSize: "0.85rem", padding: "0.5rem 0.75rem",
          borderRadius: 6, background: "var(--tint-bad-bg)", color: "var(--tint-bad-fg)",
        }}>{error}</div>
      )}

      <div style={{
        marginTop: "1.25rem", fontSize: "0.8rem", color: "var(--text-muted)",
        lineHeight: 1.7, borderTop: "1px solid var(--border)", paddingTop: "0.75rem",
      }}>
        <strong>הטוקן פג כל 7 ימים?</strong> זה קורה כשמסך ההסכמה של OAuth
        ב-Google Cloud Console במצב "Testing". פרסום ל-Production
        (OAuth consent screen → Publish app) מבטל את הפקיעה. אחרי פרסום —
        נתק והתחבר מחדש כאן כדי לקבל token מתמשך. במסך "unverified app" של Google
        בחר Advanced → המשך.
      </div>
    </section>
  );
}
