import { useEffect, useRef, useState } from "react";
import Modal from "./a11y/Modal";
import { ProgressBar } from "./a11y";
import { drive, DriveExportJob } from "../api/client";
import { useAuth } from "../auth/AuthContext";

interface Props {
  versionId: string;
  fileCount: number;
}

/**
 * Admin-only "ייצוא לדרייב" control for one version. Pushes every file in the
 * version straight from the file store into a Google Drive folder server-side,
 * so the admin never has to download hundreds of ZIPs locally (the browser
 * blocks bulk auto-downloads anyway).
 *
 * Flow: check Drive connection → (connect once if needed) → paste a folder
 * link → enqueue → poll the durable job for live progress.
 */
export default function DriveExportButton({ versionId, fileCount }: Props) {
  const { user } = useAuth();
  const [open, setOpen] = useState(false);
  const [connected, setConnected] = useState<boolean | null>(null);
  const [folderUrl, setFolderUrl] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [job, setJob] = useState<DriveExportJob | null>(null);
  const pollRef = useRef<number | null>(null);

  // On a return from the connect flow we land back here with ?drive=connected.
  // Re-check status so the modal reflects it.
  useEffect(() => {
    return () => {
      if (pollRef.current) window.clearInterval(pollRef.current);
    };
  }, []);

  if (!user?.is_admin) return null;

  const openModal = async () => {
    setOpen(true);
    setError(null);
    setJob(null);
    try {
      const s = await drive.status();
      setConnected(s.connected);
    } catch {
      setConnected(false);
    }
  };

  const connect = async () => {
    // Return to this exact page so the admin can resume after consent. The
    // authorize URL is minted server-side from an authenticated POST, so no
    // token rides in the URL.
    setError(null);
    try {
      const { authorize_url } = await drive.connect(
        window.location.pathname + window.location.search
      );
      window.location.href = authorize_url;
    } catch (e: any) {
      setError(e?.message || "לא ניתן להתחיל את חיבור ה-Drive");
    }
  };

  const startPolling = (jobId: string) => {
    if (pollRef.current) window.clearInterval(pollRef.current);
    pollRef.current = window.setInterval(async () => {
      try {
        const j = await drive.exportStatus(jobId);
        setJob(j);
        if (j.status === "success" || j.status === "failed") {
          if (pollRef.current) window.clearInterval(pollRef.current);
        }
      } catch {
        /* transient — keep polling */
      }
    }, 2500);
  };

  const submit = async () => {
    if (!folderUrl.trim()) {
      setError("הדביקו קישור לתיקייה ב-Drive");
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      const j = await drive.exportVersion(versionId, folderUrl.trim());
      setJob(j);
      startPolling(j.id);
    } catch (e: any) {
      setError(e?.message || "השליחה נכשלה");
    } finally {
      setSubmitting(false);
    }
  };

  const btnStyle: React.CSSProperties = {
    color: "var(--primary)",
    background: "none",
    border: "1px solid var(--primary)",
    borderRadius: "4px",
    padding: "0.15rem 0.6rem",
    cursor: "pointer",
    fontWeight: 600,
    fontSize: "0.85rem",
  };

  return (
    <>
      <button
        type="button"
        onClick={openModal}
        style={btnStyle}
        title="העברת כל קבצי הגרסה לתיקייה ב-Google Drive"
      >
        <span aria-hidden="true">⬆</span> ייצוא לדרייב
      </button>

      {open && (
        <Modal
          title="ייצוא לדרייב"
          width="min(440px, 92vw)"
          closeOnBackdrop={!submitting}
          onClose={() => !submitting && setOpen(false)}
        >

            {connected === null && <p>טוען…</p>}

            {connected === false && (
              <>
                <p style={{ fontSize: "0.9rem", lineHeight: 1.5 }}>
                  כדי להעביר קבצים ישירות ל-Google Drive צריך לחבר את חשבון
                  הדרייב שלך פעם אחת.
                </p>
                <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.8rem" }}>
                  <button type="button" style={btnStyle} onClick={connect}>
                    חבר את Google Drive
                  </button>
                  <button
                    type="button"
                    style={{ ...btnStyle, color: "var(--text-muted)", borderColor: "var(--border)" }}
                    onClick={() => setOpen(false)}
                  >
                    ביטול
                  </button>
                </div>
                {error && (
                  <p style={{ color: "var(--danger)", fontSize: "0.8rem", margin: "0.6rem 0 0" }}>
                    {error}
                  </p>
                )}
              </>
            )}

            {connected === true && !job && (
              <>
                <p style={{ fontSize: "0.9rem", lineHeight: 1.5, margin: "0 0 0.5rem" }}>
                  כל המסמכים הגולמיים שבתוך {fileCount} חבילות ה-ZIP של הגרסה
                  יחולצו ויועברו ישירות לתיקייה שתבחר ב-Drive (קובץ ה-CSV
                  שמשמש כאינדקס יועבר גם הוא). התיקייה חייבת להיות בבעלות החשבון
                  המחובר או משותפת לו עם הרשאת עריכה.
                </p>
                <input aria-label="https://drive.google.com/drive/folders/…"
                  type="text"
                  value={folderUrl}
                  onChange={(e) => setFolderUrl(e.target.value)}
                  placeholder="https://drive.google.com/drive/folders/…"
                  dir="ltr"
                  style={{
                    width: "100%",
                    padding: "0.45rem 0.6rem",
                    border: "1px solid var(--border)",
                    borderRadius: "5px",
                    fontSize: "0.85rem",
                    boxSizing: "border-box",
                  }}
                />
                {error && (
                  <p style={{ color: "var(--danger)", fontSize: "0.8rem", margin: "0.5rem 0 0" }}>
                    {error}
                  </p>
                )}
                <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.9rem" }}>
                  <button type="button" style={btnStyle} onClick={submit} disabled={submitting}>
                    {submitting ? "מתחיל…" : "התחל ייצוא"}
                  </button>
                  <button
                    type="button"
                    style={{ ...btnStyle, color: "var(--text-muted)", borderColor: "var(--border)" }}
                    onClick={() => setOpen(false)}
                    disabled={submitting}
                  >
                    ביטול
                  </button>
                </div>
              </>
            )}

            {job && (
              <div style={{ fontSize: "0.9rem", lineHeight: 1.6 }}>
                {job.status === "success" ? (
                  <p style={{ color: "var(--success)" }}>
                    <span aria-hidden="true">✓</span> הסתיים — {job.documents_uploaded.toLocaleString()} מסמכים
                    הועברו לדרייב.
                  </p>
                ) : job.status === "failed" ? (
                  <p style={{ color: "var(--danger)" }}>
                    <span aria-hidden="true">✗</span> נכשל: {job.error || "שגיאה לא ידועה"}
                  </p>
                ) : (
                  <>
                    <p>
                      מחלץ ומעביר מסמכים…{" "}
                      <strong>{job.documents_uploaded.toLocaleString()}</strong>{" "}
                      הועברו (חבילה {job.completed_files}/{job.total_files})
                    </p>
                    <div style={{ margin: "0.4rem 0" }}>
                      <ProgressBar
                        label="התקדמות הייצוא לדרייב"
                        value={job.completed_files}
                        max={job.total_files || 1}
                        height={8}
                      />
                    </div>
                    {job.current_file && (
                      <p style={{ fontSize: "0.75rem", color: "var(--text-muted)" }} dir="ltr">
                        {job.current_file}
                      </p>
                    )}
                    <p style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
                      העברה רצה ברקע — אפשר לסגור את החלון, היא תמשיך.
                    </p>
                  </>
                )}
                <div style={{ marginTop: "0.8rem" }}>
                  <button
                    type="button"
                    style={{ ...btnStyle, color: "var(--text-muted)", borderColor: "var(--border)" }}
                    onClick={() => setOpen(false)}
                  >
                    סגור
                  </button>
                </div>
              </div>
            )}
        </Modal>
      )}
    </>
  );
}
