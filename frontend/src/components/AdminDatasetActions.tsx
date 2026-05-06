import { useState } from "react";
import { datasets as datasetsApi } from "../api/client";
import { useAuth } from "../auth/AuthContext";

interface Props {
  datasetId: string;
  title: string;
  onDeleted?: (datasetId: string) => void;
}

export default function AdminDatasetActions({ datasetId, title, onDeleted }: Props) {
  const { user } = useAuth();
  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState<{ ok: boolean; msg: string } | null>(null);

  if (!user?.is_admin) return null;

  const handlePoll = async () => {
    setBusy(true);
    setToast(null);
    try {
      await datasetsApi.poll(datasetId);
      setToast({ ok: true, msg: "נשלח לדגום ✓" });
      setTimeout(() => setToast(null), 4000);
    } catch (e: any) {
      setToast({ ok: false, msg: e?.message || "שגיאה בדגום" });
      setTimeout(() => setToast(null), 4000);
    } finally {
      setBusy(false);
    }
  };

  const handleDelete = async () => {
    if (!confirm(`למחוק את "${title}"?`)) return;
    setBusy(true);
    try {
      await datasetsApi.untrack(datasetId);
      onDeleted?.(datasetId);
    } catch (e: any) {
      setToast({ ok: false, msg: e?.message || "שגיאה במחיקה" });
      setTimeout(() => setToast(null), 4000);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="flex" style={{ gap: "0.4rem", alignItems: "center", flexWrap: "wrap" }}>
      <button
        className="btn-secondary"
        onClick={handlePoll}
        disabled={busy}
        style={{ fontSize: "0.75rem", padding: "0.25rem 0.6rem" }}
      >
        {busy ? "..." : "דגום"}
      </button>
      <button
        className="btn-danger"
        onClick={handleDelete}
        disabled={busy}
        style={{ fontSize: "0.75rem", padding: "0.25rem 0.6rem" }}
      >
        מחק
      </button>
      {toast && (
        <span
          style={{
            fontSize: "0.7rem",
            padding: "0.15rem 0.4rem",
            borderRadius: "4px",
            background: toast.ok ? "#dcfce7" : "#fee2e2",
            color: toast.ok ? "#166534" : "#991b1b",
            whiteSpace: "nowrap",
          }}
        >
          {toast.msg}
        </span>
      )}
    </div>
  );
}
