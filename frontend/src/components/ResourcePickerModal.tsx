import { useEffect, useMemo, useState } from "react";
import { ckan } from "../api/client";

export interface ResourceLite {
  id: string;
  name?: string;
  format?: string;
}

interface ResourcePickerModalProps {
  /** CKAN dataset slug or id used to fetch the resource list. */
  ckanId: string;
  /** Currently-tracked resource ids; pre-checks these in the modal. */
  initialSelected: string[];
  /** Header title. */
  datasetTitle: string;
  /** Optional: a list of resource ids that the source has but aren't
   *  yet tracked — rendered with a "חדש" badge so the admin can spot
   *  what triggered the alert. */
  newResourceIds?: string[];
  onClose: () => void;
  onSave: (resourceIds: string[]) => Promise<void>;
}

export default function ResourcePickerModal({
  ckanId,
  initialSelected,
  datasetTitle,
  newResourceIds = [],
  onClose,
  onSave,
}: ResourcePickerModalProps) {
  const [resources, setResources] = useState<ResourceLite[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set(initialSelected));
  const [saving, setSaving] = useState(false);

  const newIdSet = useMemo(() => new Set(newResourceIds), [newResourceIds]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const pkg = await ckan.dataset(ckanId);
        if (cancelled) return;
        const list: ResourceLite[] = (pkg.resources || []).map((r: any) => ({
          id: r.id,
          name: r.name || r.description || r.id,
          format: (r.format || "").toUpperCase() || undefined,
        }));
        setResources(list);
      } catch (e: any) {
        if (!cancelled) setError(e?.message || "טעינת המשאבים נכשלה");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [ckanId]);

  const toggle = (rid: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(rid)) next.delete(rid);
      else next.add(rid);
      return next;
    });
  };

  const handleSave = async () => {
    if (selected.size === 0) {
      setError("בחרו לפחות קובץ אחד למעקב");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await onSave(Array.from(selected));
      onClose();
    } catch (e: any) {
      setError(e?.message || "שמירה נכשלה");
      setSaving(false);
    }
  };

  const overlayStyle: React.CSSProperties = {
    position: "fixed",
    inset: 0,
    background: "rgba(0,0,0,0.4)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    zIndex: 1000,
    padding: "1rem",
  };

  const dialogStyle: React.CSSProperties = {
    background: "white",
    borderRadius: "var(--radius)",
    maxWidth: "32rem",
    width: "100%",
    maxHeight: "90vh",
    display: "flex",
    flexDirection: "column",
    boxShadow: "0 12px 40px rgba(0,0,0,0.25)",
  };

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
        <div
          style={{
            padding: "0.9rem 1.1rem",
            borderBottom: "1px solid var(--border)",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: "0.5rem",
          }}
        >
          <div>
            <div style={{ fontWeight: 600, fontSize: "0.95rem" }}>בחירת קבצים למעקב</div>
            <div className="text-sm text-muted" style={{ marginTop: "0.15rem" }}>
              {datasetTitle}
            </div>
          </div>
          <button
            onClick={onClose}
            style={{ background: "none", border: "none", fontSize: "1.4rem", cursor: "pointer", color: "var(--text-muted)" }}
            aria-label="סגור"
          >
            &times;
          </button>
        </div>

        <div style={{ padding: "1rem 1.1rem", overflowY: "auto", flex: 1 }}>
          {error && (
            <div role="alert" className="badge badge-danger mb-1" style={{ display: "block" }}>
              {error}
            </div>
          )}
          {resources === null && !error && (
            <div className="text-sm text-muted">טוען רשימת קבצים…</div>
          )}
          {resources && resources.length === 0 && (
            <div className="text-sm text-muted">לא נמצאו קבצים במאגר.</div>
          )}
          {resources && resources.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: "0.3rem" }}>
              {resources.map((res) => {
                const checked = selected.has(res.id);
                const isNew = newIdSet.has(res.id);
                return (
                  <label
                    key={res.id}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "0.5rem",
                      padding: "0.45rem 0.6rem",
                      background: isNew ? "#fef3c7" : "var(--bg-secondary, #f8f9fa)",
                      border: "1px solid var(--border)",
                      borderRadius: "var(--radius)",
                      cursor: "pointer",
                      fontSize: "0.85rem",
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggle(res.id)}
                    />
                    <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis" }}>
                      {res.name || res.id}
                    </span>
                    {isNew && (
                      <span
                        className="badge"
                        style={{ background: "#f59e0b", color: "white", fontSize: "0.65rem" }}
                      >
                        חדש
                      </span>
                    )}
                    {res.format && (
                      <span className="badge" style={{ fontSize: "0.65rem" }}>
                        {res.format}
                      </span>
                    )}
                  </label>
                );
              })}
            </div>
          )}
        </div>

        <div
          style={{
            padding: "0.75rem 1.1rem",
            borderTop: "1px solid var(--border)",
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: "0.5rem",
          }}
        >
          <span className="text-sm text-muted">
            {resources ? `${selected.size}/${resources.length} נבחרו` : ""}
          </span>
          <div style={{ display: "flex", gap: "0.4rem" }}>
            <button className="btn-secondary" onClick={onClose} disabled={saving}>
              ביטול
            </button>
            <button
              className="btn-primary"
              onClick={handleSave}
              disabled={saving || !resources || selected.size === 0}
            >
              {saving ? "שומר…" : "שמור"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
