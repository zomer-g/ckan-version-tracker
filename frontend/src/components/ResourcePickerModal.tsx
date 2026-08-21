import { useEffect, useMemo, useState } from "react";
import Modal from "./a11y/Modal";
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

  return (
    <Modal
      title="בחירת קבצים למעקב"
      onClose={onClose}
      width="32rem"
      closeOnBackdrop={false}
      footer={
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", width: "100%", gap: "0.5rem" }}>
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
      }
    >
      <p className="text-sm text-muted" style={{ marginTop: 0 }}>{datasetTitle}</p>
      <>
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
                      background: isNew ? "#fef3c7" : "var(--surface-2)",
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
                      style={{ width: "1.1rem", height: "1.1rem", flexShrink: 0 }}
                    />
                    <span
                      style={{
                        flex: 1,
                        minWidth: 0,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {res.name || res.id}
                    </span>
                    {isNew && (
                      <span
                        className="badge"
                        style={{ background: "var(--fill-warn)", color: "var(--on-fill-warn)", fontSize: "0.65rem" }}
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
      </>
    </Modal>
  );
}
