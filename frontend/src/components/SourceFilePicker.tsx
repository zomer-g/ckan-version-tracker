import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { sources, SourceFile } from "../api/client";

interface Props {
  /** The pasted page URL. Previewed live; nothing is stored until submit. */
  sourceUrl: string;
  /** Currently-ticked server-relative paths, owned by the parent form. */
  selected: Set<string>;
  onChange: (next: Set<string>) => void;
  /** Reports the page's own file count so the form can require a choice only
   *  once there is something to choose from. */
  onLoaded?: (files: SourceFile[]) => void;
}

function fmtSize(bytes: number): string {
  if (!bytes) return "";
  if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${Math.max(1, Math.round(bytes / 1024))} KB`;
}

/**
 * Which files on a pasted page to import.
 *
 * A CBS publication page hangs anywhere from a handful to 110 Excel tables off
 * it, and the folder behind it also holds files belonging to OTHER pages. So
 * "track this page" is not a single decision — it is a decision per table, and
 * this is where it gets made: before the dataset exists, from a live read of
 * the page (POST /api/sources/preview).
 *
 * Files are grouped by the page's own chapter headings and listed in the page's
 * own order, so the list reads like the page it came from. Files the page does
 * not itself list are shown last under their own heading and start unticked —
 * they are public files and hiding them would be a strange thing to enforce,
 * but importing somebody else's publication by default would be worse.
 */
export default function SourceFilePicker({
  sourceUrl,
  selected,
  onChange,
  onLoaded,
}: Props) {
  const { t } = useTranslation();
  const [files, setFiles] = useState<SourceFile[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setFiles(null);
    setError(null);
    (async () => {
      try {
        const res = await sources.preview(sourceUrl);
        if (cancelled) return;
        setFiles(res.files);
        onLoaded?.(res.files);
        // Default to what the page publishes and OVER does not already have.
        // Re-offering the 23 files already in the approval queue is how a page
        // of 27 came back reporting "all of them are duplicates".
        onChange(new Set(
          res.files.filter((f) => f.on_page && !f.tracked).map((f) => f.path),
        ));
      } catch (e: any) {
        if (!cancelled) setError(e?.message || t("common.error"));
      }
    })();
    return () => {
      cancelled = true;
    };
    // onChange/onLoaded are recreated on every parent render; re-running on
    // them would refetch the page on every keystroke in the form.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceUrl, attempt]);

  const groups = useMemo(() => {
    const out: Array<{ heading: string; onPage: boolean; items: SourceFile[] }> = [];
    for (const file of files || []) {
      const heading = file.on_page
        ? [file.chapter, file.subject].filter(Boolean).join(" · ")
        : t("home.picker_not_on_page");
      const last = out[out.length - 1];
      if (last && last.heading === heading && last.onPage === file.on_page) {
        last.items.push(file);
      } else {
        out.push({ heading, onPage: file.on_page, items: [file] });
      }
    }
    return out;
  }, [files, t]);

  const toggle = (path: string) => {
    const next = new Set(selected);
    if (next.has(path)) next.delete(path);
    else next.add(path);
    onChange(next);
  };

  const boxStyle: React.CSSProperties = {
    padding: "0.7rem 0.8rem",
    border: "1px solid var(--primary-100)",
    borderRadius: "var(--radius)",
    background: "white",
  };

  if (error) {
    return (
      <div style={boxStyle}>
        <div role="alert" className="text-sm" style={{ color: "#dc2626" }}>
          {t("home.picker_failed")} {error}
        </div>
        <button
          type="button"
          className="btn-secondary"
          style={{ fontSize: "0.75rem", marginTop: "0.5rem" }}
          onClick={() => setAttempt((n) => n + 1)}
        >
          {t("home.picker_retry")}
        </button>
        <p className="text-sm text-muted" style={{ margin: "0.5rem 0 0 0" }}>
          {t("home.picker_failed_hint")}
        </p>
      </div>
    );
  }

  if (!files) {
    return (
      <div style={boxStyle}>
        <div className="text-sm text-muted">{t("home.picker_loading")}</div>
      </div>
    );
  }

  if (files.length === 0) {
    return (
      <div style={boxStyle}>
        <div className="text-sm text-muted">{t("home.picker_empty")}</div>
      </div>
    );
  }

  // "Select all" means everything still worth picking. A file OVER already has
  // is not, and including it turns the submit into a wall of duplicates.
  const tickable = files.filter((f) => !f.tracked).map((f) => f.path);
  const trackedCount = files.filter((f) => f.tracked).length;

  return (
    <div style={boxStyle}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          gap: "0.5rem",
          marginBottom: "0.5rem",
          flexWrap: "wrap",
        }}
      >
        <div className="text-sm" style={{ fontWeight: 600 }}>
          {t("home.request_pick_files_label")}
          <span style={{ color: "#dc2626", marginInlineStart: "0.25rem" }}>*</span>
        </div>
        <div style={{ display: "flex", gap: "0.25rem" }}>
          <button
            type="button"
            onClick={() => onChange(new Set(tickable))}
            style={miniButton}
          >
            {t("home.request_select_all")}
          </button>
          <button type="button" onClick={() => onChange(new Set())} style={miniButton}>
            {t("home.request_clear")}
          </button>
        </div>
      </div>

      <div
        style={{
          maxHeight: "18rem",
          overflowY: "auto",
          display: "flex",
          flexDirection: "column",
          gap: "0.1rem",
        }}
      >
        {groups.map((group, gi) => (
          <div key={`${group.heading}-${gi}`}>
            {group.heading && (
              <div
                className="text-sm"
                style={{
                  fontWeight: 600,
                  color: group.onPage ? "var(--text-muted)" : "#b45309",
                  padding: "0.4rem 0 0.2rem 0",
                }}
              >
                {group.heading}
              </div>
            )}
            {group.items.map((file) => (
              <label
                key={file.path}
                style={{
                  display: "flex",
                  alignItems: "flex-start",
                  gap: "0.4rem",
                  padding: "0.2rem 0",
                  cursor: "pointer",
                  fontSize: "0.85rem",
                }}
              >
                <input
                  type="checkbox"
                  checked={selected.has(file.path)}
                  onChange={() => toggle(file.path)}
                  disabled={file.tracked}
                  style={{ width: "1rem", height: "1rem", flexShrink: 0, marginTop: "0.2rem" }}
                />
                <span style={{ flex: 1, minWidth: 0, opacity: file.tracked ? 0.55 : 1 }}>
                  <span style={{ wordBreak: "break-word" }}>{file.title}</span>
                  {file.tracked && (
                    <span
                      style={{
                        marginInlineStart: "0.4rem",
                        padding: "0.05rem 0.35rem",
                        borderRadius: "9999px",
                        fontSize: "0.65rem",
                        fontWeight: 600,
                        background: "#dcfce7",
                        color: "#166534",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {file.tracked_dataset?.status === "pending"
                        ? t("home.picker_pending")
                        : t("home.picker_tracked")}
                    </span>
                  )}
                  <span className="text-muted" style={{ marginInlineStart: "0.4rem", fontSize: "0.75rem" }}>
                    {file.ext.toUpperCase()}
                    {file.size ? ` · ${fmtSize(file.size)}` : ""}
                    {file.modified ? ` · ${file.modified.slice(0, 10)}` : ""}
                    {!file.tabular ? ` · ${t("home.picker_no_table")}` : ""}
                  </span>
                </span>
              </label>
            ))}
          </div>
        ))}
      </div>

      <p className="text-sm text-muted" style={{ margin: "0.5rem 0 0 0" }}>
        {t("home.request_files_selected", { n: selected.size, total: files.length })}
        {trackedCount > 0 && ` · ${t("home.picker_tracked_count", { n: trackedCount })}`}
        {" — "}
        {t("home.picker_tables_hint")}
      </p>
    </div>
  );
}

const miniButton: React.CSSProperties = {
  background: "none",
  border: "1px solid var(--border)",
  borderRadius: "4px",
  padding: "0.15rem 0.5rem",
  fontSize: "0.7rem",
  cursor: "pointer",
  color: "var(--text-muted)",
};
