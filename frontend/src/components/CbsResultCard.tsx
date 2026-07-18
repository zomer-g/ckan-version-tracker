import { useState } from "react";
import { useTranslation } from "react-i18next";
import { cbs, formatBytes, CbsResult } from "../api/client";
import {
  geoLabel,
  productFormLabel,
  PRODUCT_FORM_ICONS,
  sectionLabel,
} from "../utils/cbsLabels";
import { YearVersion } from "../utils/cbsSeries";

// One emoji per file family, shown on the compact file-type chips.
const FILE_ICON: Record<string, string> = {
  xlsx: "📊",
  xls: "📊",
  csv: "📊",
  pdf: "📄",
  doc: "📝",
  docx: "📝",
  zip: "🗜️",
  json: "🗂️",
  xml: "🗂️",
};

const fileIcon = (ext: string) => FILE_ICON[ext.toLowerCase()] || "📎";

// The distinct file extensions present on a page — prefer the denormalised
// file_types column, fall back to deriving them from the links themselves.
function distinctTypes(r: CbsResult): string[] {
  if (r.file_types && r.file_types.length > 0) {
    return Array.from(new Set(r.file_types.map((e) => e.toLowerCase())));
  }
  const exts = (r.file_links ?? [])
    .map((f) => f.ext?.toLowerCase())
    .filter((e): e is string => !!e);
  return Array.from(new Set(exts));
}

interface Props {
  record: CbsResult;
  // Pin controls are rendered only for admins. `pinned` toggles the star's
  // filled/outline state; `onTogglePin` performs the pin/unpin. `busy` disables
  // the star while the request is in flight.
  canPin?: boolean;
  pinned?: boolean;
  busy?: boolean;
  onTogglePin?: (record: CbsResult) => void;
  // A pinned card at the top of the page gets an amber accent instead of the
  // default blue, so the pinned strip reads as distinct from search results.
  featured?: boolean;
  // Prior-year versions of this item (featured cards only). Always shown, as a
  // row of clickable year chips, so the admin/user can jump straight to a past
  // year. `undefined` while still loading; `[]` = no historical versions.
  history?: YearVersion[];
}

function yearSpan(r: CbsResult): string | null {
  if (r.year_start && r.year_end) {
    return r.year_start === r.year_end
      ? String(r.year_start)
      : `${r.year_start}–${r.year_end}`;
  }
  return (r.year_start && String(r.year_start)) || (r.year_end && String(r.year_end)) || null;
}

export default function CbsResultCard({
  record: r,
  canPin,
  pinned,
  busy,
  onTogglePin,
  featured,
  history,
}: Props) {
  const { t, i18n } = useTranslation();
  const he = i18n.language === "he";
  const span = yearSpan(r);

  // Files stay collapsed by default — the card shows only file-type chips, and
  // the full list is revealed on demand.
  const [filesOpen, setFilesOpen] = useState(false);
  const fileCount = r.file_links?.length ?? 0;
  const types = distinctTypes(r);

  // Edition history of this item's series, fetched lazily on first expand from
  // GET /api/cbs/series?key= — so a card with a series_key can list ALL its
  // editions (not only the prior-year strip the featured cards precompute) and
  // jump straight to any one. Rows outside a series (series_key null) show no
  // button. Featured cards already render `history`, so skip the button there.
  const [editionsOpen, setEditionsOpen] = useState(false);
  const [editions, setEditions] = useState<CbsResult[] | null>(null);
  const [editionsLoading, setEditionsLoading] = useState(false);
  const showEditionsBtn = !!r.series_key && !(history && history.length > 0);

  const toggleEditions = async () => {
    const next = !editionsOpen;
    setEditionsOpen(next);
    if (next && editions === null && r.series_key) {
      setEditionsLoading(true);
      try {
        const res = await cbs.series(r.series_key);
        setEditions(res.results);
      } catch {
        setEditions([]); // failed → show "none", not a spinner forever
      }
      setEditionsLoading(false);
    }
  };

  return (
    <article
      className="card"
      style={{
        borderRight: `4px solid ${featured ? "#f59e0b" : "#0ea5e9"}`,
        padding: "0.85rem 1rem",
      }}
    >
      <div className="flex-between mb-1" style={{ gap: "0.5rem" }}>
        <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
          <a
            href={r.url}
            target="_blank"
            rel="noopener noreferrer"
            style={{ color: "var(--text, inherit)" }}
          >
            {(he ? r.title : r.title_en) || r.title || r.title_en || r.url}
          </a>
        </h2>
        <div className="flex" style={{ gap: "0.35rem", flexShrink: 0 }}>
          {canPin && (
            <button
              type="button"
              onClick={() => onTogglePin?.(r)}
              disabled={busy}
              aria-pressed={pinned}
              title={
                pinned
                  ? t("cbs.unpin", "הסר מהמועדפים")
                  : t("cbs.pin", "נעץ למועדפים")
              }
              style={{
                border: "none",
                background: "transparent",
                cursor: busy ? "wait" : "pointer",
                fontSize: "1.1rem",
                lineHeight: 1,
                padding: "0 0.15rem",
                color: pinned ? "#f59e0b" : "#94a3b8",
              }}
            >
              {pinned ? "★" : "☆"}
            </button>
          )}
          <span
            style={{
              display: "inline-block",
              padding: "0.15rem 0.5rem",
              borderRadius: "9999px",
              fontSize: "0.65rem",
              fontWeight: 600,
              background: featured ? "#fef3c7" : "#e0f2fe",
              color: featured ? "#92400e" : "#075985",
              whiteSpace: "nowrap",
            }}
          >
            {featured ? `★ ${t("cbs.featured_badge", "מבוקש")}` : 'למ"ס'}
          </span>
        </div>
      </div>

      {r.summary && (
        <p
          className="text-sm text-muted mb-1"
          style={{ maxHeight: "3.2em", overflow: "hidden" }}
        >
          {r.summary}
        </p>
      )}

      <div
        className="flex text-sm text-muted"
        style={{ gap: "0.75rem", flexWrap: "wrap" }}
      >
        {/* Enrichment tags first — the user-vocabulary dimensions. */}
        {r.product_form && (
          <span>
            {PRODUCT_FORM_ICONS[r.product_form] || ""} {productFormLabel(r.product_form)}
          </span>
        )}
        {r.section && <span>{sectionLabel(r.section)}</span>}
        {(r.data_vintage || span) && <span>{r.data_vintage ?? span}</span>}
        {r.freq && <span>{r.freq}</span>}
        {r.source_op && <span>{r.source_op}</span>}
        {r.geo_levels && r.geo_levels.length > 0 && (
          <span>{r.geo_levels.map(geoLabel).join(", ")}</span>
        )}
        {r.series_key && r.is_latest_edition === false && (
          <span style={{ color: "#b45309" }}>
            {t("cbs.old_edition", "מהדורה ישנה")}
          </span>
        )}
        {r.geo_coverage && (
          <span title={r.geo_coverage}>⚠️ {r.geo_coverage}</span>
        )}
      </div>

      {showEditionsBtn && (
        <div style={{ marginTop: "0.5rem" }}>
          <button
            type="button"
            onClick={toggleEditions}
            aria-expanded={editionsOpen}
            className="btn-secondary"
            style={{ fontSize: "0.72rem", padding: "0.2rem 0.55rem" }}
          >
            📚 {t("cbs.all_editions", "כל המהדורות")}
            {editions ? ` (${editions.length})` : ""} {editionsOpen ? "▴" : "▾"}
          </button>

          {editionsOpen && (
            <div style={{ marginTop: "0.45rem" }}>
              {editionsLoading && (
                <span className="text-sm text-muted">{t("common.loading", "טוען…")}</span>
              )}
              {!editionsLoading && editions && editions.length === 0 && (
                <span className="text-sm text-muted">
                  {t("cbs.no_editions", "לא נמצאו מהדורות נוספות.")}
                </span>
              )}
              {!editionsLoading && editions && editions.length > 0 && (
                <div className="flex" style={{ gap: "0.3rem", flexWrap: "wrap" }}>
                  {editions.map((e) => {
                    const current = e.url === r.url;
                    return (
                      <a
                        key={e.url}
                        href={e.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        title={e.title || undefined}
                        className="badge"
                        style={{
                          fontSize: "0.72rem",
                          textDecoration: "none",
                          background: current ? "#0ea5e9" : "#fffbeb",
                          color: current ? "#fff" : "#92400e",
                          border: current ? "1px solid #0ea5e9" : "1px solid #fde68a",
                          fontWeight: current ? 700 : 400,
                        }}
                      >
                        {e.edition_year ?? "—"}
                        {e.is_latest_edition ? " ★" : ""}
                      </a>
                    );
                  })}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {history && history.length > 0 && (
        <div style={{ marginTop: "0.55rem" }}>
          <div style={{ fontSize: "0.72rem", fontWeight: 600, color: "#92400e", marginBottom: "0.3rem" }}>
            {t("cbs.history_label", "גרסאות קודמות")}:
          </div>
          <div className="flex" style={{ gap: "0.3rem", flexWrap: "wrap" }}>
            {history.map((h) => (
              <a
                key={h.url}
                href={h.url}
                target="_blank"
                rel="noopener noreferrer"
                title={h.title}
                className="badge"
                style={{
                  fontSize: "0.7rem",
                  background: "#fffbeb",
                  color: "#92400e",
                  border: "1px solid #fde68a",
                  textDecoration: "none",
                }}
              >
                {h.yearLabel}
              </a>
            ))}
          </div>
        </div>
      )}

      {r.subject_tags && r.subject_tags.length > 0 && (
        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.3rem", marginTop: "0.5rem" }}>
          {r.subject_tags.map((s) => (
            <span
              key={s}
              className="badge"
              style={{ fontSize: "0.7rem", background: "#f1f5f9", color: "#334155" }}
            >
              {s}
            </span>
          ))}
        </div>
      )}

      {fileCount > 0 && (
        <div style={{ marginTop: "0.55rem" }}>
          <div className="flex" style={{ gap: "0.35rem", flexWrap: "wrap", alignItems: "center" }}>
            {types.map((ext) => (
              <span
                key={ext}
                className="badge"
                title={ext.toUpperCase()}
                style={{ fontSize: "0.68rem", background: "#eef2ff", color: "#3730a3" }}
              >
                {fileIcon(ext)} {ext.toUpperCase()}
              </span>
            ))}
            <button
              type="button"
              onClick={() => setFilesOpen((v) => !v)}
              aria-expanded={filesOpen}
              className="btn-secondary"
              style={{ fontSize: "0.72rem", padding: "0.2rem 0.55rem", marginInlineStart: "auto" }}
            >
              {t("cbs.files", "קבצים")} ({fileCount}) {filesOpen ? "▴" : "▾"}
            </button>
          </div>

          {filesOpen && (
            <div style={{ marginTop: "0.45rem" }}>
              {r.file_links!.map((f, idx) => (
                <div
                  key={`${f.href}-${idx}`}
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    gap: "0.5rem",
                    padding: "0.35rem 0.55rem",
                    marginBottom: "0.25rem",
                    background: "var(--bg-secondary, #f8f9fa)",
                    borderRadius: "4px",
                    border: "1px solid var(--border, #e2e8f0)",
                  }}
                >
                  <a
                    href={f.href}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ fontSize: "0.82rem", color: "var(--primary)", wordBreak: "break-word" }}
                  >
                    {f.ext && <span style={{ marginInlineEnd: "0.3rem" }}>{fileIcon(f.ext)}</span>}
                    {f.label && f.label !== ">>>" ? f.label : f.href.split("/").pop()}
                    {f.ext && (
                      <span className="badge" style={{ marginInlineStart: "0.4rem", fontSize: "0.65rem" }}>
                        {f.ext.toUpperCase()}
                      </span>
                    )}
                  </a>
                  {f.size != null && (
                    <span className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>
                      {formatBytes(f.size)}
                    </span>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </article>
  );
}
