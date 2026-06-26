import { useState, useEffect, Suspense, lazy, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate, useParams, Link } from "react-router-dom";
import {
  versions as versionsApi,
  publicApi,
  admin as adminApi,
  formatBytes,
  Version,
  TrackedDataset,
} from "../api/client";
import { useAuth } from "../auth/AuthContext";
import { sourceBadgeFor } from "../utils/sourceBadge";

// Lazy so the Leaflet bundle is never pulled into the CKAN / scraper /
// idf code paths. Only govmap pages that actually have a GeoJSON
// resource load it.
const GovmapView = lazy(() => import("../components/GovmapView"));

const ODATA_BASE = "https://www.odata.org.il";

// Israeli date format DD.MM.YYYY (optionally with HH:MM). `toLocaleString()`
// renders the browser locale (e.g. US "6/21/2026, 8:12 PM"), which the team
// flagged as wrong — Israel uses day-first dotted dates.
function formatHebrewDate(value: string | null | undefined, withTime = true): string {
  if (!value) return "";
  const d = new Date(value);
  if (isNaN(d.getTime())) {
    // Non-ISO / partial strings (e.g. "local:<hash>") — show as-is.
    return value.slice(0, 19);
  }
  const p = (n: number) => String(n).padStart(2, "0");
  const date = `${p(d.getDate())}.${p(d.getMonth() + 1)}.${d.getFullYear()}`;
  return withTime ? `${date}, ${p(d.getHours())}:${p(d.getMinutes())}` : date;
}

// Downloadable files of a version, derived from resource_mappings. Returns the
// mapping KEY (what the backend /versions/{id}/download/{resource} endpoint
// looks up) plus a friendly label. Works for BOTH ODATA- and R2-backed
// versions — the backend redirects each to its real storage location.
//
// Label precedence: the version's `_names` map (the real resource title,
// captured from the source — see app/services/r2_backfill.repair_dataset_r2)
// wins over the generic per-key fallback. When several files share the same
// title (the source publishes many same-named resources with different
// content), a 1-based index is appended so each link is distinguishable.
function versionFiles(
  mappings: Record<string, unknown> | null | undefined,
  onlyKeys?: Set<string> | null,
): Array<{ name: string; index: number; label: string }> {
  if (!mappings) return [];
  const names = (mappings._names as Record<string, string> | undefined) || {};
  const skip = ["_hashes", "_resource_ids", "_appendonly_seen", "_names", "_filedates"];
  const include = (key: string) =>
    !skip.includes(key) && (!onlyKeys || onlyKeys.has(key));
  // Count how many DISPLAYED entries map to each friendly name, to decide when
  // to add a disambiguating (#n) suffix (only over the files actually shown).
  const nameCounts: Record<string, number> = {};
  for (const [key] of Object.entries(mappings)) {
    if (!include(key)) continue;
    const nm = names[key];
    if (nm) nameCounts[nm] = (nameCounts[nm] || 0) + 1;
  }
  const nameSeen: Record<string, number> = {};
  const out: Array<{ name: string; index: number; label: string }> = [];
  for (const [key, val] of Object.entries(mappings)) {
    if (!include(key)) continue;
    const friendly = names[key];
    let base: string;
    if (friendly) {
      // Append (#n) only when the same title appears more than once.
      if (nameCounts[friendly] > 1) {
        nameSeen[friendly] = (nameSeen[friendly] || 0) + 1;
        base = `${friendly} (${nameSeen[friendly]})`;
      } else {
        base = friendly;
      }
    } else {
      base =
        key === "_geojson"
          ? "GeoJSON"
          : key === "_zip" || key === "_zip_parts"
            ? "קבצים מצורפים (ZIP)"
            : key === "metadata"
              ? "מטא-דאטה"
              : key === "backfilled"
                ? "קובץ מאוחד (CSV)"
                : // The worker names the main tabular resource "נתוני הסורק"
                  // (worker.py resource_name default), which becomes the
                  // mapping key. As a download label that's opaque — it's the
                  // scraped records as a CSV table, so say so. Mapped here (not
                  // renamed in the backend) so existing versions' keys, which
                  // the download endpoint looks up, stay intact.
                  key === "נתוני הסורק"
                  ? "טבלת נתונים (CSV)"
                  : key;
    }
    if (Array.isArray(val)) {
      // List-valued (multi-part ZIP, multi-layer GeoJSON): one link per part,
      // each addressing its own element via the download endpoint's ?index=.
      const items = val.filter((x) => typeof x === "string" && x.length > 10);
      items.forEach((_, i) => {
        const label = items.length > 1 ? `${base} (חלק ${i + 1}/${items.length})` : base;
        out.push({ name: key, index: i, label });
      });
    } else if (typeof val === "string" && val.length > 10) {
      out.push({ name: key, index: 0, label: base });
    }
  }
  return out;
}

// The archive date (YYYY-MM-DD, UTC) a version was captured on. The ODATA
// resource names embed this same date, so it's the authoritative key for which
// files belong to a version (see `_filedates` / ownDateKeys below).
function versionDateUTC(detectedAt: string | null | undefined): string | null {
  if (!detectedAt) return null;
  const d = new Date(detectedAt);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

// Mapping KEYS that genuinely belong to THIS version, decided by the DATE in
// each file's source name (stored per-version as `_filedates`), NOT by the
// version number — carry-forward and content-hash recovery can attach files
// from other dates to a version's mapping, so date is the reliable signal.
// Keep only files whose `_filedates[key]` equals the version's own archive date.
function ownDateKeys(
  mappings: Record<string, unknown> | null | undefined,
  vDate: string | null,
): Set<string> | null {
  const fd = mappings?._filedates as Record<string, string> | undefined;
  if (!fd || !vDate) return null; // no date info → caller falls back
  const out = new Set<string>();
  for (const [k, d] of Object.entries(fd)) if (d === vDate) out.add(k);
  return out;
}

// Fallback for versions WITHOUT `_filedates` (e.g. new versions created by the
// forward poll): show keys whose value differs from the previous (older)
// version — the resources actually added/changed this version. The CKAN
// archiver carries unchanged resources forward verbatim (identical r2:<key>),
// so diffing against the previous version yields the 1–2 files that really
// changed. The oldest version (no previous) shows everything.
function changedKeys(
  curr: Record<string, unknown> | null | undefined,
  prev: Record<string, unknown> | null | undefined,
): Set<string> {
  const out = new Set<string>();
  if (!curr) return out;
  const norm = (x: unknown) => (Array.isArray(x) ? JSON.stringify(x) : x);
  for (const [k, val] of Object.entries(curr)) {
    if (["_hashes", "_resource_ids", "_appendonly_seen", "_names", "_filedates"].includes(k)) continue;
    if (!prev || norm(prev[k]) !== norm(val)) out.add(k);
  }
  return out;
}

// Build the backend download URL for one version file (same endpoint the
// per-file links use; it 302-redirects to the file's real storage).
function fileDownloadUrl(versionId: string, f: { name: string; index: number }): string {
  return (
    `/api/versions/${versionId}/download/${encodeURIComponent(f.name)}` +
    (f.index > 0 ? `?index=${f.index}` : "")
  );
}

// "Download all" — trigger each file's download in turn. We stagger the
// clicks (~500ms) so the browser treats them as one batch (it shows a
// single "allow multiple downloads" prompt) instead of dropping the
// rapid back-to-back navigations. Each file streams straight from its
// storage via the redirect, so this scales to large GeoJSON/ZIP parts
// without pulling bytes through the page.
function downloadAllFiles(
  versionId: string,
  files: Array<{ name: string; index: number }>,
): void {
  files.forEach((f, i) => {
    window.setTimeout(() => {
      const a = document.createElement("a");
      a.href = fileDownloadUrl(versionId, f);
      a.rel = "noopener";
      document.body.appendChild(a);
      a.click();
      a.remove();
    }, i * 500);
  });
}

export default function VersionsPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { user } = useAuth();
  const isAdmin = !!user?.is_admin;
  const { datasetId } = useParams<{ datasetId: string }>();
  const [versionsList, setVersionsList] = useState<Version[]>([]);
  const [dataset, setDataset] = useState<TrackedDataset | null>(null);
  const [loading, setLoading] = useState(true);
  const [deleting, setDeleting] = useState<string | null>(null);
  // Admin-only: version_id -> total_bytes for the size annotation. Null
  // means "not loaded" (still fetching or non-admin).
  const [versionSizes, setVersionSizes] = useState<Map<string, number> | null>(null);
  const [datasetTotalBytes, setDatasetTotalBytes] = useState<number | null>(null);

  useEffect(() => {
    if (!datasetId) return;
    Promise.all([
      versionsApi.list(datasetId),
      publicApi.datasets().then((all) => all.find((d) => d.id === datasetId) || null),
    ])
      .then(([versions, ds]) => {
        setVersionsList(versions);
        setDataset(ds);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [datasetId]);

  // Admin-only side-load for size annotations. Single endpoint returns
  // every active dataset; we pick the one matching this page. Fails open
  // (no annotation) on any error so the page still renders for admins
  // when the odata mirror is briefly unreachable.
  useEffect(() => {
    if (!isAdmin || !datasetId) return;
    adminApi
      .datasetSizes()
      .then((resp) => {
        const me = resp.datasets.find((d) => d.dataset_id === datasetId);
        if (!me) return;
        const m = new Map<string, number>();
        for (const v of me.versions) m.set(v.version_id, v.total_bytes);
        setVersionSizes(m);
        setDatasetTotalBytes(me.total_bytes);
      })
      .catch(() => {});
  }, [isAdmin, datasetId]);

  async function handleDeleteVersion(v: Version) {
    const label = `${t("versions.version")} ${v.version_number}`;
    if (!window.confirm(t("versions.delete_confirm", { label }))) return;
    setDeleting(v.id);
    try {
      await versionsApi.delete(v.id);
      setVersionsList((prev) => prev.filter((x) => x.id !== v.id));
    } catch (e: any) {
      alert(t("versions.delete_failed") + ": " + (e?.message || "unknown"));
    } finally {
      setDeleting(null);
    }
  }

  async function handleDeleteDataset() {
    if (!dataset) return;
    if (!window.confirm(t("tracked.delete_confirm", { title: dataset.title }))) return;
    try {
      const { datasets } = await import("../api/client");
      await datasets.untrack(dataset.id);
      navigate("/");
    } catch (e: any) {
      alert(t("tracked.delete_failed") + ": " + (e?.message || "unknown"));
    }
  }

  // For govmap datasets the worker uploads a GeoJSON resource and the
  // version index stores its odata id under resource_mappings._geojson
  // (a list — see app/api/worker.py push-version handler). Picking up
  // the FIRST id from the LATEST version covers the common case
  // (single-layer scrapes); multi-layer is rare and would need a
  // layer-selector dropdown we haven't built yet.
  const govmapGeojsonUrl = useMemo<string | null>(() => {
    if (!dataset || dataset.source_type !== "govmap") return null;
    if (versionsList.length === 0) return null;
    const latest = versionsList[0];
    const m = latest.resource_mappings as Record<string, unknown> | null;
    const ids = m?._geojson;
    let rid: string | null = null;
    if (Array.isArray(ids)) {
      rid = ids.find((x) => typeof x === "string" && x.length >= 30) ?? null;
    } else if (typeof ids === "string" && ids.length >= 30) {
      rid = ids;
    }
    if (!rid) return null;
    // R2-stored GeoJSON ("r2:<key>"): fetch via the backend download route,
    // which 302-redirects to the object store's public domain. This avoids
    // needing the R2 base URL on the client; downloadToBlob sniffs gzip by
    // magic bytes, so the stripped filename on the redirect target is fine.
    // (The object store must allow this app's origin via CORS for the
    // GovmapView fetch to read the body.)
    if (rid.startsWith("r2:")) {
      return `/api/versions/${latest.id}/download/_geojson`;
    }
    if (!dataset.odata_dataset_id) return null;
    return `${ODATA_BASE}/dataset/${dataset.odata_dataset_id}/resource/${rid}/download`;
  }, [dataset, versionsList]);

  // True when the dataset's latest version stores files on R2 (an "r2:<key>"
  // mapping). R2 versions have no ODATA mirror page, so the ODATA-archive CTAs
  // are hidden in favor of the per-version download links below.
  const latestIsR2 = useMemo<boolean>(() => {
    if (versionsList.length === 0) return false;
    const m = versionsList[0].resource_mappings as Record<string, unknown> | null;
    if (!m) return false;
    return Object.values(m).some(
      (val) =>
        (typeof val === "string" && val.startsWith("r2:")) ||
        (Array.isArray(val) &&
          val.some((x) => typeof x === "string" && x.startsWith("r2:"))),
    );
  }, [versionsList]);

  if (loading) return <div className="loading" role="status" aria-live="polite">{t("common.loading")}</div>;

  return (
    <div>
      <div className="page-header flex-between">
        <div>
          <h1 style={{ margin: 0 }}>
            {dataset?.title || t("versions.title")}
          </h1>
          {dataset && (
            <div className="text-sm text-muted" style={{ marginTop: "0.25rem" }}>
              {t("versions.title")}
              {" · "}
              {versionsList.length} {t("home.versions_count")}
              {isAdmin && datasetTotalBytes !== null && (
                <> · סך גודל הקבצים: {formatBytes(datasetTotalBytes)}</>
              )}
            </div>
          )}
        </div>
        <div className="flex" style={{ alignItems: "center", gap: "1rem" }}>
          {dataset?.odata_dataset_id && !latestIsR2 && (
            <a
              href={`${ODATA_BASE}/dataset/${dataset.odata_dataset_id}`}
              target="_blank"
              rel="noopener noreferrer"
              // Promoted to a filled primary button — this is the
              // canonical "see the actual files" CTA on the page and
              // was previously hiding as a faint underline. The
              // explanation card below the header now spells out
              // exactly what the user will find on the other side.
              style={{
                fontSize: "0.85rem",
                padding: "0.4rem 0.9rem",
                background: "var(--primary, #0f766e)",
                color: "white",
                border: "none",
                borderRadius: 4,
                textDecoration: "none",
                fontWeight: 500,
              }}
            >
              {t("tracked.open_archive")} &#8599;
            </a>
          )}
          {dataset && (() => {
            const sourceHref =
              dataset.source_type === "scraper" || dataset.source_type === "govmap"
                ? (dataset.source_url || "#")
                : (dataset.source_url || `https://data.gov.il/he/datasets/${dataset.organization}/${dataset.ckan_name}`);
            const linkLabel = t(sourceBadgeFor(
              dataset.source_type,
              dataset.organization,
              dataset.ckan_id,
            ).sourceLinkKey);
            return (
              <a
                href={sourceHref}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  textDecoration: "none",
                  fontSize: "0.85rem",
                  color: "var(--text-muted)",
                }}
              >
                {linkLabel} &#8599;
              </a>
            );
          })()}
          {isAdmin && (
            <button
              type="button"
              onClick={handleDeleteDataset}
              className="btn-danger"
              style={{
                fontSize: "0.8rem",
                padding: "0.3rem 0.7rem",
                background: "none",
                border: "1px solid var(--danger, #dc2626)",
                color: "var(--danger, #dc2626)",
                borderRadius: 4,
                cursor: "pointer",
              }}
              title={t("tracked.delete_dataset")}
            >
              {t("tracked.delete_dataset")}
            </button>
          )}
          <Link
            to="/"
            style={{
              textDecoration: "none",
              fontSize: "0.85rem",
              color: "var(--text-muted)",
            }}
          >
            &larr; {t("common.back")}
          </Link>
        </div>
      </div>

      {/* Archive explanation card. Visible only when the dataset has
          an ODATA mirror (almost always, but defensive). Surfaces the
          big "ארכיון הקבצים" CTA again — in case the user scrolled
          past the header — and a per-source-type explanation of what
          the user will find on the other side, so the ODATA page
          stops being a confusing wall of resources. */}
      {dataset?.odata_dataset_id && !latestIsR2 && (
        <ArchiveExplanation
          dataset={dataset}
          odataUrl={`${ODATA_BASE}/dataset/${dataset.odata_dataset_id}`}
          t={t}
        />
      )}

      {govmapGeojsonUrl && (
        <Suspense
          fallback={
            <div
              className="card"
              role="status"
              style={{
                height: 500,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                color: "var(--text-muted)",
                marginBottom: "1.5rem",
              }}
            >
              {t("common.loading")}
            </div>
          }
        >
          <GovmapView geojsonDownloadUrl={govmapGeojsonUrl} />
        </Suspense>
      )}

      {versionsList.length === 0 ? (
        <div className="empty-state">{t("versions.no_versions")}</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }} role="list" aria-label={t("versions.title")}>
          {versionsList.map((v, idx) => {
            const summary = v.change_summary;
            // versionsList is newest-first, so the previous (older) version is
            // the next item. Used to show only this version's changed files.
            const olderVersion = versionsList[idx + 1];

            return (
              <div
                key={v.id}
                className="card"
                role="listitem"
              >
                <div className="flex-between">
                  <div className="flex">
                    <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                      {t("versions.version")} {v.version_number}
                    </h2>
                    <span className="text-sm text-muted">
                      {t("versions.detected")}: {formatHebrewDate(v.detected_at)}
                    </span>
                    {isAdmin && versionSizes && (
                      <span className="text-sm text-muted" title="גודל קבצי הגרסה (אדמין בלבד)">
                        · גודל: {formatBytes(versionSizes.get(v.id))}
                      </span>
                    )}
                  </div>
                  <div className="flex" style={{ alignItems: "center", gap: "0.75rem" }}>
                    <span className="text-sm text-muted">
                      {formatHebrewDate(v.metadata_modified)}
                    </span>
                    {isAdmin && (
                      <button
                        type="button"
                        onClick={() => handleDeleteVersion(v)}
                        disabled={deleting === v.id}
                        style={{
                          fontSize: "0.75rem",
                          padding: "0.2rem 0.55rem",
                          background: "none",
                          border: "1px solid var(--danger, #dc2626)",
                          color: "var(--danger, #dc2626)",
                          borderRadius: 4,
                          cursor: deleting === v.id ? "not-allowed" : "pointer",
                          opacity: deleting === v.id ? 0.6 : 1,
                        }}
                        title={t("versions.delete_version")}
                      >
                        {deleting === v.id
                          ? t("versions.deleting")
                          : t("versions.delete_version")}
                      </button>
                    )}
                  </div>
                </div>

                {summary && summary.type === "large_dataset" ? (
                  <div className="flex mt-1" style={{ gap: "0.5rem", flexWrap: "wrap" }}>
                    <span className="badge badge-info">{t("versions.large_dataset")}</span>
                    <span className="text-sm">
                      {summary.record_count?.toLocaleString()} {t("versions.rows")}
                      {summary.delta != null && summary.delta !== summary.record_count && (
                        <span style={{ color: summary.delta >= 0 ? "var(--success)" : "var(--danger)", marginInlineStart: "0.3rem" }}>
                          ({summary.delta >= 0 ? "+" : ""}{summary.delta.toLocaleString()})
                        </span>
                      )}
                    </span>
                  </div>
                ) : summary && (
                  <div className="flex mt-1">
                    {(summary.resources_modified?.length ?? 0) > 0 && (
                      <span className="badge badge-warning">
                        {summary.resources_modified!.length} {t("versions.resources_modified")}
                      </span>
                    )}
                    {(summary.resources_added?.length ?? 0) > 0 && (
                      <span className="badge badge-success">
                        {summary.resources_added!.length} {t("versions.resources_added")}
                      </span>
                    )}
                    {(summary.resources_removed?.length ?? 0) > 0 && (
                      <span className="badge badge-danger">
                        {summary.resources_removed!.length} {t("versions.resources_removed")}
                      </span>
                    )}
                  </div>
                )}

                {/* Per-version file downloads. Each link hits the backend
                    download endpoint, which 302-redirects to the file's real
                    storage (R2 or ODATA) — so this works for every version
                    regardless of where its bytes live. */}
                {(() => {
                  // Authoritative: keep only files whose source-name date matches
                  // this version's archive date (_filedates). Falls back to the
                  // changed-vs-previous diff for versions without date info, and
                  // to the full set for the oldest version.
                  const mappings = v.resource_mappings as Record<string, unknown> | null;
                  let keyset: Set<string> | null = ownDateKeys(mappings, versionDateUTC(v.detected_at));
                  if (!keyset && olderVersion) {
                    keyset = changedKeys(
                      mappings,
                      (olderVersion.resource_mappings as Record<string, unknown> | null) ?? null,
                    );
                  }
                  const files = versionFiles(v.resource_mappings, keyset);
                  if (files.length === 0) return null;
                  return (
                    <div
                      className="mt-1 flex"
                      style={{ gap: "1rem", flexWrap: "wrap", alignItems: "center" }}
                    >
                      {files.length > 1 && (
                        <button
                          type="button"
                          onClick={() => downloadAllFiles(v.id, files)}
                          className="text-sm"
                          title="הורדת כל הקבצים בגרסה זו"
                          style={{
                            color: "var(--primary)",
                            background: "none",
                            border: "1px solid var(--primary)",
                            borderRadius: "4px",
                            padding: "0.15rem 0.6rem",
                            cursor: "pointer",
                            fontWeight: 600,
                          }}
                        >
                          &#8595; הורד הכל ({files.length})
                        </button>
                      )}
                      {files.map((f) => (
                        <a
                          key={`${f.name}-${f.index}`}
                          href={fileDownloadUrl(v.id, f)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-sm"
                          style={{ color: "var(--primary)", textDecoration: "none" }}
                        >
                          &#8595; {f.label}
                        </a>
                      ))}
                    </div>
                  );
                })()}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

/**
 * Per-source-type "what's in the archive" card.
 *
 * Picks the explanation paragraph based on:
 *   - GovMap layers → GeoJSON + matching CSV
 *   - Scraper sources (gov.il / IDF) → ZIP of documents + CSV index
 *   - CKAN data.gov.il datasets → original files with their original names
 *   - Anything else → a generic explanation
 *
 * The IDF detection mirrors sourceBadgeFor's logic (ckan_id starts
 * with "idf-scraper-" or organization is one of the IDF synonyms) so
 * the IDF copy fires in the same cases the IDF badge does.
 */
function ArchiveExplanation(props: {
  dataset: TrackedDataset;
  odataUrl: string;
  t: (k: string, opts?: Record<string, unknown>) => string;
}) {
  const { dataset, odataUrl, t } = props;
  const isIdf =
    (dataset.ckan_id || "").startsWith("idf-scraper-") ||
    ["idf.il", "israel_defense_forces", "idf"].includes(
      dataset.organization || "",
    );
  let explainKey = "versions.archive_explain_default";
  if (dataset.source_type === "govmap") {
    explainKey = "versions.archive_explain_govmap";
  } else if (dataset.source_type === "scraper") {
    explainKey = isIdf
      ? "versions.archive_explain_idf"
      : "versions.archive_explain_scraper";
  } else if (
    dataset.source_type === "ckan" ||
    dataset.source_type === undefined ||
    dataset.source_type === ""
  ) {
    explainKey = "versions.archive_explain_ckan";
  }
  return (
    <section
      className="card"
      aria-label={t("versions.archive_section_title")}
      style={{
        marginBottom: "1.5rem",
        padding: "1rem 1.25rem",
        background: "var(--bg-muted, #f8fafc)",
        borderInlineStart: "3px solid var(--primary, #0f766e)",
      }}
    >
      <div
        className="flex-between"
        style={{
          alignItems: "flex-start",
          gap: "1rem",
          flexWrap: "wrap",
          marginBottom: "0.5rem",
        }}
      >
        <h2 style={{ margin: 0, fontSize: "1.05rem", fontWeight: 600 }}>
          {t("versions.archive_section_title")}
        </h2>
        <a
          href={odataUrl}
          target="_blank"
          rel="noopener noreferrer"
          // Big visible CTA — repeats the header button for users who
          // landed on this card directly (anchor link, scroll position).
          style={{
            fontSize: "0.9rem",
            padding: "0.5rem 1rem",
            background: "var(--primary, #0f766e)",
            color: "white",
            border: "none",
            borderRadius: 4,
            textDecoration: "none",
            fontWeight: 600,
            whiteSpace: "nowrap",
          }}
        >
          {t("tracked.open_archive")} &#8599;
        </a>
      </div>
      <p
        style={{
          margin: "0 0 0.6rem 0",
          fontSize: "0.9rem",
          lineHeight: 1.5,
          color: "var(--text)",
        }}
      >
        {t("versions.archive_section_intro")}
      </p>
      <p
        style={{
          margin: 0,
          fontSize: "0.9rem",
          lineHeight: 1.5,
          color: "var(--text-muted)",
        }}
      >
        {t(explainKey)}
      </p>
    </section>
  );
}
