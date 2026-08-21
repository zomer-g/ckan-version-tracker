import { useState, FormEvent, useMemo, useEffect, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { publicApi, CkanCoverage, SourceFile } from "../api/client";
import SourceFilePicker from "./SourceFilePicker";

export interface ResourceOption {
  id: string;
  name?: string;
  format?: string;
}

interface RequestFormProps {
  ckanId?: string;
  resourceId?: string;  // legacy: pre-pin a single resource
  // For CKAN datasets, the parent passes the full resource list so the
  // user can pick which files to track. If `resourceId` is also set we
  // start with that one pre-checked; otherwise nothing is checked and
  // the user must select at least one before submit is enabled.
  availableResources?: ResourceOption[];
  datasetTitle: string;
  onClose: () => void;
  // Scraper mode
  sourceType?: "ckan" | "scraper";
  sourceUrl?: string;
  // Pre-select a refresh cadence. Worker-declared sources pass the one from
  // their manifest, since the site's own publishing rhythm is known there and
  // not to the person pasting the URL.
  defaultInterval?: number;
  // The pasted page publishes several files and the server can list them
  // (validate's `file_picker`). Renders the scraper-mode file picker, whose
  // ticks travel as `selected_files`. Nothing else about the form changes.
  filePicker?: boolean;
}

const INTERVAL_OPTIONS = [
  // 300s is settings.min_poll_interval — the floor OVER accepts, and what a
  // one-request feed (telegram's newest-messages page) declares. Without it
  // here the nearest-match snap below rounded such a manifest up to 15
  // minutes, so a declared cadence could never actually be offered.
  { value: 300, labelHe: "5 דקות", labelEn: "5 minutes" },
  { value: 900, labelHe: "15 דקות", labelEn: "15 minutes" },
  { value: 3600, labelHe: "שעה", labelEn: "1 hour" },
  { value: 43200, labelHe: "12 שעות", labelEn: "12 hours" },
  { value: 86400, labelHe: "יום", labelEn: "1 day" },
  { value: 604800, labelHe: "שבוע", labelEn: "1 week" },
  { value: 2592000, labelHe: "חודש", labelEn: "1 month" },
  { value: 7776000, labelHe: "רבעון", labelEn: "3 months" },
];

export default function RequestForm({
  ckanId,
  resourceId,
  availableResources,
  datasetTitle,
  onClose,
  sourceType = "ckan",
  sourceUrl,
  defaultInterval,
  filePicker = false,
}: RequestFormProps) {
  const { t, i18n } = useTranslation();
  // The single editable text field now NAMES THE DATASET (sent as `title`
  // for scraper requests). Prefilled with the auto-derived title so the
  // user can keep it or refine it.
  const [datasetName, setDatasetName] = useState(datasetTitle);
  const [notes, setNotes] = useState("");
  // Default to a quarterly refresh; the user reveals the picker only if
  // they want it more often. 7776000s = 3 months = the last INTERVAL option.
  // Snap to an offered option — a manifest may declare a cadence that isn't
  // one of these (e.g. 21600s), and an unlisted value renders as a blank
  // select. Nearest match keeps the intent without a broken control.
  const [interval, setInterval] = useState(() =>
    defaultInterval
      ? INTERVAL_OPTIONS.reduce((best, opt) =>
          Math.abs(opt.value - defaultInterval) < Math.abs(best.value - defaultInterval)
            ? opt
            : best,
        ).value
      : 7776000,
  );
  const [showFreq, setShowFreq] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState("");

  // If the parent swaps in a different dataset while the form is mounted,
  // refresh the prefilled name to match (mirrors the resource-picker reset).
  useEffect(() => {
    setDatasetName(datasetTitle);
  }, [datasetTitle]);

  // Resource picker state — only relevant for CKAN datasets that have
  // an `availableResources` list. We initialise with whatever the
  // parent pre-selected (single resource via URL) and let the user
  // adjust before submitting.
  const initialSelected = useMemo(() => {
    if (resourceId) return new Set([resourceId]);
    // If the dataset has exactly one resource, default-select it so the
    // user doesn't have to tick a box just to submit.
    if (availableResources && availableResources.length === 1) {
      return new Set([availableResources[0].id]);
    }
    return new Set<string>();
  }, [resourceId, availableResources]);

  const [selectedResources, setSelectedResources] = useState<Set<string>>(initialSelected);

  // A CKAN package is a folder, not a table — its CSVs are usually unrelated
  // tables on their own publishing rhythms. Default to one dataset per file so
  // each gets its own update frequency, versions page and SQL table; the user
  // can uncheck to get the old single combined dataset.
  const [splitResources, setSplitResources] = useState(true);
  // Per-file outcome of a split submit (some files may already be tracked).
  const [splitResults, setSplitResults] = useState<
    Array<{ resource_id: string; name: string; status: string }> | null
  >(null);

  // Keep the selection in sync if the parent swaps in a different
  // dataset while the form is mounted (rare but happens when the user
  // changes their mind without closing the form).
  useEffect(() => {
    setSelectedResources(initialSelected);
  }, [initialSelected]);

  const showResourcePicker =
    sourceType === "ckan" && Array.isArray(availableResources) && availableResources.length > 0;

  // What of this collection the site already holds. Fetched rather than
  // inferred: the answer is per FILE, and a collection is routinely half
  // archived — 20 of the 22 files on a package can be frozen 2019 copies
  // nobody tracks. Without it the picker submits blind and the server
  // answers "already tracked" about a choice the reader could not have
  // avoided making.
  const [coverage, setCoverage] = useState<CkanCoverage | null>(null);
  useEffect(() => {
    if (!showResourcePicker || !ckanId) return;
    let cancelled = false;
    publicApi
      .ckanCoverage(ckanId)
      .then((c) => {
        if (!cancelled) setCoverage(c);
      })
      // Best-effort: without it the picker is exactly what it was before,
      // and the server still refuses duplicates. Never block the request.
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [showResourcePicker, ckanId]);

  const coverageById = useMemo(() => {
    const map = new Map<string, CkanCoverage["resources"][number]>();
    for (const r of coverage?.resources || []) map.set(r.id, r);
    return map;
  }, [coverage]);

  const isSelectable = useCallback(
    (rid: string) => coverageById.get(rid)?.selectable ?? true,
    [coverageById],
  );

  // A file that arrived pre-ticked (?resource= in the pasted URL, or a
  // single-resource package) but turns out to be archived already must not
  // stay ticked — submitting it is the dead end this is here to remove.
  useEffect(() => {
    if (coverageById.size === 0) return;
    setSelectedResources((prev) => {
      const next = new Set([...prev].filter((rid) => isSelectable(rid)));
      return next.size === prev.size ? prev : next;
    });
  }, [coverageById, isSelectable]);

  // Scraper-mode file picker (a page publishing many files — see
  // SourceFilePicker). Kept separate from the CKAN resource picker above: that
  // one picks CKAN resource IDS out of a package the browser already has,
  // this one picks FILE PATHS out of a page the server has to go and read.
  const showFilePicker = sourceType === "scraper" && filePicker && !!sourceUrl;
  const [selectedFiles, setSelectedFiles] = useState<Set<string>>(new Set());
  const [pageFileCount, setPageFileCount] = useState<number | null>(null);
  // path → absolute URL, so a split submit can send URLs (which the registry
  // classifies on its own) while the ticks stay keyed by path.
  const [fileUrls, setFileUrls] = useState<Record<string, string>>({});
  // url → the label shown in the picker, so a split submit can name each
  // dataset what the site calls the table rather than after its filename.
  const [fileTitles, setFileTitles] = useState<Record<string, string>>({});
  const onFilesLoaded = useCallback((files: SourceFile[]) => {
    setPageFileCount(files.length);
    setFileUrls(Object.fromEntries(files.map((f) => [f.path, f.url])));
    setFileTitles(Object.fromEntries(files.map((f) => [f.url, f.title])));
  }, []);

  // A page of files is a folder of unrelated tables, so one dataset per file is
  // the default here — the same reasoning as splitting a CKAN package, and the
  // only shape in which a big publication fits into SQL at all.
  const [splitFiles, setSplitFiles] = useState(true);
  const [fileResults, setFileResults] = useState<
    Array<{ url: string; status: string; name?: string }> | null
  >(null);

  const formId = sourceType === "scraper" ? (sourceUrl || "scraper") : (ckanId || "form");

  const toggleResource = (rid: string) => {
    setSelectedResources((prev) => {
      const next = new Set(prev);
      if (next.has(rid)) next.delete(rid);
      else next.add(rid);
      return next;
    });
  };

  // WCAG 3.3.6 (Error Prevention, All) asks that a submission be reversible,
  // checked, or confirmed. A tracking request is none of those once it lands in
  // the admin queue, so it gets the third: the user sees exactly what is about
  // to be sent and confirms it.
  const [reviewing, setReviewing] = useState(false);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!reviewing) {
      setReviewing(true);
      return;
    }
    setReviewing(false);
    setSubmitting(true);
    setError("");
    try {
      const trimmedName = datasetName.trim();
      if (sourceType === "scraper" && sourceUrl) {
        // A page with files to pick and nothing picked would silently fall
        // back to "everything the page lists" — which is a reasonable default
        // but not what someone who just unticked everything meant.
        if (showFilePicker && pageFileCount !== null && pageFileCount > 0
            && selectedFiles.size === 0) {
          setError(t("home.request_pick_files"));
          setSubmitting(false);
          return;
        }
        const picked = Array.from(selectedFiles);
        const res = await publicApi.requestScraper({
          source_url: sourceUrl,
          title: trimmedName || datasetTitle,
          preferred_interval: interval,
          requester_notes: notes || undefined,
          // Splitting sends URLs, because each one becomes a dataset the
          // registry has to classify by itself; otherwise paths, which the
          // page-level engine matches against its own listing.
          selected_files: picked.length
            ? (showFilePicker && splitFiles
                ? picked.map((p) => fileUrls[p]).filter(Boolean)
                : picked)
            : undefined,
          split_files: showFilePicker && splitFiles ? true : undefined,
          file_titles: showFilePicker && splitFiles ? fileTitles : undefined,
        });
        if (res?.results) {
          // Nothing new opened because every pick was already tracked. That is
          // an answer, not a failure — but showing the success banner would be
          // a lie about what just happened.
          if (!res.created) {
            setError(t("home.split_files_none"));
            setSubmitting(false);
            return;
          }
          setFileResults(res.results);
        }
      } else {
        const ids = Array.from(selectedResources);
        if (showResourcePicker && ids.length === 0) {
          setError(t("home.request_pick_files"));
          setSubmitting(false);
          return;
        }
        const res = await publicApi.request({
          ckan_id: ckanId!,
          resource_id: resourceId,
          resource_ids: ids.length > 0 ? ids : undefined,
          split_resources: showResourcePicker && splitResources ? true : undefined,
          preferred_interval: interval,
          // CKAN keeps its own authoritative title; pass a user-supplied
          // name only when it differs from the prefill, so the admin sees
          // the requester's preferred label.
          requester_name:
            trimmedName && trimmedName !== datasetTitle ? trimmedName : undefined,
          requester_notes: notes || undefined,
        });
        if (res?.results) {
          // Every file already tracked → nothing was created. Name the dataset
          // that holds them; "already tracked" with no pointer is a dead end.
          if (!res.created) {
            const holder = res.results.find((r) => r.dataset_title)?.dataset_title;
            setError(
              holder
                ? t("home.request_split_none_named", { title: holder })
                : t("home.request_split_none"),
            );
            setSubmitting(false);
            return;
          }
          setSplitResults(res.results);
        }
      }
      setSuccess(true);
    } catch (err: any) {
      setError(err.message || t("common.error"));
    }
    setSubmitting(false);
  };

  if (success) {
    return (
      <div
        className="card"
        style={{
          background: "var(--tint-good-bg)",
          border: "1px solid var(--tint-good-bd)",
          padding: "1.25rem",
          marginTop: "0.75rem",
        }}
        role="status"
        aria-live="polite"
      >
        <p style={{ color: "var(--success)", fontWeight: 500, margin: 0 }}>
          {fileResults
            ? t("home.split_files_success", {
                n: fileResults.filter((r) => r.status === "pending").length,
              })
            : splitResults
              ? t("home.request_split_success", {
                  n: splitResults.filter((r) => r.status === "pending").length,
                })
              : t("home.request_success")}
        </p>
        {fileResults && fileResults.some((r) => r.status !== "pending") && (
          <p className="text-sm" style={{ color: "var(--success)", margin: "0.5rem 0 0 0" }}>
            {t("home.request_split_skipped", {
              n: fileResults.filter((r) => r.status !== "pending").length,
            })}
          </p>
        )}
        {splitResults && splitResults.some((r) => r.status === "duplicate") && (
          <p className="text-sm" style={{ color: "var(--success)", margin: "0.5rem 0 0 0" }}>
            {t("home.request_split_skipped", {
              n: splitResults.filter((r) => r.status === "duplicate").length,
            })}
          </p>
        )}
        <button
          onClick={onClose}
          className="btn-secondary"
          style={{ marginTop: "0.75rem", fontSize: "0.85rem" }}
        >
          {t("common.back")}
        </button>
      </div>
    );
  }

  return (
    <form
      onSubmit={handleSubmit}
      className="card"
      style={{
        marginTop: "0.75rem",
        padding: "1.25rem",
        border: "1px solid var(--primary-100)",
        background: "var(--primary-50)",
      }}
      aria-label={t("home.request_title")}
    >
      <div className="flex-between mb-1">
        <h3 style={{ fontSize: "1rem", fontWeight: 600, margin: 0, color: "var(--primary)" }}>
          {t("home.request_title")}
        </h3>
        <button
          type="button"
          onClick={onClose}
          style={{
            background: "none",
            border: "none",
            fontSize: "1.2rem",
            cursor: "pointer",
            padding: "0.25rem",
            color: "var(--text-muted)",
            lineHeight: 1,
          }}
          aria-label={t("common.back")}
        >
          &times;
        </button>
      </div>

      <p className="text-sm text-muted mb-1" style={{ margin: "0 0 0.75rem 0" }}>
        {datasetTitle}
      </p>

      {error && (
        <div
          id={`req-error-${formId}`}
          role="alert"
          className="badge badge-danger mb-1"
          style={{ display: "block" }}
        >
          {error}
        </div>
      )}

      <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
        {showFilePicker && (
          <>
            <SourceFilePicker
              sourceUrl={sourceUrl!}
              selected={selectedFiles}
              onChange={setSelectedFiles}
              onLoaded={onFilesLoaded}
            />
            <label
              style={{
                display: "flex",
                alignItems: "flex-start",
                gap: "0.4rem",
                fontSize: "0.85rem",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={splitFiles}
                onChange={(e) => setSplitFiles(e.target.checked)}
                style={{ marginTop: "0.2rem" }}
              />
              <span>
                <span style={{ fontWeight: 600 }}>{t("home.split_files_label")}</span>
                <span className="text-muted" style={{ display: "block" }}>
                  {splitFiles
                    ? t("home.split_files_hint", { n: selectedFiles.size })
                    : t("home.split_files_off_hint")}
                </span>
              </span>
            </label>
          </>
        )}
        {showResourcePicker && (
          <div
            style={{
              padding: "0.7rem 0.8rem",
              border: "1px solid var(--primary-100)",
              borderRadius: "var(--radius)",
              background: "var(--surface)",
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                gap: "0.5rem",
                marginBottom: "0.5rem",
              }}
            >
              <div className="text-sm" style={{ fontWeight: 600 }}>
                {t("home.request_pick_files_label")}
                <span style={{ color: "var(--danger)", marginInlineStart: "0.25rem" }}>*</span>
              </div>
              <div style={{ display: "flex", gap: "0.25rem" }}>
                <button
                  type="button"
                  onClick={() =>
                    setSelectedResources(
                      new Set(
                        availableResources!
                          .map((r) => r.id)
                          .filter((rid) => isSelectable(rid)),
                      ),
                    )
                  }
                  style={{
                    background: "none",
                    border: "1px solid var(--border)",
                    borderRadius: "4px",
                    padding: "0.15rem 0.5rem",
                    fontSize: "0.7rem",
                    cursor: "pointer",
                    color: "var(--text-muted)",
                  }}
                >
                  {t("home.request_select_all")}
                </button>
                <button
                  type="button"
                  onClick={() => setSelectedResources(new Set())}
                  style={{
                    background: "none",
                    border: "1px solid var(--border)",
                    borderRadius: "4px",
                    padding: "0.15rem 0.5rem",
                    fontSize: "0.7rem",
                    cursor: "pointer",
                    color: "var(--text-muted)",
                  }}
                >
                  {t("home.request_clear")}
                </button>
              </div>
            </div>
            <div
              style={{
                maxHeight: "20rem",
                overflowY: "auto",
                border: "1px solid var(--border)",
                borderRadius: "var(--radius)",
                background: "var(--surface-2)",
              }}
            >
              {availableResources!.map((res, idx) => {
                const checked = selectedResources.has(res.id);
                const cov = coverageById.get(res.id);
                // "Held" means a dataset already owns this file — an archive
                // to link to, or a request awaiting approval. Either way it is
                // not something to ask for again.
                const held = cov ? !cov.selectable : false;
                return (
                  <label
                    key={res.id}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "0.6rem",
                      padding: "0.55rem 0.7rem",
                      borderBottom:
                        idx < availableResources!.length - 1 ? "1px solid var(--border)" : "none",
                      cursor: held ? "default" : "pointer",
                      fontSize: "0.9rem",
                      background: checked
                        ? "var(--primary-50)"
                        : held
                          ? "var(--surface-2)"
                          : "transparent",
                      opacity: held ? 0.72 : 1,
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={held}
                      onChange={() => toggleResource(res.id)}
                      style={{ width: "1rem", height: "1rem", flexShrink: 0 }}
                    />
                    <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {res.name || res.id}
                      {/* The date is what separates the file that still
                          updates from the frozen copies a publisher leaves
                          behind — routinely most of the list. */}
                      {cov?.last_modified && (
                        <span className="text-muted" style={{ fontSize: "0.75rem", marginInlineStart: "0.4rem" }}>
                          {cov.last_modified.slice(0, 10)}
                        </span>
                      )}
                    </span>
                    {cov && cov.state !== "free" && (
                      cov.state === "collected" && cov.dataset_id ? (
                        <a
                          href={`/versions/${cov.dataset_id}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          onClick={(e) => e.stopPropagation()}
                          title={cov.dataset_title}
                          className="badge"
                          style={{
                            fontSize: "0.7rem",
                            flexShrink: 0,
                            background: "var(--tint-good-bg)",
                            color: "var(--success)",
                            textDecoration: "none",
                          }}
                        >
                          {t("home.picker_tracked")} &#8599;
                        <span className="sr-only"> (נפתח בחלון חדש)</span></a>
                      ) : (
                        <span
                          className="badge"
                          style={{
                            fontSize: "0.7rem",
                            flexShrink: 0,
                            background: "var(--tint-warn-bg)",
                            color: "var(--warning)",
                          }}
                        >
                          {t("home.picker_pending")}
                        </span>
                      )
                    )}
                    {res.format && (
                      <span className="badge" style={{ fontSize: "0.7rem", flexShrink: 0 }}>
                        {res.format}
                      </span>
                    )}
                  </label>
                );
              })}
            </div>
            <div className="text-sm text-muted" style={{ marginTop: "0.4rem" }}>
              {selectedResources.size === 0
                ? t("home.request_pick_at_least_one")
                : t("home.request_files_selected", {
                    n: selectedResources.size,
                    total: availableResources!.length,
                  })}
              {coverage && coverage.collected + coverage.pending > 0 && (
                <span>
                  {" · "}
                  {t("home.picker_tracked_count", {
                    n: coverage.collected + coverage.pending,
                  })}
                </span>
              )}
            </div>

            {/* One dataset per file, or one dataset for all of them. */}
            <label
              style={{
                display: "flex",
                alignItems: "flex-start",
                gap: "0.5rem",
                marginTop: "0.6rem",
                paddingTop: "0.6rem",
                borderTop: "1px solid var(--border)",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={splitResources}
                onChange={(e) => setSplitResources(e.target.checked)}
                style={{ width: "1rem", height: "1rem", flexShrink: 0, marginTop: "0.15rem" }}
              />
              <span>
                <span className="text-sm" style={{ fontWeight: 500 }}>
                  {t("home.request_split_label")}
                </span>
                <span className="text-sm text-muted" style={{ display: "block" }}>
                  {!splitResources
                    ? t("home.request_split_off_hint")
                    : selectedResources.size > 0
                      ? t("home.request_split_hint", { n: selectedResources.size })
                      : t("home.request_split_hint_empty")}
                </span>
              </span>
            </label>
          </div>
        )}

        <div>
          <label htmlFor={`req-name-${formId}`} className="text-sm" style={{ fontWeight: 500 }}>
            {t("home.request_name")}
          </label>
          <input
            id={`req-name-${formId}`}
            type="text"
            value={datasetName}
            onChange={(e) => setDatasetName(e.target.value)}
            placeholder={t("home.request_name")}
            aria-invalid={error ? true : undefined}
            aria-describedby={error ? `req-error-${formId}` : undefined}
          />
        </div>

        <div>
          <label htmlFor={`req-notes-${formId}`} className="text-sm" style={{ fontWeight: 500 }}>
            {t("home.request_notes")}
          </label>
          <textarea
            id={`req-notes-${formId}`}
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            placeholder={t("home.request_notes")}
            rows={3}
            style={{
              width: "100%",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius)",
              padding: "0.5rem 0.75rem",
              fontSize: "0.875rem",
              fontFamily: "inherit",
              resize: "vertical",
            }}
          />
        </div>

        {/* Update frequency — quarterly by default, revealed on demand. */}
        <div>
          {!showFreq ? (
            <div
              className="text-sm text-muted"
              style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: "0.4rem" }}
            >
              <span>{t("home.request_freq_default")}</span>
              <button
                type="button"
                onClick={() => setShowFreq(true)}
                style={{
                  background: "none",
                  border: "none",
                  padding: 0,
                  color: "var(--primary)",
                  cursor: "pointer",
                  textDecoration: "underline",
                  fontSize: "0.85rem",
                }}
              >
                {t("home.request_freq_more")}
              </button>
            </div>
          ) : (
            <>
              <label htmlFor={`req-interval-${formId}`} className="text-sm" style={{ fontWeight: 500 }}>
                {t("home.request_interval")}
              </label>
              <select
                id={`req-interval-${formId}`}
                value={interval}
                onChange={(e) => setInterval(Number(e.target.value))}
              >
                {INTERVAL_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {i18n.language === "he" ? opt.labelHe : opt.labelEn}
                  </option>
                ))}
              </select>
            </>
          )}
        </div>

        {reviewing && (
          <div
            role="group"
            aria-label="אישור הבקשה"
            style={{
              background: "var(--tint-info-bg)",
              color: "var(--tint-info-fg)",
              border: "1px solid var(--tint-info-bd)",
              borderRadius: "var(--radius)",
              padding: "0.85rem 1rem",
              fontSize: "0.9rem",
              lineHeight: 1.7,
            }}
          >
            <strong style={{ display: "block", marginBottom: "0.35rem" }}>
              זה מה שיישלח:
            </strong>
            <ul style={{ margin: 0, paddingInlineStart: "1.2rem" }}>
              <li>מאגר: {datasetName.trim() || datasetTitle}</li>
              <li>
                תדירות בדיקה:{" "}
                {(() => {
                  const opt = INTERVAL_OPTIONS.find((o) => o.value === interval);
                  return opt ? (i18n.language === "he" ? opt.labelHe : opt.labelEn) : interval;
                })()}
              </li>
              {notes.trim() && <li>הערות: {notes.trim()}</li>}
              {showFilePicker && selectedFiles.size > 0 && (
                <li>{selectedFiles.size} קבצים נבחרו</li>
              )}
            </ul>
          </div>
        )}

        <div style={{ display: "flex", gap: "0.5rem", alignSelf: "flex-start", flexWrap: "wrap" }}>
          <button
            type="submit"
            className="btn-primary"
            disabled={submitting || (showResourcePicker && selectedResources.size === 0)}
          >
            {submitting
              ? t("common.loading")
              : reviewing
                ? "אישור ושליחה"
                : t("home.request_submit")}
          </button>
          {reviewing && (
            <button type="button" className="btn-secondary" onClick={() => setReviewing(false)}>
              חזרה לעריכה
            </button>
          )}
        </div>
      </div>
    </form>
  );
}
