import { useState, FormEvent, useMemo, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { publicApi } from "../api/client";

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
}

const INTERVAL_OPTIONS = [
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
}: RequestFormProps) {
  const { t, i18n } = useTranslation();
  const [name, setName] = useState("");
  const [notes, setNotes] = useState("");
  const [contact, setContact] = useState("");
  const [interval, setInterval] = useState(604800);
  const [submitting, setSubmitting] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState("");

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

  // Keep the selection in sync if the parent swaps in a different
  // dataset while the form is mounted (rare but happens when the user
  // changes their mind without closing the form).
  useEffect(() => {
    setSelectedResources(initialSelected);
  }, [initialSelected]);

  const showResourcePicker =
    sourceType === "ckan" && Array.isArray(availableResources) && availableResources.length > 0;

  const formId = sourceType === "scraper" ? (sourceUrl || "scraper") : (ckanId || "form");

  const toggleResource = (rid: string) => {
    setSelectedResources((prev) => {
      const next = new Set(prev);
      if (next.has(rid)) next.delete(rid);
      else next.add(rid);
      return next;
    });
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError("");
    try {
      if (sourceType === "scraper" && sourceUrl) {
        await publicApi.requestScraper({
          source_url: sourceUrl,
          title: datasetTitle,
          preferred_interval: interval,
          requester_name: name || undefined,
          requester_notes: notes || undefined,
          requester_contact: contact || undefined,
        });
      } else {
        const ids = Array.from(selectedResources);
        if (showResourcePicker && ids.length === 0) {
          setError(t("home.request_pick_files") || "בחרו לפחות קובץ אחד למעקב");
          setSubmitting(false);
          return;
        }
        await publicApi.request({
          ckan_id: ckanId!,
          resource_id: resourceId,
          resource_ids: ids.length > 0 ? ids : undefined,
          preferred_interval: interval,
          requester_name: name || undefined,
          requester_notes: notes || undefined,
          requester_contact: contact || undefined,
        });
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
          background: "#dcfce7",
          border: "1px solid #86efac",
          padding: "1.25rem",
          marginTop: "0.75rem",
        }}
        role="status"
        aria-live="polite"
      >
        <p style={{ color: "#166534", fontWeight: 500, margin: 0 }}>
          {t("home.request_success")}
        </p>
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
        <div role="alert" className="badge badge-danger mb-1" style={{ display: "block" }}>
          {error}
        </div>
      )}

      <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
        {showResourcePicker && (
          <div>
            <div className="text-sm" style={{ fontWeight: 500, marginBottom: "0.4rem" }}>
              {t("home.request_pick_files_label") || "בחרו אילו קבצים לעקוב אחריהם"}
              <span style={{ color: "#dc2626", marginInlineStart: "0.25rem" }}>*</span>
            </div>
            <div
              style={{
                maxHeight: "12rem",
                overflowY: "auto",
                border: "1px solid var(--border)",
                borderRadius: "var(--radius)",
                background: "white",
              }}
            >
              {availableResources!.map((res) => {
                const checked = selectedResources.has(res.id);
                return (
                  <label
                    key={res.id}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "0.5rem",
                      padding: "0.4rem 0.6rem",
                      borderBottom: "1px solid var(--border)",
                      cursor: "pointer",
                      fontSize: "0.85rem",
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleResource(res.id)}
                    />
                    <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis" }}>
                      {res.name || res.id}
                    </span>
                    {res.format && (
                      <span className="badge" style={{ fontSize: "0.7rem" }}>
                        {res.format}
                      </span>
                    )}
                  </label>
                );
              })}
            </div>
            <div className="text-sm text-muted" style={{ marginTop: "0.25rem" }}>
              {selectedResources.size}/{availableResources!.length}
            </div>
          </div>
        )}

        <div>
          <label htmlFor={`req-name-${formId}`} className="text-sm" style={{ fontWeight: 500 }}>
            {t("home.request_name")}
          </label>
          <input
            id={`req-name-${formId}`}
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder={t("home.request_name")}
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

        <div>
          <label htmlFor={`req-contact-${formId}`} className="text-sm" style={{ fontWeight: 500 }}>
            {t("home.request_contact")}
          </label>
          <input
            id={`req-contact-${formId}`}
            type="text"
            value={contact}
            onChange={(e) => setContact(e.target.value)}
            placeholder={t("home.request_contact")}
          />
        </div>

        <div>
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
        </div>

        <button
          type="submit"
          className="btn-primary"
          disabled={submitting || (showResourcePicker && selectedResources.size === 0)}
          style={{ alignSelf: "flex-start" }}
        >
          {submitting ? t("common.loading") : t("home.request_submit")}
        </button>
      </div>
    </form>
  );
}
