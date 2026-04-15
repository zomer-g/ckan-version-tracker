import { useState, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { publicApi } from "../api/client";

interface RequestFormProps {
  ckanId?: string;
  resourceId?: string;
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

  const formId = sourceType === "scraper" ? (sourceUrl || "scraper") : (ckanId || "form");

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
        await publicApi.request({
          ckan_id: ckanId!,
          resource_id: resourceId,
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
          disabled={submitting}
          style={{ alignSelf: "flex-start" }}
        >
          {submitting ? t("common.loading") : t("home.request_submit")}
        </button>
      </div>
    </form>
  );
}
