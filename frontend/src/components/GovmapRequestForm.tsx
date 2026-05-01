import { useState, FormEvent, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { publicApi } from "../api/client";

const GOVMAP_LAY_RE = /[?&]lay(?:er|ers)?=(\d+)/i;
const GOVMAP_HOST_RE = /^https?:\/\/(www\.)?govmap\.gov\.il\/?\?/i;

const INTERVAL_OPTIONS = [
  { value: 86400, labelHe: "יום", labelEn: "1 day" },
  { value: 604800, labelHe: "שבוע", labelEn: "1 week" },
  { value: 2592000, labelHe: "חודש", labelEn: "1 month" },
  { value: 7776000, labelHe: "רבעון", labelEn: "3 months" },
];

interface ParsedLine {
  raw: string;
  url: string;
  layerId: string | null;
  valid: boolean;
}

interface GovmapRequestFormProps {
  initialUrl: string;
  onClose: () => void;
}

function parseLine(raw: string): ParsedLine {
  const url = raw.trim();
  if (!url) return { raw, url, layerId: null, valid: false };
  if (!GOVMAP_HOST_RE.test(url)) return { raw, url, layerId: null, valid: false };
  const m = url.match(GOVMAP_LAY_RE);
  return {
    raw,
    url,
    layerId: m ? m[1] : null,
    valid: !!m,
  };
}

export default function GovmapRequestForm({ initialUrl, onClose }: GovmapRequestFormProps) {
  const { t, i18n } = useTranslation();
  const [text, setText] = useState(initialUrl);
  const [name, setName] = useState("");
  const [notes, setNotes] = useState("");
  const [contact, setContact] = useState("");
  const [interval, setInterval] = useState(604800);
  const [submitting, setSubmitting] = useState(false);
  const [results, setResults] = useState<
    Array<{ url: string; status: string; layer_id?: string; error?: string }> | null
  >(null);
  const [error, setError] = useState("");

  const parsed = useMemo<ParsedLine[]>(() => {
    // Split ONLY on newlines, never on commas — govmap URLs contain
    // `c=x,y` ITM coordinates, so a comma split tears each URL into
    // two invalid chunks.
    return text
      .split(/\r?\n+/)
      .map((s) => s.trim())
      .filter(Boolean)
      .map(parseLine);
  }, [text]);

  const validUrls = parsed.filter((p) => p.valid);
  const invalidCount = parsed.length - validUrls.length;
  const canSubmit = validUrls.length > 0 && !submitting;

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError("");
    try {
      const resp = await publicApi.requestGovmap({
        source_urls: validUrls.map((p) => p.url),
        preferred_interval: interval,
        requester_name: name || undefined,
        requester_notes: notes || undefined,
        requester_contact: contact || undefined,
      });
      setResults(resp.results);
    } catch (err: any) {
      setError(err.message || t("common.error"));
    }
    setSubmitting(false);
  };

  if (results) {
    const created = results.filter((r) => r.status === "pending").length;
    const dup = results.filter((r) => r.status === "duplicate").length;
    const bad = results.filter((r) => r.status === "invalid").length;
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
          {t("home.govmap_request_done", { created, dup, bad })}
        </p>
        <ul
          style={{
            margin: "0.6rem 0 0 0",
            padding: 0,
            listStyle: "none",
            fontSize: "0.8rem",
          }}
        >
          {results.map((r, i) => (
            <li key={i} style={{ wordBreak: "break-all", padding: "0.15rem 0" }}>
              <strong style={{ color: r.status === "pending" ? "#166534" : r.status === "duplicate" ? "#92400e" : "#dc2626" }}>
                [{r.status}]
              </strong>{" "}
              {r.layer_id ? `lay=${r.layer_id} — ` : ""}
              {r.url}
              {r.error && <span style={{ color: "#dc2626" }}> ({r.error})</span>}
            </li>
          ))}
        </ul>
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
      aria-label={t("home.govmap_request_title")}
    >
      <div className="flex-between mb-1">
        <h3 style={{ fontSize: "1rem", fontWeight: 600, margin: 0, color: "var(--primary)" }}>
          {t("home.govmap_request_title")}
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
        {t("home.govmap_request_hint")}
      </p>

      {error && (
        <div role="alert" className="badge badge-danger mb-1" style={{ display: "block" }}>
          {error}
        </div>
      )}

      <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
        <div>
          <label htmlFor="govmap-urls" className="text-sm" style={{ fontWeight: 500 }}>
            {t("home.govmap_urls_label")}
            <span style={{ color: "#dc2626", marginInlineStart: "0.25rem" }}>*</span>
          </label>
          <textarea
            id="govmap-urls"
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="https://www.govmap.gov.il/?c=...&lay=220826"
            rows={4}
            style={{
              width: "100%",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius)",
              padding: "0.5rem 0.75rem",
              fontSize: "0.85rem",
              fontFamily: "monospace",
              direction: "ltr",
              resize: "vertical",
            }}
          />
          {parsed.length > 0 && (
            <div
              style={{
                marginTop: "0.4rem",
                maxHeight: "8rem",
                overflowY: "auto",
                border: "1px solid var(--border)",
                borderRadius: "var(--radius)",
                background: "white",
                fontSize: "0.78rem",
              }}
            >
              {parsed.map((p, i) => (
                <div
                  key={i}
                  style={{
                    padding: "0.3rem 0.6rem",
                    borderBottom: "1px solid var(--border)",
                    color: p.valid ? "#166534" : "#dc2626",
                    direction: "ltr",
                    wordBreak: "break-all",
                  }}
                >
                  {p.valid ? `✓ lay=${p.layerId}` : "✗ invalid"} — {p.url}
                </div>
              ))}
            </div>
          )}
          <div className="text-sm text-muted" style={{ marginTop: "0.25rem" }}>
            {t("home.govmap_urls_summary", {
              valid: validUrls.length,
              invalid: invalidCount,
            })}
          </div>
        </div>

        <div>
          <label htmlFor="govmap-name" className="text-sm" style={{ fontWeight: 500 }}>
            {t("home.request_name")}
          </label>
          <input
            id="govmap-name"
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder={t("home.request_name")}
          />
        </div>

        <div>
          <label htmlFor="govmap-notes" className="text-sm" style={{ fontWeight: 500 }}>
            {t("home.request_notes")}
          </label>
          <textarea
            id="govmap-notes"
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
          <label htmlFor="govmap-contact" className="text-sm" style={{ fontWeight: 500 }}>
            {t("home.request_contact")}
          </label>
          <input
            id="govmap-contact"
            type="text"
            value={contact}
            onChange={(e) => setContact(e.target.value)}
            placeholder={t("home.request_contact")}
          />
        </div>

        <div>
          <label htmlFor="govmap-interval" className="text-sm" style={{ fontWeight: 500 }}>
            {t("home.request_interval")}
          </label>
          <select
            id="govmap-interval"
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
          disabled={!canSubmit}
          style={{ alignSelf: "flex-start" }}
        >
          {submitting
            ? t("common.loading")
            : t("home.govmap_request_submit", { count: validUrls.length })}
        </button>
      </div>
    </form>
  );
}
