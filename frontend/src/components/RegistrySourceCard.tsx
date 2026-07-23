import { useTranslation } from "react-i18next";
import type { RegistrySourceValidation } from "../api/client";
import RequestForm from "./RequestForm";

interface Props {
  result: RegistrySourceValidation;
  formOpen: boolean;
  onOpenForm: () => void;
  onCloseForm: () => void;
}

/**
 * Result card for a source declared by the scraper worker rather than
 * hardcoded in this bundle.
 *
 * Every hardcoded source has its own near-identical copy of this card in
 * HomePage/SearchPage, with the chip colours and the site's name written into
 * the JSX. Here both come from the source's manifest (via
 * POST /api/sources/validate), so a source added to the worker renders
 * correctly without a frontend release.
 */
export default function RegistrySourceCard({
  result,
  formOpen,
  onOpenForm,
  onCloseForm,
}: Props) {
  const { t, i18n } = useTranslation();
  const badge = result.badge;
  const label = i18n.language === "en" ? result.label_en : result.label_he;

  return (
    <section aria-label={`${result.source_id} result`} style={{ marginBottom: "2rem" }}>
      <div className="grid grid-2">
        <article
          className="card"
          style={{ borderRight: `4px solid ${badge?.accent || "var(--primary)"}` }}
        >
          <div className="flex-between mb-1">
            <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
              <h2 style={{ fontSize: "1rem", fontWeight: 600, margin: 0 }}>
                {result.title}
              </h2>
              {badge && (
                <span
                  style={{
                    display: "inline-block",
                    padding: "0.15rem 0.5rem",
                    borderRadius: "9999px",
                    fontSize: "0.65rem",
                    fontWeight: 600,
                    background: badge.bg,
                    color: badge.fg,
                  }}
                >
                  {badge.label}
                </span>
              )}
            </div>
          </div>
          <div className="flex text-sm text-muted" style={{ gap: "0.75rem" }}>
            <span>{label}</span>
          </div>
          <p className="text-sm text-muted mt-1" style={{ wordBreak: "break-all" }}>
            <a
              href={result.url}
              target="_blank"
              rel="noopener noreferrer"
              style={{ color: "var(--primary)" }}
            >
              {result.url}
            </a>
          </p>

          <div style={{ marginTop: "0.75rem" }}>
            {formOpen ? (
              <RequestForm
                datasetTitle={result.title || ""}
                onClose={onCloseForm}
                sourceType="scraper"
                sourceUrl={result.url}
                defaultInterval={result.default_poll_interval}
              />
            ) : (
              <button
                className="btn-primary"
                onClick={onOpenForm}
                style={{ fontSize: "0.85rem" }}
              >
                {t("home.request_btn")}
              </button>
            )}
          </div>
        </article>
      </div>
    </section>
  );
}
