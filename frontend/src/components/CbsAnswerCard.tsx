import { useTranslation } from "react-i18next";
import { CbsResolveResponse, CbsAnswerType, CbsUnderstood } from "../api/client";
import {
  cutLabel,
  geoLabel,
  metricLabel,
  productFormLabel,
  PRODUCT_FORM_ICONS,
} from "../utils/cbsLabels";

// The answer card for natural-language mode: turns POST /api/cbs/resolve into a
// single actionable statement ("here's the file" / "run this generator" / "CBS
// doesn't hold this") instead of a raw result list. The supporting results are
// rendered separately by the page, below this card.
//
// Each answer_type gets its own accent + verb so the user knows what to DO. The
// two that matter most are `not_available` — an honest "CBS doesn't have this,
// here's the alternative", which no plain search engine gives — and `guidance`,
// a curated intent pointing straight at the source.
const TYPE_META: Record<
  CbsAnswerType,
  { icon: string; label: string; accent: string; cta: string }
> = {
  guidance: {
    icon: "🎯",
    label: "הפניה מומלצת",
    accent: "#2563eb",
    cta: "למקור בלמ\"ס",
  },
  generator: {
    icon: "⚙️",
    label: "מחולל / דשבורד",
    accent: "#7c3aed",
    cta: "להפעלת המחולל",
  },
  data_file: {
    icon: "📊",
    label: "קובץ נתונים",
    accent: "#059669",
    cta: "לעמוד ההורדה",
  },
  publication: {
    icon: "📄",
    label: "פרסום",
    accent: "#0891b2",
    cta: "לפרסום",
  },
  special_processing: {
    icon: "✉️",
    label: 'קיים — בעיבוד מיוחד',
    accent: "#c2410c",
    cta: "לפנייה ללמ\"ס",
  },
  not_available: {
    icon: "🚫",
    label: 'אין בלמ"ס',
    accent: "#b45309",
    cta: "לחלופה",
  },
  no_results: {
    icon: "🤷",
    label: "לא נמצא",
    accent: "#6b7280",
    cta: "",
  },
};

// The "הבנתי:" chips — every dimension the deterministic parser extracted from
// the question. Transparency of understanding: the user sees exactly what the
// engine thinks they asked, and can carry it into the advanced tab to adjust.
function understoodChips(u: CbsUnderstood): string[] {
  const chips: string[] = [];
  if (u.geo_entity) {
    chips.push(
      `📍 ${u.geo_entity.name}${u.geo_entity.subdistrict ? ` (נפת ${u.geo_entity.subdistrict})` : ""}`,
    );
  }
  if (u.geo_level) chips.push(`🗺️ רזולוציה: ${geoLabel(u.geo_level)}`);
  if (u.years.length === 1) chips.push(`📅 ${u.years[0]}`);
  if (u.years.length > 1) chips.push(`📅 ${u.years[0]}–${u.years[u.years.length - 1]}`);
  if (u.latest) chips.push("🕐 העדכני ביותר");
  if (u.series) chips.push("📈 סדרה לאורך זמן");
  if (u.product_form) chips.push(`${PRODUCT_FORM_ICONS[u.product_form] || ""} ${productFormLabel(u.product_form)}`);
  for (const m of u.metrics) chips.push(`Σ ${metricLabel(m)}`);
  for (const c of u.cuts) chips.push(`👥 לפי ${cutLabel(c)}`);
  if (u.source_op) chips.push(`🗃️ מקור: ${u.source_op}`);
  return chips;
}

interface Props {
  data: CbsResolveResponse;
  // "עריכה בחיפוש מתקדם" — the page maps the understood dimensions onto the
  // advanced tab's filters and switches mode.
  onEditInAdvanced?: () => void;
}

export default function CbsAnswerCard({ data, onEditInAdvanced }: Props) {
  const { t } = useTranslation();
  const meta = TYPE_META[data.answer_type] ?? TYPE_META.publication;
  const primary = data.primary;
  // Intents carry the clean navigational target in `link`; everything else uses
  // its page url. Never link to an intent's own `url` — it has a #intent-N
  // fragment that exists only to keep the index key unique.
  const href = primary?.link || primary?.url || "";
  const chips = understoodChips(data.understood ?? ({} as CbsUnderstood));
  const matrixEntries = Object.entries(data.geo_matrix ?? {});

  return (
    <div
      className="card mb-2"
      style={{
        borderInlineStart: `4px solid ${meta.accent}`,
        padding: "0.9rem 1rem",
      }}
    >
      <div className="flex" style={{ gap: "0.5rem", alignItems: "center", marginBottom: "0.4rem" }}>
        <span aria-hidden="true" style={{ fontSize: "1.05rem" }}>{meta.icon}</span>
        <span
          className="badge"
          style={{ background: meta.accent, color: "#fff", fontSize: "0.72rem" }}
        >
          {meta.label}
        </span>
        {primary?.data_vintage && (
          <span className="badge" style={{ fontSize: "0.72rem", background: "#ecfdf5", color: "#065f46" }}>
            {t("cbs.data_year", "שנת נתונים")}: {primary.data_vintage}
          </span>
        )}
        {data.geo_available && (
          <span className="badge" style={{ fontSize: "0.72rem" }}>
            {t("cbs.geo_available", "רזולוציה זמינה")}: {geoLabel(data.geo_available)}
          </span>
        )}
      </div>

      {/* "הבנתי:" — the parsed dimensions, the transparency layer between the
          free question and the structured search. */}
      {chips.length > 0 && (
        <div
          className="flex"
          style={{ gap: "0.3rem", flexWrap: "wrap", alignItems: "center", marginBottom: "0.55rem" }}
        >
          <span className="text-sm text-muted" style={{ fontWeight: 600 }}>
            {t("cbs.understood", "הבנתי")}:
          </span>
          {chips.map((c) => (
            <span
              key={c}
              className="badge"
              style={{ fontSize: "0.72rem", background: "#f1f5f9", color: "#334155" }}
            >
              {c}
            </span>
          ))}
          {onEditInAdvanced && (
            <button
              type="button"
              className="btn-secondary"
              onClick={onEditInAdvanced}
              style={{ fontSize: "0.7rem", padding: "0.15rem 0.5rem" }}
            >
              {t("cbs.edit_advanced", "עריכה בחיפוש מתקדם")}
            </button>
          )}
        </div>
      )}

      {data.answer && (
        <p style={{ margin: "0 0 0.6rem", lineHeight: 1.5 }}>{data.answer}</p>
      )}

      {primary && href && (
        <a
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          className="btn-primary"
          style={{ display: "inline-block", textDecoration: "none", fontSize: "0.88rem" }}
        >
          {primary.product_form ? `${PRODUCT_FORM_ICONS[primary.product_form] || ""} ` : ""}
          {meta.cta}
          {primary.title ? ` — ${primary.title}` : ""} ↗
        </a>
      )}

      {/* Availability by resolution — the community's own answer format
          ("יש עד נפה, אין א"ס"), computed over the found sources. */}
      {matrixEntries.length > 1 && (
        <div
          className="flex text-sm"
          style={{ gap: "0.45rem", flexWrap: "wrap", marginTop: "0.6rem", alignItems: "center" }}
        >
          <span className="text-muted" style={{ fontWeight: 600 }}>
            {t("cbs.geo_matrix", "זמינות לפי רזולוציה")}:
          </span>
          {matrixEntries.map(([lvl, ok]) => (
            <span
              key={lvl}
              className="badge"
              style={{
                fontSize: "0.72rem",
                background: ok ? "#ecfdf5" : "#fef2f2",
                color: ok ? "#065f46" : "#991b1b",
              }}
            >
              {geoLabel(lvl)} {ok ? "✓" : "✗"}
            </span>
          ))}
        </div>
      )}

      {/* Edition history of the primary source ("מהדורות קודמות"). */}
      {data.editions && data.editions.length > 0 && (
        <div
          className="flex text-sm"
          style={{ gap: "0.35rem", flexWrap: "wrap", marginTop: "0.55rem", alignItems: "center" }}
        >
          <span className="text-muted" style={{ fontWeight: 600 }}>
            {t("cbs.editions", "מהדורות נוספות")}:
          </span>
          {data.editions.map((e) => (
            <a
              key={e.url}
              href={e.url}
              target="_blank"
              rel="noopener noreferrer"
              title={e.title || undefined}
              className="badge"
              style={{
                fontSize: "0.72rem",
                background: "#fffbeb",
                color: "#92400e",
                border: "1px solid #fde68a",
                textDecoration: "none",
              }}
            >
              {e.edition_year ?? "—"}
            </a>
          ))}
        </div>
      )}

      {data.caveats.length > 0 && (
        <ul
          className="text-sm text-muted"
          style={{ margin: "0.6rem 0 0", paddingInlineStart: "1.1rem" }}
        >
          {data.caveats.map((c, i) => (
            <li key={i}>⚠️ {c}</li>
          ))}
        </ul>
      )}
    </div>
  );
}
