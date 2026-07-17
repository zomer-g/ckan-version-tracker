import { useTranslation } from "react-i18next";
import { CbsResolveResponse, CbsAnswerType } from "../api/client";
import { geoLabel } from "../utils/cbsLabels";

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

interface Props {
  data: CbsResolveResponse;
}

export default function CbsAnswerCard({ data }: Props) {
  const { t } = useTranslation();
  const meta = TYPE_META[data.answer_type] ?? TYPE_META.publication;
  const primary = data.primary;
  // Intents carry the clean navigational target in `link`; everything else uses
  // its page url. Never link to an intent's own `url` — it has a #intent-N
  // fragment that exists only to keep the index key unique.
  const href = primary?.link || primary?.url || "";

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
        {data.geo_available && (
          <span className="badge" style={{ fontSize: "0.72rem" }}>
            {t("cbs.geo_available", "רזולוציה זמינה")}: {geoLabel(data.geo_available)}
          </span>
        )}
      </div>

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
          {meta.cta}
          {primary.title ? ` — ${primary.title}` : ""} ↗
        </a>
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
