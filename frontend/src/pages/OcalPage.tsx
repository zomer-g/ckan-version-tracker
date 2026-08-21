import { useEffect, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import { ocal, OcalStats } from "../api/client";
import OcalSearch from "../components/ocal/OcalSearch";
import OcalCalendar from "../components/ocal/OcalCalendar";
import OcalDiaries from "../components/ocal/OcalDiaries";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
type OcalTab = "search" | "calendar" | "diaries";
const TAB_IDS: OcalTab[] = ["search", "calendar", "diaries"];
const TAB_LABELS: [OcalTab, string][] = [
  ["search", "🔍 חיפוש"],
  ["calendar", "📅 לוח שנה"],
  ["diaries", "📚 יומנים"],
];

/**
 * יומן לעם (Ocal) — migrated into OVER. Three tabs mirroring the legacy
 * ocal.org.il site: free-text search, a monthly calendar, and the diary
 * catalog with downloads. All data comes from OVER's /api/ocal/* endpoints
 * (see app/api/ocal.py) over the migrated Neon database.
 */
export default function OcalPage() {
  useDocumentTitle("יומן לעם");
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const urlTab = searchParams.get("tab") as OcalTab | null;
  const tab: OcalTab = urlTab && TAB_IDS.includes(urlTab) ? urlTab : "search";
  const setTab = (id: OcalTab) => setSearchParams(id === "search" ? {} : { tab: id });

  const [stats, setStats] = useState<OcalStats | null>(null);
  useEffect(() => {
    ocal.stats().then(setStats).catch(() => {});
  }, []);

  return (
    <div>
      {/* Processed-data notice, shared with the other "לעם" projects. */}
      <div className="processed-banner" role="note">
        <div className="container">
          <span className="processed-banner-badge">{t("projects.processed_badge")}</span>
          <span className="processed-banner-text">
            <Trans i18nKey="projects.processed_note" components={{ strong: <strong /> }} />
          </span>
        </div>
      </div>

      <div className="container mt-3">
        <div className="page-header" style={{ marginBottom: "0.75rem" }}>
          <h1 style={{ margin: 0 }}>יומן לעם</h1>
          <div className="text-sm text-muted" style={{ marginTop: "0.35rem", lineHeight: 1.7 }}>
            יומני הפגישות הרשמיים של נבחרי ציבור ובכירים בשירות הציבורי, מרוכזים, מנוקים ומקושרים
            למקור. חיפוש חופשי, תצוגת לוח שנה, וקטלוג היומנים להורדה.
            {stats && (
              <div style={{ marginTop: "0.4rem" }}>
                {stats.total_events.toLocaleString()} אירועים · {stats.total_sources.toLocaleString()} יומנים ·{" "}
                {stats.total_organizations.toLocaleString()} גופים
              </div>
            )}
          </div>
        </div>

        {/* Tabs */}
        <div className="flex" style={{ gap: "0.3rem", borderBottom: "2px solid var(--border, var(--border))", marginBottom: "1rem", flexWrap: "wrap" }}>
          {TAB_LABELS.map(([id, label]) => (
            <button
              key={id}
              type="button"
              onClick={() => setTab(id)}
              style={{
                padding: "0.5rem 1.05rem", border: "none", cursor: "pointer", background: "none",
                fontSize: "0.95rem", fontWeight: tab === id ? 700 : 500,
                color: tab === id ? "var(--primary, #0C5E58)" : "var(--text-muted)",
                borderBottom: tab === id ? "3px solid var(--primary, #0C5E58)" : "3px solid transparent",
                marginBottom: -2,
              }}
            >
              {label}
            </button>
          ))}
        </div>

        {tab === "search" && <OcalSearch />}
        {tab === "calendar" && <OcalCalendar />}
        {tab === "diaries" && <OcalDiaries />}

        <div className="text-sm text-muted" style={{ margin: "1.5rem 0 0.5rem" }}>
          המידע מעובד (חילוץ ישויות והצלבות מבוססי-בינה) — לא מקור ממשלתי ראשוני. כל אירוע מקושר לדאטהסט המקורי.
        </div>
      </div>
    </div>
  );
}
