import { useCallback, useEffect, useState } from "react";
import { Trans, useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import { ocoi, OcoiEntityType, OcoiStats } from "../api/client";
import OcoiSearch from "../components/ocoi/OcoiSearch";
import OcoiGraphTab, { GraphTarget } from "../components/ocoi/OcoiGraphTab";
import OcoiDocuments from "../components/ocoi/OcoiDocuments";
import OcoiEntities from "../components/ocoi/OcoiEntities";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
type OcoiTab = "search" | "graph" | "entities" | "documents";
const TAB_IDS: OcoiTab[] = ["search", "graph", "entities", "documents"];
const TAB_LABELS: [OcoiTab, string][] = [
  ["search", "🔍 חיפוש"],
  ["graph", "🕸️ מפת קשרים"],
  ["entities", "📊 ישויות"],
  ["documents", "📄 מסמכים"],
];

/**
 * ניגוד עניינים לעם (OCOI) — migrated into OVER.
 *
 * Four tabs over /api/ocoi/*: search, the relationship graph, entity rankings,
 * and the source declarations. Replaces the placeholder that linked out to
 * ocoi.org.il; the navbar/footer entries and i18n keys already pointed here.
 *
 * Tab AND graph focus both live in the URL (?tab=, ?type=, ?id=) so a specific
 * person's conflict web is a shareable link — the thing people actually want to
 * send each other about this corpus.
 */
export default function OcoiPage() {
  useDocumentTitle("ניגוד עניינים לעם");
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();

  const urlTab = searchParams.get("tab") as OcoiTab | null;
  const tab: OcoiTab = urlTab && TAB_IDS.includes(urlTab) ? urlTab : "search";

  const urlType = searchParams.get("type") as OcoiEntityType | null;
  const urlId = searchParams.get("id");
  const urlName = searchParams.get("name") || "";
  const target: GraphTarget | null =
    urlType && urlId ? { type: urlType, id: urlId, name: urlName } : null;

  const [stats, setStats] = useState<OcoiStats | null>(null);
  useEffect(() => {
    ocoi.stats().then((r) => setStats(r.data)).catch(() => {});
  }, []);

  const setTab = (id: OcoiTab) => {
    const next = new URLSearchParams(searchParams);
    if (id === "search") next.delete("tab");
    else next.set("tab", id);
    // Focus only means something on the graph tab; drop it elsewhere so a
    // stale ?id= doesn't reappear when the user comes back.
    if (id !== "graph") {
      next.delete("type");
      next.delete("id");
      next.delete("name");
    }
    setSearchParams(next);
  };

  const focusGraph = useCallback(
    (t: GraphTarget | null) => {
      const next = new URLSearchParams(searchParams);
      next.set("tab", "graph");
      if (t) {
        next.set("type", t.type);
        next.set("id", t.id);
        if (t.name) next.set("name", t.name);
        else next.delete("name");
      } else {
        next.delete("type");
        next.delete("id");
        next.delete("name");
      }
      setSearchParams(next);
    },
    [searchParams, setSearchParams],
  );

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
          <h1 style={{ margin: 0 }}>ניגוד עניינים לעם</h1>
          <div className="text-sm text-muted" style={{ marginTop: "0.35rem", lineHeight: 1.7 }}>
            הסדרי ניגוד העניינים של בעלי תפקידים ציבוריים בישראל — מחולצים מהמסמכים הרשמיים,
            מקושרים לחברות ולעמותות, ומוצגים כמפת קשרים שאפשר לחקור.
            {stats && (
              <div style={{ marginTop: "0.4rem" }}>
                {stats.documents.toLocaleString()} מסמכים · {stats.persons.toLocaleString()} אנשים ·{" "}
                {stats.companies.toLocaleString()} חברות · {stats.associations.toLocaleString()} עמותות ·{" "}
                {stats.relationships.toLocaleString()} קשרים
              </div>
            )}
          </div>
        </div>

        {/* Tabs */}
        <div
          className="flex"
          style={{
            gap: "0.3rem",
            borderBottom: "2px solid var(--border)",
            marginBottom: "1rem",
            flexWrap: "wrap",
          }}
        >
          {TAB_LABELS.map(([id, label]) => (
            <button
              key={id}
              type="button"
              onClick={() => setTab(id)}
              style={{
                padding: "0.5rem 1.05rem",
                border: "none",
                cursor: "pointer",
                background: "none",
                fontSize: "0.95rem",
                fontWeight: tab === id ? 700 : 500,
                color: tab === id ? "var(--primary)" : "var(--text-muted)",
                borderBottom: tab === id ? "3px solid var(--primary)" : "3px solid transparent",
                marginBottom: -2,
              }}
            >
              {label}
            </button>
          ))}
        </div>

        {tab === "search" && (
          <OcoiSearch
            onOpenEntity={(type, id, name) => focusGraph({ type, id, name })}
            stats={stats}
          />
        )}
        {tab === "graph" && <OcoiGraphTab target={target} onTarget={focusGraph} />}
        {tab === "entities" && (
          <OcoiEntities onOpenEntity={(type, id, name) => focusGraph({ type, id, name })} />
        )}
        {tab === "documents" && (
          <OcoiDocuments
            onOpenEntity={(type, id, name) => focusGraph({ type, id, name })}
          />
        )}

        <div className="text-sm text-muted" style={{ marginTop: "1.5rem", lineHeight: 1.7 }}>
          הנתונים מחולצים אוטומטית ממסמכים רשמיים ועשויים להכיל שגיאות. המקור המחייב הוא המסמך
          עצמו — כל ישות וכל קשר מקושרים למסמך שממנו חולצו.
        </div>
      </div>
    </div>
  );
}
