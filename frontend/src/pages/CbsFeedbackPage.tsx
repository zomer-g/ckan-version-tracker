import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { cbs, CbsFeedbackReport, CbsFeedbackOrder } from "../api/client";
import { useAuth } from "../auth/AuthContext";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
// Admin-only report of search like/dislike feedback, grouped by query. Default
// order puts the most-disliked queries first — the concrete list of searches
// whose results most need improving. Each query links straight into the search
// so you can reproduce what the user saw. Read at /cbs/feedback.

const ORDERS: [CbsFeedbackOrder, string][] = [
  ["dislikes", "הכי הרבה דיסלייקים"],
  ["likes", "הכי הרבה לייקים"],
  ["total", "הכי הרבה משוב"],
  ["recent", "האחרונים"],
];

export default function CbsFeedbackPage() {
  useDocumentTitle("משוב — למ\"ס");
  const { t } = useTranslation();
  const { user } = useAuth();
  const [order, setOrder] = useState<CbsFeedbackOrder>("dislikes");
  const [data, setData] = useState<CbsFeedbackReport | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    setError("");
    cbs
      .feedbackReport(order, 300)
      .then(setData)
      .catch((e) => setError(e?.message || "שגיאה בטעינת הדוח"))
      .finally(() => setLoading(false));
  }, [order]);

  if (!user?.is_admin) {
    return (
      <div className="container mt-3">
        <p className="text-muted">{t("cbs.fb_admin_only", "הדוח זמין למנהלים בלבד.")}</p>
        <Link to="/admin/login">{t("nav.login", "התחברות")}</Link>
      </div>
    );
  }

  const cell: React.CSSProperties = { padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" };
  const numCell: React.CSSProperties = { ...cell, textAlign: "center", fontVariantNumeric: "tabular-nums" };

  return (
    <div className="container mt-3">
      <div className="page-header">
        <h1>{t("cbs.fb_report_title", 'משוב על חיפוש הלמ"ס')}</h1>
        <p className="text-muted" style={{ marginTop: "0.35rem", maxWidth: "46rem" }}>
          {t(
            "cbs.fb_report_sub",
            "לייקים ודיסלייקים שגולשים נתנו לחיפושים, מקובצים לפי שאילתה. השאילתות עם הכי הרבה דיסלייקים הן יעדי השיפור — לחיצה על שאילתה פותחת אותה בחיפוש כדי לשחזר מה הוצג."
          )}
        </p>
      </div>

      {data && (
        <div className="flex mb-2" style={{ gap: "1rem", flexWrap: "wrap" }}>
          <span className="badge" style={{ background: "var(--surface-2)" }}>
            סה"כ הצבעות: {data.total_votes.toLocaleString("he-IL")}
          </span>
          <span className="badge" style={{ background: "var(--tint-good-bg)", color: "var(--success)" }}>
            👍 {data.likes.toLocaleString("he-IL")}
          </span>
          <span className="badge" style={{ background: "var(--tint-bad-bg)", color: "var(--tint-bad-fg)" }}>
            👎 {data.dislikes.toLocaleString("he-IL")}
          </span>
        </div>
      )}

      <div className="flex mb-2" style={{ gap: "0.4rem", alignItems: "center" }}>
        <span className="text-sm text-muted">{t("cbs.sort_by", "מיון")}:</span>
        <select
          aria-label={t("cbs.sort_by", "מיון")}
          value={order}
          onChange={(e) => setOrder(e.target.value as CbsFeedbackOrder)}
          style={{ width: "auto", padding: "0.25rem 0.5rem", fontSize: "0.82rem" }}
        >
          {ORDERS.map(([v, label]) => (
            <option key={v} value={v}>{label}</option>
          ))}
        </select>
      </div>

      {loading && <span className="loading" role="status">{t("common.loading", "טוען…")}</span>}
      {error && <div role="alert" className="badge badge-danger">{error}</div>}

      {data && data.queries.length === 0 && !loading && (
        <p className="text-muted">{t("cbs.fb_empty", "עדיין אין משוב.")}</p>
      )}

      {data && data.queries.length > 0 && (
        <div tabIndex={0} role="region" aria-label="משובים שהתקבלו" className="scroll-region" style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.88rem" }}>
            <thead>
              <tr>
                <th scope="col" style={{ ...cell, textAlign: "start" }}>שאילתה</th>
                <th scope="col" style={numCell}>👍</th>
                <th scope="col" style={numCell}>👎</th>
                <th scope="col" style={numCell}>סה"כ</th>
                <th scope="col" style={numCell}>ציון</th>
                <th scope="col" style={{ ...cell, textAlign: "start" }}>אחרון</th>
              </tr>
            </thead>
            <tbody>
              {data.queries.map((r) => (
                <tr key={r.query} style={{ background: r.dislikes > r.likes ? "#fff7f7" : undefined }}>
                  <td style={{ ...cell, textAlign: "start" }}>
                    <Link to={`/cbs?mode=ask&ask=${encodeURIComponent(r.query)}`} title="פתח בחיפוש">
                      {r.query}
                    </Link>
                  </td>
                  <td style={{ ...numCell, color: "var(--success)" }}>{r.likes || ""}</td>
                  <td style={{ ...numCell, color: "var(--tint-bad-fg)" }}>{r.dislikes || ""}</td>
                  <td style={numCell}>{r.total}</td>
                  <td style={{ ...numCell, fontWeight: 600, color: r.score < 0 ? "#941A1A" : "var(--success)" }}>
                    {r.score > 0 ? `+${r.score}` : r.score}
                  </td>
                  <td style={{ ...cell, textAlign: "start", color: "var(--text-muted)", fontSize: "0.8rem" }}>
                    {r.last_at ? new Date(r.last_at).toLocaleDateString("he-IL") : ""}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
