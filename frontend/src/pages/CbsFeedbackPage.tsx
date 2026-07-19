import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { cbs, CbsFeedbackReport, CbsFeedbackOrder } from "../api/client";
import { useAuth } from "../auth/AuthContext";

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

  const cell: React.CSSProperties = { padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border,#e2e8f0)" };
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
          <span className="badge" style={{ background: "#f1f5f9" }}>
            סה"כ הצבעות: {data.total_votes.toLocaleString("he-IL")}
          </span>
          <span className="badge" style={{ background: "#ecfdf5", color: "#065f46" }}>
            👍 {data.likes.toLocaleString("he-IL")}
          </span>
          <span className="badge" style={{ background: "#fef2f2", color: "#991b1b" }}>
            👎 {data.dislikes.toLocaleString("he-IL")}
          </span>
        </div>
      )}

      <div className="flex mb-2" style={{ gap: "0.4rem", alignItems: "center" }}>
        <span className="text-sm text-muted">{t("cbs.sort_by", "מיון")}:</span>
        <select
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
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.88rem" }}>
            <thead>
              <tr>
                <th style={{ ...cell, textAlign: "start" }}>שאילתה</th>
                <th style={numCell}>👍</th>
                <th style={numCell}>👎</th>
                <th style={numCell}>סה"כ</th>
                <th style={numCell}>ציון</th>
                <th style={{ ...cell, textAlign: "start" }}>אחרון</th>
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
                  <td style={{ ...numCell, color: "#065f46" }}>{r.likes || ""}</td>
                  <td style={{ ...numCell, color: "#991b1b" }}>{r.dislikes || ""}</td>
                  <td style={numCell}>{r.total}</td>
                  <td style={{ ...numCell, fontWeight: 600, color: r.score < 0 ? "#991b1b" : "#065f46" }}>
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
