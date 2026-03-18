import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams, Link } from "react-router-dom";
import { versions as versionsApi } from "../api/client";

interface DiffEntry {
  type: string;
  field: string;
  old_value: any;
  new_value: any;
}

export default function DiffPage() {
  const { t } = useTranslation();
  const { datasetId } = useParams<{ datasetId: string }>();
  const [searchParams] = useSearchParams();
  const fromId = searchParams.get("from") || "";
  const toId = searchParams.get("to") || "";

  const [diff, setDiff] = useState<DiffEntry[]>([]);
  const [fromNumber, setFromNumber] = useState(0);
  const [toNumber, setToNumber] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!fromId || !toId) return;
    setLoading(true);
    versionsApi
      .diff(fromId, toId)
      .then((data) => {
        setDiff(data.diff);
        setFromNumber(data.from_number);
        setToNumber(data.to_number);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [fromId, toId]);

  if (loading) return <div className="loading" role="status" aria-live="polite">{t("common.loading")}</div>;

  return (
    <div>
      <div className="page-header flex-between">
        <h1>
          {t("diff.title")}:{" "}
          <span dir="ltr">v{fromNumber} &rarr; v{toNumber}</span>
        </h1>
        <Link
          to={`/versions/${datasetId}`}
          className="btn-secondary"
          style={{ textDecoration: "none" }}
        >
          {t("common.back")}
        </Link>
      </div>

      {error && <div role="alert" className="badge badge-danger mb-2">{error}</div>}

      {diff.length === 0 ? (
        <div className="empty-state">{t("diff.no_changes")}</div>
      ) : (
        <div className="card" style={{ overflow: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <caption className="sr-only" style={{ position: "absolute", width: 1, height: 1, overflow: "hidden", clip: "rect(0,0,0,0)" }}>
              {t("diff.title")}: v{fromNumber} &rarr; v{toNumber}
            </caption>
            <thead>
              <tr style={{ borderBottom: "2px solid var(--border)" }}>
                <th scope="col" style={thStyle}>{t("diff.field")}</th>
                <th scope="col" style={thStyle}>{t("diff.old_value")}</th>
                <th scope="col" style={thStyle}>{t("diff.new_value")}</th>
              </tr>
            </thead>
            <tbody>
              {diff.map((entry, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--border)" }}>
                  <td style={tdStyle}>
                    <div className="flex">
                      <span
                        className={`badge ${
                          entry.type === "changed"
                            ? "badge-warning"
                            : entry.type === "added"
                            ? "badge-success"
                            : "badge-danger"
                        }`}
                      >
                        {t(`diff.${entry.type}`)}
                      </span>
                      <code style={{ fontSize: "0.8rem" }}>{entry.field}</code>
                    </div>
                  </td>
                  <td style={{ ...tdStyle, background: entry.type === "removed" ? "#fee2e2" : undefined }}>
                    <pre style={preStyle}>
                      {entry.old_value !== null ? JSON.stringify(entry.old_value, null, 2) : "\u2014"}
                    </pre>
                  </td>
                  <td style={{ ...tdStyle, background: entry.type === "added" ? "#dcfce7" : undefined }}>
                    <pre style={preStyle}>
                      {entry.new_value !== null ? JSON.stringify(entry.new_value, null, 2) : "\u2014"}
                    </pre>
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

const thStyle: React.CSSProperties = {
  textAlign: "start",
  padding: "0.75rem",
  fontSize: "0.875rem",
  fontWeight: 600,
};

const tdStyle: React.CSSProperties = {
  padding: "0.75rem",
  verticalAlign: "top",
  fontSize: "0.875rem",
};

const preStyle: React.CSSProperties = {
  margin: 0,
  whiteSpace: "pre-wrap",
  wordBreak: "break-word",
  fontSize: "0.8rem",
  maxHeight: "200px",
  overflow: "auto",
};
