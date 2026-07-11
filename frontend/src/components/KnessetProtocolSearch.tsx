import { useState, useEffect, useCallback } from "react";
import {
  knessetProtocols,
  ProtocolRow,
  ProtocolCommittee,
} from "../api/client";

function fmtDate(value: string | null | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (isNaN(d.getTime())) return value.slice(0, 10);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)}.${d.getFullYear()}`;
}

const PAGE = 50;
const inputStyle: React.CSSProperties = {
  padding: "0.4rem 0.6rem",
  border: "1px solid var(--border, #d1d5db)",
  borderRadius: 4,
  fontSize: "0.9rem",
};

export default function KnessetProtocolSearch() {
  const [knessets, setKnessets] = useState<{ knesset: number; doc_count: number }[]>([]);
  const [committees, setCommittees] = useState<ProtocolCommittee[]>([]);
  const [knesset, setKnesset] = useState<string>("");
  const [committeeId, setCommitteeId] = useState<string>("");
  const [q, setQ] = useState("");

  const [rows, setRows] = useState<ProtocolRow[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searched, setSearched] = useState(false);

  // Knesset dropdown options.
  useEffect(() => {
    knessetProtocols
      .knessets()
      .then((r) => setKnessets(r.knessets))
      .catch(() => {});
  }, []);

  // Committee dropdown follows the chosen Knesset.
  useEffect(() => {
    setCommitteeId("");
    if (!knesset) {
      setCommittees([]);
      return;
    }
    knessetProtocols
      .committees({ knesset: Number(knesset), limit: 2000 })
      .then((r) => setCommittees(r.committees))
      .catch(() => setCommittees([]));
  }, [knesset]);

  const doSearch = useCallback(
    (newOffset: number) => {
      setLoading(true);
      setError(null);
      knessetProtocols
        .search({
          q: q.trim() || undefined,
          knesset: knesset ? Number(knesset) : undefined,
          committee_id: committeeId ? Number(committeeId) : undefined,
          limit: PAGE,
          offset: newOffset,
        })
        .then((r) => {
          setRows(r.rows);
          setTotal(r.total);
          setOffset(newOffset);
          setSearched(true);
        })
        .catch((e) => setError(e?.message || "שגיאה בחיפוש"))
        .finally(() => setLoading(false));
    },
    [q, knesset, committeeId],
  );

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    doSearch(0);
  };

  return (
    <div>
      <p className="text-sm text-muted" style={{ marginTop: 0, lineHeight: 1.6 }}>
        חיפוש בכל פרוטוקולי ועדות הכנסת (מסונכרן מפיד ה-ODATA). סננו לפי{" "}
        <strong>מספר כנסת</strong> ו/או <strong>שם ועדה</strong>, והוסיפו טקסט חופשי
        לחיפוש בכותרת הפרוטוקול. כל תוצאה מקשרת לקובץ המקורי.
      </p>

      <form
        onSubmit={onSubmit}
        className="card"
        style={{ padding: "0.85rem", marginBottom: "1rem", display: "flex", gap: "0.75rem", flexWrap: "wrap", alignItems: "flex-end" }}
      >
        <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.82rem" }}>
          <span className="text-muted">מספר כנסת</span>
          <select value={knesset} onChange={(e) => setKnesset(e.target.value)} style={{ ...inputStyle, minWidth: 150 }}>
            <option value="">כל הכנסות</option>
            {knessets.map((k) => (
              <option key={k.knesset} value={k.knesset}>
                הכנסת ה-{k.knesset} ({k.doc_count.toLocaleString()})
              </option>
            ))}
          </select>
        </label>

        <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.82rem" }}>
          <span className="text-muted">שם ועדה</span>
          <select
            value={committeeId}
            onChange={(e) => setCommitteeId(e.target.value)}
            disabled={!knesset}
            title={!knesset ? "בחרו כנסת תחילה" : undefined}
            style={{ ...inputStyle, minWidth: 260, maxWidth: 420 }}
          >
            <option value="">{knesset ? "כל הוועדות" : "בחרו כנסת תחילה"}</option>
            {committees.map((c) => (
              <option key={c.committee_id} value={c.committee_id}>
                {c.name} ({c.doc_count.toLocaleString()})
              </option>
            ))}
          </select>
        </label>

        <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.82rem", flex: "1 1 220px" }}>
          <span className="text-muted">טקסט חופשי (כותרת הפרוטוקול)</span>
          <input type="search" value={q} onChange={(e) => setQ(e.target.value)} placeholder="לדוגמה: תקציב, ביטחון…" style={inputStyle} />
        </label>

        <button
          type="submit"
          disabled={loading}
          style={{ padding: "0.45rem 1.3rem", borderRadius: 4, border: "none", fontWeight: 600, background: "var(--primary, #0f766e)", color: "white", cursor: loading ? "wait" : "pointer", opacity: loading ? 0.7 : 1 }}
        >
          {loading ? "מחפש…" : "🔍 חפש"}
        </button>
      </form>

      {error && <div style={{ color: "var(--danger, #dc2626)", fontSize: "0.88rem", marginBottom: "0.6rem" }}>{error}</div>}

      {searched && !error && (
        <>
          <div className="text-sm text-muted" style={{ marginBottom: "0.4rem" }}>
            {total.toLocaleString()} פרוטוקולים נמצאו
            {total > PAGE && <> · מציג {offset + 1}–{Math.min(offset + PAGE, total)}</>}
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
              <thead>
                <tr>
                  {["כנסת", "ועדה", "מס׳ ישיבה", "תאריך", "פרוטוקול", ""].map((h) => (
                    <th key={h} style={{ textAlign: "start", padding: "0.4rem 0.6rem", borderBottom: "2px solid var(--border, #cbd5e1)", background: "var(--bg-muted, #eef2f5)", whiteSpace: "nowrap" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.document_id} style={{ borderBottom: "1px solid var(--border, #f1f5f9)" }}>
                    <td style={{ padding: "0.35rem 0.6rem", whiteSpace: "nowrap" }}>{r.knesset_num ?? ""}</td>
                    <td style={{ padding: "0.35rem 0.6rem" }}>{r.committee_name}</td>
                    <td style={{ padding: "0.35rem 0.6rem", whiteSpace: "nowrap" }}>{r.session_number ?? ""}</td>
                    <td style={{ padding: "0.35rem 0.6rem", whiteSpace: "nowrap" }}>{fmtDate(r.session_date)}</td>
                    <td style={{ padding: "0.35rem 0.6rem" }}>{r.document_name}</td>
                    <td style={{ padding: "0.35rem 0.6rem", whiteSpace: "nowrap" }}>
                      {r.file_url && (
                        <a href={r.file_url} target="_blank" rel="noreferrer" style={{ color: "var(--primary, #0f766e)", fontWeight: 600 }}>
                          פתח ({(r.application || "DOC").toUpperCase()})
                        </a>
                      )}
                    </td>
                  </tr>
                ))}
                {rows.length === 0 && (
                  <tr><td colSpan={6} style={{ padding: "1rem", textAlign: "center", color: "var(--text-muted)" }}>לא נמצאו פרוטוקולים לסינון זה.</td></tr>
                )}
              </tbody>
            </table>
          </div>
          {total > PAGE && (
            <div className="flex" style={{ gap: "0.75rem", alignItems: "center", justifyContent: "center", marginTop: "0.75rem" }}>
              <button type="button" disabled={offset === 0 || loading} onClick={() => doSearch(Math.max(0, offset - PAGE))}
                style={{ padding: "0.35rem 0.9rem", borderRadius: 4, border: "1px solid var(--border, #d1d5db)", background: "none", cursor: offset === 0 ? "default" : "pointer", opacity: offset === 0 ? 0.5 : 1 }}>
                ← הקודם
              </button>
              <span className="text-sm text-muted">עמוד {Math.floor(offset / PAGE) + 1} מתוך {Math.ceil(total / PAGE)}</span>
              <button type="button" disabled={offset + PAGE >= total || loading} onClick={() => doSearch(offset + PAGE)}
                style={{ padding: "0.35rem 0.9rem", borderRadius: 4, border: "1px solid var(--border, #d1d5db)", background: "none", cursor: offset + PAGE >= total ? "default" : "pointer", opacity: offset + PAGE >= total ? 0.5 : 1 }}>
                הבא →
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
