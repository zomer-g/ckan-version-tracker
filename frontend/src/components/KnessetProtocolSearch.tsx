import { useState, useEffect, useCallback, Fragment } from "react";
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

function DetailRow({ label, value }: { label: string; value?: string | number | null }) {
  if (value === null || value === undefined || value === "") return null;
  return (
    <div style={{ display: "flex", gap: "0.5rem", padding: "0.15rem 0" }}>
      <span style={{ color: "var(--text-muted)", minWidth: 120, flex: "0 0 auto" }}>{label}</span>
      <span>{value}</span>
    </div>
  );
}

export default function KnessetProtocolSearch() {
  const [knessets, setKnessets] = useState<{ knesset: number; doc_count: number }[]>([]);
  const [committees, setCommittees] = useState<ProtocolCommittee[]>([]);
  const [knesset, setKnesset] = useState<string>("");
  const [committee, setCommittee] = useState<string>("");
  const [q, setQ] = useState("");

  const [rows, setRows] = useState<ProtocolRow[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searched, setSearched] = useState(false);

  const [expandAll, setExpandAll] = useState(false);
  const [expanded, setExpanded] = useState<Set<number>>(new Set());

  useEffect(() => {
    knessetProtocols.knessets().then((r) => setKnessets(r.knessets)).catch(() => {});
  }, []);

  // Committee autocomplete list follows the chosen Knesset (or top committees
  // globally when none is selected).
  useEffect(() => {
    knessetProtocols
      .committees({ knesset: knesset ? Number(knesset) : undefined, limit: 1000 })
      .then((r) => setCommittees(r.committees))
      .catch(() => setCommittees([]));
  }, [knesset]);

  const doSearch = useCallback(
    (newOffset: number) => {
      setLoading(true);
      setError(null);
      setExpanded(new Set());
      knessetProtocols
        .search({
          q: q.trim() || undefined,
          knesset: knesset ? Number(knesset) : undefined,
          committee: committee.trim() || undefined,
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
    [q, knesset, committee],
  );

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    doSearch(0);
  };
  const toggle = (id: number) =>
    setExpanded((prev) => {
      const n = new Set(prev);
      n.has(id) ? n.delete(id) : n.add(id);
      return n;
    });

  const detail = (r: ProtocolRow) => (
    <div style={{ fontSize: "0.82rem", background: "var(--bg-muted, #f8fafc)", padding: "0.6rem 0.9rem", lineHeight: 1.5 }}>
      <DetailRow label="ועדה" value={r.committee_name} />
      <DetailRow label="סוג ועדה" value={r.committee_type} />
      <DetailRow label="כנסת" value={r.knesset_num} />
      <DetailRow label="מספר ישיבה" value={r.session_number} />
      <DetailRow label="תאריך" value={fmtDate(r.session_date)} />
      <DetailRow label="מיקום" value={r.session_location} />
      <DetailRow label="הערת ישיבה" value={r.session_note} />
      <DetailRow label="שם המסמך" value={r.document_name} />
      <DetailRow label="פורמט" value={(r.application || "").toUpperCase()} />
      <DetailRow label="עודכן" value={fmtDate(r.last_updated)} />
      {r.file_url && (
        <div style={{ marginTop: "0.35rem" }}>
          <a href={r.file_url} target="_blank" rel="noreferrer" style={{ color: "var(--primary, #0f766e)", fontWeight: 600 }}>
            הורדת הפרוטוקול ({(r.application || "DOC").toUpperCase()}) ↓
          </a>
        </div>
      )}
    </div>
  );

  return (
    <div>
      <p className="text-sm text-muted" style={{ marginTop: 0, lineHeight: 1.6 }}>
        חיפוש בכל פרוטוקולי ועדות הכנסת (מסונכרן מפיד ה-ODATA). סננו לפי{" "}
        <strong>מספר כנסת</strong> ו/או <strong>שם ועדה</strong>, והוסיפו טקסט חופשי — החיפוש
        סורק את כותרת הפרוטוקול, שם הוועדה, ומיקום/הערות הישיבה. הרחיבו שורה להצגת
        המטא-דאטה המלא (כולל היכן שהמונח מופיע). מוצגים מסמכים בלבד (מסמך אחד לכל
        פרוטוקול); סריקות תמונה והקלטות וידאו אינן מוצגות.
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

        <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.82rem", flex: "1 1 240px" }}>
          <span className="text-muted">שם ועדה</span>
          <input
            list="knesset-committee-list"
            value={committee}
            onChange={(e) => setCommittee(e.target.value)}
            placeholder="הקלד/י או בחר/י ועדה (חלקי מספיק)…"
            style={inputStyle}
          />
          <datalist id="knesset-committee-list">
            {committees.map((c) => (
              <option key={c.committee_id} value={c.name}>
                {c.doc_count.toLocaleString()} פרוטוקולים
              </option>
            ))}
          </datalist>
        </label>

        <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.82rem", flex: "1 1 200px" }}>
          <span className="text-muted">טקסט חופשי</span>
          <input type="search" value={q} onChange={(e) => setQ(e.target.value)} placeholder="לדוגמה: תקציב, יריחו…" style={inputStyle} />
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
          <div className="flex-between" style={{ flexWrap: "wrap", gap: "0.5rem", marginBottom: "0.4rem" }}>
            <span className="text-sm text-muted">
              {total.toLocaleString()} פרוטוקולים נמצאו
              {total > PAGE && <> · מציג {offset + 1}–{Math.min(offset + PAGE, total)}</>}
            </span>
            {rows.length > 0 && (
              <label className="text-sm text-muted" style={{ display: "flex", gap: "0.35rem", alignItems: "center", cursor: "pointer" }}>
                <input type="checkbox" checked={expandAll} onChange={(e) => setExpandAll(e.target.checked)} />
                תצוגה מורחבת (מטא-דאטה מלא)
              </label>
            )}
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
              <thead>
                <tr>
                  {["", "כנסת", "ועדה", "מס׳ ישיבה", "תאריך", "פרוטוקול", ""].map((h, i) => (
                    <th key={i} style={{ textAlign: "start", padding: "0.4rem 0.6rem", borderBottom: "2px solid var(--border, #cbd5e1)", background: "var(--bg-muted, #eef2f5)", whiteSpace: "nowrap" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const isOpen = expandAll || expanded.has(r.document_id);
                  return (
                    <Fragment key={r.document_id}>
                      <tr
                        onClick={() => toggle(r.document_id)}
                        style={{ borderBottom: isOpen ? "none" : "1px solid var(--border, #f1f5f9)", cursor: "pointer" }}
                        title="לחצו להרחבת המטא-דאטה"
                      >
                        <td style={{ padding: "0.35rem 0.6rem", color: "var(--text-muted)" }}>{isOpen ? "▾" : "▸"}</td>
                        <td style={{ padding: "0.35rem 0.6rem", whiteSpace: "nowrap" }}>{r.knesset_num ?? ""}</td>
                        <td style={{ padding: "0.35rem 0.6rem" }}>{r.committee_name}</td>
                        <td style={{ padding: "0.35rem 0.6rem", whiteSpace: "nowrap" }}>{r.session_number ?? ""}</td>
                        <td style={{ padding: "0.35rem 0.6rem", whiteSpace: "nowrap" }}>{fmtDate(r.session_date)}</td>
                        <td style={{ padding: "0.35rem 0.6rem" }}>{r.document_name}</td>
                        <td style={{ padding: "0.35rem 0.6rem", whiteSpace: "nowrap" }}>
                          {r.file_url && (
                            <a href={r.file_url} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()} style={{ color: "var(--primary, #0f766e)", fontWeight: 600 }}>
                              פתח ({(r.application || "DOC").toUpperCase()})
                            </a>
                          )}
                        </td>
                      </tr>
                      {isOpen && (
                        <tr style={{ borderBottom: "1px solid var(--border, #e2e8f0)" }}>
                          <td colSpan={7} style={{ padding: 0 }}>{detail(r)}</td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
                {rows.length === 0 && (
                  <tr><td colSpan={7} style={{ padding: "1rem", textAlign: "center", color: "var(--text-muted)" }}>לא נמצאו פרוטוקולים לסינון זה.</td></tr>
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
