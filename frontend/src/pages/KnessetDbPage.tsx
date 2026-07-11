import { useState, useEffect, useMemo, useCallback } from "react";
import { Link } from "react-router-dom";
import {
  knessetDb,
  KnessetDbTable,
  KnessetDbStatus,
  KnessetDbSqlResult,
} from "../api/client";
import { useAuth } from "../auth/AuthContext";
import KnessetProtocolSearch from "../components/KnessetProtocolSearch";

type KnessetTab = "protocols" | "sql" | "mmm";

// DD.MM.YYYY HH:MM (Israel-style, like the other pages).
function fmtDate(value: string | null | undefined): string {
  if (!value) return "";
  const d = new Date(value);
  if (isNaN(d.getTime())) return value.slice(0, 19);
  const p = (n: number) => String(n).padStart(2, "0");
  return `${p(d.getDate())}.${p(d.getMonth() + 1)}.${d.getFullYear()} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

// Client-side CSV of an in-memory SQL result (utf-8 BOM for Excel/Hebrew).
function downloadRowsCsv(
  filename: string,
  columns: string[],
  rows: Array<Record<string, unknown>>,
) {
  const esc = (v: unknown) => {
    const s = v == null ? "" : String(v);
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [columns.map(esc).join(",")];
  for (const r of rows) lines.push(columns.map((c) => esc(r[c])).join(","));
  const blob = new Blob(["﻿" + lines.join("\r\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

const EXAMPLES: { label: string; sql: string }[] = [
  {
    label: "הצעות חוק לפי כנסת",
    sql: "SELECT knessetnum, count(*) AS bills\nFROM kns_bill\nGROUP BY knessetnum\nORDER BY knessetnum DESC",
  },
  {
    label: "הצעות חוק של הכנסת ה-25 + סטטוס (JOIN)",
    sql: 'SELECT b.name, s."desc" AS status, b.lastupdateddate\nFROM kns_bill b\nJOIN kns_status s ON s.id = b.statusid\nWHERE b.knessetnum = 25\nORDER BY b.id DESC\nLIMIT 50',
  },
  {
    label: "ההצבעות האישיות האחרונות במליאה",
    sql: "SELECT votedate, firstname, lastname, resultdesc\nFROM kns_plenumvoteresult\nORDER BY id DESC\nLIMIT 100",
  },
  {
    label: "מי הצביע הכי הרבה מאז 2025",
    sql: "SELECT firstname, lastname, count(*) AS votes\nFROM kns_plenumvoteresult\nWHERE votedate > '2025-01-01'\nGROUP BY firstname, lastname\nORDER BY votes DESC\nLIMIT 30",
  },
  {
    label: "שדלנים ולקוחותיהם",
    sql: "SELECT l.fullname, l.corporationname, c.name AS client\nFROM v_lobbyists l\nJOIN v_lobbyistsclients c ON c.lobbyistid = l.id\nORDER BY l.fullname\nLIMIT 100",
  },
];

const DEFAULT_SQL = EXAMPLES[0].sql;

// Topic order for the table browser (mirrors GROUP_ORDER in
// app/services/knesset_tables_meta.py).
const GROUP_ORDER = [
  "הצעות חוק", "חוקי מדינת ישראל", "חקיקת משנה", "ועדות הכנסת", "מליאת הכנסת",
  "חברי הכנסת", "שאילתות", "הצעות לסדר היום", "שדלנים", "טבלאות עזר", "אחר",
];

export default function KnessetDbPage() {
  const { user } = useAuth();
  const [tab, setTab] = useState<KnessetTab>("protocols");
  const [status, setStatus] = useState<KnessetDbStatus | null>(null);
  const [tables, setTables] = useState<KnessetDbTable[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const [sqlText, setSqlText] = useState(DEFAULT_SQL);
  const [sqlResult, setSqlResult] = useState<KnessetDbSqlResult | null>(null);
  const [sqlError, setSqlError] = useState<string | null>(null);
  const [sqlRunning, setSqlRunning] = useState(false);
  const [syncMsg, setSyncMsg] = useState<string | null>(null);

  const load = useCallback(() => {
    knessetDb.status().then(setStatus).catch(() => {});
    knessetDb
      .tables()
      .then((r) => { setTables(r.tables); setLoadError(null); })
      .catch((e) => setLoadError(e?.message || "שגיאה בטעינת רשימת הטבלאות"));
  }, []);

  useEffect(() => { load(); }, [load]);

  const runSql = useCallback(() => {
    if (!sqlText.trim()) return;
    setSqlRunning(true);
    setSqlError(null);
    knessetDb
      .sql(sqlText)
      .then((r) => { setSqlResult(r); setSqlError(null); })
      .catch((e) => { setSqlResult(null); setSqlError(e?.message || "שגיאה"); })
      .finally(() => setSqlRunning(false));
  }, [sqlText]);

  const groups = useMemo(() => {
    const f = filter.trim().toLowerCase();
    const shown = tables.filter(
      (t) =>
        !f ||
        t.table.includes(f) ||
        t.entity_set.toLowerCase().includes(f) ||
        t.description.toLowerCase().includes(f),
    );
    const m = new Map<string, KnessetDbTable[]>();
    for (const g of GROUP_ORDER) m.set(g, []);
    for (const t of shown) {
      if (!m.has(t.group)) m.set(t.group, []);
      m.get(t.group)!.push(t);
    }
    for (const [g, list] of m) if (list.length === 0) m.delete(g);
    return m;
  }, [tables, filter]);

  const selectedTable = useMemo(
    () => tables.find((t) => t.table === selected) || null,
    [tables, selected],
  );

  const pickTable = (t: KnessetDbTable) => {
    setSelected(t.table);
    setSqlText(`SELECT *\nFROM ${t.table}\nORDER BY id DESC\nLIMIT 100`);
  };

  const adminSync = (opts: { table?: string; reset?: boolean }) => {
    setSyncMsg(null);
    knessetDb
      .sync(opts)
      .then(() => setSyncMsg("סנכרון הופעל ברקע — רעננו את הדף בעוד כמה דקות"))
      .catch((e) => setSyncMsg(e?.message || "שגיאה בהפעלת הסנכרון"));
  };

  const loadedPct =
    status?.tables ? Math.round(((status.loaded || 0) / status.tables) * 100) : 0;

  return (
    <div className="container mt-3">
      <div className="page-header" style={{ marginBottom: "0.75rem" }}>
        <h1 style={{ margin: 0 }}>מסד הנתונים של הכנסת</h1>
        <div className="text-sm text-muted" style={{ marginTop: "0.35rem", lineHeight: 1.6 }}>
          מראה מלאה של כל טבלאות שירות ה-ODATA הפרלמנטרי של הכנסת — הצעות חוק, חוקים,
          ועדות, הצבעות במליאה, חברי כנסת, שאילתות ושדלנים — מסונכרנת אל מסד PostgreSQL
          וזמינה לתשאול SQL חופשי (קריאה בלבד).{" "}
          <a href="https://knesset.gov.il/OdataV4/ParliamentInfo" target="_blank" rel="noreferrer" style={{ color: "var(--primary)" }}>
            הפיד המקורי
          </a>
          {" · "}
          <a href="https://main.knesset.gov.il/activity/info/pages/databases.aspx" target="_blank" rel="noreferrer" style={{ color: "var(--primary)" }}>
            תיעוד הכנסת
          </a>
          {" · "}
          <Link to="/knesset/guide" style={{ color: "var(--primary)", fontWeight: 600 }}>
            📖 מדריך תשאול מלא + קטלוג הטבלאות
          </Link>
        </div>
        {status?.enabled && (
          <div className="text-sm" style={{ marginTop: "0.4rem", color: "var(--text-muted)" }}>
            {status.loaded ?? 0}/{status.tables ?? 0} טבלאות נטענו במלואן ({loadedPct}%) ·{" "}
            {(status.rows ?? 0).toLocaleString()} שורות · פעילות אחרונה: {fmtDate(status.last_activity ?? status.last_sync) || "—"}
            {user?.is_admin && (
              <>
                {" · "}
                <button type="button" onClick={() => adminSync({})}
                  style={{ background: "none", border: "none", color: "var(--primary)", cursor: "pointer", padding: 0, fontSize: "inherit", textDecoration: "underline" }}>
                  סנכרן עכשיו
                </button>
              </>
            )}
          </div>
        )}
        {syncMsg && <div className="text-sm" style={{ marginTop: "0.3rem", color: "var(--primary)" }}>{syncMsg}</div>}
        {loadError && <div className="text-sm" style={{ marginTop: "0.3rem", color: "var(--danger, #dc2626)" }}>{loadError}</div>}
      </div>

      {/* Tabs */}
      <div className="flex" style={{ gap: "0.3rem", borderBottom: "2px solid var(--border, #e2e8f0)", marginBottom: "1rem", flexWrap: "wrap" }}>
        {([
          ["protocols", "🔍 חיפוש פרוטוקולים"],
          ["mmm", "מסמכי ממ”מ"],
          ["sql", "</> ממשק SQL"],
        ] as [KnessetTab, string][]).map(([id, label]) => (
          <button
            key={id}
            type="button"
            onClick={() => setTab(id)}
            style={{
              padding: "0.5rem 1.05rem", border: "none", cursor: "pointer", background: "none",
              fontSize: "0.95rem", fontWeight: tab === id ? 700 : 500,
              color: tab === id ? "var(--primary, #0f766e)" : "var(--text-muted)",
              borderBottom: tab === id ? "3px solid var(--primary, #0f766e)" : "3px solid transparent",
              marginBottom: -2,
            }}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === "protocols" && <KnessetProtocolSearch />}

      {tab === "mmm" && (
        <div className="card" style={{ padding: "1.5rem", textAlign: "center", color: "var(--text-muted)", lineHeight: 1.7 }}>
          <p style={{ marginTop: 0, fontSize: "1.05rem" }}>חיפוש במסמכי מרכז המחקר והמידע (ממ״מ) — בקרוב.</p>
          <p style={{ marginBottom: 0 }}>
            בינתיים ניתן לצפות במסמכי הממ״מ דרך{" "}
            <a href="https://main.knesset.gov.il/activity/info/research" target="_blank" rel="noreferrer" style={{ color: "var(--primary)" }}>
              אתר הכנסת
            </a>.
          </p>
        </div>
      )}

      {tab === "sql" && (
        <>
      {/* SQL console */}
      <div className="card" style={{ padding: "1rem", marginBottom: "1rem" }}>
        <div className="flex" style={{ gap: "0.75rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.5rem" }}>
          <strong style={{ fontSize: "0.95rem" }}>{"</>"} קונסולת SQL</strong>
          <span className="text-sm text-muted">SELECT בלבד · עד 1,000 שורות בתצוגה · שמות טבלאות ועמודות באותיות קטנות</span>
          <select
            aria-label="שאילתות לדוגמה"
            value=""
            onChange={(e) => {
              const ex = EXAMPLES.find((x) => x.label === e.target.value);
              if (ex) { setSqlText(ex.sql); setSqlResult(null); setSqlError(null); }
            }}
            style={{ marginInlineStart: "auto", padding: "0.3rem 0.5rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.82rem", maxWidth: 260 }}
          >
            <option value="">דוגמאות…</option>
            {EXAMPLES.map((ex) => <option key={ex.label} value={ex.label}>{ex.label}</option>)}
          </select>
        </div>
        <textarea
          value={sqlText}
          onChange={(e) => setSqlText(e.target.value)}
          onKeyDown={(e) => { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") runSql(); }}
          spellCheck={false}
          dir="ltr"
          rows={6}
          style={{
            width: "100%", fontFamily: "monospace", fontSize: "0.85rem", padding: "0.6rem",
            border: "1px solid var(--border, #d1d5db)", borderRadius: 4, resize: "vertical",
          }}
          aria-label="שאילתת SQL"
        />
        <div className="flex" style={{ gap: "0.75rem", alignItems: "center", marginTop: "0.5rem", flexWrap: "wrap" }}>
          <button
            type="button" onClick={runSql} disabled={sqlRunning}
            style={{
              padding: "0.4rem 1.1rem", borderRadius: 4, border: "none", fontWeight: 600,
              background: "var(--primary, #0f766e)", color: "white",
              cursor: sqlRunning ? "wait" : "pointer", opacity: sqlRunning ? 0.7 : 1,
            }}
          >
            {sqlRunning ? "מריץ…" : "▶ הרץ"}
          </button>
          <span className="text-sm text-muted">Ctrl/⌘+Enter</span>
          {sqlResult && (
            <span className="text-sm text-muted">
              {sqlResult.row_count.toLocaleString()} שורות{sqlResult.truncated ? " (נחתך ל-1,000)" : ""}
            </span>
          )}
          {sqlResult && sqlResult.rows.length > 0 && (
            <button
              type="button"
              onClick={() => downloadRowsCsv("knesset_query.csv", sqlResult.columns, sqlResult.rows)}
              style={{
                fontSize: "0.82rem", padding: "0.3rem 0.7rem", background: "none",
                color: "var(--primary, #0f766e)", border: "1px solid var(--primary, #0f766e)",
                borderRadius: 4, cursor: "pointer",
              }}
              title="הורדת התוצאה המוצגת כ-CSV"
            >
              &#8595; CSV — תוצאה
            </button>
          )}
          {sqlText.trim() && (
            <a
              href={knessetDb.exportUrl(sqlText)}
              style={{ fontSize: "0.82rem", color: "var(--text-muted)", textDecoration: "underline" }}
              title="הרצת השאילתה בשרת וייצוא מלא (עד 200,000 שורות)"
            >
              ייצוא מלא מהשרת (עד 200 אלף שורות)
            </a>
          )}
        </div>
        {sqlError && (
          <div style={{ marginTop: "0.6rem", color: "var(--danger, #dc2626)", fontSize: "0.85rem", whiteSpace: "pre-wrap" }}>
            {sqlError}
          </div>
        )}
        {sqlResult && !sqlError && (
          <div style={{ marginTop: "0.6rem", overflowX: "auto", maxHeight: 420 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem", whiteSpace: "nowrap" }}>
              <thead>
                <tr>
                  {sqlResult.columns.map((c) => (
                    <th key={c} style={{ textAlign: "start", padding: "0.4rem 0.6rem", position: "sticky", top: 0, zIndex: 1, background: "var(--bg-muted, #eef2f5)", borderBottom: "2px solid var(--border, #cbd5e1)" }}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sqlResult.rows.map((row, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid var(--border, #f1f5f9)" }}>
                    {sqlResult.columns.map((c) => (
                      <td key={c} style={{ padding: "0.35rem 0.6rem" }}>{String(row[c] ?? "")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Table browser + schema */}
      <div style={{ display: "flex", gap: "1rem", alignItems: "flex-start", flexWrap: "wrap" }}>
        <div className="card" style={{ flex: "1 1 340px", minWidth: 300, padding: "0.75rem", maxHeight: 560, overflowY: "auto" }}>
          <input
            type="search"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="סינון טבלאות…"
            aria-label="סינון טבלאות"
            style={{ width: "100%", padding: "0.4rem 0.6rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, marginBottom: "0.5rem" }}
          />
          {[...groups.entries()].map(([group, list]) => (
            <div key={group} style={{ marginBottom: "0.6rem" }}>
              <div style={{ fontWeight: 700, fontSize: "0.85rem", padding: "0.25rem 0.2rem", color: "var(--text-muted)" }}>{group}</div>
              {list.map((t) => (
                <button
                  key={t.table}
                  type="button"
                  onClick={() => pickTable(t)}
                  title={t.description}
                  style={{
                    display: "flex", width: "100%", gap: "0.5rem", alignItems: "center",
                    textAlign: "start", padding: "0.35rem 0.5rem", borderRadius: 4, cursor: "pointer",
                    border: "none",
                    background: selected === t.table ? "var(--bg-muted, #eef2f5)" : "none",
                  }}
                >
                  <span
                    aria-hidden
                    title={t.full_loaded ? "נטען במלואו" : t.status === "error" ? "שגיאה" : "בטעינה"}
                    style={{
                      width: 8, height: 8, borderRadius: "50%", flex: "0 0 auto",
                      background: t.status === "error" ? "#dc2626" : t.full_loaded ? "#16a34a" : "#f59e0b",
                    }}
                  />
                  <code style={{ fontSize: "0.8rem" }}>{t.table}</code>
                  <span className="text-sm text-muted" style={{ marginInlineStart: "auto", fontSize: "0.75rem" }}>
                    {t.total_rows > 0 ? t.total_rows.toLocaleString() : ""}
                  </span>
                </button>
              ))}
            </div>
          ))}
          {tables.length === 0 && !loadError && (
            <div className="text-sm text-muted" style={{ padding: "0.5rem" }}>טוען את רשימת הטבלאות…</div>
          )}
        </div>

        <div className="card" style={{ flex: "2 1 420px", minWidth: 300, padding: "1rem" }}>
          {!selectedTable && (
            <div className="text-sm text-muted" style={{ lineHeight: 1.7 }}>
              <p style={{ marginTop: 0 }}>בחרו טבלה מהרשימה כדי לראות את המבנה שלה ולקבל שאילתה מוכנה.</p>
              <p>
                הנתונים מסונכרנים ישירות מפיד ה-ODATA הרשמי של הכנסת: טעינה מלאה ראשונית,
                ולאחריה רענון תקופתי לפי <code>lastupdateddate</code>. שינויים במקור מתעדכנים;
                מחיקות במקור אינן מזוהות. הטבלה הגדולה ביותר — <code>kns_plenumvoteresult</code>{" "}
                (כ-2 מיליון תוצאות הצבעה אישיות) — נטענת אחרונה.
              </p>
              <p style={{ marginBottom: 0 }}>
                טיפ: שמות הטבלאות במדריך הכנסת (KNS_Bill) הופכים כאן לאותיות קטנות (kns_bill),
                וכך גם שמות העמודות. עמודות בשם שמור כמו <code>desc</code> דורשות מרכאות: <code>s."desc"</code>.
              </p>
            </div>
          )}
          {selectedTable && (
            <>
              <div className="flex-between" style={{ flexWrap: "wrap", gap: "0.5rem" }}>
                <h2 style={{ margin: 0, fontSize: "1.05rem" }}>
                  <code>{selectedTable.table}</code>{" "}
                  <span className="text-sm text-muted">({selectedTable.entity_set})</span>
                </h2>
                <span className="text-sm text-muted">
                  {selectedTable.total_rows.toLocaleString()} שורות
                  {selectedTable.source_count != null && !selectedTable.full_loaded && (
                    <> מתוך ~{selectedTable.source_count.toLocaleString()} במקור</>
                  )}
                  {" · "}
                  {selectedTable.full_loaded ? "נטען במלואו" : selectedTable.status === "error" ? "שגיאת סנכרון" : "בטעינה…"}
                  {selectedTable.last_synced_at && <> · עודכן {fmtDate(selectedTable.last_synced_at)}</>}
                </span>
              </div>
              {selectedTable.description && (
                <p className="text-sm" style={{ margin: "0.5rem 0 0.75rem", lineHeight: 1.6 }}>{selectedTable.description}</p>
              )}
              {selectedTable.error && (
                <p className="text-sm" style={{ color: "var(--danger, #dc2626)" }}>{selectedTable.error}</p>
              )}
              {user?.is_admin && (
                <div className="flex" style={{ gap: "0.5rem", marginBottom: "0.5rem" }}>
                  <button type="button" onClick={() => adminSync({ table: selectedTable.table })}
                    style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem", borderRadius: 4, border: "1px solid var(--primary)", background: "none", color: "var(--primary)", cursor: "pointer" }}>
                    סנכרן טבלה זו
                  </button>
                  <button type="button" onClick={() => adminSync({ table: selectedTable.table, reset: true })}
                    style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem", borderRadius: 4, border: "1px solid #b45309", background: "none", color: "#b45309", cursor: "pointer" }}>
                    סריקה מלאה מחדש
                  </button>
                </div>
              )}
              <div style={{ overflowX: "auto", maxHeight: 380 }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" }}>
                  <thead>
                    <tr>
                      <th style={{ textAlign: "start", padding: "0.35rem 0.6rem", borderBottom: "2px solid var(--border, #cbd5e1)", position: "sticky", top: 0, background: "var(--bg-muted, #eef2f5)" }}>עמודה</th>
                      <th style={{ textAlign: "start", padding: "0.35rem 0.6rem", borderBottom: "2px solid var(--border, #cbd5e1)", position: "sticky", top: 0, background: "var(--bg-muted, #eef2f5)" }}>טיפוס</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedTable.columns.map((c) => (
                      <tr key={c.name} style={{ borderBottom: "1px solid var(--border, #f1f5f9)" }}>
                        <td style={{ padding: "0.3rem 0.6rem" }}><code>{c.name}</code></td>
                        <td style={{ padding: "0.3rem 0.6rem", color: "var(--text-muted)" }}>{c.type}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </div>
        </>
      )}
    </div>
  );
}
