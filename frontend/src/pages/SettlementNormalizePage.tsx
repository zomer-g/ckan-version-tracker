import { useMemo, useRef, useState } from "react";
import DataTabs from "../components/DataTabs";
import { settlements, type ResolvedName } from "../api/client";

// Paste-a-list normalizer: paste locality names (or a CSV column) and get the
// official CBS name + code for each. All resolution happens server-side through
// the parameterized, read-only batch endpoint — nothing is stored.

const MAX = 5000;

// One name per line; for a CSV line take the first field, strip surrounding
// quotes. Blank lines dropped. Capped to MAX.
function parseNames(text: string): string[] {
  const out: string[] = [];
  for (const line of text.split(/\r?\n/)) {
    let v = line.split(",")[0].trim();
    if (v.startsWith('"') && v.endsWith('"')) v = v.slice(1, -1).trim();
    if (v) out.push(v);
    if (out.length >= MAX) break;
  }
  return out;
}

function toCsv(rows: ResolvedName[]): string {
  const esc = (v: unknown) => {
    const s = v === null || v === undefined ? "" : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const head = ["מקור", "שם רשמי", "סמל", "סוג", "נמצא"];
  const lines = [head.join(",")];
  for (const r of rows) {
    lines.push([r.input, r.official ?? "", r.code ?? "",
      r.entity === "settlement" ? "יישוב" : r.entity === "authority" ? "רשות" : "",
      r.matched ? "כן" : "לא"].map(esc).join(","));
  }
  return "﻿" + lines.join("\r\n");   // BOM for Excel/Hebrew
}

export default function SettlementNormalizePage() {
  const [text, setText] = useState("");
  const [rows, setRows] = useState<ResolvedName[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  const names = useMemo(() => parseNames(text), [text]);
  const matched = rows ? rows.filter((r) => r.matched).length : 0;

  const run = async () => {
    if (!names.length) return;
    setBusy(true); setErr(null); setRows(null); setCopied(false);
    try {
      const res = await settlements.resolveBatch(names);
      setRows(res.results);
    } catch (e) {
      setErr((e as Error)?.message || "שגיאה");
    } finally { setBusy(false); }
  };

  const onFile = (f: File | undefined) => {
    if (!f) return;
    const reader = new FileReader();
    reader.onload = () => setText(String(reader.result || ""));
    reader.readAsText(f, "utf-8");
  };

  const downloadCsv = () => {
    if (!rows) return;
    const blob = new Blob([toCsv(rows)], { type: "text/csv;charset=utf-8" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "settlements-normalized.csv";
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const copyOut = async () => {
    if (!rows) return;
    const txt = rows.map((r) => r.official ?? r.input).join("\n");
    try { await navigator.clipboard.writeText(txt); setCopied(true); setTimeout(() => setCopied(false), 1500); }
    catch { /* clipboard blocked — ignore */ }
  };

  return (
    <div className="container mt-3 nz" dir="rtl">
      <DataTabs active="normalize" />

      <header style={{ marginBottom: "1.2rem" }}>
        <div className="nz-eyebrow">גרסאות לעם · כלי ניקוי</div>
        <h1 style={{ margin: "0.3rem 0 0.5rem", fontSize: "1.7rem" }}>נרמול רשימת שמות יישובים</h1>
        <p className="nz-lead" style={{ maxWidth: "60ch" }}>
          הדביקו רשימת שמות יישובים או רשויות (שם בכל שורה, או עמודה מ-CSV) וקבלו בחזרה את השם הרשמי והסמל
          של כל אחד — כולל תיקון כתיב, קידומות, גרשיים ושמות ישנים.
        </p>
      </header>

      <div className="nz-grid">
        {/* input */}
        <section className="nz-card">
          <div className="nz-cardhead">
            <b>הרשימה שלכם</b>
            <span className="nz-count">{names.length ? `${names.length} שמות` : "שם בכל שורה"}</span>
          </div>
          <textarea
            className="nz-ta" value={text} onChange={(e) => setText(e.target.value)}
            placeholder={"בתל אביב\nהרצליה\nקרית שמונה\nנצרת עילית\nמ.א. מטה בנימין"}
            spellCheck={false}
          />
          <div className="nz-actions">
            <button className="nz-btn" onClick={run} disabled={busy || !names.length}>
              {busy ? "מנרמל…" : "▸ נרמל"}
            </button>
            <button className="nz-btn ghost" onClick={() => fileRef.current?.click()}>העלאת CSV / TXT</button>
            <input ref={fileRef} type="file" accept=".csv,.txt,text/csv,text/plain" style={{ display: "none" }}
              onChange={(e) => { onFile(e.target.files?.[0]); e.target.value = ""; }} />
            {text && <button className="nz-btn ghost" onClick={() => { setText(""); setRows(null); }}>ניקוי</button>}
          </div>
          {names.length >= MAX && <p className="nz-warn">מוצגות {MAX} השורות הראשונות בלבד.</p>}
        </section>

        {/* output */}
        <section className="nz-card">
          <div className="nz-cardhead">
            <b>התוצאה</b>
            {rows && <span className="nz-count">{matched}/{rows.length} נמצאו ({rows.length ? Math.round(matched / rows.length * 100) : 0}%)</span>}
          </div>

          {err && <div className="nz-err">{err}</div>}
          {!rows && !err && <div className="nz-empty">הדביקו רשימה ולחצו "נרמל" — התוצאה תופיע כאן.</div>}

          {rows && (
            <>
              <div className="nz-tblwrap">
                <table className="nz-tbl">
                  <thead><tr><th>מקור</th><th>שם רשמי</th><th>סמל</th></tr></thead>
                  <tbody>
                    {rows.map((r, i) => (
                      <tr key={i} className={r.matched ? "" : "miss"}>
                        <td>{r.input}</td>
                        <td>{r.matched ? r.official : <span className="nz-nomatch">לא נמצא</span>}
                          {r.entity === "authority" && <span className="nz-tagchip">רשות</span>}
                        </td>
                        <td className="num">{r.code ?? ""}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="nz-actions">
                <button className="nz-btn" onClick={downloadCsv}>הורדת CSV</button>
                <button className="nz-btn ghost" onClick={copyOut}>{copied ? "הועתק ✓" : "העתקת עמודה מתוקנת"}</button>
              </div>
            </>
          )}
        </section>
      </div>

      <div className="nz-note">
        <span className="nz-lock">🔒</span>
        <div>
          <b>פרטיות ואבטחה.</b> השמות נשלחים לשרת רק כדי להיפתר, ו<b>אינם נשמרים ואינם נרשמים</b> בשום מקום.
          ההתאמה נעשית בחיפוש פרמטרי בלבד (אין שום דרך להזריק קוד דרך השמות), על הרשאת-קריאה-בלבד, ומוגבלת
          ל-{MAX.toLocaleString()} שמות לבקשה.
        </div>
      </div>

      <style>{`
        .nz h1 { text-wrap: balance; }
        .nz-eyebrow { font-size: 0.72rem; letter-spacing: 0.12em; font-weight: 700; text-transform: uppercase; color: #a21caf; }
        .nz-lead { color: var(--text-muted, #64748b); line-height: 1.7; }
        .nz-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; align-items: start; }
        @media (max-width: 720px) { .nz-grid { grid-template-columns: 1fr; } }
        .nz-card { border: 1px solid var(--border, #e5e7eb); border-radius: 12px; background: var(--bg, #fff); padding: 1.1rem 1.2rem; }
        .nz-cardhead { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 0.6rem; }
        .nz-count { font-size: 0.8rem; color: var(--text-muted, #64748b); }
        .nz-ta { width: 100%; min-height: 260px; resize: vertical; border: 1px solid var(--border, #cbd5e1); border-radius: 8px; padding: 0.7rem; font-family: ui-monospace, Consolas, monospace; font-size: 0.9rem; line-height: 1.6; direction: rtl; background: var(--bg, #fff); color: var(--text, inherit); }
        .nz-actions { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 0.7rem; }
        .nz-btn { padding: 0.42rem 1rem; border-radius: 6px; font-weight: 600; font-size: 0.88rem; cursor: pointer; background: var(--primary, #0f766e); color: #fff; border: 1px solid var(--primary, #0f766e); }
        .nz-btn:disabled { opacity: 0.55; cursor: not-allowed; }
        .nz-btn.ghost { background: transparent; color: var(--primary, #0f766e); }
        .nz-warn { font-size: 0.8rem; color: #b45309; margin: 0.5rem 0 0; }
        .nz-empty { color: var(--text-muted, #94a3b8); font-size: 0.9rem; padding: 2rem 0; text-align: center; }
        .nz-err { color: #b91c1c; background: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 0.6rem 0.8rem; font-size: 0.88rem; }
        .nz-tblwrap { overflow: auto; max-height: 360px; border: 1px solid var(--border, #e5e7eb); border-radius: 8px; }
        .nz-tbl { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
        .nz-tbl th, .nz-tbl td { text-align: right; padding: 7px 12px; border-bottom: 1px solid var(--border, #eef2f5); }
        .nz-tbl thead th { position: sticky; top: 0; background: var(--bg-muted, #eef2f5); color: var(--primary, #0f766e); font-weight: 700; font-size: 0.8rem; }
        .nz-tbl tr.miss td { background: color-mix(in srgb, #b45309 6%, transparent); }
        .nz-tbl td.num { font-variant-numeric: tabular-nums; font-family: ui-monospace, Consolas, monospace; color: var(--text-muted, #64748b); }
        .nz-nomatch { color: #b45309; }
        .nz-tagchip { font-size: 0.68rem; font-weight: 600; padding: 1px 7px; border-radius: 999px; background: #e0f2fe; color: #0369a1; margin-inline-start: 6px; }
        .nz-note { display: flex; gap: 12px; align-items: flex-start; margin-top: 1.2rem; padding: 0.9rem 1.1rem; background: var(--bg-muted, #f8fafc); border: 1px solid var(--border, #e5e7eb); border-radius: 10px; font-size: 0.88rem; color: var(--text-muted, #64748b); line-height: 1.7; }
        .nz-lock { font-size: 1.1rem; }
      `}</style>
    </div>
  );
}
