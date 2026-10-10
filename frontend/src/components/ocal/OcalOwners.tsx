import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { ocal, OcalOwner, OcalOwnerDetail } from "../../api/client";
import { fmtDateHe } from "./ocalUtils";
import { useOcalOwnersInfo } from "./OwnerSelect";

type Has = "" | "both" | "expenses";

const nis = (n: number | null | undefined) =>
  n == null ? "—" : `₪${Math.round(n).toLocaleString("he-IL")}`;

const nisExact = (n: number) =>
  `₪${n.toLocaleString("he-IL", { minimumFractionDigits: 0, maximumFractionDigits: 2 })}`;

function matches(o: OcalOwner, q: string): boolean {
  if (!q) return true;
  const hay = `${o.label} ${o.mk_name || ""} ${o.role || ""}`.replace(/["'׳״-]/g, "");
  return q.replace(/["'׳״-]/g, "").split(/\s+/).filter(Boolean).every((t) => hay.includes(t));
}

/**
 * Diary owners — every person (or, for a diary that names nobody, the office)
 * the diary titles name. When the contact-with-the-voter expenses layer is on
 * (it stays off until a data source is loaded), each owner also shows their
 * expenses. The owner lives in the URL (?tab=owners&owner=) so a page like
 * "all of מאי גולן's diaries" can be shared.
 */
export default function OcalOwners() {
  const [searchParams, setSearchParams] = useSearchParams();
  const selected = searchParams.get("owner") || "";
  const { owners, expensesEnabled } = useOcalOwnersInfo();
  const [q, setQ] = useState("");
  const [has, setHas] = useState<Has>("");

  const shown = useMemo(() => {
    let list = owners.filter((o) => matches(o, q.trim()));
    if (has === "both") list = list.filter((o) => o.diary_count > 0 && o.expense_total != null);
    if (has === "expenses") list = list.filter((o) => o.expense_total != null);
    return list;
  }, [owners, q, has]);

  const select = (key: string) => {
    const sp = new URLSearchParams(searchParams);
    sp.set("tab", "owners");
    if (key) sp.set("owner", key);
    else sp.delete("owner");
    setSearchParams(sp);
  };

  const n = (k: Has) => k === "both"
    ? owners.filter((o) => o.diary_count > 0 && o.expense_total != null).length
    : k === "expenses" ? owners.filter((o) => o.expense_total != null).length : owners.length;
  const chip = (k: Has, label: string) => (
    <button
      type="button"
      onClick={() => setHas(k)}
      aria-pressed={has === k}
      style={{
        fontSize: "0.8rem", padding: "0.2rem 0.6rem", borderRadius: 12, cursor: "pointer",
        border: "1px solid var(--border)",
        background: has === k ? "var(--primary)" : "none",
        color: has === k ? "#fff" : "var(--text-muted)",
      }}
    >
      {label} ({n(k)})
    </button>
  );

  return (
    <div style={{ display: "flex", gap: "1rem", alignItems: "flex-start", flexWrap: "wrap" }}>
      <div style={{ flex: "1 1 260px", maxWidth: 380, minWidth: 0 }}>
        <input
          type="search"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="חיפוש בעל יומן (למשל: מאי גולן)…"
          aria-label="חיפוש בעל יומן"
          style={{ width: "100%", padding: "0.4rem 0.6rem", border: "1px solid var(--border)", borderRadius: 4, boxSizing: "border-box" }}
        />
        {expensesEnabled ? (
          <div style={{ display: "flex", gap: "0.35rem", flexWrap: "wrap", margin: "0.5rem 0" }}>
            {chip("", "הכל")}
            {chip("both", "יומנים + הוצאות")}
            {chip("expenses", "עם הוצאות קשר עם הבוחר")}
          </div>
        ) : (
          <div className="text-sm text-muted" style={{ margin: "0.5rem 0" }}>{owners.length} בעלי יומנים</div>
        )}
        <div role="listbox" aria-label="בעלי יומנים" className="scroll-region" tabIndex={0}
          style={{ maxHeight: 560, overflowY: "auto", border: "1px solid var(--border)", borderRadius: 6 }}>
          {shown.map((o) => (
            <button
              key={o.key}
              type="button"
              role="option"
              aria-selected={o.key === selected}
              onClick={() => select(o.key)}
              style={{
                display: "block", width: "100%", textAlign: "start", padding: "0.45rem 0.65rem",
                border: "none", borderBottom: "1px solid var(--border)", cursor: "pointer",
                background: o.key === selected ? "var(--surface-2)" : "none", color: "inherit",
              }}
            >
              <div style={{ fontWeight: o.key === selected ? 700 : 500, fontSize: "0.9rem" }}>
                {o.label}
                {o.kind === "subject" && <span className="text-muted" style={{ fontWeight: 400 }}> · יומן תפקיד</span>}
              </div>
              <div className="text-sm text-muted">
                {o.diary_count > 0 && `${o.diary_count} יומנים · ${o.event_count.toLocaleString()} אירועים`}
                {o.diary_count > 0 && o.expense_total != null && " · "}
                {o.expense_total != null && `הוצאות קשר: ${nis(o.expense_total)}`}
              </div>
            </button>
          ))}
          {owners.length > 0 && shown.length === 0 && (
            <div className="text-sm text-muted" style={{ padding: "0.8rem" }}>לא נמצאו בעלי יומנים.</div>
          )}
          {owners.length === 0 && <div className="text-sm text-muted" style={{ padding: "0.8rem" }}>טוען…</div>}
        </div>
      </div>

      <div style={{ flex: "3 1 420px", minWidth: 0 }}>
        {selected ? <OwnerDetail key={selected} ownerKey={selected} /> : (
          <div className="card text-muted" style={{ padding: "1rem" }}>
            בחרו בעל יומן מהרשימה כדי לראות את כל היומנים שלו
            {expensesEnabled && " ואת הוצאות הקשר עם הבוחר שלו"}.
          </div>
        )}
      </div>
    </div>
  );
}

function OwnerDetail({ ownerKey }: { ownerKey: string }) {
  const [, setSearchParams] = useSearchParams();
  const [d, setD] = useState<OcalOwnerDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    setD(null);
    setError(null);
    ocal.ownerDetail(ownerKey)
      .then((r) => { if (live) setD(r); })
      .catch((e) => { if (live) setError(e?.message || "שגיאה בטעינה"); });
    return () => { live = false; };
  }, [ownerKey]);

  if (error) return <div style={{ color: "var(--danger)" }}>{error}</div>;
  if (!d) return <div className="text-sm text-muted">טוען…</div>;
  const { owner, sources, expenses } = d;
  const goto = (tab: "search" | "calendar") =>
    setSearchParams(new URLSearchParams(tab === "search" ? { owner: owner.key } : { tab, owner: owner.key }));

  const th: React.CSSProperties = { textAlign: "start", padding: "0.4rem 0.55rem", borderBottom: "2px solid var(--border)", fontSize: "0.8rem", background: "var(--surface-2)" };
  const td: React.CSSProperties = { padding: "0.38rem 0.55rem", fontSize: "0.85rem", verticalAlign: "top", borderBottom: "1px solid var(--border)" };
  const maxYear = Math.max(1, ...(expenses?.by_year || []).map((y) => y.amount));

  return (
    <div>
      <h2 style={{ margin: "0 0 0.2rem" }}>{owner.label}</h2>
      <div className="text-sm text-muted" style={{ marginBottom: "0.6rem", lineHeight: 1.7 }}>
        {owner.role && <div>{owner.role}</div>}
        {owner.mk_name && owner.mk_name !== owner.label && <div>בדיווחי ההוצאות: {owner.mk_name}</div>}
        {owner.diary_count > 0 && (
          <div>
            {owner.diary_count} יומנים · {owner.event_count.toLocaleString()} אירועים
            {owner.first_event_date && ` · ${fmtDateHe(owner.first_event_date)} – ${fmtDateHe(owner.last_event_date)}`}
          </div>
        )}
      </div>
      {owner.diary_count > 0 && (
        <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginBottom: "1rem" }}>
          <button type="button" className="btn-primary" onClick={() => goto("search")}>🔍 כל האירועים ביומנים</button>
          <button type="button" className="btn-secondary" onClick={() => goto("calendar")}>📅 בלוח השנה</button>
        </div>
      )}

      <section aria-labelledby="owner-diaries" style={{ marginBottom: "1.4rem" }}>
        <h3 id="owner-diaries" style={{ fontSize: "1.05rem", margin: "0 0 0.5rem" }}>יומנים ({sources.length})</h3>
        {sources.length === 0 ? (
          <div className="text-sm text-muted">אין יומנים לבעלים זה במאגר.</div>
        ) : (
          <div className="scroll-region" tabIndex={0} role="region" aria-label="היומנים" style={{ overflowX: "auto", border: "1px solid var(--border)", borderRadius: 6 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 560 }}>
              <thead>
                <tr>
                  <th scope="col" style={th}>יומן</th>
                  <th scope="col" style={{ ...th, textAlign: "end" }}>אירועים</th>
                  <th scope="col" style={th}>טווח</th>
                  <th scope="col" style={th}>הורדה</th>
                </tr>
              </thead>
              <tbody>
                {sources.map((s) => (
                  <tr key={s.id}>
                    <td style={td}>
                      <span style={{ display: "inline-flex", gap: "0.4rem", alignItems: "baseline" }}>
                        <span aria-hidden style={{ width: 9, height: 9, borderRadius: "50%", background: s.color || "#3B82F6", flex: "0 0 auto" }} />
                        <span>
                          {s.dataset_url
                            ? <a href={s.dataset_url} target="_blank" rel="noopener noreferrer">{s.name}<span className="sr-only"> (נפתח בחלון חדש)</span></a>
                            : s.name}
                          {(s.role || (s.co_owners && s.co_owners.length > 0)) && (
                            <div className="text-sm text-muted">
                              {s.role}
                              {s.role && s.co_owners && s.co_owners.length > 0 && " · "}
                              {s.co_owners && s.co_owners.length > 0 && `משותף עם ${s.co_owners.map((c) => c.label).join(", ")}`}
                            </div>
                          )}
                        </span>
                      </span>
                    </td>
                    <td style={{ ...td, textAlign: "end" }}>{(s.total_events || 0).toLocaleString()}</td>
                    <td style={{ ...td, color: "var(--text-muted)", whiteSpace: "nowrap" }}>
                      {s.first_event_date ? `${fmtDateHe(s.first_event_date)} – ${fmtDateHe(s.last_event_date)}` : "—"}
                    </td>
                    <td style={td}>
                      <a href={ocal.downloadSourceUrl(s.id, { format: "csv" })}>CSV</a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {expenses && (
      <section aria-labelledby="owner-expenses">
        <h3 id="owner-expenses" style={{ fontSize: "1.05rem", margin: "0 0 0.3rem" }}>
          הוצאות קשר עם הבוחר{expenses.total != null && ` · סה"כ ${nis(expenses.total)}`}
        </h3>
        <div className="text-sm text-muted" style={{ marginBottom: "0.6rem", lineHeight: 1.6 }}>
          הוצאות חבר/ת הכנסת מתקציב "קשר עם הציבור", כפי שפרסמה הכנסת (מקור:{" "}
          <a href="https://www.over.org.il/versions/6ee5fb22-749f-447b-b7b0-ccec5e258b6c" target="_blank" rel="noopener noreferrer">
            הוצאות חברי הכנסת מתקציב קשר עם הציבור<span className="sr-only"> (נפתח בחלון חדש)</span>
          </a>). סכומים שליליים הם זיכויים; ל-2023 פורסם רק סיכום לפי סעיף.
        </div>
        {expenses.items.length === 0 ? (
          <div className="text-sm text-muted">
            לא נמצאו הוצאות קשר עם הבוחר בשם זה.
          </div>
        ) : (
          <>
            <table style={{ borderCollapse: "collapse", marginBottom: "1rem", width: "100%", maxWidth: 520 }}>
              <caption className="sr-only">הוצאות לפי שנה</caption>
              <thead>
                <tr>
                  <th scope="col" style={th}>שנה</th>
                  <th scope="col" style={{ ...th, textAlign: "end" }}>סכום</th>
                  <th scope="col" style={{ ...th, width: "45%" }}><span className="sr-only">יחס</span></th>
                </tr>
              </thead>
              <tbody>
                {expenses.by_year.map((y) => (
                  <tr key={String(y.year)}>
                    <th scope="row" style={{ ...td, fontWeight: 600, whiteSpace: "nowrap" }}>
                      {y.year ?? "—"}{y.partial && <span className="text-muted" style={{ fontWeight: 400 }}> (חלקית)</span>}
                    </th>
                    <td style={{ ...td, textAlign: "end", whiteSpace: "nowrap" }}>{nis(y.amount)}</td>
                    <td style={td}>
                      <div aria-hidden style={{ height: 10, borderRadius: 3, background: "var(--primary)", width: `${(100 * y.amount) / maxYear}%`, minWidth: 2 }} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {expenses.by_category.length > 0 && (
              <div className="scroll-region" tabIndex={0} role="region" aria-label="הוצאות לפי סוג" style={{ overflowX: "auto", marginBottom: "1rem" }}>
                <table style={{ borderCollapse: "collapse", width: "100%", minWidth: 420 }}>
                  <thead>
                    <tr>
                      <th scope="col" style={th}>סוג הוצאה</th>
                      <th scope="col" style={{ ...th, textAlign: "end" }}>סכום</th>
                      <th scope="col" style={th}>שנים</th>
                    </tr>
                  </thead>
                  <tbody>
                    {expenses.by_category.map((c) => (
                      <tr key={c.category || "-"}>
                        <td style={td}>{c.category || "ללא סיווג"}</td>
                        <td style={{ ...td, textAlign: "end", whiteSpace: "nowrap" }}>{nis(c.amount)}</td>
                        <td style={{ ...td, color: "var(--text-muted)" }}>{(c.years || []).join(", ")}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            <details>
              <summary style={{ cursor: "pointer" }} className="text-sm">
                כל השורות כפי שפורסמו ({expenses.item_count.toLocaleString()}
                {expenses.item_count > expenses.items.length && `, מוצגות ${expenses.items.length.toLocaleString()} האחרונות`})
              </summary>
              <div className="scroll-region" tabIndex={0} role="region" aria-label="שורות ההוצאות" style={{ overflowX: "auto", maxHeight: 420, marginTop: "0.5rem", border: "1px solid var(--border)", borderRadius: 6 }}>
                <table style={{ borderCollapse: "collapse", width: "100%", minWidth: 720 }}>
                  <thead>
                    <tr>
                      <th scope="col" style={th}>תאריך</th>
                      <th scope="col" style={th}>סעיף</th>
                      <th scope="col" style={th}>ספק / פירוט</th>
                      <th scope="col" style={{ ...th, textAlign: "end" }}>סכום</th>
                      <th scope="col" style={th}>אסמכתא</th>
                    </tr>
                  </thead>
                  <tbody>
                    {expenses.items.map((it, i) => (
                      <tr key={i} style={it.is_total ? { fontWeight: 600 } : undefined}>
                        <td style={{ ...td, whiteSpace: "nowrap" }}>
                          {it.expense_date ? fmtDateHe(it.expense_date) : (it.year ?? "—")}
                        </td>
                        <td style={td}>{it.category || "—"}{it.is_total && " (סיכום)"}</td>
                        <td style={td}>
                          {it.supplier}
                          {it.description && <div className="text-sm text-muted">{it.description}</div>}
                          {!it.supplier && !it.description && <span className="text-muted">{it.file_title || ""}</span>}
                        </td>
                        <td style={{ ...td, textAlign: "end", whiteSpace: "nowrap", color: it.amount < 0 ? "var(--success, #15803d)" : undefined }}>
                          {nisExact(it.amount)}
                        </td>
                        <td style={td}>
                          {it.receipt_url
                            ? <a href={it.receipt_url} target="_blank" rel="noopener noreferrer">קבלה<span className="sr-only"> (נפתח בחלון חדש)</span></a>
                            : <span className="text-muted">—</span>}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </details>
          </>
        )}
      </section>
      )}
    </div>
  );
}
