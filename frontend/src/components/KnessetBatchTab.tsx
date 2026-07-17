import { useState, useEffect, useMemo, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import {
  knessetDb,
  KnessetProtocolFacets,
  KnessetBatchFilter,
} from "../api/client";

// The "אצוות" (Batch) tab of /knesset: pick a Knesset number and/or a
// committee and download EVERY matching protocol file as one ZIP (streamed
// from the server, which fetches the files live from fs.knesset.gov.il), or
// just the link manifest as CSV. Mirrors the server caps in
// app/api/knesset_db.py.

export default function KnessetBatchTab() {
  // The selection lives in the URL (?tab=batch&knesset=25&committee=4186&q=…)
  // so a batch is deep-linkable / shareable.
  const [searchParams, setSearchParams] = useSearchParams();
  const [facets, setFacets] = useState<KnessetProtocolFacets | null>(null);
  const [facetsError, setFacetsError] = useState<string | null>(null);
  const [knesset, setKnesset] = useState<number | "">(
    () => (searchParams.get("knesset") ? Number(searchParams.get("knesset")) : ""));
  const [committeeId, setCommitteeId] = useState<number | "">(
    () => (searchParams.get("committee") ? Number(searchParams.get("committee")) : ""));
  const [committeeQuery, setCommitteeQuery] = useState(() => searchParams.get("q") || "");
  const [count, setCount] = useState<number | null>(null);
  // Real cap comes from /protocols/count (zip_max_files); this is only the
  // pre-fetch placeholder. Keep it in step with the server default.
  const [zipMax, setZipMax] = useState(200);
  const [counting, setCounting] = useState(false);

  useEffect(() => {
    knessetDb
      .protocolFacets()
      .then((f) => { setFacets(f); setFacetsError(null); })
      .catch((e) => setFacetsError(e?.message || "שגיאה בטעינת רשימת הוועדות"));
  }, []);

  // Mirror the selection into the URL (replace — no history spam while typing).
  useEffect(() => {
    const p: Record<string, string> = { tab: "batch" };
    if (knesset !== "") p.knesset = String(knesset);
    if (committeeId !== "") p.committee = String(committeeId);
    else if (committeeQuery.trim()) p.q = committeeQuery.trim();
    setSearchParams(p, { replace: true });
  }, [knesset, committeeId, committeeQuery, setSearchParams]);

  const filter: KnessetBatchFilter = useMemo(() => {
    const f: KnessetBatchFilter = {};
    if (knesset !== "") f.knesset_num = knesset;
    if (committeeId !== "") f.committee_id = committeeId;
    // Free-text committee-name filter only when no specific committee picked.
    else if (committeeQuery.trim()) f.q = committeeQuery.trim();
    return f;
  }, [knesset, committeeId, committeeQuery]);

  const hasFilter =
    filter.knesset_num !== undefined || filter.committee_id !== undefined || !!filter.q;

  // Debounced live count for the current selection.
  const tRef = useRef<number | undefined>(undefined);
  useEffect(() => {
    window.clearTimeout(tRef.current);
    if (!hasFilter) { setCount(null); return; }
    setCounting(true);
    tRef.current = window.setTimeout(() => {
      knessetDb
        .protocolCount(filter)
        .then((r) => { setCount(r.files); setZipMax(r.zip_max_files); })
        .catch(() => setCount(null))
        .finally(() => setCounting(false));
    }, 350);
    return () => window.clearTimeout(tRef.current);
  }, [filter, hasFilter]);

  const committees = useMemo(() => {
    if (!facets) return [];
    const q = committeeQuery.trim();
    return facets.committees.filter(
      (c) =>
        (knesset === "" || c.knesset_num === knesset) &&
        (!q || c.name.includes(q)),
    );
  }, [facets, knesset, committeeQuery]);

  const selectedCommittee = useMemo(
    () => facets?.committees.find((c) => c.id === committeeId) || null,
    [facets, committeeId],
  );

  const tooBig = count !== null && count > zipMax;

  return (
    <div style={{ display: "flex", gap: "1rem", alignItems: "flex-start", flexWrap: "wrap" }}>
      {/* Filters */}
      <div className="card" style={{ flex: "1 1 340px", minWidth: 300, padding: "1rem" }}>
        <h2 style={{ margin: "0 0 0.75rem", fontSize: "1.02rem" }}>בחירת אצווה</h2>

        <label className="text-sm" style={{ display: "block", marginBottom: "0.75rem" }}>
          מספר כנסת
          <select
            value={knesset}
            onChange={(e) => {
              setKnesset(e.target.value === "" ? "" : Number(e.target.value));
              setCommitteeId("");
            }}
            style={{ display: "block", width: "100%", marginTop: "0.3rem", padding: "0.4rem 0.6rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4 }}
          >
            <option value="">כל הכנסות</option>
            {facets?.knessets.map((k) => (
              <option key={k.knesset_num} value={k.knesset_num}>
                כנסת {k.knesset_num} — {k.protocols.toLocaleString()} פרוטוקולים
              </option>
            ))}
          </select>
        </label>

        <label className="text-sm" style={{ display: "block", marginBottom: "0.35rem" }}>
          ועדה
          <input
            type="search"
            value={committeeQuery}
            onChange={(e) => { setCommitteeQuery(e.target.value); setCommitteeId(""); }}
            placeholder="חיפוש שם ועדה… (ריק = כל הוועדות)"
            style={{ display: "block", width: "100%", marginTop: "0.3rem", padding: "0.4rem 0.6rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4 }}
          />
        </label>
        {facetsError && <div className="text-sm" style={{ color: "var(--danger, #dc2626)" }}>{facetsError}</div>}
        {!facets && !facetsError && <div className="text-sm text-muted">טוען רשימת ועדות…</div>}
        {facets && committeeQuery.trim() && (
          <div style={{ maxHeight: 260, overflowY: "auto", border: "1px solid var(--border, #eef2f5)", borderRadius: 4 }}>
            {committees.slice(0, 60).map((c) => (
              <button
                key={c.id}
                type="button"
                onClick={() => { setCommitteeId(c.id); setKnesset(c.knesset_num ?? ""); }}
                style={{
                  display: "flex", width: "100%", gap: "0.5rem", alignItems: "baseline",
                  textAlign: "start", padding: "0.35rem 0.6rem", border: "none", cursor: "pointer",
                  background: committeeId === c.id ? "var(--bg-muted, #eef2f5)" : "none",
                  fontSize: "0.84rem",
                }}
              >
                <span>{c.name}</span>
                <span className="text-muted" style={{ marginInlineStart: "auto", fontSize: "0.76rem", whiteSpace: "nowrap" }}>
                  כנסת {c.knesset_num ?? "?"} · {c.protocols.toLocaleString()}
                </span>
              </button>
            ))}
            {committees.length === 0 && (
              <div className="text-sm text-muted" style={{ padding: "0.5rem" }}>אין ועדות תואמות</div>
            )}
          </div>
        )}
        {selectedCommittee && (
          <div className="text-sm" style={{ marginTop: "0.5rem" }}>
            נבחרה: <strong>{selectedCommittee.name}</strong> (כנסת {selectedCommittee.knesset_num ?? "?"})
            {" · "}
            <button type="button" onClick={() => setCommitteeId("")}
              style={{ background: "none", border: "none", color: "var(--primary)", cursor: "pointer", padding: 0, fontSize: "inherit", textDecoration: "underline" }}>
              ניקוי
            </button>
          </div>
        )}
      </div>

      {/* Download */}
      <div className="card" style={{ flex: "2 1 420px", minWidth: 300, padding: "1rem" }}>
        <h2 style={{ margin: "0 0 0.5rem", fontSize: "1.02rem" }}>הורדת האצווה</h2>
        {!hasFilter ? (
          <p className="text-sm text-muted" style={{ lineHeight: 1.7 }}>
            בחרו מספר כנסת ו/או ועדה — ותקבלו קובץ ZIP אחד עם <strong>כל קובצי הפרוטוקולים</strong> של
            הבחירה, מאורגנים בתיקיות לפי ועדה, כולל קובץ אינדקס (CSV). הקבצים נמשכים ישירות משרת
            הכנסת בזמן ההורדה.
          </p>
        ) : (
          <>
            <p className="text-sm" style={{ margin: "0 0 0.75rem" }}>
              {counting || count === null
                ? "סופר פרוטוקולים…"
                : <><strong>{count.toLocaleString()}</strong> קובצי פרוטוקול בבחירה הנוכחית.</>}
            </p>
            {tooBig && (
              <p className="text-sm" style={{ color: "#b45309", lineHeight: 1.6 }}>
                האצווה גדולה ממגבלת ה-ZIP ({zipMax.toLocaleString()} קבצים). צמצמו לוועדה
                או לכנסת ספציפית — או הורידו את רשימת הקישורים המלאה כ-CSV (עד 50,000
                שורות) לשימוש במנהל הורדות.
              </p>
            )}
            <div className="flex" style={{ gap: "0.75rem", flexWrap: "wrap", alignItems: "center" }}>
              <a
                href={tooBig || !count ? undefined : knessetDb.batchZipUrl(filter)}
                aria-disabled={tooBig || !count}
                style={{
                  fontSize: "0.9rem", padding: "0.5rem 1.1rem", borderRadius: 4, fontWeight: 600,
                  background: tooBig || !count ? "var(--bg-muted, #e5e7eb)" : "var(--primary, #0f766e)",
                  color: tooBig || !count ? "var(--text-muted)" : "white",
                  textDecoration: "none", pointerEvents: tooBig || !count ? "none" : "auto",
                }}
                title="הורדת כל קובצי הפרוטוקולים כ-ZIP אחד"
              >
                ⬇ הורדת ZIP{count && !tooBig ? ` (${count.toLocaleString()} קבצים)` : ""}
              </a>
              <a
                href={count ? knessetDb.batchLinksUrl(filter) : undefined}
                aria-disabled={!count}
                style={{
                  fontSize: "0.85rem", padding: "0.45rem 0.9rem", borderRadius: 4,
                  border: "1px solid var(--primary, #0f766e)", color: "var(--primary, #0f766e)",
                  textDecoration: "none", pointerEvents: count ? "auto" : "none",
                  opacity: count ? 1 : 0.5,
                }}
                title="רשימת הקישורים בלבד — קובץ CSV קטן"
              >
                ⬇ רשימת קישורים (CSV)
              </a>
            </div>
            <ul className="text-sm text-muted" style={{ margin: "1rem 0 0", paddingInlineStart: "1.1rem", lineHeight: 1.7 }}>
              <li>ה-ZIP נבנה תוך כדי ההורדה — הקבצים נמשכים אחד-אחד משרת הכנסת (fs.knesset.gov.il); אצווה גדולה עשויה להימשך מספר דקות. אל תסגרו את החלון.</li>
              <li>בתוך ה-ZIP: תיקייה לכל ועדה, שם קובץ = תאריך הישיבה + שם הקובץ המקורי, ‏<code>_index.csv</code> עם פירוט מלא, ו-<code>_errors.txt</code> אם קבצים בודדים נכשלו.</li>
              <li>ההורדות נספרות במכסת התעבורה היומית (2GB לכתובת IP), ומספר הורדות ZIP במקביל מוגבל — אם קיבלתם שגיאה נסו שוב בעוד רגע או השתמשו ברשימת הקישורים (CSV).</li>
            </ul>
          </>
        )}
      </div>
    </div>
  );
}
