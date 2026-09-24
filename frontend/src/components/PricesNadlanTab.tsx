/**
 * מדף מול נדל"ן — does a supermarket shelf predict what an apartment costs?
 *
 * A research tab on /projects/prices. It crosses two of OVER's corpora:
 *   housing — median price per m² per settlement from the מיסוי מקרקעין
 *             register (apartments, 07/2025–06/2026, ≥100 deals a year),
 *             in seven models (all, small/mid/large, 4 rooms, new, old)
 *   shelf   — Shufersal's published prices and assortment in the same
 *             settlements: staple baskets of 30/50/100/200 items, single
 *             products, and what a branch stocks at all (offal, a
 *             cross-validated assortment score)
 *
 * The numbers are a SNAPSHOT (src/data/prices-nadlan-shelf.json): building them
 * takes dozens of heavy queries over ~15M price rows, far past what a page view
 * can run. The correlations themselves are computed here, from that snapshot,
 * so the chart and the matrix can never disagree.
 */
import { useMemo, useState } from "react";
import snapshot from "../data/prices-nadlan-shelf.json";

type Row = {
  code: number; name: string; deals: number; stores: number;
  general: number | null; small: number | null; mid: number | null; large: number | null;
  four: number | null; new: number | null; old: number | null;
  b30: number | null; b50: number | null; b100: number | null; b200: number | null;
  offal: number | null; assort: number | null; slim: number | null; pretzel: number | null;
  basket_all: number | null;
};
const ROWS = snapshot.rows as Row[];

type ShelfKey = "b30" | "b50" | "b100" | "b200" | "offal" | "assort" | "slim" | "pretzel" | "basket_all";
type HousingKey = "general" | "small" | "mid" | "large" | "four" | "new" | "old";

const SHELF: Record<ShelfKey, { label: string; short: string; axis: string; nis?: boolean; linear?: boolean; dec: number; note: string }> = {
  b30: { label: "סל בסיס, 30 מוצרים", short: "סל בסיס 30", axis: "מדד סל הבסיס (100 = חציון ארצי)", dec: 1,
    note: "30 מוצרי יסוד, אחד לכל קטגוריה, כמו במדד המחירים לצרכן. ממוצע גיאומטרי של המחיר ביישוב ביחס לחציון הארצי. נתיבות וגבעת שמואל הוצאו: הסניף היחיד שלהן מחזיק רק 6–7 ממוצרי הסל." },
  b50: { label: "סל 50 מוצרים", short: "סל 50", axis: "מדד סל 50 (100 = חציון ארצי)", dec: 1, note: "סל הבסיס ועוד 20 מוצרי מזון נפוצים. אותה שיטת חישוב." },
  b100: { label: "סל 100 מוצרים", short: "סל 100", axis: "מדד סל 100 (100 = חציון ארצי)", dec: 1, note: "סל הבסיס ועוד 70 מוצרי מזון נפוצים. אותה שיטת חישוב." },
  b200: { label: "סל 200 מוצרים", short: "סל 200", axis: "מדד סל 200 (100 = חציון ארצי)", dec: 1, note: "סל הבסיס ועוד 170 מוצרי מזון נפוצים. אותה שיטת חישוב." },
  offal: { label: "מדד הקרביים", short: "מדד הקרביים", axis: "מוצרי קרביים לכל 1,000 מוצרים בקטלוג", linear: true, dec: 1,
    note: "167 מוצרים ששמם מכיל טחול, מוח, לב עוף, לבבות, קורקבן, ראש בקר, כבד עוף, עטינים או ריאות, כחלק מגודל הקטלוג של הסניף. קטגוריה שהוגדרה לפי מושג, לא לפי ברקוד." },
  assort: { label: "סל מבחר מאומת", short: "סל מבחר", axis: "ציון מבחר (מאומת בהצלבה)", linear: true, dec: 0,
    note: "נוכחות (באחוזי סניפים) של 50 המוצרים שנוכחותם הכי מתואמת חיובית, פחות 50 הכי שלילית. כל יישוב קיבל ציון ממוצרים שנבחרו רק על חצי היישובים השני, כך שאף נקודה לא 'ראתה' את עצמה." },
  slim: { label: "סלים דליס טופינג אגוזי לוז", short: "סלים דליס", axis: "מחיר המוצר (₪)", nis: true, dec: 2,
    note: "ברקוד 8423207210928. המוצר הבודד עם המתאם הגבוה ביותר, אבל הוא נבחר מתוך כ-9,500 מוצרים על אותם נתונים, ולכן המתאם שלו אופטימי." },
  pretzel: { label: "בייגלה שמיניות אסם 400 ג׳", short: "בייגלה אסם", axis: "מחיר המוצר (₪)", nis: true, dec: 2, note: "ברקוד 7290000461625." },
  basket_all: { label: "סל מחירים מלא (5,587 מוצרים)", short: "סל מלא", axis: "מדד סל (100 = חציון ארצי)", dec: 1,
    note: "כל המוצרים שנמכרים ב-60 יישובים ומעלה ומחירם משתנה בין יישובים." },
};
const SHELF_GROUPS: [string, ShelfKey[]][] = [
  ["סלי מזון מקובלים (מחיר)", ["b30", "b50", "b100", "b200"]],
  ["מבחר (מה הסניף מחזיק)", ["offal", "assort"]],
  ["מוצר בודד וסל מלא", ["slim", "pretzel", "basket_all"]],
];

const HOUSING: Record<HousingKey, { label: string; short: string; axis: string; perSqm: boolean }> = {
  general: { label: "כללי: כל הדירות", short: "כללי", axis: 'חציון מחיר למ"ר, כל הדירות', perSqm: true },
  small: { label: 'דירות קטנות (עד 75 מ"ר)', short: "קטנות", axis: 'חציון מחיר למ"ר, דירות עד 75 מ"ר', perSqm: true },
  mid: { label: 'דירות בינוניות (75–110 מ"ר)', short: "בינוניות", axis: 'חציון מחיר למ"ר, דירות 75–110 מ"ר', perSqm: true },
  large: { label: 'דירות גדולות (110 מ"ר ומעלה)', short: "גדולות", axis: 'חציון מחיר למ"ר, דירות 110 מ"ר ומעלה', perSqm: true },
  four: { label: "מחיר דירת 4 חדרים (₪ לדירה)", short: "4 חדרים", axis: "חציון מחיר דירת 4 חדרים", perSqm: false },
  new: { label: "בנייה חדשה (2021 ומעלה)", short: "חדשה", axis: 'חציון מחיר למ"ר, בניינים משנת 2021', perSqm: true },
  old: { label: "בניין ישן (עד 1990)", short: "ישנה", axis: 'חציון מחיר למ"ר, בניינים עד 1990', perSqm: true },
};
const HOUSING_KEYS = Object.keys(HOUSING) as HousingKey[];

const LABELED = new Set(["תל אביב-יפו", "ירושלים", "בני ברק", "אילת", "חריש", "נוף הגליל", "אופקים", "הרצליה",
  "קריית אונו", "גדרה", "אלעד", "רכסים", "חיפה", "באר שבע", "דימונה"]);

function isShelf(v: string | null): v is ShelfKey { return !!v && v in SHELF; }
function isHousing(v: string | null): v is HousingKey { return !!v && v in HOUSING; }

type Fit = { r: number; slope: number; intercept: number; n: number };

/** Pearson r on log(housing) against log(shelf), or the raw shelf value for the
 *  indexes that can be zero or negative — the same transform the chart draws. */
function fit(k: ShelfKey, m: HousingKey): Fit & { pts: Row[] } {
  const pts = ROWS.filter((d) => d[k] != null && d[m] != null);
  const xs = pts.map((d) => Math.log(d[m] as number));
  const ys = pts.map((d) => (SHELF[k].linear ? (d[k] as number) : Math.log(d[k] as number)));
  const n = xs.length;
  const mx = xs.reduce((a, b) => a + b, 0) / n, my = ys.reduce((a, b) => a + b, 0) / n;
  let sxy = 0, sxx = 0, syy = 0;
  for (let i = 0; i < n; i++) { sxy += (xs[i] - mx) * (ys[i] - my); sxx += (xs[i] - mx) ** 2; syy += (ys[i] - my) ** 2; }
  const slope = sxy / sxx;
  return { r: sxy / Math.sqrt(sxx * syy), slope, intercept: my - slope * mx, n, pts };
}

function niceTicks(lo: number, hi: number, count: number): number[] {
  const raw = (hi - lo) / count;
  const p = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = ([1, 2, 2.5, 5, 10].find((s) => s * p >= raw) ?? 10) * p;
  const out: number[] = [];
  for (let v = Math.ceil(lo / step) * step; v <= hi + 1e-9; v += step) out.push(+v.toFixed(2));
  return out;
}

const W = 900, H = 500, M = { l: 70, r: 20, t: 20, b: 56 };
const FONT = "inherit";

function Scatter({ k, m }: { k: ShelfKey; m: HousingKey }) {
  const [hover, setHover] = useState<Row | null>(null);
  const f = useMemo(() => fit(k, m), [k, m]);
  const s = SHELF[k], h = HOUSING[m];

  const xv = f.pts.map((d) => d[m] as number);
  const x0 = Math.log(Math.min(...xv) * 0.9), x1 = Math.log(Math.max(...xv) * 1.08);
  const yv = f.pts.map((d) => d[k] as number);
  const pad = (Math.max(...yv) - Math.min(...yv)) * 0.1 || 1;
  const y0 = Math.min(...yv) - pad, y1 = Math.max(...yv) + pad;
  const X = (v: number) => M.l + ((Math.log(v) - x0) / (x1 - x0)) * (W - M.l - M.r);
  const Y = (v: number) => H - M.b - ((v - y0) / (y1 - y0)) * (H - M.t - M.b);
  const radius = (d: Row) => Math.max(4, Math.min(13, Math.sqrt(d.deals) / 4.5));

  const xTicks = (h.perSqm ? [6000, 8000, 10000, 15000, 20000, 30000, 40000, 55000, 70000]
    : [800000, 1000000, 1500000, 2000000, 3000000, 4000000, 5000000])
    .filter((v) => Math.log(v) >= x0 && Math.log(v) <= x1);
  const yTicks = niceTicks(y0, y1, 6);

  const line: string[] = [];
  for (let i = 0; i <= 60; i++) {
    const lx = x0 + ((x1 - x0) * i) / 60;
    const raw = f.intercept + f.slope * lx;
    const yy = s.linear ? raw : Math.exp(raw);
    if (yy < y0 || yy > y1) continue;
    line.push(`${line.length ? "L" : "M"}${X(Math.exp(lx)).toFixed(1)},${Y(yy).toFixed(1)}`);
  }
  const fmtY = (v: number) => (s.nis ? `₪${v.toFixed(2)}` : v.toFixed(s.dec));
  const fmtX = (v: number) => `₪${Math.round(v).toLocaleString("he-IL")} ${h.perSqm ? 'למ"ר' : "לדירה"}`;
  const what = h.perSqm ? "מחיר המטר" : "מחיר הדירה";

  return (
    <>
      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.3rem 1.2rem", alignItems: "baseline", margin: "0.4rem 0" }}>
        <span style={{ fontSize: "2rem", fontWeight: 700, color: "var(--primary)", direction: "ltr", unicodeBidi: "isolate" }}>
          r = {f.r.toFixed(2)}
        </span>
        <span className="text-sm text-muted">
          {f.n} יישובים ·{" "}
          {s.linear
            ? `מתאם פירסון מול לוג ${what}`
            : `מתאם פירסון על לוג המחירים · ${what} גבוה ב-10% ↔ מדד המדף גבוה ב-${(100 * (Math.pow(1.1, f.slope) - 1)).toFixed(1)}%`}
        </span>
      </div>
      <div style={{ position: "relative", direction: "ltr" }}>
        <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`גרף פיזור: ${s.label} מול ${h.label}`}
             style={{ width: "100%", height: "auto", display: "block", fontFamily: FONT }}>
          {xTicks.map((v) => (
            <g key={`x${v}`}>
              <line x1={X(v)} x2={X(v)} y1={M.t} y2={H - M.b} style={{ stroke: "var(--border)" }} />
              <text x={X(v)} y={H - M.b + 20} textAnchor="middle" fontSize={13} style={{ fill: "var(--text-muted)" }}>
                {h.perSqm ? `₪${v / 1000}K` : `₪${v / 1e6}M`}
              </text>
            </g>
          ))}
          {yTicks.map((v) => (
            <g key={`y${v}`}>
              <line x1={M.l} x2={W - M.r} y1={Y(v)} y2={Y(v)} style={{ stroke: "var(--border)" }} />
              <text x={M.l - 8} y={Y(v) + 4} textAnchor="end" fontSize={13} style={{ fill: "var(--text-muted)" }}>
                {s.nis ? `₪${v.toFixed(2)}` : String(v)}
              </text>
            </g>
          ))}
          <text x={(W + M.l - M.r) / 2} y={H - 10} textAnchor="middle" fontSize={14} fontWeight={600}
                direction="rtl" style={{ fill: "var(--text)" }}>
            {h.axis}, 07/2025–06/2026 (סקאלה לוגריתמית)
          </text>
          <text x={18} y={(H - M.b + M.t) / 2} textAnchor="middle" fontSize={14} fontWeight={600} direction="rtl"
                transform={`rotate(-90 18 ${(H - M.b + M.t) / 2})`} style={{ fill: "var(--text)" }}>
            {s.axis}
          </text>
          <path d={line.join("")} fill="none" strokeWidth={2.5} strokeDasharray="7 5" style={{ stroke: "var(--warning)" }} />
          {[...f.pts].sort((a, b) => b.deals - a.deals).map((d) => (
            <circle key={d.code} cx={X(d[m] as number)} cy={Y(d[k] as number)} r={radius(d)} strokeWidth={1.4}
                    tabIndex={0} aria-label={`${d.name}: ${fmtX(d[m] as number)}, ${fmtY(d[k] as number)}`}
                    style={{ fill: "color-mix(in srgb, var(--primary) 22%, transparent)", stroke: "var(--primary)", cursor: "default" }}
                    onMouseEnter={() => setHover(d)} onMouseLeave={() => setHover(null)}
                    onFocus={() => setHover(d)} onBlur={() => setHover(null)} />
          ))}
          {f.pts.filter((d) => LABELED.has(d.name)).map((d) => (
            <text key={`l${d.code}`} x={X(d[m] as number)} y={Y(d[k] as number) - radius(d) - 5} textAnchor="middle"
                  fontSize={12.5} fontWeight={600} direction="rtl" pointerEvents="none" style={{ fill: "var(--text)" }}>
              {d.name}
            </text>
          ))}
        </svg>
        <div className="text-sm" aria-live="polite"
             style={{ minHeight: "1.6em", direction: "rtl", color: hover ? "var(--text)" : "var(--text-muted)" }}>
          {hover
            ? `${hover.name} · ${fmtX(hover[m] as number)} · ${fmtY(hover[k] as number)} · ${hover.deals.toLocaleString("he-IL")} עסקאות · ${hover.stores} סניפים`
            : "רחפו על נקודה (או עברו אליה במקלדת) כדי לראות את היישוב."}
        </div>
      </div>
      <p className="text-sm text-muted" style={{ margin: "0.3rem 0 0", lineHeight: 1.7 }}>{s.note}</p>
    </>
  );
}

function Matrix({ k, m, pick }: { k: ShelfKey; m: HousingKey; pick: (k: ShelfKey, m: HousingKey) => void }) {
  const cells = useMemo(() => {
    const out: Record<string, { r: number; n: number }> = {};
    for (const [, keys] of SHELF_GROUPS) for (const sk of keys) for (const hk of HOUSING_KEYS) {
      const f = fit(sk, hk); out[`${sk}|${hk}`] = { r: f.r, n: f.n };
    }
    return out;
  }, []);
  const th: React.CSSProperties = { padding: "0.35rem 0.4rem", fontSize: "0.8rem", color: "var(--text-muted)", whiteSpace: "nowrap", textAlign: "center" };
  return (
    <div tabIndex={0} role="region" aria-label="מטריצת ההצלבות" className="scroll-region" style={{ overflowX: "auto" }}>
      <table style={{ borderCollapse: "collapse", width: "100%", minWidth: 640, fontSize: "0.88rem" }}>
        <thead>
          <tr>
            <th scope="col" style={{ ...th, textAlign: "start" }}>מדד מדף \ מודל דיור</th>
            {HOUSING_KEYS.map((hk) => <th key={hk} scope="col" style={th}>{HOUSING[hk].short}</th>)}
          </tr>
        </thead>
        <tbody>
          {SHELF_GROUPS.map(([group, keys]) => [
            <tr key={group}>
              <td colSpan={HOUSING_KEYS.length + 1}
                  style={{ padding: "0.7rem 0.4rem 0.2rem", fontSize: "0.78rem", fontWeight: 700, color: "var(--warning)" }}>{group}</td>
            </tr>,
            ...keys.map((sk) => (
              <tr key={sk} style={{ borderTop: "1px solid var(--border)" }}>
                <th scope="row" style={{ padding: "0.35rem 0.4rem", textAlign: "start", fontWeight: 500, whiteSpace: "nowrap" }}>{SHELF[sk].short}</th>
                {HOUSING_KEYS.map((hk) => {
                  const c = cells[`${sk}|${hk}`];
                  const a = Math.min(1, Math.max(0, (Math.abs(c.r) - 0.25) / 0.45));
                  const on = sk === k && hk === m;
                  return (
                    <td key={hk} style={{ padding: 0 }}>
                      <button type="button" onClick={() => pick(sk, hk)} aria-pressed={on}
                              title={`${SHELF[sk].label} × ${HOUSING[hk].label}: r=${c.r.toFixed(3)}, ${c.n} יישובים`}
                              style={{
                                width: "100%", border: "none", cursor: "pointer", padding: "0.4rem 0.2rem", lineHeight: 1.2,
                                font: "inherit", fontWeight: 700, direction: "ltr",
                                background: `color-mix(in srgb, var(--primary) ${Math.round(a * 62)}%, transparent)`,
                                // --surface flips with the theme, and so does --primary:
                                // white on dark blue in light mode, dark on light blue in dark.
                                color: a > 0.62 ? "var(--surface)" : "var(--text)",
                                outline: on ? "3px solid var(--warning)" : "none", outlineOffset: -3,
                              }}>
                        {c.r >= 0 ? "+" : "−"}{Math.abs(c.r).toFixed(2)}
                        <span style={{ display: "block", fontWeight: 400, fontSize: "0.7rem", opacity: 0.8 }}>{c.n}</span>
                      </button>
                    </td>
                  );
                })}
              </tr>
            )),
          ])}
        </tbody>
      </table>
    </div>
  );
}

export default function PricesNadlanTab({ params, patch }: {
  params: URLSearchParams; patch: (p: Record<string, string | null>) => void;
}) {
  const rawK = params.get("shelf"), rawM = params.get("housing");
  const k: ShelfKey = isShelf(rawK) ? rawK : "b30";
  const m: HousingKey = isHousing(rawM) ? rawM : "general";
  const pick = (sk: ShelfKey, hk: HousingKey) =>
    patch({ shelf: sk === "b30" ? null : sk, housing: hk === "general" ? null : hk });

  const select: React.CSSProperties = {
    font: "inherit", fontSize: "0.92rem", fontWeight: 600, padding: "0.4rem 0.55rem",
    border: "1px solid var(--border)", borderRadius: 6, background: "var(--surface)", color: "var(--text)", maxWidth: "100%",
  };
  const label: React.CSSProperties = { display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.8rem", fontWeight: 700, color: "var(--text-muted)", flex: "1 1 240px", minWidth: 0 };
  const h2: React.CSSProperties = { fontSize: "1.1rem", margin: "1.6rem 0 0.4rem" };

  return (
    <>
      <div className="text-sm text-muted" style={{ lineHeight: 1.7, maxWidth: "75ch", marginBottom: "0.8rem" }}>
        האם מדף הסופר מנבא כמה עולה דירה? מחירי הדירות ב-{snapshot.settlements_in_index} יישובים (לפחות{" "}
        {snapshot.min_deals_per_settlement} עסקאות בשנה, מתוך <a href="/projects/deals">עסקאות נדל"ן</a>) מול מה
        שסניפי שופרסל באותם יישובים גובים על אותו ברקוד, ומול מה שהם בכלל מחזיקים על המדף. כל נקודה היא יישוב.
        צילום מצב מ-{snapshot.as_of.split("-").reverse().join("/")}.
      </div>

      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.6rem 1.5rem", marginBottom: "0.4rem" }}>
        <label style={label}>
          מדד מדף (ציר אנכי)
          <select id="nadlan-shelf" value={k} style={select} onChange={(e) => pick(e.target.value as ShelfKey, m)}>
            {SHELF_GROUPS.map(([group, keys]) => (
              <optgroup key={group} label={group}>
                {keys.map((sk) => <option key={sk} value={sk}>{SHELF[sk].label}</option>)}
              </optgroup>
            ))}
          </select>
        </label>
        <label style={label}>
          מודל דיור (ציר אופקי)
          <select id="nadlan-housing" value={m} style={select} onChange={(e) => pick(k, e.target.value as HousingKey)}>
            {HOUSING_KEYS.map((hk) => <option key={hk} value={hk}>{HOUSING[hk].label}</option>)}
          </select>
        </label>
      </div>

      <Scatter k={k} m={m} />

      <h2 style={h2}>כל ההצלבות: מתאם פירסון (r)</h2>
      <p className="text-sm text-muted" style={{ margin: "0 0 0.5rem", lineHeight: 1.7 }}>
        שורה לכל מדד מדף, עמודה לכל מודל דיור. ככל שהתא כהה יותר, הקשר חזק יותר (חיובי או שלילי). המספר הקטן הוא
        מספר היישובים. יישוב נכלל במודל רק אם יש לו לפחות {snapshot.min_deals_per_segment} עסקאות בפלח. לחיצה על תא מציגה אותו בגרף.
      </p>
      <Matrix k={k} m={m} pick={pick} />

      <h2 style={h2}>מה עולה מזה</h2>
      <ul className="text-sm" style={{ lineHeight: 1.8, maxWidth: "80ch", paddingInlineStart: "1.2rem" }}>
        <li><strong>מה שהסניף מחזיק חוזה יותר ממה שהוא גובה.</strong> סלי המחירים נעצרים סביב 0.5–0.58. מדד הקרביים
          וסל המבחר מגיעים ל-0.6 ויותר, והכי חדים מול דירות קטנות ובניינים ישנים.</li>
        <li><strong>האות החזק הוא מה שחסר בערים היקרות:</strong> קרביים, גזוז קריסטל 2 ליטר, עשבי תיבול בדוקי תולעים "(ק)",
          טורטים של קילו ואבקת כביסה של 6 ק"ג.</li>
        <li><strong>סל הבסיס (30) מנצח את הסל הגדול (200).</strong> היסודות נושאים את פער הפורמטים, והזנב הארוך של חטיפים ומשקאות מדלל אותו.</li>
        <li><strong>למה שום סל מחירים לא עובר את ~0.6:</strong> מדד הסל מתקבץ לשלוש מדרגות (בערך 96, 107.5 ו-114). אלה שכבות
          התמחור של שופרסל לפי פורמט (דיל, שלי, אקספרס), כך שהסל מודד בעיקר איזה פורמט נפתח בעיר.</li>
      </ul>

      <details style={{ margin: "1rem 0" }}>
        <summary className="text-sm" style={{ cursor: "pointer" }}>איך זה חושב</summary>
        <ul className="text-sm" style={{ lineHeight: 1.8, maxWidth: "80ch" }}>
          <li><strong>דיור:</strong> "דירה בבית קומות", מכירה של 100% מהנכס, 30–250 מ"ר, מחיר של ₪300 אלף ומעלה,
            07/2025–06/2026 (מיולי 2026 הדיווח עוד חלקי). חציון מחיר למ"ר ביישוב.</li>
          <li><strong>למה רשת אחת:</strong> בהשוואה בין כל הרשתות, מחיר מוצר בעיר משקף בעיקר <em>אילו רשתות</em> פועלות בה.
            שופרסל נמצאת ב-70 מתוך 81 היישובים, ולכן המדף נמדד בתוכה.</li>
          <li><strong>הסלים:</strong> אין נתוני מכירות, ולכן "הכי נמכר" הוא הכי נפוץ. סל הבסיס בנוי כמו מדד המחירים לצרכן:
            חלב, לחם, ביצים, קוטג׳, גבינות, חמאה, יוגורט, שמנת, עוף, בשר טחון, אורז, פסטה, קמח, סוכר, שמן, קפה, תה, טונה,
            חומוס, חמישה פירות וירקות, במבה, קולה ומים. הסלים הגדולים מוסיפים מוצרי מזון לפי מספר רשתות הסופרמרקט שמחזיקות אותם.</li>
          <li><strong>מתאם:</strong> פירסון על לוג המחירים (מדד הקרביים וסל המבחר מול לוג מחיר הדיור). סל המבחר מאומת בהצלבה.
            המוצר הבודד לא: הוא נבחר על אותם נתונים.</li>
        </ul>
      </details>
      <details style={{ margin: "0 0 1rem" }}>
        <summary className="text-sm" style={{ cursor: "pointer" }}>מגבלות</summary>
        <ul className="text-sm" style={{ lineHeight: 1.8, maxWidth: "80ch" }}>
          <li>מחיר מדף בלבד, בלי מבצעים ומחירי מועדון.</li>
          <li>כ-11% מהשורות במאגר העסקאות בחלון הזמן כפולות. החציונים כמעט לא זזים, אבל הסף בפועל הוא כ-90 עסקאות ייחודיות.</li>
          <li>אילת פטורה ממע"מ ולכן זולה בכל סל. מתאם אינו סיבתיות: הפורמט נבחר לפי האוכלוסייה באזור.</li>
          <li>צילום מצב: המחירים מתעדכנים כל יום, והניתוח הזה לא.</li>
        </ul>
      </details>
    </>
  );
}
