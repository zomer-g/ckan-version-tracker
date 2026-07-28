import { Link } from "react-router-dom";
import DataTabs from "../components/DataTabs";

// Guide for the "הצלבה מתוקנת" (healed cross-reference) feature — a dedicated
// /data/guide route, styled with the site's own tokens. Written to be simple and
// example-first: lots of "try it" links that open the console with a ready query,
// plus faithful CSS mockups of the builder and result so readers see how it looks.

// A link that opens the SQL console with a query pre-loaded (user clicks הרץ).
const tryUrl = (sql: string) => `/data?sql=${encodeURIComponent(sql)}`;

const TRY_RESOLVE = `-- כותבים ערך "מלוכלך", מקבלים את השם הרשמי:
SELECT v AS "מה שכתוב", over_settlement(v) AS "השם הרשמי"
FROM (VALUES
  ('בתל אביב'), ('הרצליה'), ('קרית שמונה'),
  ('נצרת עילית'), ('Sderot'), ('כפר חב~ד'), ('מ.א. מטה בנימין')
) t(v)`;

const TRY_ALIASES = `-- כל צורות הכתיבה שמזוהות כ"תל אביב":
SELECT a.surface AS "צורת כתיבה", a.kind AS "סוג"
FROM over_settlement_aliases a
JOIN over_settlements s ON s.code = a.code
WHERE s.name = 'תל אביב -יפו'
ORDER BY a.weight DESC
LIMIT 100`;

const TRY_ENRICH = `-- מוסיפים מחוז ואוכלוסייה לכל שורה לפי היישוב:
SELECT "כתובת - ישוב" AS יישוב,
       over_settlement("כתובת - ישוב") AS יישוב_רשמי,
       s.district AS מחוז, s.population AS אוכלוסייה
FROM append_moj_amutot_73f3cd78 t
LEFT JOIN over_settlements s ON s.code = over_settlement_code(t."כתובת - ישוב")
LIMIT 100`;

const TRY_DENSITY = `WITH ngos AS (
  SELECT over_settlement_code("כתובת - ישוב") AS code, count(*) AS עמותות
  FROM append_moj_amutot_73f3cd78
  WHERE "כתובת - ישוב" IS NOT NULL GROUP BY 1
)
SELECT s.name AS יישוב, s.population AS אוכלוסייה, n.עמותות,
       round(n.עמותות * 10000.0 / NULLIF(s.population,0), 1) AS ל10אלף
FROM ngos n JOIN over_settlements s ON s.code = n.code
WHERE s.population > 20000
ORDER BY ל10אלף DESC LIMIT 30`;

const BA: [string, string, string][] = [
  ["כתיב מלא/חסר", "הרצליה", "הרצלייה"],
  ["גרש משובש", "כפר חב~ד", "כפר חב״ד"],
  ["צורה קצרה", "בנימינה", "בנימינה-גבעת עדה"],
  ["קידומת", "בתל אביב", "תל אביב -יפו"],
  ["שם ישן", "נצרת עילית", "נוף הגליל"],
  ["אנגלית", "Sderot", "שדרות"],
  ["קידומת מנהלית", "מ.א. מטה בנימין", "מטה בנימין"],
  ["רווח נסתר", "נהריה␣␣␣", "נהרייה"],
];

export default function DataGuidePage() {
  return (
    <div className="container mt-3 jg" dir="rtl">
      <DataTabs active="guide" />

      <header style={{ marginBottom: "1.4rem" }}>
        <div className="jg-eyebrow">גרסאות לעם · קונסולת /data</div>
        <h1 style={{ margin: "0.3rem 0 0.6rem", fontSize: "1.9rem" }}>הצלבה מתוקנת — לחבר מאגרים לפי יישוב</h1>
        <p className="jg-lead" style={{ maxWidth: "60ch", fontSize: "1.05rem" }}>
          כל משרד כותב שמות יישובים אחרת. הכלי הזה מזהה שכולם מדברים על אותו מקום, ומאפשר לחבר מאגרים לפיו —
          <b> בלי לגעת בנתונים המקוריים.</b>
        </p>
        <div className="jg-try-hero">
          <span>הכי מהיר להבין — לנסות:</span>
          <Link className="jg-btn" to={tryUrl(TRY_RESOLVE)}>נסה: תיקון שמות ▸</Link>
        </div>
      </header>

      {/* what it does — 3 simple examples */}
      <section className="jg-card">
        <div className="jg-kicker">מה זה עושה</div>
        <h2>מזהה שם יישוב — לא משנה איך כתבו אותו</h2>
        <p className="jg-lead">כל אלה מזוהים כאותו יישוב, ומקבלים את השם הרשמי:</p>
        <div className="jg-ba">
          {BA.map(([why, dirty, healed]) => (
            <div className="jg-ba-row" key={dirty}>
              <div className="jg-val dirty">{dirty}<span className="jg-tag">{why}</span></div>
              <div className="jg-arrow">←</div>
              <div className="jg-val healed">{healed}</div>
            </div>
          ))}
        </div>
        <Link className="jg-btn ghost" to={tryUrl(TRY_RESOLVE)}>נסה את כל אלה בקונסולה ▸</Link>
      </section>

      {/* how it looks — mockup */}
      <section className="jg-card">
        <div className="jg-kicker">איך זה נראה בפועל</div>
        <h2>לוחצים כמה כפתורים — לא כותבים SQL</h2>
        <p className="jg-lead">
          בעמוד של כל טבלה עם שדה יישוב, מתחת לפרופיל, נפתח <b>"בונה הצלבה מתוקנת"</b>. כך הוא נראה:
        </p>

        {/* faithful mockup of the JoinBuilder */}
        <div className="jg-mock">
          <div className="jg-mock-title">🔗 בונה הצלבה מתוקנת</div>
          <div className="jg-mock-row">
            <b>צד שמאל:</b> <span className="jg-mchip gray">append_moj_amutot</span>
            שדה: <span className="jg-mchip blue">כתובת - ישוב</span>
            <span className="jg-mbtn">בדוק כיסוי</span>
            <span className="jg-mchip green">753/812 (91%)</span>
          </div>
          <div className="jg-mock-row">
            <span className="jg-mbtn on">העשרה מהאינדקס</span>
            <span className="jg-mbtn">הצלבה למאגר אחר</span>
          </div>
          <div className="jg-mock-row" style={{ gap: 14 }}>
            <label><input type="checkbox" checked readOnly /> מחוז</label>
            <label><input type="checkbox" checked readOnly /> אוכלוסייה</label>
            <label><input type="checkbox" readOnly /> נפה</label>
          </div>
          <div className="jg-mock-row"><span className="jg-mbtn run">▶ צור והרץ</span></div>
        </div>
        <p className="jg-lead" style={{ fontSize: "0.9rem" }}>
          <b>"בדוק כיסוי"</b> מראה מראש כמה מהערכים יימצאו — כאן 91%. <b>"צור והרץ"</b> כותב את ה-SQL ומריץ.
        </p>
      </section>

      {/* the raw aliases file */}
      <section className="jg-card">
        <div className="jg-kicker">קובץ ההטיות הגולמי</div>
        <h2>רוצים לראות מה מאחורי הקלעים?</h2>
        <p className="jg-lead">
          כל צורות הכתיבה שמופו לכל יישוב שמורות בטבלה פתוחה לתשאול — <span className="mono">over_settlement_aliases</span>
          (תחת המקור "גרסאות לעם" ב-/data). כך תראו, למשל, את כל הצורות שמזוהות כ"תל אביב":
        </p>
        <div className="jg-tryrow">
          <Link className="jg-btn" to={tryUrl(TRY_ALIASES)}>נסה: כל ההטיות של תל אביב ▸</Link>
          <Link className="jg-btn ghost" to="/data?table=over_settlement_aliases">עיון בטבלת ההטיות ▸</Link>
        </div>
        <p className="jg-lead" style={{ fontSize: "0.9rem" }}>
          זה גם המקום שבו הכלי <b>לומד</b>: הוא סורק את שדות היישוב בכל המאגרים, וכל צורת כתיבה חדשה שהוא פוגש
          נוספת לכאן — כך הכיסוי משתפר עם הזמן.
        </p>
      </section>

      {/* two things you can do */}
      <section className="jg-card">
        <div className="jg-kicker">שתי פעולות עיקריות</div>
        <div className="jg-two">
          <div className="jg-mini">
            <h3>1 · העשרה</h3>
            <p>להוסיף לכל שורה <b>מחוז, אוכלוסייה, נפה</b> — לפי היישוב שבשדה.</p>
            <Link className="jg-btn ghost sm" to={tryUrl(TRY_ENRICH)}>נסה ▸</Link>
          </div>
          <div className="jg-mini">
            <h3>2 · הצלבה בין מאגרים</h3>
            <p>לחבר שני מאגרים לפי יישוב — כששני הצדדים מרופאים לשם הקנוני.</p>
            <Link className="jg-btn ghost sm" to={tryUrl(TRY_DENSITY)}>נסה ▸</Link>
          </div>
        </div>
      </section>

      {/* worked example */}
      <section className="jg-card">
        <div className="jg-kicker">דוגמה שלמה</div>
        <h2>עמותות ל-10,000 תושבים, לפי יישוב</h2>
        <p className="jg-lead">
          מצליבים את מאגר העמותות (כתובות מלוכלכות) עם אוכלוסיית היישוב, ומחשבים צפיפות. בלי הריפוי, השמות
          פשוט לא היו מתאימים. הנה התוצאה (אמת חיה):
        </p>
        <div className="jg-tblwrap">
          <table className="jg-tbl">
            <thead><tr><th>יישוב</th><th>אוכלוסייה</th><th>עמותות</th><th>ל-10 אלף</th></tr></thead>
            <tbody>
              <tr><td>בני ברק</td><td className="num">229,995</td><td className="num">4,227</td><td className="hl">183.8</td></tr>
              <tr><td>צפת</td><td className="num">38,687</td><td className="num">655</td><td className="hl">169.3</td></tr>
              <tr><td>ירושלים</td><td className="num">1,050,151</td><td className="num">16,657</td><td className="hl">158.6</td></tr>
              <tr><td>תל אביב-יפו</td><td className="num">494,900</td><td className="num">7,054</td><td className="hl">142.5</td></tr>
              <tr><td>בית שמש</td><td className="num">176,786</td><td className="num">2,289</td><td className="hl">129.5</td></tr>
            </tbody>
          </table>
        </div>
        <Link className="jg-btn" to={tryUrl(TRY_DENSITY)}>נסה את השאילתה הזו ▸</Link>
      </section>

      <section className="jg-card">
        <div className="jg-callout">
          <span className="jg-mark">◆</span>
          <div><b>המקור נשאר נקי.</b> הכלי לא מוסיף עמודות ולא משנה את מה שהמדינה סיפקה — הריפוי קורה רק תוך כדי
            התשאול. מי שרוצה את ההצלבה מקבל אותה; הטבלה המקורית לא זזה.</div>
        </div>
        <p style={{ textAlign: "center", marginTop: "1rem" }}>
          <Link to="/data?table=append_moj_amutot_73f3cd78" style={{ color: "var(--primary,#0f766e)", fontWeight: 600 }}>
            ← פתחו מאגר עם שדה יישוב ותראו את "בונה ההצלבה המתוקנת" בעצמכם
          </Link>
        </p>
      </section>

      <style>{`
        .jg h1, .jg h2, .jg h3 { text-wrap: balance; }
        .jg .mono { font-family: ui-monospace, Consolas, monospace; direction: ltr; unicode-bidi: embed; font-size: 0.9em; }
        .jg-eyebrow { font-size: 0.72rem; letter-spacing: 0.12em; font-weight: 700; text-transform: uppercase; color: #a21caf; }
        .jg-lead { color: var(--text-muted, #64748b); line-height: 1.7; }
        .jg-card { border: 1px solid var(--border, #e5e7eb); border-radius: 12px; background: var(--bg, #fff); padding: 1.3rem 1.5rem; margin-bottom: 1.1rem; }
        .jg-card h2 { font-size: 1.3rem; margin: 0.2rem 0 0; }
        .jg-card h3 { font-size: 1rem; margin: 0 0 0.3rem; }
        .jg-kicker { font-size: 0.78rem; font-weight: 700; color: var(--primary, #0f766e); margin-bottom: 0.2rem; }

        /* buttons */
        .jg-btn { display: inline-block; margin-top: 0.9rem; padding: 0.45rem 1rem; border-radius: 6px; text-decoration: none; font-weight: 600; font-size: 0.9rem; background: var(--primary, #0f766e); color: #fff; border: 1px solid var(--primary, #0f766e); }
        .jg-btn.ghost { background: transparent; color: var(--primary, #0f766e); }
        .jg-btn.sm { font-size: 0.82rem; padding: 0.3rem 0.7rem; margin-top: 0.6rem; }
        .jg-try-hero { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-top: 1.1rem; font-weight: 600; color: var(--text-muted,#64748b); }
        .jg-try-hero .jg-btn { margin-top: 0; }
        .jg-tryrow { display: flex; gap: 10px; flex-wrap: wrap; }
        .jg-tryrow .jg-btn { margin-top: 0.6rem; }

        /* before/after */
        .jg-ba { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin: 1rem 0; }
        .jg-ba-row { display: grid; grid-template-columns: 1fr auto 1fr; align-items: center; gap: 8px; border: 1px solid var(--border,#e5e7eb); border-radius: 10px; padding: 8px 12px; background: var(--bg-muted,#f8fafc); }
        .jg-val { font-family: ui-monospace, Consolas, monospace; font-size: 0.88rem; }
        .jg-val.dirty { color: #b45309; }
        .jg-val.healed { color: #15803d; font-weight: 600; }
        .jg-tag { display: block; font-family: inherit; font-size: 0.66rem; color: var(--text-muted,#94a3b8); margin-top: 2px; font-weight: 600; }
        .jg-arrow { color: var(--primary,#0f766e); font-size: 1.1rem; }

        /* mockup */
        .jg-mock { border: 1.5px dashed color-mix(in srgb, var(--primary,#0f766e) 45%, var(--border,#cbd5e1)); border-radius: 10px; padding: 0.9rem 1.1rem; margin: 1rem 0; background: var(--bg-muted,#f8fafc); display: grid; gap: 0.6rem; }
        .jg-mock-title { font-weight: 700; font-size: 0.92rem; }
        .jg-mock-row { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; font-size: 0.85rem; }
        .jg-mock-row label { font-size: 0.85rem; }
        .jg-mchip { font-size: 0.75rem; font-weight: 600; padding: 2px 9px; border-radius: 999px; border: 1px solid var(--border,#e5e7eb); }
        .jg-mchip.gray { color: var(--text-muted,#475569); background: var(--bg,#fff); }
        .jg-mchip.blue { color: #0369a1; background: #e0f2fe; border-color: #bae6fd; }
        .jg-mchip.green { color: #15803d; background: #dcfce7; border-color: #bbf7d0; }
        .jg-mbtn { font-size: 0.8rem; padding: 3px 10px; border-radius: 5px; border: 1px solid var(--border,#cbd5e1); background: var(--bg,#fff); color: var(--text-muted,#475569); }
        .jg-mbtn.on { background: #dbeafe; color: #1d4ed8; font-weight: 700; border-color: #bfdbfe; }
        .jg-mbtn.run { background: var(--primary,#0f766e); color: #fff; font-weight: 700; border-color: var(--primary,#0f766e); }

        /* two */
        .jg-two { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 0.4rem; }
        .jg-mini { border: 1px solid var(--border,#e5e7eb); border-radius: 10px; padding: 0.9rem 1.1rem; background: var(--bg-muted,#f8fafc); }
        .jg-mini p { font-size: 0.9rem; color: var(--text-muted,#64748b); margin: 0.2rem 0 0; }

        /* table */
        .jg-tblwrap { overflow-x: auto; border: 1px solid var(--border,#e5e7eb); border-radius: 10px; margin: 1rem 0; }
        .jg-tbl { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
        .jg-tbl th, .jg-tbl td { text-align: right; padding: 9px 14px; border-bottom: 1px solid var(--border,#eef2f5); }
        .jg-tbl thead th { background: var(--bg-muted,#eef2f5); color: var(--primary,#0f766e); font-weight: 700; font-size: 0.82rem; }
        .jg-tbl tbody tr:last-child td { border-bottom: none; }
        .jg-tbl td.num { font-variant-numeric: tabular-nums; font-family: ui-monospace, Consolas, monospace; }
        .jg-tbl td.hl { color: #15803d; font-weight: 700; font-variant-numeric: tabular-nums; font-family: ui-monospace, Consolas, monospace; }

        .jg-callout { display: flex; gap: 12px; align-items: flex-start; padding: 14px 16px; background: #fbeafc; border: 1px solid #edc8f2; border-radius: 10px; }
        .jg-mark { color: #a21caf; font-weight: 800; }
        @media (max-width: 620px) { .jg-two, .jg-ba { grid-template-columns: 1fr; } }
      `}</style>
    </div>
  );
}
