import { Link } from "react-router-dom";
import DataTabs from "../components/DataTabs";
import SqlCell from "../components/SqlCell";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import { PlainSummary } from "../components/a11y";
// Guide for the "הצלבה מתוקנת" feature. Simple and example-first: every example
// is a live SqlCell that runs in place (Jupyter-style), so nothing opens a
// separate page. Styled with the site's own tokens; theme-aware; RTL.

const TRY_RESOLVE = `SELECT v AS "מה שכתוב",
       COALESCE(over_settlement(v), over_authority(v)) AS "השם הרשמי"
FROM (VALUES
  ('בתל אביב'), ('הרצליה'), ('קרית שמונה'),
  ('נצרת עילית'), ('Sderot'), ('כפר חב~ד'), ('מ.א. מטה בנימין')
) t(v)`;

const TRY_ALIASES = `SELECT a.surface AS "צורת כתיבה", a.kind AS "סוג"
FROM over_settlement_aliases a
JOIN over_settlements s ON s.code = a.code
WHERE s.name = 'תל אביב -יפו'
ORDER BY a.weight DESC
LIMIT 40`;

const TRY_ENRICH = `SELECT "כתובת - ישוב" AS יישוב,
       over_settlement("כתובת - ישוב") AS יישוב_רשמי,
       s.district AS מחוז, s.population AS אוכלוסייה
FROM append_moj_amutot_73f3cd78 t
LEFT JOIN over_settlements s ON s.code = over_settlement_code(t."כתובת - ישוב")
LIMIT 40`;

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
  useDocumentTitle("מדריך — הצלבה מתוקנת");
  return (
    <div className="container mt-3 jg" dir="rtl">
      <DataTabs active="guide" />

      <header style={{ marginBottom: "1.4rem" }}>
        <div className="jg-eyebrow">גרסאות לעם · קונסולת /data</div>
        <h1 style={{ margin: "0.3rem 0 0.6rem", fontSize: "1.9rem" }}>הצלבה מתוקנת — לחבר מאגרים לפי יישוב</h1>
        <p className="jg-lead" style={{ maxWidth: "60ch", fontSize: "1.05rem" }}>
          כל משרד כותב שמות יישובים אחרת. הכלי מזהה שכולם מדברים על אותו מקום, ומאפשר לחבר מאגרים לפיו —
          <b> בלי לגעת בנתונים המקוריים.</b> כל דוגמה כאן רצה במקום — לחצו "הרץ".
        </p>
      </header>
      <PlainSummary>
        כשמחברים שני מאגרים לפי שם יישוב או שם רשות מקומית, כתיב שונה של אותו שם גורם לשורות ליפול בשקט — והתוצאה נראית תקינה אבל חסרה. המדריך הזה מראה איך לחבר לפי קוד קבוע במקום לפי טקסט חופשי, כדי שלא יאבד מידע.
      </PlainSummary>


      {/* what it does — example */}
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
        <p className="jg-lead" style={{ fontSize: "0.92rem" }}>נסו בעצמכם — לחצו "הרץ":</p>
        <SqlCell sql={TRY_RESOLVE} />
      </section>

      {/* how it looks — mockup */}
      <section className="jg-card">
        <div className="jg-kicker">איך זה נראה בפועל</div>
        <h2>לוחצים כמה כפתורים — לא כותבים SQL</h2>
        <p className="jg-lead">
          בעמוד של כל טבלה עם שדה יישוב, מתחת לפרופיל, נפתח <b>"בונה הצלבה מתוקנת"</b>. כך הוא נראה:
        </p>
        <div className="jg-mock">
          <div className="jg-mock-title"><span aria-hidden="true">🔗</span> בונה הצלבה מתוקנת</div>
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

      {/* raw aliases */}
      <section className="jg-card">
        <div className="jg-kicker">קובץ ההטיות הגולמי</div>
        <h2>מה מאחורי הקלעים?</h2>
        <p className="jg-lead">
          כל צורות הכתיבה שמופו לכל יישוב שמורות בטבלה פתוחה, <span className="mono">over_settlement_aliases</span>.
          הנה כל הצורות שמזוהות כ"תל אביב" (הריצו):
        </p>
        <SqlCell sql={TRY_ALIASES} />
        <p className="jg-lead" style={{ fontSize: "0.9rem" }}>
          זה גם המקום שבו הכלי <b>לומד</b>: הוא סורק את שדות היישוב בכל המאגרים, וכל צורה חדשה שהוא פוגש נוספת
          לכאן — כך הכיסוי משתפר עם הזמן.
        </p>
      </section>

      {/* two actions, each with a live cell */}
      <section className="jg-card">
        <div className="jg-kicker">שתי פעולות עיקריות</div>

        <h3 style={{ marginTop: "0.4rem" }}>1 · העשרה — להוסיף מחוז ואוכלוסייה</h3>
        <p className="jg-lead">מוסיפים לכל שורה נתונים על היישוב שבשדה, לפי קובץ הלמ״ס:</p>
        <SqlCell sql={TRY_ENRICH} />

        <h3 style={{ marginTop: "1.4rem" }}>2 · הצלבה בין מאגרים</h3>
        <p className="jg-lead">
          עמותות ל-10,000 תושבים לפי יישוב — מצליבים את מאגר העמותות (כתובות מלוכלכות) עם אוכלוסיית היישוב.
          בלי הריפוי, השמות פשוט לא היו מתאימים:
        </p>
        <SqlCell sql={TRY_DENSITY} />
      </section>

      <section className="jg-card">
        <div className="jg-callout">
          <span className="jg-mark">◆</span>
          <div><b>המקור נשאר נקי.</b> הכלי לא מוסיף עמודות ולא משנה את מה שהמדינה סיפקה — הריפוי קורה רק תוך כדי
            התשאול. מי שרוצה את ההצלבה מקבל אותה; הטבלה המקורית לא זזה.</div>
        </div>
        <p style={{ textAlign: "center", marginTop: "1rem" }}>
          <Link to="/data?table=append_moj_amutot_73f3cd78" style={{ color: "var(--primary)", fontWeight: 600 }}>
            ← פתחו מאגר עם שדה יישוב בקונסולה ותראו את "בונה ההצלבה" בעצמכם
          </Link>
        </p>
      </section>

      <style>{`
        .jg h1, .jg h2, .jg h3 { text-wrap: balance; }
        .jg .mono { font-family: ui-monospace, Consolas, monospace; direction: ltr; unicode-bidi: embed; font-size: 0.9em; }
        .jg-eyebrow { font-size: 0.72rem; letter-spacing: 0.12em; font-weight: 700; text-transform: uppercase; color: var(--tint-pink-fg); }
        .jg-lead { color: var(--text-muted); line-height: 1.7; }
        .jg-card { border: 1px solid var(--border); border-radius: 12px; background: var(--surface); padding: 1.3rem 1.5rem; margin-bottom: 1.1rem; }
        .jg-card h2 { font-size: 1.3rem; margin: 0.2rem 0 0; }
        .jg-card h3 { font-size: 1.02rem; margin: 0 0 0.3rem; }
        .jg-kicker { font-size: 0.78rem; font-weight: 700; color: var(--primary); margin-bottom: 0.2rem; }
        .jg-ba { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin: 1rem 0; }
        .jg-ba-row { display: grid; grid-template-columns: 1fr auto 1fr; align-items: center; gap: 8px; border: 1px solid var(--border); border-radius: 10px; padding: 8px 12px; background: var(--surface-2); }
        .jg-val { font-family: ui-monospace, Consolas, monospace; font-size: 0.88rem; }
        .jg-val.dirty { color: var(--warning); }
        .jg-val.healed { color: var(--success); font-weight: 600; }
        .jg-tag { display: block; font-family: inherit; font-size: 0.66rem; color: var(--text-muted); margin-top: 2px; font-weight: 600; }
        .jg-arrow { color: var(--primary); font-size: 1.1rem; }
        .jg-mock { border: 1.5px dashed color-mix(in srgb, var(--primary) 45%, var(--border)); border-radius: 10px; padding: 0.9rem 1.1rem; margin: 1rem 0; background: var(--surface-2); display: grid; gap: 0.6rem; }
        .jg-mock-title { font-weight: 700; font-size: 0.92rem; }
        .jg-mock-row { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; font-size: 0.85rem; }
        .jg-mchip { font-size: 0.75rem; font-weight: 600; padding: 2px 9px; border-radius: 999px; border: 1px solid var(--border); }
        .jg-mchip.gray { color: var(--tint-neutral-fg); background: var(--tint-neutral-bg); }
        .jg-mchip.blue { color: var(--tint-sky-fg); background: var(--tint-sky-bg); border-color: var(--tint-sky-bd); }
        .jg-mchip.green { color: var(--tint-good-fg); background: var(--tint-good-bg); border-color: var(--tint-good-bd); }
        .jg-mbtn { font-size: 0.8rem; padding: 3px 10px; border-radius: 5px; border: 1px solid var(--border); background: var(--surface); color: var(--text-muted); }
        .jg-mbtn.on { background: var(--tint-sky-bg); color: var(--tint-sky-fg); font-weight: 700; border-color: var(--tint-sky-bd); }
        .jg-mbtn.run { background: var(--fill-brand); color: var(--on-fill); font-weight: 700; border-color: var(--fill-brand); }
        .jg-callout { display: flex; gap: 12px; align-items: flex-start; padding: 14px 16px; background: var(--tint-pink-bg); border: 1px solid var(--tint-pink-bd); border-radius: 10px; }
        .jg-mark { color: var(--tint-pink-fg); font-weight: 800; }
        @media (max-width: 620px) { .jg-ba { grid-template-columns: 1fr; } }
      `}</style>
    </div>
  );
}
