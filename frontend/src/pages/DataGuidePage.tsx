import { Link } from "react-router-dom";
import DataTabs from "../components/DataTabs";

// Guide for the "הצלבה מתוקנת" (healed cross-reference) feature — a dedicated
// /data/guide route, styled with the site's own tokens (--primary / --border /
// --bg-muted / --text-muted). Self-contained JSX (not markdown) because of the
// before/after cards, chips and result table.

const BA: [string, string, string][] = [
  ["כפי שנכתב במאגר", "הרצליה", "הרצלייה"],
  ["גרש משובש מקידוד ישן", "כפר חב~ד", "כפר חב״ד"],
  ["צורה קצרה", "בנימינה", "בנימינה-גבעת עדה"],
  ["קידומת מנהלית", "מ.א. מטה בנימין", "מטה בנימין"],
];

const RESULT: [string, string, string, string, string][] = [
  ["בני ברק", "תל אביב", "229,995", "4,227", "183.8"],
  ["צפת", "הצפון", "38,687", "655", "169.3"],
  ["ירושלים", "ירושלים", "1,050,151", "16,657", "158.6"],
  ["תל אביב-יפו", "תל אביב", "494,900", "7,054", "142.5"],
  ["בית שמש", "ירושלים", "176,786", "2,289", "129.5"],
];

const QUERY = `WITH ngos AS (
  SELECT over_settlement_code("כתובת - ישוב") AS code, count(*) AS עמותות
  FROM append_moj_amutot_73f3cd78
  WHERE "כתובת - ישוב" IS NOT NULL GROUP BY 1
)
SELECT s.name AS יישוב, s.district AS מחוז, s.population AS אוכלוסייה, n.עמותות,
       round(n.עמותות * 10000.0 / NULLIF(s.population,0), 1) AS ל10אלף
FROM ngos n JOIN over_settlements s ON s.code = n.code
WHERE s.population > 20000
ORDER BY ל10אלף DESC LIMIT 30`;

export default function DataGuidePage() {
  return (
    <div className="container mt-3 jg" dir="rtl">
      <DataTabs active="guide" />

      <header style={{ marginBottom: "1.5rem" }}>
        <div className="jg-eyebrow">גרסאות לעם · קונסולת /data</div>
        <h1 style={{ margin: "0.3rem 0 0.6rem", fontSize: "1.9rem" }}>הצלבה מתוקנת של שדות יישוב ורשות</h1>
        <p className="jg-lead" style={{ maxWidth: "62ch" }}>
          לחבר מאגרי מידע ממשלתיים לפי יישוב או רשות מקומית — גם כשכל מאגר כותב את אותו שם קצת אחרת.
          הכלי מתקן את השם בזמן התשאול, בלי לגעת בנתונים המקוריים.
        </p>
        <div className="jg-chips">
          <span className="jg-chip">1,490 יישובים</span>
          <span className="jg-chip">257 רשויות</span>
          <span className="jg-chip accent">~50K הטיות שם</span>
          <span className="jg-chip">מבוסס קובץ היישובים של הלמ״ס</span>
        </div>
      </header>

      {/* problem */}
      <section className="jg-card">
        <div className="jg-kicker">הבעיה</div>
        <h2>אותו יישוב, עשר צורות כתיבה</h2>
        <p className="jg-lead">
          כל משרד מקליד שמות יישובים בדרכו: כתיב מלא או חסר, קידומות, גרשיים משובשים מקידוד ישן, ורווחים
          נסתרים. התוצאה — שני מאגרים שמדברים על אותו מקום פשוט לא מצליבים.
        </p>
        <div className="jg-ba">
          {BA.map(([why, dirty, healed]) => (
            <div className="jg-ba-row" key={dirty}>
              <div className="jg-val dirty"><span className="jg-tag">{why}</span>{dirty}</div>
              <div className="jg-arrow">←</div>
              <div className="jg-val healed"><span className="jg-tag">הצורה הרשמית</span>{healed}</div>
            </div>
          ))}
        </div>
      </section>

      {/* how it works */}
      <section className="jg-card">
        <div className="jg-kicker">איך זה עובד</div>
        <h2>שלוש שכבות, אחת מעל השנייה</h2>
        <div className="jg-layers">
          <div className="jg-layer">
            <span className="jg-num">01</span>
            <div>
              <b>אינדקס היישובים והרשויות</b> — מאגר מעובד תחת המקור "גרסאות לעם" ב-/data: השמות הרשמיים
              מקובץ הלמ״ס, וליד כל שם — כל ההטיות שלו. ההטיות נוצרות אוטומטית (קידומות ב/ל/מ, איחוד
              רווח·מקף·גרש, אנגלית) <b>ונלמדות מהדאטה עצמו</b> — הכלי סורק את השדות בפועל וכל צורה חדשה
              נכנסת לאינדקס.
            </div>
          </div>
          <div className="jg-layer">
            <span className="jg-num">02</span>
            <div>
              <b>פונקציות SQL</b> — עוטפות כל ערך ומחזירות את הצורה הקנונית, לתשאול חופשי:
              <pre className="jg-pre" style={{ marginTop: "0.6rem" }}>{`over_settlement("שם עיר")      → שם היישוב הרשמי
over_settlement_code("שם עיר") → סמל היישוב
over_authority("רשות")        → שם הרשות הרשמי`}</pre>
            </div>
          </div>
          <div className="jg-layer">
            <span className="jg-num">03</span>
            <div>
              <b>בונה הצלבה ויזואלי</b> — למי שלא כותב SQL. בעמוד של כל טבלה עם שדה יישוב נפתח
              "בונה הצלבה מתוקנת": בוחרים מאגר שני (או את האינדקס להעשרה), לוחצים "צור והרץ", והכלי כותב
              ומריץ את ה-SQL. לפני ההרצה מוצג <b>כיסוי חי</b> — כמה מהערכים נפתרים.
            </div>
          </div>
        </div>
      </section>

      {/* usage */}
      <section className="jg-card">
        <div className="jg-kicker">שימוש</div>
        <h2>שני מצבים בבונה</h2>
        <div className="jg-two">
          <div className="jg-mini">
            <h3>הצלבה למאגר אחר</h3>
            <p>מחברים שני מאגרים לפי יישוב/רשות. <b>שני הצדדים</b> מרופאים לסמל הקנוני, וההצלבה נעשית עליו — כך ערכים בכתיב שונה עדיין נפגשים.</p>
          </div>
          <div className="jg-mini">
            <h3>העשרה מהאינדקס</h3>
            <p>מוסיפים לכל שורה <b>מחוז · נפה · אוכלוסייה · מעמד מוניציפלי</b> מקובץ הלמ״ס — לפי היישוב/רשות שבשדה, גם אם כתוב לא-סטנדרטית.</p>
          </div>
        </div>
        <h3 style={{ marginTop: "1.2rem" }}>הצעדים בעמוד /data</h3>
        <ol className="jg-steps">
          <li>בוחרים טבלה עם שדה יישוב או רשות (למשל מאגר העמותות).</li>
          <li>פותחים את "🔗 בונה הצלבה מתוקנת" מתחת לפאנל הפרופיל.</li>
          <li>בוחרים מצב ושדות, ולוחצים "בדוק כיסוי" כדי לראות את איכות ההתאמה.</li>
          <li>לוחצים "צור והרץ" — ה-SQL נטען לעורך ורץ מיד.</li>
        </ol>
      </section>

      {/* worked example */}
      <section className="jg-card">
        <div className="jg-kicker">דוגמה עובדת</div>
        <h2>עמותות ביחס לאוכלוסייה, לפי יישוב</h2>
        <p className="jg-lead">
          כתובות היישוב במאגר העמותות מלוכלכות. אחרי ריפוי לסמל הקנוני אפשר להצליב לאינדקס היישובים
          ולחשב צפיפות עמותות ל-10,000 תושבים — הצלבה שבלי הריפוי לא הייתה מתאימה.
        </p>
        <pre className="jg-pre">{QUERY}</pre>
        <div className="jg-tblwrap">
          <table className="jg-tbl">
            <thead><tr><th>יישוב</th><th>מחוז</th><th>אוכלוסייה</th><th>עמותות</th><th>ל-10 אלף</th></tr></thead>
            <tbody>
              {RESULT.map((r) => (
                <tr key={r[0]}>
                  <td>{r[0]}</td><td>{r[1]}</td>
                  <td className="num">{r[2]}</td><td className="num">{r[3]}</td>
                  <td className="hl">{r[4]}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p style={{ textAlign: "center" }}>
          <Link to="/data?table=append_moj_amutot_73f3cd78" style={{ color: "var(--primary, #0f766e)", fontWeight: 600 }}>
            ← פתחו את מאגר העמותות בקונסולה ונסו בעצמכם
          </Link>
        </p>
      </section>

      {/* enables */}
      <section className="jg-card">
        <div className="jg-kicker">מה זה מאפשר</div>
        <h2>הצלבות שהיו בלתי אפשריות</h2>
        <ul className="jg-list">
          <li>לחבר נתוני משרד אחד (עמותות, מכרזים, רישוי) עם נתוני רשות אחרת — <b>לפי יישוב</b>.</li>
          <li>להוסיף לכל טבלה מחוז, נפה, אוכלוסייה או מעמד מוניציפלי — בלחיצה.</li>
          <li>לנרמל שדה יישוב מלוכלך לצורך קיבוץ, מיפוי או ניתוח לאורך זמן.</li>
          <li>לראות מראש כמה מהערכים יצליחו להצליב — ואילו נשארו חריגים.</li>
        </ul>
        <div className="jg-callout">
          <span className="jg-mark">◆</span>
          <div><b>המקור נשאר נקי.</b> הכלי לא מוסיף עמודות ולא משנה את הנתונים שהמדינה סיפקה — הריפוי קורה
            תוך כדי התשאול בלבד, דרך הפונקציות. מי שרוצה את ההצלבה מקבל אותה; הטבלה המקורית לא זזה.</div>
        </div>
      </section>

      {/* reference */}
      <section className="jg-card">
        <div className="jg-kicker">מונחי מפתח</div>
        <dl className="jg-ref">
          <dt>over_settlement(text)</dt>
          <dd>ערך יישוב חופשי → השם הרשמי מקובץ הלמ״ס (או NULL אם אינו מקום).</dd>
          <dt>over_authority(text)</dt>
          <dd>אותו הדבר לרשויות מקומיות — כולל מועצות אזוריות, שאינן יישוב בודד.</dd>
          <dt>over_settlements / over_authorities</dt>
          <dd>טבלאות האינדקס לתשאול או JOIN ישיר — עם מחוז, נפה, אוכלוסייה ומעמד.</dd>
          <dt>over_settlement_aliases</dt>
          <dd>אינדקס ההטיות: כל צורת כתיבה שמופתה לסמל יישוב — הבסיס לריפוי.</dd>
        </dl>
      </section>

      <style>{`
        .jg h1, .jg h2, .jg h3 { text-wrap: balance; }
        .jg-eyebrow { font-size: 0.72rem; letter-spacing: 0.12em; font-weight: 700; text-transform: uppercase; color: var(--accent, #a21caf); }
        .jg-lead { color: var(--text-muted, #64748b); line-height: 1.7; }
        .jg-chips { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 1rem; }
        .jg-chip { font-size: 0.75rem; font-weight: 600; padding: 3px 10px; border-radius: 999px; border: 1px solid var(--border, #e5e7eb); color: var(--text-muted, #64748b); background: var(--bg, #fff); }
        .jg-chip.accent { color: #a21caf; background: #fbeafc; border-color: #edc8f2; }
        .jg-card { border: 1px solid var(--border, #e5e7eb); border-radius: 12px; background: var(--bg, #fff); padding: 1.3rem 1.5rem; margin-bottom: 1.1rem; }
        .jg-card h2 { font-size: 1.3rem; margin: 0.2rem 0 0; }
        .jg-card h3 { font-size: 1.02rem; margin: 0 0 0.3rem; }
        .jg-card p { line-height: 1.7; }
        .jg-kicker { font-size: 0.78rem; font-weight: 700; letter-spacing: 0.03em; color: var(--primary, #0f766e); margin-bottom: 0.2rem; }
        .jg-ba { display: grid; gap: 8px; margin-top: 1rem; }
        .jg-ba-row { display: grid; grid-template-columns: 1fr auto 1fr; align-items: center; gap: 12px; border: 1px solid var(--border, #e5e7eb); border-radius: 10px; padding: 10px 14px; background: var(--bg-muted, #f8fafc); }
        .jg-val { font-family: ui-monospace, Consolas, monospace; font-size: 0.92rem; }
        .jg-val.dirty { color: #b45309; }
        .jg-val.healed { color: #15803d; font-weight: 600; }
        .jg-tag { display: block; font-family: inherit; font-size: 0.68rem; letter-spacing: .03em; color: var(--text-muted, #94a3b8); margin-bottom: 3px; font-weight: 600; }
        .jg-arrow { color: var(--primary, #0f766e); font-size: 1.2rem; }
        .jg-layers { display: grid; gap: 12px; margin-top: 1rem; }
        .jg-layer { display: grid; grid-template-columns: auto 1fr; gap: 12px; align-items: start; }
        .jg-num { font-family: ui-monospace, Consolas, monospace; font-weight: 700; color: #fff; background: var(--primary, #0f766e); width: 30px; height: 30px; display: grid; place-items: center; border-radius: 8px; font-size: 0.85rem; }
        .jg-two { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 1rem; }
        .jg-mini { border: 1px solid var(--border, #e5e7eb); border-radius: 10px; padding: 0.9rem 1.1rem; background: var(--bg-muted, #f8fafc); }
        .jg-mini p { font-size: 0.9rem; color: var(--text-muted, #64748b); margin: 0; }
        .jg-steps { counter-reset: s; list-style: none; padding: 0; margin: 0.8rem 0 0; display: grid; gap: 8px; }
        .jg-steps li { counter-increment: s; position: relative; padding: 10px 44px 10px 14px; border: 1px solid var(--border, #e5e7eb); border-radius: 10px; font-size: 0.92rem; background: var(--bg, #fff); }
        .jg-steps li::before { content: counter(s); position: absolute; right: 12px; top: 9px; width: 24px; height: 24px; display: grid; place-items: center; border-radius: 6px; background: var(--primary, #0f766e); color: #fff; font-family: ui-monospace, Consolas, monospace; font-weight: 700; font-size: 0.8rem; }
        .jg-pre { direction: ltr; text-align: left; background: #0e1a18; color: #d7e6e2; border-radius: 10px; padding: 14px 16px; overflow-x: auto; font-family: ui-monospace, Consolas, monospace; font-size: 0.82rem; line-height: 1.6; }
        .jg-tblwrap { overflow-x: auto; border: 1px solid var(--border, #e5e7eb); border-radius: 10px; margin: 1rem 0; }
        .jg-tbl { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
        .jg-tbl th, .jg-tbl td { text-align: right; padding: 9px 14px; border-bottom: 1px solid var(--border, #eef2f5); }
        .jg-tbl thead th { background: var(--bg-muted, #eef2f5); color: var(--primary, #0f766e); font-weight: 700; font-size: 0.82rem; }
        .jg-tbl tbody tr:last-child td { border-bottom: none; }
        .jg-tbl td.num { font-variant-numeric: tabular-nums; font-family: ui-monospace, Consolas, monospace; }
        .jg-tbl td.hl { color: #15803d; font-weight: 700; font-variant-numeric: tabular-nums; font-family: ui-monospace, Consolas, monospace; }
        .jg-list { padding-right: 20px; line-height: 1.8; }
        .jg-callout { display: flex; gap: 12px; align-items: flex-start; margin-top: 1rem; padding: 14px 16px; background: #fbeafc; border: 1px solid #edc8f2; border-radius: 10px; }
        .jg-mark { color: #a21caf; font-weight: 800; }
        .jg-ref dt { font-family: ui-monospace, Consolas, monospace; font-weight: 700; color: var(--primary, #0f766e); direction: ltr; text-align: right; margin-top: 0.7rem; }
        .jg-ref dd { margin: 0.15rem 0 0; color: var(--text-muted, #64748b); font-size: 0.92rem; }
        @media (max-width: 620px) { .jg-two { grid-template-columns: 1fr; } .jg-ba-row { grid-template-columns: 1fr; text-align: center; } .jg-arrow { transform: rotate(90deg); } }
      `}</style>
    </div>
  );
}
